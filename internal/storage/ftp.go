package storage

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/textproto"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/jlaffaye/ftp"

	"oap-backup/internal/config"
)

// FTP stores backups on an FTP server. Every operation opens its own
// connection, so a broken connection never outlives one operation.
type FTP struct {
	cfg config.FTP
	// sessions keeps TLS sessions so data connections can resume the
	// control connection's session (required by e.g. vsftpd require_ssl_reuse).
	sessions tls.ClientSessionCache
}

// NewFTP returns an FTP storage for cfg.
func NewFTP(cfg config.FTP) *FTP {
	return &FTP{cfg: cfg, sessions: tls.NewLRUClientSessionCache(16)}
}

// remoteDir is <ftp_folder>/<dir> as an absolute path.
func (f *FTP) remoteDir(dir string) string {
	return path.Join("/", f.cfg.Folder, dir)
}

// Upload implements Storage.
func (f *FTP) Upload(ctx context.Context, dir, name, localPath string) (string, error) {
	file, err := os.Open(localPath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	remoteDir := f.remoteDir(dir)
	remotePath := path.Join(remoteDir, name)
	err = f.with(ctx, func(c *ftp.ServerConn) error {
		if err := ensureDir(c, remoteDir); err != nil {
			return err
		}
		if err := c.Stor(remotePath, file); err != nil {
			return fmt.Errorf("ftp STOR %s: %w", remotePath, err)
		}
		return nil
	})
	return "ftp:" + remotePath, err
}

// List implements Storage.
func (f *FTP) List(ctx context.Context, dir string) ([]string, error) {
	remoteDir := f.remoteDir(dir)
	var names []string
	err := f.with(ctx, func(c *ftp.ServerConn) error {
		entries, err := c.NameList(remoteDir)
		if err != nil {
			// Same as the Python pruner: a 4xx/5xx reply (directory missing
			// or not readable) means there is nothing to prune.
			var protoErr *textproto.Error
			if errors.As(err, &protoErr) {
				return nil
			}
			return fmt.Errorf("ftp NLST %s: %w", remoteDir, err)
		}
		names = basenames(entries)
		return nil
	})
	return names, err
}

// Delete implements Storage.
func (f *FTP) Delete(ctx context.Context, dir, name string) error {
	remotePath := path.Join(f.remoteDir(dir), name)
	return f.with(ctx, func(c *ftp.ServerConn) error {
		if err := c.Delete(remotePath); err != nil {
			return fmt.Errorf("ftp DELE %s: %w", remotePath, err)
		}
		return nil
	})
}

// basenames strips directory parts (servers differ on full paths vs bare
// names in NLST output) and drops "." / ".." entries.
func basenames(entries []string) []string {
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		name := e[strings.LastIndex(e, "/")+1:]
		if name == "" || name == "." || name == ".." {
			continue
		}
		out = append(out, name)
	}
	return out
}

// ensureDir creates every missing component of an absolute remote dir.
func ensureDir(c *ftp.ServerConn, dir string) error {
	current := ""
	for _, part := range strings.Split(dir, "/") {
		if part == "" {
			continue
		}
		current += "/" + part
		if err := c.ChangeDir(current); err == nil {
			continue
		}
		if err := c.MakeDir(current); err != nil {
			return fmt.Errorf("ftp MKD %s: %w", current, err)
		}
	}
	return nil
}

// with connects, logs in, runs op and disconnects. Cancelling ctx closes
// every network connection of the session, which aborts a blocked transfer.
func (f *FTP) with(ctx context.Context, op func(*ftp.ServerConn) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	var tlsCfg *tls.Config
	if f.cfg.TLS {
		host, _, err := net.SplitHostPort(f.cfg.Addr)
		if err != nil {
			host = f.cfg.Addr
		}
		tlsCfg = &tls.Config{ServerName: host, ClientSessionCache: f.sessions, MinVersion: tls.VersionTLS12}
	}

	tracker := &connTracker{}
	stop := context.AfterFunc(ctx, tracker.closeAll)
	defer stop()

	dialer := &net.Dialer{Timeout: f.cfg.Timeout}
	dial := func(network, address string) (net.Conn, error) {
		conn, err := dialer.DialContext(ctx, network, address)
		if err != nil {
			return nil, err
		}
		// The first connection is the control connection, which the ftp
		// library upgrades itself (AUTH TLS). Data connections opened via a
		// custom dial func are not wrapped by the library, so wrap them here.
		if tracker.add(conn) > 1 && tlsCfg != nil {
			return tls.Client(conn, tlsCfg), nil
		}
		return conn, nil
	}

	opts := []ftp.DialOption{ftp.DialWithDialFunc(dial)}
	if tlsCfg != nil {
		opts = append(opts, ftp.DialWithExplicitTLS(tlsCfg))
	}
	c, err := ftp.Dial(f.cfg.Addr, opts...)
	if err != nil {
		return ctxErr(ctx, fmt.Errorf("ftp connect %s: %w", f.cfg.Addr, err))
	}
	defer func() {
		_ = c.Quit()
		tracker.closeAll()
	}()

	user := f.cfg.User
	if user == "" {
		user = "anonymous"
	}
	if err := c.Login(user, f.cfg.Password); err != nil {
		return ctxErr(ctx, fmt.Errorf("ftp login as %s: %w", user, err))
	}
	return ctxErr(ctx, op(c))
}

// ctxErr prefers the context error, which explains why a connection was cut.
func ctxErr(ctx context.Context, err error) error {
	if err != nil && ctx.Err() != nil {
		return fmt.Errorf("%w (%w)", ctx.Err(), err)
	}
	return err
}

type connTracker struct {
	mu    sync.Mutex
	conns []net.Conn
	n     int
}

// add records conn and returns how many connections were opened so far.
func (t *connTracker) add(conn net.Conn) int {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.conns = append(t.conns, conn)
	t.n++
	return t.n
}

func (t *connTracker) closeAll() {
	t.mu.Lock()
	conns := t.conns
	t.conns = nil
	t.mu.Unlock()
	for _, c := range conns {
		_ = c.Close()
	}
}
