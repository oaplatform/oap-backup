package storage

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
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

// TempSuffix is appended to the name of a file while FTP uploads it.
const TempSuffix = ".tmp"

// Upload implements Storage. The file is sent as <name>.tmp and renamed to
// <name> only when it is complete, so a reader of the folder never takes a
// half-uploaded file for a backup. A .tmp file left behind by an interrupted
// upload is not a backup file; retention pruning deletes it.
func (f *FTP) Upload(ctx context.Context, dir, name, localPath string, progress func(n int64)) (string, error) {
	file, err := os.Open(localPath)
	if err != nil {
		return "", err
	}
	defer file.Close()
	var body io.Reader = file
	if progress != nil {
		body = &progressReader{r: file, fn: progress}
	}

	remoteDir := f.remoteDir(dir)
	remotePath := path.Join(remoteDir, name)
	err = f.with(ctx, func(c *ftp.ServerConn) error {
		if err := ensureDir(c, remoteDir); err != nil {
			return err
		}
		tempPath := remotePath + TempSuffix
		if err := c.Stor(tempPath, body); err != nil {
			// Best effort: the connection may already be gone.
			_ = c.Delete(tempPath)
			return fmt.Errorf("ftp STOR %s: %w", tempPath, err)
		}
		if err := c.Rename(tempPath, remotePath); err != nil {
			// Some servers refuse to rename onto an existing file, which
			// happens when the same backup is uploaded again.
			if delErr := c.Delete(remotePath); delErr != nil {
				return fmt.Errorf("ftp RNTO %s -> %s: %w", tempPath, remotePath, err)
			}
			if err := c.Rename(tempPath, remotePath); err != nil {
				return fmt.Errorf("ftp RNTO %s -> %s: %w", tempPath, remotePath, err)
			}
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

// ListDirs implements Storage.
func (f *FTP) ListDirs(ctx context.Context, dir string) ([]string, error) {
	entries, err := f.entries(ctx, dir)
	var names []string
	for _, e := range entries {
		if e.Type == ftp.EntryTypeFolder {
			names = append(names, e.Name)
		}
	}
	return names, err
}

// ListFiles implements Storage.
func (f *FTP) ListFiles(ctx context.Context, dir string) ([]File, error) {
	entries, err := f.entries(ctx, dir)
	var files []File
	for _, e := range entries {
		if e.Type == ftp.EntryTypeFile {
			files = append(files, File{Name: e.Name, Size: int64(e.Size), ModTime: e.Time})
		}
	}
	return files, err
}

// entries returns the detailed listing (LIST/MLSD) of <ftp_folder>/<dir>
// without "." and "..". As in List, a missing or unreadable directory is
// empty.
func (f *FTP) entries(ctx context.Context, dir string) ([]*ftp.Entry, error) {
	remoteDir := f.remoteDir(dir)
	var out []*ftp.Entry
	err := f.with(ctx, func(c *ftp.ServerConn) error {
		entries, err := c.List(remoteDir)
		if err != nil {
			var protoErr *textproto.Error
			if errors.As(err, &protoErr) {
				return nil
			}
			return fmt.Errorf("ftp LIST %s: %w", remoteDir, err)
		}
		for _, e := range entries {
			if e.Name == "." || e.Name == ".." {
				continue
			}
			out = append(out, e)
		}
		return nil
	})
	return out, err
}

// RemoveDir implements Storage.
func (f *FTP) RemoveDir(ctx context.Context, dir string) error {
	remoteDir := f.remoteDir(dir)
	return f.with(ctx, func(c *ftp.ServerConn) error {
		if err := c.RemoveDir(remoteDir); err != nil {
			return fmt.Errorf("ftp RMD %s: %w", remoteDir, err)
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
