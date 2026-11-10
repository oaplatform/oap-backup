// Package backup runs one backup of an annotated pod: optional helper pod,
// exec of the backup command, upload to storage, and retention pruning.
package backup

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	"oap-backup/internal/annotations"
	"oap-backup/internal/kube"
	"oap-backup/internal/metrics"
	"oap-backup/internal/storage"
)

// HelperManager creates and deletes helper pods (implemented by kube.Helpers).
type HelperManager interface {
	Create(ctx context.Context, namespace, pod, container, image string) (string, error)
	Delete(ctx context.Context, namespace, name string) error
}

// Runner executes backups. All fields except Metrics and Now are required.
type Runner struct {
	Exec    kube.Executor
	Helpers HelperManager
	Storage storage.Storage
	Metrics *metrics.Metrics
	Logger  *slog.Logger

	TmpDir        string
	Retries       int
	RetryDelay    time.Duration
	BackupTimeout time.Duration

	// Now returns the current time; defaults to time.Now.
	Now func() time.Time
}

// Result describes one uploaded backup file.
type Result struct {
	FileName  string
	SizeBytes int64
	Location  string
}

func (r *Runner) now() time.Time {
	if r.Now != nil {
		return r.Now()
	}
	return time.Now()
}

// Run performs the full pipeline for t: backup (with retries, each attempt
// in a fresh helper pod when t.ExternalImage is set), then prune. As in the
// original DAG, pruning runs even when the backup failed; it only ever keeps
// the newest RetentionKeepLast files, so older good backups are not lost.
func (r *Runner) Run(ctx context.Context, t annotations.Target) error {
	log := r.Logger.With("target", t.Key(), "folder", t.FileFolder)
	start := r.now()

	res, backupErr := r.backupWithRetries(ctx, t, log)

	duration := r.now().Sub(start)
	if r.Metrics != nil {
		r.Metrics.LastDuration.WithLabelValues(t.Namespace, t.FileFolder).Set(duration.Seconds())
	}
	if backupErr != nil {
		log.Error("backup failed", "duration", duration.Round(time.Millisecond), "error", backupErr)
		if r.Metrics != nil {
			r.Metrics.Runs.WithLabelValues(t.Namespace, t.FileFolder, metrics.ResultFailure).Inc()
		}
	} else {
		log.Info("backup complete", "file", res.FileName, "size_bytes", res.SizeBytes,
			"location", res.Location, "duration", duration.Round(time.Millisecond))
		if r.Metrics != nil {
			r.Metrics.Runs.WithLabelValues(t.Namespace, t.FileFolder, metrics.ResultSuccess).Inc()
			r.Metrics.LastSuccess.WithLabelValues(t.Namespace, t.FileFolder).Set(float64(r.now().Unix()))
			r.Metrics.LastSize.WithLabelValues(t.Namespace, t.FileFolder).Set(float64(res.SizeBytes))
		}
	}

	if ctx.Err() != nil {
		return errors.Join(backupErr, ctx.Err())
	}
	pruneCtx, cancel := context.WithTimeout(ctx, r.BackupTimeout)
	defer cancel()
	if _, err := r.Prune(pruneCtx, t); err != nil {
		log.Error("prune failed", "error", err)
		if r.Metrics != nil {
			r.Metrics.PruneFailures.WithLabelValues(t.Namespace, t.FileFolder).Inc()
		}
		return errors.Join(backupErr, fmt.Errorf("prune: %w", err))
	}
	return backupErr
}

func (r *Runner) backupWithRetries(ctx context.Context, t annotations.Target, log *slog.Logger) (Result, error) {
	var err error
	for attempt := 0; attempt <= r.Retries; attempt++ {
		if attempt > 0 {
			log.Warn("backup attempt failed, retrying", "attempt", attempt, "of", r.Retries+1,
				"retry_delay", r.RetryDelay, "error", err)
			select {
			case <-ctx.Done():
				return Result{}, errors.Join(err, ctx.Err())
			case <-time.After(r.RetryDelay):
			}
		}
		var res Result
		res, err = r.attempt(ctx, t, log)
		if err == nil {
			return res, nil
		}
		if ctx.Err() != nil {
			return Result{}, err
		}
	}
	return Result{}, err
}

// attempt runs one backup, in a helper pod if configured, bounded by
// BackupTimeout. The helper pod is always deleted afterwards.
func (r *Runner) attempt(ctx context.Context, t annotations.Target, log *slog.Logger) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, r.BackupTimeout)
	defer cancel()

	execPod, execContainer := t.Pod, t.Container
	if t.ExternalImage != "" {
		helper, err := r.Helpers.Create(ctx, t.Namespace, t.Pod, t.Container, t.ExternalImage)
		if err != nil {
			return Result{}, fmt.Errorf("prebackup: %w", err)
		}
		log.Info("created helper pod", "helper_pod", helper, "image", t.ExternalImage)
		defer func() {
			// Fresh context: clean up even after a timeout or shutdown.
			delCtx, delCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
			defer delCancel()
			if err := r.Helpers.Delete(delCtx, t.Namespace, helper); err != nil {
				log.Error("postbackup: deleting helper pod failed", "helper_pod", helper, "error", err)
				return
			}
			log.Info("deleted helper pod", "helper_pod", helper)
		}()
		execPod, execContainer = helper, kube.HelperContainer
	}

	return r.Backup(ctx, t, execPod, execContainer, log)
}

// Backup execs t.Command in namespace t.Namespace, pod execPod, container
// execContainer, writes its stdout (gzip-compressed unless the extension
// already ends in .gz) to a temp file, and uploads the file to
// <file-folder>/backup-<timestamp><suffix>. The temp file is always removed.
func (r *Runner) Backup(ctx context.Context, t annotations.Target, execPod, execContainer string, log *slog.Logger) (Result, error) {
	name := t.FileName(r.now())
	log.Info("running backup command", "pod", execPod, "container", execContainer, "command", t.Command, "file", name)

	f, err := os.CreateTemp(r.TmpDir, "oap-backup-*-"+name)
	if err != nil {
		return Result{}, fmt.Errorf("creating temp file: %w", err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	var out io.Writer = f
	var gz *gzip.Writer
	if t.Gzip() {
		gz = gzip.NewWriter(f)
		out = gz
	}
	stderr := &lineLogger{log: log, pod: execPod}

	execErr := r.Exec.Exec(ctx, t.Namespace, execPod, execContainer, []string{"sh", "-c", t.Command}, out, stderr)
	stderr.Flush()
	if execErr != nil {
		var exitErr *kube.ExitError
		if errors.As(execErr, &exitErr) {
			return Result{}, fmt.Errorf("backup command exited with code %d", exitErr.Code)
		}
		return Result{}, fmt.Errorf("exec backup command: %w", execErr)
	}
	if gz != nil {
		if err := gz.Close(); err != nil {
			return Result{}, fmt.Errorf("finishing gzip stream: %w", err)
		}
	}
	if err := f.Close(); err != nil {
		return Result{}, fmt.Errorf("writing temp file: %w", err)
	}
	info, err := os.Stat(f.Name())
	if err != nil {
		return Result{}, err
	}
	log.Info("backup file written", "file", name, "size_bytes", info.Size())

	location, err := r.Storage.Upload(ctx, t.FileFolder, name, f.Name())
	if err != nil {
		return Result{}, fmt.Errorf("upload: %w", err)
	}
	return Result{FileName: name, SizeBytes: info.Size(), Location: location}, nil
}

// lineLogger logs each line written to it (the backup command's stderr).
type lineLogger struct {
	log *slog.Logger
	pod string
	buf []byte
}

const maxLine = 16 << 10

func (l *lineLogger) Write(p []byte) (int, error) {
	l.buf = append(l.buf, p...)
	for {
		i := bytes.IndexByte(l.buf, '\n')
		if i < 0 {
			if len(l.buf) > maxLine {
				l.emit(l.buf)
				l.buf = l.buf[:0]
			}
			return len(p), nil
		}
		l.emit(l.buf[:i])
		l.buf = l.buf[i+1:]
	}
}

// Flush logs a trailing line without a newline.
func (l *lineLogger) Flush() {
	if len(l.buf) > 0 {
		l.emit(l.buf)
		l.buf = nil
	}
}

func (l *lineLogger) emit(line []byte) {
	line = bytes.TrimRight(line, "\r")
	if len(line) == 0 {
		return
	}
	l.log.Info("backup command stderr", "pod", l.pod, "line", string(line))
}
