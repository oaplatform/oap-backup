// Package backup runs one backup of an annotated pod: optional helper pod,
// exec of the backup command, upload to storage, and retention pruning.
package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"

	"oap-backup/internal/annotations"
	"oap-backup/internal/kube"
	"oap-backup/internal/metrics"
	"oap-backup/internal/storage"
)

// HelperManager creates and deletes helper pods (implemented by kube.Helpers).
type HelperManager interface {
	Create(ctx context.Context, namespace, pod, container, image string, env []corev1.EnvVar) (string, error)
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

// Result describes what one backup run uploaded: a single file, or for a
// partitioned target the files of all changed parts (FileName and Location
// are then empty and SizeBytes is their total size).
type Result struct {
	FileName  string
	SizeBytes int64
	Location  string
	// Files is the number of uploaded files.
	Files int
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
//
// A partitioned target (t.Parts) backs up every changed part into its own
// folder and prunes each part folder; see partsAttempt.
//
// The Result describes what the backup uploaded, also when pruning failed.
func (r *Runner) Run(ctx context.Context, t annotations.Target) (Result, error) {
	log := r.Logger.With("target", t.Key(), "folder", t.FileFolder)
	start := r.now()

	attempt := r.attempt
	if t.Parts {
		attempt = r.partsAttempt
	}
	res, backupErr := r.withRetries(ctx, log, func() (Result, error) { return attempt(ctx, t, log) })

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
		if t.Parts {
			log.Info("backup complete", "files", res.Files, "size_bytes", res.SizeBytes,
				"duration", duration.Round(time.Millisecond))
		} else {
			log.Info("backup complete", "file", res.FileName, "size_bytes", res.SizeBytes,
				"location", res.Location, "duration", duration.Round(time.Millisecond))
		}
		if r.Metrics != nil {
			r.Metrics.Runs.WithLabelValues(t.Namespace, t.FileFolder, metrics.ResultSuccess).Inc()
			r.Metrics.LastSuccess.WithLabelValues(t.Namespace, t.FileFolder).Set(float64(r.now().Unix()))
			r.Metrics.LastSize.WithLabelValues(t.Namespace, t.FileFolder).Set(float64(res.SizeBytes))
		}
	}

	if ctx.Err() != nil {
		return res, errors.Join(backupErr, ctx.Err())
	}
	pruneCtx, cancel := context.WithTimeout(ctx, r.BackupTimeout)
	defer cancel()
	var pruneErr error
	if t.Parts {
		pruneErr = r.pruneParts(pruneCtx, t, log)
	} else {
		_, pruneErr = r.Prune(pruneCtx, t)
	}
	if pruneErr != nil {
		log.Error("prune failed", "error", pruneErr)
		if r.Metrics != nil {
			r.Metrics.PruneFailures.WithLabelValues(t.Namespace, t.FileFolder).Inc()
		}
		return res, errors.Join(backupErr, fmt.Errorf("prune: %w", pruneErr))
	}
	return res, backupErr
}

// withRetries runs attempt until it succeeds, at most Retries+1 times,
// RetryDelay apart. It stops early when ctx is done.
func (r *Runner) withRetries(ctx context.Context, log *slog.Logger, attempt func() (Result, error)) (Result, error) {
	var err error
	for n := 0; n <= r.Retries; n++ {
		if n > 0 {
			log.Warn("backup attempt failed, retrying", "attempt", n, "of", r.Retries+1,
				"retry_delay", r.RetryDelay, "error", err)
			select {
			case <-ctx.Done():
				return Result{}, errors.Join(err, ctx.Err())
			case <-time.After(r.RetryDelay):
			}
		}
		var res Result
		res, err = attempt()
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
		helper, cleanup, err := r.startHelper(ctx, t, nil, log)
		if err != nil {
			return Result{}, err
		}
		defer cleanup()
		execPod, execContainer = helper, kube.HelperContainer
	}

	return r.Backup(ctx, t, execPod, execContainer, log)
}

// startHelper creates t's helper pod with env on its container and returns
// its name and a cleanup func that deletes it. cleanup must always be called;
// it works even after ctx timed out or was cancelled.
func (r *Runner) startHelper(ctx context.Context, t annotations.Target, env []corev1.EnvVar, log *slog.Logger) (string, func(), error) {
	helper, err := r.Helpers.Create(ctx, t.Namespace, t.Pod, t.Container, t.ExternalImage, env)
	if err != nil {
		return "", nil, fmt.Errorf("prebackup: %w", err)
	}
	log.Info("created helper pod", "helper_pod", helper, "image", t.ExternalImage)
	cleanup := func() {
		// Fresh context: clean up even after a timeout or shutdown.
		delCtx, delCancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer delCancel()
		if err := r.Helpers.Delete(delCtx, t.Namespace, helper); err != nil {
			log.Error("postbackup: deleting helper pod failed", "helper_pod", helper, "error", err)
			return
		}
		log.Info("deleted helper pod", "helper_pod", helper)
	}
	return helper, cleanup, nil
}

// Backup execs t.Command in namespace t.Namespace, pod execPod, container
// execContainer and uploads its output to
// <file-folder>/backup-<timestamp><suffix>; see backupTo.
func (r *Runner) Backup(ctx context.Context, t annotations.Target, execPod, execContainer string, log *slog.Logger) (Result, error) {
	return r.backupTo(ctx, t, t.FileFolder, t.FileName(r.now()), t.Command, execPod, execContainer, log)
}

// backupTo execs command in namespace t.Namespace, pod execPod, container
// execContainer, writes its stdout (compressed with t.Codec, unless t's
// extension says it already is) to a temp file, and uploads the file as
// <dir>/<name>. The temp file is always removed.
func (r *Runner) backupTo(ctx context.Context, t annotations.Target, dir, name, command, execPod, execContainer string, log *slog.Logger) (Result, error) {
	log.Info("running backup command", "pod", execPod, "container", execContainer, "command", command,
		"dir", dir, "file", name)

	f, err := os.CreateTemp(r.TmpDir, "oap-backup-*-"+name)
	if err != nil {
		return Result{}, fmt.Errorf("creating temp file: %w", err)
	}
	defer os.Remove(f.Name())
	defer f.Close()

	var out io.Writer = f
	compressor, err := newCompressor(f, t.Codec(), t.CompressionLevel)
	if err != nil {
		return Result{}, err
	}
	if compressor != nil {
		out = compressor
	}
	stderr := &lineLogger{log: log, pod: execPod}

	execErr := r.Exec.Exec(ctx, t.Namespace, execPod, execContainer, []string{"sh", "-c", command}, out, stderr)
	stderr.Flush()
	if execErr != nil {
		var exitErr *kube.ExitError
		if errors.As(execErr, &exitErr) {
			return Result{}, fmt.Errorf("backup command exited with code %d", exitErr.Code)
		}
		return Result{}, fmt.Errorf("exec backup command: %w", execErr)
	}
	if compressor != nil {
		if err := compressor.Close(); err != nil {
			return Result{}, fmt.Errorf("finishing %s stream: %w", t.Codec(), err)
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

	location, err := r.Storage.Upload(ctx, dir, name, f.Name())
	if err != nil {
		return Result{}, fmt.Errorf("upload: %w", err)
	}
	return Result{FileName: name, SizeBytes: info.Size(), Location: location, Files: 1}, nil
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
