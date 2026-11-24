package backup

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"

	corev1 "k8s.io/api/core/v1"

	"oap-backup/internal/annotations"
	"oap-backup/internal/kube"
	"oap-backup/internal/metrics"
)

// maxPartsOutput caps the captured stdout of a parts command.
const maxPartsOutput = 4 << 20

// remotePart is one part folder on the storage. MT is the timestamp
// (YYYYMMDDHHMMSS) of its newest backup file, or "" if it has none.
type remotePart struct {
	ID string
	MT string
}

// remoteParts reads the part folders of t from the storage, sorted by id.
// Folders whose name is not a valid part id are not parts and are ignored.
func (r *Runner) remoteParts(ctx context.Context, t annotations.Target, log *slog.Logger) ([]remotePart, error) {
	ids, err := r.Storage.ListDirs(ctx, t.FileFolder)
	if err != nil {
		return nil, fmt.Errorf("listing part folders: %w", err)
	}
	sort.Strings(ids)

	var parts []remotePart
	for _, id := range ids {
		if !annotations.ValidPartID(id) {
			log.Warn("ignoring folder that is not a valid part id", "name", id)
			continue
		}
		names, err := r.Storage.List(ctx, t.PartFolder(id))
		if err != nil {
			return nil, fmt.Errorf("listing part %s: %w", id, err)
		}
		part := remotePart{ID: id}
		for _, name := range names {
			if ts, ok := t.Timestamp(name); ok {
				// Fixed-width timestamps: string order is chronological.
				part.MT = max(part.MT, annotations.FormatTimestamp(ts))
			}
		}
		parts = append(parts, part)
	}
	return parts, nil
}

// partsEnv describes the stored parts to the parts command:
// PART_COUNT, and PART<X>_ID / PART<X>_MT for X in [0, PART_COUNT).
func partsEnv(parts []remotePart) []corev1.EnvVar {
	env := []corev1.EnvVar{{Name: "PART_COUNT", Value: strconv.Itoa(len(parts))}}
	for i, p := range parts {
		prefix := "PART" + strconv.Itoa(i)
		env = append(env,
			corev1.EnvVar{Name: prefix + "_ID", Value: p.ID},
			corev1.EnvVar{Name: prefix + "_MT", Value: p.MT},
		)
	}
	return env
}

// partsAttempt runs one partitioned backup:
//
//  1. reads the stored parts and their newest backup timestamps;
//  2. creates the helper pod with them in PART<X>_ID / PART<X>_MT;
//  3. runs t.PartsCommand there, which prints "<part>=<YYYYMMDDHHMMSS>" for
//     every new or changed part and "<part>=SKIP" for every unchanged one
//     (a stored part it does not print no longer exists);
//  4. for every changed part runs t.Command, with {PART_ID} replaced, in the
//     same helper pod and uploads its output to
//     <file-folder>/<part>/backup-<that timestamp><suffix>.
//
// Steps 1-3 share one BackupTimeout and each part backup gets its own. A
// failed part does not stop the others, but fails the attempt. A retry
// starts from step 1, so parts that were already uploaded are reported to
// the parts command with their new timestamp.
func (r *Runner) partsAttempt(ctx context.Context, t annotations.Target, log *slog.Logger) (Result, error) {
	listCtx, cancel := context.WithTimeout(ctx, r.BackupTimeout)
	defer cancel()

	stored, err := r.remoteParts(listCtx, t, log)
	if err != nil {
		return Result{}, err
	}
	helper, cleanup, err := r.startHelper(listCtx, t, partsEnv(stored), log)
	if err != nil {
		return Result{}, err
	}
	defer cleanup()

	parts, err := r.listParts(listCtx, t, helper, log)
	if err != nil {
		return Result{}, err
	}
	cancel()

	listed := map[string]bool{}
	var changed []annotations.Part
	for _, p := range parts {
		listed[p.ID] = true
		if !p.Skip {
			changed = append(changed, p)
		}
	}
	gone := 0
	for _, p := range stored {
		if !listed[p.ID] {
			gone++
			log.Info("part no longer exists, not backed up", "part", p.ID, "last_backup", p.MT)
		}
	}
	log.Info("parts listed", "changed", len(changed), "unchanged", len(parts)-len(changed), "gone", gone)
	if r.Metrics != nil {
		r.Metrics.Parts.WithLabelValues(t.Namespace, t.FileFolder, metrics.PartChanged).Set(float64(len(changed)))
		r.Metrics.Parts.WithLabelValues(t.Namespace, t.FileFolder, metrics.PartUnchanged).Set(float64(len(parts) - len(changed)))
		r.Metrics.Parts.WithLabelValues(t.Namespace, t.FileFolder, metrics.PartGone).Set(float64(gone))
	}

	var total Result
	var errs []error
	for _, p := range changed {
		if err := ctx.Err(); err != nil {
			errs = append(errs, err)
			break
		}
		res, err := r.backupPart(ctx, t, p, helper, log)
		if err != nil {
			log.Error("part backup failed", "part", p.ID, "error", err)
			errs = append(errs, fmt.Errorf("part %s: %w", p.ID, err))
			if r.Metrics != nil {
				r.Metrics.PartBackups.WithLabelValues(t.Namespace, t.FileFolder, metrics.ResultFailure).Inc()
			}
			continue
		}
		if r.Metrics != nil {
			r.Metrics.PartBackups.WithLabelValues(t.Namespace, t.FileFolder, metrics.ResultSuccess).Inc()
		}
		log.Info("part backup complete", "part", p.ID, "file", res.FileName, "size_bytes", res.SizeBytes,
			"location", res.Location)
		total.SizeBytes += res.SizeBytes
		total.Files++
	}
	return total, errors.Join(errs...)
}

// listParts runs t.PartsCommand in the helper pod and parses its stdout.
func (r *Runner) listParts(ctx context.Context, t annotations.Target, helper string, log *slog.Logger) ([]annotations.Part, error) {
	log.Info("running parts command", "pod", helper, "command", t.PartsCommand)
	stdout := &cappedBuffer{limit: maxPartsOutput}
	stderr := &lineLogger{log: log, pod: helper}

	err := r.Exec.Exec(ctx, t.Namespace, helper, kube.HelperContainer, []string{"sh", "-c", t.PartsCommand}, stdout, stderr)
	stderr.Flush()
	if err != nil {
		var exitErr *kube.ExitError
		if errors.As(err, &exitErr) {
			return nil, fmt.Errorf("parts command exited with code %d", exitErr.Code)
		}
		return nil, fmt.Errorf("exec parts command: %w", err)
	}
	parts, err := annotations.ParsePartList(stdout.String())
	if err != nil {
		return nil, fmt.Errorf("parts command output: %w", err)
	}
	return parts, nil
}

// backupPart backs up one changed part, bounded by BackupTimeout.
func (r *Runner) backupPart(ctx context.Context, t annotations.Target, p annotations.Part, helper string, log *slog.Logger) (Result, error) {
	ctx, cancel := context.WithTimeout(ctx, r.BackupTimeout)
	defer cancel()
	return r.backupTo(ctx, t, t.PartFolder(p.ID), t.FileName(p.Timestamp), t.PartCommand(p.ID),
		helper, kube.HelperContainer, log.With("part", p.ID))
}

// pruneParts applies t's retention to every part folder on the storage and
// removes the folders that end up empty. Folders that are not valid part ids
// and files directly in <file-folder> are left alone. An error in one part
// does not stop the others.
func (r *Runner) pruneParts(ctx context.Context, t annotations.Target, log *slog.Logger) error {
	ids, err := r.Storage.ListDirs(ctx, t.FileFolder)
	if err != nil {
		return fmt.Errorf("listing part folders: %w", err)
	}
	sort.Strings(ids)

	var errs []error
	stored := 0
	for _, id := range ids {
		if !annotations.ValidPartID(id) {
			continue
		}
		stored++
		dir := t.PartFolder(id)
		res, err := r.pruneDir(ctx, t, dir)
		if err != nil {
			errs = append(errs, fmt.Errorf("part %s: %w", id, err))
			continue
		}
		if len(res.Kept) > 0 {
			continue
		}
		if err := r.Storage.RemoveDir(ctx, dir); err != nil {
			errs = append(errs, fmt.Errorf("part %s: removing empty folder: %w", id, err))
			continue
		}
		stored--
		log.Info("removed empty part folder", "part", id)
		if r.Metrics != nil {
			r.Metrics.PartFoldersPruned.WithLabelValues(t.Namespace, t.FileFolder).Inc()
		}
	}
	if r.Metrics != nil {
		r.Metrics.PartsStored.WithLabelValues(t.Namespace, t.FileFolder).Set(float64(stored))
	}
	return errors.Join(errs...)
}

// cappedBuffer is a bytes.Buffer that refuses to grow beyond limit.
type cappedBuffer struct {
	bytes.Buffer
	limit int
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	if b.Len()+len(p) > b.limit {
		return 0, fmt.Errorf("output is larger than %d bytes", b.limit)
	}
	return b.Buffer.Write(p)
}
