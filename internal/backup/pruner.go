package backup

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"oap-backup/internal/annotations"
)

// PruneResult lists what Prune deleted and kept.
type PruneResult struct {
	Deleted []string
	Kept    []string
}

// Prune applies retention to <file-folder> (port of backup_pruner.py):
//   - deletes every file that does not end with the target's backup suffix
//     (stale or foreign files);
//   - deletes the oldest backups until at most RetentionKeepLast remain. The
//     fixed-width UTC timestamp in the file names makes lexicographic order
//     chronological.
//
// A missing folder is not an error. Deletion errors do not stop pruning of
// the remaining files; they are returned joined.
func (r *Runner) Prune(ctx context.Context, t annotations.Target) (PruneResult, error) {
	log := r.Logger.With("target", t.Key(), "folder", t.FileFolder)

	names, err := r.Storage.List(ctx, t.FileFolder)
	if err != nil {
		return PruneResult{}, err
	}

	suffix := t.Suffix()
	var res PruneResult
	var errs []error
	var matching []string

	del := func(name, reason string) {
		if err := r.Storage.Delete(ctx, t.FileFolder, name); err != nil {
			errs = append(errs, fmt.Errorf("delete %s: %w", name, err))
			return
		}
		log.Info("pruned file", "file", name, "reason", reason)
		res.Deleted = append(res.Deleted, name)
	}

	for _, name := range names {
		if strings.HasSuffix(name, suffix) {
			matching = append(matching, name)
		} else {
			del(name, "not a backup file (suffix "+suffix+")")
		}
	}

	sort.Strings(matching)
	for len(matching) > t.RetentionKeepLast {
		oldest := matching[0]
		matching = matching[1:]
		del(oldest, fmt.Sprintf("retention_keep_last=%d", t.RetentionKeepLast))
	}
	res.Kept = matching

	if r.Metrics != nil && len(res.Deleted) > 0 {
		r.Metrics.PrunedFiles.WithLabelValues(t.Namespace, t.FileFolder).Add(float64(len(res.Deleted)))
	}
	log.Info("prune complete", "deleted", len(res.Deleted), "kept", len(res.Kept))
	return res, errors.Join(errs...)
}
