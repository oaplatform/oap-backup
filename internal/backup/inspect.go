package backup

import (
	"context"
	"fmt"
	"sort"
	"time"

	"oap-backup/internal/annotations"
)

// StoredFile is one file in a backup folder on the storage.
type StoredFile struct {
	Name      string    `json:"name"`
	SizeBytes int64     `json:"size_bytes"`
	Modified  time.Time `json:"modified,omitzero"`
	// Backup is false for a file that is not a backup of the target; the
	// next retention pruning deletes it.
	Backup bool `json:"backup"`
	// Timestamp is the time in the backup's file name; nil if it has none.
	Timestamp *time.Time `json:"timestamp,omitempty"`
}

// StoredFolder is one backup folder: the target's file folder, or one part
// folder of a partitioned target.
type StoredFolder struct {
	// Part is the part id; empty for a target that is not partitioned.
	Part string `json:"part,omitempty"`
	// Folder is the path below the storage root.
	Folder string `json:"folder"`
	// Files are sorted by name, newest backup first.
	Files []StoredFile `json:"files"`
}

// Inspect reads what the storage holds for t: the files of its folder, or
// for a partitioned target the files of every part folder (sorted by part
// id; folders that are not valid part ids are not parts and are left out).
// It changes nothing.
func (r *Runner) Inspect(ctx context.Context, t annotations.Target) ([]StoredFolder, error) {
	if !t.Parts {
		folder, err := r.inspectFolder(ctx, t, "", t.FileFolder)
		if err != nil {
			return nil, err
		}
		return []StoredFolder{folder}, nil
	}

	ids, err := r.Storage.ListDirs(ctx, t.FileFolder)
	if err != nil {
		return nil, fmt.Errorf("listing part folders: %w", err)
	}
	sort.Strings(ids)
	folders := []StoredFolder{}
	for _, id := range ids {
		if !annotations.ValidPartID(id) {
			continue
		}
		folder, err := r.inspectFolder(ctx, t, id, t.PartFolder(id))
		if err != nil {
			return nil, fmt.Errorf("part %s: %w", id, err)
		}
		folders = append(folders, folder)
	}
	return folders, nil
}

func (r *Runner) inspectFolder(ctx context.Context, t annotations.Target, part, dir string) (StoredFolder, error) {
	files, err := r.Storage.ListFiles(ctx, dir)
	if err != nil {
		return StoredFolder{}, err
	}
	out := StoredFolder{Part: part, Folder: dir, Files: make([]StoredFile, 0, len(files))}
	for _, f := range files {
		sf := StoredFile{Name: f.Name, SizeBytes: f.Size, Modified: f.ModTime, Backup: t.IsBackupFile(f.Name)}
		if ts, ok := t.Timestamp(f.Name); ok {
			sf.Timestamp = &ts
		}
		out.Files = append(out.Files, sf)
	}
	// Fixed-width timestamps: reverse name order is newest first.
	sort.Slice(out.Files, func(i, j int) bool { return out.Files[i].Name > out.Files[j].Name })
	return out, nil
}
