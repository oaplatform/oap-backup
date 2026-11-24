package cli

import (
	"fmt"
	"io"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"oap-backup/internal/annotations"
	"oap-backup/internal/api"
	"oap-backup/internal/backup"
	"oap-backup/internal/scheduler"
)

const timeLayout = "2006-01-02 15:04:05 MST"

func renderList(w io.Writer, list api.TargetList, now time.Time) {
	if len(list.Targets) == 0 && len(list.Invalid) == 0 {
		fmt.Fprintln(w, "No pods with the oap/backup-command annotation were found.")
		return
	}
	if len(list.Targets) > 0 {
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "TARGET\tSTATUS\tMODE\tSCHEDULE\tNEXT RUN\tLAST RUN\tFOLDER")
		for _, t := range list.Targets {
			fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
				t.Target.Key(), status(t), mode(t.Target), t.Target.Schedule,
				"in "+shortDuration(t.NextRun.Sub(now)), lastRunShort(t.LastRun, now), t.Target.FileFolder)
		}
		_ = tw.Flush()
	}
	if len(list.Invalid) > 0 {
		if len(list.Targets) > 0 {
			fmt.Fprintln(w)
		}
		tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
		fmt.Fprintln(tw, "INVALID (skipped)\tERROR")
		for _, t := range list.Invalid {
			fmt.Fprintf(tw, "%s\t%s\n", t.Key, t.Error)
		}
		_ = tw.Flush()
	}
}

func renderDetail(w io.Writer, d api.TargetDetail, now time.Time) {
	t := d.Target
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	row := func(name, value string) { fmt.Fprintf(tw, "%s:\t%s\n", name, value) }

	row("Target", t.Key())
	row("Status", status(d.TargetStatus))
	row("Mode", mode(t))
	row("Container", t.Container)
	if t.ExternalImage != "" {
		row("Helper image", t.ExternalImage)
	}
	if t.Parts {
		row("Parts command", oneLine(t.PartsCommand))
	}
	row("Command", oneLine(t.Command))
	row("Schedule", t.Schedule)
	row("Next run", fmt.Sprintf("%s (in %s)", d.NextRun.Format(timeLayout), shortDuration(d.NextRun.Sub(now))))
	row("Folder", t.FileFolder)
	row("Extension", t.FileExtension)
	row("Compression", compression(t))
	row("Keep last", fmt.Sprintf("%d", t.RetentionKeepLast))
	row("Last run", lastRunLong(d.LastRun, now))
	if d.LastRun != nil && d.LastRun.Error != "" {
		row("Last error", oneLine(d.LastRun.Error))
	}
	row("Storage", d.Storage.Location)
	_ = tw.Flush()

	fmt.Fprintln(w)
	switch {
	case d.Storage.Error != "":
		fmt.Fprintf(w, "Storage could not be read: %s\n", d.Storage.Error)
	case t.Parts:
		renderParts(w, d.Storage.Folders)
	case len(d.Storage.Folders) > 0:
		renderFolder(w, d.Storage.Folders[0])
	}
}

func renderParts(w io.Writer, folders []backup.StoredFolder) {
	if len(folders) == 0 {
		fmt.Fprintln(w, "No parts on the storage yet.")
		return
	}
	var files int
	var size int64
	for _, f := range folders {
		n, s, _ := summarize(f)
		files += n
		size += s
	}
	fmt.Fprintf(w, "Parts on the storage: %d (%s, %s)\n", len(folders), plural(files, "backup"), humanSize(size))
	for _, f := range folders {
		fmt.Fprintln(w)
		fmt.Fprintf(w, "Part %s\n", f.Part)
		renderFolder(w, f)
	}
}

// renderFolder prints the files of one folder, newest first.
func renderFolder(w io.Writer, f backup.StoredFolder) {
	n, size, newest := summarize(f)
	if len(f.Files) == 0 {
		fmt.Fprintf(w, "%s: no files\n", f.Folder)
		return
	}
	line := fmt.Sprintf("%s: %s, %s", f.Folder, plural(n, "backup"), humanSize(size))
	if newest != nil {
		line += ", newest " + newest.UTC().Format(timeLayout)
	}
	fmt.Fprintln(w, line)

	tw := tabwriter.NewWriter(w, 2, 0, 2, ' ', 0)
	fmt.Fprintln(tw, "  FILE\tBACKUP TIME\tSIZE\tNOTE")
	for _, file := range f.Files {
		ts, note := "-", ""
		if file.Timestamp != nil {
			ts = file.Timestamp.UTC().Format(timeLayout)
		}
		if !file.Backup {
			note = "not a backup file: the next run deletes it"
		}
		fmt.Fprintf(tw, "  %s\t%s\t%s\t%s\n", file.Name, ts, humanSize(file.SizeBytes), note)
	}
	_ = tw.Flush()
}

// summarize counts the backup files of f, their total size and the newest
// backup time.
func summarize(f backup.StoredFolder) (n int, size int64, newest *time.Time) {
	for _, file := range f.Files {
		if !file.Backup {
			continue
		}
		n++
		size += file.SizeBytes
		if file.Timestamp != nil && (newest == nil || file.Timestamp.After(*newest)) {
			newest = file.Timestamp
		}
	}
	return n, size, newest
}

func status(t scheduler.TargetStatus) string {
	if t.Running {
		return "running"
	}
	return "scheduled"
}

func mode(t annotations.Target) string {
	if t.Parts {
		return "parts"
	}
	return "single"
}

func compression(t annotations.Target) string {
	codec := t.Codec()
	if codec == "" {
		return "none (the command output is already compressed)"
	}
	if t.CompressionLevel == 0 {
		return codec + " (default level)"
	}
	return fmt.Sprintf("%s (level %d)", codec, t.CompressionLevel)
}

func lastRunShort(l *scheduler.LastRun, now time.Time) string {
	if l == nil {
		return "-"
	}
	result := "ok"
	if l.Error != "" {
		result = "failed"
	}
	return fmt.Sprintf("%s %s ago", result, shortDuration(now.Sub(l.Finished)))
}

func lastRunLong(l *scheduler.LastRun, now time.Time) string {
	if l == nil {
		return "none since oap-backup started"
	}
	result := "ok"
	if l.Error != "" {
		result = "failed"
	}
	return fmt.Sprintf("%s, %s ago (%s, started %s, took %s, uploaded %s)",
		result, shortDuration(now.Sub(l.Finished)), l.Trigger, l.Started.Format(timeLayout), runDuration(l), uploaded(l))
}

func runDuration(l *scheduler.LastRun) string {
	return shortDuration(l.Finished.Sub(l.Started))
}

func uploaded(l *scheduler.LastRun) string {
	if l.Files == 0 {
		return "nothing"
	}
	return fmt.Sprintf("%s, %s", plural(l.Files, "file"), humanSize(l.SizeBytes))
}

// shortDuration renders d with at most two units: 42s, 5m10s, 3h12m, 2d5h.
func shortDuration(d time.Duration) string {
	if d < 0 {
		d = 0
	}
	d = d.Round(time.Second)
	days := int(d / (24 * time.Hour))
	hours := int(d % (24 * time.Hour) / time.Hour)
	minutes := int(d % time.Hour / time.Minute)
	seconds := int(d % time.Minute / time.Second)
	switch {
	case days > 0:
		return fmt.Sprintf("%dd%dh", days, hours)
	case hours > 0:
		return fmt.Sprintf("%dh%dm", hours, minutes)
	case minutes > 0:
		return fmt.Sprintf("%dm%ds", minutes, seconds)
	default:
		return fmt.Sprintf("%ds", seconds)
	}
}

func humanSize(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	value, suffix := float64(n), ""
	for _, s := range []string{"KiB", "MiB", "GiB", "TiB"} {
		value /= unit
		suffix = s
		if value < unit {
			break
		}
	}
	return fmt.Sprintf("%.1f %s", value, suffix)
}

func plural(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, noun)
}

// oneLine keeps a multi-line command or error on one table row.
func oneLine(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func sortedKeys(m map[string]bool) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
