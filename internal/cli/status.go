package cli

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"text/tabwriter"
	"time"

	"oap-backup/internal/api"
	"oap-backup/internal/runlog"
	"oap-backup/internal/scheduler"
)

// StatusReport is the output of "oap-backup status -o json".
type StatusReport struct {
	// Scheduled and Invalid count the detected pods.
	Scheduled int `json:"scheduled"`
	Invalid   int `json:"invalid"`
	// InProgress are the backups that are running or queued right now.
	InProgress []InProgress `json:"in_progress"`
	// Failed are the targets whose last finished run failed.
	Failed []scheduler.TargetStatus `json:"failed"`
	// Next is the target with the earliest scheduled run.
	Next *scheduler.TargetStatus `json:"next,omitempty"`
}

// InProgress is one backup in progress with the log of its current run.
type InProgress struct {
	scheduler.TargetStatus
	// Log holds the lines the service recorded since the run was triggered
	// (the newest ones, if the run logged more than the service keeps).
	Log []runlog.Line `json:"log"`
}

func (a *App) status(args []string) error {
	var o options
	var lines int
	fs := a.flagSet("status", &o, true)
	fs.IntVar(&lines, "lines", 15, "")
	positional, err := parse(fs, args)
	if err != nil {
		return err
	}
	if len(positional) > 0 {
		return usageError{"status takes no arguments"}
	}
	if err := checkOutput(o); err != nil {
		return err
	}
	if lines < 0 {
		return usageError{"--lines must not be negative"}
	}

	var list api.TargetList
	if err := a.call(o.server, http.MethodGet, "/targets", &list); err != nil {
		return err
	}
	report := StatusReport{
		Scheduled:  len(list.Targets),
		Invalid:    len(list.Invalid),
		InProgress: []InProgress{},
		Failed:     []scheduler.TargetStatus{},
	}
	for i, t := range list.Targets {
		if report.Next == nil || t.NextRun.Before(report.Next.NextRun) {
			report.Next = &list.Targets[i]
		}
		if t.LastRun != nil && t.LastRun.Error != "" {
			report.Failed = append(report.Failed, t)
		}
		if t.Running {
			report.InProgress = append(report.InProgress, InProgress{TargetStatus: t, Log: a.runLog(o.server, t)})
		}
	}

	if o.output == "json" {
		return writeJSON(a.Stdout, report)
	}
	renderStatus(a.Stdout, report, lines, a.Now())
	return nil
}

// runLog returns the log lines of t's current run. A log that cannot be
// read is empty: the status is still worth showing.
func (a *App) runLog(server string, t scheduler.TargetStatus) []runlog.Line {
	out := []runlog.Line{}
	path := fmt.Sprintf("/targets/%s/%s/log", url.PathEscape(t.Target.Namespace), url.PathEscape(t.Target.Pod))
	var resp api.LogResponse
	if err := a.call(server, http.MethodGet, path, &resp); err != nil || t.CurrentRun == nil {
		return out
	}
	for _, line := range resp.Lines {
		if !line.Time.Before(t.CurrentRun.Triggered) {
			out = append(out, line)
		}
	}
	return out
}

func renderStatus(w io.Writer, r StatusReport, lines int, now time.Time) {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintf(tw, "Backups:\t%d scheduled, %d invalid, %d in progress\n", r.Scheduled, r.Invalid, len(r.InProgress))
	if r.Next != nil {
		fmt.Fprintf(tw, "Next run:\t%s in %s (schedule %s)\n",
			r.Next.Target.Key(), shortDuration(r.Next.NextRun.Sub(now)), r.Next.Target.Schedule)
	}
	for i, t := range r.Failed {
		label := ""
		if i == 0 {
			label = "Last run failed:"
		}
		fmt.Fprintf(tw, "%s\t%s, %s ago: %s\n", label, t.Target.Key(), shortDuration(now.Sub(t.LastRun.Finished)), oneLine(t.LastRun.Error))
	}
	_ = tw.Flush()

	if len(r.InProgress) == 0 {
		fmt.Fprintln(w, "\nNo backup in progress.")
		return
	}
	for _, p := range r.InProgress {
		fmt.Fprintf(w, "\nBackup in progress: %s\n", p.Target.Key())
		renderInProgress(w, p, lines, now)
	}
}

func renderInProgress(w io.Writer, p InProgress, lines int, now time.Time) {
	t := p.Target
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	row := func(name, value string) { fmt.Fprintf(tw, "  %s:\t%s\n", name, value) }

	if cur := p.CurrentRun; cur != nil {
		if cur.Started != nil {
			row("State", "running for "+shortDuration(now.Sub(*cur.Started)))
		} else {
			row("State", fmt.Sprintf("queued for %s, waiting for a free slot (--max_concurrent of the service)",
				shortDuration(now.Sub(cur.Triggered))))
		}
		row("Trigger", fmt.Sprintf("%s, at %s", cur.Trigger, cur.Triggered.Format(timeLayout)))
	} else {
		row("State", "running")
	}
	row("Mode", mode(t))
	row("Container", t.Container)
	if t.ExternalImage != "" {
		row("Helper image", t.ExternalImage)
	}
	if t.Parts {
		row("Parts command", oneLine(t.PartsCommand))
	}
	row("Command", oneLine(t.Command))
	row("Folder", t.FileFolder)
	row("Compression", compression(t))
	row("Previous run", lastRunLong(p.LastRun, now))
	_ = tw.Flush()

	switch {
	case lines == 0:
	case len(p.Log) == 0:
		fmt.Fprintln(w, "  Log: no lines yet")
	default:
		shown := p.Log
		if len(shown) > lines {
			shown = shown[len(shown)-lines:]
			fmt.Fprintf(w, "  Log (last %d of %s):\n", len(shown), plural(len(p.Log), "line"))
		} else {
			fmt.Fprintln(w, "  Log:")
		}
		for _, line := range shown {
			fmt.Fprintf(w, "    %s\n", formatLogLine(line))
		}
	}
}

// formatLogLine renders one recorded log line.
func formatLogLine(line runlog.Line) string {
	return strings.TrimRight(fmt.Sprintf("%s %-5s %s", line.Time.Local().Format("15:04:05"), line.Level, line.Text), " ")
}
