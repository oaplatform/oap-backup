package cli

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"oap-backup/internal/annotations"
	"oap-backup/internal/runlog"
	"oap-backup/internal/scheduler"
)

func TestRenderStatusQueuedAndRunning(t *testing.T) {
	now := time.Date(2026, 11, 24, 12, 0, 0, 0, time.UTC)
	started := now.Add(-40 * time.Second)
	target := func(pod string) annotations.Target {
		return annotations.Target{Namespace: "db", Pod: pod, Container: "main", Command: "dump", FileFolder: "db/" + pod,
			FileExtension: ".sql", Schedule: "@daily"}
	}
	var lines []runlog.Line
	for i := range 4 {
		lines = append(lines, runlog.Line{Seq: int64(i + 1), Time: started, Level: "INFO", Text: "line " + string(rune('a'+i))})
	}
	report := StatusReport{
		Scheduled: 2,
		InProgress: []InProgress{
			{TargetStatus: scheduler.TargetStatus{Target: target("a"), Running: true,
				CurrentRun: &scheduler.CurrentRun{Trigger: "schedule", Triggered: now.Add(-45 * time.Second), Started: &started}}, Log: lines},
			{TargetStatus: scheduler.TargetStatus{Target: target("b"), Running: true,
				CurrentRun: &scheduler.CurrentRun{Trigger: "manual", Triggered: now.Add(-5 * time.Second)}}},
		},
	}
	var buf bytes.Buffer
	renderStatus(&buf, report, 3, now)
	out := buf.String()
	for _, want := range []string{
		"2 scheduled, 0 invalid, 2 in progress",
		"Backup in progress: db/a", "running for 40s", "schedule, at 2026-11-24 11:59:15 UTC",
		"Log (last 3 of 4 lines):", "INFO  line b", "INFO  line d",
		"Backup in progress: db/b", "queued for 5s, waiting for a free slot", "Log: no lines yet",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("status does not contain %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "line a") {
		t.Errorf("only the last 3 lines are expected:\n%s", out)
	}
}

func TestShortDuration(t *testing.T) {
	cases := map[time.Duration]string{
		-time.Minute:                          "0s",
		400 * time.Millisecond:                "0s",
		42 * time.Second:                      "42s",
		5*time.Minute + 10*time.Second:        "5m10s",
		3*time.Hour + 12*time.Minute:          "3h12m",
		53*time.Hour + 59*time.Minute:         "2d5h",
		time.Hour:                             "1h0m",
		24*time.Hour - 500*time.Millisecond:   "1d0h",
		59*time.Second + 600*time.Millisecond: "1m0s",
	}
	for d, want := range cases {
		if got := shortDuration(d); got != want {
			t.Errorf("shortDuration(%s) = %q, want %q", d, got, want)
		}
	}
}

func TestHumanSize(t *testing.T) {
	cases := map[int64]string{
		0:              "0 B",
		1023:           "1023 B",
		1024:           "1.0 KiB",
		1536:           "1.5 KiB",
		3 << 20:        "3.0 MiB",
		5 << 30:        "5.0 GiB",
		2 << 40:        "2.0 TiB",
		3000 * 1 << 40: "3000.0 TiB",
	}
	for n, want := range cases {
		if got := humanSize(n); got != want {
			t.Errorf("humanSize(%d) = %q, want %q", n, got, want)
		}
	}
}

func TestCompression(t *testing.T) {
	cases := map[string]annotations.Target{
		"none": {FileExtension: ".sql"},
		"none (the command output is already compressed)": {FileExtension: ".tar.gz", Compression: annotations.Zstd},
		"gzip (default level)":                            {FileExtension: ".sql", Compression: annotations.Gzip},
		"zstd (level 19)":                                 {FileExtension: ".sql", Compression: annotations.Zstd, CompressionLevel: 19},
	}
	for want, target := range cases {
		if got := compression(target); got != want {
			t.Errorf("compression(%+v) = %q, want %q", target, got, want)
		}
	}
}

func TestPluralAndOneLine(t *testing.T) {
	if plural(1, "file") != "1 file" || plural(0, "file") != "0 files" || plural(2, "backup") != "2 backups" {
		t.Error("plural")
	}
	if got := oneLine("cd /srv\n  for d in */; do\n\techo $d\ndone\n"); got != "cd /srv for d in */; do echo $d done" {
		t.Errorf("oneLine = %q", got)
	}
}
