package runlog

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestRecorder(t *testing.T) {
	var service bytes.Buffer
	rec := New(0)
	logger := slog.New(rec.Handler(slog.NewTextHandler(&service, &slog.HandlerOptions{Level: slog.LevelInfo})))

	logger.Info("starting", "port", 9090)                                  // no target: not recorded
	logger.Info("backup triggered", "target", "db/a", "trigger", "manual") // target on the record
	run := logger.With("target", "db/a", "folder", "pg")
	run.Info("running backup command", "command", "pg_dump -t x", "file", "backup-1.sql.gz")
	run.Debug("hidden") // below the service's level
	run.With("part", "p1").Error("backup failed", "error", errors.New("exit code 2"), "took", 1500*time.Millisecond, "empty", "")
	logger.With("target", "db/b").Warn("other target")

	lines, next := rec.Lines("db/a", 0)
	if len(lines) != 3 || next != 3 || rec.Cursor("db/a") != 3 {
		t.Fatalf("lines = %+v, next = %d", lines, next)
	}
	want := []Line{
		{Seq: 1, Level: "INFO", Text: "backup triggered trigger=manual"},
		{Seq: 2, Level: "INFO", Text: `running backup command folder=pg command="pg_dump -t x" file=backup-1.sql.gz`},
		{Seq: 3, Level: "ERROR", Text: `backup failed folder=pg part=p1 error="exit code 2" took=1.5s empty=""`},
	}
	for i, w := range want {
		got := lines[i]
		if got.Seq != w.Seq || got.Level != w.Level || got.Text != w.Text || got.Time.IsZero() {
			t.Errorf("line %d = %+v, want %+v", i, got, w)
		}
	}

	// Only the lines after a cursor.
	if lines, next := rec.Lines("db/a", 2); len(lines) != 1 || lines[0].Seq != 3 || next != 3 {
		t.Errorf("Lines(after 2) = %+v, %d", lines, next)
	}
	if lines, next := rec.Lines("db/a", 3); len(lines) != 0 || lines == nil || next != 3 {
		t.Errorf("Lines(after 3) = %#v, %d", lines, next)
	}
	if lines, next := rec.Lines("db/none", 0); len(lines) != 0 || lines == nil || next != 0 || rec.Cursor("db/none") != 0 {
		t.Errorf("Lines(unknown) = %#v, %d", lines, next)
	}
	if lines, _ := rec.Lines("db/b", 0); len(lines) != 1 || lines[0].Text != "other target" || lines[0].Level != "WARN" {
		t.Errorf("db/b = %+v", lines)
	}

	// The service's own log is unchanged: every enabled record, with target.
	out := service.String()
	for _, w := range []string{"msg=starting", "target=db/a", `command="pg_dump -t x"`, "part=p1", "target=db/b"} {
		if !strings.Contains(out, w) {
			t.Errorf("service log does not contain %q:\n%s", w, out)
		}
	}
	if strings.Contains(out, "hidden") || strings.Count(out, "\n") != 5 {
		t.Errorf("service log:\n%s", out)
	}
}

func TestRecorderBoundsTargets(t *testing.T) {
	rec := New(0)
	logger := slog.New(rec.Handler(slog.NewTextHandler(io.Discard, nil)))
	for i := range maxTargets + 5 {
		logger.Info("line", "target", fmt.Sprintf("ns/pod-%d", i))
	}
	if len(rec.logs) != maxTargets {
		t.Errorf("targets = %d, want %d", len(rec.logs), maxTargets)
	}
	// The newest targets are kept.
	if rec.Cursor(fmt.Sprintf("ns/pod-%d", maxTargets+4)) != 1 {
		t.Error("newest target was dropped")
	}
}

func TestRecorderKeepsNewestLines(t *testing.T) {
	rec := New(3)
	logger := slog.New(rec.Handler(slog.NewTextHandler(io.Discard, nil))).With("target", "db/a")
	for i := range 10 {
		logger.Info("line", "n", i)
	}
	lines, next := rec.Lines("db/a", 0)
	if len(lines) != 3 || next != 10 || lines[0].Seq != 8 || lines[2].Text != "line n=9" {
		t.Errorf("lines = %+v, next = %d", lines, next)
	}
	// A cursor that points at dropped lines still returns what is left.
	if lines, _ := rec.Lines("db/a", 2); len(lines) != 3 {
		t.Errorf("Lines(after 2) = %+v", lines)
	}
}
