package cli_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"oap-backup/internal/annotations"
	"oap-backup/internal/api"
	"oap-backup/internal/backup"
	"oap-backup/internal/cli"
	"oap-backup/internal/runlog"
	"oap-backup/internal/scheduler"
)

// fakeJob is the backup runner behind the service under test. Like the real
// one, it logs with the target attribute.
type fakeJob struct {
	mu     sync.Mutex
	block  chan struct{}
	err    error
	logger *slog.Logger
}

func (f *fakeJob) Run(ctx context.Context, t annotations.Target) (scheduler.RunResult, error) {
	f.mu.Lock()
	block, err := f.block, f.err
	f.mu.Unlock()
	log := f.logger.With("target", t.Key(), "folder", t.FileFolder)
	log.Info("running backup command", "pod", t.Pod, "command", t.Command)
	defer log.Info("prune complete", "deleted", 0, "kept", 1)
	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
		}
	}
	if err != nil {
		return scheduler.RunResult{}, err
	}
	return scheduler.RunResult{Files: 1, SizeBytes: 2048}, nil
}

type fakeInspector struct {
	folders map[string][]backup.StoredFolder
	err     error
}

func (f *fakeInspector) Inspect(_ context.Context, t annotations.Target) ([]backup.StoredFolder, error) {
	return f.folders[t.Key()], f.err
}

func pod(name string, extra map[string]string) *corev1.Pod {
	ann := map[string]string{
		annotations.Command:           "pg_dumpall",
		annotations.FileFolder:        "db/" + name,
		annotations.FileExtension:     ".sql",
		annotations.RetentionKeepLast: "3",
		annotations.Schedule:          "0 */6 * * *",
		annotations.Compression:       "gzip",
	}
	for k, v := range extra {
		if v == "" {
			delete(ann, k)
		} else {
			ann[k] = v
		}
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "db", Name: name, Annotations: ann},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "main"}}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

type env struct {
	client    *fake.Clientset
	server    *httptest.Server
	job       *fakeJob
	inspector *fakeInspector
}

// newEnv starts the real HTTP API on top of a real scheduler with three
// pods: db/pg-0 (single), db/parts-0 (partitioned) and db/broken (invalid).
func newEnv(t *testing.T) *env {
	t.Helper()
	ts := func(s string) *time.Time {
		v, err := annotations.ParseTimestamp(s)
		if err != nil {
			t.Fatal(err)
		}
		return &v
	}
	client := fake.NewClientset(
		pod("pg-0", nil),
		pod("parts-0", map[string]string{
			annotations.Parts:            "true",
			annotations.PartsCommand:     "list-parts",
			annotations.Command:          "pg_dump -t {PART_ID}",
			annotations.ExternalImage:    "postgres:17",
			annotations.Compression:      "zstd",
			annotations.CompressionLevel: "19",
		}),
		pod("broken", map[string]string{annotations.FileFolder: ""}),
	)
	logs := runlog.New(0)
	logger := slog.New(logs.Handler(slog.NewTextHandler(io.Discard, nil)))
	job := &fakeJob{logger: logger}
	mgr := &scheduler.Manager{
		Client: client, DefaultSchedule: "@daily", Refresh: time.Minute, MaxConcurrent: 2,
		ShutdownTimeout: time.Second, Runner: job, Logger: logger,
	}
	mgr.Reconcile(context.Background())

	inspector := &fakeInspector{folders: map[string][]backup.StoredFolder{
		"db/pg-0": {{Folder: "db/pg-0", Files: []backup.StoredFile{
			{Name: "notes.txt", SizeBytes: 10},
			{Name: "backup-20261124000000.sql.gz", SizeBytes: 3 << 20, Backup: true, Timestamp: ts("20261124000000")},
			{Name: "backup-20261123000000.sql.gz", SizeBytes: 1024, Backup: true, Timestamp: ts("20261123000000")},
		}}},
		"db/parts-0": {
			{Part: "p1", Folder: "db/parts-0/p1", Files: []backup.StoredFile{
				{Name: "backup-20261120101500.sql.zst", SizeBytes: 500, Backup: true, Timestamp: ts("20261120101500")},
			}},
			{Part: "p2", Folder: "db/parts-0/p2", Files: []backup.StoredFile{}},
		},
	}}
	srv := httptest.NewServer((&api.Server{
		Metrics:   http.NotFoundHandler(),
		Scheduler: mgr, Inspector: inspector, Logs: logs,
		StorageLocation: "s3://bucket/prefix (endpoint s3.local)",
		InspectTimeout:  time.Second,
		Logger:          logger,
	}).Handler())
	t.Cleanup(srv.Close)
	return &env{client: client, server: srv, job: job, inspector: inspector}
}

// waitIdle waits until no backup is running.
func (e *env) waitIdle(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, out, _ := e.run("list", "-o", "json")
		if !strings.Contains(out, `"running": true`) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("backups still running:\n%s", out)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// run executes the CLI against the test service and returns its exit code,
// stdout and stderr.
func (e *env) run(args ...string) (int, string, string) {
	var stdout, stderr bytes.Buffer
	app := cli.New(func(k string) string {
		if k == cli.ServerEnv {
			return e.server.URL
		}
		return ""
	}, &stdout, &stderr)
	app.PollInterval = 5 * time.Millisecond
	app.QuietInterval = 20 * time.Millisecond
	return app.Run(args), stdout.String(), stderr.String()
}

func wantContains(t *testing.T, out string, wants ...string) {
	t.Helper()
	for _, want := range wants {
		if !strings.Contains(out, want) {
			t.Errorf("output does not contain %q:\n%s", want, out)
		}
	}
}

func TestList(t *testing.T) {
	e := newEnv(t)
	code, out, stderr := e.run("list")
	if code != cli.ExitOK {
		t.Fatalf("exit %d, stderr: %s", code, stderr)
	}
	wantContains(t, out,
		"TARGET", "db/pg-0", "scheduled", "single", "0 */6 * * *", "db/parts-0", "parts",
		"INVALID (skipped)", "db/broken", annotations.FileFolder)
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "db/pg-0") && !strings.Contains(line, " in ") {
			t.Errorf("no next run in %q", line)
		}
	}

	code, out, _ = e.run("list", "-o", "json")
	var list api.TargetList
	if err := json.Unmarshal([]byte(out), &list); code != cli.ExitOK || err != nil {
		t.Fatalf("exit %d, json error %v:\n%s", code, err, out)
	}
	if len(list.Targets) != 2 || len(list.Invalid) != 1 || list.Targets[0].Target.Key() != "db/parts-0" || !list.Targets[0].Target.Parts {
		t.Errorf("list = %+v", list)
	}
}

func TestRefresh(t *testing.T) {
	e := newEnv(t)
	ctx := context.Background()
	// A new annotated pod, a fixed one and a deleted one; the service has
	// not rescanned yet.
	if _, err := e.client.CoreV1().Pods("db").Create(ctx, pod("mongo-0", nil), metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	if _, err := e.client.CoreV1().Pods("db").Update(ctx, pod("broken", nil), metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := e.client.CoreV1().Pods("db").Delete(ctx, "parts-0", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	_, out, _ := e.run("list")
	if strings.Contains(out, "db/mongo-0") || !strings.Contains(out, "db/parts-0") || !strings.Contains(out, "INVALID") {
		t.Fatalf("list before refresh:\n%s", out)
	}

	code, out, stderr := e.run("refresh")
	if code != cli.ExitOK {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
	wantContains(t, out, "db/mongo-0", "db/broken", "db/pg-0")
	if strings.Contains(out, "db/parts-0") || strings.Contains(out, "INVALID") {
		t.Errorf("refresh output still has the deleted or the fixed pod:\n%s", out)
	}
	// The rescan is persistent, not just a view.
	_, out, _ = e.run("list")
	wantContains(t, out, "db/mongo-0")

	code, out, _ = e.run("refresh", "-o", "json")
	var list api.TargetList
	if err := json.Unmarshal([]byte(out), &list); code != cli.ExitOK || err != nil || len(list.Targets) != 3 {
		t.Fatalf("exit %d, json error %v:\n%s", code, err, out)
	}

	// The Kubernetes API fails: refresh fails and says why.
	e.client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, errors.New("api down")
	})
	code, _, stderr = e.run("refresh")
	if code != cli.ExitFailed || !strings.Contains(stderr, "pods were not rescanned") || !strings.Contains(stderr, "api down") {
		t.Errorf("failed refresh: exit %d, stderr %q", code, stderr)
	}
}

func TestDescribe(t *testing.T) {
	e := newEnv(t)

	code, out, stderr := e.run("describe", "db/pg-0")
	if code != cli.ExitOK {
		t.Fatalf("exit %d, stderr: %s", code, stderr)
	}
	wantContains(t, out,
		"Target:", "db/pg-0", "single", "pg_dumpall", "gzip (default level)", "Keep last:", "none since oap-backup started",
		"s3://bucket/prefix",
		"db/pg-0: 2 backups, 3.0 MiB, newest 2026-11-24 00:00:00 UTC",
		"backup-20261123000000.sql.gz", "1.0 KiB",
		"notes.txt", "not a backup file")

	code, out, _ = e.run("describe", "db/parts-0")
	if code != cli.ExitOK {
		t.Fatalf("exit %d", code)
	}
	wantContains(t, out,
		"Parts command:", "list-parts", "pg_dump -t {PART_ID}", "postgres:17", "zstd (level 19)",
		"Parts on the storage: 2 (1 backup, 500 B)",
		"Part p1", "backup-20261120101500.sql.zst", "Part p2", "db/parts-0/p2: no files")

	code, out, _ = e.run("describe", "-o", "json", "db/pg-0")
	var detail api.TargetDetail
	if err := json.Unmarshal([]byte(out), &detail); code != cli.ExitOK || err != nil {
		t.Fatalf("exit %d, json error %v:\n%s", code, err, out)
	}
	if detail.Target.Pod != "pg-0" || len(detail.Storage.Folders) != 1 || len(detail.Storage.Folders[0].Files) != 3 {
		t.Errorf("detail = %+v", detail)
	}

	// A storage failure does not hide the rest.
	e.inspector.err = errors.New("ftp is down")
	code, out, _ = e.run("describe", "db/pg-0")
	if code != cli.ExitOK {
		t.Fatalf("exit %d", code)
	}
	wantContains(t, out, "Schedule:", "Storage could not be read: ftp is down")

	code, _, stderr = e.run("describe", "db/nope")
	if code != cli.ExitFailed || !strings.Contains(stderr, "unknown backup target") {
		t.Errorf("unknown target: exit %d, stderr %q", code, stderr)
	}
}

func TestRun(t *testing.T) {
	e := newEnv(t)

	code, out, stderr := e.run("run", "db/pg-0", "--wait")
	if code != cli.ExitOK {
		t.Fatalf("exit %d, stdout %q, stderr %q", code, out, stderr)
	}
	wantContains(t, out, "started backup of db/pg-0", "db/pg-0: ok in ", "1 file, 2.0 KiB uploaded")
	// The service's log of this run, from the trigger to the last line,
	// before the result.
	wantContains(t, out,
		"INFO  backup triggered trigger=manual",
		"INFO  running backup command folder=db/pg-0 pod=pg-0 command=pg_dumpall",
		"INFO  prune complete folder=db/pg-0 deleted=0 kept=1")
	if strings.Index(out, "prune complete") > strings.Index(out, "db/pg-0: ok in ") {
		t.Errorf("the log must come before the result:\n%s", out)
	}
	if strings.Contains(out, "[db/pg-0]") {
		t.Errorf("a single target needs no prefix:\n%s", out)
	}

	// A second run prints only its own log, not the first one again.
	_, out, _ = e.run("run", "db/pg-0", "--wait")
	if n := strings.Count(out, "running backup command"); n != 1 {
		t.Errorf("second run printed %d runs:\n%s", n, out)
	}

	// Without --wait there is no log, only a hint.
	_, out, _ = e.run("run", "db/parts-0")
	wantContains(t, out, "started backup of db/parts-0", "Add --wait")
	if strings.Contains(out, "running backup command") {
		t.Errorf("no log expected without --wait:\n%s", out)
	}
	e.waitIdle(t)

	// The finished run shows up in list and describe.
	_, out, _ = e.run("list")
	wantContains(t, out, "ok 0s ago")
	_, out, _ = e.run("describe", "db/pg-0")
	wantContains(t, out, "Last run:", "ok, ", "manual", "uploaded 1 file, 2.0 KiB")

	// A failing backup makes --wait exit 1 with the error.
	e.job.mu.Lock()
	e.job.err = errors.New("backup command exited with code 2")
	e.job.mu.Unlock()
	code, out, stderr = e.run("run", "--wait", "db/pg-0")
	if code != cli.ExitFailed {
		t.Fatalf("exit %d, want %d", code, cli.ExitFailed)
	}
	wantContains(t, out, "db/pg-0: failed after ", "backup command exited with code 2")
	wantContains(t, stderr, "1 of 1 backups did not succeed")
	_, out, _ = e.run("describe", "db/pg-0")
	wantContains(t, out, "Last run:", "failed, ", "Last error:", "exited with code 2")

	code, _, stderr = e.run("run", "db/nope")
	if code != cli.ExitFailed || !strings.Contains(stderr, "unknown backup target") {
		t.Errorf("unknown target: exit %d, stderr %q", code, stderr)
	}
	code, _, stderr = e.run("run", "db/broken")
	if code != cli.ExitFailed || !strings.Contains(stderr, "unknown backup target") {
		t.Errorf("invalid target: exit %d, stderr %q", code, stderr)
	}
}

func TestRunAlreadyRunningAndAll(t *testing.T) {
	e := newEnv(t)
	block := make(chan struct{})
	e.job.mu.Lock()
	e.job.block = block
	e.job.mu.Unlock()

	code, out, _ := e.run("run", "db/pg-0")
	if code != cli.ExitOK || !strings.Contains(out, "started backup of db/pg-0") {
		t.Fatalf("exit %d, out %q", code, out)
	}
	_, out, _ = e.run("list")
	wantContains(t, out, "running")

	code, _, stderr := e.run("run", "db/pg-0")
	if code != cli.ExitFailed || !strings.Contains(stderr, "already running") {
		t.Errorf("second run: exit %d, stderr %q", code, stderr)
	}

	// --all starts only what is not running; --timeout gives up on a stuck one.
	code, out, stderr = e.run("run", "--all", "--wait", "--timeout", "50ms")
	if code != cli.ExitFailed {
		t.Fatalf("exit %d, out %q", code, out)
	}
	wantContains(t, out, "started backup of db/parts-0")
	if strings.Contains(out, "started backup of db/pg-0") {
		t.Errorf("db/pg-0 is running and must not be started again:\n%s", out)
	}
	wantContains(t, stderr, "timed out after 50ms; still running: db/parts-0")
	// While the backup is silent, --wait shows that it is still running.
	wantContains(t, out, "running backup command", "... still running after ")

	// Once the backups are unblocked, --all --wait reports every result.
	close(block)
	e.job.mu.Lock()
	e.job.block = nil
	e.job.mu.Unlock()
	e.waitIdle(t)
	code, out, stderr = e.run("run", "--all", "--wait")
	if code != cli.ExitOK {
		t.Fatalf("exit %d, out %q, stderr %q", code, out, stderr)
	}
	wantContains(t, out, "db/parts-0: ok in ", "db/pg-0: ok in ",
		"[db/pg-0]   ", "[db/parts-0]   ", "running backup command folder=db/parts-0")
}

func TestStatus(t *testing.T) {
	e := newEnv(t)

	code, out, stderr := e.run("status")
	if code != cli.ExitOK {
		t.Fatalf("exit %d, stderr %q", code, stderr)
	}
	wantContains(t, out, "2 scheduled, 1 invalid, 0 in progress", "Next run:", "schedule 0 */6 * * *", "No backup in progress.")
	if strings.Contains(out, "Last run failed") {
		t.Errorf("nothing failed yet:\n%s", out)
	}

	// A failed run is reported until the next successful one.
	e.job.mu.Lock()
	e.job.err = errors.New("backup command exited with code 2")
	e.job.mu.Unlock()
	e.run("run", "db/parts-0", "--wait")
	e.job.mu.Lock()
	e.job.err = nil
	block := make(chan struct{})
	e.job.block = block
	e.job.mu.Unlock()
	_, out, _ = e.run("status")
	wantContains(t, out, "Last run failed:", "db/parts-0, ", "backup command exited with code 2", "No backup in progress.")

	// A backup in progress: its details and the log of this run only.
	e.run("run", "db/pg-0")
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, out, _ = e.run("status")
		if strings.Contains(out, "running backup command") || time.Now().After(deadline) {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	wantContains(t, out,
		"2 scheduled, 1 invalid, 1 in progress",
		"Backup in progress: db/pg-0",
		"State:", "running for ", "Trigger:", "manual, at ",
		"Mode:", "single", "Command:", "pg_dumpall", "Folder:", "db/pg-0", "Compression:", "gzip",
		"Previous run:", "none since oap-backup started",
		"Log:", "INFO  backup triggered trigger=manual", "INFO  running backup command folder=db/pg-0")
	if strings.Contains(out, "backup scheduled") {
		t.Errorf("the log must start at this run's trigger:\n%s", out)
	}

	// --lines limits the log, --lines 0 hides it.
	_, out, _ = e.run("status", "--lines", "1")
	wantContains(t, out, "Log (last 1 of 2 lines):", "running backup command")
	if strings.Contains(out, "backup triggered") {
		t.Errorf("--lines 1 printed more than one line:\n%s", out)
	}
	_, out, _ = e.run("status", "--lines=0")
	if strings.Contains(out, "Log") || !strings.Contains(out, "Backup in progress: db/pg-0") {
		t.Errorf("--lines 0:\n%s", out)
	}

	code, out, _ = e.run("status", "-o", "json")
	var report cli.StatusReport
	if err := json.Unmarshal([]byte(out), &report); code != cli.ExitOK || err != nil {
		t.Fatalf("exit %d, json error %v:\n%s", code, err, out)
	}
	if report.Scheduled != 2 || report.Invalid != 1 || len(report.Failed) != 1 || report.Next == nil || len(report.InProgress) != 1 {
		t.Fatalf("report = %+v", report)
	}
	p := report.InProgress[0]
	if p.Target.Key() != "db/pg-0" || p.CurrentRun == nil || p.CurrentRun.Started == nil || p.CurrentRun.Trigger != scheduler.TriggerManual || len(p.Log) != 2 {
		t.Errorf("in progress = %+v", p)
	}

	close(block)
	e.waitIdle(t)
	_, out, _ = e.run("status")
	wantContains(t, out, "0 in progress", "No backup in progress.")

	for _, args := range [][]string{{"status", "db/pg-0"}, {"status", "--lines", "-1"}, {"status", "-o", "yaml"}} {
		if code, _, _ := e.run(args...); code != cli.ExitUsage {
			t.Errorf("%v: exit %d, want usage error", args, code)
		}
	}
}

func TestLogEndpoint(t *testing.T) {
	e := newEnv(t)
	get := func(path string) (int, api.LogResponse) {
		t.Helper()
		resp, err := http.Get(e.server.URL + path)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		var out api.LogResponse
		_ = json.NewDecoder(resp.Body).Decode(&out)
		return resp.StatusCode, out
	}
	// Scheduling the pod is already part of its log.
	code, log := get("/targets/db/pg-0/log")
	if code != http.StatusOK || len(log.Lines) != 1 || !strings.Contains(log.Lines[0].Text, "backup scheduled") || log.Next != 1 {
		t.Fatalf("log = %d %+v", code, log)
	}
	if code, log := get("/targets/db/pg-0/log?after=1"); code != http.StatusOK || len(log.Lines) != 0 || log.Next != 1 {
		t.Errorf("after=1: %d %+v", code, log)
	}
	if code, _ := get("/targets/db/pg-0/log?after=x"); code != http.StatusBadRequest {
		t.Errorf("after=x: status %d", code)
	}
	if code, _ := get("/targets/db/nope/log"); code != http.StatusNotFound {
		t.Errorf("unknown target: status %d", code)
	}
}

func TestUsageErrors(t *testing.T) {
	e := newEnv(t)
	for _, args := range [][]string{
		{},
		{"frobnicate"},
		{"list", "extra"},
		{"list", "-o", "xml"},
		{"list", "--nope"},
		{"refresh", "now"},
		{"describe"},
		{"describe", "pg-0"},
		{"describe", "db/pg-0", "db/parts-0"},
		{"run"},
		{"run", "db/"},
		{"run", "db/pg-0", "--all"},
		{"run", "db/pg-0", "--timeout", "-1s"},
	} {
		code, out, stderr := e.run(args...)
		if code != cli.ExitUsage || !strings.Contains(stderr, "Usage:") || out != "" {
			t.Errorf("%v: exit %d, stdout %q, stderr %q; want usage error", args, code, out, stderr)
		}
	}
	code, out, _ := e.run("help")
	if code != cli.ExitOK || !strings.Contains(out, "oap-backup describe") {
		t.Errorf("help: exit %d, out %q", code, out)
	}
	code, out, _ = e.run("list", "-h")
	if code != cli.ExitOK || !strings.Contains(out, "Usage:") {
		t.Errorf("list -h: exit %d, out %q", code, out)
	}
}

func TestServerFlagAndUnreachable(t *testing.T) {
	e := newEnv(t)
	// --server overrides the environment, with or without a scheme, before
	// or after the positional argument.
	addr := strings.TrimPrefix(e.server.URL, "http://")
	var stdout, stderr bytes.Buffer
	app := cli.New(func(string) string { return "http://127.0.0.1:1" }, &stdout, &stderr)
	if code := app.Run([]string{"describe", "db/pg-0", "--server", addr}); code != cli.ExitOK {
		t.Fatalf("exit %d, stderr %q", code, stderr.String())
	}
	stdout.Reset()
	if code := app.Run([]string{"list"}); code != cli.ExitFailed || !strings.Contains(stderr.String(), "cannot reach oap-backup at http://127.0.0.1:1") {
		t.Errorf("unreachable: exit %d, stderr %q", code, stderr.String())
	}

	// Something that is not oap-backup.
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "nope", http.StatusBadGateway)
	}))
	defer other.Close()
	stderr.Reset()
	if code := app.Run([]string{"list", "--server", other.URL}); code != cli.ExitFailed || !strings.Contains(stderr.String(), "502") {
		t.Errorf("bad gateway: exit %d, stderr %q", code, stderr.String())
	}
}

func TestIsCommand(t *testing.T) {
	for _, c := range []string{"list", "describe", "run", "refresh", "status", "help"} {
		if !cli.IsCommand(c) {
			t.Errorf("IsCommand(%q) = false", c)
		}
	}
	for _, c := range []string{"", "--storage=s3", "-h", "--help", "serve"} {
		if cli.IsCommand(c) {
			t.Errorf("IsCommand(%q) = true", c)
		}
	}
}
