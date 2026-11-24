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
	"oap-backup/internal/scheduler"
)

// fakeJob is the backup runner behind the service under test.
type fakeJob struct {
	mu    sync.Mutex
	block chan struct{}
	err   error
}

func (f *fakeJob) Run(ctx context.Context, _ annotations.Target) (scheduler.RunResult, error) {
	f.mu.Lock()
	block, err := f.block, f.err
	f.mu.Unlock()
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
	job := &fakeJob{}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
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
		Scheduler: mgr, Inspector: inspector,
		StorageLocation: "s3://bucket/prefix (endpoint s3.local)",
		InspectTimeout:  time.Second,
		Logger:          logger,
	}).Handler())
	t.Cleanup(srv.Close)
	return &env{client: client, server: srv, job: job, inspector: inspector}
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

	// Once the backups are unblocked, --all --wait reports every result.
	close(block)
	e.job.mu.Lock()
	e.job.block = nil
	e.job.mu.Unlock()
	deadline := time.Now().Add(5 * time.Second)
	for {
		_, out, _ = e.run("list")
		if !strings.Contains(out, "running") {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("backups still running:\n%s", out)
		}
		time.Sleep(10 * time.Millisecond)
	}
	code, out, stderr = e.run("run", "--all", "--wait")
	if code != cli.ExitOK {
		t.Fatalf("exit %d, out %q, stderr %q", code, out, stderr)
	}
	wantContains(t, out, "db/parts-0: ok in ", "db/pg-0: ok in ")
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
	for _, c := range []string{"list", "describe", "run", "refresh", "help"} {
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
