package backup

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"oap-backup/internal/annotations"
	"oap-backup/internal/kube"
)

type execCall struct{ ns, pod, container string }

type fakeExec struct {
	mu     sync.Mutex
	calls  []execCall
	stdout string
	// errs are returned by successive calls; nil once exhausted.
	errs []error
}

func (f *fakeExec) Exec(_ context.Context, ns, pod, container string, cmd []string, stdout, stderr io.Writer) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, execCall{ns, pod, container})
	if len(cmd) != 3 || cmd[0] != "sh" || cmd[1] != "-c" {
		return errors.New("unexpected command")
	}
	_, _ = io.WriteString(stdout, f.stdout)
	_, _ = io.WriteString(stderr, "progress 1\nprogress 2")
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		return err
	}
	return nil
}

type memStorage struct {
	mu      sync.Mutex
	files   map[string]map[string][]byte
	failDel map[string]bool
}

func newMem() *memStorage {
	return &memStorage{files: map[string]map[string][]byte{}, failDel: map[string]bool{}}
}

func (m *memStorage) Upload(_ context.Context, dir, name, local string) (string, error) {
	data, err := os.ReadFile(local)
	if err != nil {
		return "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.files[dir] == nil {
		m.files[dir] = map[string][]byte{}
	}
	m.files[dir][name] = data
	return "mem:" + dir + "/" + name, nil
}

func (m *memStorage) List(_ context.Context, dir string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for n := range m.files[dir] {
		out = append(out, n)
	}
	return out, nil
}

func (m *memStorage) Delete(_ context.Context, dir, name string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.failDel[name] {
		return errors.New("denied")
	}
	delete(m.files[dir], name)
	return nil
}

func (m *memStorage) names(dir string) []string {
	n, _ := m.List(context.Background(), dir)
	sort.Strings(n)
	return n
}

type fakeHelpers struct{ created, deleted []string }

func (h *fakeHelpers) Create(_ context.Context, ns, pod, container, image string) (string, error) {
	name := kube.HelperPodName(pod)
	h.created = append(h.created, ns+"/"+name+"@"+image)
	return name, nil
}

func (h *fakeHelpers) Delete(_ context.Context, ns, name string) error {
	h.deleted = append(h.deleted, ns+"/"+name)
	return nil
}

func newRunner(ex *fakeExec, st *memStorage, h *fakeHelpers, now time.Time) *Runner {
	return &Runner{
		Exec: ex, Helpers: h, Storage: st,
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
		TmpDir:  os.TempDir(),
		Retries: 2, RetryDelay: time.Millisecond, BackupTimeout: time.Minute,
		Now: func() time.Time { return now },
	}
}

func target(ext string) annotations.Target {
	return annotations.Target{
		Namespace: "db", Pod: "pg-0", Container: "postgres", Command: "pg_dumpall",
		FileFolder: "postgres", FileExtension: ext, RetentionKeepLast: 2, Schedule: "@daily",
	}
}

var now = time.Date(2026, 11, 10, 1, 30, 0, 0, time.UTC)

func TestRunGzipsAndUploads(t *testing.T) {
	ex, st, h := &fakeExec{stdout: "dump-data"}, newMem(), &fakeHelpers{}
	if err := newRunner(ex, st, h, now).Run(context.Background(), target(".sql")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	data := st.files["postgres"]["backup-20261110013000.sql.gz"]
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("not gzip: %v (files=%v)", err, st.names("postgres"))
	}
	plain, _ := io.ReadAll(zr)
	if string(plain) != "dump-data" {
		t.Errorf("content = %q", plain)
	}
	if want := []execCall{{"db", "pg-0", "postgres"}}; !reflect.DeepEqual(ex.calls, want) {
		t.Errorf("exec calls = %v", ex.calls)
	}
	if len(h.created) != 0 {
		t.Errorf("unexpected helper pods: %v", h.created)
	}
}

func TestRunAlreadyGzipped(t *testing.T) {
	ex, st := &fakeExec{stdout: "raw-gz-bytes"}, newMem()
	if err := newRunner(ex, st, &fakeHelpers{}, now).Run(context.Background(), target(".archive.gz")); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := string(st.files["postgres"]["backup-20261110013000.archive.gz"]); got != "raw-gz-bytes" {
		t.Errorf("content = %q, want stored as-is", got)
	}
}

func TestRunHelperPodAndRetries(t *testing.T) {
	ex := &fakeExec{stdout: "x", errs: []error{&kube.ExitError{Code: 1}, errors.New("stream reset")}}
	st, h := newMem(), &fakeHelpers{}
	tg := target(".sql")
	tg.ExternalImage = "postgres:17"
	if err := newRunner(ex, st, h, now).Run(context.Background(), tg); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if len(ex.calls) != 3 {
		t.Fatalf("exec calls = %d, want 3 (2 failures + success)", len(ex.calls))
	}
	for _, c := range ex.calls {
		if c.pod != "pg-0-backup-helper" || c.container != kube.HelperContainer {
			t.Errorf("exec in %+v, want helper pod", c)
		}
	}
	if len(h.created) != 3 || len(h.deleted) != 3 {
		t.Errorf("helpers created=%v deleted=%v, want 3 each", h.created, h.deleted)
	}
	if len(st.names("postgres")) != 1 {
		t.Errorf("files = %v", st.names("postgres"))
	}
}

func TestRunFailsAfterRetriesButStillPrunes(t *testing.T) {
	exitErr := &kube.ExitError{Code: 2}
	ex := &fakeExec{errs: []error{exitErr, exitErr, exitErr}}
	st := newMem()
	st.files["postgres"] = map[string][]byte{
		"backup-20261101000000.sql.gz": nil,
		"backup-20261102000000.sql.gz": nil,
		"backup-20261103000000.sql.gz": nil,
	}
	err := newRunner(ex, st, &fakeHelpers{}, now).Run(context.Background(), target(".sql"))
	if err == nil {
		t.Fatal("want error")
	}
	if len(ex.calls) != 3 {
		t.Errorf("exec calls = %d, want 3", len(ex.calls))
	}
	want := []string{"backup-20261102000000.sql.gz", "backup-20261103000000.sql.gz"}
	if got := st.names("postgres"); !reflect.DeepEqual(got, want) {
		t.Errorf("after prune = %v, want %v", got, want)
	}
}

func TestPrune(t *testing.T) {
	st := newMem()
	st.files["postgres"] = map[string][]byte{
		"backup-20261103000000.sql.gz": nil,
		"backup-20261101000000.sql.gz": nil,
		"backup-20261104000000.sql.gz": nil,
		"backup-20261102000000.sql.gz": nil,
		"notes.txt":                    nil,
		"backup-20261105000000.sql":    nil,
	}
	st.files["other"] = map[string][]byte{"keep-me": nil}
	r := newRunner(&fakeExec{}, st, &fakeHelpers{}, now)

	res, err := r.Prune(context.Background(), target(".sql"))
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if want := []string{"backup-20261103000000.sql.gz", "backup-20261104000000.sql.gz"}; !reflect.DeepEqual(res.Kept, want) {
		t.Errorf("kept = %v, want %v", res.Kept, want)
	}
	if len(res.Deleted) != 4 {
		t.Errorf("deleted = %v, want 4", res.Deleted)
	}
	if got := st.names("postgres"); !reflect.DeepEqual(got, res.Kept) {
		t.Errorf("storage = %v", got)
	}
	if len(st.names("other")) != 1 {
		t.Error("other folder must be untouched")
	}

	// Missing folder: nothing to do.
	if res, err := r.Prune(context.Background(), annotations.Target{FileFolder: "nope", FileExtension: ".x", RetentionKeepLast: 1}); err != nil || len(res.Deleted) != 0 {
		t.Errorf("missing folder: res=%+v err=%v", res, err)
	}
}

func TestPruneContinuesOnDeleteError(t *testing.T) {
	st := newMem()
	st.files["postgres"] = map[string][]byte{"junk": nil, "other-junk": nil}
	st.failDel["junk"] = true
	res, err := newRunner(&fakeExec{}, st, &fakeHelpers{}, now).Prune(context.Background(), target(".sql"))
	if err == nil {
		t.Fatal("want error")
	}
	if !reflect.DeepEqual(res.Deleted, []string{"other-junk"}) || !reflect.DeepEqual(st.names("postgres"), []string{"junk"}) {
		t.Errorf("deleted=%v remaining=%v", res.Deleted, st.names("postgres"))
	}
}

func TestLineLogger(t *testing.T) {
	var buf bytes.Buffer
	l := &lineLogger{log: slog.New(slog.NewTextHandler(&buf, nil)), pod: "p"}
	_, _ = l.Write([]byte("a\r\nb"))
	_, _ = l.Write([]byte("c\n\nd"))
	l.Flush()
	out := buf.String()
	for _, want := range []string{"line=a\n", "line=bc\n", "line=d\n"} {
		if !bytes.Contains([]byte(out), []byte(want)) {
			t.Errorf("missing %q in:\n%s", want, out)
		}
	}
	if bytes.Count([]byte(out), []byte("backup command stderr")) != 3 {
		t.Errorf("want 3 lines, got:\n%s", out)
	}
}
