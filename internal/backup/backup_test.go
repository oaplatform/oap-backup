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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/klauspost/compress/zstd"
	corev1 "k8s.io/api/core/v1"

	"oap-backup/internal/annotations"
	"oap-backup/internal/kube"
	"oap-backup/internal/progress"
	"oap-backup/internal/storage"
)

type execCall struct{ ns, pod, container string }

type fakeExec struct {
	mu     sync.Mutex
	calls  []execCall
	stdout string
	// errs are returned by successive calls; nil once exhausted.
	errs []error
	// cmds records the shell command of every call.
	cmds []string
	// handler, when set, replaces the stdout/errs behaviour.
	handler func(cmd string, stdout io.Writer) error
}

func (f *fakeExec) Exec(_ context.Context, ns, pod, container string, cmd []string, stdout, stderr io.Writer) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, execCall{ns, pod, container})
	if len(cmd) != 3 || cmd[0] != "sh" || cmd[1] != "-c" {
		return errors.New("unexpected command")
	}
	f.cmds = append(f.cmds, cmd[2])
	_, _ = io.WriteString(stderr, "progress 1\nprogress 2")
	if f.handler != nil {
		return f.handler(cmd[2], stdout)
	}
	_, _ = io.WriteString(stdout, f.stdout)
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
	// midUpload, when set, is called after half of a file was reported as
	// sent.
	midUpload func()
	// failUpload fails the upload of these file names after half of the
	// file was sent, once each.
	failUpload map[string]bool
}

func newMem() *memStorage {
	return &memStorage{files: map[string]map[string][]byte{}, failDel: map[string]bool{}}
}

func (m *memStorage) Upload(_ context.Context, dir, name, local string, progress func(n int64)) (string, error) {
	data, err := os.ReadFile(local)
	if err != nil {
		return "", err
	}
	// Like a real store, report the upload in pieces.
	half := int64(len(data) / 2)
	if progress != nil {
		progress(half)
	}
	if m.midUpload != nil {
		m.midUpload()
	}
	m.mu.Lock()
	fail := m.failUpload[name]
	delete(m.failUpload, name)
	m.mu.Unlock()
	if fail {
		return "", errors.New("connection reset")
	}
	if progress != nil {
		progress(int64(len(data)) - half)
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

func (m *memStorage) ListFiles(_ context.Context, dir string) ([]storage.File, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []storage.File
	for n, data := range m.files[dir] {
		out = append(out, storage.File{Name: n, Size: int64(len(data))})
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

// ListDirs returns the folders directly inside dir, including empty ones
// (as on FTP) until RemoveDir removes them.
func (m *memStorage) ListDirs(_ context.Context, dir string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for d := range m.files {
		if name, ok := strings.CutPrefix(d, dir+"/"); ok && !strings.Contains(name, "/") {
			out = append(out, name)
		}
	}
	return out, nil
}

func (m *memStorage) RemoveDir(_ context.Context, dir string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.files[dir]) > 0 {
		return errors.New("directory not empty")
	}
	delete(m.files, dir)
	return nil
}

func (m *memStorage) names(dir string) []string {
	n, _ := m.List(context.Background(), dir)
	sort.Strings(n)
	return n
}

type fakeHelpers struct {
	created, deleted []string
	// envs records the env of every created helper pod.
	envs [][]corev1.EnvVar
}

func (h *fakeHelpers) Create(_ context.Context, ns, pod, container, image string, env []corev1.EnvVar) (string, error) {
	name := kube.HelperPodName(pod)
	h.created = append(h.created, ns+"/"+name+"@"+image)
	h.envs = append(h.envs, env)
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
		Compression: annotations.Gzip,
	}
}

var now = time.Date(2026, 11, 10, 1, 30, 0, 0, time.UTC)

func TestRunGzipsAndUploads(t *testing.T) {
	ex, st, h := &fakeExec{stdout: "dump-data"}, newMem(), &fakeHelpers{}
	if _, err := newRunner(ex, st, h, now).Run(context.Background(), target(".sql"), nil); err != nil {
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
	if _, err := newRunner(ex, st, &fakeHelpers{}, now).Run(context.Background(), target(".archive.gz"), nil); err != nil {
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
	if _, err := newRunner(ex, st, h, now).Run(context.Background(), tg, nil); err != nil {
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
	_, err := newRunner(ex, st, &fakeHelpers{}, now).Run(context.Background(), target(".sql"), nil)
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
	// The uncompressed file is a backup too (made without a codec), and it is
	// the newest one.
	if want := []string{"backup-20261104000000.sql.gz", "backup-20261105000000.sql"}; !reflect.DeepEqual(res.Kept, want) {
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

func TestRunZstd(t *testing.T) {
	ex, st := &fakeExec{stdout: strings.Repeat("dump-data ", 1000)}, newMem()
	// An older gzip backup: after switching the codec it is still a backup,
	// so retention counts it instead of deleting it as a foreign file.
	st.files["postgres"] = map[string][]byte{
		"backup-20261101000000.sql.gz": nil,
		"backup-20261102000000.sql.gz": nil,
	}
	tg := target(".sql")
	tg.Compression = annotations.Zstd
	tg.CompressionLevel = 19
	if _, err := newRunner(ex, st, &fakeHelpers{}, now).Run(context.Background(), tg, nil); err != nil {
		t.Fatalf("Run: %v", err)
	}
	want := []string{"backup-20261102000000.sql.gz", "backup-20261110013000.sql.zst"}
	if got := st.names("postgres"); !reflect.DeepEqual(got, want) {
		t.Fatalf("files = %v, want %v", got, want)
	}
	data := st.files["postgres"]["backup-20261110013000.sql.zst"]
	zr, err := zstd.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("zstd reader: %v", err)
	}
	defer zr.Close()
	plain, err := io.ReadAll(zr)
	if err != nil || string(plain) != ex.stdout {
		t.Errorf("content: %d bytes, err %v; want the %d-byte dump", len(plain), err, len(ex.stdout))
	}
	if len(data) >= len(ex.stdout) {
		t.Errorf("stored %d bytes, want less than the %d-byte dump", len(data), len(ex.stdout))
	}
}

func TestRunGzipLevel(t *testing.T) {
	dump := strings.Repeat("dump-data ", 1000)
	sizes := map[int]int{}
	for _, level := range []int{1, 9} {
		ex, st := &fakeExec{stdout: dump}, newMem()
		tg := target(".sql")
		tg.CompressionLevel = level
		if _, err := newRunner(ex, st, &fakeHelpers{}, now).Run(context.Background(), tg, nil); err != nil {
			t.Fatalf("level %d: Run: %v", level, err)
		}
		data := st.files["postgres"]["backup-20261110013000.sql.gz"]
		zr, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			t.Fatalf("level %d: not gzip: %v", level, err)
		}
		if plain, _ := io.ReadAll(zr); string(plain) != dump {
			t.Errorf("level %d: content differs", level)
		}
		sizes[level] = len(data)
	}
	if sizes[9] > sizes[1] {
		t.Errorf("sizes = %v, want level 9 not larger than level 1", sizes)
	}
}

func TestRunPrecompressedZstd(t *testing.T) {
	ex, st := &fakeExec{stdout: "raw-zst-bytes"}, newMem()
	if _, err := newRunner(ex, st, &fakeHelpers{}, now).Run(context.Background(), target(".tar.zst"), nil); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if got := string(st.files["postgres"]["backup-20261110013000.tar.zst"]); got != "raw-zst-bytes" {
		t.Errorf("content = %q, want stored as-is", got)
	}
}

func TestRunNoCompression(t *testing.T) {
	dump := strings.Repeat("dump-data ", 100)
	for name, compression := range map[string]string{"zero value": "", "none": annotations.None} {
		t.Run(name, func(t *testing.T) {
			ex, st := &fakeExec{stdout: dump}, newMem()
			// Backups made while the pod still had a codec stay and count.
			st.files["postgres"] = map[string][]byte{
				"backup-20261101000000.sql.gz":  nil,
				"backup-20261102000000.sql.zst": nil,
			}
			tg := target(".sql")
			tg.Compression = compression
			if _, err := newRunner(ex, st, &fakeHelpers{}, now).Run(context.Background(), tg, nil); err != nil {
				t.Fatalf("Run: %v", err)
			}
			want := []string{"backup-20261102000000.sql.zst", "backup-20261110013000.sql"}
			if got := st.names("postgres"); !reflect.DeepEqual(got, want) {
				t.Fatalf("files = %v, want %v", got, want)
			}
			if got := string(st.files["postgres"]["backup-20261110013000.sql"]); got != dump {
				t.Errorf("stored %d bytes, want the %d-byte dump as is", len(got), len(dump))
			}
		})
	}
}

func TestRunTracksUploadedBytes(t *testing.T) {
	dump := strings.Repeat("x", 1000)
	ex, st := &fakeExec{stdout: dump}, newMem()
	tracker := progress.New()
	// Seen from the middle of the upload: half of the file is on the storage.
	var mid progress.Snapshot
	st.midUpload = func() { mid, _ = tracker.Snapshot() }
	// The first attempt loses the connection after half of the file.
	st.failUpload = map[string]bool{"backup-20261110013000.sql": true}

	tg := target(".sql")
	tg.Compression = annotations.None
	if _, err := newRunner(ex, st, &fakeHelpers{}, now).Run(context.Background(), tg, tracker); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if mid != (progress.Snapshot{UploadedBytes: 500}) {
		t.Errorf("mid-upload snapshot = %+v, want 500 bytes, 0 files", mid)
	}
	// The retry starts from zero, so the failed half is not counted twice.
	if got, _ := tracker.Snapshot(); got != (progress.Snapshot{UploadedBytes: 1000, FilesUploaded: 1}) {
		t.Errorf("final snapshot = %+v, want 1000 bytes, 1 file", got)
	}
}
