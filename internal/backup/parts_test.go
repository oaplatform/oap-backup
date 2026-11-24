package backup

import (
	"bytes"
	"compress/gzip"
	"context"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"

	"oap-backup/internal/annotations"
	"oap-backup/internal/kube"
	"oap-backup/internal/metrics"
	"oap-backup/internal/progress"
)

func partsTarget() annotations.Target {
	tg := target(".sql")
	tg.ExternalImage = "postgres:17"
	tg.Parts = true
	tg.PartsCommand = "list-parts"
	tg.Command = "pg_dump -t {PART_ID}"
	return tg
}

func envMap(env []corev1.EnvVar) map[string]string {
	out := map[string]string{}
	for _, e := range env {
		out[e.Name] = e.Value
	}
	return out
}

func gunzip(t *testing.T, data []byte) string {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		t.Fatalf("not gzip: %v", err)
	}
	plain, _ := io.ReadAll(zr)
	return string(plain)
}

func TestRunParts(t *testing.T) {
	st, h := newMem(), &fakeHelpers{}
	st.files["postgres/p1"] = map[string][]byte{"backup-20261101000000.sql.gz": nil}
	st.files["postgres/p2"] = map[string][]byte{
		"backup-20261101000000.sql.gz": nil,
		"backup-20261102000000.sql.gz": nil,
		"notes.txt":                    nil,
	}
	st.files["postgres/gone"] = map[string][]byte{"backup-20261001000000.sql.gz": nil}
	st.files["postgres/empty"] = map[string][]byte{}
	st.files["postgres/not a part"] = map[string][]byte{"x": nil}
	st.files["postgres"] = map[string][]byte{"root-file": nil}

	ex := &fakeExec{handler: func(cmd string, stdout io.Writer) error {
		if cmd == "list-parts" {
			_, _ = io.WriteString(stdout, "# parts\np1=SKIP # no changes\n\np2=20261105101500\r\n p3 = 20261106000000 \n")
			return nil
		}
		_, _ = io.WriteString(stdout, "out of "+cmd)
		return nil
	}}

	if _, err := newRunner(ex, st, h, now).Run(context.Background(), partsTarget(), nil); err != nil {
		t.Fatalf("Run: %v", err)
	}

	wantEnv := map[string]string{
		"PART_COUNT": "4",
		"PART0_ID":   "empty", "PART0_MT": "",
		"PART1_ID": "gone", "PART1_MT": "20261001000000",
		"PART2_ID": "p1", "PART2_MT": "20261101000000",
		"PART3_ID": "p2", "PART3_MT": "20261102000000",
	}
	if len(h.envs) != 1 || !reflect.DeepEqual(envMap(h.envs[0]), wantEnv) {
		t.Errorf("helper env = %v\nwant %v", h.envs, wantEnv)
	}
	if want := []string{"list-parts", "pg_dump -t p2", "pg_dump -t p3"}; !reflect.DeepEqual(ex.cmds, want) {
		t.Errorf("commands = %v, want %v", ex.cmds, want)
	}
	for _, c := range ex.calls {
		if c.pod != "pg-0-backup-helper" || c.container != kube.HelperContainer {
			t.Errorf("exec in %+v, want helper pod", c)
		}
	}
	if len(h.created) != 1 || len(h.deleted) != 1 {
		t.Errorf("helpers created=%v deleted=%v, want 1 each", h.created, h.deleted)
	}

	// p2: named by the reported timestamp, pruned to keep-last 2, stray file removed.
	if want := []string{"backup-20261102000000.sql.gz", "backup-20261105101500.sql.gz"}; !reflect.DeepEqual(st.names("postgres/p2"), want) {
		t.Errorf("p2 = %v, want %v", st.names("postgres/p2"), want)
	}
	if got := gunzip(t, st.files["postgres/p2"]["backup-20261105101500.sql.gz"]); got != "out of pg_dump -t p2" {
		t.Errorf("p2 content = %q", got)
	}
	// p3: new part.
	if want := []string{"backup-20261106000000.sql.gz"}; !reflect.DeepEqual(st.names("postgres/p3"), want) {
		t.Errorf("p3 = %v, want %v", st.names("postgres/p3"), want)
	}
	// Unchanged and gone parts keep their backups.
	if len(st.names("postgres/p1")) != 1 || len(st.names("postgres/gone")) != 1 {
		t.Errorf("p1 = %v, gone = %v; want untouched", st.names("postgres/p1"), st.names("postgres/gone"))
	}
	// The empty part folder is removed; foreign folders and root files are not touched.
	if _, ok := st.files["postgres/empty"]; ok {
		t.Error("empty part folder must be removed")
	}
	if len(st.names("postgres/not a part")) != 1 || len(st.names("postgres")) != 1 {
		t.Error("non-part folder and root files must be untouched")
	}
}

func TestRunPartsRetryAfterPartFailure(t *testing.T) {
	st, h := newMem(), &fakeHelpers{}
	lists := 0
	failed := false
	ex := &fakeExec{handler: func(cmd string, stdout io.Writer) error {
		switch cmd {
		case "list-parts":
			lists++
			if lists == 1 {
				_, _ = io.WriteString(stdout, "a=20261105000000\nb=20261105000000\n")
			} else {
				// b was uploaded by the first attempt.
				_, _ = io.WriteString(stdout, "a=20261105000000\nb=SKIP\n")
			}
			return nil
		case "pg_dump -t a":
			if !failed {
				failed = true
				return &kube.ExitError{Code: 1}
			}
		}
		_, _ = io.WriteString(stdout, "x")
		return nil
	}}

	if _, err := newRunner(ex, st, h, now).Run(context.Background(), partsTarget(), nil); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// The failed part does not stop the others; the retry backs up only it.
	want := []string{"list-parts", "pg_dump -t a", "pg_dump -t b", "list-parts", "pg_dump -t a"}
	if !reflect.DeepEqual(ex.cmds, want) {
		t.Errorf("commands = %v, want %v", ex.cmds, want)
	}
	if len(h.created) != 2 || len(h.deleted) != 2 {
		t.Errorf("helpers created=%v deleted=%v, want 2 each", h.created, h.deleted)
	}
	wantEnv := map[string]string{"PART_COUNT": "1", "PART0_ID": "b", "PART0_MT": "20261105000000"}
	if len(h.envs) != 2 || !reflect.DeepEqual(envMap(h.envs[1]), wantEnv) {
		t.Errorf("retry env = %v, want %v", h.envs, wantEnv)
	}
	for _, p := range []string{"postgres/a", "postgres/b"} {
		if want := []string{"backup-20261105000000.sql.gz"}; !reflect.DeepEqual(st.names(p), want) {
			t.Errorf("%s = %v, want %v", p, st.names(p), want)
		}
	}
}

func TestRunPartsBadOutput(t *testing.T) {
	for name, output := range map[string]string{
		"no value":      "p1\n",
		"bad timestamp": "p1=2026-11-05\n",
		"bad id":        "../p1=SKIP\n",
		"duplicate":     "p1=SKIP\np1=20261105000000\n",
	} {
		t.Run(name, func(t *testing.T) {
			st, h := newMem(), &fakeHelpers{}
			ex := &fakeExec{handler: func(cmd string, stdout io.Writer) error {
				_, _ = io.WriteString(stdout, output)
				return nil
			}}
			_, err := newRunner(ex, st, h, now).Run(context.Background(), partsTarget(), nil)
			if err == nil || !strings.Contains(err.Error(), "parts command output") {
				t.Fatalf("err = %v, want parts command output error", err)
			}
			// Only the parts command ran (once per attempt); nothing was uploaded.
			if want := []string{"list-parts", "list-parts", "list-parts"}; !reflect.DeepEqual(ex.cmds, want) {
				t.Errorf("commands = %v, want %v", ex.cmds, want)
			}
			if len(st.files) != 0 {
				t.Errorf("files = %v, want none", st.files)
			}
			if len(h.deleted) != 3 {
				t.Errorf("helpers deleted = %v, want 3", h.deleted)
			}
		})
	}
}

func TestRunPartsCommandFails(t *testing.T) {
	ex := &fakeExec{handler: func(string, io.Writer) error { return &kube.ExitError{Code: 3} }}
	_, err := newRunner(ex, newMem(), &fakeHelpers{}, now).Run(context.Background(), partsTarget(), nil)
	if err == nil || !strings.Contains(err.Error(), "parts command exited with code 3") {
		t.Fatalf("err = %v", err)
	}
}

func TestInspect(t *testing.T) {
	st := newMem()
	st.files["postgres"] = map[string][]byte{
		"backup-20261101000000.sql.gz":  []byte("12345"),
		"backup-20261102000000.sql.zst": []byte("123"),
		"notes.txt":                     []byte("1"),
	}
	st.files["postgres/p1"] = map[string][]byte{"backup-20261103000000.sql.gz": []byte("12")}
	st.files["postgres/p2"] = map[string][]byte{}
	st.files["postgres/not a part"] = map[string][]byte{"x": nil}
	r := newRunner(&fakeExec{}, st, &fakeHelpers{}, now)

	folders, err := r.Inspect(context.Background(), target(".sql"))
	if err != nil || len(folders) != 1 {
		t.Fatalf("Inspect = %+v, %v", folders, err)
	}
	f := folders[0]
	if f.Part != "" || f.Folder != "postgres" || len(f.Files) != 3 {
		t.Fatalf("folder = %+v", f)
	}
	// Newest backup first; the foreign file is flagged.
	if f.Files[0].Name != "notes.txt" || f.Files[0].Backup || f.Files[0].Timestamp != nil {
		t.Errorf("files[0] = %+v", f.Files[0])
	}
	if b := f.Files[1]; b.Name != "backup-20261102000000.sql.zst" || !b.Backup || b.SizeBytes != 3 ||
		b.Timestamp == nil || annotations.FormatTimestamp(*b.Timestamp) != "20261102000000" {
		t.Errorf("files[1] = %+v", b)
	}
	if b := f.Files[2]; b.Name != "backup-20261101000000.sql.gz" || b.SizeBytes != 5 {
		t.Errorf("files[2] = %+v", b)
	}

	folders, err = r.Inspect(context.Background(), partsTarget())
	if err != nil || len(folders) != 2 {
		t.Fatalf("Inspect(parts) = %+v, %v", folders, err)
	}
	if folders[0].Part != "p1" || folders[0].Folder != "postgres/p1" || len(folders[0].Files) != 1 ||
		folders[1].Part != "p2" || len(folders[1].Files) != 0 {
		t.Errorf("parts = %+v", folders)
	}

	// Nothing on the storage yet.
	empty := newRunner(&fakeExec{}, newMem(), &fakeHelpers{}, now)
	if folders, err := empty.Inspect(context.Background(), partsTarget()); err != nil || folders == nil || len(folders) != 0 {
		t.Errorf("Inspect(empty parts) = %#v, %v", folders, err)
	}
}

func TestRunPartsMetrics(t *testing.T) {
	st := newMem()
	st.files["postgres/gone"] = map[string][]byte{"backup-20261001000000.sql.gz": nil}
	st.files["postgres/empty"] = map[string][]byte{}
	ex := &fakeExec{handler: func(cmd string, stdout io.Writer) error {
		switch cmd {
		case "list-parts":
			_, _ = io.WriteString(stdout, "a=20261105000000\nb=20261105000000\nc=SKIP\n")
		case "pg_dump -t b":
			return &kube.ExitError{Code: 1}
		default:
			_, _ = io.WriteString(stdout, "x")
		}
		return nil
	}}
	r := newRunner(ex, st, &fakeHelpers{}, now)
	r.Retries = 0
	r.Metrics = metrics.New()
	if _, err := r.Run(context.Background(), partsTarget(), nil); err == nil {
		t.Fatal("want error: part b fails")
	}

	m := r.Metrics
	check := func(name string, got, want float64) {
		t.Helper()
		if got != want {
			t.Errorf("%s = %v, want %v", name, got, want)
		}
	}
	check("parts changed", testutil.ToFloat64(m.Parts.WithLabelValues("db", "postgres", metrics.PartChanged)), 2)
	check("parts unchanged", testutil.ToFloat64(m.Parts.WithLabelValues("db", "postgres", metrics.PartUnchanged)), 1)
	// "gone" and "empty" are stored but no longer listed.
	check("parts gone", testutil.ToFloat64(m.Parts.WithLabelValues("db", "postgres", metrics.PartGone)), 2)
	check("part backups ok", testutil.ToFloat64(m.PartBackups.WithLabelValues("db", "postgres", metrics.ResultSuccess)), 1)
	check("part backups failed", testutil.ToFloat64(m.PartBackups.WithLabelValues("db", "postgres", metrics.ResultFailure)), 1)
	// After pruning: a and gone remain, the empty folder was removed.
	check("parts stored", testutil.ToFloat64(m.PartsStored.WithLabelValues("db", "postgres")), 2)
	check("part folders pruned", testutil.ToFloat64(m.PartFoldersPruned.WithLabelValues("db", "postgres")), 1)
	check("runs failed", testutil.ToFloat64(m.Runs.WithLabelValues("db", "postgres", metrics.ResultFailure)), 1)
}

func TestRunPartsTracksUploadedBytes(t *testing.T) {
	ex := &fakeExec{handler: func(cmd string, stdout io.Writer) error {
		if cmd == "list-parts" {
			_, _ = io.WriteString(stdout, "a=20261105000000\nb=20261105000000\nc=SKIP\n")
			return nil
		}
		_, _ = io.WriteString(stdout, strings.Repeat("x", 300))
		return nil
	}}
	st := newMem()
	tracker := progress.New()
	var seen []progress.Snapshot
	st.midUpload = func() {
		snap, _ := tracker.Snapshot()
		seen = append(seen, snap)
	}
	tg := partsTarget()
	tg.Compression = annotations.None
	if _, err := newRunner(ex, st, &fakeHelpers{}, now).Run(context.Background(), tg, tracker); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Half of part a; then all of a plus half of b.
	want := []progress.Snapshot{{UploadedBytes: 150}, {UploadedBytes: 450, FilesUploaded: 1}}
	if !reflect.DeepEqual(seen, want) {
		t.Errorf("snapshots during the uploads = %+v, want %+v", seen, want)
	}
	if got, _ := tracker.Snapshot(); got != (progress.Snapshot{UploadedBytes: 600, FilesUploaded: 2}) {
		t.Errorf("final snapshot = %+v, want 600 bytes, 2 files", got)
	}
}
