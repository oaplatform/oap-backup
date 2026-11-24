package backup

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"strings"
	"testing"

	"oap-backup/internal/annotations"
)

// fakeScripts resolves references from a map. errs are returned by
// successive calls first.
type fakeScripts struct {
	scripts map[annotations.Ref]string
	errs    []error
	calls   []string
}

func (f *fakeScripts) Resolve(_ context.Context, namespace string, ref annotations.Ref) (string, error) {
	f.calls = append(f.calls, namespace+" "+ref.String())
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		if err != nil {
			return "", err
		}
	}
	script, ok := f.scripts[ref]
	if !ok {
		return "", errors.New(ref.String() + " not found")
	}
	return script, nil
}

var (
	backupScript = annotations.Ref{Kind: annotations.RefSecret, Name: "pg-secret", Field: "dump.sh"}
	partsScript  = annotations.Ref{Kind: annotations.RefConfigMap, Name: "pg-scripts", Field: "list.sh"}
)

// withLog makes r log into the returned buffer.
func withLog(r *Runner) *bytes.Buffer {
	var buf bytes.Buffer
	r.Logger = slog.New(slog.NewTextHandler(&buf, nil))
	return &buf
}

func TestRunCommandFromScript(t *testing.T) {
	const script = "PGPASSWORD=s3cret pg_dumpall -U postgres"
	scripts := &fakeScripts{
		scripts: map[annotations.Ref]string{backupScript: script},
		// The first attempt cannot read the script; the retry reads it again.
		errs: []error{errors.New("secret db/pg-secret not found")},
	}
	ex, st := &fakeExec{stdout: "dump"}, newMem()
	r := newRunner(ex, st, &fakeHelpers{}, now)
	r.Scripts = scripts
	log := withLog(r)

	tg := target(".sql")
	tg.Command = ""
	tg.CommandFrom = backupScript
	if _, err := r.Run(context.Background(), tg, nil); err != nil {
		t.Fatalf("Run: %v", err)
	}
	if want := []string{script}; !reflect.DeepEqual(ex.cmds, want) {
		t.Errorf("executed %q, want the script's content", ex.cmds)
	}
	if want := []string{"db secret pg-secret/dump.sh", "db secret pg-secret/dump.sh"}; !reflect.DeepEqual(scripts.calls, want) {
		t.Errorf("resolved %v, want once per attempt in the pod's namespace", scripts.calls)
	}
	if len(st.names("postgres")) != 1 {
		t.Errorf("files = %v", st.names("postgres"))
	}
	// The log names the reference and why the first attempt failed, never
	// the script.
	out := log.String()
	for _, want := range []string{`command="secret pg-secret/dump.sh"`, "backup command: secret db/pg-secret not found"} {
		if !strings.Contains(out, want) {
			t.Errorf("log does not contain %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "s3cret") || strings.Contains(out, "pg_dumpall") {
		t.Errorf("log leaks the script:\n%s", out)
	}
}

func TestRunScriptUnreadable(t *testing.T) {
	ex, h := &fakeExec{}, &fakeHelpers{}
	r := newRunner(ex, newMem(), h, now)
	r.Scripts = &fakeScripts{}
	tg := target(".sql")
	tg.Command, tg.CommandFrom, tg.ExternalImage = "", backupScript, "postgres:17"

	_, err := r.Run(context.Background(), tg, nil)
	if err == nil || !strings.Contains(err.Error(), "backup command: secret pg-secret/dump.sh not found") {
		t.Fatalf("err = %v", err)
	}
	// Nothing ran and no helper pod was created for a script that is missing.
	if len(ex.cmds) != 0 || len(h.created) != 0 {
		t.Errorf("executed %v, helpers %v; want none", ex.cmds, h.created)
	}

	// A target with a reference but no resolver configured.
	r.Scripts = nil
	if _, err := r.Run(context.Background(), tg, nil); err == nil || !strings.Contains(err.Error(), "not configured") {
		t.Errorf("no resolver: err = %v", err)
	}
}

func TestRunPartsFromScripts(t *testing.T) {
	scripts := &fakeScripts{scripts: map[annotations.Ref]string{
		partsScript:  "list-parts --all",
		backupScript: "TOKEN=s3cret dump {PART_ID} > /dev/stdout # {PART_ID}",
	}}
	st, h := newMem(), &fakeHelpers{}
	ex := &fakeExec{handler: func(cmd string, stdout io.Writer) error {
		if cmd == "list-parts --all" {
			_, _ = io.WriteString(stdout, "a=20261105000000\nb=SKIP\n")
			return nil
		}
		_, _ = io.WriteString(stdout, "x")
		return nil
	}}
	r := newRunner(ex, st, h, now)
	r.Scripts = scripts
	log := withLog(r)

	tg := partsTarget()
	tg.Command, tg.CommandFrom = "", backupScript
	tg.PartsCommand, tg.PartsCommandFrom = "", partsScript
	if _, err := r.Run(context.Background(), tg, nil); err != nil {
		t.Fatalf("Run: %v", err)
	}
	// Both scripts ran with their content; every {PART_ID} was replaced.
	want := []string{"list-parts --all", "TOKEN=s3cret dump a > /dev/stdout # a"}
	if !reflect.DeepEqual(ex.cmds, want) {
		t.Errorf("executed %q, want %q", ex.cmds, want)
	}
	if got := st.names("postgres/a"); !reflect.DeepEqual(got, []string{"backup-20261105000000.sql.gz"}) {
		t.Errorf("postgres/a = %v", got)
	}
	out := log.String()
	for _, want := range []string{`command="configmap pg-scripts/list.sh"`, `command="secret pg-secret/dump.sh"`} {
		if !strings.Contains(out, want) {
			t.Errorf("log does not contain %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "s3cret") || strings.Contains(out, "list-parts") {
		t.Errorf("log leaks a script:\n%s", out)
	}

	// A backup script without {PART_ID} fails before anything runs.
	scripts.scripts[backupScript] = "dump-everything"
	ex.cmds, h.created = nil, nil
	r.Retries = 0
	_, err := r.Run(context.Background(), tg, nil)
	if err == nil || !strings.Contains(err.Error(), "backup command (secret pg-secret/dump.sh) must contain {PART_ID}") {
		t.Fatalf("err = %v", err)
	}
	if len(ex.cmds) != 0 || len(h.created) != 0 {
		t.Errorf("executed %v, helpers %v; want none", ex.cmds, h.created)
	}
}

func TestLogSizesAreHumanReadable(t *testing.T) {
	// 5000 bytes, stored as is: 4.9 KiB.
	ex, st := &fakeExec{stdout: strings.Repeat("x", 5000)}, newMem()
	r := newRunner(ex, st, &fakeHelpers{}, now)
	log := withLog(r)
	tg := target(".sql")
	tg.Compression = annotations.None
	if _, err := r.Run(context.Background(), tg, nil); err != nil {
		t.Fatalf("Run: %v", err)
	}
	out := log.String()
	for _, want := range []string{
		`msg="backup file written" target=db/pg-0 folder=postgres file=backup-20261110013000.sql size="4.9 KiB"`,
		`msg="backup complete" target=db/pg-0 folder=postgres file=backup-20261110013000.sql size="4.9 KiB"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log does not contain %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "size_bytes") {
		t.Errorf("log still has size_bytes:\n%s", out)
	}

	// Parts: every part and the total.
	ex = &fakeExec{handler: func(cmd string, stdout io.Writer) error {
		if cmd == "list-parts" {
			_, _ = io.WriteString(stdout, "a=20261105000000\nb=20261105000000\n")
			return nil
		}
		_, _ = io.WriteString(stdout, strings.Repeat("x", 2048))
		return nil
	}}
	r = newRunner(ex, newMem(), &fakeHelpers{}, now)
	log = withLog(r)
	ptg := partsTarget()
	ptg.Compression = annotations.None
	if _, err := r.Run(context.Background(), ptg, nil); err != nil {
		t.Fatalf("Run(parts): %v", err)
	}
	out = log.String()
	for _, want := range []string{
		`msg="part backup complete" target=db/pg-0 folder=postgres part=a file=backup-20261105000000.sql size="2.0 KiB"`,
		`msg="backup complete" target=db/pg-0 folder=postgres files=2 size="4.0 KiB"`,
	} {
		if !strings.Contains(out, want) {
			t.Errorf("log does not contain %q:\n%s", want, out)
		}
	}
	if strings.Contains(out, "size_bytes") {
		t.Errorf("log still has size_bytes:\n%s", out)
	}
}

func TestPruneWarnsAboutTemporaryFiles(t *testing.T) {
	st := newMem()
	st.files["postgres"] = map[string][]byte{
		"backup-20261101000000.sql.gz":     nil,
		"backup-20261102000000.sql.gz.tmp": nil, // interrupted upload
		"notes.txt":                        nil,
	}
	r := newRunner(&fakeExec{}, st, &fakeHelpers{}, now)
	log := withLog(r)
	res, err := r.Prune(context.Background(), target(".sql"))
	if err != nil {
		t.Fatalf("Prune: %v", err)
	}
	if got := st.names("postgres"); !reflect.DeepEqual(got, []string{"backup-20261101000000.sql.gz"}) || len(res.Deleted) != 2 {
		t.Errorf("files = %v, deleted = %v", got, res.Deleted)
	}
	out := log.String()
	// A warning for the temporary file only; the foreign file is pruned as before.
	if !strings.Contains(out, `level=WARN msg="found a temporary file of an interrupted upload, deleting it" target=db/pg-0 folder=postgres file=backup-20261102000000.sql.gz.tmp`) {
		t.Errorf("no warning for the temporary file:\n%s", out)
	}
	if strings.Count(out, "level=WARN") != 1 {
		t.Errorf("want exactly one warning:\n%s", out)
	}

	// A target whose own extension is .tmp: these are its backups.
	st.files["postgres"] = map[string][]byte{"backup-20261101000000.tmp": nil}
	log.Reset()
	tg := target(".tmp")
	tg.Compression = annotations.None
	if res, err := r.Prune(context.Background(), tg); err != nil || len(res.Deleted) != 0 || strings.Contains(log.String(), "level=WARN") {
		t.Errorf(".tmp extension: deleted = %v, err = %v, log:\n%s", res.Deleted, err, log.String())
	}
}
