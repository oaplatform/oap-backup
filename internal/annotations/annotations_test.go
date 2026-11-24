package annotations

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func pod(ann map[string]string, containers ...string) *corev1.Pod {
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "db", Name: "postgres-0", Annotations: ann}}
	for _, c := range containers {
		p.Spec.Containers = append(p.Spec.Containers, corev1.Container{Name: c})
	}
	return p
}

func valid() map[string]string {
	return map[string]string{
		Command:           "pg_dumpall -U postgres",
		FileFolder:        "postgres",
		FileExtension:     ".sql",
		RetentionKeepLast: "7",
	}
}

func TestParseNotAnnotated(t *testing.T) {
	_, ok, err := Parse(pod(map[string]string{"foo": "bar"}, "main"), "@daily")
	if ok || err != nil {
		t.Fatalf("ok=%v err=%v, want false/nil", ok, err)
	}
}

func TestParseDefaults(t *testing.T) {
	tg, ok, err := Parse(pod(valid(), "postgres", "sidecar"), "@daily")
	if !ok || err != nil {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	want := Target{
		Namespace: "db", Pod: "postgres-0", Container: "postgres", Command: "pg_dumpall -U postgres",
		Schedule: "@daily", FileFolder: "postgres", FileExtension: ".sql", RetentionKeepLast: 7,
		Compression: Gzip,
	}
	if tg != want {
		t.Fatalf("got %+v\nwant %+v", tg, want)
	}
	if tg.Key() != "db/postgres-0" {
		t.Errorf("Key = %q", tg.Key())
	}
}

func TestParseOptional(t *testing.T) {
	ann := valid()
	ann[Schedule] = " CRON_TZ=Europe/Kyiv 30 1 * * * "
	ann[Container] = "sidecar"
	ann[ExternalImage] = "postgres:17"
	ann[FileFolder] = "/prod/postgres/"
	tg, _, err := Parse(pod(ann, "postgres", "sidecar"), "@daily")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if tg.Schedule != "CRON_TZ=Europe/Kyiv 30 1 * * *" || tg.Container != "sidecar" ||
		tg.ExternalImage != "postgres:17" || tg.FileFolder != "prod/postgres" {
		t.Fatalf("unexpected target %+v", tg)
	}
}

func TestParseSchedules(t *testing.T) {
	for _, s := range []string{"@hourly", "@daily", "@weekly", "@every 6h", "*/15 * * * *", "0 3 * * 1-5", "TZ=UTC 0 0 1 * *"} {
		if _, err := ParseSchedule(s); err != nil {
			t.Errorf("ParseSchedule(%q): %v", s, err)
		}
	}
}

func TestParseErrors(t *testing.T) {
	cases := map[string]func(map[string]string){
		"empty command":     func(a map[string]string) { a[Command] = "  " },
		"bad schedule":      func(a map[string]string) { a[Schedule] = "every day" },
		"six fields":        func(a map[string]string) { a[Schedule] = "0 0 1 * * *" },
		"bad tz":            func(a map[string]string) { a[Schedule] = "CRON_TZ=Mars/Base @daily" },
		"missing folder":    func(a map[string]string) { delete(a, FileFolder) },
		"dotdot folder":     func(a map[string]string) { a[FileFolder] = "a/../b" },
		"missing extension": func(a map[string]string) { delete(a, FileExtension) },
		"slash extension":   func(a map[string]string) { a[FileExtension] = "/x.sql" },
		"missing keep":      func(a map[string]string) { delete(a, RetentionKeepLast) },
		"zero keep":         func(a map[string]string) { a[RetentionKeepLast] = "0" },
		"text keep":         func(a map[string]string) { a[RetentionKeepLast] = "seven" },
		"unknown container": func(a map[string]string) { a[Container] = "nope" },
		"bad compression":   func(a map[string]string) { a[Compression] = "xz" },
		"text level":        func(a map[string]string) { a[CompressionLevel] = "max" },
		"zero level":        func(a map[string]string) { a[CompressionLevel] = "0" },
		"gzip level 10":     func(a map[string]string) { a[CompressionLevel] = "10" },
		"zstd level 23":     func(a map[string]string) { a[Compression] = "zstd"; a[CompressionLevel] = "23" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			ann := valid()
			mutate(ann)
			_, ok, err := Parse(pod(ann, "postgres"), "@daily")
			if !ok || err == nil {
				t.Fatalf("ok=%v err=%v, want true/error", ok, err)
			}
		})
	}
	if _, _, err := Parse(pod(valid()), "@daily"); err == nil || !strings.Contains(err.Error(), "no containers") {
		t.Errorf("pod without containers: err=%v", err)
	}
}

func TestFileNaming(t *testing.T) {
	now := time.Date(2026, 11, 10, 1, 30, 5, 0, time.FixedZone("x", 2*3600))
	plain := Target{FileExtension: ".sql"}
	if plain.Codec() != Gzip || plain.Suffix() != ".sql.gz" || plain.FileName(now) != "backup-20261109233005.sql.gz" {
		t.Errorf("plain: codec=%q suffix=%q name=%q", plain.Codec(), plain.Suffix(), plain.FileName(now))
	}
	gz := Target{FileExtension: ".archive.gz"}
	if gz.Codec() != "" || gz.Suffix() != ".archive.gz" || gz.FileName(now) != "backup-20261109233005.archive.gz" {
		t.Errorf("gz: codec=%q suffix=%q name=%q", gz.Codec(), gz.Suffix(), gz.FileName(now))
	}
	zst := Target{FileExtension: ".sql", Compression: Zstd}
	if zst.Codec() != Zstd || zst.Suffix() != ".sql.zst" || zst.FileName(now) != "backup-20261109233005.sql.zst" {
		t.Errorf("zstd: codec=%q suffix=%q name=%q", zst.Codec(), zst.Suffix(), zst.FileName(now))
	}
	// Output that is already compressed is stored as is, whatever the codec.
	pre := Target{FileExtension: ".tar.zst", Compression: Gzip}
	if pre.Codec() != "" || pre.Suffix() != ".tar.zst" {
		t.Errorf("precompressed: codec=%q suffix=%q", pre.Codec(), pre.Suffix())
	}

	// Backups of the other codec are still this target's backup files.
	for _, name := range []string{"backup-1.sql.gz", "backup-1.sql.zst"} {
		if !plain.IsBackupFile(name) || !zst.IsBackupFile(name) {
			t.Errorf("IsBackupFile(%q) = false", name)
		}
	}
	for _, name := range []string{"backup-1.sql", "backup-1.tar.gz", "notes.txt"} {
		if plain.IsBackupFile(name) {
			t.Errorf("IsBackupFile(%q) = true", name)
		}
	}
	if !gz.IsBackupFile("backup-1.archive.gz") || gz.IsBackupFile("backup-1.archive.gz.zst") {
		t.Error("precompressed target must match only its own extension")
	}
}

func TestParseCompression(t *testing.T) {
	ann := valid()
	ann[Compression] = " ZSTD "
	ann[CompressionLevel] = "19"
	tg, _, err := Parse(pod(ann, "postgres"), "@daily")
	if err != nil || tg.Compression != Zstd || tg.CompressionLevel != 19 {
		t.Fatalf("target %+v, err %v", tg, err)
	}
	ann = valid()
	ann[CompressionLevel] = "9"
	tg, _, err = Parse(pod(ann, "postgres"), "@daily")
	if err != nil || tg.Compression != Gzip || tg.CompressionLevel != 9 {
		t.Fatalf("target %+v, err %v", tg, err)
	}
}

func partsAnn() map[string]string {
	ann := valid()
	ann[Parts] = "true"
	ann[PartsCommand] = "/scripts/list-parts.sh"
	ann[Command] = "pg_dump -t {PART_ID}"
	ann[ExternalImage] = "postgres:17"
	return ann
}

func TestParseParts(t *testing.T) {
	tg, _, err := Parse(pod(partsAnn(), "postgres"), "@daily")
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !tg.Parts || tg.PartsCommand != "/scripts/list-parts.sh" {
		t.Fatalf("unexpected target %+v", tg)
	}
	if got := tg.PartCommand("p_1"); got != "pg_dump -t p_1" {
		t.Errorf("PartCommand = %q", got)
	}
	if got := tg.PartFolder("p_1"); got != "postgres/p_1" {
		t.Errorf("PartFolder = %q", got)
	}

	// Without oap/backup-parts=true the parts annotations are not validated.
	ann := valid()
	ann[Parts] = "false"
	ann[PartsCommand] = "ignored"
	tg, _, err = Parse(pod(ann, "postgres"), "@daily")
	if err != nil || tg.Parts || tg.PartsCommand != "" {
		t.Errorf("parts=false: target %+v, err %v", tg, err)
	}

	cases := map[string]func(map[string]string){
		"bad bool":              func(a map[string]string) { a[Parts] = "yes please" },
		"missing parts command": func(a map[string]string) { delete(a, PartsCommand) },
		"empty parts command":   func(a map[string]string) { a[PartsCommand] = " " },
		"missing image":         func(a map[string]string) { delete(a, ExternalImage) },
		"missing placeholder":   func(a map[string]string) { a[Command] = "pg_dumpall" },
	}
	for name, mutate := range cases {
		t.Run(name, func(t *testing.T) {
			ann := partsAnn()
			mutate(ann)
			if _, ok, err := Parse(pod(ann, "postgres"), "@daily"); !ok || err == nil {
				t.Fatalf("ok=%v err=%v, want true/error", ok, err)
			}
		})
	}
}

func TestParsePartList(t *testing.T) {
	got, err := ParsePartList("# header\np1=SKIP # unchanged\n\n p-2.x = 20261105101500 \r\n")
	want := []Part{
		{ID: "p1", Skip: true},
		{ID: "p-2.x", Timestamp: time.Date(2026, 11, 5, 10, 15, 0, 0, time.UTC)},
	}
	if err != nil || len(got) != 2 || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("got %+v, %v\nwant %+v", got, err, want)
	}
	if got, err := ParsePartList(""); err != nil || len(got) != 0 {
		t.Errorf("empty output: %+v, %v", got, err)
	}
	for _, bad := range []string{"p1", "p1=", "p1=skip", "p1=20261305000000", "p1=2026110510150", "a/b=SKIP", "..=SKIP", "$(x)=SKIP", "=SKIP", "p1=SKIP\np1=SKIP"} {
		if _, err := ParsePartList(bad); err == nil {
			t.Errorf("ParsePartList(%q): want error", bad)
		}
	}
}

func TestTimestamp(t *testing.T) {
	tg := Target{FileExtension: ".sql"}
	ts, ok := tg.Timestamp("backup-20261105101500.sql.gz")
	if !ok || FormatTimestamp(ts) != "20261105101500" {
		t.Errorf("ts=%v ok=%v", ts, ok)
	}
	if ts, ok := tg.Timestamp("backup-20261105101500.sql.zst"); !ok || FormatTimestamp(ts) != "20261105101500" {
		t.Errorf("other codec: ts=%v ok=%v", ts, ok)
	}
	for _, name := range []string{"backup-20261105101500.sql", "backup-2026.sql.gz", "notes.txt", "x-20261105101500.sql.gz"} {
		if _, ok := tg.Timestamp(name); ok {
			t.Errorf("Timestamp(%q): want !ok", name)
		}
	}
}
