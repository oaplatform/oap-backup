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
	if !plain.Gzip() || plain.Suffix() != ".sql.gz" || plain.FileName(now) != "backup-20261109233005.sql.gz" {
		t.Errorf("plain: gzip=%v suffix=%q name=%q", plain.Gzip(), plain.Suffix(), plain.FileName(now))
	}
	gz := Target{FileExtension: ".archive.gz"}
	if gz.Gzip() || gz.Suffix() != ".archive.gz" || gz.FileName(now) != "backup-20261109233005.archive.gz" {
		t.Errorf("gz: gzip=%v suffix=%q name=%q", gz.Gzip(), gz.Suffix(), gz.FileName(now))
	}
}
