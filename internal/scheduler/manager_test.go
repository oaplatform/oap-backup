package scheduler

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"oap-backup/internal/annotations"
	"oap-backup/internal/metrics"
)

type fakeRunner struct {
	mu    sync.Mutex
	runs  []string
	block chan struct{}
	done  chan string
}

func (f *fakeRunner) Run(ctx context.Context, t annotations.Target) error {
	if f.block != nil {
		select {
		case <-f.block:
		case <-ctx.Done():
		}
	}
	f.mu.Lock()
	f.runs = append(f.runs, t.Key())
	f.mu.Unlock()
	if f.done != nil {
		f.done <- t.Key()
	}
	return nil
}

func annotatedPod(ns, name, schedule string) *corev1.Pod {
	ann := map[string]string{
		annotations.Command:           "pg_dumpall",
		annotations.FileFolder:        name,
		annotations.FileExtension:     ".sql",
		annotations.RetentionKeepLast: "3",
	}
	if schedule != "" {
		ann[annotations.Schedule] = schedule
	}
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: ns, Name: name, Annotations: ann},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "main"}}},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func newManager(client *fake.Clientset, r JobRunner) *Manager {
	return &Manager{
		Client: client, DefaultSchedule: "@daily", Refresh: time.Minute, MaxConcurrent: 2,
		ShutdownTimeout: time.Second, Runner: r, Metrics: metrics.New(),
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
}

func keys(m *Manager) map[string]string {
	out := map[string]string{}
	for _, s := range m.Targets() {
		out[s.Target.Key()] = s.Target.Schedule
	}
	return out
}

func TestReconcile(t *testing.T) {
	ctx := context.Background()
	pending := annotatedPod("db", "pending", "")
	pending.Status.Phase = corev1.PodPending
	broken := annotatedPod("db", "broken", "")
	delete(broken.Annotations, annotations.FileFolder)
	plain := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "db", Name: "plain"}, Status: corev1.PodStatus{Phase: corev1.PodRunning}}

	client := fake.NewClientset(annotatedPod("db", "pg-0", ""), annotatedPod("db", "mongo-0", "*/5 * * * *"), pending, broken, plain)
	m := newManager(client, &fakeRunner{})
	m.Reconcile(ctx)

	got := keys(m)
	want := map[string]string{"db/pg-0": "@daily", "db/mongo-0": "*/5 * * * *"}
	if len(got) != len(want) || got["db/pg-0"] != want["db/pg-0"] || got["db/mongo-0"] != want["db/mongo-0"] {
		t.Fatalf("targets = %v, want %v", got, want)
	}
	if v := testutil.ToFloat64(m.Metrics.Targets); v != 2 {
		t.Errorf("targets metric = %v", v)
	}
	if v := testutil.ToFloat64(m.Metrics.InvalidTargets); v != 1 {
		t.Errorf("invalid metric = %v", v)
	}
	if len(m.cron.Entries()) != 2 {
		t.Errorf("cron entries = %d", len(m.cron.Entries()))
	}

	// Change a schedule, delete a pod.
	updated := annotatedPod("db", "mongo-0", "CRON_TZ=Europe/Kyiv 0 3 * * *")
	if _, err := client.CoreV1().Pods("db").Update(ctx, updated, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	if err := client.CoreV1().Pods("db").Delete(ctx, "pg-0", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	m.Reconcile(ctx)
	got = keys(m)
	if len(got) != 1 || got["db/mongo-0"] != "CRON_TZ=Europe/Kyiv 0 3 * * *" {
		t.Fatalf("after update targets = %v", got)
	}
	if len(m.cron.Entries()) != 1 {
		t.Errorf("cron entries = %d, want 1", len(m.cron.Entries()))
	}
	next := m.Targets()[0].NextRun
	if kyiv, _ := time.LoadLocation("Europe/Kyiv"); next.In(kyiv).Hour() != 3 {
		t.Errorf("next run %s is not 03:00 Kyiv", next)
	}
}

func TestReconcileNamespace(t *testing.T) {
	client := fake.NewClientset(annotatedPod("a", "x", ""), annotatedPod("b", "y", ""))
	m := newManager(client, &fakeRunner{})
	m.Namespace = "b"
	m.Reconcile(context.Background())
	if got := keys(m); len(got) != 1 || got["b/y"] == "" {
		t.Fatalf("targets = %v", got)
	}
}

func TestTrigger(t *testing.T) {
	client := fake.NewClientset(annotatedPod("db", "a", ""), annotatedPod("db", "b", ""))
	r := &fakeRunner{block: make(chan struct{}), done: make(chan string, 4)}
	m := newManager(client, r)
	m.Reconcile(context.Background())

	if _, err := m.Trigger("db/nope"); !errors.Is(err, ErrUnknownTarget) {
		t.Fatalf("unknown target err = %v", err)
	}
	started, err := m.Trigger("db/a")
	if err != nil || len(started) != 1 {
		t.Fatalf("Trigger(db/a) = %v, %v", started, err)
	}
	// Still running: a second trigger of the same target is skipped.
	started, _ = m.Trigger("")
	if len(started) != 1 || started[0] != "db/b" {
		t.Fatalf("Trigger(all) while db/a running = %v, want [db/b]", started)
	}
	close(r.block)
	for range 2 {
		select {
		case <-r.done:
		case <-time.After(5 * time.Second):
			t.Fatal("backup did not finish")
		}
	}
	m.wg.Wait()
	if len(m.Targets()) != 2 || m.Targets()[0].Running {
		t.Errorf("unexpected status %+v", m.Targets())
	}
}

func TestRunShutdownCancelsRunningBackups(t *testing.T) {
	client := fake.NewClientset(annotatedPod("db", "a", ""))
	r := &fakeRunner{block: make(chan struct{}), done: make(chan string, 1)}
	m := newManager(client, r)
	m.ShutdownTimeout = 50 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	stopped := make(chan struct{})
	go func() {
		m.Run(ctx)
		close(stopped)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for len(m.Targets()) == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if _, err := m.Trigger("db/a"); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after shutdown timeout")
	}
	if _, err := m.Trigger(""); err != nil {
		t.Fatal(err)
	}
	if started, _ := m.Trigger("db/a"); len(started) != 0 {
		t.Errorf("trigger after shutdown started %v", started)
	}
}
