// Package scheduler keeps one cron entry per annotated pod, each on the
// pod's own schedule, and runs backups through a JobRunner.
package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"oap-backup/internal/annotations"
	"oap-backup/internal/metrics"
)

// JobRunner runs one complete backup of a target (implemented by backup.Runner).
type JobRunner interface {
	Run(ctx context.Context, t annotations.Target) error
}

// ErrUnknownTarget is returned by Trigger for a target that is not scheduled.
var ErrUnknownTarget = errors.New("unknown backup target")

// Manager discovers annotated pods and schedules their backups.
type Manager struct {
	Client kubernetes.Interface
	// Namespace limits discovery; empty means all namespaces.
	Namespace       string
	DefaultSchedule string
	// Location is the default time zone of schedules without CRON_TZ.
	Location        *time.Location
	Refresh         time.Duration
	MaxConcurrent   int
	ShutdownTimeout time.Duration
	Runner          JobRunner
	Metrics         *metrics.Metrics
	Logger          *slog.Logger

	once    sync.Once
	mu      sync.Mutex
	cron    *cron.Cron
	entries map[string]*entry
	running map[string]bool
	warned  map[string]string
	closed  bool
	sem     chan struct{}
	wg      sync.WaitGroup
	jobCtx  context.Context
	cancel  context.CancelFunc
}

type entry struct {
	target   annotations.Target
	schedule cron.Schedule
	id       cron.EntryID
}

// TargetStatus is a snapshot of one scheduled target.
type TargetStatus struct {
	Target  annotations.Target `json:"target"`
	NextRun time.Time          `json:"next_run"`
	Running bool               `json:"running"`
}

func (m *Manager) init() {
	m.once.Do(func() {
		loc := m.Location
		if loc == nil {
			loc = time.UTC
		}
		m.Location = loc
		m.cron = cron.New(cron.WithLocation(loc))
		m.entries = map[string]*entry{}
		m.running = map[string]bool{}
		m.warned = map[string]string{}
		n := m.MaxConcurrent
		if n <= 0 {
			n = 1
		}
		m.sem = make(chan struct{}, n)
		m.jobCtx, m.cancel = context.WithCancel(context.Background())
	})
}

// Run reconciles immediately and then every Refresh until ctx is done. On
// shutdown it stops scheduling, gives running backups ShutdownTimeout to
// finish, then cancels them and waits for them to return.
func (m *Manager) Run(ctx context.Context) {
	m.init()
	m.cron.Start()
	m.Reconcile(ctx)

	ticker := time.NewTicker(m.Refresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			m.shutdown()
			return
		case <-ticker.C:
			m.Reconcile(ctx)
		}
	}
}

func (m *Manager) shutdown() {
	m.mu.Lock()
	m.closed = true
	m.mu.Unlock()
	<-m.cron.Stop().Done()

	done := make(chan struct{})
	go func() {
		m.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return
	case <-time.After(m.ShutdownTimeout):
		m.Logger.Warn("backups still running after shutdown timeout, cancelling", "shutdown_timeout", m.ShutdownTimeout)
	}
	m.cancel()
	<-done
}

// Reconcile lists pods and brings cron entries in line with their
// annotations: new targets are added, changed ones replaced, and targets of
// pods that are gone (or no longer valid) removed. If listing fails, the
// current entries are kept.
func (m *Manager) Reconcile(ctx context.Context) {
	m.init()
	list, err := m.Client.CoreV1().Pods(m.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		if ctx.Err() == nil {
			m.Logger.Error("listing pods failed, keeping current schedules", "error", err)
		}
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return
	}

	seen := map[string]bool{}
	warned := map[string]string{}
	invalid := 0
	now := time.Now().In(m.Location)

	for i := range list.Items {
		pod := &list.Items[i]
		if pod.Status.Phase != corev1.PodRunning || pod.DeletionTimestamp != nil {
			continue
		}
		t, ok, err := annotations.Parse(pod, m.DefaultSchedule)
		if !ok {
			continue
		}
		key := pod.Namespace + "/" + pod.Name
		if err != nil {
			invalid++
			msg := err.Error()
			if m.warned[key] != msg {
				m.Logger.Warn("invalid backup annotations, pod skipped", "target", key, "error", msg)
			}
			warned[key] = msg
			continue
		}
		seen[key] = true

		cur, exists := m.entries[key]
		if exists && cur.target == t {
			continue
		}
		schedule, err := annotations.ParseSchedule(t.Schedule)
		if err != nil { // already validated by Parse
			continue
		}
		if exists {
			m.cron.Remove(cur.id)
		}
		id := m.cron.Schedule(schedule, cron.FuncJob(func() { m.runScheduled(key) }))
		m.entries[key] = &entry{target: t, schedule: schedule, id: id}

		action := "backup scheduled"
		if exists {
			action = "backup schedule updated"
		}
		m.Logger.Info(action, "target", key, "container", t.Container, "schedule", t.Schedule,
			"next_run", schedule.Next(now).Format(time.RFC3339), "folder", t.FileFolder,
			"extension", t.FileExtension, "retention_keep_last", t.RetentionKeepLast,
			"external_image", t.ExternalImage)
	}

	for key, e := range m.entries {
		if !seen[key] {
			m.cron.Remove(e.id)
			delete(m.entries, key)
			m.Logger.Info("backup unscheduled (pod gone or annotations removed/invalid)", "target", key)
		}
	}
	m.warned = warned

	if m.Metrics != nil {
		m.Metrics.Targets.Set(float64(len(m.entries)))
		m.Metrics.InvalidTargets.Set(float64(invalid))
	}
}

func (m *Manager) runScheduled(key string) {
	if t, ok := m.start(key, "schedule"); ok {
		m.run(t)
	}
}

// Trigger starts an immediate backup of target ("<namespace>/<pod>"), or of
// every scheduled target when target is empty, and returns the keys that
// were started. Targets whose previous backup is still running are skipped.
func (m *Manager) Trigger(target string) ([]string, error) {
	m.init()
	m.mu.Lock()
	var keys []string
	if target == "" {
		for k := range m.entries {
			keys = append(keys, k)
		}
		sort.Strings(keys)
	} else {
		if _, ok := m.entries[target]; !ok {
			m.mu.Unlock()
			return nil, ErrUnknownTarget
		}
		keys = []string{target}
	}
	m.mu.Unlock()

	var started []string
	for _, k := range keys {
		if t, ok := m.start(k, "manual"); ok {
			started = append(started, k)
			go m.run(t)
		}
	}
	return started, nil
}

// start marks key as running and returns its target, unless the manager is
// shutting down, the target is gone, or it is already running.
func (m *Manager) start(key, reason string) (annotations.Target, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[key]
	if !ok || m.closed {
		return annotations.Target{}, false
	}
	if m.running[key] {
		m.Logger.Warn("previous backup still running, skipping", "target", key, "trigger", reason)
		return annotations.Target{}, false
	}
	m.running[key] = true
	m.wg.Add(1)
	m.Logger.Info("backup triggered", "target", key, "trigger", reason)
	return e.target, true
}

func (m *Manager) run(t annotations.Target) {
	defer m.wg.Done()
	defer func() {
		m.mu.Lock()
		delete(m.running, t.Key())
		m.mu.Unlock()
	}()

	select {
	case m.sem <- struct{}{}:
	case <-m.jobCtx.Done():
		return
	}
	defer func() { <-m.sem }()

	_ = m.Runner.Run(m.jobCtx, t) // the runner logs and records the outcome
}

// Targets returns a snapshot of all scheduled targets, sorted by key.
func (m *Manager) Targets() []TargetStatus {
	m.init()
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().In(m.Location)
	out := make([]TargetStatus, 0, len(m.entries))
	for k, e := range m.entries {
		out = append(out, TargetStatus{Target: e.target, NextRun: e.schedule.Next(now), Running: m.running[k]})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Target.Key() < out[j].Target.Key() })
	return out
}
