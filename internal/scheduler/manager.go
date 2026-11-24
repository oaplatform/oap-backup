// Package scheduler keeps one cron entry per annotated pod, each on the
// pod's own schedule, and runs backups through a JobRunner.
package scheduler

import (
	"context"
	"errors"
	"fmt"
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
	"oap-backup/internal/progress"
)

// JobRunner runs one complete backup of a target (backup.Runner, adapted in
// main) and reports what it uploaded.
type JobRunner interface {
	// Run reports how much it has sent to the storage through tracker.
	Run(ctx context.Context, t annotations.Target, tracker *progress.Tracker) (RunResult, error)
}

// RunResult is what one backup run uploaded.
type RunResult struct {
	Files     int
	SizeBytes int64
}

// Triggers of a backup run.
const (
	TriggerSchedule = "schedule"
	TriggerManual   = "manual"
)

// LastRun describes the most recent finished run of a target since
// oap-backup started.
type LastRun struct {
	// Seq numbers the runs of this process, so that two runs can be told
	// apart even if they finish within the same clock tick.
	Seq int64 `json:"seq"`
	// Trigger is TriggerSchedule or TriggerManual.
	Trigger  string    `json:"trigger"`
	Started  time.Time `json:"started"`
	Finished time.Time `json:"finished"`
	// Error is empty for a successful run.
	Error string `json:"error,omitempty"`
	// Files and SizeBytes describe what the run uploaded.
	Files     int   `json:"files"`
	SizeBytes int64 `json:"size_bytes"`
}

// CurrentRun describes a backup that is in progress.
type CurrentRun struct {
	// Trigger is TriggerSchedule or TriggerManual.
	Trigger string `json:"trigger"`
	// Triggered is when the run was requested.
	Triggered time.Time `json:"triggered"`
	// Started is when the backup itself began. It is nil while the run
	// waits for a free slot (MaxConcurrent backups are already running).
	Started *time.Time `json:"started,omitempty"`
	// Progress is how much the backup has sent to the storage so far. It
	// is set once the run has started.
	Progress *progress.Snapshot `json:"progress,omitempty"`

	tracker *progress.Tracker
}

// InvalidTarget is an opted-in pod that is skipped because of its annotations.
type InvalidTarget struct {
	Key   string `json:"key"`
	Error string `json:"error"`
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
	current map[string]*CurrentRun
	warned  map[string]string
	lastRun map[string]*LastRun
	runSeq  int64
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
	// CurrentRun is set while Running.
	CurrentRun *CurrentRun `json:"current_run,omitempty"`
	// LastRun is nil until a run of the target has finished.
	LastRun *LastRun `json:"last_run,omitempty"`
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
		m.current = map[string]*CurrentRun{}
		m.warned = map[string]string{}
		m.lastRun = map[string]*LastRun{}
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
	_ = m.Reconcile(ctx) // logged; retried on the next tick

	ticker := time.NewTicker(m.Refresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			m.shutdown()
			return
		case <-ticker.C:
			_ = m.Reconcile(ctx)
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
// current entries are kept and the error is returned (it is also logged).
// It is safe to call at any time, also while Run is reconciling.
func (m *Manager) Reconcile(ctx context.Context) error {
	m.init()
	list, err := m.Client.CoreV1().Pods(m.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		if ctx.Err() == nil {
			m.Logger.Error("listing pods failed, keeping current schedules", "error", err)
		}
		return fmt.Errorf("listing pods: %w", err)
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return errors.New("oap-backup is shutting down")
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
			delete(m.lastRun, key)
			m.Logger.Info("backup unscheduled (pod gone or annotations removed/invalid)", "target", key)
		}
	}
	m.warned = warned

	if m.Metrics != nil {
		m.Metrics.Targets.Set(float64(len(m.entries)))
		m.Metrics.InvalidTargets.Set(float64(invalid))
	}
	return nil
}

func (m *Manager) runScheduled(key string) {
	if t, ok := m.start(key, TriggerSchedule); ok {
		m.run(t, TriggerSchedule)
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
		if t, ok := m.start(k, TriggerManual); ok {
			started = append(started, k)
			go m.run(t, TriggerManual)
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
	m.current[key] = &CurrentRun{Trigger: reason, Triggered: time.Now(), tracker: progress.New()}
	m.wg.Add(1)
	m.Logger.Info("backup triggered", "target", key, "trigger", reason)
	return e.target, true
}

// run executes one backup of t and records it as the target's last run. The
// record and the end of the "running" state become visible together, so a
// client that polls a target never sees a finished run without its result.
func (m *Manager) run(t annotations.Target, trigger string) {
	defer m.wg.Done()
	var last *LastRun
	defer func() {
		m.mu.Lock()
		delete(m.running, t.Key())
		delete(m.current, t.Key())
		// The target may have been unscheduled while the backup ran.
		if _, ok := m.entries[t.Key()]; ok && last != nil {
			m.runSeq++
			last.Seq = m.runSeq
			m.lastRun[t.Key()] = last
		}
		m.mu.Unlock()
	}()

	select {
	case m.sem <- struct{}{}:
	case <-m.jobCtx.Done():
		return
	}
	defer func() { <-m.sem }()

	started := time.Now()
	var tracker *progress.Tracker
	m.mu.Lock()
	if cur := m.current[t.Key()]; cur != nil {
		cur.Started = &started
		tracker = cur.tracker
	}
	m.mu.Unlock()
	res, err := m.Runner.Run(m.jobCtx, t, tracker) // the runner logs the outcome
	last = &LastRun{Trigger: trigger, Started: started, Finished: time.Now(), Files: res.Files, SizeBytes: res.SizeBytes}
	if err != nil {
		last.Error = err.Error()
	}
}

// Targets returns a snapshot of all scheduled targets, sorted by key.
func (m *Manager) Targets() []TargetStatus {
	m.init()
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now().In(m.Location)
	out := make([]TargetStatus, 0, len(m.entries))
	for k, e := range m.entries {
		out = append(out, m.status(k, e, now))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Target.Key() < out[j].Target.Key() })
	return out
}

// Target returns a snapshot of the scheduled target key ("<namespace>/<pod>").
func (m *Manager) Target(key string) (TargetStatus, bool) {
	m.init()
	m.mu.Lock()
	defer m.mu.Unlock()
	e, ok := m.entries[key]
	if !ok {
		return TargetStatus{}, false
	}
	return m.status(key, e, time.Now().In(m.Location)), true
}

// status builds the snapshot of one entry; m.mu must be held.
func (m *Manager) status(key string, e *entry, now time.Time) TargetStatus {
	st := TargetStatus{Target: e.target, NextRun: e.schedule.Next(now), Running: m.running[key]}
	if last := m.lastRun[key]; last != nil {
		copied := *last
		st.LastRun = &copied
	}
	if cur := m.current[key]; cur != nil {
		copied := *cur
		copied.tracker = nil
		if cur.Started != nil {
			if snap, ok := cur.tracker.Snapshot(); ok {
				copied.Progress = &snap
			}
		}
		st.CurrentRun = &copied
	}
	return st
}

// Invalid returns the opted-in pods that are skipped because their
// annotations are invalid, as of the last reconcile, sorted by key.
func (m *Manager) Invalid() []InvalidTarget {
	m.init()
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]InvalidTarget, 0, len(m.warned))
	for key, msg := range m.warned {
		out = append(out, InvalidTarget{Key: key, Error: msg})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}
