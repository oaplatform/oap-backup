// Package runlog keeps the most recent log lines of every backup target in
// memory, so that a client (oap-backup run --wait) can follow a backup
// without access to the service's own log.
package runlog

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"
)

// TargetKey is the log attribute that assigns a record to a backup target
// ("<namespace>/<pod>").
const TargetKey = "target"

// DefaultMaxLines is how many lines are kept per target.
const DefaultMaxLines = 2000

// maxTargets bounds the number of targets with recorded lines. Pods come
// and go (a Deployment's pods get new names), so the target that has been
// silent the longest is dropped when the limit is reached.
const maxTargets = 1000

// Line is one recorded log line of a target.
type Line struct {
	// Seq numbers the lines of one target, starting at 1.
	Seq   int64     `json:"seq"`
	Time  time.Time `json:"time"`
	Level string    `json:"level"`
	// Text is the message followed by its key=value attributes.
	Text string `json:"text"`
}

// Recorder collects the lines. The zero value is not usable; see New.
type Recorder struct {
	maxLines int

	mu   sync.Mutex
	logs map[string]*targetLog
}

type targetLog struct {
	last  int64 // Seq of the newest line ever recorded
	lines []Line
}

// New returns a Recorder that keeps the newest maxLines lines per target
// (DefaultMaxLines if maxLines <= 0).
func New(maxLines int) *Recorder {
	if maxLines <= 0 {
		maxLines = DefaultMaxLines
	}
	return &Recorder{maxLines: maxLines, logs: map[string]*targetLog{}}
}

// Handler wraps inner: every record goes to inner unchanged, and a record
// that carries the TargetKey attribute (set with Logger.With or on the
// record itself) is also recorded for that target.
func (r *Recorder) Handler(inner slog.Handler) slog.Handler {
	return &handler{rec: r, inner: inner}
}

// Cursor returns the Seq of the newest line of target, or 0 if it has none.
// Pass it to Lines to get only the lines recorded later.
func (r *Recorder) Cursor(target string) int64 {
	r.mu.Lock()
	defer r.mu.Unlock()
	if l := r.logs[target]; l != nil {
		return l.last
	}
	return 0
}

// Lines returns the lines of target with Seq > after, oldest first, and the
// cursor for the next call. Lines that were already dropped are skipped.
func (r *Recorder) Lines(target string, after int64) ([]Line, int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	l := r.logs[target]
	if l == nil {
		return []Line{}, 0
	}
	out := []Line{}
	for _, line := range l.lines {
		if line.Seq > after {
			out = append(out, line)
		}
	}
	return out, l.last
}

func (r *Recorder) add(target string, line Line) {
	r.mu.Lock()
	defer r.mu.Unlock()
	l := r.logs[target]
	if l == nil {
		if len(r.logs) >= maxTargets {
			r.dropSilent()
		}
		l = &targetLog{}
		r.logs[target] = l
	}
	l.last++
	line.Seq = l.last
	l.lines = append(l.lines, line)
	if len(l.lines) > r.maxLines {
		l.lines = append(l.lines[:0:0], l.lines[len(l.lines)-r.maxLines:]...)
	}
}

// dropSilent removes the target whose newest line is the oldest; r.mu must
// be held.
func (r *Recorder) dropSilent() {
	var silent string
	var oldest time.Time
	for target, l := range r.logs {
		newest := l.lines[len(l.lines)-1].Time
		if silent == "" || newest.Before(oldest) {
			silent, oldest = target, newest
		}
	}
	delete(r.logs, silent)
}

type handler struct {
	rec    *Recorder
	inner  slog.Handler
	target string
	// attrs are the attributes added with Logger.With, except TargetKey.
	attrs []slog.Attr
}

func (h *handler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *handler) Handle(ctx context.Context, rec slog.Record) error {
	target := h.target
	var b strings.Builder
	b.WriteString(rec.Message)
	for _, a := range h.attrs {
		writeAttr(&b, a)
	}
	rec.Attrs(func(a slog.Attr) bool {
		if a.Key == TargetKey {
			target = a.Value.String()
		} else {
			writeAttr(&b, a)
		}
		return true
	})
	if target != "" {
		h.rec.add(target, Line{Time: rec.Time, Level: rec.Level.String(), Text: b.String()})
	}
	return h.inner.Handle(ctx, rec)
}

func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := &handler{rec: h.rec, inner: h.inner.WithAttrs(attrs), target: h.target}
	next.attrs = append(next.attrs, h.attrs...)
	for _, a := range attrs {
		if a.Key == TargetKey {
			next.target = a.Value.String()
		} else {
			next.attrs = append(next.attrs, a)
		}
	}
	return next
}

func (h *handler) WithGroup(name string) slog.Handler {
	next := *h
	next.inner = h.inner.WithGroup(name)
	return &next
}

// writeAttr appends " key=value", quoting values that would be ambiguous.
func writeAttr(b *strings.Builder, a slog.Attr) {
	value := a.Value.Resolve()
	var s string
	switch value.Kind() {
	case slog.KindTime:
		s = value.Time().Format(time.RFC3339)
	case slog.KindAny:
		if err, ok := value.Any().(error); ok {
			s = err.Error()
		} else {
			s = fmt.Sprint(value.Any())
		}
	default:
		s = value.String()
	}
	if s == "" || strings.ContainsAny(s, " \t\n\"=") {
		s = strconv.Quote(s)
	}
	b.WriteByte(' ')
	b.WriteString(a.Key)
	b.WriteByte('=')
	b.WriteString(s)
}
