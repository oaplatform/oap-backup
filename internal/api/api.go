// Package api is the HTTP API of the oap-backup service: metrics, health,
// the scheduled targets and manual runs. The CLI (internal/cli) is its client.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"oap-backup/internal/annotations"
	"oap-backup/internal/backup"
	"oap-backup/internal/runlog"
	"oap-backup/internal/scheduler"
)

// TargetList is the response of GET /targets.
type TargetList struct {
	// Targets are the scheduled pods, sorted by key.
	Targets []scheduler.TargetStatus `json:"targets"`
	// Invalid are the opted-in pods skipped because of their annotations.
	Invalid []scheduler.InvalidTarget `json:"invalid"`
}

// TargetDetail is the response of GET /targets/{namespace}/{pod}.
type TargetDetail struct {
	scheduler.TargetStatus
	Storage Storage `json:"storage"`
}

// Storage is what the backup storage holds for one target.
type Storage struct {
	// Location describes the storage root.
	Location string `json:"location"`
	// Folders is the target's folder, or one entry per part folder.
	Folders []backup.StoredFolder `json:"folders"`
	// Error is set when the storage could not be read; Folders is then empty.
	Error string `json:"error,omitempty"`
}

// RunResponse is the response of POST /run.
type RunResponse struct {
	Started []string `json:"started"`
	// LogAfter is, for every started target, the log cursor from just
	// before its backup was triggered: GET .../log?after=<cursor> returns
	// the log of this run from its first line.
	LogAfter map[string]int64 `json:"log_after"`
}

// LogResponse is the response of GET /targets/{namespace}/{pod}/log.
type LogResponse struct {
	// Lines are the recorded lines after the requested cursor, oldest first.
	Lines []runlog.Line `json:"lines"`
	// Next is the cursor to pass as "after" to get only newer lines.
	Next int64 `json:"next"`
}

// ErrorResponse is the body of every non-2xx response.
type ErrorResponse struct {
	Error string `json:"error"`
}

// Scheduler is the part of scheduler.Manager the API uses.
type Scheduler interface {
	Targets() []scheduler.TargetStatus
	Invalid() []scheduler.InvalidTarget
	Target(key string) (scheduler.TargetStatus, bool)
	Trigger(target string) ([]string, error)
	Reconcile(ctx context.Context) error
}

// Inspector reads a target's files from the storage (backup.Runner).
type Inspector interface {
	Inspect(ctx context.Context, t annotations.Target) ([]backup.StoredFolder, error)
}

// LogSource holds the recent log lines of every target (runlog.Recorder).
type LogSource interface {
	Cursor(target string) int64
	Lines(target string, after int64) ([]runlog.Line, int64)
}

// Server serves the API. All fields are required.
type Server struct {
	// Metrics serves GET /metrics.
	Metrics   http.Handler
	Scheduler Scheduler
	Inspector Inspector
	Logs      LogSource
	// StorageLocation describes the storage root, for display.
	StorageLocation string
	// InspectTimeout bounds reading the storage, or re-listing the pods,
	// for one request.
	InspectTimeout time.Duration
	Logger         *slog.Logger
}

// Handler returns the API's routes.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", s.Metrics)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /targets", s.listTargets)
	mux.HandleFunc("GET /targets/{namespace}/{pod}", s.describeTarget)
	mux.HandleFunc("GET /targets/{namespace}/{pod}/log", s.targetLog)
	mux.HandleFunc("POST /run", s.run)
	mux.HandleFunc("POST /refresh", s.refresh)
	return mux
}

func (s *Server) listTargets(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, TargetList{Targets: s.Scheduler.Targets(), Invalid: s.Scheduler.Invalid()})
}

// refresh re-lists the pods now instead of waiting for the next periodic
// reconcile, and returns the resulting targets.
func (s *Server) refresh(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), s.InspectTimeout)
	defer cancel()
	if err := s.Scheduler.Reconcile(ctx); err != nil {
		writeError(w, http.StatusServiceUnavailable, fmt.Sprintf("pods were not rescanned: %v", err))
		return
	}
	s.Logger.Info("manual refresh requested")
	s.listTargets(w, r)
}

func (s *Server) describeTarget(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("namespace") + "/" + r.PathValue("pod")
	status, ok := s.Scheduler.Target(key)
	if !ok {
		writeError(w, http.StatusNotFound, unknownTarget(key))
		return
	}
	detail := TargetDetail{TargetStatus: status, Storage: Storage{Location: s.StorageLocation}}

	ctx, cancel := context.WithTimeout(r.Context(), s.InspectTimeout)
	defer cancel()
	folders, err := s.Inspector.Inspect(ctx, status.Target)
	if err != nil {
		// The rest of the description is still worth returning.
		s.Logger.Warn("reading backup storage failed", "target", key, "error", err)
		detail.Storage.Error = err.Error()
		folders = nil
	}
	if folders == nil {
		folders = []backup.StoredFolder{}
	}
	detail.Storage.Folders = folders
	writeJSON(w, http.StatusOK, detail)
}

// targetLog returns the recent log lines of one target, recorded since
// oap-backup started: its backup runs, schedule changes and prunes.
func (s *Server) targetLog(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("namespace") + "/" + r.PathValue("pod")
	if _, ok := s.Scheduler.Target(key); !ok {
		writeError(w, http.StatusNotFound, unknownTarget(key))
		return
	}
	var after int64
	if raw := r.URL.Query().Get("after"); raw != "" {
		n, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || n < 0 {
			writeError(w, http.StatusBadRequest, fmt.Sprintf("after=%q: must be a non-negative integer", raw))
			return
		}
		after = n
	}
	lines, next := s.Logs.Lines(key, after)
	writeJSON(w, http.StatusOK, LogResponse{Lines: lines, Next: next})
}

func (s *Server) run(w http.ResponseWriter, r *http.Request) {
	target := r.URL.Query().Get("target")
	// The cursors are taken before the trigger, so that the log of the run
	// includes its very first line.
	logAfter := map[string]int64{}
	if target != "" {
		logAfter[target] = s.Logs.Cursor(target)
	} else {
		for _, t := range s.Scheduler.Targets() {
			logAfter[t.Target.Key()] = s.Logs.Cursor(t.Target.Key())
		}
	}
	started, err := s.Scheduler.Trigger(target)
	if errors.Is(err, scheduler.ErrUnknownTarget) {
		writeError(w, http.StatusNotFound, unknownTarget(target))
		return
	}
	if err != nil {
		writeError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if target != "" && len(started) == 0 {
		writeError(w, http.StatusConflict,
			fmt.Sprintf("backup of %q was not started: it is already running, or oap-backup is shutting down", target))
		return
	}
	if started == nil {
		started = []string{}
	}
	all := target
	if all == "" {
		all = "<all>"
	}
	s.Logger.Info("manual backup requested", "requested", all, "started", strings.Join(started, ","))
	resp := RunResponse{Started: started, LogAfter: map[string]int64{}}
	for _, key := range started {
		resp.LogAfter[key] = logAfter[key]
	}
	writeJSON(w, http.StatusAccepted, resp)
}

func unknownTarget(key string) string {
	return fmt.Sprintf("%v: %q (not a running pod with valid oap/backup-* annotations)", scheduler.ErrUnknownTarget, key)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, ErrorResponse{Error: msg})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}
