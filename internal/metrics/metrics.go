// Package metrics defines the Prometheus metrics exported by oap-backup.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Result label values of oap_backup_runs_total.
const (
	ResultSuccess = "success"
	ResultFailure = "failure"
)

// Metrics groups every oap-backup collector. Per-backup series are labelled
// by namespace and file folder, which (unlike pod names) stay stable when a
// pod is recreated.
type Metrics struct {
	Registry *prometheus.Registry

	Runs           *prometheus.CounterVec
	LastSuccess    *prometheus.GaugeVec
	LastSize       *prometheus.GaugeVec
	LastDuration   *prometheus.GaugeVec
	PrunedFiles    *prometheus.CounterVec
	PruneFailures  *prometheus.CounterVec
	Targets        prometheus.Gauge
	InvalidTargets prometheus.Gauge
}

// New creates the collectors and registers them, plus the Go and process
// collectors, on a dedicated registry.
func New() *Metrics {
	labels := []string{"namespace", "folder"}
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
		Runs: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "oap_backup_runs_total",
			Help: "Backup runs (after retries) by result.",
		}, append(labels, "result")),
		LastSuccess: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "oap_backup_last_success_timestamp_seconds",
			Help: "Unix time of the last successful backup.",
		}, labels),
		LastSize: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "oap_backup_last_size_bytes",
			Help: "Size of the last successfully uploaded backup file.",
		}, labels),
		LastDuration: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "oap_backup_last_duration_seconds",
			Help: "Duration of the last backup run, successful or not, including retries.",
		}, labels),
		PrunedFiles: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "oap_backup_pruned_files_total",
			Help: "Files deleted from the backup storage by retention pruning.",
		}, labels),
		PruneFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "oap_backup_prune_failures_total",
			Help: "Failed retention pruning runs.",
		}, labels),
		Targets: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "oap_backup_targets",
			Help: "Pods with valid backup annotations currently scheduled.",
		}),
		InvalidTargets: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "oap_backup_invalid_targets",
			Help: "Pods with oap/backup-command whose annotations are invalid (skipped).",
		}),
	}
	m.Registry.MustRegister(
		m.Runs, m.LastSuccess, m.LastSize, m.LastDuration, m.PrunedFiles, m.PruneFailures,
		m.Targets, m.InvalidTargets,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}
