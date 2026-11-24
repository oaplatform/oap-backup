// Package metrics defines the Prometheus metrics exported by oap-backup.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// Result label values of oap_backup_runs_total and
// oap_backup_part_backups_total.
const (
	ResultSuccess = "success"
	ResultFailure = "failure"
)

// State label values of oap_backup_parts.
const (
	PartChanged   = "changed"
	PartUnchanged = "unchanged"
	PartGone      = "gone"
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

	// Partitioned backups. Series are per target, not per part: a part id
	// label would create one series per part and target.
	Parts             *prometheus.GaugeVec
	PartBackups       *prometheus.CounterVec
	PartsStored       *prometheus.GaugeVec
	PartFoldersPruned *prometheus.CounterVec
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
		Parts: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "oap_backup_parts",
			Help: "Parts of a partitioned backup by state, as of the last parts listing: changed (to back up), unchanged (SKIP) or gone (stored but no longer listed).",
		}, append(labels, "state")),
		PartBackups: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "oap_backup_part_backups_total",
			Help: "Backups of single parts of a partitioned backup by result, counting every attempt.",
		}, append(labels, "result")),
		PartsStored: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Name: "oap_backup_parts_stored",
			Help: "Part folders of a partitioned backup on the storage after the last retention pruning.",
		}, labels),
		PartFoldersPruned: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "oap_backup_part_folders_pruned_total",
			Help: "Empty part folders removed from the storage by retention pruning.",
		}, labels),
	}
	m.Registry.MustRegister(
		m.Runs, m.LastSuccess, m.LastSize, m.LastDuration, m.PrunedFiles, m.PruneFailures,
		m.Targets, m.InvalidTargets,
		m.Parts, m.PartBackups, m.PartsStored, m.PartFoldersPruned,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return m
}
