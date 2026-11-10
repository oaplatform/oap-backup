// Command oap-backup backs up Kubernetes pods that opt in via oap/backup-*
// annotations: on each pod's own cron schedule it execs the annotated
// command in the pod (or in a helper pod), uploads the output to FTP or S3,
// and prunes old backups.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"
	_ "time/tzdata" // CRON_TZ / --timezone work without OS zoneinfo

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"

	"oap-backup/internal/backup"
	"oap-backup/internal/config"
	"oap-backup/internal/kube"
	"oap-backup/internal/metrics"
	"oap-backup/internal/scheduler"
	"oap-backup/internal/storage"
)

func main() {
	cfg, err := config.Load(os.Args[1:], os.Getenv)
	if err != nil {
		fmt.Fprintln(os.Stderr, "oap-backup:", err)
		os.Exit(2)
	}

	logger := newLogger(cfg.LogLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	restCfg, err := kubeConfig()
	if err != nil {
		logger.Error("cannot load Kubernetes client config", "error", err)
		os.Exit(1)
	}
	client, err := kubernetes.NewForConfig(restCfg)
	if err != nil {
		logger.Error("cannot create Kubernetes client", "error", err)
		os.Exit(1)
	}

	store, err := storage.New(cfg)
	if err != nil {
		logger.Error("cannot create storage", "error", err)
		os.Exit(1)
	}
	if err := os.MkdirAll(cfg.TmpDir, 0o700); err != nil {
		logger.Error("cannot create --tmp_dir", "tmp_dir", cfg.TmpDir, "error", err)
		os.Exit(1)
	}

	m := metrics.New()
	helpers := &kube.Helpers{Client: client, Timeout: cfg.HelperTimeout}
	runner := &backup.Runner{
		Exec:          &kube.RemoteExecutor{Config: restCfg, Client: client},
		Helpers:       helpers,
		Storage:       store,
		Metrics:       m,
		Logger:        logger,
		TmpDir:        cfg.TmpDir,
		Retries:       cfg.Retries,
		RetryDelay:    cfg.RetryDelay,
		BackupTimeout: cfg.BackupTimeout,
	}
	mgr := &scheduler.Manager{
		Client:          client,
		Namespace:       cfg.Namespace,
		DefaultSchedule: cfg.DefaultSchedule,
		Location:        cfg.Timezone,
		Refresh:         cfg.Refresh,
		MaxConcurrent:   cfg.MaxConcurrent,
		ShutdownTimeout: cfg.ShutdownTimeout,
		Runner:          runner,
		Metrics:         m,
		Logger:          logger,
	}

	logger.Info("starting oap-backup",
		"namespace", orAll(cfg.Namespace),
		"storage", storageDescription(cfg),
		"default_schedule", cfg.DefaultSchedule,
		"timezone", cfg.Timezone.String(),
		"refresh", cfg.Refresh,
		"max_concurrent", cfg.MaxConcurrent,
		"retries", cfg.Retries,
		"retry_delay", cfg.RetryDelay,
		"backup_timeout", cfg.BackupTimeout,
		"tmp_dir", cfg.TmpDir,
		"metrics_port", cfg.MetricsPort,
		"loglevel", cfg.LogLevel,
	)

	cleanupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	deleted, err := helpers.CleanupOrphans(cleanupCtx, cfg.Namespace)
	cancel()
	if err != nil {
		logger.Warn("cleaning up orphaned helper pods failed", "error", err)
	}
	if len(deleted) > 0 {
		logger.Info("deleted orphaned helper pods", "pods", strings.Join(deleted, ","))
	}

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.MetricsPort),
		Handler:           newMux(m, mgr, logger),
		ReadHeaderTimeout: 10 * time.Second,
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		logger.Info("http server listening", "addr", srv.Addr)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("http server failed", "error", err)
		}
	}()
	go func() {
		defer wg.Done()
		mgr.Run(ctx)
	}()

	<-ctx.Done()
	logger.Info("received shutdown signal, shutting down", "shutdown_timeout", cfg.ShutdownTimeout)

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancelShutdown()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("error shutting down http server", "error", err)
	}

	wg.Wait()
	logger.Info("shutdown complete")
}

func newMux(m *metrics.Metrics, mgr *scheduler.Manager, logger *slog.Logger) *http.ServeMux {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("GET /targets", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, mgr.Targets())
	})
	mux.HandleFunc("POST /run", func(w http.ResponseWriter, r *http.Request) {
		target := r.URL.Query().Get("target")
		started, err := mgr.Trigger(target)
		if errors.Is(err, scheduler.ErrUnknownTarget) {
			writeJSON(w, http.StatusNotFound, map[string]string{"error": fmt.Sprintf("%v: %q", err, target)})
			return
		}
		if started == nil {
			started = []string{}
		}
		logger.Info("manual backup requested", "target", orAll(target), "started", strings.Join(started, ","))
		writeJSON(w, http.StatusAccepted, map[string][]string{"started": started})
	})
	return mux
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(v)
}

// kubeConfig uses the in-cluster service account, falling back to the
// standard kubeconfig loading rules ($KUBECONFIG, ~/.kube/config) for local
// development.
func kubeConfig() (*rest.Config, error) {
	cfg, err := rest.InClusterConfig()
	if err == nil {
		return cfg, nil
	}
	if !errors.Is(err, rest.ErrNotInCluster) {
		return nil, err
	}
	return clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		clientcmd.NewDefaultClientConfigLoadingRules(), &clientcmd.ConfigOverrides{},
	).ClientConfig()
}

func storageDescription(cfg config.Config) string {
	switch cfg.Storage {
	case config.StorageFTP:
		tls := ""
		if cfg.FTP.TLS {
			tls = " (tls)"
		}
		return fmt.Sprintf("ftp://%s@%s%s%s", cfg.FTP.User, cfg.FTP.Addr, cfg.FTP.Folder, tls)
	case config.StorageS3:
		return fmt.Sprintf("s3://%s/%s (endpoint %s)", cfg.S3.Bucket, cfg.S3.Prefix, cfg.S3.Endpoint)
	}
	return cfg.Storage
}

func orAll(s string) string {
	if s == "" {
		return "<all>"
	}
	return s
}

func newLogger(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "info", "":
		lvl = slog.LevelInfo
	case "warn", "warning":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl}))
	if _, ok := knownLevels[strings.ToLower(level)]; level != "" && !ok {
		logger.Warn("unknown --loglevel, defaulting to info", "loglevel", level)
	}
	return logger
}

var knownLevels = map[string]struct{}{
	"debug": {}, "info": {}, "warn": {}, "warning": {}, "error": {},
}
