// Command oap-backup backs up Kubernetes pods that opt in via oap/backup-*
// annotations: on each pod's own cron schedule it execs the annotated
// command in the pod (or in a helper pod), uploads the output to FTP or S3,
// and prunes old backups.
//
// With a subcommand (list, describe, run) it is instead a client of a running
// oap-backup service; see internal/cli.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
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

	"oap-backup/internal/annotations"
	"oap-backup/internal/api"
	"oap-backup/internal/backup"
	"oap-backup/internal/cli"
	"oap-backup/internal/config"
	"oap-backup/internal/kube"
	"oap-backup/internal/metrics"
	"oap-backup/internal/progress"
	"oap-backup/internal/runlog"
	"oap-backup/internal/scheduler"
	"oap-backup/internal/storage"
)

func main() {
	args := os.Args[1:]
	switch {
	case len(args) == 0:
		// The service cannot start without storage flags, so this is a
		// usage error in either role. A non-zero exit also keeps a
		// Deployment that lost its args failing visibly.
		writeHelp(os.Stderr)
		os.Exit(2)
	case helpRequested(args):
		writeHelp(os.Stdout)
	case cli.IsCommand(args[0]):
		os.Exit(cli.New(os.Getenv, os.Stdout, os.Stderr).Run(args))
	default:
		serve(args)
	}
}

// helpRequested reports whether the first argument asks for the general help.
func helpRequested(args []string) bool {
	if len(args) == 0 {
		return false
	}
	switch args[0] {
	case "help", "-h", "-help", "--help":
		return true
	}
	return false
}

// writeHelp prints the help of both roles of the binary: the CLI commands
// and the service with its flags.
func writeHelp(w io.Writer) {
	fmt.Fprint(w, "oap-backup backs up Kubernetes pods that are annotated with oap/backup-*.\n")
	fmt.Fprint(w, "The commands below talk to a running oap-backup service.\n\n")
	fmt.Fprint(w, cli.Usage)
	fmt.Fprint(w, `
Service (this is what the Deployment runs):
  oap-backup --storage=ftp --ftp_addr=<host:port> [flags]
  oap-backup --storage=s3 --s3_endpoint=<host[:port]> --s3_bucket=<bucket> [flags]

Service flags:
`)
	config.Usage(w)
	fmt.Fprintf(w, `
Environment:
  %s                   FTP password
  %s, %s   S3 static credentials
  %s              default --server of the commands
`, config.EnvFTPPassword, config.EnvS3AccessKey, config.EnvS3SecretKey, cli.ServerEnv)
}

// serve runs the oap-backup service with the flags args until it is
// signalled to stop.
func serve(args []string) {
	cfg, err := config.Load(args, os.Getenv)
	if errors.Is(err, flag.ErrHelp) {
		writeHelp(os.Stdout)
		return
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "oap-backup: %v\nRun 'oap-backup --help' for usage.\n", err)
		os.Exit(2)
	}

	logs := runlog.New(0)
	logger := newLogger(cfg.LogLevel, logs)

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
		Scripts:       &kube.Scripts{Client: client},
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
		Runner:          jobRunner{runner},
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
		Addr: fmt.Sprintf(":%d", cfg.MetricsPort),
		Handler: (&api.Server{
			Metrics:         promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{}),
			Scheduler:       mgr,
			Inspector:       runner,
			Logs:            logs,
			StorageLocation: storageDescription(cfg),
			InspectTimeout:  time.Minute,
			Logger:          logger,
		}).Handler(),
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

// jobRunner adapts backup.Runner to scheduler.JobRunner.
type jobRunner struct{ runner *backup.Runner }

func (j jobRunner) Run(ctx context.Context, t annotations.Target, tracker *progress.Tracker) (scheduler.RunResult, error) {
	res, err := j.runner.Run(ctx, t, tracker)
	return scheduler.RunResult{Files: res.Files, SizeBytes: res.SizeBytes}, err
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

// newLogger logs to stderr at level. Records of a backup target are also
// kept in logs, for oap-backup run --wait.
func newLogger(level string, logs *runlog.Recorder) *slog.Logger {
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

	logger := slog.New(logs.Handler(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})))
	if _, ok := knownLevels[strings.ToLower(level)]; level != "" && !ok {
		logger.Warn("unknown --loglevel, defaulting to info", "loglevel", level)
	}
	return logger
}

var knownLevels = map[string]struct{}{
	"debug": {}, "info": {}, "warn": {}, "warning": {}, "error": {},
}
