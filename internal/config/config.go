// Package config parses and validates command-line configuration for the
// oap-backup binary.
package config

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"
)

// Storage backend names accepted by --storage.
const (
	StorageFTP = "ftp"
	StorageS3  = "s3"
)

// Environment variables holding storage credentials. Secrets are read from
// the environment only (never from flags) so they do not show up in the pod
// spec args or in `ps` output.
const (
	EnvFTPPassword = "FTP_PASSWORD"
	EnvS3AccessKey = "S3_ACCESS_KEY"
	EnvS3SecretKey = "S3_SECRET_KEY"
)

// FTP holds the FTP storage configuration.
type FTP struct {
	Addr     string
	User     string
	Password string
	Folder   string
	TLS      bool
	Timeout  time.Duration
}

// S3 holds the S3-compatible storage configuration.
type S3 struct {
	Endpoint  string
	Bucket    string
	Prefix    string
	Region    string
	Insecure  bool
	AccessKey string
	SecretKey string
}

// Config holds fully parsed and validated oap-backup configuration.
type Config struct {
	// Refresh is how often pods are re-listed and cron entries reconciled.
	Refresh time.Duration
	// Namespace restricts pod discovery to one namespace; empty means all.
	Namespace string
	// DefaultSchedule is used for pods without the oap/backup-schedule annotation.
	DefaultSchedule string
	// Timezone is the default location for cron schedules (overridable per
	// pod with a CRON_TZ= prefix in the schedule annotation).
	Timezone *time.Location

	MaxConcurrent int
	Retries       int
	RetryDelay    time.Duration
	BackupTimeout time.Duration
	HelperTimeout time.Duration
	TmpDir        string
	// ShutdownTimeout is how long running backups may continue after
	// SIGTERM before they are cancelled.
	ShutdownTimeout time.Duration

	Storage string
	FTP     FTP
	S3      S3

	MetricsPort int
	LogLevel    string
}

// Load parses command-line arguments into a Config, returning an error for
// malformed or inconsistent flags. args should not include the program name
// (i.e. os.Args[1:]). getenv supplies secrets; pass os.Getenv in production.
func Load(args []string, getenv func(string) string) (Config, error) {
	if getenv == nil {
		getenv = os.Getenv
	}
	fs := flag.NewFlagSet("oap-backup", flag.ContinueOnError)

	refresh := fs.Duration("refresh", 60*time.Second, "how often to re-list pods and reconcile backup schedules")
	namespace := fs.String("namespace", "", "only discover pods in this namespace (default: all namespaces)")
	defaultSchedule := fs.String("default_schedule", "@daily", "cron schedule for pods without the oap/backup-schedule annotation")
	timezone := fs.String("timezone", "UTC", "default time zone for cron schedules (IANA name, e.g. Europe/Kyiv)")

	maxConcurrent := fs.Int("max_concurrent", 2, "maximum number of backups running at the same time")
	retries := fs.Int("retries", 2, "how many times a failed backup is retried")
	retryDelay := fs.Duration("retry_delay", 5*time.Minute, "delay between backup retries")
	backupTimeout := fs.Duration("backup_timeout", time.Hour, "timeout of a single backup attempt (exec + upload)")
	helperTimeout := fs.Duration("helper_timeout", 2*time.Minute, "how long to wait for a helper pod to become Running")
	tmpDir := fs.String("tmp_dir", os.TempDir(), "directory for temporary backup files")
	shutdownTimeout := fs.Duration("shutdown_timeout", 30*time.Second, "on SIGTERM, how long running backups may finish before they are cancelled")

	storage := fs.String("storage", StorageFTP, "backup storage backend: ftp or s3")

	ftpAddr := fs.String("ftp_addr", "", "ftp: server address host:port")
	ftpUser := fs.String("ftp_user", "", "ftp: user name (password from $"+EnvFTPPassword+")")
	ftpFolder := fs.String("ftp_folder", "/", "ftp: root folder for backups")
	ftpTLS := fs.Bool("ftp_tls", false, "ftp: use explicit TLS (AUTH TLS)")
	ftpTimeout := fs.Duration("ftp_timeout", 30*time.Second, "ftp: dial/command timeout")

	s3Endpoint := fs.String("s3_endpoint", "", "s3: endpoint host[:port], e.g. s3.amazonaws.com or minio:9000")
	s3Bucket := fs.String("s3_bucket", "", "s3: bucket name")
	s3Prefix := fs.String("s3_prefix", "", "s3: key prefix (root folder) for backups")
	s3Region := fs.String("s3_region", "", "s3: region (optional)")
	s3Insecure := fs.Bool("s3_insecure", false, "s3: use plain HTTP instead of HTTPS")

	metricsPort := fs.Int("metrics_port", 9090, "TCP port for /metrics, /healthz and /run")
	logLevel := fs.String("loglevel", "info", "log level: debug, info, warn, or error")

	if err := fs.Parse(args); err != nil {
		return Config{}, err
	}
	if fs.NArg() > 0 {
		return Config{}, fmt.Errorf("unexpected arguments: %s", strings.Join(fs.Args(), " "))
	}

	if *refresh <= 0 {
		return Config{}, fmt.Errorf("--refresh must be positive, got %s", *refresh)
	}
	if strings.TrimSpace(*defaultSchedule) == "" {
		return Config{}, fmt.Errorf("--default_schedule must not be empty")
	}
	loc, err := time.LoadLocation(*timezone)
	if err != nil {
		return Config{}, fmt.Errorf("invalid --timezone %q: %w", *timezone, err)
	}
	if *maxConcurrent <= 0 {
		return Config{}, fmt.Errorf("--max_concurrent must be positive, got %d", *maxConcurrent)
	}
	if *retries < 0 {
		return Config{}, fmt.Errorf("--retries must not be negative, got %d", *retries)
	}
	if *retryDelay < 0 {
		return Config{}, fmt.Errorf("--retry_delay must not be negative, got %s", *retryDelay)
	}
	if *backupTimeout <= 0 {
		return Config{}, fmt.Errorf("--backup_timeout must be positive, got %s", *backupTimeout)
	}
	if *helperTimeout <= 0 {
		return Config{}, fmt.Errorf("--helper_timeout must be positive, got %s", *helperTimeout)
	}
	if *tmpDir == "" {
		return Config{}, fmt.Errorf("--tmp_dir must not be empty")
	}
	if *shutdownTimeout < 0 {
		return Config{}, fmt.Errorf("--shutdown_timeout must not be negative, got %s", *shutdownTimeout)
	}
	if *metricsPort <= 0 || *metricsPort > 65535 {
		return Config{}, fmt.Errorf("--metrics_port must be between 1 and 65535, got %d", *metricsPort)
	}

	cfg := Config{
		ShutdownTimeout: *shutdownTimeout,
		Refresh:         *refresh,
		Namespace:       *namespace,
		DefaultSchedule: *defaultSchedule,
		Timezone:        loc,
		MaxConcurrent:   *maxConcurrent,
		Retries:         *retries,
		RetryDelay:      *retryDelay,
		BackupTimeout:   *backupTimeout,
		HelperTimeout:   *helperTimeout,
		TmpDir:          *tmpDir,
		Storage:         strings.ToLower(*storage),
		MetricsPort:     *metricsPort,
		LogLevel:        *logLevel,
	}

	switch cfg.Storage {
	case StorageFTP:
		if *ftpAddr == "" {
			return Config{}, fmt.Errorf("--ftp_addr is required with --storage=ftp")
		}
		if *ftpTimeout <= 0 {
			return Config{}, fmt.Errorf("--ftp_timeout must be positive, got %s", *ftpTimeout)
		}
		cfg.FTP = FTP{
			Addr:     *ftpAddr,
			User:     *ftpUser,
			Password: getenv(EnvFTPPassword),
			Folder:   *ftpFolder,
			TLS:      *ftpTLS,
			Timeout:  *ftpTimeout,
		}
	case StorageS3:
		if *s3Endpoint == "" {
			return Config{}, fmt.Errorf("--s3_endpoint is required with --storage=s3")
		}
		if *s3Bucket == "" {
			return Config{}, fmt.Errorf("--s3_bucket is required with --storage=s3")
		}
		cfg.S3 = S3{
			Endpoint:  *s3Endpoint,
			Bucket:    *s3Bucket,
			Prefix:    *s3Prefix,
			Region:    *s3Region,
			Insecure:  *s3Insecure,
			AccessKey: getenv(EnvS3AccessKey),
			SecretKey: getenv(EnvS3SecretKey),
		}
	default:
		return Config{}, fmt.Errorf("--storage must be %q or %q, got %q", StorageFTP, StorageS3, *storage)
	}

	return cfg, nil
}
