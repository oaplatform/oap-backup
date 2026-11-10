package config

import (
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadFTPDefaults(t *testing.T) {
	cfg, err := Load([]string{"--ftp_addr=ftp:21", "--ftp_user=backup"}, env(map[string]string{EnvFTPPassword: "secret"}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Storage != StorageFTP {
		t.Errorf("Storage = %q, want ftp", cfg.Storage)
	}
	if cfg.FTP.Addr != "ftp:21" || cfg.FTP.User != "backup" || cfg.FTP.Password != "secret" || cfg.FTP.Folder != "/" {
		t.Errorf("unexpected FTP config: %+v", cfg.FTP)
	}
	if cfg.Refresh != 60*time.Second || cfg.DefaultSchedule != "@daily" || cfg.Timezone != time.UTC {
		t.Errorf("unexpected defaults: refresh=%s schedule=%q tz=%s", cfg.Refresh, cfg.DefaultSchedule, cfg.Timezone)
	}
	if cfg.MaxConcurrent != 2 || cfg.Retries != 2 || cfg.RetryDelay != 5*time.Minute || cfg.BackupTimeout != time.Hour {
		t.Errorf("unexpected run defaults: %+v", cfg)
	}
	if cfg.MetricsPort != 9090 {
		t.Errorf("MetricsPort = %d, want 9090", cfg.MetricsPort)
	}
}

func TestLoadS3(t *testing.T) {
	cfg, err := Load([]string{
		"--storage=S3", "--s3_endpoint=minio:9000", "--s3_bucket=backups", "--s3_prefix=prod", "--s3_insecure",
		"--timezone=Europe/Kyiv",
	}, env(map[string]string{EnvS3AccessKey: "ak", EnvS3SecretKey: "sk"}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Storage != StorageS3 {
		t.Errorf("Storage = %q, want s3", cfg.Storage)
	}
	want := S3{Endpoint: "minio:9000", Bucket: "backups", Prefix: "prod", Insecure: true, AccessKey: "ak", SecretKey: "sk"}
	if cfg.S3 != want {
		t.Errorf("S3 = %+v, want %+v", cfg.S3, want)
	}
	if cfg.Timezone.String() != "Europe/Kyiv" {
		t.Errorf("Timezone = %s", cfg.Timezone)
	}
}

func TestLoadErrors(t *testing.T) {
	cases := map[string][]string{
		"missing ftp addr":    {},
		"missing s3 bucket":   {"--storage=s3", "--s3_endpoint=x"},
		"missing s3 endpoint": {"--storage=s3", "--s3_bucket=x"},
		"unknown storage":     {"--storage=gcs"},
		"bad timezone":        {"--ftp_addr=x:21", "--timezone=Mars/Base"},
		"zero refresh":        {"--ftp_addr=x:21", "--refresh=0s"},
		"zero concurrency":    {"--ftp_addr=x:21", "--max_concurrent=0"},
		"negative retries":    {"--ftp_addr=x:21", "--retries=-1"},
		"bad port":            {"--ftp_addr=x:21", "--metrics_port=70000"},
		"empty schedule":      {"--ftp_addr=x:21", "--default_schedule= "},
		"extra args":          {"--ftp_addr=x:21", "oops"},
	}
	for name, args := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Load(args, env(nil)); err == nil {
				t.Fatalf("Load(%s) succeeded, want error", strings.Join(args, " "))
			}
		})
	}
}
