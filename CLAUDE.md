# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Status

Implemented. Single-binary Kubernetes backup service (Go port of the Airflow DAG
`ml-orchestrator/dags/system_backup_dag.py` + `dags/common/backup/*.py`), with a per-pod cron
schedule taken from the `oap/backup-schedule` annotation. Uses `k8s.io/client-go`,
`github.com/robfig/cron/v3`, `github.com/jlaffaye/ftp`, `github.com/minio/minio-go/v7`,
`prometheus/client_golang`, stdlib `flag`/`log/slog`.

## Architecture

- `main.go` — flag parsing, logger, k8s client (in-cluster, kubeconfig fallback), storage factory, orphan helper-pod cleanup, HTTP server (`/metrics`, `/healthz`, `/targets`, `POST /run`), signal-driven graceful shutdown.
- `internal/config` — `Load(args, getenv)`: all flags + validation; secrets only from env (`FTP_PASSWORD`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`).
- `internal/annotations` — `oap/backup-*` constants, pure `Parse(pod, defaultSchedule) (Target, ok, err)`, `ParseSchedule` (5-field cron + descriptors + `CRON_TZ=`), `Target` naming helpers (`Key`, `Gzip`, `Suffix`, `FileName`).
- `internal/scheduler` — `Manager`: `Reconcile` lists pods every `--refresh` and keeps one cron entry per `<ns>/<pod>` (add/replace/remove, warn-once on invalid annotations); per-target "already running" guard + global semaphore; `Trigger` for `/run`; graceful `shutdown` (stop cron, wait `ShutdownTimeout`, cancel).
- `internal/backup` — `Runner.Run`: retries of `attempt` (helper pod → `Backup` → delete helper), then `Prune`. `Backup` execs `sh -c <cmd>`, gzips stdout to a temp file unless ext ends in `.gz`, uploads, logs stderr lines. `Prune` deletes non-suffix files, then oldest beyond keep-last.
- `internal/kube` — `Executor`/`RemoteExecutor` (pods/exec, WebSocket with SPDY fallback, `*ExitError` on non-zero exit); `BuildHelperPod` (pure) + `Helpers` create/wait/delete/`CleanupOrphans` (label `app.kubernetes.io/managed-by=oap-backup`).
- `internal/storage` — `Storage` interface (`Upload`, `List`, `Delete`); `FTP` (connection per op, ctx cancel closes all conns, recursive mkdir, NLST errors ⇒ empty list), `S3` (minio-go, keys `<prefix>/<folder>/<file>`).
- `internal/metrics` — dedicated registry, per-backup series labelled `{namespace, folder}`.
- `deploy/` — RBAC, Deployment (1 replica, Recreate), Secret example, annotated pod examples.

## Commands

Go is at `C:\Users\nofate\sdk\go1.26.5\bin` (not on PATH).

- Build: `go build ./...`
- Test: `go test ./...`
- Test single package / test: `go test ./internal/scheduler -run TestReconcile`
- Race: `go test -race ./...` (needs cgo; run in `golang:1.26` container on Windows)
- Storage integration tests: `go test -tags integration ./internal/storage/` (env vars in README "Development")
- Format: `gofmt -l .`
- Vet: `go vet ./...`
- Docker build: `docker build -t oap-backup .`

## Purpose

See README.md for annotations, schedule syntax, storage layout, flags, metrics, RBAC/Deployment and
migration from the Airflow DAG.
