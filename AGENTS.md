# AGENTS.md

This file provides guidance to coding agents working with code in this repository.

## Status

Implemented. Single-binary Kubernetes backup service (Go port of the Airflow DAG
`ml-orchestrator/dags/system_backup_dag.py` + `dags/common/backup/*.py`), with a per-pod cron
schedule taken from the `oap/backup-schedule` annotation. Uses `k8s.io/client-go`,
`github.com/robfig/cron/v3`, `github.com/jlaffaye/ftp`, `github.com/minio/minio-go/v7`,
`github.com/klauspost/compress/zstd`,
`prometheus/client_golang`, stdlib `flag`/`log/slog`.

## Architecture

- `main.go` — dispatch: no arguments → help on stderr, exit 2; `help`/`-h`/`--help` → `writeHelp` (CLI commands + service flags from `config.Usage`); `list`/`describe`/`run` → `internal/cli`; anything else → `serve()`: flag parsing, logger, k8s client (in-cluster, kubeconfig fallback), storage factory, orphan helper-pod cleanup, HTTP server, signal-driven graceful shutdown.
- `internal/api` — HTTP API (`Server.Handler`): `/metrics`, `/healthz`, `GET /targets` (scheduled + invalid), `GET /targets/{namespace}/{pod}` (status + storage content via `backup.Runner.Inspect`), `POST /run` (404 unknown, 409 already running); response types shared with the CLI.
- `internal/cli` — `App.Run(args)`: `list`, `describe <ns>/<pod>`, `run <ns>/<pod>|--all [--wait]`; thin HTTP client of the service (`--server` / `$OAP_BACKUP_SERVER` / `http://localhost:9090`), table or JSON output, exit codes 0/1/2. Tests run it against the real `api.Server` + `scheduler.Manager` with fakes.
- `internal/config` — `Load(args, getenv)`: all flags + validation; secrets only from env (`FTP_PASSWORD`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`).
- `internal/annotations` — `oap/backup-*` constants, pure `Parse(pod, defaultSchedule) (Target, ok, err)`, `ParseSchedule` (5-field cron + descriptors + `CRON_TZ=`), `Target` naming helpers (`Key`, `Precompressed`, `Codec`, `Suffix`, `Suffixes`, `IsBackupFile`, `FileName`, `Timestamp`, `PartFolder`, `PartCommand`), `ParsePartList` (output of `oap/backup-parts-command`: `<part>=<YYYYMMDDHHMMSS>` / `<part>=SKIP`).
- `internal/scheduler` — `Manager`: `Reconcile` lists pods every `--refresh` and keeps one cron entry per `<ns>/<pod>` (add/replace/remove, warn-once on invalid annotations); per-target "already running" guard + global semaphore; `Trigger` for `/run`; in-memory `LastRun` per target (`Target`, `Targets`), `Invalid` (skipped pods); graceful `shutdown` (stop cron, wait `ShutdownTimeout`, cancel).
- `internal/backup` — `Runner.Run`: retries of `attempt` (helper pod → `Backup` → delete helper), then `Prune`. `Backup` execs `sh -c <cmd>`, compresses stdout to a temp file (`compress.go`: gzip or zstd per `oap/backup-compression`, level per `oap/backup-compression-level`) unless ext ends in `.gz`/`.zst`, uploads, logs stderr lines. `Prune` deletes files without a backup suffix (`<ext>.gz` and `<ext>.zst` both count, so switching codec keeps old backups), then oldest beyond keep-last. Partitioned targets (`oap/backup-parts: "true"`, `parts.go`): `partsAttempt` reads part folders from storage → helper pod with `PART_COUNT` / `PART<X>_ID` / `PART<X>_MT` env → parts command → `oap/backup-command` with `{PART_ID}` replaced, once per changed part, into `<folder>/<part>/backup-<reported ts>`; `pruneParts` prunes every part folder and removes empty ones. `Inspect` (`inspect.go`) lists the stored files of a target read-only.
- `internal/kube` — `Executor`/`RemoteExecutor` (pods/exec, WebSocket with SPDY fallback, `*ExitError` on non-zero exit); `BuildHelperPod` (pure; copies the source container's volumes/mounts and `env`/`envFrom`, plus optional extra env that wins on a name clash) + `Helpers` create/wait/delete/`CleanupOrphans` (label `app.kubernetes.io/managed-by=oap-backup`).
- `internal/storage` — `Storage` interface (`Upload`, `List`, `ListFiles` (name, size, mtime), `Delete`, `ListDirs`, `RemoveDir`); `FTP` (connection per op, ctx cancel closes all conns, recursive mkdir, NLST/LIST errors ⇒ empty list), `S3` (minio-go, keys `<prefix>/<folder>/<file>`, folders = common prefixes, `RemoveDir` only drops a `<dir>/` placeholder).
- `internal/metrics` — dedicated registry, per-backup series labelled `{namespace, folder}`; part metrics (`oap_backup_parts{state}`, `oap_backup_part_backups_total{result}`, `oap_backup_parts_stored`, `oap_backup_part_folders_pruned_total`) are per target, never per part id.
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
