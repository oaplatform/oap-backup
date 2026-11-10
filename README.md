# oap-backup

Kubernetes service that backs up pods which opt in through `oap/backup-*`
annotations. Each pod sets its **own schedule** (cron) in an annotation. On
that schedule oap-backup runs the pod's backup command with `kubectl exec`
semantics. The command runs either in the pod itself or in a short-lived
helper pod that mounts the same volumes. oap-backup gzips the output if it is
not compressed yet, uploads it to **FTP** or **S3-compatible** storage, and
keeps only the newest *N* files.

It replaces the Airflow DAG `system_backup` (`ml-orchestrator/dags/system_backup_dag.py`),
keeping its logic and lifting the fixed `@hourly` / `@daily` limitation. See
[Migration from the Airflow DAG](#migration-from-the-airflow-dag).

## How it works

oap-backup runs as a single-replica Deployment.

1. **Discovery.** Every `--refresh` (60s) it lists pods in all namespaces (or
   only `--namespace`). It considers pods that have the `oap/backup-command`
   annotation, are in phase `Running` and are not being deleted.
2. **Scheduling.** Each valid pod gets one cron entry with its own
   `oap/backup-schedule`. When the pod's annotations change, its entry is
   replaced. When the pod disappears, or its annotations are removed or become
   invalid, the entry is removed. All of this happens without a restart.
3. **Backup run** (per pod, when its schedule fires):
   1. *prebackup*: if `oap/backup-external-container-image` is set, oap-backup
      creates the helper pod `<pod>-backup-helper` (see [Helper pods](#helper-pods))
      and waits until it is `Running` (`--helper_timeout`).
   2. *backup*: runs `sh -c "<oap/backup-command>"` in the target container.
      stdout is the backup stream and stderr is logged line by line. stdout is
      written to a temp file in `--tmp_dir`, gzip-compressed unless
      `oap/backup-file-extension` already ends in `.gz`. A non-zero exit code
      fails the attempt. The file is uploaded as
      `<storage root>/<oap/backup-file-folder>/backup-<YYYYMMDDHHMMSS><ext>[.gz]`
      (UTC timestamp). The temp file is always deleted. Each attempt is limited
      by `--backup_timeout`. A failed attempt is retried `--retries` times,
      `--retry_delay` apart.
   3. *postbackup*: the helper pod is always deleted, including after a failure
      or a timeout.
   4. *prune*: runs after every run, even a failed one, as in the DAG. See
      [Retention](#retention).
4. **Concurrency.** At most `--max_concurrent` backups run at once. A pod
   whose previous backup is still running is skipped for that tick, with a
   warning.

On startup, helper pods left over from a previous process (label
`app.kubernetes.io/managed-by=oap-backup`) are deleted. On `SIGTERM`, no new
backups start. Running backups get `--shutdown_timeout` to finish, then they
are cancelled and their helper pods deleted.

## Annotations

Set on the **Pod**. With a Deployment or StatefulSet, put them in
`spec.template.metadata.annotations`.

| Annotation | Required | Description |
|---|---|---|
| `oap/backup-command` | **yes** | Shell command whose **stdout** is the backup, run as `sh -c "<command>"`. Its presence opts the pod in. |
| `oap/backup-schedule` | no | The pod's own schedule (see [Schedule syntax](#schedule-syntax)). Default: `--default_schedule` (`@daily`). |
| `oap/backup-file-folder` | **yes** | Folder under the storage root, e.g. `db/postgres`. Leading/trailing `/` are ignored; `.` and `..` segments are rejected. |
| `oap/backup-file-extension` | **yes** | File extension, e.g. `.sql`, `.archive.gz`. If it ends in `.gz`, the output is stored as is. Otherwise `.gz` is appended and the stream is gzipped. |
| `oap/backup-retention-keep-last` | **yes** | Integer ≥ 1: how many newest backups to keep in the folder. |
| `oap/backup-container` | no | Container to exec into (and whose volume mounts a helper pod copies). Must exist in the pod spec. Default: the first container. |
| `oap/backup-external-container-image` | no | Run the command in a helper pod with this image instead of in the pod itself. |

If a pod's annotations are invalid (missing required key, bad cron, unknown
container, ...), oap-backup logs a warning naming the pod and the problem, and
skips that pod. The warning is logged once and repeated only if the error
changes. Other pods are not affected. `oap_backup_invalid_targets` counts such
pods.

### Schedule syntax

[robfig/cron](https://pkg.go.dev/github.com/robfig/cron/v3) standard syntax:

| Value | Meaning |
|---|---|
| `30 1 * * *` | 5 fields: minute hour day-of-month month day-of-week. Every day at 01:30. |
| `0 */6 * * *` | every 6 hours, on the hour |
| `*/15 * * * *` | every 15 minutes |
| `0 3 * * 1-5` | 03:00 on weekdays |
| `@hourly`, `@daily` (= `@midnight`), `@weekly`, `@monthly`, `@yearly` | 00 min / 00:00 / Sunday 00:00 / 1st 00:00 / Jan 1st 00:00 |
| `@every 6h`, `@every 90m` | fixed interval, counted from when the entry was (re)scheduled |
| `CRON_TZ=Europe/Kyiv 15 3 * * *` | 03:15 Kyiv time (prefix works with any of the above) |

Schedules without a `CRON_TZ=` (or `TZ=`) prefix use `--timezone` (default
`UTC`). Time zone data is built into the binary.

> `@every` restarts its interval whenever the entry is rescheduled, which
> happens when oap-backup restarts or the pod is recreated. For backups that
> must run at a fixed time of day, use a cron expression.

### Example

```yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: postgres
  namespace: db
spec:
  template:
    metadata:
      annotations:
        oap/backup-command: "pg_dumpall -U postgres"
        oap/backup-schedule: "0 */6 * * *"
        oap/backup-file-folder: "db/postgres"
        oap/backup-file-extension: ".sql"
        oap/backup-retention-keep-last: "8"
        oap/backup-container: "postgres"
    # ...
```

More examples are in [`deploy/examples/`](deploy/examples).

Typical commands:

| Database | `oap/backup-command` | `oap/backup-file-extension` |
|---|---|---|
| PostgreSQL | `pg_dumpall -U postgres` | `.sql` |
| MySQL / MariaDB | `mysqldump -uroot -p"$MYSQL_ROOT_PASSWORD" --all-databases --single-transaction` | `.sql` |
| MongoDB | `mongodump --archive --gzip` | `.archive.gz` |
| Redis | `redis-cli --rdb /tmp/dump.rdb >/dev/null && cat /tmp/dump.rdb` | `.rdb` |
| Files | `tar -czf - -C /data .` | `.tar.gz` |

The command runs through `sh -c` in the target container, so the container's
environment variables (e.g. passwords) are available, and the image must
contain `sh`.

## Helper pods

With `oap/backup-external-container-image`, the backup runs in a pod named
`<pod>-backup-helper` (in the same namespace). The helper pod:

- runs the given image with `sleep infinity` and `restartPolicy: Never`;
- has the same `volumes` and, for the target container, the same
  `volumeMounts` (same paths) as the source pod;
- is pinned to the source pod's node (`nodeName`) and copies its
  `tolerations`, `imagePullSecrets` and pod `securityContext`. ReadWriteOnce
  PVCs, local PVs and `hostPath` volumes therefore resolve to the same data;
- reuses the source pod's existing PVC for generic `ephemeral` volumes
  (`<pod>-<volume>`), so no new PVC is provisioned;
- gets no service account token (`automountServiceAccountToken: false`);
- is labelled `app.kubernetes.io/managed-by=oap-backup`;
- is deleted immediately after every attempt, and replaced if a stale one
  exists.

Use a helper pod when the application image has no dump/archive tools.
`emptyDir` volumes are **not** shared: the helper pod gets a new, empty one.

## Storage

Select it with `--storage=ftp` or `--storage=s3`. Files are stored as
`<root>/<oap/backup-file-folder>/backup-<YYYYMMDDHHMMSS><suffix>`.

- **FTP**: the root is `--ftp_folder` (default `/`). The path is absolute on
  the server, so on servers that do not chroot users, point it at a writable
  directory. Missing directories are created. Each operation uses its own
  connection (passive mode). `--ftp_tls` enables explicit TLS (`AUTH TLS`),
  with TLS session reuse for data connections. The password is read from
  `$FTP_PASSWORD`.
- **S3**: any S3-compatible service (AWS S3, MinIO, Ceph RGW, SeaweedFS,
  Hetzner, Wasabi, ...). Object keys are `<s3_prefix>/<folder>/<file>`. The
  bucket must already exist. Keys are read from `$S3_ACCESS_KEY` /
  `$S3_SECRET_KEY`. If both are empty, the standard AWS env vars or the
  instance/IRSA role are used.

### Retention

After each run, for `<root>/<folder>`, the same way as the DAG's pruner:

1. every file whose name does **not** end with the target's suffix
   (`<ext>.gz`, or `<ext>` if it already ends in `.gz`) is deleted;
2. the remaining files are sorted by name (the fixed-width timestamp makes
   this chronological) and the oldest are deleted until
   `oap/backup-retention-keep-last` remain.

A missing folder is not an error. **Give every pod its own folder** and keep
nothing else in it: foreign files there are deleted.

## Flags

| Flag | Default | Description |
|---|---|---|
| `--refresh` | `60s` | How often pods are re-listed and schedules reconciled. |
| `--namespace` | *(all)* | Only discover pods in this namespace. |
| `--default_schedule` | `@daily` | Schedule for pods without `oap/backup-schedule`. |
| `--timezone` | `UTC` | Default time zone of schedules (IANA name). |
| `--max_concurrent` | `2` | Maximum number of backups running at the same time. |
| `--retries` | `2` | Retries of a failed backup. |
| `--retry_delay` | `5m` | Delay between retries. |
| `--backup_timeout` | `1h` | Timeout of one attempt (helper pod + exec + upload). Also bounds pruning. |
| `--helper_timeout` | `2m` | How long to wait for a helper pod to become `Running`. |
| `--tmp_dir` | `/tmp` | Where backups are staged before upload. Needs room for `--max_concurrent` compressed dumps. |
| `--shutdown_timeout` | `30s` | On `SIGTERM`, how long running backups may finish before they are cancelled. Keep it below `terminationGracePeriodSeconds`. |
| `--storage` | `ftp` | `ftp` or `s3`. |
| `--ftp_addr` | — | FTP `host:port` (required for ftp). |
| `--ftp_user` | `anonymous` | FTP user. |
| `--ftp_folder` | `/` | FTP root folder for backups. |
| `--ftp_tls` | `false` | Explicit FTPS (`AUTH TLS`). |
| `--ftp_timeout` | `30s` | FTP dial timeout. |
| `--s3_endpoint` | — | S3 endpoint `host[:port]` (required for s3), e.g. `s3.eu-central-1.amazonaws.com`. |
| `--s3_bucket` | — | Bucket (required for s3). |
| `--s3_prefix` | — | Key prefix (root folder). |
| `--s3_region` | — | Region (optional). |
| `--s3_insecure` | `false` | Plain HTTP instead of HTTPS. |
| `--metrics_port` | `9090` | Port of the HTTP server. |
| `--loglevel` | `info` | `debug`, `info`, `warn`, `error`. |

| Env var | Used for |
|---|---|
| `FTP_PASSWORD` | FTP password |
| `S3_ACCESS_KEY`, `S3_SECRET_KEY` | S3 static credentials |

Secrets are read only from the environment, never from flags.

## HTTP endpoints

Served on `--metrics_port` (9090):

| Endpoint | Description |
|---|---|
| `GET /metrics` | Prometheus metrics. |
| `GET /healthz` | Liveness/readiness, returns `ok`. |
| `GET /targets` | JSON list of scheduled pods: parsed annotations, `next_run`, `running`. |
| `POST /run?target=<namespace>/<pod>` | Start a backup of one pod now (replaces the DAG's `force` param). Returns `202 {"started": [...]}`, or `404` if the pod is not scheduled. |
| `POST /run` | Start a backup of **every** scheduled pod now. |

```sh
kubectl -n oap-backup port-forward svc/oap-backup 9090:9090
curl -s localhost:9090/targets
curl -s -X POST 'localhost:9090/run?target=db/postgres-0'
```

`/run` has no authentication. Do not expose the service outside the cluster.

## Metrics

Per-backup series are labelled `{namespace, folder}`. The folder stays the
same when a pod is recreated under a new name.

| Metric | Type | Description |
|---|---|---|
| `oap_backup_runs_total{namespace,folder,result}` | counter | Runs (after retries), `result` = `success` / `failure`. |
| `oap_backup_last_success_timestamp_seconds{namespace,folder}` | gauge | Unix time of the last successful backup. |
| `oap_backup_last_size_bytes{namespace,folder}` | gauge | Size of the last uploaded file. |
| `oap_backup_last_duration_seconds{namespace,folder}` | gauge | Duration of the last run, including retries. |
| `oap_backup_pruned_files_total{namespace,folder}` | counter | Files deleted by retention. |
| `oap_backup_prune_failures_total{namespace,folder}` | counter | Failed prune runs. |
| `oap_backup_targets` | gauge | Scheduled pods. |
| `oap_backup_invalid_targets` | gauge | Opted-in pods skipped due to invalid annotations. |

Example alerts:

```yaml
- alert: BackupStale
  expr: time() - oap_backup_last_success_timestamp_seconds > 26 * 3600
  annotations:
    summary: "No successful backup of {{ $labels.namespace }}/{{ $labels.folder }} for 26h"
- alert: BackupFailing
  expr: increase(oap_backup_runs_total{result="failure"}[6h]) > 0
- alert: BackupInvalidAnnotations
  expr: oap_backup_invalid_targets > 0
```

## Kubernetes

Manifests are in [`deploy/`](deploy):

```sh
kubectl apply -f deploy/rbac.yaml            # Namespace, ServiceAccount, ClusterRole(+Binding)
cp deploy/secret.example.yaml secret.yaml    # fill in FTP_PASSWORD / S3 keys
kubectl apply -f secret.yaml
kubectl apply -f deploy/deployment.yaml      # edit image and storage args first
```

Required permissions (ClusterRole, or a Role with `--namespace`):

| Resource | Verbs | Why |
|---|---|---|
| `pods` | `get`, `list`, `create`, `delete` | discovery, helper pods |
| `pods/exec` | `get`, `create` | run the backup command (WebSocket / SPDY) |

Deployment notes:

- **`replicas: 1` and `strategy: Recreate`.** There is no leader election, so
  every replica would run every backup.
- Mount an `emptyDir` at `/tmp` (`--tmp_dir`) sized for the largest
  compressed dump × `--max_concurrent`. The image runs as UID 65532 and works
  with `readOnlyRootFilesystem: true`.
- Set `terminationGracePeriodSeconds` above `--shutdown_timeout`.

## Migration from the Airflow DAG

| DAG (`lightad.io/*`) | oap-backup (`oap/*`) |
|---|---|
| `lightad.io/backup-command` | `oap/backup-command` |
| `lightad.io/schedule` (`@hourly` / `@daily` only) | `oap/backup-schedule` (any cron, optional) |
| `lightad.io/file-folder` | `oap/backup-file-folder` |
| `lightad.io/file-extension` | `oap/backup-file-extension` |
| `lightad.io/retention-keep-last` | `oap/backup-retention-keep-last` |
| `lightad.io/container` | `oap/backup-container` |
| `lightad.io/external-container-image` | `oap/backup-external-container-image` |
| Airflow connection `backup` (ftp) + extra `backup_folder` | `--storage=ftp --ftp_addr --ftp_user --ftp_folder` + `$FTP_PASSWORD` |
| DAG param `force=true` | `POST /run` |

Behaviour differences:

- In the DAG, `@daily` meant "the hourly run between 01:30 and 02:30 UTC". In
  oap-backup, `@daily` means 00:00 in `--timezone`. Use `30 1 * * *` to keep
  the old time.
- An invalid pod is skipped with a warning. In the DAG it failed the whole run.
- Helper pods are pinned to the source pod's node and copy its tolerations,
  pull secrets and security context.
- `retention-keep-last` must be ≥ 1.

File names and storage layout are unchanged, so existing backups are pruned
as before.

## Limitations

- One replica only (no leader election).
- Pods are identified by `<namespace>/<pod>`. If several pods share a
  `file-folder` (e.g. annotated StatefulSet replicas), each one backs up and
  prunes the same folder. Annotate one pod per folder.
- `emptyDir` content is not visible to helper pods.
- The backup is staged on local disk before upload. It is not streamed.

## Docker

```sh
docker build -t oap-backup .
```

Outside a cluster, oap-backup uses `$KUBECONFIG` / `~/.kube/config`, which is
handy for local runs:

```sh
FTP_PASSWORD=... go run . --ftp_addr=localhost:21 --ftp_user=backup --namespace=dev --loglevel=debug
```

## Logging

Structured logs (`log/slog`, text) go to stderr. Every backup line carries
`target=<namespace>/<pod>` and `folder=...`.

- `info`: startup configuration, schedule changes (with `next_run`), backup
  start and finish (file, size, location, duration), stderr of the backup
  command, helper pod lifecycle, pruned files.
- `warn`: invalid annotations, retries, skipped runs (previous run still
  going), unknown `--loglevel`.
- `error`: failed backups (after retries), failed pruning, pod listing
  failures, HTTP server errors.

## Development

```sh
go build ./...
go vet ./...
gofmt -l .
go test ./...
go test -race ./...            # needs cgo (e.g. in the golang:1.26 container)
```

Storage integration tests (build tag `integration`) run against real servers:

```sh
docker run -d --name ftp -p 2121:21 -p 21000-21010:21000-21010 -e USERS="one|1234" -e ADDRESS=localhost delfer/alpine-ftp-server
docker run -d --name s3 -p 8333:8333 chrislusf/seaweedfs server -s3 -dir=/data

OAP_TEST_FTP_ADDR=localhost:2121 OAP_TEST_FTP_USER=one OAP_TEST_FTP_PASSWORD=1234 OAP_TEST_FTP_FOLDER=/ftp/one/backups \
OAP_TEST_S3_ENDPOINT=localhost:8333 \
go test -tags integration ./internal/storage/
```
