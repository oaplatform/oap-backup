# oap-backup

Kubernetes service that backs up pods which opt in through `oap/backup-*`
annotations. Each pod sets its **own schedule** (cron) in an annotation. On
that schedule oap-backup runs the pod's backup command with `kubectl exec`
semantics. The command runs either in the pod itself or in a short-lived
helper pod that mounts the same volumes. oap-backup optionally compresses the
output (gzip or zstd), uploads it to **FTP** or **S3-compatible** storage, and
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
      written to a temp file in `--tmp_dir`, compressed if
      `oap/backup-compression` says so (see [Compression](#compression)). A non-zero exit code fails the attempt.
      The file is uploaded as
      `<storage root>/<oap/backup-file-folder>/backup-<YYYYMMDDHHMMSS><ext>[.gz|.zst]`
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
| `oap/backup-file-extension` | **yes** | File extension, e.g. `.sql`, `.archive.gz`. With `oap/backup-compression` `gzip` or `zstd`, `.gz` or `.zst` is appended, unless the extension already ends in `.gz` or `.zst`. |
| `oap/backup-compression` | no | `none`, `gzip` or `zstd`: how oap-backup compresses the output. Default: `none` (stored as is). See [Compression](#compression). |
| `oap/backup-compression-level` | no | Integer: `1`–`9` for gzip, `1`–`22` for zstd. Default: the codec's default level. Not allowed with `none`. |
| `oap/backup-retention-keep-last` | **yes** | Integer ≥ 1: how many newest backups to keep in the folder. |
| `oap/backup-container` | no | Container to exec into (and whose volume mounts a helper pod copies). Must exist in the pod spec. Default: the first container. |
| `oap/backup-external-container-image` | no | Run the command in a helper pod with this image instead of in the pod itself. Required with `oap/backup-parts`. |
| `oap/backup-parts` | no | `true` enables [partitioned backups](#partitioned-backups). Default: `false`. |
| `oap/backup-parts-command` | with parts | Shell command that prints the list of parts. |

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
        oap/backup-compression: "gzip"
        oap/backup-retention-keep-last: "8"
        oap/backup-container: "postgres"
    # ...
```

More examples are in [`deploy/examples/`](deploy/examples).

### Compression

| `oap/backup-compression` | Suffix | `oap/backup-compression-level` | Default level |
|---|---|---|---|
| `none` (default) | — | not allowed | — |
| `gzip` | `.gz` | `1` (fastest) – `9` (smallest) | `6` |
| `zstd` | `.zst` | `1` (fastest) – `22` (smallest) | `3` |

- Without `oap/backup-compression`, or with `none`, the command output is
  stored byte for byte as `backup-<timestamp><ext>`. Plain-text dumps (SQL)
  usually shrink several times with `gzip` or `zstd`, so set one of them
  unless the command already compresses.
- `oap/backup-compression-level` together with `none` makes the pod invalid,
  so that a forgotten codec does not go unnoticed.
- If `oap/backup-file-extension` already ends in `.gz` or `.zst`, the command
  compresses its own output. It is stored as is, and both annotations are
  ignored.
- zstd has four encoder presets, and a level selects the nearest one: `1`–`2`
  fastest, `3`–`6` default, `7`–`10` better, `11`–`22` best. Levels inside one
  group produce the same output.
- You can change the codec at any time. Backups made with another codec,
  or with none, stay in the folder and count towards
  `oap/backup-retention-keep-last` together with the new ones.
- To restore: `gunzip -c backup-….sql.gz` or `zstd -dc backup-….sql.zst`.

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
- has the target container's `env` and `envFrom`, so the command sees the
  same variables (passwords, hosts, ...) as in the pod itself. Values from
  `fieldRef` describe the helper pod, e.g. `metadata.name` is
  `<pod>-backup-helper`. A `resourceFieldRef` to another container is
  dropped;
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

## Partitioned backups

For data that consists of independent parts (table partitions, per-tenant
databases, directories), `oap/backup-parts: "true"` backs up only the parts
that changed since their last backup. Each part has its own folder:

```
<storage root>/<oap/backup-file-folder>/<part>/backup-<YYYYMMDDHHMMSS><ext>[.gz|.zst]
```

This mode always runs in a helper pod, so
`oap/backup-external-container-image` is required. One run:

1. oap-backup reads the part folders from the storage. For each one it takes
   the timestamp of the newest backup file.
2. It creates the helper pod with this state in the container's environment:

   | Variable | Value |
   |---|---|
   | `PART_COUNT` | number of parts on the storage |
   | `PART<X>_ID` | part id, `X` = `0` … `PART_COUNT - 1` |
   | `PART<X>_MT` | timestamp of its newest backup (`YYYYMMDDHHMMSS`, UTC); empty if the folder has no backup |

3. It runs `sh -c "<oap/backup-parts-command>"` in the helper pod. The command
   prints one line per part that **exists now**:

   ```
   <part>=<YYYYMMDDHHMMSS>   # new, or modified after PART<X>_MT: back it up
   <part>=SKIP               # not modified: nothing to do
   ```

   The timestamp is the part's modification time in UTC. Blank lines and `#`
   comments are ignored. A stored part that is not printed no longer exists
   and is not backed up. stderr is logged.
4. For every part with a timestamp, in the printed order, it runs
   `oap/backup-command` in the same helper pod with every `{PART_ID}` replaced
   by the part id. The output is handled like any other backup (see
   [Compression](#compression)) and uploaded as
   `<part>/backup-<that timestamp><ext>[.gz|.zst]`. So the next run gets exactly
   this timestamp back in `PART<X>_MT`.
5. Retention runs for every part folder (see [Retention](#retention)).

Rules:

- `{PART_ID}` is **required** in `oap/backup-command`. Without it the pod is
  invalid and skipped.
- A part id may contain letters, digits, `.`, `_` and `-`, and must start
  with a letter or digit. It is used as a folder name and inside the shell
  command.
- If the parts command fails or prints an invalid line, the attempt fails and
  no part is backed up.
- A failed part does not stop the other parts, but the attempt counts as
  failed and is retried (`--retries`). A retry starts again from step 1 in a
  new helper pod, so parts that were already uploaded come back in
  `PART<X>_MT` and the parts command answers `SKIP` for them.
- `--backup_timeout` applies to steps 1–3 together and to each part backup
  separately.
- The parts of one pod are backed up one after another.

See [`deploy/examples/parts.yaml`](deploy/examples/parts.yaml).

## Storage

Select it with `--storage=ftp` or `--storage=s3`. Files are stored as
`<root>/<oap/backup-file-folder>/backup-<YYYYMMDDHHMMSS><suffix>`, or, for
[partitioned backups](#partitioned-backups), as
`<root>/<oap/backup-file-folder>/<part>/backup-<YYYYMMDDHHMMSS><suffix>`.

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

1. every file whose name does **not** end with a suffix of the target is
   deleted. The suffixes are `<ext>`, `<ext>.gz` and `<ext>.zst`, whichever
   codec is selected now, or only `<ext>` if it already ends in `.gz` or
   `.zst`;
2. the remaining files are sorted by name (the fixed-width timestamp makes
   this chronological) and the oldest are deleted until
   `oap/backup-retention-keep-last` remain.

A missing folder is not an error. **Give every pod its own folder** and keep
nothing else in it: foreign files there are deleted.

For [partitioned backups](#partitioned-backups) the same two steps run for
every part folder `<root>/<folder>/<part>`, including parts that no longer
exist. A part folder that ends up empty is removed. Retention counts files, so
a part that no longer exists keeps its last `oap/backup-retention-keep-last`
backups until you delete them. Files directly in `<root>/<folder>` and
folders whose name is not a valid part id are not touched.

## Flags

These are the flags of the service. `oap-backup --help` prints them too.

| Flag | Default | Description |
|---|---|---|
| `--refresh` | `60s` | How often pods are re-listed and schedules reconciled. |
| `--namespace` | *(all)* | Only discover pods in this namespace. |
| `--default_schedule` | `@daily` | Schedule for pods without `oap/backup-schedule`. |
| `--timezone` | `UTC` | Default time zone of schedules (IANA name). |
| `--max_concurrent` | `2` | Maximum number of backups running at the same time. |
| `--retries` | `2` | Retries of a failed backup. |
| `--retry_delay` | `5m` | Delay between retries. |
| `--backup_timeout` | `1h` | Timeout of one attempt (helper pod + exec + upload). Also bounds pruning. With `oap/backup-parts`: of listing the parts, and of each part backup. |
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

## CLI

The same binary is also a command-line client of the running service. It is
in the image, so the simplest way to use it is `kubectl exec`:

```sh
alias oapb='kubectl -n oap-backup exec deploy/oap-backup -- /oap-backup'

oapb list                          # backups that oap-backup detected
oapb describe db/postgres-0        # everything about one backup
oapb status                        # what is running now
oapb refresh                       # rescan the pods now
oapb run db/postgres-0 --wait      # back up this pod now
```

| Command | Description |
|---|---|
| `list [-o table\|json]` | Every pod with `oap/backup-command`: status (`scheduled` / `running`), mode (`single` / `parts`), schedule, time to the next run, result of the last run, folder. Pods skipped because of invalid annotations are listed below, with the error. |
| `status [--lines <n>] [-o table\|json]` | What oap-backup is doing now: how many pods are scheduled, invalid and being backed up, the next scheduled run, and the pods whose last run failed. For every backup in progress: whether it is running (and for how long) or queued for a free slot (`--max_concurrent`), what triggered it, its settings, the previous run, and the last `--lines` (15) lines of its log. `--lines 0` hides the log. |
| `describe <namespace>/<pod> [-o table\|json]` | The parsed annotations, next run, last run (trigger, start, duration, uploaded files and size, error), and the storage: every file in the backup folder with its backup time and size, newest first. For [partitioned backups](#partitioned-backups): the same for every part folder. Files that the next retention run deletes are marked. |
| `refresh [-o table\|json]` | Rescan the pods now, without waiting for the next periodic scan (`--refresh`, 60s), and print the same list as `list`. Use it right after you changed a pod's `oap/backup-*` annotations. It fails if the Kubernetes API cannot be reached; the current schedules are then kept. |
| `run <namespace>/<pod> [--wait] [--timeout <duration>]` | Start a backup of the pod now. It fails if the pod is not scheduled or its backup is already running. |
| `run --all [--wait] [--timeout <duration>]` | Start a backup of every scheduled pod that is not running one. |
| `help`, `-h`, `--help` | Print the help: the commands, and how to start the service with all its flags. Running the binary with no arguments prints it too, and exits with `2`. |

- `--wait` follows the started backups: it prints the service's log lines
  of these backups as they appear (helper pod, backup command and its
  stderr, uploaded file, retries, pruned files), then each result. If the
  backup stays silent for 30s, it prints that it is still running. With
  several backups (`--all`) every line starts with `[<namespace>/<pod>]`.
  `--timeout` (e.g. `30m`) limits the wait. The backup itself keeps running
  when the wait times out or the CLI is interrupted.
- Without `--wait`, `run` only starts the backup and returns.
- Exit codes: `0` success; `1` the request failed, the backup failed
  (`--wait`) or the wait timed out; `2` wrong usage.
- The CLI talks to the service over HTTP: `--server <url>`, else
  `$OAP_BACKUP_SERVER`, else `http://localhost:9090`. The default works inside
  the oap-backup pod. From your machine, use
  `kubectl -n oap-backup port-forward svc/oap-backup 9090:9090`, or run the
  image: `docker run --rm --network host oap-backup list`.
- "Last run" is kept in memory: it is empty after oap-backup restarts, and
  only covers runs since then. The files on the storage are always read live.
- Without a subcommand, the binary runs the service.

```
$ oapb list
TARGET          STATUS     MODE    SCHEDULE     NEXT RUN  LAST RUN       FOLDER
app/tenants-0   scheduled  parts   0 */6 * * *  in 2h14m  ok 3h45m ago   app/tenants
db/postgres-0   running    single  0 */6 * * *  in 2h14m  failed 6h ago  db/postgres

INVALID (skipped)  ERROR
db/mongo-0         missing required annotation oap/backup-file-folder

$ oapb status
Backups:   2 scheduled, 1 invalid, 1 in progress
Next run:  app/tenants-0 in 2h14m (schedule 0 */6 * * *)

Backup in progress: db/postgres-0
  State:         running for 31s
  Trigger:       manual, at 2026-11-24 12:00:00 UTC
  Mode:          single
  Container:     postgres
  Command:       pg_dumpall -U postgres
  Folder:        db/postgres
  Compression:   zstd (level 19)
  Previous run:  ok, 6h0m ago (schedule, started 2026-11-24 06:00:00 UTC, took 41s, uploaded 1 file, 117.7 MiB)
  Log:
    12:00:00 INFO  backup triggered trigger=manual
    12:00:00 INFO  running backup command folder=db/postgres pod=postgres-0 container=postgres command="pg_dumpall -U postgres" dir=db/postgres file=backup-20261124120000.sql.zst
    12:00:31 INFO  backup command stderr folder=db/postgres pod=postgres-0 line="pg_dumpall: dumping database \"app\""

$ oapb run db/postgres-0 --wait
started backup of db/postgres-0
  12:00:00 INFO  backup triggered trigger=manual
  12:00:00 INFO  running backup command folder=db/postgres pod=postgres-0 container=postgres command="pg_dumpall -U postgres" dir=db/postgres file=backup-20261124120000.sql.zst
  12:00:31 INFO  backup command stderr folder=db/postgres pod=postgres-0 line="pg_dumpall: dumping database \"app\""
  12:00:40 INFO  backup file written folder=db/postgres file=backup-20261124120000.sql.zst size_bytes=124151398
  12:00:42 INFO  backup complete folder=db/postgres file=backup-20261124120000.sql.zst size_bytes=124151398 location=s3://backups/prod/db/postgres/backup-20261124120000.sql.zst duration=42s
  12:00:42 INFO  pruned file folder=db/postgres file=backup-20261122120000.sql.zst reason=retention_keep_last=8
  12:00:42 INFO  prune complete folder=db/postgres deleted=1 kept=8
db/postgres-0: ok in 42s, 1 file, 118.4 MiB uploaded

$ oapb describe db/postgres-0
Target:       db/postgres-0
Status:       scheduled
Mode:         single
Container:    postgres
Command:      pg_dumpall -U postgres
Schedule:     0 */6 * * *
Next run:     2026-11-24 18:00:00 UTC (in 2h14m)
Folder:       db/postgres
Extension:    .sql
Compression:  zstd (level 19)
Keep last:    8
Last run:     ok, 3h45m ago (schedule, started 2026-11-24 12:00:00 UTC, took 42s, uploaded 1 file, 118.4 MiB)
Storage:      s3://backups/prod (endpoint s3.example.com)

db/postgres: 2 backups, 236.1 MiB, newest 2026-11-24 12:00:00 UTC
  FILE                           BACKUP TIME              SIZE       NOTE
  backup-20261124120000.sql.zst  2026-11-24 12:00:00 UTC  118.4 MiB
  backup-20261124060000.sql.zst  2026-11-24 06:00:00 UTC  117.7 MiB
```

## HTTP endpoints

Served on `--metrics_port` (9090):

| Endpoint | Description |
|---|---|
| `GET /metrics` | Prometheus metrics. |
| `GET /healthz` | Liveness/readiness, returns `ok`. |
| `GET /targets` | `{"targets": [...], "invalid": [...]}`. `targets`: scheduled pods with parsed annotations, `next_run`, `running`, `current_run` (trigger, `triggered`, `started`; while a backup is in progress) and `last_run`. `invalid`: pods skipped because of their annotations, with the error. |
| `GET /targets/<namespace>/<pod>` | One scheduled pod, as in `/targets`, plus `storage`: its location and the files of the backup folder (or of every part folder). If the storage cannot be read, `storage.error` is set. `404` if the pod is not scheduled. |
| `POST /run?target=<namespace>/<pod>` | Start a backup of one pod now (replaces the DAG's `force` param). Returns `202 {"started": [...]}`, `404` if the pod is not scheduled, or `409` if its backup is already running. |
| `POST /run` | Start a backup of **every** scheduled pod that is not running one. |
| `GET /targets/<namespace>/<pod>/log?after=<cursor>` | Recent log lines of one pod's backups (kept in memory, the newest 2000 per pod, at the service's `--loglevel`): `{"lines": [{seq, time, level, text}], "next": <cursor>}`. Pass `next` as `after` to get only newer lines. `POST /run` returns the cursor of each started backup in `log_after`. |
| `POST /refresh` | Rescan the pods now and return the same body as `GET /targets`. `503` if the pods could not be listed. |

The [CLI](#cli) is a client of these endpoints. Errors are returned as
`{"error": "..."}`.

```sh
kubectl -n oap-backup port-forward svc/oap-backup 9090:9090
curl -s localhost:9090/targets
curl -s localhost:9090/targets/db/postgres-0
curl -s -X POST 'localhost:9090/run?target=db/postgres-0'
```

The API has no authentication. Do not expose the service outside the cluster.

## Metrics

Per-backup series are labelled `{namespace, folder}`. The folder stays the
same when a pod is recreated under a new name.

| Metric | Type | Description |
|---|---|---|
| `oap_backup_runs_total{namespace,folder,result}` | counter | Runs (after retries), `result` = `success` / `failure`. |
| `oap_backup_last_success_timestamp_seconds{namespace,folder}` | gauge | Unix time of the last successful backup. |
| `oap_backup_last_size_bytes{namespace,folder}` | gauge | Size of the last uploaded file. With `oap/backup-parts`: total size of the parts uploaded by the last successful run (`0` if nothing changed). |
| `oap_backup_last_duration_seconds{namespace,folder}` | gauge | Duration of the last run, including retries. |
| `oap_backup_pruned_files_total{namespace,folder}` | counter | Files deleted by retention. |
| `oap_backup_prune_failures_total{namespace,folder}` | counter | Failed prune runs. |
| `oap_backup_parts{namespace,folder,state}` | gauge | [Partitioned backups](#partitioned-backups): parts by `state`, as of the last parts listing. `changed` = to back up, `unchanged` = `SKIP`, `gone` = on the storage but no longer listed. |
| `oap_backup_part_backups_total{namespace,folder,result}` | counter | Backups of single parts, `result` = `success` / `failure`. Every attempt counts, so a retried part counts again. |
| `oap_backup_parts_stored{namespace,folder}` | gauge | Part folders on the storage after the last retention run. |
| `oap_backup_part_folders_pruned_total{namespace,folder}` | counter | Empty part folders removed by retention. |
| `oap_backup_targets` | gauge | Scheduled pods. |
| `oap_backup_invalid_targets` | gauge | Opted-in pods skipped due to invalid annotations. |

The part metrics are per pod, not per part: a `part` label would create one
series for every part. For a single part, use `oap-backup describe`.

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
- alert: BackupPartsFailing
  expr: increase(oap_backup_part_backups_total{result="failure"}[6h]) > 0
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
| *(always gzip)* | `oap/backup-compression: gzip` |
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

- The DAG always gzipped the output. oap-backup compresses only when asked:
  **add `oap/backup-compression: gzip` to every migrated pod** whose extension
  does not already end in `.gz`. Without it the backups are stored
  uncompressed, as `backup-<timestamp><ext>` instead of
  `backup-<timestamp><ext>.gz`.

With that annotation, file names and storage layout are unchanged, so
existing backups are pruned as before.

## Limitations

- One replica only (no leader election).
- Pods are identified by `<namespace>/<pod>`. If several pods share a
  `file-folder` (e.g. annotated StatefulSet replicas), each one backs up and
  prunes the same folder. Annotate one pod per folder.
- `emptyDir` content is not visible to helper pods.
- The backup is staged on local disk before upload. It is not streamed.
- With `oap/backup-parts`, the stored parts are passed as environment
  variables of the helper pod (three per part plus one). Thousands of parts
  work; hundreds of thousands exceed the size limit of a pod object.

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
