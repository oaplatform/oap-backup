// Package annotations defines the oap/backup-* pod annotations and parses
// them into a backup Target.
package annotations

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/robfig/cron/v3"
	corev1 "k8s.io/api/core/v1"
)

// Pod annotations understood by oap-backup. The presence of Command opts a
// pod in; all other keys are validated only for opted-in pods.
const (
	Command           = "oap/backup-command"
	Schedule          = "oap/backup-schedule"
	FileFolder        = "oap/backup-file-folder"
	FileExtension     = "oap/backup-file-extension"
	RetentionKeepLast = "oap/backup-retention-keep-last"
	Container         = "oap/backup-container"
	ExternalImage     = "oap/backup-external-container-image"
)

// FilePrefix is the common prefix of every backup file name.
const FilePrefix = "backup-"

// timestampLayout is the UTC timestamp embedded in backup file names. It is
// fixed-width, so lexicographic order of file names is chronological.
const timestampLayout = "20060102150405"

// scheduleParser accepts standard 5-field cron expressions, descriptors
// (@hourly, @daily, @weekly, @monthly, @yearly, @every <duration>) and an
// optional CRON_TZ=<zone> / TZ=<zone> prefix.
var scheduleParser = cron.NewParser(cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)

// ParseSchedule parses a schedule annotation value. Without a CRON_TZ prefix
// the schedule runs in the scheduler's default location.
func ParseSchedule(spec string) (cron.Schedule, error) {
	return scheduleParser.Parse(spec)
}

// Target is one pod to back up, as described by its annotations.
type Target struct {
	Namespace         string
	Pod               string
	Container         string
	Command           string
	Schedule          string
	FileFolder        string
	FileExtension     string
	RetentionKeepLast int
	// ExternalImage, when set, makes the backup run in a helper pod using
	// this image and the source container's volumes.
	ExternalImage string
}

// Key identifies the target: "<namespace>/<pod>".
func (t Target) Key() string {
	return t.Namespace + "/" + t.Pod
}

// Gzip reports whether the command output must be gzip-compressed by
// oap-backup. Extensions already ending in ".gz" mean the command's own
// output is compressed (e.g. mongodump --gzip).
func (t Target) Gzip() bool {
	return !strings.HasSuffix(t.FileExtension, ".gz")
}

// Suffix is the file name suffix of every backup of this target.
func (t Target) Suffix() string {
	if t.Gzip() {
		return t.FileExtension + ".gz"
	}
	return t.FileExtension
}

// FileName returns "backup-<YYYYMMDDHHMMSS><suffix>" for now (in UTC).
func (t Target) FileName(now time.Time) string {
	return FilePrefix + now.UTC().Format(timestampLayout) + t.Suffix()
}

// Parse reads the oap/backup-* annotations of pod. ok is false (and err nil)
// when the pod has no oap/backup-command annotation. A non-nil error names
// the offending annotation; the pod must then be skipped.
func Parse(pod *corev1.Pod, defaultSchedule string) (target Target, ok bool, err error) {
	ann := pod.Annotations
	command, ok := ann[Command]
	if !ok {
		return Target{}, false, nil
	}
	if strings.TrimSpace(command) == "" {
		return Target{}, true, fmt.Errorf("annotation %s must not be empty", Command)
	}

	schedule := strings.TrimSpace(ann[Schedule])
	if schedule == "" {
		schedule = defaultSchedule
	}
	if _, err := ParseSchedule(schedule); err != nil {
		return Target{}, true, fmt.Errorf("annotation %s: invalid schedule %q: %w", Schedule, schedule, err)
	}

	folder, err := cleanFolder(ann[FileFolder])
	if err != nil {
		return Target{}, true, err
	}

	ext := strings.TrimSpace(ann[FileExtension])
	if ext == "" {
		return Target{}, true, fmt.Errorf("missing required annotation %s", FileExtension)
	}
	if strings.ContainsAny(ext, "/\\") {
		return Target{}, true, fmt.Errorf("annotation %s: %q must not contain path separators", FileExtension, ext)
	}

	keepRaw, present := ann[RetentionKeepLast]
	if !present || strings.TrimSpace(keepRaw) == "" {
		return Target{}, true, fmt.Errorf("missing required annotation %s", RetentionKeepLast)
	}
	keep, err := strconv.Atoi(strings.TrimSpace(keepRaw))
	if err != nil || keep < 1 {
		return Target{}, true, fmt.Errorf("annotation %s: %q must be an integer >= 1", RetentionKeepLast, keepRaw)
	}

	container, err := resolveContainer(pod, strings.TrimSpace(ann[Container]))
	if err != nil {
		return Target{}, true, err
	}

	return Target{
		Namespace:         pod.Namespace,
		Pod:               pod.Name,
		Container:         container,
		Command:           command,
		Schedule:          schedule,
		FileFolder:        folder,
		FileExtension:     ext,
		RetentionKeepLast: keep,
		ExternalImage:     strings.TrimSpace(ann[ExternalImage]),
	}, true, nil
}

// cleanFolder trims surrounding slashes and rejects empty values and "." /
// ".." segments, so a folder can never escape the storage root.
func cleanFolder(raw string) (string, error) {
	folder := strings.Trim(strings.TrimSpace(raw), "/")
	if folder == "" {
		return "", fmt.Errorf("missing required annotation %s", FileFolder)
	}
	for _, seg := range strings.Split(folder, "/") {
		if seg == "" || seg == "." || seg == ".." {
			return "", fmt.Errorf("annotation %s: invalid folder %q", FileFolder, raw)
		}
	}
	return folder, nil
}

// resolveContainer returns the annotated container (which must exist in the
// pod spec) or, without the annotation, the pod's first container.
func resolveContainer(pod *corev1.Pod, name string) (string, error) {
	if name == "" {
		if len(pod.Spec.Containers) == 0 {
			return "", fmt.Errorf("pod has no containers, cannot resolve exec target")
		}
		return pod.Spec.Containers[0].Name, nil
	}
	for _, c := range pod.Spec.Containers {
		if c.Name == name {
			return name, nil
		}
	}
	return "", fmt.Errorf("annotation %s: container %q not found in pod spec", Container, name)
}
