// Package annotations defines the oap/backup-* pod annotations and parses
// them into a backup Target.
package annotations

import (
	"fmt"
	"regexp"
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
	Parts             = "oap/backup-parts"
	PartsCommand      = "oap/backup-parts-command"
	Compression       = "oap/backup-compression"
	CompressionLevel  = "oap/backup-compression-level"
)

// Compression codecs (values of the Compression annotation). None, the
// default, stores the command output as is.
const (
	None = "none"
	Gzip = "gzip"
	Zstd = "zstd"
)

// codecs describes every supported codec: the file suffix it appends and the
// compression levels it accepts (none for None).
var codecs = map[string]struct {
	suffix             string
	minLevel, maxLevel int
}{
	None: {},
	Gzip: {".gz", 1, 9},
	Zstd: {".zst", 1, 22},
}

// codecSuffixes lists the suffixes of the compressing codecs, in a fixed
// order.
var codecSuffixes = []string{codecs[Gzip].suffix, codecs[Zstd].suffix}

// PartPlaceholder is replaced by the part id in the backup command of a
// partitioned target. It is required there.
const PartPlaceholder = "{PART_ID}"

// PartSkip is the value a parts command prints for an unchanged part.
const PartSkip = "SKIP"

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
	// Parts enables partitioned backups: PartsCommand lists the parts and
	// Command (which contains PartPlaceholder) runs once per changed part.
	Parts        bool
	PartsCommand string
	// Compression is the codec oap-backup compresses the command output
	// with: None (also when empty), Gzip or Zstd. It is not used when the
	// extension says the output is already compressed.
	Compression string
	// CompressionLevel is the codec's level; 0 selects the codec's default.
	CompressionLevel int
}

// Key identifies the target: "<namespace>/<pod>".
func (t Target) Key() string {
	return t.Namespace + "/" + t.Pod
}

// Precompressed reports whether the command's own output is compressed: the
// extension ends in ".gz" or ".zst" (e.g. mongodump --gzip). Such output is
// stored as is.
func (t Target) Precompressed() bool {
	for _, suffix := range codecSuffixes {
		if strings.HasSuffix(t.FileExtension, suffix) {
			return true
		}
	}
	return false
}

// Codec is the codec oap-backup must compress the command output with: Gzip
// or Zstd, or "" when the output is stored as is (Compression is None or
// empty, or the output is already compressed).
func (t Target) Codec() string {
	if t.Precompressed() || t.Compression == None {
		return ""
	}
	return t.Compression
}

// Suffix is the file name suffix of every new backup of this target.
func (t Target) Suffix() string {
	return t.FileExtension + codecs[t.Codec()].suffix
}

// Suffixes lists the suffixes of this target's backup files under any codec:
// the plain extension (None) and one per compressing codec. Besides Suffix
// it covers backups made before the Compression annotation was changed, so
// they stay subject to retention instead of being foreign.
func (t Target) Suffixes() []string {
	out := []string{t.FileExtension}
	if t.Precompressed() {
		return out
	}
	for _, suffix := range codecSuffixes {
		out = append(out, t.FileExtension+suffix)
	}
	return out
}

// IsBackupFile reports whether fileName ends with one of Suffixes.
func (t Target) IsBackupFile(fileName string) bool {
	for _, suffix := range t.Suffixes() {
		if strings.HasSuffix(fileName, suffix) {
			return true
		}
	}
	return false
}

// FileName returns "backup-<YYYYMMDDHHMMSS><suffix>" for now (in UTC).
func (t Target) FileName(now time.Time) string {
	return FilePrefix + FormatTimestamp(now) + t.Suffix()
}

// Timestamp extracts the time from a backup file name of this target. ok is
// false for any name that is not "backup-<YYYYMMDDHHMMSS><suffix>", where
// suffix is one of Suffixes.
func (t Target) Timestamp(fileName string) (ts time.Time, ok bool) {
	raw, found := strings.CutPrefix(fileName, FilePrefix)
	if !found {
		return time.Time{}, false
	}
	for _, suffix := range t.Suffixes() {
		if stamp, found := strings.CutSuffix(raw, suffix); found {
			if ts, err := ParseTimestamp(stamp); err == nil {
				return ts, true
			}
		}
	}
	return time.Time{}, false
}

// PartFolder is the storage folder of one part: "<file-folder>/<id>".
func (t Target) PartFolder(id string) string {
	return t.FileFolder + "/" + id
}

// PartCommand is the backup command for one part: Command with every
// PartPlaceholder replaced by id.
func (t Target) PartCommand(id string) string {
	return strings.ReplaceAll(t.Command, PartPlaceholder, id)
}

// FormatTimestamp renders ts as the UTC "YYYYMMDDHHMMSS" used in backup file
// names and PART<X>_MT variables.
func FormatTimestamp(ts time.Time) string {
	return ts.UTC().Format(timestampLayout)
}

// ParseTimestamp parses a UTC "YYYYMMDDHHMMSS" timestamp.
func ParseTimestamp(raw string) (time.Time, error) {
	if len(raw) != len(timestampLayout) {
		return time.Time{}, fmt.Errorf("timestamp %q is not YYYYMMDDHHMMSS", raw)
	}
	ts, err := time.Parse(timestampLayout, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("timestamp %q is not YYYYMMDDHHMMSS", raw)
	}
	return ts, nil
}

// partID limits part ids to names that are safe both as a storage folder and
// inside a shell command.
var partID = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// ValidPartID reports whether id can be used as a part id.
func ValidPartID(id string) bool {
	return partID.MatchString(id)
}

// Part is one line of a parts command's output.
type Part struct {
	ID string
	// Skip means the part is unchanged and must not be backed up.
	Skip bool
	// Timestamp is the part's modification time; it names the backup file.
	// Zero when Skip is set.
	Timestamp time.Time
}

// ParsePartList parses the output of a parts command: one
// "<part>=<YYYYMMDDHHMMSS>" (changed) or "<part>=SKIP" (unchanged) per line.
// Blank lines and "#" comments, including trailing ones, are ignored.
func ParsePartList(output string) ([]Part, error) {
	var parts []Part
	seen := map[string]bool{}
	for n, line := range strings.Split(output, "\n") {
		if i := strings.IndexByte(line, '#'); i >= 0 {
			line = line[:i]
		}
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		id, value, found := strings.Cut(line, "=")
		id, value = strings.TrimSpace(id), strings.TrimSpace(value)
		if !found {
			return nil, fmt.Errorf("line %d: %q is not <part>=<YYYYMMDDHHMMSS> or <part>=%s", n+1, line, PartSkip)
		}
		if !ValidPartID(id) {
			return nil, fmt.Errorf("line %d: invalid part id %q (allowed: letters, digits, '.', '_', '-')", n+1, id)
		}
		if seen[id] {
			return nil, fmt.Errorf("line %d: part %q listed twice", n+1, id)
		}
		seen[id] = true
		if value == PartSkip {
			parts = append(parts, Part{ID: id, Skip: true})
			continue
		}
		ts, err := ParseTimestamp(value)
		if err != nil {
			return nil, fmt.Errorf("line %d: part %q: %w", n+1, id, err)
		}
		parts = append(parts, Part{ID: id, Timestamp: ts})
	}
	return parts, nil
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

	compression := strings.ToLower(strings.TrimSpace(ann[Compression]))
	if compression == "" {
		compression = None
	}
	codec, known := codecs[compression]
	if !known {
		return Target{}, true, fmt.Errorf("annotation %s: %q must be %s, %s or %s", Compression, ann[Compression], None, Gzip, Zstd)
	}
	level := 0
	if raw := strings.TrimSpace(ann[CompressionLevel]); raw != "" {
		if compression == None {
			// Ignoring the level would hide a forgotten or misspelt codec.
			return Target{}, true, fmt.Errorf("annotation %s: requires %s %s or %s", CompressionLevel, Compression, Gzip, Zstd)
		}
		level, err = strconv.Atoi(raw)
		if err != nil || level < codec.minLevel || level > codec.maxLevel {
			return Target{}, true, fmt.Errorf("annotation %s: %q must be an integer from %d to %d for %s",
				CompressionLevel, ann[CompressionLevel], codec.minLevel, codec.maxLevel, compression)
		}
	}

	externalImage := strings.TrimSpace(ann[ExternalImage])

	parts := false
	if raw := strings.TrimSpace(ann[Parts]); raw != "" {
		parts, err = strconv.ParseBool(raw)
		if err != nil {
			return Target{}, true, fmt.Errorf("annotation %s: %q must be true or false", Parts, ann[Parts])
		}
	}
	partsCommand := ""
	if parts {
		partsCommand = ann[PartsCommand]
		if strings.TrimSpace(partsCommand) == "" {
			return Target{}, true, fmt.Errorf("missing required annotation %s (%s is true)", PartsCommand, Parts)
		}
		if externalImage == "" {
			return Target{}, true, fmt.Errorf("missing required annotation %s (%s is true)", ExternalImage, Parts)
		}
		if !strings.Contains(command, PartPlaceholder) {
			return Target{}, true, fmt.Errorf("annotation %s: must contain %s when %s is true", Command, PartPlaceholder, Parts)
		}
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
		ExternalImage:     externalImage,
		Parts:             parts,
		PartsCommand:      partsCommand,
		Compression:       compression,
		CompressionLevel:  level,
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
