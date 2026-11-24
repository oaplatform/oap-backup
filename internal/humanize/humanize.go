// Package humanize formats values for people, in logs and in CLI output.
package humanize

import "fmt"

// Bytes renders a size in binary units with one decimal: "512 B", "1.2 MiB",
// "118.4 GiB".
func Bytes(n int64) string {
	const unit = 1024
	if n < unit {
		return fmt.Sprintf("%d B", n)
	}
	value, suffix := float64(n), ""
	for _, s := range []string{"KiB", "MiB", "GiB", "TiB"} {
		value /= unit
		suffix = s
		if value < unit {
			break
		}
	}
	return fmt.Sprintf("%.1f %s", value, suffix)
}
