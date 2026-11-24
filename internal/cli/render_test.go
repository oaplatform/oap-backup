package cli

import (
	"testing"
	"time"
)

func TestShortDuration(t *testing.T) {
	cases := map[time.Duration]string{
		-time.Minute:                          "0s",
		400 * time.Millisecond:                "0s",
		42 * time.Second:                      "42s",
		5*time.Minute + 10*time.Second:        "5m10s",
		3*time.Hour + 12*time.Minute:          "3h12m",
		53*time.Hour + 59*time.Minute:         "2d5h",
		time.Hour:                             "1h0m",
		24*time.Hour - 500*time.Millisecond:   "1d0h",
		59*time.Second + 600*time.Millisecond: "1m0s",
	}
	for d, want := range cases {
		if got := shortDuration(d); got != want {
			t.Errorf("shortDuration(%s) = %q, want %q", d, got, want)
		}
	}
}

func TestHumanSize(t *testing.T) {
	cases := map[int64]string{
		0:              "0 B",
		1023:           "1023 B",
		1024:           "1.0 KiB",
		1536:           "1.5 KiB",
		3 << 20:        "3.0 MiB",
		5 << 30:        "5.0 GiB",
		2 << 40:        "2.0 TiB",
		3000 * 1 << 40: "3000.0 TiB",
	}
	for n, want := range cases {
		if got := humanSize(n); got != want {
			t.Errorf("humanSize(%d) = %q, want %q", n, got, want)
		}
	}
}

func TestPluralAndOneLine(t *testing.T) {
	if plural(1, "file") != "1 file" || plural(0, "file") != "0 files" || plural(2, "backup") != "2 backups" {
		t.Error("plural")
	}
	if got := oneLine("cd /srv\n  for d in */; do\n\techo $d\ndone\n"); got != "cd /srv for d in */; do echo $d done" {
		t.Errorf("oneLine = %q", got)
	}
}
