package humanize

import "testing"

func TestBytes(t *testing.T) {
	cases := map[int64]string{
		0:              "0 B",
		1023:           "1023 B",
		1024:           "1.0 KiB",
		1536:           "1.5 KiB",
		1233408:        "1.2 MiB",
		3 << 20:        "3.0 MiB",
		5 << 30:        "5.0 GiB",
		2 << 40:        "2.0 TiB",
		3000 * 1 << 40: "3000.0 TiB",
	}
	for n, want := range cases {
		if got := Bytes(n); got != want {
			t.Errorf("Bytes(%d) = %q, want %q", n, got, want)
		}
	}
}
