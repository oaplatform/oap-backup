package backup

import (
	"compress/gzip"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"

	"oap-backup/internal/annotations"
)

// newCompressor returns a writer that compresses into w with codec at level
// (0 = the codec's default). It returns nil for the empty codec: the data is
// stored as is. The caller must Close the compressor to finish the stream.
func newCompressor(w io.Writer, codec string, level int) (io.WriteCloser, error) {
	switch codec {
	case "":
		return nil, nil
	case annotations.Gzip:
		if level == 0 {
			level = gzip.DefaultCompression
		}
		zw, err := gzip.NewWriterLevel(w, level)
		if err != nil {
			return nil, fmt.Errorf("gzip level %d: %w", level, err)
		}
		return zw, nil
	case annotations.Zstd:
		// The encoder has four speed presets; a zstd level (1-22) selects
		// the closest one.
		preset := zstd.SpeedDefault
		if level != 0 {
			preset = zstd.EncoderLevelFromZstd(level)
		}
		zw, err := zstd.NewWriter(w, zstd.WithEncoderLevel(preset))
		if err != nil {
			return nil, fmt.Errorf("zstd level %d: %w", level, err)
		}
		return zw, nil
	default:
		return nil, fmt.Errorf("unsupported compression %q", codec)
	}
}
