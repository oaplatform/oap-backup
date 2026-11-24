package progress

import (
	"context"
	"testing"
)

func TestTracker(t *testing.T) {
	tr := New()
	want := func(step string, bytes int64, files int) {
		t.Helper()
		got, ok := tr.Snapshot()
		if !ok || got != (Snapshot{UploadedBytes: bytes, FilesUploaded: files}) {
			t.Errorf("%s: snapshot = %+v, want %d bytes, %d files", step, got, bytes, files)
		}
	}
	want("new", 0, 0)

	tr.StartUpload(1000)
	want("upload started", 0, 0)
	tr.AddUploaded(400)
	want("uploading", 400, 0)
	// A store that resends data cannot push the count over the file size.
	tr.AddUploaded(900)
	want("capped", 1000, 0)
	tr.FileDone()
	want("file done", 1000, 1)

	// A second file adds to the total.
	tr.StartUpload(50)
	tr.AddUploaded(20)
	want("second file", 1020, 1)
	tr.FileDone()
	want("second file done", 1050, 2)

	// A failed upload: the next file does not inherit its partial count.
	tr.StartUpload(500)
	tr.AddUploaded(100)
	want("third file", 1150, 2)
	tr.StartUpload(10)
	want("after a failed file", 1050, 2)

	tr.Reset()
	want("reset", 0, 0)
}

func TestNilTracker(t *testing.T) {
	var tr *Tracker
	tr.Reset()
	tr.StartUpload(1)
	tr.AddUploaded(1)
	tr.FileDone()
	if snap, ok := tr.Snapshot(); ok || snap != (Snapshot{}) {
		t.Errorf("nil tracker: %+v, %v", snap, ok)
	}
}

func TestContext(t *testing.T) {
	if FromContext(context.Background()) != nil {
		t.Error("empty context must carry no tracker")
	}
	tr := New()
	if FromContext(NewContext(context.Background(), tr)) != tr {
		t.Error("tracker not found in its context")
	}
}
