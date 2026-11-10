package storage

import (
	"reflect"
	"testing"

	"oap-backup/internal/config"
)

func TestBasenames(t *testing.T) {
	got := basenames([]string{"/backups/pg/backup-1.sql.gz", "backup-2.sql.gz", ".", "..", "/backups/pg/..", "dir/"})
	want := []string{"backup-1.sql.gz", "backup-2.sql.gz"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("basenames = %v, want %v", got, want)
	}
}

func TestFTPRemoteDir(t *testing.T) {
	cases := map[string]string{"/": "/pg", "": "/pg", "backups": "/backups/pg", "/backups/": "/backups/pg"}
	for folder, want := range cases {
		f := NewFTP(config.FTP{Folder: folder})
		if got := f.remoteDir("pg"); got != want {
			t.Errorf("folder %q: remoteDir = %q, want %q", folder, got, want)
		}
	}
}

func TestS3Key(t *testing.T) {
	cases := map[string]string{"": "pg/backup-1.sql.gz", "prod": "prod/pg/backup-1.sql.gz", "/prod/": "prod/pg/backup-1.sql.gz"}
	for prefix, want := range cases {
		s, err := NewS3(config.S3{Endpoint: "localhost:9000", Bucket: "b", Prefix: prefix})
		if err != nil {
			t.Fatal(err)
		}
		if got := s.key("pg", "backup-1.sql.gz"); got != want {
			t.Errorf("prefix %q: key = %q, want %q", prefix, got, want)
		}
	}
}
