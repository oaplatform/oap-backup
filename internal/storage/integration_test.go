//go:build integration

// Integration tests against real servers. See README "Development" for how
// to start them:
//
//	OAP_TEST_FTP_ADDR=localhost:21 OAP_TEST_FTP_USER=one OAP_TEST_FTP_PASSWORD=1234 \
//	OAP_TEST_S3_ENDPOINT=localhost:9000 \
//	go test -tags integration ./internal/storage/
package storage

import (
	"context"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"oap-backup/internal/config"
)

func exercise(t *testing.T, s Storage) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()

	dir := "it-" + time.Now().UTC().Format("20060102150405") + "/nested/pg"
	names, err := s.List(ctx, dir)
	if err != nil || len(names) != 0 {
		t.Fatalf("List(missing) = %v, %v; want empty, nil", names, err)
	}

	local := filepath.Join(t.TempDir(), "x")
	if err := os.WriteFile(local, []byte("backup-bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, n := range []string{"backup-1.sql.gz", "backup-2.sql.gz"} {
		loc, err := s.Upload(ctx, dir, n, local)
		if err != nil {
			t.Fatalf("Upload %s: %v", n, err)
		}
		t.Logf("uploaded %s", loc)
	}
	names, err = s.List(ctx, dir)
	sort.Strings(names)
	if err != nil || !reflect.DeepEqual(names, []string{"backup-1.sql.gz", "backup-2.sql.gz"}) {
		t.Fatalf("List = %v, %v", names, err)
	}
	if err := s.Delete(ctx, dir, "backup-1.sql.gz"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	names, err = s.List(ctx, dir)
	if err != nil || !reflect.DeepEqual(names, []string{"backup-2.sql.gz"}) {
		t.Fatalf("List after delete = %v, %v", names, err)
	}

	files, err := s.ListFiles(ctx, dir)
	if err != nil || len(files) != 1 || files[0].Name != "backup-2.sql.gz" || files[0].Size != int64(len("backup-bytes")) {
		t.Fatalf("ListFiles = %+v, %v", files, err)
	}
	if age := time.Since(files[0].ModTime); age < -time.Hour || age > 24*time.Hour {
		t.Errorf("ListFiles ModTime = %s, want about now", files[0].ModTime)
	}
	if files, err := s.ListFiles(ctx, dir+"/missing"); err != nil || len(files) != 0 {
		t.Fatalf("ListFiles(missing) = %v, %v; want empty, nil", files, err)
	}

	// Folders: dir's parent holds exactly one folder; an emptied folder can
	// be removed and is then no longer listed.
	parent, base := path.Split(dir)
	parent = strings.TrimSuffix(parent, "/")
	dirs, err := s.ListDirs(ctx, parent)
	if err != nil || !reflect.DeepEqual(dirs, []string{base}) {
		t.Fatalf("ListDirs(%s) = %v, %v; want [%s]", parent, dirs, err, base)
	}
	if dirs, err := s.ListDirs(ctx, dir+"/missing"); err != nil || len(dirs) != 0 {
		t.Fatalf("ListDirs(missing) = %v, %v; want empty, nil", dirs, err)
	}
	if err := s.Delete(ctx, dir, "backup-2.sql.gz"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if err := s.RemoveDir(ctx, dir); err != nil {
		t.Fatalf("RemoveDir: %v", err)
	}
	if dirs, err := s.ListDirs(ctx, parent); err != nil || len(dirs) != 0 {
		t.Fatalf("ListDirs after RemoveDir = %v, %v; want empty, nil", dirs, err)
	}

	// A cancelled context aborts operations.
	cctx, ccancel := context.WithCancel(ctx)
	ccancel()
	if _, err := s.List(cctx, dir); err == nil {
		t.Error("List with cancelled context succeeded")
	}
}

func TestFTPIntegration(t *testing.T) {
	addr := os.Getenv("OAP_TEST_FTP_ADDR")
	if addr == "" {
		t.Skip("OAP_TEST_FTP_ADDR not set")
	}
	exercise(t, NewFTP(config.FTP{
		Addr:     addr,
		User:     os.Getenv("OAP_TEST_FTP_USER"),
		Password: os.Getenv("OAP_TEST_FTP_PASSWORD"),
		Folder:   os.Getenv("OAP_TEST_FTP_FOLDER"),
		Timeout:  10 * time.Second,
	}))
}

func TestS3Integration(t *testing.T) {
	endpoint := os.Getenv("OAP_TEST_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("OAP_TEST_S3_ENDPOINT not set")
	}
	ak, sk := os.Getenv("OAP_TEST_S3_ACCESS_KEY"), os.Getenv("OAP_TEST_S3_SECRET_KEY")
	if ak == "" {
		ak, sk = "minioadmin", "minioadmin"
	}
	bucket := "oap-backup-it"
	mc, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4(ak, sk, "")})
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if ok, err := mc.BucketExists(ctx, bucket); err != nil {
		t.Fatal(err)
	} else if !ok {
		if err := mc.MakeBucket(ctx, bucket, minio.MakeBucketOptions{}); err != nil {
			t.Fatal(err)
		}
	}
	s, err := NewS3(config.S3{Endpoint: endpoint, Bucket: bucket, Prefix: "prod", Insecure: true, AccessKey: ak, SecretKey: sk})
	if err != nil {
		t.Fatal(err)
	}
	exercise(t, s)
}
