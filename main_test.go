package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestHelpRequested(t *testing.T) {
	for _, args := range [][]string{{"help"}, {"-h"}, {"-help"}, {"--help"}, {"help", "list"}} {
		if !helpRequested(args) {
			t.Errorf("helpRequested(%v) = false", args)
		}
	}
	for _, args := range [][]string{nil, {"list"}, {"list", "-h"}, {"--storage=s3"}, {"--storage=s3", "--help"}} {
		if helpRequested(args) {
			t.Errorf("helpRequested(%v) = true", args)
		}
	}
}

func TestWriteHelp(t *testing.T) {
	var buf bytes.Buffer
	writeHelp(&buf)
	out := buf.String()
	for _, want := range []string{
		"oap-backup list", "oap-backup describe <namespace>/<pod>", "oap-backup run --all", "oap-backup refresh", "oap-backup status", "--server URL",
		"oap-backup --storage=ftp --ftp_addr=<host:port>", "oap-backup --storage=s3 --s3_endpoint=",
		"Service flags:", "--backup_timeout duration", "--s3_bucket string",
		"FTP_PASSWORD", "S3_ACCESS_KEY, S3_SECRET_KEY", "OAP_BACKUP_SERVER",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("help does not contain %q:\n%s", want, out)
		}
	}
}
