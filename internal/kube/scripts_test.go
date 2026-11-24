package kube

import (
	"context"
	"strings"
	"testing"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"oap-backup/internal/annotations"
)

func TestScriptsResolve(t *testing.T) {
	client := fake.NewClientset(
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: "db", Name: "pg-scripts"},
			Data:       map[string]string{"backup.sh": "pg_dumpall -U postgres\n", "empty.sh": " \n", "huge.sh": strings.Repeat("x", MaxScriptSize+1)},
			BinaryData: map[string][]byte{"binary.sh": []byte("echo binary")},
		},
		&corev1.Secret{
			ObjectMeta: metav1.ObjectMeta{Namespace: "db", Name: "pg-secret"},
			Data:       map[string][]byte{"dump.sh": []byte("PGPASSWORD=s3cret pg_dumpall")},
		},
		// Same name in another namespace: must never be used.
		&corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: "other", Name: "pg-scripts"},
			Data:       map[string]string{"backup.sh": "wrong namespace"},
		},
	)
	s := &Scripts{Client: client}
	ctx := context.Background()
	cm := func(field string) annotations.Ref {
		return annotations.Ref{Kind: annotations.RefConfigMap, Name: "pg-scripts", Field: field}
	}

	ok := map[annotations.Ref]string{
		cm("backup.sh"): "pg_dumpall -U postgres\n",
		cm("binary.sh"): "echo binary",
		{Kind: annotations.RefSecret, Name: "pg-secret", Field: "dump.sh"}: "PGPASSWORD=s3cret pg_dumpall",
	}
	for ref, want := range ok {
		if got, err := s.Resolve(ctx, "db", ref); err != nil || got != want {
			t.Errorf("Resolve(%s) = %q, %v; want %q", ref, got, err, want)
		}
	}

	bad := map[annotations.Ref]string{
		cm("nope.sh"):  `configmap db/pg-scripts has no field "nope.sh" (it has: backup.sh, binary.sh, empty.sh, huge.sh)`,
		cm("empty.sh"): `field "empty.sh" of configmap db/pg-scripts is empty`,
		cm("huge.sh"):  "a script can be at most",
		{Kind: annotations.RefConfigMap, Name: "missing", Field: "x"}: "configmap db/missing not found",
		{Kind: annotations.RefSecret, Name: "missing", Field: "x"}:    "secret db/missing not found",
		{Kind: annotations.RefSecret, Name: "pg-secret", Field: "x"}:  `secret db/pg-secret has no field "x" (it has: dump.sh)`,
		{Kind: annotations.RefSecret, Name: "pg-scripts", Field: "x"}: "secret db/pg-scripts not found", // it is a ConfigMap
		{Kind: "file", Name: "pg-scripts", Field: "backup.sh"}:        "unsupported script source",
	}
	for ref, want := range bad {
		got, err := s.Resolve(ctx, "db", ref)
		if err == nil || got != "" || !strings.Contains(err.Error(), want) {
			t.Errorf("Resolve(%s) = %q, %v; want error containing %q", ref, got, err, want)
		}
		if err != nil && strings.Contains(err.Error(), "s3cret") {
			t.Errorf("Resolve(%s): the error leaks the script: %v", ref, err)
		}
	}

	// No permission to read Secrets.
	client.PrependReactor("get", "secrets", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, apierrors.NewForbidden(schema.GroupResource{Resource: "secrets"}, "pg-secret", nil)
	})
	_, err := s.Resolve(ctx, "db", annotations.Ref{Kind: annotations.RefSecret, Name: "pg-secret", Field: "dump.sh"})
	if err == nil || !strings.Contains(err.Error(), "not allowed to read secret db/pg-secret") || !strings.Contains(err.Error(), `"get" on secrets`) {
		t.Errorf("forbidden: err = %v", err)
	}
}
