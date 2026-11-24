package kube

import (
	"context"
	"fmt"
	"sort"
	"strings"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"

	"oap-backup/internal/annotations"
)

// MaxScriptSize is the largest script Resolve returns. A script runs as one
// argument of "sh -c", and Linux limits a single argument to 128 KiB.
const MaxScriptSize = 128<<10 - 1

// Scripts reads command scripts from ConfigMaps and Secrets.
type Scripts struct {
	Client kubernetes.Interface
}

// Resolve returns the script that ref points at in namespace. The error says
// what is wrong (object or field missing, no permission, empty, too large)
// and never contains the script.
func (s *Scripts) Resolve(ctx context.Context, namespace string, ref annotations.Ref) (string, error) {
	object := fmt.Sprintf("%s %s/%s", ref.Kind, namespace, ref.Name)

	var data map[string][]byte
	var err error
	switch ref.Kind {
	case annotations.RefConfigMap:
		data, err = s.configMap(ctx, namespace, ref.Name)
	case annotations.RefSecret:
		data, err = s.secret(ctx, namespace, ref.Name)
	default:
		return "", fmt.Errorf("unsupported script source %q", ref.Kind)
	}
	switch {
	case apierrors.IsNotFound(err):
		return "", fmt.Errorf("%s not found", object)
	case apierrors.IsForbidden(err):
		return "", fmt.Errorf("oap-backup is not allowed to read %s (its role needs \"get\" on %ss): %w", object, ref.Kind, err)
	case err != nil:
		return "", fmt.Errorf("reading %s: %w", object, err)
	}

	script, ok := data[ref.Field]
	if !ok {
		fields := make([]string, 0, len(data))
		for field := range data {
			fields = append(fields, field)
		}
		sort.Strings(fields)
		return "", fmt.Errorf("%s has no field %q (it has: %s)", object, ref.Field, strings.Join(fields, ", "))
	}
	if strings.TrimSpace(string(script)) == "" {
		return "", fmt.Errorf("field %q of %s is empty", ref.Field, object)
	}
	if len(script) > MaxScriptSize {
		return "", fmt.Errorf("field %q of %s is %d bytes; a script can be at most %d bytes", ref.Field, object, len(script), MaxScriptSize)
	}
	return string(script), nil
}

func (s *Scripts) configMap(ctx context.Context, namespace, name string) (map[string][]byte, error) {
	cm, err := s.Client.CoreV1().ConfigMaps(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	data := make(map[string][]byte, len(cm.Data)+len(cm.BinaryData))
	for field, value := range cm.BinaryData {
		data[field] = value
	}
	for field, value := range cm.Data {
		data[field] = []byte(value)
	}
	return data, nil
}

func (s *Scripts) secret(ctx context.Context, namespace, name string) (map[string][]byte, error) {
	secret, err := s.Client.CoreV1().Secrets(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	data := make(map[string][]byte, len(secret.Data)+len(secret.StringData))
	for field, value := range secret.Data {
		data[field] = value
	}
	// The API server never returns StringData; the fake clientset does.
	for field, value := range secret.StringData {
		data[field] = []byte(value)
	}
	return data, nil
}
