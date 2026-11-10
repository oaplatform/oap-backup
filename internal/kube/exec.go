// Package kube wraps the Kubernetes operations oap-backup needs: exec into a
// container and managing helper pods.
package kube

import (
	"context"
	"errors"
	"fmt"
	"io"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/httpstream"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/remotecommand"
	utilexec "k8s.io/client-go/util/exec"
)

// Executor runs a command inside a container, streaming its output.
type Executor interface {
	// Exec runs command in namespace/pod/container. A command that exits
	// non-zero yields an *ExitError.
	Exec(ctx context.Context, namespace, pod, container string, command []string, stdout, stderr io.Writer) error
}

// ExitError reports a command that ran but exited with a non-zero code.
type ExitError struct {
	Code int
}

func (e *ExitError) Error() string {
	return fmt.Sprintf("command exited with code %d", e.Code)
}

// RemoteExecutor implements Executor with the pods/exec subresource, the same
// way kubectl exec does (WebSocket, falling back to SPDY on old API servers).
type RemoteExecutor struct {
	Config *rest.Config
	Client kubernetes.Interface
}

// Exec implements Executor.
func (e *RemoteExecutor) Exec(ctx context.Context, namespace, pod, container string, command []string, stdout, stderr io.Writer) error {
	req := e.Client.CoreV1().RESTClient().Post().
		Resource("pods").
		Namespace(namespace).
		Name(pod).
		SubResource("exec").
		VersionedParams(&corev1.PodExecOptions{
			Container: container,
			Command:   command,
			Stdout:    true,
			Stderr:    true,
		}, scheme.ParameterCodec)

	ws, err := remotecommand.NewWebSocketExecutor(e.Config, "GET", req.URL().String())
	if err != nil {
		return fmt.Errorf("creating websocket executor: %w", err)
	}
	spdy, err := remotecommand.NewSPDYExecutor(e.Config, "POST", req.URL())
	if err != nil {
		return fmt.Errorf("creating spdy executor: %w", err)
	}
	executor, err := remotecommand.NewFallbackExecutor(ws, spdy, func(err error) bool {
		return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err)
	})
	if err != nil {
		return fmt.Errorf("creating exec fallback executor: %w", err)
	}

	err = executor.StreamWithContext(ctx, remotecommand.StreamOptions{Stdout: stdout, Stderr: stderr})
	var exitErr utilexec.ExitError
	if errors.As(err, &exitErr) && exitErr.Exited() {
		return &ExitError{Code: exitErr.ExitStatus()}
	}
	return err
}
