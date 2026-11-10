package kube

import (
	"context"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
)

// Helper pod metadata.
const (
	ManagedByLabel   = "app.kubernetes.io/managed-by"
	ManagedByValue   = "oap-backup"
	SourcePodAnno    = "oap/backup-source-pod"
	HelperContainer  = "backup-helper"
	helperNameSuffix = "-backup-helper"
	saTokenMountPath = "/var/run/secrets/kubernetes.io/serviceaccount"
)

// HelperPodName returns the helper pod name for a source pod.
func HelperPodName(pod string) string {
	name := pod + helperNameSuffix
	if len(name) > 253 {
		name = name[:253]
	}
	return name
}

// BuildHelperPod builds a pod that runs image with "sleep infinity" and has
// the same volumes and volume mounts as src's container, so a backup command
// can read the source's data with tools the source image does not ship.
//
// The helper is pinned to the source pod's node (and tolerates its taints),
// so ReadWriteOnce / local PersistentVolumes and hostPath volumes resolve to
// the same data. Generic ephemeral volumes are rewritten to reference the
// source pod's existing PVC instead of provisioning a new one. The service
// account token is neither mounted nor copied.
func BuildHelperPod(src *corev1.Pod, container, image string) (*corev1.Pod, error) {
	var source *corev1.Container
	for i := range src.Spec.Containers {
		if src.Spec.Containers[i].Name == container {
			source = &src.Spec.Containers[i]
			break
		}
	}
	if source == nil {
		return nil, fmt.Errorf("container %q not found in pod %s/%s", container, src.Namespace, src.Name)
	}

	var mounts []corev1.VolumeMount
	used := map[string]bool{}
	for _, m := range source.VolumeMounts {
		if m.MountPath == saTokenMountPath || strings.HasPrefix(m.MountPath, saTokenMountPath+"/") {
			continue
		}
		mounts = append(mounts, *m.DeepCopy())
		used[m.Name] = true
	}

	var volumes []corev1.Volume
	for _, v := range src.Spec.Volumes {
		if !used[v.Name] {
			continue
		}
		v := *v.DeepCopy()
		if v.Ephemeral != nil {
			v.VolumeSource = corev1.VolumeSource{
				PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: src.Name + "-" + v.Name},
			}
		}
		volumes = append(volumes, v)
	}

	automount := false
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:        HelperPodName(src.Name),
			Namespace:   src.Namespace,
			Labels:      map[string]string{ManagedByLabel: ManagedByValue},
			Annotations: map[string]string{SourcePodAnno: src.Name},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:         HelperContainer,
				Image:        image,
				Command:      []string{"sleep", "infinity"},
				VolumeMounts: mounts,
			}},
			Volumes:                      volumes,
			RestartPolicy:                corev1.RestartPolicyNever,
			NodeName:                     src.Spec.NodeName,
			Tolerations:                  append([]corev1.Toleration(nil), src.Spec.Tolerations...),
			ImagePullSecrets:             append([]corev1.LocalObjectReference(nil), src.Spec.ImagePullSecrets...),
			AutomountServiceAccountToken: &automount,
		},
	}
	if src.Spec.SecurityContext != nil {
		pod.Spec.SecurityContext = src.Spec.SecurityContext.DeepCopy()
	}
	return pod, nil
}

// Helpers creates and deletes helper pods.
type Helpers struct {
	Client       kubernetes.Interface
	Timeout      time.Duration
	PollInterval time.Duration
}

func (h *Helpers) poll() time.Duration {
	if h.PollInterval > 0 {
		return h.PollInterval
	}
	return 2 * time.Second
}

// Create starts a helper pod for namespace/pod's container and waits until it
// is Running. A stale helper with the same name is deleted first. On error,
// any helper pod that was created is deleted again. Returns the helper name.
func (h *Helpers) Create(ctx context.Context, namespace, pod, container, image string) (string, error) {
	src, err := h.Client.CoreV1().Pods(namespace).Get(ctx, pod, metav1.GetOptions{})
	if err != nil {
		return "", fmt.Errorf("reading source pod: %w", err)
	}
	manifest, err := BuildHelperPod(src, container, image)
	if err != nil {
		return "", err
	}
	name := manifest.Name

	if err := h.deleteAndWait(ctx, namespace, name); err != nil {
		return "", fmt.Errorf("removing stale helper pod %s: %w", name, err)
	}
	if _, err := h.Client.CoreV1().Pods(namespace).Create(ctx, manifest, metav1.CreateOptions{}); err != nil {
		return "", fmt.Errorf("creating helper pod %s: %w", name, err)
	}

	var phase corev1.PodPhase
	err = wait.PollUntilContextTimeout(ctx, h.poll(), h.Timeout, true, func(ctx context.Context) (bool, error) {
		p, err := h.Client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		phase = p.Status.Phase
		switch phase {
		case corev1.PodRunning:
			return true, nil
		case corev1.PodFailed, corev1.PodSucceeded:
			return false, fmt.Errorf("helper pod %s terminated (phase=%s)", name, phase)
		}
		return false, nil
	})
	if err != nil {
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		defer cancel()
		_ = h.Delete(cleanupCtx, namespace, name)
		return "", fmt.Errorf("helper pod %s did not reach Running within %s (last phase=%q): %w", name, h.Timeout, phase, err)
	}
	return name, nil
}

// Delete deletes a helper pod immediately; a missing pod is not an error.
func (h *Helpers) Delete(ctx context.Context, namespace, name string) error {
	zero := int64(0)
	err := h.Client.CoreV1().Pods(namespace).Delete(ctx, name, metav1.DeleteOptions{GracePeriodSeconds: &zero})
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	return nil
}

func (h *Helpers) deleteAndWait(ctx context.Context, namespace, name string) error {
	if err := h.Delete(ctx, namespace, name); err != nil {
		return err
	}
	return wait.PollUntilContextTimeout(ctx, h.poll(), h.Timeout, true, func(ctx context.Context) (bool, error) {
		_, err := h.Client.CoreV1().Pods(namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, err
	})
}

// CleanupOrphans deletes every helper pod (in namespace, or all namespaces
// when empty) left behind by a previous oap-backup process, e.g. after a
// crash. It returns the deleted pods as "<namespace>/<name>".
func (h *Helpers) CleanupOrphans(ctx context.Context, namespace string) ([]string, error) {
	list, err := h.Client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{
		LabelSelector: ManagedByLabel + "=" + ManagedByValue,
	})
	if err != nil {
		return nil, err
	}
	var deleted []string
	for _, p := range list.Items {
		if err := h.Delete(ctx, p.Namespace, p.Name); err != nil {
			return deleted, fmt.Errorf("deleting helper pod %s/%s: %w", p.Namespace, p.Name, err)
		}
		deleted = append(deleted, p.Namespace+"/"+p.Name)
	}
	return deleted, nil
}
