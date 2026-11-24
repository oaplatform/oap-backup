package kube

import (
	"context"
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
)

func sourcePod() *corev1.Pod {
	uid := int64(999)
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Namespace: "db", Name: "mongo-0"},
		Spec: corev1.PodSpec{
			NodeName:         "node-1",
			Tolerations:      []corev1.Toleration{{Key: "dedicated", Value: "db", Effect: corev1.TaintEffectNoSchedule}},
			ImagePullSecrets: []corev1.LocalObjectReference{{Name: "regcred"}},
			SecurityContext:  &corev1.PodSecurityContext{FSGroup: &uid},
			Containers: []corev1.Container{
				{Name: "sidecar", VolumeMounts: []corev1.VolumeMount{{Name: "logs", MountPath: "/logs"}}},
				{Name: "mongo",
					Env: []corev1.EnvVar{
						{Name: "MONGO_USER", Value: "root"},
						{Name: "MONGO_PASSWORD", ValueFrom: &corev1.EnvVarSource{SecretKeyRef: &corev1.SecretKeySelector{
							LocalObjectReference: corev1.LocalObjectReference{Name: "mongo"}, Key: "password"}}},
						{Name: "MEM_LIMIT", ValueFrom: &corev1.EnvVarSource{ResourceFieldRef: &corev1.ResourceFieldSelector{
							ContainerName: "mongo", Resource: "limits.memory"}}},
						{Name: "SIDECAR_CPU", ValueFrom: &corev1.EnvVarSource{ResourceFieldRef: &corev1.ResourceFieldSelector{
							ContainerName: "sidecar", Resource: "limits.cpu"}}},
						{Name: "PART_COUNT", Value: "from-source"},
					},
					EnvFrom: []corev1.EnvFromSource{{ConfigMapRef: &corev1.ConfigMapEnvSource{
						LocalObjectReference: corev1.LocalObjectReference{Name: "mongo-config"}}}},
					VolumeMounts: []corev1.VolumeMount{
						{Name: "data", MountPath: "/data/db"},
						{Name: "scratch", MountPath: "/scratch"},
						{Name: "kube-api-access-x", MountPath: saTokenMountPath, ReadOnly: true},
					}},
			},
			Volumes: []corev1.Volume{
				{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "data-mongo-0"}}},
				{Name: "scratch", VolumeSource: corev1.VolumeSource{Ephemeral: &corev1.EphemeralVolumeSource{}}},
				{Name: "logs", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
				{Name: "kube-api-access-x", VolumeSource: corev1.VolumeSource{Projected: &corev1.ProjectedVolumeSource{}}},
			},
		},
	}
}

func TestBuildHelperPod(t *testing.T) {
	env := []corev1.EnvVar{{Name: "PART_COUNT", Value: "1"}, {Name: "PART0_ID", Value: "p1"}}
	p, err := BuildHelperPod(sourcePod(), "mongo", "mongo:8", env)
	if err != nil {
		t.Fatalf("BuildHelperPod: %v", err)
	}
	// Source env first (without the reference to another container), then
	// the extra env, which overrides a source variable of the same name.
	got := p.Spec.Containers[0].Env
	var names []string
	for _, e := range got {
		names = append(names, e.Name)
	}
	if want := "MONGO_USER MONGO_PASSWORD MEM_LIMIT PART_COUNT PART_COUNT PART0_ID"; strings.Join(names, " ") != want {
		t.Fatalf("env names = %v, want %s", names, want)
	}
	if got[0].Value != "root" || got[1].ValueFrom.SecretKeyRef.Name != "mongo" || got[4] != env[0] || got[5] != env[1] {
		t.Errorf("env = %+v", got)
	}
	if ref := got[2].ValueFrom.ResourceFieldRef; ref.ContainerName != HelperContainer || ref.Resource != "limits.memory" {
		t.Errorf("resourceFieldRef = %+v, want container %s", ref, HelperContainer)
	}
	if from := p.Spec.Containers[0].EnvFrom; len(from) != 1 || from[0].ConfigMapRef.Name != "mongo-config" {
		t.Errorf("envFrom = %+v", from)
	}
	if p.Name != "mongo-0-backup-helper" || p.Namespace != "db" || p.Labels[ManagedByLabel] != ManagedByValue {
		t.Errorf("unexpected metadata: %+v", p.ObjectMeta)
	}
	if p.Spec.NodeName != "node-1" || len(p.Spec.Tolerations) != 1 || len(p.Spec.ImagePullSecrets) != 1 ||
		p.Spec.SecurityContext == nil || *p.Spec.SecurityContext.FSGroup != 999 {
		t.Errorf("scheduling/security not copied: %+v", p.Spec)
	}
	if p.Spec.RestartPolicy != corev1.RestartPolicyNever || *p.Spec.AutomountServiceAccountToken {
		t.Errorf("restartPolicy=%s automount=%v", p.Spec.RestartPolicy, *p.Spec.AutomountServiceAccountToken)
	}
	c := p.Spec.Containers[0]
	if c.Name != HelperContainer || c.Image != "mongo:8" || strings.Join(c.Command, " ") != "sleep infinity" {
		t.Errorf("unexpected container: %+v", c)
	}
	if len(c.VolumeMounts) != 2 || c.VolumeMounts[0].MountPath != "/data/db" || c.VolumeMounts[1].MountPath != "/scratch" {
		t.Errorf("unexpected mounts: %+v", c.VolumeMounts)
	}
	if len(p.Spec.Volumes) != 2 {
		t.Fatalf("want 2 volumes (data, scratch), got %+v", p.Spec.Volumes)
	}
	if p.Spec.Volumes[0].PersistentVolumeClaim.ClaimName != "data-mongo-0" {
		t.Errorf("data volume: %+v", p.Spec.Volumes[0])
	}
	if pvc := p.Spec.Volumes[1].PersistentVolumeClaim; pvc == nil || pvc.ClaimName != "mongo-0-scratch" || p.Spec.Volumes[1].Ephemeral != nil {
		t.Errorf("ephemeral volume not rewritten to PVC: %+v", p.Spec.Volumes[1])
	}

	if _, err := BuildHelperPod(sourcePod(), "nope", "x", nil); err == nil {
		t.Error("want error for unknown container")
	}
}

func TestHelpersCreateDeleteCleanup(t *testing.T) {
	src := sourcePod()
	stale := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: "other", Name: "x-backup-helper",
		Labels: map[string]string{ManagedByLabel: ManagedByValue}}}
	client := fake.NewClientset(src, stale)
	// The fake API never runs pods; mark helpers Running on create.
	client.PrependReactor("create", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		p := a.(k8stesting.CreateAction).GetObject().(*corev1.Pod)
		p.Status.Phase = corev1.PodRunning
		return false, nil, nil
	})
	h := &Helpers{Client: client, Timeout: 5 * time.Second, PollInterval: 10 * time.Millisecond}
	ctx := context.Background()

	name, err := h.Create(ctx, "db", "mongo-0", "mongo", "mongo:8", nil)
	if err != nil {
		t.Fatalf("Create: %v", err)
	}
	if name != "mongo-0-backup-helper" {
		t.Errorf("name = %q", name)
	}
	// Creating again replaces the existing helper.
	if _, err := h.Create(ctx, "db", "mongo-0", "mongo", "mongo:8", nil); err != nil {
		t.Fatalf("second Create: %v", err)
	}

	deleted, err := h.CleanupOrphans(ctx, "")
	if err != nil {
		t.Fatalf("CleanupOrphans: %v", err)
	}
	if len(deleted) != 2 {
		t.Errorf("deleted = %v, want both helpers", deleted)
	}
	if err := h.Delete(ctx, "db", name); err != nil {
		t.Errorf("Delete of missing pod: %v", err)
	}
	if _, err := client.CoreV1().Pods("db").Get(ctx, "mongo-0", metav1.GetOptions{}); err != nil {
		t.Errorf("source pod must survive cleanup: %v", err)
	}
}

func TestHelpersCreateFailedPod(t *testing.T) {
	client := fake.NewClientset(sourcePod())
	client.PrependReactor("create", "pods", func(a k8stesting.Action) (bool, runtime.Object, error) {
		a.(k8stesting.CreateAction).GetObject().(*corev1.Pod).Status.Phase = corev1.PodFailed
		return false, nil, nil
	})
	h := &Helpers{Client: client, Timeout: time.Second, PollInterval: 10 * time.Millisecond}
	if _, err := h.Create(context.Background(), "db", "mongo-0", "mongo", "bad:image", nil); err == nil {
		t.Fatal("want error for failed helper pod")
	}
	if _, err := client.CoreV1().Pods("db").Get(context.Background(), "mongo-0-backup-helper", metav1.GetOptions{}); err == nil {
		t.Error("failed helper pod must be deleted")
	}
}
