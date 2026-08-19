/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

// Benchmarks CronJobReconciler (cronjob_controller.go) against
// FastCronJobReconciler (cronjob_controller_fast.go) using the
// controller-runtime fake client, so we're only measuring reconciler logic
// and client-call volume, not real API server latency.
//
// Run with:
//   go test ./internal/controller/ -run '^$' -bench . -benchmem

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	kbatch "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	batchv1 "tutorial.kubebuilder.io/cronjob/api/v1"
)

func init() {
	logf.SetLogger(logr.Discard())
}

// manualClock lets a benchmark iteration control "now" precisely: it stays
// fixed within a single Reconcile call (which may call Now() several
// times) and only moves forward when the benchmark loop calls Advance.
type manualClock struct {
	t time.Time
}

func (c *manualClock) Now() time.Time          { return c.t }
func (c *manualClock) Advance(d time.Duration) { c.t = c.t.Add(d) }

// benchReconciler is satisfied by both CronJobReconciler and
// FastCronJobReconciler.
type benchReconciler interface {
	Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error)
}

func benchScheme(tb testing.TB) *runtime.Scheme {
	tb.Helper()
	scheme := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(scheme); err != nil {
		tb.Fatal(err)
	}
	if err := batchv1.AddToScheme(scheme); err != nil {
		tb.Fatal(err)
	}
	return scheme
}

func jobIndexFunc(rawObj client.Object) []string {
	job := rawObj.(*kbatch.Job)
	owner := metav1.GetControllerOf(job)
	if owner == nil {
		return nil
	}
	if owner.APIVersion != apiGVStr || owner.Kind != "CronJob" {
		return nil
	}
	return []string{owner.Name}
}

func newBenchCronJob(name, ns string, createdAt time.Time) *batchv1.CronJob {
	suspend := false
	successLimit := int32(3)
	failedLimit := int32(1)
	return &batchv1.CronJob{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         ns,
			CreationTimestamp: metav1.NewTime(createdAt),
		},
		Spec: batchv1.CronJobSpec{
			Schedule:                  "* * * * *", // every minute
			ConcurrencyPolicy:         batchv1.ReplaceConcurrent,
			Suspend:                   &suspend,
			SuccessfulJobHistoryLimit: &successLimit,
			FailedJobHistoryLimit:     &failedLimit,
			JobTemplate: kbatch.JobTemplateSpec{
				Spec: kbatch.JobSpec{
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyOnFailure,
							Containers: []corev1.Container{{
								Name:  "bench",
								Image: "busybox",
							}},
						},
					},
				},
			},
		},
	}
}

func runReconcileBenchmark(b *testing.B, build func(c client.Client, scheme *runtime.Scheme, clock Clock) benchReconciler) {
	b.Helper()

	scheme := benchScheme(b)
	now := time.Now()
	const name, ns = "bench-cronjob", "default"

	cronJob := newBenchCronJob(name, ns, now)
	fakeClient := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&batchv1.CronJob{}).
		WithIndex(&kbatch.Job{}, jobOwnerKey, jobIndexFunc).
		WithObjects(cronJob).
		Build()

	clock := &manualClock{t: now}
	r := build(fakeClient, scheme, clock)
	req := ctrl.Request{NamespacedName: client.ObjectKey{Name: name, Namespace: ns}}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clock.Advance(time.Minute)
		if _, err := r.Reconcile(ctx, req); err != nil {
			b.Fatalf("reconcile failed: %v", err)
		}
	}
}

func BenchmarkReconcile_Base(b *testing.B) {
	runReconcileBenchmark(b, func(c client.Client, scheme *runtime.Scheme, clock Clock) benchReconciler {
		return &CronJobReconciler{Client: c, Scheme: scheme, Clock: clock}
	})
}

func BenchmarkReconcile_Fast(b *testing.B) {
	runReconcileBenchmark(b, func(c client.Client, scheme *runtime.Scheme, clock Clock) benchReconciler {
		return &FastCronJobReconciler{Client: c, Scheme: scheme, Clock: clock}
	})
}
