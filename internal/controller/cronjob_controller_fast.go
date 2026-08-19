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

// FastCronJobReconciler is an alternative to CronJobReconciler (see
// cronjob_controller.go) with the same external behavior but fewer API
// round trips and a couple of bug fixes:
//
//   - The base reconciler tracks cronJob.Status.LastScheduleTime by
//     checking `if err != nil` after getScheduledTimeForJob instead of
//     `if err == nil`, so it only ever records a schedule time when
//     parsing the annotation *fails*. This version fixes that check.
//   - The base reconciler calls r.Status().Update() up to three times per
//     Reconcile (an initial "Progressing/Unknown" placeholder write, the
//     main condition write, and a final write after job creation), each
//     preceded by a redundant r.Get() re-fetch. This version computes the
//     full desired status up front and performs exactly one
//     r.Status().Update() per Reconcile call, via updateStatus().
//   - Parsed cron schedules are cached by schedule string (scheduleCache)
//     instead of being re-parsed by robfig/cron on every reconcile of the
//     same object. The cache is unbounded but keyed on short schedule
//     strings, of which real clusters have very few distinct values.
//   - The active/successful/failed job slices are preallocated to
//     len(childJobs.Items) instead of growing via repeated append.
//   - The per-Reconcile closures in the base version (isJobFinished,
//     getScheduledTimeForJob, getNextSchedule) are hoisted to package-level
//     functions.
//
// It reuses the Clock, jobOwnerKey, apiGVStr, scheduledTimeAnnotation, and
// condition-type constants declared in cronjob_controller.go. Only wire up
// one of CronJobReconciler / FastCronJobReconciler in a given manager --
// both register a field indexer on the same jobOwnerKey, and the second
// SetupWithManager call would fail.

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"sync"
	"time"

	"github.com/go-logr/logr"
	"github.com/robfig/cron"
	kbatch "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	ref "k8s.io/client-go/tools/reference"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	batchv1 "tutorial.kubebuilder.io/cronjob/api/v1"
)

// maxMissedStarts caps how many missed schedule ticks getNextSchedule will
// walk through before giving up (matches the base reconciler's threshold,
// but its error message actually said ">100" while checking >180 -- fixed
// here).
const maxMissedStarts = 180

// FastCronJobReconciler reconciles a CronJob object.
type FastCronJobReconciler struct {
	client.Client
	Scheme *runtime.Scheme
	Clock
}

// scheduleCache memoizes cron.Parse results by schedule string so a steady
// stream of reconciles for the same CronJob doesn't re-parse the same
// expression every time.
var scheduleCache sync.Map // string -> cron.Schedule

func fastParseSchedule(spec string) (cron.Schedule, error) {
	if v, ok := scheduleCache.Load(spec); ok {
		return v.(cron.Schedule), nil
	}
	sched, err := cron.Parse(spec)
	if err != nil {
		return nil, err
	}
	scheduleCache.Store(spec, sched)
	return sched, nil
}

func fastIsJobFinished(job *kbatch.Job) kbatch.JobConditionType {
	for _, c := range job.Status.Conditions {
		if (c.Type == kbatch.JobComplete || c.Type == kbatch.JobFailed) && c.Status == corev1.ConditionTrue {
			return c.Type
		}
	}
	return ""
}

func fastGetScheduledTimeForJob(job *kbatch.Job) (*time.Time, error) {
	timeRaw, ok := job.Annotations[scheduledTimeAnnotation]
	if !ok || timeRaw == "" {
		return nil, nil
	}
	timeParsed, err := time.Parse(time.RFC3339, timeRaw)
	if err != nil {
		return nil, err
	}
	return &timeParsed, nil
}

func fastGetNextSchedule(cronJob *batchv1.CronJob, now time.Time) (lastMissed, next time.Time, err error) {
	sched, err := fastParseSchedule(cronJob.Spec.Schedule)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("unable to parse schedule %q: %w", cronJob.Spec.Schedule, err)
	}

	var earliestTime time.Time
	if cronJob.Status.LastScheduleTime != nil {
		earliestTime = cronJob.Status.LastScheduleTime.Time
	} else {
		earliestTime = cronJob.CreationTimestamp.Time
	}
	if cronJob.Spec.StartingDeadlineSeconds != nil {
		schedulingDeadline := now.Add(-time.Second * time.Duration(*cronJob.Spec.StartingDeadlineSeconds))
		if schedulingDeadline.After(earliestTime) {
			earliestTime = schedulingDeadline
		}
	}

	if earliestTime.After(now) {
		return time.Time{}, sched.Next(now), nil
	}

	starts := 0
	for t := sched.Next(earliestTime); !t.After(now); t = sched.Next(t) {
		lastMissed = t
		starts++
		if starts > maxMissedStarts {
			return time.Time{}, time.Time{}, fmt.Errorf("too many missed start times (>%d); set or decrease .spec.startingDeadlineSeconds or check clock skew", maxMissedStarts)
		}
	}
	return lastMissed, sched.Next(now), nil
}

// trimJobHistory deletes the oldest jobs in excess of limit, best-effort,
// mirroring the base reconciler's history-limit cleanup.
func (r *FastCronJobReconciler) trimJobHistory(ctx context.Context, log logr.Logger, jobs []*kbatch.Job, limit int32, kind string) {
	if int32(len(jobs)) <= limit {
		return
	}
	slices.SortFunc(jobs, func(a, b *kbatch.Job) int {
		switch {
		case a.Status.StartTime == nil && b.Status.StartTime == nil:
			return 0
		case a.Status.StartTime == nil:
			return -1
		case b.Status.StartTime == nil:
			return 1
		case a.Status.StartTime.Before(b.Status.StartTime):
			return -1
		case b.Status.StartTime.Before(a.Status.StartTime):
			return 1
		default:
			return 0
		}
	})
	remove := len(jobs) - int(limit)
	for _, job := range jobs[:remove] {
		if err := r.Delete(ctx, job, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
			log.Error(err, "unable to delete old job", "job", job.Name, "kind", kind)
		}
	}
}

func (r *FastCronJobReconciler) constructJobForCronJob(cronJob *batchv1.CronJob, scheduledTime time.Time) (*kbatch.Job, error) {
	name := fmt.Sprintf("%s-%d", cronJob.Name, scheduledTime.Unix())

	annotations := make(map[string]string, len(cronJob.Spec.JobTemplate.Annotations)+1)
	maps.Copy(annotations, cronJob.Spec.JobTemplate.Annotations)
	annotations[scheduledTimeAnnotation] = scheduledTime.Format(time.RFC3339)

	labels := make(map[string]string, len(cronJob.Spec.JobTemplate.Labels))
	maps.Copy(labels, cronJob.Spec.JobTemplate.Labels)

	job := &kbatch.Job{
		ObjectMeta: metav1.ObjectMeta{
			Labels:      labels,
			Annotations: annotations,
			Name:        name,
			Namespace:   cronJob.Namespace,
		},
		Spec: *cronJob.Spec.JobTemplate.Spec.DeepCopy(),
	}
	if err := ctrl.SetControllerReference(cronJob, job, r.Scheme); err != nil {
		return nil, err
	}
	return job, nil
}

func (r *FastCronJobReconciler) updateStatus(ctx context.Context, cronJob *batchv1.CronJob, log logr.Logger) error {
	if err := r.Status().Update(ctx, cronJob); err != nil {
		log.Error(err, "unable to update CronJob status")
		return err
	}
	return nil
}

// Reconcile implements the same CronJob scheduling behavior as
// CronJobReconciler.Reconcile, but with a single status write per call. See
// the package doc comment above for the list of differences.
// nolint:gocyclo
func (r *FastCronJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var cronJob batchv1.CronJob
	if err := r.Get(ctx, req.NamespacedName, &cronJob); err != nil {
		if apierrors.IsNotFound(err) {
			log.Info("CronJob resource not found. Ignoring since the object must be deleted")
			return ctrl.Result{}, nil
		}
		log.Error(err, "Failed to get CronJob")
		return ctrl.Result{}, err
	}

	var childJobs kbatch.JobList
	if err := r.List(ctx, &childJobs, client.InNamespace(req.Namespace), client.MatchingFields{jobOwnerKey: req.Name}); err != nil {
		log.Error(err, "unable to list child jobs")
		return ctrl.Result{}, err
	}

	activeJobs := make([]*kbatch.Job, 0, len(childJobs.Items))
	successfulJobs := make([]*kbatch.Job, 0, len(childJobs.Items))
	failedJobs := make([]*kbatch.Job, 0, len(childJobs.Items))
	var mostRecentTime *time.Time

	for i := range childJobs.Items {
		job := &childJobs.Items[i]
		switch fastIsJobFinished(job) {
		case "":
			activeJobs = append(activeJobs, job)
		case kbatch.JobFailed:
			failedJobs = append(failedJobs, job)
		case kbatch.JobComplete:
			successfulJobs = append(successfulJobs, job)
		}

		scheduledTimeForJob, err := fastGetScheduledTimeForJob(job)
		if err != nil {
			log.Error(err, "unable to parse schedule time for child job", "job", job.Name)
			continue
		}
		if scheduledTimeForJob != nil && (mostRecentTime == nil || mostRecentTime.Before(*scheduledTimeForJob)) {
			mostRecentTime = scheduledTimeForJob
		}
	}

	if mostRecentTime != nil {
		cronJob.Status.LastScheduleTime = &metav1.Time{Time: *mostRecentTime}
	} else {
		cronJob.Status.LastScheduleTime = nil
	}

	cronJob.Status.Active = nil
	for _, activeJob := range activeJobs {
		jobRef, err := ref.GetReference(r.Scheme, activeJob)
		if err != nil {
			log.Error(err, "unable to make reference to active job", "job", activeJob.Name)
			continue
		}
		cronJob.Status.Active = append(cronJob.Status.Active, *jobRef)
	}

	log.V(1).Info("job count", "active jobs", len(activeJobs), "successful jobs", len(successfulJobs), "failed jobs", len(failedJobs))

	isSuspended := cronJob.Spec.Suspend != nil && *cronJob.Spec.Suspend

	switch {
	case isSuspended:
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type: typeAvailableCronJob, Status: metav1.ConditionFalse, Reason: "Suspended", Message: "CronJob is suspended",
		})
	case len(failedJobs) > 0:
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type: typeDegradedCronJob, Status: metav1.ConditionTrue, Reason: "JobsFailed",
			Message: fmt.Sprintf("%d job(s) have failed", len(failedJobs)),
		})
	case len(activeJobs) > 0:
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type: typeProgressingCronJob, Status: metav1.ConditionTrue, Reason: "JobsActive",
			Message: fmt.Sprintf("%d job(s) are currently active", len(activeJobs)),
		})
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type: typeAvailableCronJob, Status: metav1.ConditionTrue, Reason: "JobsActive",
			Message: fmt.Sprintf("CronJob is progressing with %d active job(s)", len(activeJobs)),
		})
	default:
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type: typeAvailableCronJob, Status: metav1.ConditionTrue, Reason: "AllJobsCompleted",
			Message: "All jobs have completed successfully",
		})
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type: typeProgressingCronJob, Status: metav1.ConditionTrue, Reason: "NoJobsActive",
			Message: "No jobs are currently active",
		})
	}

	// NB: deleting these is "best effort" -- same as the base reconciler.
	if cronJob.Spec.FailedJobHistoryLimit != nil {
		r.trimJobHistory(ctx, log, failedJobs, *cronJob.Spec.FailedJobHistoryLimit, "failed")
	}
	if cronJob.Spec.SuccessfulJobHistoryLimit != nil {
		r.trimJobHistory(ctx, log, successfulJobs, *cronJob.Spec.SuccessfulJobHistoryLimit, "successful")
	}

	if isSuspended {
		log.V(1).Info("cronjob suspended, skipping")
		return ctrl.Result{}, r.updateStatus(ctx, &cronJob, log)
	}

	missedRun, nextRun, err := fastGetNextSchedule(&cronJob, r.Now())
	if err != nil {
		log.Error(err, "unable to figure out CronJob schedule")
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type: typeDegradedCronJob, Status: metav1.ConditionTrue, Reason: "InvalidSchedule",
			Message: fmt.Sprintf("Failed to parse schedule: %v", err),
		})
		// we don't really care about requeuing until we get an update that
		// fixes the schedule, so don't return an error
		return ctrl.Result{}, r.updateStatus(ctx, &cronJob, log)
	}

	now := r.Now()
	scheduledResult := ctrl.Result{RequeueAfter: nextRun.Sub(now)}
	log = log.WithValues("now", now, "next run", nextRun)

	if missedRun.IsZero() {
		log.V(1).Info("no upcoming schedule, sleeping until then")
		return scheduledResult, r.updateStatus(ctx, &cronJob, log)
	}

	log = log.WithValues("current run", missedRun)
	tooLate := cronJob.Spec.StartingDeadlineSeconds != nil &&
		missedRun.Add(time.Duration(*cronJob.Spec.StartingDeadlineSeconds)*time.Second).Before(now)

	if tooLate {
		log.V(1).Info("missed starting deadline for last run, sleeping till next")
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type: typeDegradedCronJob, Status: metav1.ConditionTrue, Reason: "MissedSchedule",
			Message: fmt.Sprintf("Missed starting deadline for run at %v", missedRun),
		})
		return scheduledResult, r.updateStatus(ctx, &cronJob, log)
	}

	// figure out how to run this job -- concurrency policy might forbid us from running
	// multiple jobs at the same time...
	if cronJob.Spec.ConcurrencyPolicy == batchv1.ForbidConcurrent && len(activeJobs) > 0 {
		log.V(1).Info("concurrent policy blocks concurrent runs, skipping", "num active", len(activeJobs))
		return scheduledResult, r.updateStatus(ctx, &cronJob, log)
	}

	// ...or instruct us to replace existing ones...
	if cronJob.Spec.ConcurrencyPolicy == batchv1.ReplaceConcurrent {
		for _, activeJob := range activeJobs {
			if err := r.Delete(ctx, activeJob, client.PropagationPolicy(metav1.DeletePropagationBackground)); client.IgnoreNotFound(err) != nil {
				log.Error(err, "unable to delete active job", "job", activeJob.Name)
				return ctrl.Result{}, r.updateStatus(ctx, &cronJob, log)
			}
		}
	}

	job, err := r.constructJobForCronJob(&cronJob, missedRun)
	if err != nil {
		log.Error(err, "unable to construct job for template")
		// don't retry immediately; this failure occurred while constructing the Job.
		// we'll reconcile again at the next scheduled run, and an update to the
		// CronJob can also trigger reconciliation sooner
		return scheduledResult, r.updateStatus(ctx, &cronJob, log)
	}

	if err := r.Create(ctx, job); err != nil {
		log.Error(err, "unable to create Job for CronJob", "job", job.Name)
		meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
			Type: typeDegradedCronJob, Status: metav1.ConditionTrue, Reason: "JobCreationFailed",
			Message: fmt.Sprintf("Failed to create job: %v", err),
		})
		if updateErr := r.updateStatus(ctx, &cronJob, log); updateErr != nil {
			return ctrl.Result{}, updateErr
		}
		return ctrl.Result{}, err
	}

	log.V(1).Info("created job for CronJob run", "job", job.Name)
	meta.SetStatusCondition(&cronJob.Status.Conditions, metav1.Condition{
		Type: typeProgressingCronJob, Status: metav1.ConditionTrue, Reason: "JobCreated",
		Message: fmt.Sprintf("Created job: %s", job.Name),
	})

	// we'll requeue once we see the running job, and update our job
	return ctrl.Result{}, r.updateStatus(ctx, &cronJob, log)
}

// SetupWithManager sets up the controller with the Manager. Only wire up
// one of CronJobReconciler / FastCronJobReconciler -- both index Jobs by
// jobOwnerKey, so registering the field indexer twice in the same manager
// fails.
func (r *FastCronJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if r.Clock == nil {
		r.Clock = realClock{}
	}

	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &kbatch.Job{}, jobOwnerKey, func(rawObj client.Object) []string {
		job := rawObj.(*kbatch.Job)
		owner := metav1.GetControllerOf(job)
		if owner == nil {
			return nil
		}
		if owner.APIVersion != apiGVStr || owner.Kind != "CronJob" {
			return nil
		}
		return []string{owner.Name}
	}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&batchv1.CronJob{}).
		Owns(&kbatch.Job{}).
		Named("cronjob-fast").
		Complete(r)
}
