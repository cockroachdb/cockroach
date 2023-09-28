// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfigmanager

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// checkReconciliationJobInterval is a cluster setting to control how often we
// check if the span config reconciliation job exists. If it's not found, it
// will be started. It has no effect unless
// spanconfig.reconciliation_job.enabled is configured.
var checkReconciliationJobInterval = settings.RegisterDurationSetting(
	settings.TenantWritable,
	"spanconfig.reconciliation_job.check_interval",
	"the frequency at which to check if the span config reconciliation job exists (and to start it if not)",
	10*time.Minute,
	settings.NonNegativeDuration,
)

// jobEnabledSetting gates the activation of the span config reconciliation job.
//
// TODO(irfansharif): This should be a tenant read-only setting once the work
// for #73349 is completed.
var jobEnabledSetting = settings.RegisterBoolSetting(
	settings.TenantWritable,
	"spanconfig.reconciliation_job.enabled",
	"enable the use of the kv accessor", true)

// Manager is the coordinator of the span config subsystem. It ensures that
// there's only one span config reconciliation job[1] for every tenant. It also
// captures all relevant dependencies for the job.
//
// [1]: The reconciliation job is responsible for reconciling a tenant's zone
//
//	configurations with the clusters span configurations.
type Manager struct {
	db       isql.DB
	jr       *jobs.Registry
	stopper  *stop.Stopper
	settings *cluster.Settings
	knobs    *spanconfig.TestingKnobs

	spanconfig.Reconciler
}

// New constructs a new Manager.
func New(
	idb isql.DB,
	jr *jobs.Registry,
	stopper *stop.Stopper,
	settings *cluster.Settings,
	reconciler spanconfig.Reconciler,
	knobs *spanconfig.TestingKnobs,
) *Manager {
	if knobs == nil {
		knobs = &spanconfig.TestingKnobs{}
	}
	return &Manager{
		db:         idb,
		jr:         jr,
		stopper:    stopper,
		settings:   settings,
		Reconciler: reconciler,
		knobs:      knobs,
	}
}

// Start creates a background task that starts the auto span config
// reconciliation job. It also periodically ensures that the job exists,
// recreating it if it doesn't.
func (m *Manager) Start(ctx context.Context) error {
	return m.stopper.RunAsyncTask(ctx, "span-config-mgr", func(ctx context.Context) {
		m.run(ctx)
	})
}

func (m *Manager) run(ctx context.Context) {
	jobCheckCh := make(chan struct{}, 1)
	triggerJobCheck := func() {
		select {
		case jobCheckCh <- struct{}{}:
		default:
		}
	}

	// We have a few conditions that should trigger a job check:
	// - when the setting to enable/disable the reconciliation job is toggled;
	// - when the setting controlling the reconciliation job check interval is
	//   changed;
	// - when the cluster version is changed; if we don't it's possible to have
	//   started a tenant pod with a conservative view of the cluster version,
	//   skip starting the reconciliation job, learning about the cluster
	//   version shortly, and only checking the job after an interval has
	//   passed.
	jobEnabledSetting.SetOnChange(&m.settings.SV, func(ctx context.Context) {
		triggerJobCheck()
	})
	checkReconciliationJobInterval.SetOnChange(&m.settings.SV, func(ctx context.Context) {
		triggerJobCheck()
	})
	m.settings.Version.SetOnChange(func(_ context.Context, _ clusterversion.ClusterVersion) {
		triggerJobCheck()
	})

	checkJob := func() {
		if fn := m.knobs.ManagerCheckJobInterceptor; fn != nil {
			fn()
		}

		if !jobEnabledSetting.Get(&m.settings.SV) {
			return
		}

		started, err := m.createAndStartJobIfNoneExists(ctx)
		if err != nil {
			log.Errorf(ctx, "error starting auto span config reconciliation job: %v", err)
		}
		if started {
			log.Infof(ctx, "started auto span config reconciliation job")
		}
	}

	// Periodically check if the span config reconciliation job exists and start
	// it if it doesn't.
	timer := timeutil.NewTimer()
	defer timer.Stop()

	triggerJobCheck()
	for {
		timer.Reset(checkReconciliationJobInterval.Get(&m.settings.SV))
		select {
		case <-jobCheckCh:
			checkJob()
		case <-timer.C:
			timer.Read = true
			checkJob()
		case <-m.stopper.ShouldQuiesce():
			return
		case <-ctx.Done():
			return
		}
	}
}

// createAndStartJobIfNoneExists creates span config reconciliation job iff it
// hasn't been created already and notifies the jobs registry to adopt it.
// Returns a boolean indicating if the job was created.
func (m *Manager) createAndStartJobIfNoneExists(ctx context.Context) (bool, error) {
	if m.knobs.ManagerDisableJobCreation {
		return false, nil
	}
	record := jobs.Record{
		JobID:         m.jr.MakeJobID(),
		Description:   "reconciling span configurations",
		Username:      username.NodeUserName(),
		Details:       jobspb.AutoSpanConfigReconciliationDetails{},
		Progress:      jobspb.AutoSpanConfigReconciliationProgress{},
		NonCancelable: true,
	}

	var job *jobs.Job
	if err := m.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		exists, err := jobs.RunningJobExists(ctx, jobspb.InvalidJobID, txn, m.settings.Version,
			jobspb.TypeAutoSpanConfigReconciliation)
		if err != nil {
			return err
		}

		if fn := m.knobs.ManagerAfterCheckedReconciliationJobExistsInterceptor; fn != nil {
			fn(exists)
		}

		if exists {
			// Nothing to do here.
			job = nil
			return nil
		}
		job, err = m.jr.CreateJobWithTxn(ctx, record, record.JobID, txn)
		return err
	}); err != nil {
		return false, err
	}

	if job == nil {
		return false, nil
	}

	if fn := m.knobs.ManagerCreatedJobInterceptor; fn != nil {
		fn(job)
	}
	m.jr.NotifyToResume(ctx, job.ID())
	return true, nil
}
