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
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// checkAndStartReconciliationJobInterval is a cluster setting to control how
// often the existence of the automatic span config reconciliation job will be
// checked. If the check concludes that the job doesn't exist it will be started.
var checkAndStartReconciliationJobInterval = settings.RegisterDurationSetting(
	"spanconfig.reconciliation_job.check_interval",
	"the frequency at which to check if the span config reconciliation job exists (and to start it if not)",
	10*time.Minute,
	settings.NonNegativeDuration,
)

// Manager is the coordinator of the span config subsystem. It is responsible
// for the following tasks:
//
// 1. Ensuring that one (and only one) span config reconciliation job exists for
// every tenant.
// 2. Encapsulating all dependencies required by the span config reconciliation
// job to perform its task.
type Manager struct {
	db       *kv.DB
	jr       *jobs.Registry
	ie       sqlutil.InternalExecutor
	stopper  *stop.Stopper
	settings *cluster.Settings
	knobs    *spanconfig.TestingKnobs

	spanconfig.KVAccessor
}

var _ spanconfig.ReconciliationDependencies = &Manager{}

// New constructs a new Manager.
func New(
	db *kv.DB,
	jr *jobs.Registry,
	ie sqlutil.InternalExecutor,
	stopper *stop.Stopper,
	settings *cluster.Settings,
	kvAccessor spanconfig.KVAccessor,
	knobs *spanconfig.TestingKnobs,
) *Manager {
	if knobs == nil {
		knobs = &spanconfig.TestingKnobs{}
	}
	return &Manager{
		db:         db,
		jr:         jr,
		ie:         ie,
		stopper:    stopper,
		settings:   settings,
		knobs:      knobs,
		KVAccessor: kvAccessor,
	}
}

// Start creates a background task that starts the auto span config
// reconciliation job. The background task also periodically (as dictated by the
// cluster setting) checks to ensure that the job exists. We don't expect this
// to happen, but if the periodic check indicates that the job doesn't exist, it
// will be started again.
func (m *Manager) Start(ctx context.Context) error {
	return m.stopper.RunAsyncTask(ctx, "span-config-mgr", func(ctx context.Context) {
		m.run(ctx)
	})
}

func (m *Manager) run(ctx context.Context) {
	reconciliationIntervalChanged := make(chan struct{}, 1)
	checkAndStartReconciliationJobInterval.SetOnChange(
		&m.settings.SV, func(ctx context.Context) {
			select {
			case reconciliationIntervalChanged <- struct{}{}:
			default:
			}
		})

	lastChecked := time.Time{}
	timer := timeutil.NewTimer()
	defer timer.Stop()
	// Periodically check the span config reconciliation job exists and start it
	// if for some reason it does not.
	for {
		timer.Reset(timeutil.Until(
			lastChecked.Add(checkAndStartReconciliationJobInterval.Get(&m.settings.SV)),
		))
		select {
		case <-timer.C:
			timer.Read = true
			if m.settings.Version.IsActive(ctx, clusterversion.AutoSpanConfigReconciliationJob) {
				started, err := m.createAndStartJobIfNoneExists(ctx)
				if err != nil {
					log.Errorf(ctx, "error starting auto span config reconciliation job: %v", err)
				}
				if started {
					log.Infof(ctx, "started auto span config reconciliation job")
				}
			}
			lastChecked = timeutil.Now()
		case <-reconciliationIntervalChanged:
			// loop back around
		case <-m.stopper.ShouldQuiesce():
			return
		case <-ctx.Done():
			return
		}
	}
}

// createAndStartJobIfNoneExists creates span config reconciliation job iff it
// hasn't been created already and notifies the jobs registry to adopt it.
// Returns a  boolean indicating if the job was created.
func (m *Manager) createAndStartJobIfNoneExists(ctx context.Context) (bool, error) {
	if m.knobs.ManagerDisableJobCreation {
		return false, nil
	}
	record := jobs.Record{
		JobID:         m.jr.MakeJobID(),
		Description:   "reconciling span configurations",
		Username:      security.RootUserName(),
		Details:       jobspb.AutoSpanConfigReconciliationDetails{},
		Progress:      jobspb.AutoSpanConfigReconciliationProgress{},
		NonCancelable: true,
	}

	var job *jobs.Job
	if err := m.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// TODO(arul): Switch this to use jobs.RunningJobExists once #68434 lands.
		exists, err := m.checkIfReconciliationJobExists(ctx, txn)
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
	err := m.jr.NotifyToAdoptJobs(ctx)
	return true, err
}

// checkIfReconciliationJobExists checks if an span config reconciliation job
// already exists.
func (m *Manager) checkIfReconciliationJobExists(
	ctx context.Context, txn *kv.Txn,
) (exists bool, _ error) {
	stmt := fmt.Sprintf(`
SELECT EXISTS(
         SELECT job_id
           FROM [SHOW AUTOMATIC JOBS]
          WHERE job_type = '%s'
					AND status IN %s
       );
`, jobspb.TypeAutoSpanConfigReconciliation.String(), jobs.NonTerminalStatusTupleString)
	row, err := m.ie.QueryRowEx(ctx, "check-if-reconciliation-job-already-exists", txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()}, stmt)
	if err != nil {
		return false, err
	}
	return bool(*row[0].(*tree.DBool)), nil
}
