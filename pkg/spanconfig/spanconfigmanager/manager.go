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

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// Manager is responsible for ensuring that one (and only one) span config
// reconciliation job is ever running. It also provides the job with all the
// components it needs to actually perform reconciliation.
type Manager struct {
	db    *kv.DB
	jr    *jobs.Registry
	ie    sqlutil.InternalExecutor
	knobs *spanconfig.TestingKnobs
}

var _ spanconfig.ReconciliationDependencies = &Manager{}

// New constructs a new Manager.
func New(
	db *kv.DB, jr *jobs.Registry, ie sqlutil.InternalExecutor, knobs *spanconfig.TestingKnobs,
) *Manager {
	if knobs == nil {
		knobs = &spanconfig.TestingKnobs{}
	}
	return &Manager{
		db:    db,
		jr:    jr,
		ie:    ie,
		knobs: knobs,
	}
}

// StartJobIfNoneExists will create and start the span config reconciliation job
// iff it hadn't been created already. Returns a boolean indicating if the job
// was started.
func (m *Manager) StartJobIfNoneExists(ctx context.Context) bool {
	if m.knobs.ManagerDisableJobCreation {
		return false
	}
	started, err := m.createAndStartJobIfNoneExists(ctx)
	if err != nil {
		log.Errorf(ctx, "error starting auto span config reconciliation job: %v", err)
	}
	if started {
		log.Infof(ctx, "started auto span config reconciliation job")
	}
	return started
}

// createAndStartJobIfNoneExists creates span config reconciliation job iff it
// hasn't been created already and notifies the jobs registry to adopt it.
// Returns a  boolean indicating if the job was created.
func (m *Manager) createAndStartJobIfNoneExists(ctx context.Context) (bool, error) {
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
					AND status IN ('%s', '%s', '%s')
       );
`, jobspb.TypeAutoSpanConfigReconciliation.String(), jobs.StatusRunning, jobs.StatusPaused, jobs.StatusPending)
	row, err := m.ie.QueryRowEx(ctx, "check-if-reconciliation-job-already-exists", txn,
		sessiondata.InternalExecutorOverride{User: security.RootUserName()}, stmt)
	if err != nil {
		return false, err
	}
	return bool(*row[0].(*tree.DBool)), nil
}
