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

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// Manager is responsible for ensuring that only one (and only one) span config
// reconciliation job is ever created.
type Manager struct {
	db    *kv.DB
	jr    *jobs.Registry
	ie    tree.InternalExecutor
	knobs *spanconfig.TestingKnobs

	spanconfig.Accessor
}

var _ spanconfig.ReconciliationDependencies = &Manager{}

// New constructs a new reconciliation manager.
func New(
	db *kv.DB,
	jr *jobs.Registry,
	ie tree.InternalExecutor,
	accessor spanconfig.Accessor,
	knobs *spanconfig.TestingKnobs,
) *Manager {
	if knobs == nil {
		knobs = &spanconfig.TestingKnobs{}
	}
	return &Manager{
		db:       db,
		jr:       jr,
		ie:       ie,
		knobs:    knobs,
		Accessor: accessor,
	}
}

// StartJobIfNoneExist will create and start the span config
// reconciliation job iff it hadn't been created already.
func (m *Manager) StartJobIfNoneExist(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(ctx, "create-and-start-reconciliation-job", func(ctx context.Context) {
		if interceptor := m.knobs.PreJobCreationInterceptor; interceptor != nil {
			interceptor()
		}
		if err := m.createAndStartJobIfNoneExist(ctx); err != nil {
			log.Errorf(ctx, "span config reconciliation error: %v", err)
		}
	})
}

func (m *Manager) createAndStartJobIfNoneExist(ctx context.Context) error {
	record := jobs.Record{
		Description:   "reconciling span configurations",
		Username:      security.RootUserName(),
		Details:       jobspb.AutoSpanConfigReconciliationDetails{},
		Progress:      jobspb.AutoSpanConfigReconciliationProgress{},
		NonCancelable: true,
	}

	var job *jobs.StartableJob
	jobID := m.jr.MakeJobID()
	if err := m.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		exists, err := m.checkIfReconciliationJobExists(ctx, txn)
		if err != nil {
			return err
		}
		if exists {
			// Nothing to do here.
			return nil
		}

		return m.jr.CreateStartableJobWithTxn(ctx, &job, jobID, txn, record)
	}); err != nil {
		if job != nil {
			if err := job.CleanupOnRollback(ctx); err != nil {
				log.Warningf(ctx, "failed to cleanup reconciliation job: %v", err)
			}
		}

		return err
	}

	if job == nil {
		return nil
	}
	return job.Start(ctx)
}

func (m *Manager) checkIfReconciliationJobExists(
	ctx context.Context, txn *kv.Txn,
) (exists bool, _ error) {
	const stmt = `
SELECT EXISTS(
         SELECT job_id
           FROM [SHOW AUTOMATIC JOBS]
          WHERE job_type = 'AUTO SPAN CONFIG RECONCILIATION'
       );
`
	row, err := m.ie.QueryRow(ctx, "check-if-reconciliation-job-already-exists", txn, stmt)
	if err != nil {
		return false, err
	}
	return bool(*row[0].(*tree.DBool)), nil
}
