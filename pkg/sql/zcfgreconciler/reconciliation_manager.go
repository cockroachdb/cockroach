// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package zcfgreconciler

import (
	"context"
	"errors"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// ZcfgReconciliationManager is responsible for ensuring one and only one zone
// config reconciliation job exists.
type ZcfgReconciliationManager struct {
	DB *kv.DB

	jobRegistry *jobs.Registry

	ie *sql.InternalExecutor
}

// NewZcfgReconciliationManager initializes a ZcfgReconciliationManager with the
// given arguments but does not start it.
func NewZcfgReconciliationManager(
	db *kv.DB, registry *jobs.Registry, ie *sql.InternalExecutor,
) *ZcfgReconciliationManager {
	return &ZcfgReconciliationManager{
		DB:          db,
		jobRegistry: registry,
		ie:          ie,
	}
}

// Start will begin the ZcfgReconciliationManager's loop.
func (z *ZcfgReconciliationManager) Start(ctx context.Context, stopper *stop.Stopper) {
	_ = stopper.RunAsyncTask(ctx, "zcfg-reconciliation-manager", func(ctx context.Context) {
		for {
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-ctx.Done():
				return
			default:
				if err := z.startZcfgReconciliationJob(ctx); err != nil {
					log.Infof(ctx, "zcfg reconciliation error: err", err)
				}
				return
			}
		}
	})
}

// startZcfgReconciliationJob starts the ZcfgReconciliationJob iff one hasn't
// been started already.
func (z *ZcfgReconciliationManager) startZcfgReconciliationJob(ctx context.Context) error {
	var job *jobs.StartableJob
	record := jobs.Record{
		Description:   "zcfg reconciliation job",
		Username:      security.RootUserName(),
		Details:       jobspb.ZcfgReconciliationDetails{},
		Progress:      jobspb.ZcfgReconciliationProgress{},
		NonCancelable: true,
	}

	if err := z.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		err := z.checkZcfgReconciliationJobExists(ctx, txn)
		if err != nil {
			return err
		}
		return z.jobRegistry.CreateStartableJobWithTxn(ctx, &job, zcfgReconciliationJobID, txn, record)
	}); err != nil {
		if job != nil {
			cleanupErr := job.CleanupOnRollback(ctx)
			if cleanupErr != nil {
				log.Warningf(ctx, "failed to cleanup StartableJob: %v", cleanupErr)
			}
		}
		// Nothing to do here.
		if errors.Is(err, ZcfgReconciliationJobExistsError) {
			log.Infof(ctx, "zcfg job already exists, no need to start one up")
			return nil
		}
		return err
	}

	if err := job.Start(ctx); err != nil {
		return err
	}

	return nil
}

// checkZcfgReconciliationJobExists checks to see if a ZcfgReconciliationJob
// already exists, and if it does, it returns a sentinel
// ZcfgReconciliationJobExistsError error.
func (z *ZcfgReconciliationManager) checkZcfgReconciliationJobExists(
	ctx context.Context, txn *kv.Txn,
) error {
	const stmt = `SELECT id, payload FROM system.jobs WHERE id=$1 AND status IN ($2, $3, $4)`
	rows, err := z.ie.QueryRow(
		ctx,
		"get-zcfg-jobs",
		txn,
		stmt,
		zcfgReconciliationJobID,
		jobs.StatusRunning,
		jobs.StatusPending,
		jobs.StatusPaused,
	)
	if err != nil {
		return err
	}
	if len(rows) > 0 {
		return ZcfgReconciliationJobExistsError
	}
	return nil
}
