// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/errors"
)

// deleteTempObjectsResumer implements the jobs.Resumer interface.
type deleteTempObjectsResumer struct {
	job *jobs.Job
}

var _ jobs.Resumer = &deleteTempObjectsResumer{}

// defaultCleanupInterval is the interval of time after which a background job
// runs to cleanup temporary tables that weren't deleted at session exit.
var defaultCleanupInterval = 30 * time.Minute

// Resume is part of the jobs.Resumer interface.
func (r *deleteTempObjectsResumer) Resume(
	ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	p := phs.(*planner)
	p.txn = client.NewTxn(ctx, p.ExecCfg().DB, 0)

	// There should only be one temp table deletion job at any point.
	if err := checkRunningTempTablesJob(ctx, r.job, p); err != nil {
		return err
	}

	// Build a set of all session IDs with temporary objects.
	dbIDs, err := GetAllDatabaseDescriptorIDs(ctx, p.txn)
	if err != nil {
		return err
	}
	sessionIDs := make(map[ClusterWideID]struct{})
	for _, dbID := range dbIDs {
		schemaNames, err := p.Tables().getSchemasForDatabase(ctx, p.txn, dbID)
		if err != nil {
			return err
		}
		for _, scName := range schemaNames {
			isTempSchema, sessionID, err := temporarySchemaSessionID(scName)
			if err != nil {
				return err
			}
			if isTempSchema {
				sessionIDs[sessionID] = struct{}{}
			}
		}
	}

	// Get active sessions.
	req := p.makeSessionsRequest(ctx)
	response, err := p.extendedEvalCtx.StatusServer.ListSessions(ctx, &req)
	if err != nil {
		return err
	}
	activeSessions := make(map[uint128.Uint128]struct{})
	for _, session := range response.Sessions {
		activeSessions[uint128.FromBytes(session.ID)] = struct{}{}
	}

	// Clean up temporary data for inactive sessions.
	for sessionID := range sessionIDs {
		if _, ok := activeSessions[sessionID.Uint128]; !ok {
			ie := r.job.MakeSessionBoundInternalExecutor(ctx, sessionDataForCleanup(sessionID))
			if err := cleanupSessionTempObjects(
				ctx, p.execCfg.Settings, r.job.DB(), ie.Exec, sessionID); err != nil {
				return err
			}
		}
	}
	return nil
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (r *deleteTempObjectsResumer) OnFailOrCancel(context.Context, interface{}) error {
	return nil
}

// OnSuccess is part of the jobs.Resumer interface.
func (r *deleteTempObjectsResumer) OnSuccess(ctx context.Context, _ *client.Txn) error {
	return nil
}

// OnTerminal is part of the jobs.Resumer interface.
func (r *deleteTempObjectsResumer) OnTerminal(context.Context, jobs.Status, chan<- tree.Datums) {}

type concurrentDeleteTempObjectsError struct{}

var _ error = concurrentDeleteTempObjectsError{}

func (concurrentDeleteTempObjectsError) Error() string {
	return "another delete temp tables job is already running"
}

// ConcurrentDeleteTempObjectsError is reported when two delete temp tables jobs
// are issued concurrently. This is a sentinel error.
var ConcurrentDeleteTempObjectsError error = concurrentDeleteTempObjectsError{}

func checkRunningTempTablesJob(ctx context.Context, job *jobs.Job, p *planner) error {
	err := checkRunningJobs(ctx, job, p, jobspb.TypeDeleteTempObjects)
	if err == DuplicateJobError {
		return ConcurrentDeleteTempObjectsError
	}
	return err
}

func startTempTablesCleanupJob(ctx context.Context, p *planner) error {
	if err := checkRunningTempTablesJob(ctx, nil /* job */, p); err != nil {
		return err
	}
	record := jobs.Record{
		Description: "delete temp tables of inactive sessions",
		Username:    p.User(),
		Details:     jobspb.DeleteTempObjectsDetails{},
		Progress:    jobspb.DeleteTempObjectsProgress{},
	}

	resultsCh := make(chan tree.Datums)
	defer close(resultsCh)
	job, errCh, err := p.ExecCfg().JobRegistry.CreateAndStartJob(ctx, resultsCh, record)
	if err != nil {
		return err
	}

	if err = <-errCh; err != nil {
		if errors.Is(err, ConcurrentDeleteTempObjectsError) {
			// Delete the job so users don't see it and get confused by the error.
			const stmt = `DELETE FROM system.jobs WHERE id = $1`
			if _ /* cols */, delErr := p.ExecCfg().InternalExecutor.Exec(
				ctx, "delete-job", nil /* txn */, stmt, *job.ID(),
			); delErr != nil {
				log.Warningf(ctx, "failed to delete job: %v", delErr)
			}
		}
	}
	return err
}

func SetupBackgroundDeletionJob(stopper *stop.Stopper, config *ExecutorConfig) error {
	stopper.RunWorker(context.Background(), func(ctx context.Context) {
		cleanupInterval := defaultCleanupInterval
		if config.TestingKnobs.TempTableCleanupInterval > 0 {
			cleanupInterval = config.TestingKnobs.TempTableCleanupInterval
		}
		ticker := time.NewTicker(cleanupInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				if err := stopper.RunTask(ctx, "maybeDeleteTemporaryTables", func(ctx context.Context) {
					p, cleanup := newInternalPlanner(
						"delete-temp-tables", nil /* txn */, security.RootUser, &MemoryMetrics{}, config)
					defer cleanup()
					if err := startTempTablesCleanupJob(ctx, p); err != nil {
						log.Errorf(ctx, "failed to delete temp tables: %v", err)
					}
				}); err != nil {
					log.Errorf(ctx, "failed to delete temp tables: %v", err)
				}
			case <-stopper.ShouldStop():
				return
			}
		}
	})
	return nil
}

func init() {
	jobs.RegisterConstructor(
		jobspb.TypeDeleteTempObjects, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
			return &deleteTempObjectsResumer{job: job}
		})
}
