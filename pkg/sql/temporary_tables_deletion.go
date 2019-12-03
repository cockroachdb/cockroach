// Copyright 2019 The Cockroach Authors.
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
	"errors"
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
)

// deleteTempTablesResumer implements the jobs.Resumer interface.
type deleteTempTablesResumer struct {
	job *jobs.Job
}

var _ jobs.Resumer = &deleteTempTablesResumer{}

// defaultCleanupInterval is the interval of time after which a background job
// runs to cleanup temporary tables that weren't deleted at session exit.
var defaultCleanupInterval = 30 * time.Minute

// Resume is part of the jobs.Resumer interface.
func (r *deleteTempTablesResumer) Resume(
	ctx context.Context, phs interface{}, resultsCh chan<- tree.Datums,
) error {
	p := phs.(*planner)
	p.txn = client.NewTxn(ctx, p.ExecCfg().DB, 0, client.RootTxn)

	// There should only be one temp table deletion job at any point.
	if err := checkRunningTempTablesJob(ctx, r.job, p); err != nil {
		return err
	}

	req := p.makeSessionsRequest(ctx)
	response, err := p.extendedEvalCtx.StatusServer.ListSessions(ctx, &req)
	if err != nil {
		return err
	}
	activeSessions := make(map[uint128.Uint128]bool)
	for _, session := range response.Sessions {
		activeSessions[uint128.FromBytes(session.ID)] = true
	}
	dbIDs, err := GetAllDatabaseDescriptorIDs(ctx, p.txn)
	if err != nil {
		return err
	}
	for _, dbID := range dbIDs {
		schemaNames, err := p.GetAllSchemaNames(ctx, dbID, p.txn)
		if err != nil {
			return err
		}
		for _, scName := range schemaNames {
			isTempSchema, sessionID, err := temporarySchemaSessionID(scName)
			if err != nil {
				return err
			}
			if isTempSchema && !activeSessions[sessionID.Uint128] {
				if err := cleanupSessionTempObjects(ctx, p, sessionID); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// OnFailOrCancel is part of the jobs.Resumer interface.
func (r *deleteTempTablesResumer) OnFailOrCancel(context.Context, *client.Txn) error {
	return nil
}

// OnSuccess is part of the jobs.Resumer interface.
func (r *deleteTempTablesResumer) OnSuccess(ctx context.Context, _ *client.Txn) error { return nil }

// OnTerminal is part of the jobs.Resumer interface.
func (r *deleteTempTablesResumer) OnTerminal(context.Context, jobs.Status, chan<- tree.Datums) {}

type concurrentDeleteTempTablesError struct{}

var _ error = concurrentDeleteTempTablesError{}

func (concurrentDeleteTempTablesError) Error() string {
	return "another delete temp tables job is already running"
}

// ConcurrentDeleteTempTablesError is reported when two delete temp tables jobs
// are issued concurrently. This is a sentinel error.
var ConcurrentDeleteTempTablesError error = concurrentDeleteTempTablesError{}

func checkRunningTempTablesJob(ctx context.Context, job *jobs.Job, p *planner) error {
	var jobID int64
	if job != nil {
		jobID = *job.ID()
	}
	const stmt = `SELECT id, payload FROM system.jobs WHERE status IN ($1, $2, $3) ORDER BY created`

	rows, err := p.ExecCfg().InternalExecutor.Query(
		ctx,
		"get-jobs",
		nil, /* txn */
		stmt,
		jobs.StatusPending,
		jobs.StatusRunning,
		jobs.StatusPaused,
	)
	if err != nil {
		return err
	}

	for _, row := range rows {
		payload, err := jobs.UnmarshalPayload(row[1])
		if err != nil {
			return err
		}

		if payload.Type() == jobspb.TypeDeleteTempTables {
			id := (*int64)(row[0].(*tree.DInt))
			if *id == jobID {
				break
			}

			// This is not the first DeleteTempTables job running. This job should fail
			// so that the earlier job can succeed.
			return ConcurrentDeleteTempTablesError
		}
	}
	return nil
}

func (p *planner) startJob(ctx context.Context, resultsCh chan<- tree.Datums) error {
	if err := checkRunningTempTablesJob(ctx, nil /* job */, p); err != nil {
		return err
	}
	record := jobs.Record{
		Description: "delete temp tables of inactive sessions",
		Username:    p.User(),
		Details:     jobspb.DeleteTempTablesDetails{},
		Progress:    jobspb.DeleteTempTablesProgress{},
	}

	job, errCh, err := p.ExecCfg().JobRegistry.StartJob(ctx, resultsCh, record)
	if err != nil {
		return err
	}

	if err = <-errCh; err != nil {
		if errors.Is(err, ConcurrentDeleteTempTablesError) {
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

func setupTempTableDeletionJob(ctx context.Context, config *ExecutorConfig) error {
	go func() {
		newPlanner, cleanup := newInternalPlanner("delete-temp-tables", nil /* txn */, security.RootUser, &MemoryMetrics{}, config)
		defer cleanup()
		resultsCh := make(chan tree.Datums)
		errCh := make(chan error)
		err := newPlanner.startJob(ctx, resultsCh)
		select {
		case <-ctx.Done():
		case errCh <- err:
		}
		close(errCh)
		close(resultsCh)
	}()
	return nil
}

func SetupBackgroundDeletionJob(stopper *stop.Stopper, config *ExecutorConfig) error {
	stopper.RunWorker(context.Background(), func(ctx context.Context) {
		timer := time.NewTimer(defaultCleanupInterval)
		defer timer.Stop()

		for {
			select {
			case <-timer.C:
				if err := stopper.RunAsyncTask(
					ctx, "maybeDeleteTemporaryTables", func(ctx context.Context) {
						if err := setupTempTableDeletionJob(ctx, config); err != nil {
							return
						}

						timer.Reset(defaultCleanupInterval)
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
	jobs.RegisterConstructor(jobspb.TypeDeleteTempTables, func(job *jobs.Job, settings *cluster.Settings) jobs.Resumer {
		return &deleteTempTablesResumer{job: job}
	})
}
