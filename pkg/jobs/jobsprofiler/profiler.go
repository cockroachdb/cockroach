// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jobsprofiler

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler/profilerconstants"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

const MaxRetainedDSPDiagramsPerJob = 5
const MaxDiagSize = 512 << 10

// dspDiagMaxCulledPerWrite limits how many old diagrams writing a new one will
// cull to try to maintain the limit of 5; typically it would cull no more than
// one in the steady-state but an upgrading cluster that has accumulated many
// old rows might try to cull more, so we bound how many are eligible at a time
// to some large but finite upper-bound.
const dspDiagMaxCulledPerWrite = 100

// StorePlanDiagram stores the DistSQL diagram generated from p in the job info
// table. The generation of the plan diagram and persistence to the info table
// are done asynchronously and this method does not block on their completion.
func StorePlanDiagram(
	ctx context.Context, stopper *stop.Stopper, p *sql.PhysicalPlan, db isql.DB, jobID jobspb.JobID,
) {
	if err := stopper.RunAsyncTask(ctx, "jobs-store-plan-diagram", func(ctx context.Context) {
		var cancel func()
		ctx, cancel = stopper.WithCancelOnQuiesce(ctx)
		defer cancel()

		flowSpecs, cleanup := p.GenerateFlowSpecs()
		defer cleanup(flowSpecs)
		_, diagURL, err := execinfrapb.GeneratePlanDiagramURL(fmt.Sprintf("job:%d", jobID), flowSpecs,
			execinfrapb.DiagramFlags{})
		if err != nil {
			log.Warningf(ctx, "plan diagram failed to generate: %v", err)
			return
		}
		diagString := diagURL.String()
		if len(diagString) > MaxDiagSize {
			log.Warningf(ctx, "plan diagram is too large (%dk) to store", len(diagString)/1024)
			return
		}
		if err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			dspKey := profilerconstants.MakeDSPDiagramInfoKey(timeutil.Now().UnixNano())
			infoStorage := jobs.InfoStorageForJob(txn, jobID)

			// Limit total retained diagrams by culling older ones as needed.
			count, err := infoStorage.Count(ctx, profilerconstants.DSPDiagramInfoKeyPrefix, profilerconstants.DSPDiagramInfoKeyMax)
			if err != nil {
				return err
			}
			const keep = MaxRetainedDSPDiagramsPerJob - 1
			if toCull := min(count-keep, dspDiagMaxCulledPerWrite); toCull > 0 {
				if err := infoStorage.DeleteRange(
					ctx, profilerconstants.DSPDiagramInfoKeyPrefix, profilerconstants.DSPDiagramInfoKeyMax, toCull,
				); err != nil {
					return err
				}
			}

			// Write the new diagram.
			return infoStorage.Write(ctx, dspKey, []byte(diagString))
		}); err != nil && ctx.Err() == nil {
			log.Warningf(ctx, "failed to write DistSQL diagram for job %d: %v", jobID, err.Error())
		}
	}); err != nil {
		log.Warningf(ctx, "failed to spawn task to generate DistSQL plan diagram for job %d: %v",
			jobID, err.Error())
	}
}

// StorePerNodeProcessorProgressFraction stores the progress fraction for each
// node and processor executing as part of the job's DistSQL flow as and when it
// is sent on the progressCh. It is the callers responsibility to close the
// progress channel once all updates have been sent.
func StorePerNodeProcessorProgressFraction(
	ctx context.Context,
	db isql.DB,
	jobID jobspb.JobID,
	perComponentProgress map[execinfrapb.ComponentID]float32,
) {
	if err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		infoStorage := jobs.InfoStorageForJob(txn, jobID)
		for componentID, fraction := range perComponentProgress {
			key := profilerconstants.MakeNodeProcessorProgressInfoKey(componentID.FlowID.String(),
				componentID.SQLInstanceID.String(), componentID.ID)
			fractionBytes := []byte(fmt.Sprintf("%f", fraction))
			return infoStorage.Write(ctx, key, fractionBytes)
		}
		return nil
	}); err != nil {
		log.Warningf(ctx, "failed to write progress fraction for job %d: %v",
			jobID, err.Error())
	}
}
