// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

		err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			flowSpecs := p.GenerateFlowSpecs()
			_, diagURL, err := execinfrapb.GeneratePlanDiagramURL(fmt.Sprintf("job:%d", jobID), flowSpecs,
				execinfrapb.DiagramFlags{})
			if err != nil {
				return err
			}

			dspKey := profilerconstants.MakeDSPDiagramInfoKey(timeutil.Now().UnixNano())
			infoStorage := jobs.InfoStorageForJob(txn, jobID)
			return infoStorage.Write(ctx, dspKey, []byte(diagURL.String()))
		})
		// Don't log the error if the context has been canceled. This will likely be
		// when the node is shutting down and so it doesn't add value to spam the
		// logs with the error.
		if err != nil && ctx.Err() == nil {
			log.Warningf(ctx, "failed to generate and write DistSQL diagram for job %d: %v",
				jobID, err.Error())
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
