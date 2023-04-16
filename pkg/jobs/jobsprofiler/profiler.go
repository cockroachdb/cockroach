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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler/profilerconstants"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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

			infoStorage := jobs.InfoStorageForJob(txn, jobID)
			return infoStorage.Write(ctx, fmt.Sprintf(profilerconstants.DSPDiagramInfoKeyPrefix+"%d",
				timeutil.Now().UnixNano()), []byte(diagURL.String()))
		})
		if err != nil {
			log.Warningf(ctx, "failed to generate and write DistSQL diagram for job %d: %v",
				jobID, err.Error())
		}
	}); err != nil {
		log.Warningf(ctx, "failed to spawn task to generate DistSQL plan diagram for job %d: %v",
			jobID, err.Error())
	}
}

// TestingCheckForPlanDiagram is a method used in tests to wait for the
// existence of a DSP diagram for the provided jobID.
func TestingCheckForPlanDiagram(ctx context.Context, t *testing.T, db isql.DB, jobID jobspb.JobID) {
	testutils.SucceedsSoon(t, func() error {
		return db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			infoStorage := jobs.InfoStorageForJob(txn, jobID)
			var found bool
			err := infoStorage.GetLast(ctx, profilerconstants.DSPDiagramInfoKeyPrefix,
				func(infoKey string, value []byte) error {
					found = true
					return nil
				})
			if err != nil || !found {
				return errors.New("not found")
			}
			return nil
		})
	})
}
