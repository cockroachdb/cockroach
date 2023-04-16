// Copyright 2023 The Cockroach Authors.
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
	gojson "encoding/json"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler/profilerconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
)

// GenerateExecutionDetailsJSON implements the Profiler interface.
func (p *planner) GenerateExecutionDetailsJSON(
	ctx context.Context, evalCtx *eval.Context, jobID jobspb.JobID,
) ([]byte, error) {
	execCfg := evalCtx.Planner.ExecutorConfig().(*ExecutorConfig)
	j, err := execCfg.JobRegistry.LoadJob(ctx, jobID)
	if err != nil {
		return nil, err
	}

	var executionDetailsJSON []byte
	payload := j.Payload()
	switch payload.Type() {
	// TODO(adityamaru): This allows different job types to implement custom
	// execution detail aggregation.
	default:
		executionDetailsJSON, err = constructDefaultExecutionDetails(ctx, jobID, execCfg.InternalDB)
	}

	return executionDetailsJSON, err
}

// defaultExecutionDetails is a JSON serializable struct that captures the
// execution details that are not specific to a particular job type.
type defaultExecutionDetails struct {
	// PlanDiagram is the URL to render the latest DistSQL plan that is being
	// executed by the job.
	PlanDiagram string `json:"plan_diagram"`
}

func constructDefaultExecutionDetails(
	ctx context.Context, jobID jobspb.JobID, db isql.DB,
) ([]byte, error) {
	executionDetails := &defaultExecutionDetails{}
	err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// Populate the latest DSP diagram URL.
		infoStorage := jobs.InfoStorageForJob(txn, jobID)
		err := infoStorage.GetLast(ctx, profilerconstants.DSPDiagramInfoKeyPrefix, func(infoKey string, value []byte) error {
			executionDetails.PlanDiagram = string(value)
			return nil
		})
		if err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	j, err := gojson.Marshal(executionDetails)
	return j, err
}
