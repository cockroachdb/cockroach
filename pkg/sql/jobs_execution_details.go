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
	"net/url"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler/profilerconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
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
	case jobspb.TypeBackup:
		executionDetailsJSON, err = constructBackupExecutionDetails(ctx, jobID, execCfg.InternalDB)
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

type backupExecutionDetails struct {
	defaultExecutionDetails
}

func constructBackupExecutionDetails(
	ctx context.Context, jobID jobspb.JobID, db isql.DB,
) ([]byte, error) {
	var annotatedURL url.URL
	if err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// Read current DistSQL diagram.
		infoStorage := jobs.InfoStorageForJob(txn, jobID)
		var distSQLURL string
		if err := infoStorage.GetLast(ctx, profilerconstants.DSPDiagramInfoKeyPrefix, func(infoKey string, value []byte) error {
			distSQLURL = string(value)
			return nil
		}); err != nil {
			return err
		}

		if distSQLURL == "" {
			return errors.New("no URL found")
		}

		// Read the per processor fraction progressed.
		perComponentProgress := make(map[execinfrapb.ComponentID]float32)
		if err := infoStorage.Iterate(ctx, profilerconstants.NodeProcessorProgressInfoKeyPrefix, func(infoKey string, value []byte) error {
			parts, err := profilerconstants.GetNodeProcessorProgressInfoKeyParts(infoKey)
			if err != nil {
				return err
			}
			flowID, instanceID, processorID := parts[0], parts[1], parts[2]
			componentID := execinfrapb.ComponentID{
				FlowID:        execinfrapb.FlowID{UUID: flowID.(uuid.UUID)},
				Type:          execinfrapb.ComponentID_PROCESSOR,
				ID:            int32(processorID.(int)),
				SQLInstanceID: base.SQLInstanceID(instanceID.(int)),
			}
			progress, err := strconv.ParseFloat(string(value), 64)
			if err != nil {
				return err
			}
			perComponentProgress[componentID] = float32(progress)
			return nil
		}); err != nil {
			return err
		}

		flowDiag, err := execinfrapb.FromURL(distSQLURL)
		if err != nil {
			return errors.Wrap(err, "failed to FromURL")
		}

		flowDiag.UpdateComponentFractionProgressed(perComponentProgress)

		// Re-serialize and write the update DistSQL diagram.
		_, annotatedURL, err = flowDiag.ToURL()
		if err != nil {
			return err
		}
		key, err := profilerconstants.MakeDSPDiagramInfoKey(timeutil.Now().UnixNano())
		if err != nil {
			return errors.Wrap(err, "failed to construct DSP info key")
		}
		return infoStorage.Write(ctx, key, []byte(annotatedURL.String()))
	}); err != nil {
		return nil, err
	}

	executionDetails := backupExecutionDetails{defaultExecutionDetails{
		PlanDiagram: annotatedURL.String()}}
	j, err := gojson.Marshal(executionDetails)
	return j, err
}
