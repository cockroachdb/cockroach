// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
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

// backupExecutionDetails is a JSON serializable struct that captures the
// execution details that are specific to BACKUPs.
type backupExecutionDetails struct {
	defaultExecutionDetails

	// PerComponentFractionProgressed is a mapping from a string representing
	// execinfra.ComponentID to the progress fraction reported by the executing
	// job for that component.
	PerComponentFractionProgressed map[string]float32 `json:"per_component_fraction_progressed"`
}

func constructBackupExecutionDetails(
	ctx context.Context, jobID jobspb.JobID, db isql.DB,
) ([]byte, error) {
	var annotatedURL url.URL
	marshallablePerComponentProgress := make(map[string]float32)
	if err := db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		// Read the latest DistSQL diagram.
		infoStorage := jobs.InfoStorageForJob(txn, jobID)
		var distSQLURL string
		if err := infoStorage.GetLast(ctx, profilerconstants.DSPDiagramInfoKeyPrefix, func(infoKey string, value []byte) error {
			distSQLURL = string(value)
			return nil
		}); err != nil {
			return err
		}

		// Read the per node, per processor fraction progressed from the
		// system.job_info table.
		perComponentProgress := make(map[execinfrapb.ComponentID]float32)
		if err := infoStorage.Iterate(ctx, profilerconstants.NodeProcessorProgressInfoKeyPrefix, func(infoKey string, value []byte) error {
			flowID, instanceID, processorID, err := profilerconstants.GetNodeProcessorProgressInfoKeyParts(infoKey)
			if err != nil {
				return err
			}
			componentID := execinfrapb.ComponentID{
				FlowID:        execinfrapb.FlowID{UUID: flowID},
				Type:          execinfrapb.ComponentID_PROCESSOR,
				ID:            int32(processorID),
				SQLInstanceID: base.SQLInstanceID(instanceID),
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

		// If the job has generated a DistSQL diagram, update it with the latest
		// progress fractions.
		if distSQLURL != "" {
			flowDiag, err := execinfrapb.FromURL(distSQLURL)
			if err != nil {
				return errors.Wrap(err, "failed to FromURL")
			}
			flowDiag.UpdateComponentFractionProgressed(perComponentProgress)

			// Re-serialize and write the updated DistSQL diagram.
			_, annotatedURL, err = flowDiag.ToURL()
			if err != nil {
				return err
			}
			key := profilerconstants.MakeDSPDiagramInfoKey(timeutil.Now().UnixNano())
			if err := infoStorage.Write(ctx, key, []byte(annotatedURL.String())); err != nil {
				return err
			}
		}

		for component, progress := range perComponentProgress {
			marshallablePerComponentProgress[component.String()] = progress
		}
		return nil
	}); err != nil {
		return nil, err
	}

	executionDetails := backupExecutionDetails{
		defaultExecutionDetails:        defaultExecutionDetails{PlanDiagram: annotatedURL.String()},
		PerComponentFractionProgressed: marshallablePerComponentProgress,
	}
	j, err := gojson.Marshal(executionDetails)
	return j, err
}

// RequestExecutionDetailFiles implements the JobProfiler interface.
func (p *planner) RequestExecutionDetailFiles(ctx context.Context, jobID jobspb.JobID) error {
	execCfg := p.ExecCfg()
	_, err := execCfg.SQLStatusServer.RequestJobProfilerExecutionDetails(ctx,
		&serverpb.RequestJobProfilerExecutionDetailsRequest{
			JobId: int64(jobID),
		})
	return err
}
