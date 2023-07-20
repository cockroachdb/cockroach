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
	"fmt"
	"net/url"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobsprofiler/profilerconstants"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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
	if !execCfg.Settings.Version.IsActive(ctx, clusterversion.V23_2) {
		return errors.Newf("execution details can only be requested on a cluster with version >= %s",
			clusterversion.V23_2.String())
	}

	e := makeJobProfilerExecutionDetailsBuilder(execCfg.SQLStatusServer, execCfg.InternalDB, jobID)

	// Check if the job exists otherwise we can bail early.
	exists, err := jobs.JobExists(ctx, jobID, p.Txn(), e.db.Executor())
	if err != nil {
		return err
	}
	if !exists {
		return errors.Newf("job %d not found; cannot request execution details", jobID)
	}

	// TODO(adityamaru): When we start collecting more information we can consider
	// parallelize the collection of the various pieces.
	e.addDistSQLDiagram(ctx)
	e.addLabelledGoroutines(ctx)

	return nil
}

// executionDetailsBuilder can be used to read and write execution details corresponding
// to a job.
type executionDetailsBuilder struct {
	srv   serverpb.SQLStatusServer
	db    isql.DB
	jobID jobspb.JobID
}

// makeJobProfilerExecutionDetailsBuilder returns an instance of an executionDetailsBuilder.
func makeJobProfilerExecutionDetailsBuilder(
	srv serverpb.SQLStatusServer, db isql.DB, jobID jobspb.JobID,
) executionDetailsBuilder {
	e := executionDetailsBuilder{
		srv: srv, db: db, jobID: jobID,
	}
	return e
}

// addLabelledGoroutines collects and persists goroutines from all nodes in the
// cluster that have a pprof label tying it to the job whose execution details
// are being collected.
func (e *executionDetailsBuilder) addLabelledGoroutines(ctx context.Context) {
	profileRequest := serverpb.ProfileRequest{
		NodeId:      "all",
		Type:        serverpb.ProfileRequest_GOROUTINE,
		Labels:      true,
		LabelFilter: fmt.Sprintf("%d", e.jobID),
	}
	resp, err := e.srv.Profile(ctx, &profileRequest)
	if err != nil {
		log.Errorf(ctx, "failed to collect goroutines for job %d: %+v", e.jobID, err.Error())
		return
	}
	filename := fmt.Sprintf("goroutines.%s.txt", timeutil.Now().Format("20060102_150405.00"))
	if err := jobs.WriteExecutionDetailFile(ctx, filename, resp.Data, e.db, e.jobID); err != nil {
		log.Errorf(ctx, "failed to write goroutine for job %d: %+v", e.jobID, err.Error())
	}
}

// addDistSQLDiagram generates and persists a `distsql.<timestamp>.html` file.
func (e *executionDetailsBuilder) addDistSQLDiagram(ctx context.Context) {
	query := `SELECT plan_diagram FROM [SHOW JOB $1 WITH EXECUTION DETAILS]`
	row, err := e.db.Executor().QueryRowEx(ctx, "profiler-bundler-add-diagram", nil, /* txn */
		sessiondata.NoSessionDataOverride, query, e.jobID)
	if err != nil {
		log.Errorf(ctx, "failed to write DistSQL diagram for job %d: %+v", e.jobID, err.Error())
		return
	}
	if row != nil && row[0] != tree.DNull {
		dspDiagramURL := string(tree.MustBeDString(row[0]))
		filename := fmt.Sprintf("distsql.%s.html", timeutil.Now().Format("20060102_150405.00"))
		if err := jobs.WriteExecutionDetailFile(ctx, filename,
			[]byte(fmt.Sprintf(`<meta http-equiv="Refresh" content="0; url=%s">`, dspDiagramURL)),
			e.db, e.jobID); err != nil {
			log.Errorf(ctx, "failed to write DistSQL diagram for job %d: %+v", e.jobID, err.Error())
		}
	}
}
