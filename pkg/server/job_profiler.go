// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/zipper"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RequestJobProfilerExecutionDetails requests the profiler details for a job.
// This method ensures that the details are requested on the current coordinator
// node of the job to allow for the collection of Resumer specific details.
func (s *statusServer) RequestJobProfilerExecutionDetails(
	ctx context.Context, req *serverpb.RequestJobProfilerExecutionDetailsRequest,
) (*serverpb.RequestJobProfilerExecutionDetailsResponse, error) {
	ctx = s.AnnotateCtx(ctx)
	// TODO(adityamaru): Figure out the correct privileges required to request execution details.
	_, err := s.privilegeChecker.RequireAdminUser(ctx)
	if err != nil {
		return nil, err
	}

	execCfg := s.sqlServer.execCfg
	var coordinatorID int32
	err = execCfg.DB.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		// Find the jobs' current coordinator node ID.
		coordinatorID, err = jobs.JobCoordinatorID(ctx, jobspb.JobID(req.JobId),
			txn, execCfg.InternalDB.Executor())
		return err
	})
	if err != nil {
		return nil, err
	}

	nodeID, local, err := s.parseNodeID(strconv.Itoa(int(coordinatorID)))
	if err != nil {
		return nil, status.Errorf(codes.InvalidArgument, err.Error())
	}

	// If this node is the current coordinator of the job then we can collect the
	// profiler details.
	if local {
		jobID := jobspb.JobID(req.JobId)
		if !execCfg.Settings.Version.IsActive(ctx, clusterversion.V23_2) {
			return nil, errors.Newf("execution details can only be requested on a cluster with version >= %s",
				clusterversion.V23_2.String())
		}
		e := makeJobProfilerExecutionDetailsBuilder(execCfg.SQLStatusServer, execCfg.InternalDB, jobID, execCfg.JobRegistry)

		// TODO(adityamaru): When we start collecting more information we can consider
		// parallelize the collection of the various pieces.
		e.addDistSQLDiagram(ctx)
		e.addLabelledGoroutines(ctx)
		e.addClusterWideTraces(ctx)

		// TODO(dt,adityamaru): add logic to reach out the registry and call resumer
		// specific execution details collection logic.

		return &serverpb.RequestJobProfilerExecutionDetailsResponse{}, nil
	}

	// Forward the request to the coordinator node
	status, err := s.dialNode(ctx, nodeID)
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	return status.RequestJobProfilerExecutionDetails(ctx, req)
}

// executionDetailsBuilder can be used to read and write execution details corresponding
// to a job.
type executionDetailsBuilder struct {
	srv      serverpb.SQLStatusServer
	db       isql.DB
	jobID    jobspb.JobID
	registry *jobs.Registry
}

// makeJobProfilerExecutionDetailsBuilder returns an instance of an executionDetailsBuilder.
func makeJobProfilerExecutionDetailsBuilder(
	srv serverpb.SQLStatusServer, db isql.DB, jobID jobspb.JobID, registry *jobs.Registry,
) executionDetailsBuilder {
	e := executionDetailsBuilder{
		srv: srv, db: db, jobID: jobID, registry: registry,
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
		log.Errorf(ctx, "failed to collect goroutines for job %d: %v", e.jobID, err.Error())
		return
	}
	filename := fmt.Sprintf("goroutines.%s.txt", timeutil.Now().Format("20060102_150405.00"))
	if err := e.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return jobs.WriteExecutionDetailFile(ctx, filename, resp.Data, txn, e.jobID)
	}); err != nil {
		log.Errorf(ctx, "failed to write goroutine for job %d: %v", e.jobID, err.Error())
	}
}

// addDistSQLDiagram generates and persists a `distsql.<timestamp>.html` file.
func (e *executionDetailsBuilder) addDistSQLDiagram(ctx context.Context) {
	query := `SELECT plan_diagram FROM [SHOW JOB $1 WITH EXECUTION DETAILS]`
	row, err := e.db.Executor().QueryRowEx(ctx, "profiler-bundler-add-diagram", nil, /* txn */
		sessiondata.NoSessionDataOverride, query, e.jobID)
	if err != nil {
		log.Errorf(ctx, "failed to write DistSQL diagram for job %d: %v", e.jobID, err.Error())
		return
	}
	if row != nil && row[0] != tree.DNull {
		dspDiagramURL := string(tree.MustBeDString(row[0]))
		filename := fmt.Sprintf("distsql.%s.html", timeutil.Now().Format("20060102_150405.00"))
		if err := e.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
			return jobs.WriteExecutionDetailFile(ctx, filename,
				[]byte(fmt.Sprintf(`<meta http-equiv="Refresh" content="0; url=%s">`, dspDiagramURL)),
				txn, e.jobID)
		}); err != nil {
			log.Errorf(ctx, "failed to write DistSQL diagram for job %d: %v", e.jobID, err.Error())
		}
	}
}

// addClusterWideTraces generates and persists a `trace.<timestamp>.zip` file
// that captures the active tracing spans of a job on all nodes in the cluster.
func (e *executionDetailsBuilder) addClusterWideTraces(ctx context.Context) {
	z := zipper.MakeInternalExecutorInflightTraceZipper(e.db.Executor())
	traceID, err := jobs.GetJobTraceID(ctx, e.db, e.jobID)
	if err != nil {
		log.Warningf(ctx, "failed to fetch job trace ID: %+v", err.Error())
		return
	}
	zippedTrace, err := z.Zip(ctx, int64(traceID))
	if err != nil {
		log.Errorf(ctx, "failed to collect cluster wide traces for job %d: %v", e.jobID, err.Error())
		return
	}

	filename := fmt.Sprintf("trace.%s.zip", timeutil.Now().Format("20060102_150405.00"))
	if err := e.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		return jobs.WriteExecutionDetailFile(ctx, filename, zippedTrace, txn, e.jobID)
	}); err != nil {
		log.Errorf(ctx, "failed to write traces for job %d: %v", e.jobID, err.Error())
	}
}
