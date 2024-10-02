// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/zipper"
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
	err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx)
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
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}

	// If this node is the current coordinator of the job then we can collect the
	// profiler details.
	if local {
		jobID := jobspb.JobID(req.JobId)
		e := makeJobProfilerExecutionDetailsBuilder(execCfg.SQLStatusServer, execCfg.InternalDB, jobID, execCfg.JobRegistry)

		// TODO(adityamaru): When we start collecting more information we can consider
		// parallelize the collection of the various pieces.
		e.addDistSQLDiagram(ctx)
		e.addLabelledGoroutines(ctx)
		e.addClusterWideTraces(ctx)

		user, err := authserver.UserFromIncomingRPCContext(ctx)
		if err != nil {
			return nil, srverrors.ServerError(ctx, err)
		}
		r, err := execCfg.JobRegistry.GetResumerForClaimedJob(jobID)
		if err != nil {
			return nil, err
		}
		jobExecCtx, close := sql.MakeJobExecContext(ctx, "collect-profile", user, &sql.MemoryMetrics{}, execCfg)
		defer close()
		if err := r.CollectProfile(ctx, jobExecCtx); err != nil {
			return nil, err
		}

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
