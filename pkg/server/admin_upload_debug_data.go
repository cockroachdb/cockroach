// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// StartUploadDebugData creates a background job that uploads debug
// data from all nodes to the specified upload server.
func (s *systemAdminServer) StartUploadDebugData(
	ctx context.Context, req *serverpb.StartUploadDebugDataRequest,
) (*serverpb.StartUploadDebugDataResponse, error) {
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewClusterMetadataPermission(ctx); err != nil {
		return nil, err
	}

	if !s.sqlServer.execCfg.Settings.Version.IsActive(ctx, clusterversion.V26_2_UploadDebugDataJob) {
		return nil, status.Error(codes.FailedPrecondition,
			"all nodes must be upgraded to 26.2 before using upload debug data jobs")
	}

	if req.ServerUrl == "" {
		return nil, status.Error(codes.InvalidArgument, "server_url is required")
	}
	if req.ApiKey == "" {
		return nil, status.Error(codes.InvalidArgument, "api_key is required")
	}

	registry := s.sqlServer.jobRegistry

	var jobID jobspb.JobID
	if err := s.sqlServer.internalDB.Txn(ctx, func(
		ctx context.Context, txn isql.Txn,
	) error {
		jobID = registry.MakeJobID()
		record := jobs.Record{
			JobID:       jobID,
			Description: "upload debug data to " + req.ServerUrl,
			Username:    username.NodeUserName(),
			Details: jobspb.UploadDebugDataDetails{
				ServerUrl:              req.ServerUrl,
				ApiKey:                 req.ApiKey,
				Redact:                 req.Redact,
				CpuProfSeconds:         req.CpuProfSeconds,
				Labels:                 req.Labels,
				IncludeRangeInfo:       req.IncludeRangeInfo,
				IncludeGoroutineStacks: req.IncludeGoroutineStacks,
				NodeIds:                req.NodeIds,
				ReuploadSessionId:      req.ReuploadSessionId,
			},
			Progress: jobspb.UploadDebugDataProgress{},
		}
		_, err := registry.CreateAdoptableJobWithTxn(ctx, record, jobID, txn)
		return err
	}); err != nil {
		return nil, status.Errorf(codes.Internal, "creating upload job: %v", err)
	}

	return &serverpb.StartUploadDebugDataResponse{
		JobID: int64(jobID),
	}, nil
}

// StartUploadDebugData on the base adminServer returns unimplemented
// for non-system tenants.
func (s *adminServer) StartUploadDebugData(
	ctx context.Context, req *serverpb.StartUploadDebugDataRequest,
) (*serverpb.StartUploadDebugDataResponse, error) {
	return nil, status.Error(codes.Unimplemented,
		"upload debug data is not supported on secondary tenants")
}
