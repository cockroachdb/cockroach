// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/metadata"
)

// stmtDiagnosticsRequest contains a subset of columns that are stored in
// system.statement_diagnostics_requests and are exposed in
// serverpb.StatementDiagnosticsReport.
type stmtDiagnosticsRequest struct {
	ID                     int
	StatementFingerprint   string
	Completed              bool
	StatementDiagnosticsID int
	RequestedAt            time.Time
	// Zero value indicates that there is no minimum latency set on the request.
	MinExecutionLatency time.Duration
	// Zero value indicates that the request never expires.
	ExpiresAt time.Time
}

type stmtDiagnostics struct {
	ID                   int
	StatementFingerprint string
	CollectedAt          time.Time
}

func (request *stmtDiagnosticsRequest) toProto() serverpb.StatementDiagnosticsReport {
	resp := serverpb.StatementDiagnosticsReport{
		Id:                     int64(request.ID),
		Completed:              request.Completed,
		StatementFingerprint:   request.StatementFingerprint,
		StatementDiagnosticsId: int64(request.StatementDiagnosticsID),
		RequestedAt:            request.RequestedAt,
		MinExecutionLatency:    request.MinExecutionLatency,
		ExpiresAt:              request.ExpiresAt,
	}
	return resp
}

func (diagnostics *stmtDiagnostics) toProto() serverpb.StatementDiagnostics {
	resp := serverpb.StatementDiagnostics{
		Id:                   int64(diagnostics.ID),
		StatementFingerprint: diagnostics.StatementFingerprint,
		CollectedAt:          diagnostics.CollectedAt,
	}
	return resp
}

// CreateStatementDiagnosticsReport creates a statement diagnostics
// request in the `system.statement_diagnostics_requests` table
// to trace the next query matching the provided fingerprint.
func (s *statusServer) CreateStatementDiagnosticsReport(
	ctx context.Context, req *serverpb.CreateStatementDiagnosticsReportRequest,
) (*serverpb.CreateStatementDiagnosticsReportResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewActivityAndNoViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	response := &serverpb.CreateStatementDiagnosticsReportResponse{
		Report: &serverpb.StatementDiagnosticsReport{},
	}

	var username string
	if user, err := authserver.UserFromIncomingRPCContext(ctx); err != nil {
		return nil, srverrors.ServerError(ctx, err)
	} else {
		username = user.Normalized()
	}
	err := s.stmtDiagnosticsRequester.InsertRequest(
		ctx,
		req.StatementFingerprint,
		req.PlanGist,
		req.AntiPlanGist,
		req.SamplingProbability,
		req.MinExecutionLatency,
		req.ExpiresAfter,
		req.Redacted,
		username,
	)
	if err != nil {
		return nil, err
	}

	response.Report.StatementFingerprint = req.StatementFingerprint
	return response, nil
}

// CancelStatementDiagnosticsReport cancels the statement diagnostics request by
// updating the corresponding row from the system.statement_diagnostics_requests
// table to be expired.
func (s *statusServer) CancelStatementDiagnosticsReport(
	ctx context.Context, req *serverpb.CancelStatementDiagnosticsReportRequest,
) (*serverpb.CancelStatementDiagnosticsReportResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewActivityAndNoViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	var response serverpb.CancelStatementDiagnosticsReportResponse
	err := s.stmtDiagnosticsRequester.CancelRequest(ctx, req.RequestID)
	if err != nil {
		response.Canceled = false
		response.Error = err.Error()
	} else {
		response.Canceled = true
	}
	return &response, nil
}

// StatementDiagnosticsRequests retrieves all statement diagnostics
// requests in the `system.statement_diagnostics_requests` table that
// have either completed or have not yet expired.
func (s *statusServer) StatementDiagnosticsRequests(
	ctx context.Context, _ *serverpb.StatementDiagnosticsReportsRequest,
) (*serverpb.StatementDiagnosticsReportsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewActivityAndNoViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	// TODO(davidh): Add pagination to this request.
	it, err := s.internalExecutor.QueryIteratorEx(ctx, "stmt-diag-get-all", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`SELECT
			id,
			statement_fingerprint,
			completed,
			statement_diagnostics_id,
			requested_at,
			min_execution_latency,
			expires_at
		FROM
			system.statement_diagnostics_requests`)
	if err != nil {
		return nil, err
	}

	var requests []stmtDiagnosticsRequest
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		id := int(*row[0].(*tree.DInt))
		statementFingerprint := string(*row[1].(*tree.DString))
		completed := bool(*row[2].(*tree.DBool))
		req := stmtDiagnosticsRequest{
			ID:                   id,
			StatementFingerprint: statementFingerprint,
			Completed:            completed,
		}
		if row[3] != tree.DNull {
			sdi := int(*row[3].(*tree.DInt))
			req.StatementDiagnosticsID = sdi
		}
		if requestedAt, ok := row[4].(*tree.DTimestampTZ); ok {
			req.RequestedAt = requestedAt.Time
		}
		if minExecutionLatency, ok := row[5].(*tree.DInterval); ok {
			req.MinExecutionLatency = time.Duration(minExecutionLatency.Duration.Nanos())
		}
		if expiresAt, ok := row[6].(*tree.DTimestampTZ); ok {
			req.ExpiresAt = expiresAt.Time
			// Don't return already expired requests.
			if !completed && req.ExpiresAt.Before(timeutil.Now()) {
				continue
			}
		}
		requests = append(requests, req)
	}
	if err != nil {
		return nil, err
	}

	response := &serverpb.StatementDiagnosticsReportsResponse{
		Reports: make([]serverpb.StatementDiagnosticsReport, len(requests)),
	}

	for i, request := range requests {
		response.Reports[i] = request.toProto()
	}
	return response, nil
}

// StatementDiagnostics retrieves a statement diagnostics instance
// identified by the given ID. These are in the
// `system.statement_diagnostics` table.
//
// This is generated once the trace is completed on a request created
// by the CreateStatementDiagnosticsRequest call and is linked to
// the original request with its ID.
func (s *statusServer) StatementDiagnostics(
	ctx context.Context, req *serverpb.StatementDiagnosticsRequest,
) (*serverpb.StatementDiagnosticsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewActivityAndNoViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	var err error
	row, err := s.internalExecutor.QueryRowEx(ctx, "stmt-diag-get-one", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`SELECT
			id,
			statement_fingerprint,
			collected_at
		FROM
			system.statement_diagnostics
		WHERE
			id = $1`, req.StatementDiagnosticsId)
	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, errors.Newf(
			"requested a statement diagnostic (%d) that does not exist",
			req.StatementDiagnosticsId,
		)
	}

	diagnostics := stmtDiagnostics{
		ID: int(req.StatementDiagnosticsId),
	}

	if statementFingerprint, ok := row[1].(*tree.DString); ok {
		diagnostics.StatementFingerprint = statementFingerprint.String()
	}

	if collectedAt, ok := row[2].(*tree.DTimestampTZ); ok {
		diagnostics.CollectedAt = collectedAt.Time
	}

	diagnosticsProto := diagnostics.toProto()
	response := &serverpb.StatementDiagnosticsResponse{
		Diagnostics: &diagnosticsProto,
	}

	return response, nil
}

func (s *adminServer) buildBundle(
	ctx context.Context, chunkIds *tree.DArray,
) (bytes.Buffer, error) {
	var bundle bytes.Buffer
	for _, chunkID := range chunkIds.Array {
		chunkRow, err := s.internalExecutor.QueryRowEx(
			ctx, "admin-stmt-bundle", nil, /* txn */
			sessiondata.NodeUserSessionDataOverride,
			"SELECT data FROM system.statement_bundle_chunks WHERE id=$1",
			chunkID,
		)
		if err != nil {
			return bytes.Buffer{}, err
		}
		if chunkRow == nil {
			return bytes.Buffer{}, errors.Newf("No bundle chunks found for id: %s", chunkID.String())
		}
		data := chunkRow[0].(*tree.DBytes)
		bundle.WriteString(string(*data))
	}

	return bundle, nil
}

func (s *adminServer) StmtBundleHandler(w http.ResponseWriter, req *http.Request) {
	idStr := req.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}

	// The privilege checks in the privilege checker below checks the user in the incoming
	// gRPC metadata.
	md := authserver.TranslateHTTPAuthInfoToGRPCMetadata(req.Context(), req)
	authCtx := metadata.NewIncomingContext(req.Context(), md)
	authCtx = s.AnnotateCtx(authCtx)
	if err := s.privilegeChecker.RequireViewActivityAndNoViewActivityRedactedPermission(authCtx); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}
	row, err := s.sqlServer.internalExecutor.QueryRowEx(
		authCtx, "admin-stmt-bundle", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		"SELECT bundle_chunks FROM system.statement_diagnostics WHERE id=$1 AND bundle_chunks IS NOT NULL",
		id,
	)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if row == nil {
		http.Error(w, http.StatusText(http.StatusNotFound), http.StatusNotFound)
		return
	}
	// Put together the entire bundle. Ideally we would stream it in chunks,
	// but it's hard to return errors once we start.
	chunkIDs := row[0].(*tree.DArray)
	bundle, err := s.buildBundle(authCtx, chunkIDs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set(
		"Content-Disposition",
		fmt.Sprintf("attachment; filename=stmt-bundle-%d.zip", id),
	)

	_, _ = io.Copy(w, &bundle)
}
