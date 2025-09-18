// Copyright 2025 The Cockroach Authors.
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
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/server/authserver"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/server/srverrors"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/sql/stmtdiagnostics"
	"github.com/cockroachdb/cockroach/pkg/util/memzipper"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"google.golang.org/grpc/metadata"
)

type TxnDiagnosticsRequester interface {
	InsertRequest(
		ctx context.Context,
		txnFingerprintID appstatspb.TransactionFingerprintID,
		stmtFingerprintIDs []appstatspb.StmtFingerprintID,
		samplingProbability float64,
		minExecutionLatency time.Duration,
		expiresAfter time.Duration,
		redacted bool,
		username string,
	) (requestID int64, err error)
	CancelRequest(ctx context.Context, requestID int64) error
}

var _ TxnDiagnosticsRequester = &stmtdiagnostics.TxnRegistry{}

// txnDiagnosticsRequest contains a subset of columns that are stored in
// system.transaction_diagnostics_requests and are exposed in
// serverpb.TransactionDiagnosticsReport.
type txnDiagnosticsRequest struct {
	ID                       int
	TransactionFingerprintID appstatspb.TransactionFingerprintID
	StatementFingerprintIDs  []appstatspb.StmtFingerprintID
	TransactionFingerprint   string
	Completed                bool
	TransactionDiagnosticsID int
	RequestedAt              time.Time
	// Zero value indicates that there is no minimum latency set on the request.
	MinExecutionLatency time.Duration
	// Zero value indicates that the request never expires.
	ExpiresAt time.Time
}

func (request *txnDiagnosticsRequest) toProto() serverpb.TransactionDiagnosticsReport {
	stmtFingerprintIDs := make([][]byte, len(request.StatementFingerprintIDs))
	for i, s := range request.StatementFingerprintIDs {
		stmtFingerprintIDs[i] = []byte(sqlstatsutil.EncodeStmtFingerprintIDToString(s))
	}
	resp := serverpb.TransactionDiagnosticsReport{
		Id:                       int64(request.ID),
		Completed:                request.Completed,
		TransactionFingerprintId: []byte(sqlstatsutil.EncodeTxnFingerprintIDToString(request.TransactionFingerprintID)),
		StatementFingerprintIds:  stmtFingerprintIDs,
		TransactionFingerprint:   request.TransactionFingerprint,
		TransactionDiagnosticsId: int64(request.TransactionDiagnosticsID),
		RequestedAt:              request.RequestedAt,
		MinExecutionLatency:      request.MinExecutionLatency,
		ExpiresAt:                request.ExpiresAt,
	}
	return resp
}

// CreateTransactionDiagnosticsReport creates a transaction diagnostics
// request in the `system.transaction_diagnostics_requests` table
// to trace the next query matching the provided fingerprint.
func (s *statusServer) CreateTransactionDiagnosticsReport(
	ctx context.Context, req *serverpb.CreateTransactionDiagnosticsReportRequest,
) (*serverpb.CreateTransactionDiagnosticsReportResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewActivityAndNoViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	response := &serverpb.CreateTransactionDiagnosticsReportResponse{
		Report: &serverpb.TransactionDiagnosticsReport{},
	}

	var username string
	if user, err := authserver.UserFromIncomingRPCContext(ctx); err != nil {
		return nil, srverrors.ServerError(ctx, err)
	} else {
		username = user.Normalized()
	}
	txnFingerprintID, err := sqlstatsutil.DecodeStringToTxnFingerprintID(string(req.TransactionFingerprintId))
	if err != nil {
		return nil, srverrors.ServerError(ctx, err)
	}
	stmtFingerprintIDs := make([]appstatspb.StmtFingerprintID, len(req.StatementFingerprintIds))
	for i, sf := range req.StatementFingerprintIds {
		sfid, err := sqlstatsutil.DecodeStringToStmtFingerprintID(string(sf))
		if err != nil {
			return nil, err
		}
		stmtFingerprintIDs[i] = sfid
	}

	id, err := s.txnDiagnosticsRequester.InsertRequest(
		ctx,
		txnFingerprintID,
		stmtFingerprintIDs,
		req.SamplingProbability,
		req.MinExecutionLatency,
		req.ExpiresAt,
		req.Redacted,
		username,
	)
	if err != nil {
		return nil, err
	}

	response.Report.TransactionFingerprintId = req.TransactionFingerprintId
	response.Report.Id = id
	return response, nil
}

// CancelTransactionDiagnosticsReport cancels the transaction diagnostics request by
// updating the corresponding row from the system.transaction_diagnostics_requests
// table to be expired.
func (s *statusServer) CancelTransactionDiagnosticsReport(
	ctx context.Context, req *serverpb.CancelTransactionDiagnosticsReportRequest,
) (*serverpb.CancelTransactionDiagnosticsReportResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewActivityAndNoViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	var response serverpb.CancelTransactionDiagnosticsReportResponse
	err := s.txnDiagnosticsRequester.CancelRequest(ctx, req.RequestID)
	if err != nil {
		response.Canceled = false
		response.Error = err.Error()
	} else {
		response.Canceled = true
	}
	return &response, nil
}

// TransactionDiagnosticsRequests retrieves all transaction diagnostics
// requests in the `system.transaction_diagnostics_requests` table that
// have either completed or have not yet expired.
func (s *statusServer) TransactionDiagnosticsRequests(
	ctx context.Context, _ *serverpb.TransactionDiagnosticsReportsRequest,
) (*serverpb.TransactionDiagnosticsReportsResponse, error) {
	ctx = authserver.ForwardSQLIdentityThroughRPCCalls(ctx)
	ctx = s.AnnotateCtx(ctx)

	if err := s.privilegeChecker.RequireViewActivityAndNoViewActivityRedactedPermission(ctx); err != nil {
		return nil, err
	}

	it, err := s.internalExecutor.QueryIteratorEx(ctx, "txn-diag-get-all", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		`SELECT
			tdr.id,
			tdr.completed,
			tdr.transaction_fingerprint_id,
			tdr.statement_fingerprint_ids,
			tdr.transaction_diagnostics_id,
			tdr.requested_at,
			tdr.min_execution_latency,
			tdr.expires_at,
			td.transaction_fingerprint
		FROM
			system.transaction_diagnostics_requests tdr LEFT JOIN
			system.transaction_diagnostics td ON tdr.transaction_diagnostics_id = td.id`)
	if err != nil {
		return nil, err
	}

	var requests []txnDiagnosticsRequest
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		row := it.Cur()
		id := int(*row[0].(*tree.DInt))
		completed := bool(*row[1].(*tree.DBool))
		txnFingerprintIDBytes := []byte(*row[2].(*tree.DBytes))
		txnFingerprintID, err := sqlstatsutil.DecodeBytesToTxnFingerprintID(txnFingerprintIDBytes)
		if err != nil {
			return nil, err
		}
		stmtFingerprintArray := row[3].(*tree.DArray).Array
		stmtFingerprintIDs := make([]appstatspb.StmtFingerprintID, len(stmtFingerprintArray))
		for i, d := range stmtFingerprintArray {
			bytesID, ok := d.(*tree.DBytes)
			if !ok {
				return nil, errors.New("unable to decode statement fingerprints from SQL response")
			}
			stmtFingerprintIDs[i], err = sqlstatsutil.DecodeBytesToStmtFingerprintID([]byte(*bytesID))
		}
		req := txnDiagnosticsRequest{
			ID:                       id,
			TransactionFingerprintID: txnFingerprintID,
			StatementFingerprintIDs:  stmtFingerprintIDs,
			Completed:                completed,
		}
		if row[4] != tree.DNull {
			sdi := int(*row[4].(*tree.DInt))
			req.TransactionDiagnosticsID = sdi
		}
		if requestedAt, ok := row[5].(*tree.DTimestampTZ); ok {
			req.RequestedAt = requestedAt.Time
		}
		if minExecutionLatency, ok := row[6].(*tree.DInterval); ok {
			req.MinExecutionLatency = time.Duration(minExecutionLatency.Duration.Nanos())
		}
		if expiresAt, ok := row[7].(*tree.DTimestampTZ); ok {
			req.ExpiresAt = expiresAt.Time
			// Don't return already expired requests.
			if !completed && req.ExpiresAt.Before(timeutil.Now()) {
				continue
			}
		}
		if txnFingerprint, ok := row[8].(*tree.DString); ok {
			req.TransactionFingerprint = string(*txnFingerprint)
		}
		requests = append(requests, req)
	}
	if err != nil {
		return nil, err
	}

	response := &serverpb.TransactionDiagnosticsReportsResponse{
		Reports: make([]serverpb.TransactionDiagnosticsReport, len(requests)),
	}

	for i, request := range requests {
		response.Reports[i] = request.toProto()
	}
	return response, nil
}

// BuildTransactionBundle constructs a complete transaction diagnostic bundle
// including the transaction bundle and all associated statement bundles ordered by ID.
func (s *adminServer) buildTransactionBundle(
	ctx context.Context, txnID int64,
) (bytes.Buffer, error) {
	var z memzipper.Zipper
	z.Init()

	// First, get the transaction diagnostic bundle
	txnRow, err := s.internalExecutor.QueryRowEx(
		ctx, "admin-txn-bundle", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		"SELECT bundle_chunks FROM system.transaction_diagnostics WHERE id=$1 AND bundle_chunks IS NOT NULL",
		txnID,
	)
	if err != nil {
		return bytes.Buffer{}, errors.Wrap(err, "internal server error fetching transaction bundle")
	}
	if txnRow == nil {
		return bytes.Buffer{}, errors.Newf("Not Found")
	}

	// Build the transaction bundle and add it to zip
	txnChunkIDs := txnRow[0].(*tree.DArray)
	txnBundle, err := s.buildBundle(ctx, txnChunkIDs)
	if err != nil {
		return bytes.Buffer{}, errors.Wrapf(err, "building transaction bundle")
	}

	z.AddFile(fmt.Sprintf("transaction-%d.zip", txnID), txnBundle.String())

	// Get all associated statement bundles ordered by ID
	it, err := s.internalExecutor.QueryIteratorEx(
		ctx, "admin-txn-stmt-bundles", nil, /* txn */
		sessiondata.NodeUserSessionDataOverride,
		"SELECT id, bundle_chunks, statement_fingerprint FROM system.statement_diagnostics WHERE transaction_diagnostics_id=$1 AND bundle_chunks IS NOT NULL ORDER BY id",
		txnID,
	)
	if err != nil {
		return bytes.Buffer{}, errors.Wrap(err, "internal server error fetching statement bundles")
	}
	defer func() { _ = it.Close() }()

	// Add each statement bundle in order
	var ok bool
	i := 1
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		stmtRow := it.Cur()
		stmtChunkIDs := stmtRow[1].(*tree.DArray)
		stmtFingerprint := stmtRow[2].(*tree.DString)
		stmtBundle, err := s.buildBundle(ctx, stmtChunkIDs)
		stmtName := strings.Split(string(*stmtFingerprint), " ")[0]
		if err != nil {
			return bytes.Buffer{}, errors.Wrapf(err, "building statement bundle")
		}

		z.AddFile(fmt.Sprintf("%d-%s.zip", i, stmtName), stmtBundle.String())
		i++
	}
	if err != nil {
		return bytes.Buffer{}, errors.Wrap(err, "internal server error iterating statement bundles")
	}

	result, err := z.Finalize()
	if err != nil {
		return bytes.Buffer{}, errors.Wrapf(err, "finalizing zip")
	}

	return *result, nil
}

func (s *adminServer) TxnBundleHandler(w http.ResponseWriter, req *http.Request) {
	idStr := req.PathValue("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid id", http.StatusBadRequest)
		return
	}
	ctx := req.Context()
	// The privilege checks in the privilege checker below checks the user in the incoming
	// gRPC metadata.
	md := authserver.TranslateHTTPAuthInfoToGRPCMetadata(ctx, req)
	authCtx := metadata.NewIncomingContext(req.Context(), md)
	authCtx = s.AnnotateCtx(authCtx)
	if err := s.privilegeChecker.RequireViewActivityAndNoViewActivityRedactedPermission(authCtx); err != nil {
		http.Error(w, err.Error(), http.StatusForbidden)
		return
	}

	bundle, err := s.buildTransactionBundle(ctx, id)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set(
		"Content-Disposition",
		fmt.Sprintf("attachment; filename=txn-bundle-%d.zip", id),
	)

	_, _ = io.Copy(w, &bundle)
}
