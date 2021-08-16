// Copyright 2021 The Cockroach Authors.
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
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func (s *statusServer) PersistedStatementStats(
	ctx context.Context, req *serverpb.PersistedStatementStatsRequest,
) (*serverpb.StatementsResponse, error) {
	ctx = propagateGatewayMetadata(ctx)
	ctx = s.AnnotateCtx(ctx)

	if _, err := s.privilegeChecker.requireViewActivityPermission(ctx); err != nil {
		return nil, err
	}

	startTime := timeutil.Unix(req.Start, 0)
	endTime := timeutil.Unix(req.End, 0)

	statements, err := collectPersistedStatements(ctx, s.internalExecutor, startTime, endTime)

	transactions, err := collectPersistedTransactions(ctx, s.internalExecutor, startTime, endTime)

	if err != nil {
		return nil, err
	}

	response := &serverpb.StatementsResponse{
		Statements:            statements,
		Transactions:          transactions,
		LastReset:             timeutil.Now(),
		InternalAppNamePrefix: catconstants.InternalAppNamePrefix,
	}

	return response, nil
}

func collectPersistedStatements(
	ctx context.Context, ie *sql.InternalExecutor, start time.Time, end time.Time,
) ([]serverpb.StatementsResponse_CollectedStatementStatistics, error) {
	query :=
		`SELECT
				fingerprint_id,
				statistics,
				metadata
			FROM crdb_internal.statement_statistics
			WHERE
				aggregated_ts >= $1 AND aggregated_ts <= $2`

	it, err := ie.QueryIteratorEx(ctx, "persisted-stmts-by-interval", nil,
		sessiondata.InternalExecutorOverride{
			User: security.NodeUserName(),
		}, query,
		start,
		end)

	if err != nil {
		return nil, err
	}

	var statements []serverpb.StatementsResponse_CollectedStatementStatistics
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		row := it.Cur()

		statementFingerprintID, err := datumToStmtFingerprintId(row[0])

		if err != nil {
			return nil, err
		}

		var stats roachpb.StatementStatistics
		statsJson := tree.MustBeDJSON(row[1]).JSON
		err = sqlstatsutil.DecodeStmtStatsStatisticsJSON(statsJson, &stats)
		if err != nil {
			return nil, err
		}

		var metadata roachpb.CollectedStatementStatistics
		metadataJson := tree.MustBeDJSON(row[2]).JSON
		err = sqlstatsutil.DecodeStmtStatsMetadataJSON(metadataJson, &metadata)
		if err != nil {
			return nil, err
		}

		stmt := serverpb.StatementsResponse_CollectedStatementStatistics{
			Key: serverpb.StatementsResponse_ExtendedStatementStatisticsKey{
				KeyData: metadata.Key,
				NodeID:  roachpb.NodeID(0),
			},
			ID:    statementFingerprintID,
			Stats: stats,
		}

		statements = append(statements, stmt)

	}

	if err != nil {
		return nil, err
	}

	return statements, nil
}

func collectPersistedTransactions(
	ctx context.Context, ie *sql.InternalExecutor, start time.Time, end time.Time,
) ([]serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics, error) {

	query :=
		`SELECT
				node_id,
				app_name,
				aggregated_ts,
				statistics,
				metadata
			FROM system.transaction_statistics
			WHERE
				aggregated_ts >= $1 AND aggregated_ts <= $2`

	it, err := ie.QueryIteratorEx(ctx, "persisted-txns-by-interval", nil,
		sessiondata.InternalExecutorOverride{
			User: security.NodeUserName(),
		}, query,
		start,
		end)

	if err != nil {
		return nil, err
	}

	var transactions []serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics
	var ok bool
	for ok, err = it.Next(ctx); ok; ok, err = it.Next(ctx) {
		row := it.Cur()

		if err != nil {
			return nil, err
		}

		nodeId := int32(tree.MustBeDInt(row[0]))

		app := string(tree.MustBeDString(row[1]))

		aggregatedTs := tree.MustBeDTimestampTZ(row[2]).Time

		var stats roachpb.TransactionStatistics
		statsJson := tree.MustBeDJSON(row[3]).JSON
		err = sqlstatsutil.DecodeTxnStatsStatisticsJSON(statsJson, &stats)

		if err != nil {
			return nil, err
		}

		var metadata roachpb.CollectedTransactionStatistics
		metadataJson := tree.MustBeDJSON(row[4]).JSON
		err = sqlstatsutil.DecodeTxnStatsMetadataJSON(metadataJson, &metadata)

		if err != nil {
			return nil, err
		}

		stmt := serverpb.StatementsResponse_ExtendedCollectedTransactionStatistics{
			NodeID: roachpb.NodeID(nodeId),
			StatsData: roachpb.CollectedTransactionStatistics{
				StatementFingerprintIDs: metadata.StatementFingerprintIDs,
				App:                     app,
				Stats:                   stats,
				AggregatedTs:            aggregatedTs,
			},
		}

		transactions = append(transactions, stmt)

	}

	if err != nil {
		return nil, err
	}

	return transactions, nil
}

func datumToStmtFingerprintId(datum tree.Datum) (roachpb.StmtFingerprintID, error) {
	b := []byte(tree.MustBeDBytes(datum))

	_, statementFingerprintID, err := encoding.DecodeUint64Ascending(b)

	if err != nil {
		return 0, err
	}

	return roachpb.StmtFingerprintID(statementFingerprintID), nil
}
