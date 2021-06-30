// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlstatsutil

import (
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/json"
)

// DecodeTxnStatsMetadataJSON decodes the 'metadata' field of the JSON
// representation of transaction statistics into
// roachpb.CollectedTransactionStatistics.
func DecodeTxnStatsMetadataJSON(
	metadata json.JSON, result *roachpb.CollectedTransactionStatistics,
) error {
	return jsonFields{
		{"stmtFingerprintIDs", (*stmtFingerprintIDArray)(&result.StatementFingerprintIDs)},
	}.decodeJSON(metadata)
}

// DecodeTxnStatsStatisticsJSON decodes the 'statistics' section of the
// transaction statistics JSON payload into roachpb.TransactionStatistics
// protobuf.
func DecodeTxnStatsStatisticsJSON(jsonVal json.JSON, result *roachpb.TransactionStatistics) error {
	return (*txnStats)(result).decodeJSON(jsonVal)
}

// DecodeStmtStatsMetadataJSON decodes the 'metadata' field of the JSON
// representation of the statement statistics into
// roachpb.CollectedStatementStatistics.
func DecodeStmtStatsMetadataJSON(
	metadata json.JSON, result *roachpb.CollectedStatementStatistics,
) error {
	return (*stmtStatsMetadata)(result).jsonFields().decodeJSON(metadata)
}

// DecodeStmtStatsStatisticsJSON decodes the 'statistics' field and the
// 'execution_statistics' field in the given json into
// roachpb.StatementStatistics.
func DecodeStmtStatsStatisticsJSON(jsonVal json.JSON, result *roachpb.StatementStatistics) error {
	return (*stmtStats)(result).decodeJSON(jsonVal)
}
