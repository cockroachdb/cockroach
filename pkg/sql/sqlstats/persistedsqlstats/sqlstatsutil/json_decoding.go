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
	"encoding/hex"

	"github.com/cockroachdb/apd/v2"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// DecodeTxnStatsMetadataJSON decodes the 'metadata' field of the JSON
// representation of transaction statistics into
// roachpb.CollectedTransactionStatistics.
func DecodeTxnStatsMetadataJSON(
	metadata json.JSON, result *roachpb.CollectedTransactionStatistics,
) error {
	field, err := safeFetchVal(metadata, "stmtFingerprintIDs")
	if err != nil {
		return err
	}

	arrLen := field.Len()
	for i := 0; i < arrLen; i++ {
		stmtFingerprintIDJSON, err := field.FetchValIdx(i)
		if err != nil {
			return err
		}
		if stmtFingerprintIDJSON == nil {
			return errors.Errorf("unable to retrieve statement fingerprint ID at %d", i)
		}
		stmtFingerprintIDEncoded, err := stmtFingerprintIDJSON.AsText()
		if err != nil {
			return errors.Errorf("unable to decode statement fingerprint ID: %s", err)
		}

		stmtFingerprintIDDecoded, err := hex.DecodeString(*stmtFingerprintIDEncoded)
		if err != nil {
			return errors.Errorf("unable to decode statement fingerprint ID: %s", err)
		}

		_, stmtFingerprintID, err := encoding.DecodeUint64Ascending(stmtFingerprintIDDecoded)
		if err != nil {
			return errors.Errorf("unable to decode statement fingerprint ID: %s", err)
		}

		result.StatementFingerprintIDs = append(result.StatementFingerprintIDs, roachpb.StmtFingerprintID(stmtFingerprintID))
	}

	return nil
}

// DecodeTxnStatsStatisticsJSON decodes the 'statistics' section of the
// transaction statistics JSON payload into roachpb.TransactionStatistics
// protobuf.
func DecodeTxnStatsStatisticsJSON(jsonVal json.JSON, result *roachpb.TransactionStatistics) error {
	stats, err := safeFetchVal(jsonVal, "statistics")
	if err != nil {
		return err
	}

	err = decodeTxnStatsStatisticsField(stats, result)
	if err != nil {
		return errors.Errorf("failed to decode statistics field: %s", err)
	}

	execStats, err := safeFetchVal(jsonVal, "execution_statistics")
	if err != nil {
		return err
	}

	err = decodeExecStats(execStats, &result.ExecStats)
	if err != nil {
		return errors.Errorf("failed to decode execution_statistics field: %s", err)
	}

	return nil
}

func decodeTxnStatsStatisticsField(stats json.JSON, result *roachpb.TransactionStatistics) error {
	err := decodeInt64(stats, "maxRetries", &result.MaxRetries)
	if err != nil {
		return err
	}

	err = decodeNumericStats(stats, "numRows", &result.NumRows)
	if err != nil {
		return err
	}

	err = decodeNumericStats(stats, "svcLat", &result.ServiceLat)
	if err != nil {
		return err
	}

	err = decodeNumericStats(stats, "retryLat", &result.RetryLat)
	if err != nil {
		return err
	}

	err = decodeNumericStats(stats, "commitLat", &result.CommitLat)
	if err != nil {
		return err
	}

	err = decodeNumericStats(stats, "bytesRead", &result.BytesRead)
	if err != nil {
		return err
	}

	err = decodeNumericStats(stats, "rowsRead", &result.RowsRead)
	if err != nil {
		return err
	}

	return nil
}

// DecodeStmtStatsMetadataJSON decodes the 'metadata' field of the JSON
// representation of the statement statistics into
// roachpb.CollectedStatementStatistics.
func DecodeStmtStatsMetadataJSON(
	metadata json.JSON, result *roachpb.CollectedStatementStatistics,
) error {
	err := decodeString(metadata, "stmtTyp", &result.Stats.SQLType)
	if err != nil {
		return err
	}

	err = decodeString(metadata, "query", &result.Key.Query)
	if err != nil {
		return err
	}

	err = decodeString(metadata, "db", &result.Key.Database)
	if err != nil {
		return err
	}

	err = decodeBool(metadata, "distsql", &result.Key.DistSQL)
	if err != nil {
		return err
	}

	err = decodeBool(metadata, "failed", &result.Key.Failed)
	if err != nil {
		return err
	}

	err = decodeBool(metadata, "opt", &result.Key.Opt)
	if err != nil {
		return err
	}

	err = decodeBool(metadata, "implicitTxn", &result.Key.ImplicitTxn)
	if err != nil {
		return err
	}

	err = decodeBool(metadata, "vec", &result.Key.Vec)
	if err != nil {
		return err
	}

	err = decodeBool(metadata, "fullScan", &result.Key.FullScan)
	if err != nil {
		return err
	}

	return nil
}

// DecodeStmtStatsStatisticsJSON decodes the 'statistics' field and the
// 'execution_statistics' field in the given json into
// roachpb.StatementStatistics.
func DecodeStmtStatsStatisticsJSON(jsonVal json.JSON, result *roachpb.StatementStatistics) error {
	stats, err := safeFetchVal(jsonVal, "statistics")
	if err != nil {
		return err
	}

	err = decodeStmtStatsStatisticsField(stats, result)
	if err != nil {
		return errors.Errorf("failed to decode statistics field: %s", err)
	}

	execStats, err := safeFetchVal(jsonVal, "execution_statistics")
	if err != nil {
		return err
	}

	err = decodeExecStats(execStats, &result.ExecStats)
	if err != nil {
		return errors.Errorf("failed to decode execution_statistics field: %s", err)
	}

	return nil
}

// decodeStmtStatsStatisticsField decodels the 'statistics' field of the
// given json into result.
func decodeStmtStatsStatisticsField(stats json.JSON, result *roachpb.StatementStatistics) error {
	err := decodeInt64(stats, "firstAttemptCnt", &result.FirstAttemptCount)
	if err != nil {
		return err
	}

	err = decodeInt64(stats, "maxRetries", &result.MaxRetries)
	if err != nil {
		return err
	}

	var lastExecTimestamp string
	err = decodeString(stats, "lastExecAt", &lastExecTimestamp)
	if err != nil {
		return err
	}

	err = result.LastExecTimestamp.UnmarshalText([]byte(lastExecTimestamp))
	if err != nil {
		return err
	}

	err = decodeNumericStats(stats, "numRows", &result.NumRows)
	if err != nil {
		return err
	}

	err = decodeNumericStats(stats, "parseLat", &result.ParseLat)
	if err != nil {
		return err
	}

	err = decodeNumericStats(stats, "planLat", &result.PlanLat)
	if err != nil {
		return err
	}

	err = decodeNumericStats(stats, "runLat", &result.RunLat)
	if err != nil {
		return err
	}

	err = decodeNumericStats(stats, "svcLat", &result.ServiceLat)
	if err != nil {
		return err
	}

	err = decodeNumericStats(stats, "ovhLat", &result.OverheadLat)
	if err != nil {
		return err
	}

	err = decodeNumericStats(stats, "bytesRead", &result.BytesRead)
	if err != nil {
		return err
	}

	err = decodeNumericStats(stats, "rowsRead", &result.RowsRead)
	if err != nil {
		return err
	}

	return nil
}

// decodeStmtStatsStatisticsField decodels the 'execution_statistics'
// field of the given json into result.
func decodeExecStats(jsonVal json.JSON, result *roachpb.ExecStats) error {
	err := decodeInt64(jsonVal, "cnt", &result.Count)
	if err != nil {
		return err
	}

	err = decodeNumericStats(jsonVal, "networkBytes", &result.NetworkBytes)
	if err != nil {
		return err
	}

	err = decodeNumericStats(jsonVal, "maxMemUsage", &result.MaxMemUsage)
	if err != nil {
		return err
	}

	err = decodeNumericStats(jsonVal, "contentionTime", &result.ContentionTime)
	if err != nil {
		return err
	}

	err = decodeNumericStats(jsonVal, "networkMsgs", &result.NetworkMessages)
	if err != nil {
		return err
	}

	err = decodeNumericStats(jsonVal, "maxDiskUsage", &result.MaxDiskUsage)
	if err != nil {
		return err
	}

	return nil
}

func decodeNumericStats(jsonVal json.JSON, key string, result *roachpb.NumericStat) error {
	field, err := safeFetchVal(jsonVal, key)
	if err != nil {
		return err
	}

	err = decodeFloat64(field, "mean", &result.Mean)
	if err != nil {
		return err
	}

	err = decodeFloat64(field, "sqDiff", &result.SquaredDiffs)
	if err != nil {
		return err
	}

	return nil
}

func decodeBool(jsonVal json.JSON, key string, res *bool) error {
	field, err := safeFetchVal(jsonVal, key)
	if err != nil {
		return err
	}

	if field == json.TrueJSONValue {
		*res = true
		return nil
	}

	*res = false
	return nil
}

func decodeString(jsonVal json.JSON, key string, result *string) error {
	field, err := safeFetchVal(jsonVal, key)
	if err != nil {
		return err
	}

	text, err := field.AsText()
	if err != nil {
		return err
	}

	*result = *text
	return nil
}

func decodeInt64(jsonVal json.JSON, key string, result *int64) error {
	decimal, err := decodeDecimal(jsonVal, key)
	if err != nil {
		return errors.New("unable to decode float64")
	}

	i, err := decimal.Int64()
	if err != nil {
		return err
	}
	*result = i
	return nil
}

func decodeFloat64(jsonVal json.JSON, key string, result *float64) error {
	decimal, err := decodeDecimal(jsonVal, key)
	if err != nil {
		return err
	}

	f, err := decimal.Float64()
	if err != nil {
		return err
	}
	*result = f
	return nil
}

func decodeDecimal(jsonVal json.JSON, key string) (*apd.Decimal, error) {
	field, err := safeFetchVal(jsonVal, key)
	if err != nil {
		return nil, err
	}

	decimal, ok := field.AsDecimal()
	if !ok {
		return nil, errors.New("unable to decode float64")
	}

	return decimal, nil
}

func safeFetchVal(jsonVal json.JSON, key string) (json.JSON, error) {
	field, err := jsonVal.FetchValKey(key)
	if err != nil {
		return nil, err
	}
	if field == nil {
		return nil, errors.Errorf("%s field is not found in the JSON payload", key)
	}
	return field, nil
}
