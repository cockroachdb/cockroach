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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// ExplainTreePlanNodeToJSON builds a formatted JSON object from the explain tree nodes.
func ExplainTreePlanNodeToJSON(node *roachpb.ExplainTreePlanNode) json.JSON {

	// Create a new json.ObjectBuilder with key-value pairs for the node's name (1),
	// node's attributes (len(node.Attrs)), and the node's children (1).
	nodePlan := json.NewObjectBuilder(len(node.Attrs) + 2 /* numAddsHint */)
	nodeChildren := json.NewArrayBuilder(len(node.Children))

	nodePlan.Add("Name", json.FromString(node.Name))

	for _, attr := range node.Attrs {
		nodePlan.Add(strings.Title(attr.Key), json.FromString(attr.Value))
	}

	for _, childNode := range node.Children {
		nodeChildren.Add(ExplainTreePlanNodeToJSON(childNode))
	}

	nodePlan.Add("Children", nodeChildren.Build())
	return nodePlan.Build()
}

// BuildStmtMetadataJSON returns a json.JSON object for the metadata section of
// the roachpb.CollectedStatementStatistics.
// JSON Schema for statement metadata:
//    {
//      "$schema": "https://json-schema.org/draft/2020-12/schema",
//      "title": "system.statement_statistics.metadata",
//      "type": "object",
//      "properties": {
//        "stmtTyp":              { "type": "string" },
//        "query":                { "type": "string" },
//        "db":                   { "type": "string" },
//        "distsql":              { "type": "boolean" },
//        "failed":               { "type": "boolean" },
//        "opt":                  { "type": "boolean" },
//        "implicitTxn":          { "type": "boolean" },
//        "vec":                  { "type": "boolean" },
//        "fullScan":             { "type": "boolean" },
//      }
//    }
func BuildStmtMetadataJSON(statistics *roachpb.CollectedStatementStatistics) (json.JSON, error) {
	builder := json.NewObjectBuilder(11 /* numAddsHint */)
	builder.Add("stmtTyp", json.FromString(statistics.Stats.SQLType))
	builder.Add("query", json.FromString(statistics.Key.Query))
	builder.Add("db", json.FromString(statistics.Key.Database))
	builder.Add("distsql", json.FromBool(statistics.Key.DistSQL))
	builder.Add("failed", json.FromBool(statistics.Key.Failed))
	builder.Add("opt", json.FromBool(statistics.Key.Opt))
	builder.Add("implicitTxn", json.FromBool(statistics.Key.ImplicitTxn))
	builder.Add("vec", json.FromBool(statistics.Key.Vec))
	builder.Add("fullScan", json.FromBool(statistics.Key.FullScan))

	return builder.Build(), nil
}

// BuildStmtStatisticsJSON encodes the statistics section a given
// roachpb.CollectedStatementStatistics into a json.JSON object.
//
// JSON Schema for stats portion:
//    {
//      "$schema": "https://json-schema.org/draft/2020-12/schema",
//      "title": "system.statement_statistics.statistics",
//      "type": "object",
//
//      "definitions": {
//        "numeric_stats": {
//          "type": "object",
//          "properties": {
//            "mean":   { "type": "number" },
//            "sqDiff": { "type": "number" }
//          },
//          "required": ["mean", "sqDiff"]
//        },
//        "statistics": {
//          "type": "object",
//          "properties": {
//            "firstAttemptCnt":   { "type": "number" },
//            "maxRetries":        { "type": "number" },
//            "numRows":           { "$ref": "#/definitions/numeric_stats" },
//            "parseLat":          { "$ref": "#/definitions/numeric_stats" },
//            "planLat":           { "$ref": "#/definitions/numeric_stats" },
//            "runLat":            { "$ref": "#/definitions/numeric_stats" },
//            "svcLat":            { "$ref": "#/definitions/numeric_stats" },
//            "ovhLat":            { "$ref": "#/definitions/numeric_stats" },
//            "bytesRead":         { "$ref": "#/definitions/numeric_stats" },
//            "rowsRead":          { "$ref": "#/definitions/numeric_stats" }
//            "firstExecAt":       { "type": "string" },
//            "lastExecAt":        { "type": "string" },
//          },
//          "required": [
//            "firstAttemptCnt",
//            "maxRetries",
//            "numRows",
//            "parseLat",
//            "planLat",
//            "runLat",
//            "svcLat",
//            "ovhLat",
//            "bytesRead",
//            "rowsRead"
//          ]
//        },
//        "execution_statistics": {
//          "type": "object",
//          "properties": {
//            "cnt":             { "type": "number" },
//            "networkBytes":    { "$ref": "#/definitions/numeric_stats" },
//            "maxMemUsage":     { "$ref": "#/definitions/numeric_stats" },
//            "contentionTime":  { "$ref": "#/definitions/numeric_stats" },
//            "networkMsgs":     { "$ref": "#/definitions/numeric_stats" },
//            "maxDiskUsage":    { "$ref": "#/definitions/numeric_stats" },
//          },
//          "required": [
//            "cnt",
//            "networkBytes",
//            "maxMemUsage",
//            "contentionTime",
//            "networkMsgs",
//            "maxDiskUsage",
//          ]
//        }
//      },
//
//      "properties": {
//        "stats": { "$ref": "#/definitions/statistics" },
//        "execStats": {
//          "$ref": "#/definitions/execution_statistics"
//        }
//      }
//    }
func BuildStmtStatisticsJSON(statistics *roachpb.CollectedStatementStatistics) (json.JSON, error) {

	builder := json.NewObjectBuilder(2 /* numAddsHint */)

	stats, err := buildStmtStatisticsStats(&statistics.Stats)
	if err != nil {
		return nil, errors.Errorf("unable to serialize statistics field: %s", err)
	}
	builder.Add("statistics", stats)

	execStats, err := buildExecStatisticsJSON(&statistics.Stats.ExecStats)
	if err != nil {
		return nil, errors.Errorf("unable to serialize execution_statistics field: %s", err)
	}
	builder.Add("execution_statistics", execStats)

	return builder.Build(), nil
}

// BuildTxnMetadataJSON encodes the metadata portion a given
// roachpb.CollectedTransactionStatistics into a json.JSON object.
//
// JSON Schema:
// {
//   "$schema": "https://json-schema.org/draft/2020-12/schema",
//   "title": "system.transaction_statistics.metadata",
//   "type": "object",
//   "properties": {
//     "stmtFingerprintIDs": {
//       "type": "array",
//       "items": {
//         "type": "string"
//       }
//     },
//     "firstExecAt": { "type": "string" },
//     "lastExecAt":  { "type": "string" }
//   }
// }
func BuildTxnMetadataJSON(statistics *roachpb.CollectedTransactionStatistics) json.JSON {
	builder := json.NewObjectBuilder(3 /* numAddsHint */)

	builder.Add("stmtFingerprintIDs", buildStmtFingerprintIDArray(statistics.StatementFingerprintIDs))

	return builder.Build()
}

// BuildTxnStatisticsJSON encodes the statistics portion a given
// roachpb.CollectedTransactionStatistics into a json.JSON.
//
// JSON Schema
// {
//   "$schema": "https://json-schema.org/draft/2020-12/schema",
//   "title": "system.statement_statistics.statistics",
//   "type": "object",
//
//   "definitions": {
//     "numeric_stats": {
//       "type": "object",
//       "properties": {
//         "mean":   { "type": "number" },
//         "sqDiff": { "type": "number" }
//       },
//       "required": ["mean", "sqDiff"]
//     },
//     "statistics": {
//       "type": "object",
//       "properties": {
//         "maxRetries": { "type": "number" },
//         "numRows":    { "$ref": "#/definitions/numeric_stats" },
//         "svcLat":     { "$ref": "#/definitions/numeric_stats" },
//         "retryLat":   { "$ref": "#/definitions/numeric_stats" },
//         "commitLat":  { "$ref": "#/definitions/numeric_stats" },
//         "bytesRead":  { "$ref": "#/definitions/numeric_stats" },
//         "rowsRead":   { "$ref": "#/definitions/numeric_stats" }
//       },
//       "required": [
//         "maxRetries",
//         "numRows",
//         "svcLat",
//         "retryLat",
//         "commitLat",
//         "bytesRead",
//         "rowsRead",
//       ]
//     },
//     "execution_statistics": {
//       "type": "object",
//       "properties": {
//         "cnt":             { "type": "number" },
//         "networkBytes":    { "$ref": "#/definitions/numeric_stats" },
//         "maxMemUsage":     { "$ref": "#/definitions/numeric_stats" },
//         "contentionTime":  { "$ref": "#/definitions/numeric_stats" },
//         "networkMsg":      { "$ref": "#/definitions/numeric_stats" },
//         "maxDiskUsage":    { "$ref": "#/definitions/numeric_stats" },
//       },
//       "required": [
//         "cnt",
//         "networkBytes",
//         "maxMemUsage",
//         "contentionTime",
//         "networkMsg",
//         "maxDiskUsage",
//       ]
//     }
//   },
//
//   "properties": {
//     "stats": { "$ref": "#/definitions/statistics" },
//     "execStats": {
//       "$ref": "#/definitions/execution_statistics"
//     }
//   }
// }
func BuildTxnStatisticsJSON(statistics *roachpb.CollectedTransactionStatistics) (json.JSON, error) {
	builder := json.NewObjectBuilder(2 /* numAddsHint */)

	stats, err := buildTxnStatisticsStats(&statistics.Stats)
	if err != nil {
		return nil, errors.Errorf("unable to serialize statistics field: %s", err)
	}
	builder.Add("statistics", stats)

	execStats, err := buildExecStatisticsJSON(&statistics.Stats.ExecStats)
	if err != nil {
		return nil, errors.Errorf("unable to serialize execution_statistics field: %s", err)
	}
	builder.Add("execution_statistics", execStats)

	return builder.Build(), nil
}

// TODO(azhng): add firstExecAt and lastExecAt timestamp into the
//  protobuf definition.
func buildTxnStatisticsStats(statistics *roachpb.TransactionStatistics) (json.JSON, error) {

	builder := json.NewObjectBuilder(7 /* numAddsHint */)

	builder.Add("maxRetries", json.FromInt64(statistics.MaxRetries))

	numRows, err := buildNumericStats(&statistics.NumRows)
	if err != nil {
		return nil, err
	}
	builder.Add("numRows", numRows)

	svcLat, err := buildNumericStats(&statistics.ServiceLat)
	if err != nil {
		return nil, err
	}
	builder.Add("svcLat", svcLat)

	retryLat, err := buildNumericStats(&statistics.RetryLat)
	if err != nil {
		return nil, err
	}
	builder.Add("retryLat", retryLat)

	commitLat, err := buildNumericStats(&statistics.CommitLat)
	if err != nil {
		return nil, err
	}
	builder.Add("commitLat", commitLat)

	bytesRead, err := buildNumericStats(&statistics.BytesRead)
	if err != nil {
		return nil, err
	}
	builder.Add("bytesRead", bytesRead)

	rowsRead, err := buildNumericStats(&statistics.RowsRead)
	if err != nil {
		return nil, err
	}
	builder.Add("rowsRead", rowsRead)

	return builder.Build(), nil
}

func buildStmtStatisticsStats(stats *roachpb.StatementStatistics) (json.JSON, error) {
	builder := json.NewObjectBuilder(10 /* numAddsHint */)

	builder.Add("firstAttemptCnt", json.FromInt64(stats.FirstAttemptCount))
	builder.Add("maxRetries", json.FromInt64(stats.MaxRetries))

	dateString, err := stats.LastExecTimestamp.MarshalText()
	if err != nil {
		return nil, err
	}
	builder.Add("lastExecAt", json.FromString(string(dateString)))

	numRows, err := buildNumericStats(&stats.NumRows)
	if err != nil {
		return nil, err
	}
	builder.Add("numRows", numRows)

	parseLat, err := buildNumericStats(&stats.ParseLat)
	if err != nil {
		return nil, err
	}
	builder.Add("parseLat", parseLat)

	planLat, err := buildNumericStats(&stats.PlanLat)
	if err != nil {
		return nil, err
	}
	builder.Add("planLat", planLat)

	runLat, err := buildNumericStats(&stats.RunLat)
	if err != nil {
		return nil, err
	}
	builder.Add("runLat", runLat)

	svcLat, err := buildNumericStats(&stats.ServiceLat)
	if err != nil {
		return nil, err
	}
	builder.Add("svcLat", svcLat)

	ovhLat, err := buildNumericStats(&stats.OverheadLat)
	if err != nil {
		return nil, err
	}
	builder.Add("ovhLat", ovhLat)

	bytesRead, err := buildNumericStats(&stats.BytesRead)
	if err != nil {
		return nil, err
	}
	builder.Add("bytesRead", bytesRead)

	rowsRead, err := buildNumericStats(&stats.RowsRead)
	if err != nil {
		return nil, err
	}
	builder.Add("rowsRead", rowsRead)

	return builder.Build(), nil
}

func buildExecStatisticsJSON(stats *roachpb.ExecStats) (json.JSON, error) {
	builder := json.NewObjectBuilder(6 /* numAddsHint */)

	builder.Add("cnt", json.FromInt64(stats.Count))

	networkBytes, err := buildNumericStats(&stats.NetworkBytes)
	if err != nil {
		return nil, err
	}
	builder.Add("networkBytes", networkBytes)

	maxMemUsage, err := buildNumericStats(&stats.MaxMemUsage)
	if err != nil {
		return nil, err
	}
	builder.Add("maxMemUsage", maxMemUsage)

	contentionTime, err := buildNumericStats(&stats.ContentionTime)
	if err != nil {
		return nil, err
	}
	builder.Add("contentionTime", contentionTime)

	networkMsgs, err := buildNumericStats(&stats.NetworkMessages)
	if err != nil {
		return nil, err
	}
	builder.Add("networkMsgs", networkMsgs)

	maxDiskUsage, err := buildNumericStats(&stats.MaxDiskUsage)
	if err != nil {
		return nil, err
	}
	builder.Add("maxDiskUsage", maxDiskUsage)

	return builder.Build(), nil
}

func buildNumericStats(stats *roachpb.NumericStat) (json.JSON, error) {
	builder := json.NewObjectBuilder(2 /* numAddsHint */)

	mean, err := json.FromFloat64(stats.Mean)
	if err != nil {
		return nil, err
	}
	builder.Add("mean", mean)

	sqDiff, err := json.FromFloat64(stats.SquaredDiffs)
	if err != nil {
		return nil, err
	}
	builder.Add("sqDiff", sqDiff)

	return builder.Build(), nil
}

func buildStmtFingerprintIDArray(stmtFingerprintIDs []roachpb.StmtFingerprintID) json.JSON {
	// It is hard to predict how many statement fingerprints a transaction would
	// have.
	builder := json.NewArrayBuilder(1 /* numAddsHint */)

	for _, stmtFingerprintID := range stmtFingerprintIDs {
		builder.Add(json.FromString(encodeStmtFingerprintIDToString(stmtFingerprintID)))
	}

	return builder.Build()
}

// EncodeUint64ToBytes returns the []byte representation of an uint64 value.
func EncodeUint64ToBytes(id uint64) []byte {
	result := make([]byte, 0, 8)
	return encoding.EncodeUint64Ascending(result, id)
}

func encodeStmtFingerprintIDToString(id roachpb.StmtFingerprintID) string {
	return hex.EncodeToString(EncodeUint64ToBytes(uint64(id)))
}
