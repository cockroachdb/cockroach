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
	return (*stmtStatsMetadata)(statistics).jsonFields().encodeJSON()
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
func BuildStmtStatisticsJSON(statistics *roachpb.StatementStatistics) (json.JSON, error) {
	return (*stmtStats)(statistics).encodeJSON()
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
// TODO(azhng): add `firstExecAt` and `lastExecAt` into the protobuf definition.
func BuildTxnMetadataJSON(statistics *roachpb.CollectedTransactionStatistics) (json.JSON, error) {
	return jsonFields{
		{"stmtFingerprintIDs", (*stmtFingerprintIDArray)(&statistics.StatementFingerprintIDs)},
	}.encodeJSON()
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
	return (*txnStats)(&statistics.Stats).encodeJSON()
}

// EncodeUint64ToBytes returns the []byte representation of an uint64 value.
func EncodeUint64ToBytes(id uint64) []byte {
	result := make([]byte, 0, 8)
	return encoding.EncodeUint64Ascending(result, id)
}

func encodeStmtFingerprintIDToString(id roachpb.StmtFingerprintID) string {
	return hex.EncodeToString(EncodeUint64ToBytes(uint64(id)))
}
