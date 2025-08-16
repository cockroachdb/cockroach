// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlstatsutil

import (
	"encoding/hex"

	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

// ExplainTreePlanNodeToJSON builds a formatted JSON object from the explain tree nodes.
func ExplainTreePlanNodeToJSON(node *appstatspb.ExplainTreePlanNode) json.JSON {
	// Create a new json.ObjectBuilder with key-value pairs for the node's name (1),
	// node's attributes (len(node.Attrs)), and the node's children (1).
	nodePlan := json.NewObjectBuilder(len(node.Attrs) + 2 /* numAddsHint */)
	nodeChildren := json.NewArrayBuilder(len(node.Children))

	nodePlan.Add("Name", json.FromString(node.Name))

	for _, attr := range node.Attrs {
		nodePlan.Add(cases.Title(language.English, cases.NoLower).String(attr.Key), json.FromString(attr.Value))
	}

	for _, childNode := range node.Children {
		nodeChildren.Add(ExplainTreePlanNodeToJSON(childNode))
	}

	nodePlan.Add("Children", nodeChildren.Build())
	return nodePlan.Build()
}

// BuildStmtMetadataJSON returns a json.JSON object for the metadata section of
// the appstatspb.CollectedStatementStatistics.
// JSON Schema for statement metadata:
//
//	{
//	  "$schema": "https://json-schema.org/draft/2020-12/schema",
//	  "title": "system.statement_statistics.metadata",
//	  "type": "object",
//	  "properties": {
//	    "stmtType":             { "type": "string" },
//	    "query":                { "type": "string" },
//	    "db":                   { "type": "string" },
//	    "distsql":              { "type": "boolean" },
//	    "failed":               { "type": "boolean" },
//	    "implicitTxn":          { "type": "boolean" },
//	    "vec":                  { "type": "boolean" },
//	    "fullScan":             { "type": "boolean" },
//	  }
//	}
func BuildStmtMetadataJSON(statistics *appstatspb.CollectedStatementStatistics) (json.JSON, error) {
	builder := json.NewObjectBuilder(8)
	val, err := (*jsonString)(&statistics.Stats.SQLType).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("stmtType", val)
	val, err = (*jsonString)(&statistics.Key.Query).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("query", val)
	val, err = (*jsonString)(&statistics.Key.QuerySummary).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("querySummary", val)
	val, err = (*jsonString)(&statistics.Key.Database).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("db", val)
	val, err = (*jsonBool)(&statistics.Key.DistSQL).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("distsql", val)
	val, err = (*jsonBool)(&statistics.Key.ImplicitTxn).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("implicitTxn", val)
	val, err = (*jsonBool)(&statistics.Key.Vec).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("vec", val)
	val, err = (*jsonBool)(&statistics.Key.FullScan).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("fullScan", val)
	return builder.Build(), nil
}

// BuildStmtStatisticsJSON encodes the statistics section a given
// appstatspb.CollectedStatementStatistics into a json.JSON object.
//
// JSON Schema for stats portion:
//
//		{
//		  "$schema": "https://json-schema.org/draft/2020-12/schema",
//		  "title": "system.statement_statistics.statistics",
//		  "type": "object",
//
//		  "definitions": {
//		    "numeric_stats": {
//		      "type": "object",
//		      "properties": {
//		        "mean":   { "type": "number" },
//		        "sqDiff": { "type": "number" }
//		      },
//		      "required": ["mean", "sqDiff"]
//		    },
//		    "indexes": {
//		      "type": "array",
//		      "items": {
//		        "type": "string",
//		      },
//		    },
//		    "node_ids": {
//		      "type": "array",
//		      "items": {
//		        "type": "int",
//		      },
//		    },
//		    "kv_node_ids": {
//		      "type": "array",
//		      "items": {
//		        "type": "int32",
//		      },
//		    },
//		    "regions": {
//		      "type": "array",
//		      "items": {
//		        "type": "string",
//		      },
//		    },
//		    "usedFollowerRead": { "type": "boolean" },
//		    "mvcc_iterator_stats": {
//		      "type": "object",
//		      "properties": {
//		        "stepCount": {
//		          "$ref": "#/definitions/numeric_stats"
//		        },
//		        "stepCountInternal": {
//		          "$ref": "#/definitions/numeric_stats"
//		        },
//		        "seekCount": {
//		          "$ref": "#/definitions/numeric_stats"
//		        },
//		        "seekCountInternal": {
//		          "$ref": "#/definitions/numeric_stats"
//		        },
//		        "blockBytes": {
//		          "$ref": "#/definitions/numeric_stats"
//		        },
//		        "blockBytesInCache": {
//		          "$ref": "#/definitions/numeric_stats"
//		        },
//		        "keyBytes": {
//		          "$ref": "#/definitions/numeric_stats"
//		        },
//		        "valueBytes": {
//		          "$ref": "#/definitions/numeric_stats"
//		        },
//		        "pointCount": {
//		          "$ref": "#/definitions/numeric_stats"
//		        },
//		        "pointsCoveredByRangeTombstones": {
//		          "$ref": "#/definitions/numeric_stats"
//		        },
//		        "rangeKeyCount": {
//		          "$ref": "#/definitions/numeric_stats"
//		        },
//		        "rangeKeyContainedPoints": {
//		          "$ref": "#/definitions/numeric_stats"
//		        },
//		        "rangeKeySkippedPoints": {
//		          "$ref": "#/definitions/numeric_stats"
//		        }
//		      },
//		      "required": [
//		        "stepCount",
//		        "stepCountInternal",
//		        "seekCount",
//		        "seekCountInternal",
//		        "blockBytes",
//		        "blockBytesInCache",
//		        "keyBytes",
//		        "valueBytes",
//		        "pointCount",
//		        "pointsCoveredByRangeTombstones",
//		        "rangeKeyCount",
//		        "rangeKeyContainedPoints",
//		        "rangeKeySkippedPoints"
//		      ]
//	     },
//		    "statistics": {
//		      "type": "object",
//		      "properties": {
//		        "firstAttemptCnt":   { "type": "number" },
//		        "maxRetries":        { "type": "number" },
//		        "numRows":           { "$ref": "#/definitions/numeric_stats" },
//		        "idleLat":           { "$ref": "#/definitions/numeric_stats" },
//		        "parseLat":          { "$ref": "#/definitions/numeric_stats" },
//		        "planLat":           { "$ref": "#/definitions/numeric_stats" },
//		        "runLat":            { "$ref": "#/definitions/numeric_stats" },
//		        "svcLat":            { "$ref": "#/definitions/numeric_stats" },
//		        "ovhLat":            { "$ref": "#/definitions/numeric_stats" },
//		        "bytesRead":         { "$ref": "#/definitions/numeric_stats" },
//		        "rowsRead":          { "$ref": "#/definitions/numeric_stats" },
//		        "firstExecAt":       { "type": "string" },
//		        "lastExecAt":        { "type": "string" },
//		        "nodes":             { "type": "node_ids" },
//		        "kvNodeIds":         { "type": "kv_node_ids" },
//		        "regions":           { "type": "regions" },
//		        "usedFollowerRead":  { "type": "boolean" },
//		        "indexes":           { "type": "indexes" },
//		        "lastErrorCode":     { "type": "string" },
//		      },
//		      "required": [
//		        "firstAttemptCnt",
//		        "maxRetries",
//		        "numRows",
//		        "idleLat",
//		        "parseLat",
//		        "planLat",
//		        "runLat",
//		        "svcLat",
//		        "ovhLat",
//		        "bytesRead",
//		        "rowsRead",
//		        "nodes",
//		        "kvNodeIds",
//		        "regions",
//		        "indexes
//		      ]
//		    },
//		    "execution_statistics": {
//		      "type": "object",
//		      "properties": {
//		        "cnt":             { "type": "number" },
//		        "networkBytes":    { "$ref": "#/definitions/numeric_stats" },
//		        "maxMemUsage":     { "$ref": "#/definitions/numeric_stats" },
//		        "contentionTime":  { "$ref": "#/definitions/numeric_stats" },
//		        "networkMsgs":     { "$ref": "#/definitions/numeric_stats" },
//		        "maxDiskUsage":    { "$ref": "#/definitions/numeric_stats" },
//		        "cpuSQLNanos":     { "$ref": "#/definitions/numeric_stats" },
//		        "mvccIteratorStats": { "$ref": "#/definitions/mvcc_iterator_stats" }
//		        }
//		      },
//		      "required": [
//		        "cnt",
//		        "networkBytes",
//		        "maxMemUsage",
//		        "contentionTime",
//		        "networkMsg",
//		        "maxDiskUsage",
//		        "cpuSQLNanos",
//		        "mvccIteratorStats"
//		      ]
//		    }
//		  },
//
//		  "properties": {
//		    "stats": { "$ref": "#/definitions/statistics" },
//		    "execStats": {
//		      "$ref": "#/definitions/execution_statistics"
//		    }
//		}
func BuildStmtStatisticsJSON(statistics *appstatspb.StatementStatistics) (json.JSON, error) {
	builder := json.NewObjectBuilder(3)
	val, err := (*innerStmtStats)(statistics).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("statistics", val)
	val, err = (*execStats)(&statistics.ExecStats).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("execution_statistics", val)
	val, err = (*stringArray)(&statistics.IndexRecommendations).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("index_recommendations", val)
	return builder.Build(), nil
}

// BuildTxnMetadataJSON encodes the metadata portion a given
// appstatspb.CollectedTransactionStatistics into a json.JSON object.
//
// JSON Schema:
//
//	{
//	  "$schema": "https://json-schema.org/draft/2020-12/schema",
//	  "title": "system.transaction_statistics.metadata",
//	  "type": "object",
//	  "properties": {
//	    "stmtFingerprintIDs": {
//	      "type": "array",
//	      "items": {
//	        "type": "string"
//	      }
//	    },
//	    "firstExecAt": { "type": "string" },
//	    "lastExecAt":  { "type": "string" }
//	  }
//	}
//
// TODO(azhng): add `firstExecAt` and `lastExecAt` into the protobuf definition.
func BuildTxnMetadataJSON(
	statistics *appstatspb.CollectedTransactionStatistics,
) (json.JSON, error) {
	builder := json.NewObjectBuilder(1)
	val, err := (*stmtFingerprintIDArray)(&statistics.StatementFingerprintIDs).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("stmtFingerprintIDs", val)
	return builder.Build(), nil
}

// BuildTxnStatisticsJSON encodes the statistics portion a given
// appstatspb.CollectedTransactionStatistics into a json.JSON.
//
// JSON Schema
//
//	{
//	  "$schema": "https://json-schema.org/draft/2020-12/schema",
//	  "title": "system.statement_statistics.statistics",
//	  "type": "object",
//
//	  "definitions": {
//	    "numeric_stats": {
//	      "type": "object",
//	      "properties": {
//	        "mean":   { "type": "number" },
//	        "sqDiff": { "type": "number" }
//	      },
//	      "required": ["mean", "sqDiff"]
//	    },
//	    "mvcc_iterator_stats": {
//	      "type": "object",
//	      "properties": {
//	        "stepCount": {
//	          "$ref": "#/definitions/numeric_stats"
//	        },
//	        "stepCountInternal": {
//	          "$ref": "#/definitions/numeric_stats"
//	        },
//	        "seekCount": {
//	          "$ref": "#/definitions/numeric_stats"
//	        },
//	        "seekCountInternal": {
//	          "$ref": "#/definitions/numeric_stats"
//	        },
//	        "blockBytes": {
//	          "$ref": "#/definitions/numeric_stats"
//	        },
//	        "blockBytesInCache": {
//	          "$ref": "#/definitions/numeric_stats"
//	        },
//	        "keyBytes": {
//	          "$ref": "#/definitions/numeric_stats"
//	        },
//	        "valueBytes": {
//	          "$ref": "#/definitions/numeric_stats"
//	        },
//	        "pointCount": {
//	          "$ref": "#/definitions/numeric_stats"
//	        },
//	        "pointsCoveredByRangeTombstones": {
//	          "$ref": "#/definitions/numeric_stats"
//	        },
//	        "rangeKeyCount": {
//	          "$ref": "#/definitions/numeric_stats"
//	        },
//	        "rangeKeyContainedPoints": {
//	          "$ref": "#/definitions/numeric_stats"
//	        },
//	        "rangeKeySkippedPoints": {
//	          "$ref": "#/definitions/numeric_stats"
//	        }
//	      },
//	      "required": [
//	        "stepCount",
//	        "stepCountInternal",
//	        "seekCount",
//	        "seekCountInternal",
//	        "blockBytes",
//	        "blockBytesInCache",
//	        "keyBytes",
//	        "valueBytes",
//	        "pointCount",
//	        "pointsCoveredByRangeTombstones",
//	        "rangeKeyCount",
//	        "rangeKeyContainedPoints",
//	        "rangeKeySkippedPoints"
//	      ]
//	    },
//	    "statistics": {
//	      "type": "object",
//	      "properties": {
//	        "maxRetries": { "type": "number" },
//	        "numRows":    { "$ref": "#/definitions/numeric_stats" },
//	        "svcLat":     { "$ref": "#/definitions/numeric_stats" },
//	        "retryLat":   { "$ref": "#/definitions/numeric_stats" },
//	        "commitLat":  { "$ref": "#/definitions/numeric_stats" },
//	        "idleLat":    { "$ref": "#/definitions/numeric_stats" },
//	        "bytesRead":  { "$ref": "#/definitions/numeric_stats" },
//	        "rowsRead":   { "$ref": "#/definitions/numeric_stats" }
//	      },
//	      "required": [
//	        "maxRetries",
//	        "numRows",
//	        "svcLat",
//	        "retryLat",
//	        "commitLat",
//	        "idleLat",
//	        "bytesRead",
//	        "rowsRead",
//	      ]
//	    },
//	    "execution_statistics": {
//	      "type": "object",
//	      "properties": {
//	        "cnt":             { "type": "number" },
//	        "networkBytes":    { "$ref": "#/definitions/numeric_stats" },
//	        "maxMemUsage":     { "$ref": "#/definitions/numeric_stats" },
//	        "contentionTime":  { "$ref": "#/definitions/numeric_stats" },
//	        "networkMsg":      { "$ref": "#/definitions/numeric_stats" },
//	        "maxDiskUsage":    { "$ref": "#/definitions/numeric_stats" },
//	        "cpuSQLNanos":     { "$ref": "#/definitions/numeric_stats" },
//	        "mvccIteratorStats": { "$ref": "#/definitions/mvcc_iterator_stats" }
//	      },
//	      "required": [
//	        "cnt",
//	        "networkBytes",
//	        "maxMemUsage",
//	        "contentionTime",
//	        "networkMsg",
//	        "maxDiskUsage",
//	        "cpuSQLNanos",
//	        "mvccIteratorStats"
//	      ]
//	    }
//	  },
//
//	  "properties": {
//	    "stats": { "$ref": "#/definitions/statistics" },
//	    "execStats": {
//	      "$ref": "#/definitions/execution_statistics"
//	    }
//	  }
//	}
func BuildTxnStatisticsJSON(
	statistics *appstatspb.CollectedTransactionStatistics,
) (json.JSON, error) {
	builder := json.NewObjectBuilder(2)
	val, err := (*innerTxnStats)(&statistics.Stats).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("statistics", val)
	val, err = (*execStats)(&statistics.Stats.ExecStats).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("execution_statistics", val)
	return builder.Build(), nil
}

// BuildStmtDetailsMetadataJSON returns a json.JSON object for the aggregated metadata
// appstatspb.AggregatedStatementMetadata.
// JSON Schema for statement aggregated metadata:
//
//	{
//	  "$schema": "https://json-schema.org/draft/2020-12/schema",
//	  "title": "system.statement_statistics.aggregated_metadata",
//	  "type": "object",
//
//	  "properties": {
//	    "stmtType":             { "type": "string" },
//	    "query":                { "type": "string" },
//	    "fingerprintID":        { "type": "string" },
//	    "querySummary":         { "type": "string" },
//	    "formattedQuery":       { "type": "string" },
//	    "implicitTxn":          { "type": "boolean" },
//	    "distSQLCount":         { "type": "number" },
//	    "failedCount":          { "type": "number" },
//	    "vecCount":             { "type": "number" },
//	    "fullScanCount":        { "type": "number" },
//	    "totalCount":           { "type": "number" },
//	    "db":                   {
//	   		"type": "array",
//	   		"items": {
//	   		  "type": "string"
//	   		}
//	  	},
//	    "appNames":             {
//	   		"type": "array",
//	   		"items": {
//	   		  "type": "string"
//	   		}
//	  	},
//	  }
//	}
func BuildStmtDetailsMetadataJSON(
	metadata *appstatspb.AggregatedStatementMetadata,
) (json.JSON, error) {
	builder := json.NewObjectBuilder(12)
	val, err := (*stringArray)(&metadata.Databases).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("db", val)
	val, err = (*stringArray)(&metadata.AppNames).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("appNames", val)
	val, err = (*jsonInt)(&metadata.DistSQLCount).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("distSQLCount", val)
	val, err = (*jsonInt)(&metadata.FullScanCount).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("fullScanCount", val)
	val, err = (*jsonBool)(&metadata.ImplicitTxn).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("implicitTxn", val)
	val, err = (*jsonString)(&metadata.Query).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("query", val)
	val, err = (*jsonString)(&metadata.FormattedQuery).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("formattedQuery", val)
	val, err = (*jsonString)(&metadata.QuerySummary).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("querySummary", val)
	val, err = (*jsonString)(&metadata.StmtType).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("stmtType", val)
	val, err = (*jsonInt)(&metadata.VecCount).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("vecCount", val)
	val, err = (*jsonInt)(&metadata.TotalCount).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("totalCount", val)
	val, err = (*jsonString)(&metadata.FingerprintID).encodeJSON()
	if err != nil {
		return nil, err
	}
	builder.Add("fingerprintID", val)
	return builder.Build(), nil
}

// EncodeUint64ToBytes returns the []byte representation of an uint64 value.
func EncodeUint64ToBytes(id uint64) []byte {
	result := make([]byte, 0, 8)
	return encoding.EncodeUint64Ascending(result, id)
}

func encodeStmtFingerprintIDToString(id appstatspb.StmtFingerprintID) string {
	return hex.EncodeToString(EncodeUint64ToBytes(uint64(id)))
}
