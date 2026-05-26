// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlstatsutil

import (
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// DecodeTxnStatsMetadataJSON decodes the 'metadata' field of the JSON
// representation of transaction statistics into
// appstatspb.CollectedTransactionStatistics.
func DecodeTxnStatsMetadataJSON(
	metadata json.JSON, result *appstatspb.CollectedTransactionStatistics,
) error {
	return jsonFields{
		{"stmtFingerprintIDs", (*stmtFingerprintIDArray)(&result.StatementFingerprintIDs)},
	}.decodeJSON(metadata)
}

// DecodeTxnStatsStatisticsJSON decodes the 'statistics' section of the
// transaction statistics JSON payload into appstatspb.TransactionStatistics
// protobuf.
func DecodeTxnStatsStatisticsJSON(
	jsonVal json.JSON, result *appstatspb.TransactionStatistics,
) error {
	return (*txnStats)(result).decodeJSON(jsonVal)
}

// DecodeStmtStatsMetadataJSON decodes the 'metadata' field of the JSON
// representation of the statement statistics into
// appstatspb.CollectedStatementStatistics.
func DecodeStmtStatsMetadataJSON(
	metadata json.JSON, result *appstatspb.CollectedStatementStatistics,
) error {
	return (*stmtStatsMetadata)(result).jsonFields().decodeJSON(metadata)
}

// DecodeStmtStatsMetadataFlagsOnlyJSON decodes the 'metadata' flags only fields
// of the JSON representation of the statement statistics into
// appstatspb.CollectedStatementStatistics. This avoids the overhead of query
// string and query summary decoding.
func DecodeStmtStatsMetadataFlagsOnlyJSON(
	metadata json.JSON, result *appstatspb.CollectedStatementStatistics,
) error {
	return (*stmtStatsMetadata)(result).jsonFlagsOnlyFields().decodeJSON(metadata)
}

// DecodeAggregatedMetadataJSON decodes the 'aggregated metadata' represented by appstatspb.AggregatedStatementMetadata.
func DecodeAggregatedMetadataJSON(
	metadata json.JSON, result *appstatspb.AggregatedStatementMetadata,
) error {
	return (*aggregatedMetadata)(result).jsonFields().decodeJSON(metadata)
}

// DecodeAggregatedMetadataAggregatedFieldsOnlyJSON decodes the 'aggregated metadata' represented by
// appstatspb.AggregatedStatementMetadata. It only includes the fields that are
// aggregated across multiple statements.
func DecodeAggregatedMetadataAggregatedFieldsOnlyJSON(
	metadata json.JSON, result *appstatspb.AggregatedStatementMetadata,
) error {
	return (*aggregatedMetadata)(result).jsonAggregatedFields().decodeJSON(metadata)
}

// DecodeStmtStatsStatisticsJSON decodes the 'statistics' field and the
// 'execution_statistics' field in the given json into
// appstatspb.StatementStatistics.
func DecodeStmtStatsStatisticsJSON(
	jsonVal json.JSON, result *appstatspb.StatementStatistics,
) error {
	return (*stmtStats)(result).decodeJSON(jsonVal)
}

// JSONToExplainTreePlanNode decodes the JSON-formatted ExplainTreePlanNode
// produced by ExplainTreePlanNodeToJSON.
func JSONToExplainTreePlanNode(jsonVal json.JSON) (*appstatspb.ExplainTreePlanNode, error) {
	node := appstatspb.ExplainTreePlanNode{}

	nameAttr, err := jsonVal.FetchValKey("Name")
	if err != nil {
		return nil, err
	}

	if nameAttr != nil {
		str, err := nameAttr.AsText()
		if err != nil {
			return nil, err
		}
		if str == nil {
			node.Name = "<null>"
		} else {
			node.Name = *str
		}
	}

	iter, err := jsonVal.ObjectIter()
	if err != nil {
		return nil, err
	}

	if iter == nil {
		return nil, errors.New("unable to deconstruct json object")
	}

	for iter.Next() {
		key := iter.Key()
		value := iter.Value()

		if key == "Name" {
			// We already handled the name, so we skip it.
			continue
		}

		if key == "Children" {
			for childIdx := 0; childIdx < value.Len(); childIdx++ {
				childJSON, err := value.FetchValIdx(childIdx)
				if err != nil {
					return nil, err
				}
				if childJSON != nil {
					child, err := JSONToExplainTreePlanNode(childJSON)
					if err != nil {
						return nil, err
					}
					node.Children = append(node.Children, child)
				}
			}
		} else {
			str, err := value.AsText()
			if err != nil {
				return nil, err
			}
			value := "<null>"
			if str != nil {
				value = *str
			}
			node.Attrs = append(node.Attrs, &appstatspb.ExplainTreePlanNode_Attr{
				Key:   key,
				Value: value,
			})
		}
	}

	return &node, nil
}
