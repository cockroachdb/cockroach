// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlstatsutil

import (
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

var panicOnUnknownFieldsForTest = true

// DecodeTxnStatsMetadataJSON decodes the 'metadata' field of the JSON
// representation of transaction statistics into
// appstatspb.CollectedTransactionStatistics.
func DecodeTxnStatsMetadataJSON(
	metadata json.JSON, result *appstatspb.CollectedTransactionStatistics,
) error {
	valJSON, err := metadata.FetchValKey("stmtFingerprintIDs")
	if err != nil {
		return err
	}
	if valJSON != nil {
		return (*stmtFingerprintIDArray)(&result.StatementFingerprintIDs).decodeJSON(valJSON)
	}
	return nil
}

// DecodeTxnStatsStatisticsJSON decodes the 'statistics' section of the
// transaction statistics JSON payload into appstatspb.TransactionStatistics
// protobuf.
func DecodeTxnStatsStatisticsJSON(
	jsonVal json.JSON, result *appstatspb.TransactionStatistics,
) error {
	if statsJSON, err := jsonVal.FetchValKey("statistics"); err != nil {
		return err
	} else if statsJSON != nil {
		if err := (*innerTxnStats)(result).decodeJSON(statsJSON); err != nil {
			return err
		}
	}

	if execJSON, err := jsonVal.FetchValKey("execution_statistics"); err != nil {
		return err
	} else if execJSON != nil {
		if err := (*execStats)(&result.ExecStats).decodeJSON(execJSON); err != nil {
			return err
		}
	}

	return nil
}

// DecodeStmtStatsMetadataJSON decodes the 'metadata' field of the JSON
// representation of the statement statistics into
// appstatspb.CollectedStatementStatistics.
func DecodeStmtStatsMetadataJSON(
	metadata json.JSON, result *appstatspb.CollectedStatementStatistics,
) error {
	return (*stmtStatsMetadata)(result).decodeJSON(metadata)
}

// DecodeStmtStatsMetadataFlagsOnlyJSON decodes the 'metadata' flags only fields
// of the JSON representation of the statement statistics into
// appstatspb.CollectedStatementStatistics. This avoids the overhead of query
// string and query summary decoding.
func DecodeStmtStatsMetadataFlagsOnlyJSON(
	metadata json.JSON, result *appstatspb.CollectedStatementStatistics,
) error {
	return (*stmtStatsMetadata)(result).decodeFlagsOnlyJSON(metadata)
}

// DecodeAggregatedMetadataJSON decodes the 'aggregated metadata' represented by appstatspb.AggregatedStatementMetadata.
func DecodeAggregatedMetadataJSON(
	metadata json.JSON, result *appstatspb.AggregatedStatementMetadata,
) error {
	return (*aggregatedMetadata)(result).decodeJSON(metadata)
}

func (am *aggregatedMetadata) decodeJSON(js json.JSON) (err error) {
	iter, err := js.ObjectIter()
	if err != nil {
		return err
	}

	var fieldName string
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "decoding field %s", fieldName)
		}
	}()

	for ok := iter.Next(); ok; ok = iter.Next() {
		switch iter.Key() {
		case "fingerprintID":
			err = (*jsonString)(&am.FingerprintID).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "db":
			err = (*stringArray)(&am.Databases).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "appNames":
			err = (*stringArray)(&am.AppNames).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "distSQLCount":
			err = (*jsonInt)(&am.DistSQLCount).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "fullScanCount":
			err = (*jsonInt)(&am.FullScanCount).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "implicitTxn":
			err = (*jsonBool)(&am.ImplicitTxn).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "query":
			err = (*jsonString)(&am.Query).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "formattedQuery":
			err = (*jsonString)(&am.FormattedQuery).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "querySummary":
			err = (*jsonString)(&am.QuerySummary).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "stmtType":
			err = (*jsonString)(&am.StmtType).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "vecCount":
			err = (*jsonInt)(&am.VecCount).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "totalCount":
			err = (*jsonInt)(&am.TotalCount).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		default:
			if buildutil.CrdbTestBuild && panicOnUnknownFieldsForTest {
				panic(errors.AssertionFailedf("unknown field: %s", iter.Key()))
			}
		}
	}

	return nil
}

func (sm *stmtStatsMetadata) decodeJSON(js json.JSON) (err error) {
	iter, err := js.ObjectIter()
	if err != nil {
		return err
	}

	var fieldName string
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "decoding field %s", fieldName)
		}
	}()

	for ok := iter.Next(); ok; ok = iter.Next() {
		switch iter.Key() {
		case "stmtType":
			err = (*jsonString)(&sm.Stats.SQLType).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "query":
			err = (*jsonString)(&sm.Key.Query).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "querySummary":
			err = (*jsonString)(&sm.Key.QuerySummary).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "db":
			err = (*jsonString)(&sm.Key.Database).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "distsql":
			err = (*jsonBool)(&sm.Key.DistSQL).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "implicitTxn":
			err = (*jsonBool)(&sm.Key.ImplicitTxn).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "vec":
			err = (*jsonBool)(&sm.Key.Vec).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "fullScan":
			err = (*jsonBool)(&sm.Key.FullScan).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		default:
			if buildutil.CrdbTestBuild && panicOnUnknownFieldsForTest {
				panic(errors.AssertionFailedf("unknown field: %s", iter.Key()))
			}
		}
	}

	return nil
}

func (sm *stmtStatsMetadata) decodeFlagsOnlyJSON(js json.JSON) (err error) {
	iter, err := js.ObjectIter()
	if err != nil {
		return err
	}

	var fieldName string
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "decoding field %s", fieldName)
		}
	}()

	for ok := iter.Next(); ok; ok = iter.Next() {
		switch iter.Key() {
		case "db":
			err = (*jsonString)(&sm.Key.Database).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "distsql":
			err = (*jsonBool)(&sm.Key.DistSQL).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "implicitTxn":
			err = (*jsonBool)(&sm.Key.ImplicitTxn).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "vec":
			err = (*jsonBool)(&sm.Key.Vec).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "fullScan":
			err = (*jsonBool)(&sm.Key.FullScan).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		default:
			if buildutil.CrdbTestBuild && panicOnUnknownFieldsForTest {
				panic(errors.AssertionFailedf("unknown field: %s", iter.Key()))
			}
		}
	}

	return nil
}

func (am *aggregatedMetadata) decodeAggregatedFieldsOnlyJSON(js json.JSON) (err error) {
	iter, err := js.ObjectIter()
	if err != nil {
		return err
	}

	var fieldName string
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "decoding field %s", fieldName)
		}
	}()

	for ok := iter.Next(); ok; ok = iter.Next() {
		switch iter.Key() {
		case "db":
			err = (*stringArray)(&am.Databases).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "appNames":
			err = (*stringArray)(&am.AppNames).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "distSQLCount":
			err = (*jsonInt)(&am.DistSQLCount).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "fullScanCount":
			err = (*jsonInt)(&am.FullScanCount).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "implicitTxn":
			err = (*jsonBool)(&am.ImplicitTxn).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "vecCount":
			err = (*jsonInt)(&am.VecCount).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "totalCount":
			err = (*jsonInt)(&am.TotalCount).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		default:
			if buildutil.CrdbTestBuild && panicOnUnknownFieldsForTest {
				panic(errors.AssertionFailedf("unknown field: %s", iter.Key()))
			}
		}
	}

	return nil
}

// DecodeAggregatedMetadataAggregatedFieldsOnlyJSON decodes the 'aggregated metadata' represented by
// appstatspb.AggregatedStatementMetadata. It only includes the fields that are
// aggregated across multiple statements.
func DecodeAggregatedMetadataAggregatedFieldsOnlyJSON(
	metadata json.JSON, result *appstatspb.AggregatedStatementMetadata,
) error {
	return (*aggregatedMetadata)(result).decodeAggregatedFieldsOnlyJSON(metadata)
}

// DecodeStmtStatsStatisticsJSON decodes the 'statistics' field and the
// 'execution_statistics' field in the given json into
// appstatspb.StatementStatistics.
func DecodeStmtStatsStatisticsJSON(
	jsonVal json.JSON, result *appstatspb.StatementStatistics,
) (err error) {
	iter, err := jsonVal.ObjectIter()
	if err != nil {
		return err
	}

	var fieldName string
	defer func() {
		if err != nil {
			err = errors.Wrapf(err, "decoding field %s", fieldName)
		}
	}()

	for ok := iter.Next(); ok; ok = iter.Next() {
		switch iter.Key() {
		case "statistics":
			err = (*innerStmtStats)(result).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "execution_statistics":
			err = (*execStats)(&result.ExecStats).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		case "index_recommendations":
			err = (*stringArray)(&result.IndexRecommendations).decodeJSON(iter.Value())
			if err != nil {
				return err
			}
		default:
			if buildutil.CrdbTestBuild && panicOnUnknownFieldsForTest {
				panic(errors.AssertionFailedf("unknown field: %s", iter.Key()))
			}
		}
	}

	return nil
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
