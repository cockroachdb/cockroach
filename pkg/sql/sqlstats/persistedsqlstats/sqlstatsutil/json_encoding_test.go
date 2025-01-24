// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sqlstatsutil

import (
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

func jsonTestHelper(t *testing.T, expectedStr string, actual json.JSON) {
	expected, err := json.ParseJSON(expectedStr)
	require.NoError(t, err)

	cmp, err := actual.Compare(expected)
	require.NoError(t, err)
	require.Zerof(t, cmp, "expected %s\nbut found %s", expected.String(), actual.String())
}
func TestSQLStatsJsonEncoding(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("statement_statistics", func(t *testing.T) {
		data := GenRandomData()
		input := appstatspb.CollectedStatementStatistics{}

		expectedMetadataStrTemplate := `
{
  "stmtType":     "{{.String}}",
  "query":        "{{.String}}",
  "querySummary": "{{.String}}",
  "db":           "{{.String}}",
  "distsql": {{.Bool}},
  "implicitTxn": {{.Bool}},
  "vec":         {{.Bool}},
  "fullScan":    {{.Bool}}
}
`

		expectedStatisticsStrTemplate := `
     {
       "statistics": {
         "cnt": {{.Int64}},
         "firstAttemptCnt": {{.Int64}},
         "failureCount":    {{.Int64}},
         "maxRetries":      {{.Int64}},
         "lastExecAt":      "{{stringifyTime .Time}}",
         "numRows": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "idleLat": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "parseLat": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "planLat": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "runLat": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "svcLat": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "ovhLat": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "bytesRead": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "rowsRead": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "rowsWritten": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "nodes": [{{joinInts .IntArray}}],
         "kvNodeIds": [{{joinInt32s .Int32Array}}],
         "regions": [{{joinStrings .StringArray}}],
         "usedFollowerRead": {{.Bool}},
         "planGists": [{{joinStrings .StringArray}}],
         "indexes": [{{joinStrings .StringArray}}],
         "latencyInfo": {
           "min": {{.Float}},
           "max": {{.Float}}
         },
         "lastErrorCode": "{{.String}}"
       },
       "execution_statistics": {
         "cnt": {{.Int64}},
         "networkBytes": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "maxMemUsage": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "contentionTime": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "networkMsgs": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "maxDiskUsage": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "cpuSQLNanos": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "mvccIteratorStats": {
           "stepCount": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           },
           "stepCountInternal": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           },
           "seekCount": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           },
           "seekCountInternal": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           },
           "blockBytes": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           },
           "blockBytesInCache": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           },
           "keyBytes": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           },
           "valueBytes": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           },
           "pointCount": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           },
           "pointsCoveredByRangeTombstones": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           },
           "rangeKeyCount": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           },
           "rangeKeyContainedPoints": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           },
           "rangeKeySkippedPoints": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           }
			   }
       },
       "index_recommendations": [{{joinStrings .StringArray}}]
     }
		 `

		expectedMetadataStr := fillTemplate(t, expectedMetadataStrTemplate, data)
		expectedStatisticsStr := fillTemplate(t, expectedStatisticsStrTemplate, data)
		FillObject(t, reflect.ValueOf(&input), &data)

		actualMetadataJSON, err := BuildStmtMetadataJSON(&input)
		require.NoError(t, err)
		jsonTestHelper(t, expectedMetadataStr, actualMetadataJSON)

		actualStatisticsJSON, err := BuildStmtStatisticsJSON(&input.Stats)
		require.NoError(t, err)
		jsonTestHelper(t, expectedStatisticsStr, actualStatisticsJSON)

		// Ensure that we get the same protobuf after we decode the JSON.
		var actualJSONUnmarshalled appstatspb.CollectedStatementStatistics

		err = DecodeStmtStatsMetadataJSON(actualMetadataJSON, &actualJSONUnmarshalled)
		require.NoError(t, err)
		// Strip the monononic part of timestamps, as it doesn't roundtrip. UTC()
		// has that stripping side-effect.
		input.Stats.LastExecTimestamp = input.Stats.LastExecTimestamp.UTC()
		// We are no longer setting the latency percentiles.
		input.Stats.LatencyInfo.P50 = 0
		input.Stats.LatencyInfo.P90 = 0
		input.Stats.LatencyInfo.P99 = 0

		err = DecodeStmtStatsStatisticsJSON(actualStatisticsJSON, &actualJSONUnmarshalled.Stats)
		require.NoError(t, err)
		require.Equal(t, input, actualJSONUnmarshalled)
	})

	// When a new statistic is added to a statement payload, older versions won't have the
	// new parameter, so this test is to confirm that all other parameters will be set and
	// the new one will be empty, without breaking the decoding process.
	t.Run("statement_statistics with new parameter", func(t *testing.T) {
		data := GenRandomData()
		expectedStatistics := appstatspb.CollectedStatementStatistics{}

		expectedMetadataStrTemplate := `
			{
				"stmtType":     "{{.String}}",
				"query":        "{{.String}}",
				"querySummary": "{{.String}}",
				"db":           "{{.String}}",
				"distsql": {{.Bool}},
				"failed":  {{.Bool}},
				"implicitTxn": {{.Bool}},
				"vec":         {{.Bool}},
				"fullScan":    {{.Bool}}
			}
			`
		expectedStatisticsStrTemplate := `
     {
       "statistics": {
         "cnt": {{.Int64}},
         "firstAttemptCnt": {{.Int64}},
         "maxRetries":      {{.Int64}},
         "lastExecAt":      "{{stringifyTime .Time}}",
         "numRows": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "parseLat": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "planLat": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "runLat": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "svcLat": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "ovhLat": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "bytesRead": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "rowsRead": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "rowsWritten": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "nodes": [{{joinInts .IntArray}}],
         "kvNodeIds": [{{joinInt32s .Int32Array}}],
         "usedFollowerRead": {{.Bool}},
         "planGists": [{{joinStrings .StringArray}}],
         "latencyInfo": {
           "min": {{.Float}},
           "max": {{.Float}}
         },
         "errorCode": "{{.String}}"
       },
       "execution_statistics": {
         "cnt": {{.Int64}},
         "networkBytes": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "maxMemUsage": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "contentionTime": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "networkMsgs": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "maxDiskUsage": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "cpuSQLNanos": {
           "mean": {{.Float}},
           "sqDiff": {{.Float}}
         },
         "mvccIteratorStats": {
           "stepCount": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           },
           "stepCountInternal": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           },
           "seekCount": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           },
           "seekCountInternal": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           },
           "blockBytes": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           },
           "blockBytesInCache": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           },
           "keyBytes": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           },
           "valueBytes": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           },
           "pointCount": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           },
           "pointsCoveredByRangeTombstones": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           },
           "rangeKeyCount": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           },
           "rangeKeyContainedPoints": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           },
           "rangeKeySkippedPoints": {
             "mean": {{.Float}},
             "sqDiff": {{.Float}}
           }
			   }
       },
       "index_recommendations": [{{joinStrings .StringArray}}]
     }
		 `

		fillTemplate(t, expectedMetadataStrTemplate, data)
		fillTemplate(t, expectedStatisticsStrTemplate, data)
		FillObject(t, reflect.ValueOf(&expectedStatistics), &data)

		actualMetadataJSON, err := BuildStmtMetadataJSON(&expectedStatistics)
		require.NoError(t, err)
		actualStatisticsJSON, err := BuildStmtStatisticsJSON(&expectedStatistics.Stats)
		require.NoError(t, err)

		var actualJSONUnmarshalled appstatspb.CollectedStatementStatistics

		err = DecodeStmtStatsMetadataJSON(actualMetadataJSON, &actualJSONUnmarshalled)
		require.NoError(t, err)

		// Remove one of the statistics on the object so its value doesn't get populated on
		// the final actualJSONUnmarshalled.Stats.
		actualStatisticsJSON, _, _ = actualStatisticsJSON.RemovePath([]string{"statistics", "numRows"})
		// Initialize the field again to remove the existing value.
		expectedStatistics.Stats.NumRows = appstatspb.NumericStat{}
		// Strip the monononic part of timestamps, as it doesn't roundtrip. UTC()
		// has that stripping side-effect.
		expectedStatistics.Stats.LastExecTimestamp = expectedStatistics.Stats.LastExecTimestamp.UTC()
		// We no longer set the percentile latencies.
		expectedStatistics.Stats.LatencyInfo.P50 = 0
		expectedStatistics.Stats.LatencyInfo.P90 = 0
		expectedStatistics.Stats.LatencyInfo.P99 = 0

		err = DecodeStmtStatsStatisticsJSON(actualStatisticsJSON, &actualJSONUnmarshalled.Stats)
		require.NoError(t, err)
		require.Equal(t, expectedStatistics, actualJSONUnmarshalled)
	})

	t.Run("transaction_statistics", func(t *testing.T) {
		data := GenRandomData()

		input := appstatspb.CollectedTransactionStatistics{
			StatementFingerprintIDs: []appstatspb.StmtFingerprintID{
				1, 100, 1000, 5467890,
			},
		}
		expectedMetadataStr := `
{
  "stmtFingerprintIDs": [
    "0000000000000001",
    "0000000000000064",
    "00000000000003e8",
    "0000000000536ef2"
  ]
}
`

		expectedStatisticsStrTemplate := `
{
  "statistics": {
    "cnt": {{.Int64}},
    "maxRetries": {{.Int64}},
    "numRows": {
      "mean": {{.Float}},
      "sqDiff": {{.Float}}
    },
    "svcLat": {
      "mean": {{.Float}},
      "sqDiff": {{.Float}}
    },
    "retryLat": {
      "mean": {{.Float}},
      "sqDiff": {{.Float}}
    },
    "commitLat": {
      "mean": {{.Float}},
      "sqDiff": {{.Float}}
    },
    "idleLat": {
      "mean": {{.Float}},
      "sqDiff": {{.Float}}
    },
    "bytesRead": {
      "mean": {{.Float}},
      "sqDiff": {{.Float}}
    },
    "rowsRead": {
      "mean": {{.Float}},
      "sqDiff": {{.Float}}
    },
    "rowsWritten": {
      "mean": {{.Float}},
      "sqDiff": {{.Float}}
    }
  },
  "execution_statistics": {
    "cnt": {{.Int64}},
    "networkBytes": {
      "mean": {{.Float}},
      "sqDiff": {{.Float}}
    },
    "maxMemUsage": {
      "mean": {{.Float}},
      "sqDiff": {{.Float}}
    },
    "contentionTime": {
      "mean": {{.Float}},
      "sqDiff": {{.Float}}
    },
    "networkMsgs": {
      "mean": {{.Float}},
      "sqDiff": {{.Float}}
    },
    "maxDiskUsage": {
      "mean": {{.Float}},
      "sqDiff": {{.Float}}
    },
    "cpuSQLNanos": {
      "mean": {{.Float}},
      "sqDiff": {{.Float}}
    },
    "mvccIteratorStats": {
      "stepCount": {
        "mean": {{.Float}},
        "sqDiff": {{.Float}}
      },
      "stepCountInternal": {
        "mean": {{.Float}},
        "sqDiff": {{.Float}}
      },
      "seekCount": {
        "mean": {{.Float}},
        "sqDiff": {{.Float}}
      },
      "seekCountInternal": {
        "mean": {{.Float}},
        "sqDiff": {{.Float}}
      },
      "blockBytes": {
        "mean": {{.Float}},
        "sqDiff": {{.Float}}
      },
      "blockBytesInCache": {
        "mean": {{.Float}},
        "sqDiff": {{.Float}}
      },
      "keyBytes": {
        "mean": {{.Float}},
        "sqDiff": {{.Float}}
      },
      "valueBytes": {
        "mean": {{.Float}},
        "sqDiff": {{.Float}}
      },
      "pointCount": {
        "mean": {{.Float}},
        "sqDiff": {{.Float}}
      },
      "pointsCoveredByRangeTombstones": {
        "mean": {{.Float}},
        "sqDiff": {{.Float}}
      },
      "rangeKeyCount": {
        "mean": {{.Float}},
        "sqDiff": {{.Float}}
      },
      "rangeKeyContainedPoints": {
        "mean": {{.Float}},
        "sqDiff": {{.Float}}
      },
      "rangeKeySkippedPoints": {
        "mean": {{.Float}},
        "sqDiff": {{.Float}}
      }
    }
  }
}
		 `
		expectedStatisticsStr := fillTemplate(t, expectedStatisticsStrTemplate, data)
		FillObject(t, reflect.ValueOf(&input), &data)

		actualMetadataJSON, err := BuildTxnMetadataJSON(&input)
		require.NoError(t, err)
		jsonTestHelper(t, expectedMetadataStr, actualMetadataJSON)

		actualStatisticsJSON, err := BuildTxnStatisticsJSON(&input)
		require.NoError(t, err)
		jsonTestHelper(t, expectedStatisticsStr, actualStatisticsJSON)

		// Ensure that we get the same protobuf after we decode the JSON.
		var actualJSONUnmarshalled appstatspb.CollectedTransactionStatistics

		err = DecodeTxnStatsMetadataJSON(actualMetadataJSON, &actualJSONUnmarshalled)
		require.NoError(t, err)

		err = DecodeTxnStatsStatisticsJSON(actualStatisticsJSON, &actualJSONUnmarshalled.Stats)
		require.NoError(t, err)
		require.Equal(t, input, actualJSONUnmarshalled)
	})

	t.Run("statement aggregated metadata", func(t *testing.T) {
		data := GenRandomData()

		input := appstatspb.AggregatedStatementMetadata{}

		expectedAggregatedMetadataStrTemplate := `
{
  "stmtType": "{{.String}}",
  "query": "{{.String}}",
  "formattedQuery": "{{.String}}",
  "querySummary": "{{.String}}",
  "implicitTxn": {{.Bool}},
  "distSQLCount": {{.Int64}},
  "vecCount": {{.Int64}},
  "fullScanCount": {{.Int64}},
  "totalCount": {{.Int64}},
  "db": [{{joinStrings .StringArray}}],
  "appNames": [{{joinStrings .StringArray}}],
  "fingerprintID": "{{.String}}"
}
		 `
		expectedAggregatedMetadataStr := fillTemplate(t, expectedAggregatedMetadataStrTemplate, data)
		FillObject(t, reflect.ValueOf(&input), &data)

		actualMetadataJSON, err := BuildStmtDetailsMetadataJSON(&input)
		require.NoError(t, err)
		jsonTestHelper(t, expectedAggregatedMetadataStr, actualMetadataJSON)

		// Ensure that we get the same protobuf after we decode the JSON.
		var actualJSONUnmarshalled appstatspb.AggregatedStatementMetadata
		err = DecodeAggregatedMetadataJSON(actualMetadataJSON, &actualJSONUnmarshalled)
		require.NoError(t, err)
		require.Equal(t, input, actualJSONUnmarshalled)
	})

	t.Run("random metadata JSON structure and values", func(t *testing.T) {
		input := `{"HcEN0pht": null, "bar": [false, "foobar", true], "c": {"8r8qmK": 1.4687388461922657, "vbF3TH0I": null, "yNNmkGr6": 2.0887609844362762}, "db": {"KKDmambo": 0.10124464952424544}, "foobar": {"LM8G": {"zniUw24Z": 0.14422951431111297}, "bar": "lp7jfTq2"}, "r0O": false}`
		value, err := json.ParseJSON(input)
		require.NoError(t, err)
		var actualJSONUnmarshalled appstatspb.AggregatedStatementMetadata
		err = DecodeAggregatedMetadataJSON(value, &actualJSONUnmarshalled)
		require.NoError(t, err)
	})
}

func BenchmarkSQLStatsJson(b *testing.B) {
	defer log.Scope(b).Close(b)
	b.Run("statement_stats", func(b *testing.B) {
		inputStmtStats := appstatspb.CollectedStatementStatistics{}
		b.Run("encoding", func(b *testing.B) {
			b.SetBytes(int64(inputStmtStats.Size()))

			for i := 0; i < b.N; i++ {
				_, err := BuildStmtMetadataJSON(&inputStmtStats)
				if err != nil {
					b.Fatal(err)
				}
				_, err = BuildStmtStatisticsJSON(&inputStmtStats.Stats)
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		inputStmtStatsMetaJSON, _ := BuildStmtMetadataJSON(&inputStmtStats)
		inputStmtStatsJSON, _ := BuildStmtStatisticsJSON(&inputStmtStats.Stats)
		result := appstatspb.CollectedStatementStatistics{}

		b.Run("decoding", func(b *testing.B) {
			b.SetBytes(int64(inputStmtStatsJSON.Size() + inputStmtStatsMetaJSON.Size()))

			for i := 0; i < b.N; i++ {
				err := DecodeStmtStatsMetadataJSON(inputStmtStatsMetaJSON, &result)
				if err != nil {
					b.Fatal(err)
				}
				err = DecodeStmtStatsStatisticsJSON(inputStmtStatsJSON, &result.Stats)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	})

	b.Run("transaction_stats", func(b *testing.B) {
		inputTxnStats := appstatspb.CollectedTransactionStatistics{}
		b.Run("encoding", func(b *testing.B) {
			b.SetBytes(int64(inputTxnStats.Size()))

			for i := 0; i < b.N; i++ {
				_, err := BuildTxnMetadataJSON(&inputTxnStats)
				if err != nil {
					b.Fatal(err)
				}
				_, err = BuildTxnStatisticsJSON(&inputTxnStats)
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		inputTxnStatsJSON, _ := BuildTxnStatisticsJSON(&inputTxnStats)
		inputTxnStatsMetaJSON, err := BuildTxnMetadataJSON(&inputTxnStats)
		if err != nil {
			b.Fatal(err)
		}

		result := appstatspb.CollectedTransactionStatistics{}

		b.Run("decoding", func(b *testing.B) {
			b.SetBytes(int64(inputTxnStatsJSON.Size() + inputTxnStatsMetaJSON.Size()))

			for i := 0; i < b.N; i++ {
				err := DecodeTxnStatsMetadataJSON(inputTxnStatsMetaJSON, &result)
				if err != nil {
					b.Fatal(err)
				}
				err = DecodeTxnStatsStatisticsJSON(inputTxnStatsJSON, &result.Stats)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	})

	b.Run("statement_metadata", func(b *testing.B) {
		inputStmtStats := appstatspb.CollectedStatementStatistics{}
		inputStmtMetadata := appstatspb.AggregatedStatementMetadata{}
		b.Run("encoding", func(b *testing.B) {
			b.SetBytes(int64(inputStmtStats.Size()))

			for i := 0; i < b.N; i++ {
				_, err := BuildStmtDetailsMetadataJSON(&inputStmtMetadata)
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		inputStmtStatsAggregatedMetaJSON, _ := BuildStmtMetadataJSON(&inputStmtStats)
		result := appstatspb.AggregatedStatementMetadata{}

		b.Run("decoding", func(b *testing.B) {
			b.SetBytes(int64(inputStmtStatsAggregatedMetaJSON.Size()))

			for i := 0; i < b.N; i++ {
				err := DecodeAggregatedMetadataJSON(inputStmtStatsAggregatedMetaJSON, &result)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	})
}

// TestExplainTreePlanNodeToJSON tests whether the ExplainTreePlanNode function
// correctly builds a JSON object from an ExplainTreePlanNode.
func TestExplainTreePlanNodeToJSON(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testDataArr := []struct {
		explainTree appstatspb.ExplainTreePlanNode
		expected    string
	}{
		// Test data using a node with multiple inner children.
		{
			appstatspb.ExplainTreePlanNode{
				Name: "root",
				Attrs: []*appstatspb.ExplainTreePlanNode_Attr{
					{
						Key:   "rootKey",
						Value: "rootValue",
					},
				},
				Children: []*appstatspb.ExplainTreePlanNode{
					{
						Name: "child",
						Attrs: []*appstatspb.ExplainTreePlanNode_Attr{
							{
								Key:   "childKey",
								Value: "childValue",
							},
						},
						Children: []*appstatspb.ExplainTreePlanNode{
							{
								Name: "innerChild",
								Attrs: []*appstatspb.ExplainTreePlanNode_Attr{
									{
										Key:   "innerChildKey",
										Value: "innerChildValue",
									},
								},
							},
						},
					},
				},
			},
			`{"Children": [{"ChildKey": "childValue", "Children": [{"Children": [], "InnerChildKey": "innerChildValue", "Name": "innerChild"}], "Name": "child"}], "Name": "root", "RootKey": "rootValue"}`,
		},
		// Test using a node with multiple attributes.
		{
			appstatspb.ExplainTreePlanNode{
				Name: "root",
				Attrs: []*appstatspb.ExplainTreePlanNode_Attr{
					{
						Key:   "rootFirstKey",
						Value: "rootFirstValue",
					},
					{
						Key:   "rootSecondKey",
						Value: "rootSecondValue",
					},
				},
				Children: []*appstatspb.ExplainTreePlanNode{
					{
						Name: "child",
						Attrs: []*appstatspb.ExplainTreePlanNode_Attr{
							{
								Key:   "childKey",
								Value: "childValue",
							},
						},
					},
				},
			},
			`{"Children": [{"ChildKey": "childValue", "Children": [], "Name": "child"}], "Name": "root", "RootFirstKey": "rootFirstValue", "RootSecondKey": "rootSecondValue"}`,
		},
		// Test using a node with multiple children and multiple inner children.
		{
			appstatspb.ExplainTreePlanNode{
				Name: "root",
				Attrs: []*appstatspb.ExplainTreePlanNode_Attr{
					{
						Key:   "rootKey",
						Value: "rootValue",
					},
				},
				Children: []*appstatspb.ExplainTreePlanNode{
					{
						Name: "firstChild",
						Attrs: []*appstatspb.ExplainTreePlanNode_Attr{
							{
								Key:   "firstChildKey",
								Value: "firstChildValue",
							},
						},
						Children: []*appstatspb.ExplainTreePlanNode{
							{
								Name: "innerChild",
								Attrs: []*appstatspb.ExplainTreePlanNode_Attr{
									{
										Key:   "innerChildKey",
										Value: "innerChildValue",
									},
								},
							},
						},
					},
					{
						Name: "secondChild",
						Attrs: []*appstatspb.ExplainTreePlanNode_Attr{
							{
								Key:   "secondChildKey",
								Value: "secondChildValue",
							},
						},
					},
				},
			},
			`{"Children": [{"Children": [{"Children": [], "InnerChildKey": "innerChildValue", "Name": "innerChild"}], "FirstChildKey": "firstChildValue", "Name": "firstChild"}, {"Children": [], "Name": "secondChild", "SecondChildKey": "secondChildValue"}], "Name": "root", "RootKey": "rootValue"}`,
		},
	}

	for _, testData := range testDataArr {
		explainTreeJSON := ExplainTreePlanNodeToJSON(&testData.explainTree)
		require.Equal(t, testData.expected, explainTreeJSON.String())

		explainTreeProto, err := JSONToExplainTreePlanNode(explainTreeJSON)
		require.NoError(t, err)
		compareExplainTree(t, &testData.explainTree, explainTreeProto)
	}
}

type nodeAttrList []*appstatspb.ExplainTreePlanNode_Attr

var _ sort.Interface = nodeAttrList{}

func (n nodeAttrList) Len() int {
	return len(n)
}

func (n nodeAttrList) Less(i, j int) bool {
	return strings.Compare(n[i].Key, n[j].Key) == -1
}

func (n nodeAttrList) Swap(i, j int) {
	tmp := n[i]
	n[i] = n[j]
	n[j] = tmp
}

type nodeList []*appstatspb.ExplainTreePlanNode

var _ sort.Interface = nodeList{}

func (n nodeList) Len() int {
	return len(n)
}

func (n nodeList) Less(i, j int) bool {
	return strings.Compare(n[i].Name, n[j].Name) == -1
}

func (n nodeList) Swap(i, j int) {
	tmp := n[i]
	n[i] = n[j]
	n[j] = tmp
}

func compareExplainTree(t *testing.T, expected, actual *appstatspb.ExplainTreePlanNode) {
	require.Equal(t, strings.ToLower(expected.Name), strings.ToLower(actual.Name))
	require.Equal(t, len(expected.Attrs), len(actual.Attrs))

	// Sorts the Attrs so we have consistent result.
	sort.Sort(nodeAttrList(expected.Attrs))
	sort.Sort(nodeAttrList(actual.Attrs))

	for i := range expected.Attrs {
		require.Equal(t, strings.ToLower(expected.Attrs[i].Key), strings.ToLower(actual.Attrs[i].Key))
		require.Equal(t, expected.Attrs[i].Value, actual.Attrs[i].Value)
	}

	require.Equal(t, len(expected.Children), len(actual.Children), "expected %+v, but found %+v", expected.Children, actual.Children)

	// Sorts the Children so we can recursively compare them.
	sort.Sort(nodeList(expected.Children))
	sort.Sort(nodeList(actual.Children))

	for i := range expected.Children {
		compareExplainTree(t, expected.Children[i], actual.Children[i])
	}
}
