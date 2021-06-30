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
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
	require.True(t, cmp == 0, "expected %s\nbut found %s", expected.String(), actual.String())
}

func TestSQLStatsJsonEncoding(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("encode statement metadata", func(t *testing.T) {
		input := roachpb.CollectedStatementStatistics{}
		expectedMetadataStr := `
{
  "stmtTyp": "",
  "query":   "",
  "db":      "",
  "distsql": false,
  "failed":  false,
  "opt":     false,
  "implicitTxn": false,
  "vec":         false,
  "fullScan":    false
}
`

		actualJSON, err := BuildStmtMetadataJSON(&input)
		require.NoError(t, err)

		// Ensure that we get the same protobuf after we decode the JSON.
		var actualJSONUnmarshalled roachpb.CollectedStatementStatistics
		err = DecodeStmtStatsMetadataJSON(actualJSON, &actualJSONUnmarshalled)
		require.NoError(t, err)
		require.Equal(t, input, actualJSONUnmarshalled)

		jsonTestHelper(t, expectedMetadataStr, actualJSON)
	})

	t.Run("encode statement statistics", func(t *testing.T) {
		input := roachpb.CollectedStatementStatistics{}
		expectedStatisticsStr := `
     {
       "statistics": {
         "firstAttemptCnt": 0,
         "maxRetries":      0,
         "lastExecAt":      "0001-01-01T00:00:00Z",
         "numRows": {
           "mean": 0.0,
           "sqDiff": 0.0
         },
         "parseLat": {
           "mean": 0.0,
           "sqDiff": 0.0
         },
         "planLat": {
           "mean": 0.0,
           "sqDiff": 0.0
         },
         "runLat": {
           "mean": 0.0,
           "sqDiff": 0.0
         },
         "svcLat": {
           "mean": 0.0,
           "sqDiff": 0.0
         },
         "ovhLat": {
           "mean": 0.0,
           "sqDiff": 0.0
         },
         "bytesRead": {
           "mean": 0.0,
           "sqDiff": 0.0
         },
         "rowsRead": {
           "mean": 0.0,
           "sqDiff": 0.0
         }
       },
       "execution_statistics": {
         "cnt": 0,
         "networkBytes": {
           "mean": 0.0,
           "sqDiff": 0.0
         },
         "maxMemUsage": {
           "mean": 0.0,
           "sqDiff": 0.0
         },
         "contentionTime": {
           "mean": 0.0,
           "sqDiff": 0.0
         },
         "networkMsgs": {
           "mean": 0.0,
           "sqDiff": 0.0
         },
         "maxDiskUsage": {
           "mean": 0.0,
           "sqDiff": 0.0
         }
       }
     }
		 `

		actualJSON, err := BuildStmtStatisticsJSON(&input)
		require.NoError(t, err)

		// Ensure that we get the same protobuf after we decode the JSON.
		var actualJSONUnmarshalled roachpb.CollectedStatementStatistics
		err = DecodeStmtStatsStatisticsJSON(actualJSON, &actualJSONUnmarshalled.Stats)
		require.NoError(t, err)
		require.Equal(t, input, actualJSONUnmarshalled)

		jsonTestHelper(t, expectedStatisticsStr, actualJSON)
	})

	t.Run("encode transaction metadata", func(t *testing.T) {
		input := roachpb.CollectedTransactionStatistics{
			StatementFingerprintIDs: []roachpb.StmtFingerprintID{
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
		actualJSON := BuildTxnMetadataJSON(&input)

		// Ensure that we get the same protobuf after we decode the JSON.
		var actualJSONUnmarshalled roachpb.CollectedTransactionStatistics
		err := DecodeTxnStatsMetadataJSON(actualJSON, &actualJSONUnmarshalled)
		require.NoError(t, err)
		require.Equal(t, input, actualJSONUnmarshalled)

		jsonTestHelper(t, expectedMetadataStr, actualJSON)
	})

	t.Run("encode transaction statistics", func(t *testing.T) {
		input := roachpb.CollectedTransactionStatistics{}
		expectedStatisticsStr := `
{
  "statistics": {
    "maxRetries": 0,
    "numRows": {
      "mean": 0.0,
      "sqDiff": 0.0
    },
    "svcLat": {
      "mean": 0.0,
      "sqDiff": 0.0
    },
    "retryLat": {
      "mean": 0.0,
      "sqDiff": 0.0
    },
    "commitLat": {
      "mean": 0.0,
      "sqDiff": 0.0
    },
    "bytesRead": {
      "mean": 0.0,
      "sqDiff": 0.0
    },
    "rowsRead": {
      "mean": 0.0,
      "sqDiff": 0.0
    }
  },
  "execution_statistics": {
    "cnt": 0,
    "networkBytes": {
      "mean": 0.0,
      "sqDiff": 0.0
    },
    "maxMemUsage": {
      "mean": 0.0,
      "sqDiff": 0.0
    },
    "contentionTime": {
      "mean": 0.0,
      "sqDiff": 0.0
    },
    "networkMsgs": {
      "mean": 0.0,
      "sqDiff": 0.0
    },
    "maxDiskUsage": {
      "mean": 0.0,
      "sqDiff": 0.0
    }
  }
}
`
		actualJSON, err := BuildTxnStatisticsJSON(&input)
		require.NoError(t, err)

		// Ensure that we get the same protobuf after we decode the JSON.
		var actualJSONUnmarshalled roachpb.CollectedTransactionStatistics
		err = DecodeTxnStatsStatisticsJSON(actualJSON, &actualJSONUnmarshalled.Stats)
		require.NoError(t, err)
		require.Equal(t, input, actualJSONUnmarshalled)

		jsonTestHelper(t, expectedStatisticsStr, actualJSON)
	})
}

func BenchmarkSQLStatsJson(b *testing.B) {
	defer log.Scope(b).Close(b)
	b.Run("statement_stats", func(b *testing.B) {
		inputStmtStats := roachpb.CollectedStatementStatistics{}
		b.Run("encoding", func(b *testing.B) {
			b.SetBytes(int64(inputStmtStats.Size()))

			for i := 0; i < b.N; i++ {
				_, err := BuildStmtMetadataJSON(&inputStmtStats)
				if err != nil {
					b.Fatal(err)
				}
				_, err = BuildStmtStatisticsJSON(&inputStmtStats)
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		inputStmtStatsMetaJSON, _ := BuildStmtMetadataJSON(&inputStmtStats)
		inputStmtStatsJSON, _ := BuildStmtStatisticsJSON(&inputStmtStats)
		result := roachpb.CollectedStatementStatistics{}

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
		inputTxnStats := roachpb.CollectedTransactionStatistics{}
		b.Run("encoding", func(b *testing.B) {
			b.SetBytes(int64(inputTxnStats.Size()))

			for i := 0; i < b.N; i++ {
				_ = BuildTxnMetadataJSON(&inputTxnStats)
				_, err := BuildTxnStatisticsJSON(&inputTxnStats)
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		inputTxnStatsJSON, _ := BuildTxnStatisticsJSON(&inputTxnStats)
		inputTxnStatsMetaJSON := BuildTxnMetadataJSON(&inputTxnStats)
		result := roachpb.CollectedTransactionStatistics{}

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
}

// TestExplainTreePlanNodeToJSON tests whether the ExplainTreePlanNode function
// correctly builds a JSON object from an ExplainTreePlanNode.
func TestExplainTreePlanNodeToJSON(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testDataArr := []struct {
		explainTree roachpb.ExplainTreePlanNode
		expected    string
	}{
		// Test data using a node with multiple inner children.
		{
			roachpb.ExplainTreePlanNode{
				Name: "root",
				Attrs: []*roachpb.ExplainTreePlanNode_Attr{
					{
						Key:   "rootKey",
						Value: "rootValue",
					},
				},
				Children: []*roachpb.ExplainTreePlanNode{
					{
						Name: "child",
						Attrs: []*roachpb.ExplainTreePlanNode_Attr{
							{
								Key:   "childKey",
								Value: "childValue",
							},
						},
						Children: []*roachpb.ExplainTreePlanNode{
							{
								Name: "innerChild",
								Attrs: []*roachpb.ExplainTreePlanNode_Attr{
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
			roachpb.ExplainTreePlanNode{
				Name: "root",
				Attrs: []*roachpb.ExplainTreePlanNode_Attr{
					{
						Key:   "rootFirstKey",
						Value: "rootFirstValue",
					},
					{
						Key:   "rootSecondKey",
						Value: "rootSecondValue",
					},
				},
				Children: []*roachpb.ExplainTreePlanNode{
					{
						Name: "child",
						Attrs: []*roachpb.ExplainTreePlanNode_Attr{
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
			roachpb.ExplainTreePlanNode{
				Name: "root",
				Attrs: []*roachpb.ExplainTreePlanNode_Attr{
					{
						Key:   "rootKey",
						Value: "rootValue",
					},
				},
				Children: []*roachpb.ExplainTreePlanNode{
					{
						Name: "firstChild",
						Attrs: []*roachpb.ExplainTreePlanNode_Attr{
							{
								Key:   "firstChildKey",
								Value: "firstChildValue",
							},
						},
						Children: []*roachpb.ExplainTreePlanNode{
							{
								Name: "innerChild",
								Attrs: []*roachpb.ExplainTreePlanNode_Attr{
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
						Attrs: []*roachpb.ExplainTreePlanNode_Attr{
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
	}
}
