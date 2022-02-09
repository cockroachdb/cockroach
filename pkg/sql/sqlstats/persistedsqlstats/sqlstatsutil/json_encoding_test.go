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
	"reflect"
	"sort"
	"strings"
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
	require.Zerof(t, cmp, "expected %s\nbut found %s", expected.String(), actual.String())
}
func TestSQLStatsJsonEncoding(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	t.Run("statement_statistics", func(t *testing.T) {
		data := genRandomData()
		input := roachpb.CollectedStatementStatistics{}

		expectedMetadataStrTemplate := `
{
  "stmtTyp":      "{{.String}}",
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
         "planGists": [{{joinStrings .StringArray}}]
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
         }
       }
     }
		 `

		expectedMetadataStr := fillTemplate(t, expectedMetadataStrTemplate, data)
		expectedStatisticsStr := fillTemplate(t, expectedStatisticsStrTemplate, data)
		fillObject(t, reflect.ValueOf(&input), &data)

		actualMetadataJSON, err := BuildStmtMetadataJSON(&input)
		require.NoError(t, err)
		jsonTestHelper(t, expectedMetadataStr, actualMetadataJSON)

		actualStatisticsJSON, err := BuildStmtStatisticsJSON(&input.Stats)
		require.NoError(t, err)
		jsonTestHelper(t, expectedStatisticsStr, actualStatisticsJSON)

		// Ensure that we get the same protobuf after we decode the JSON.
		var actualJSONUnmarshalled roachpb.CollectedStatementStatistics

		err = DecodeStmtStatsMetadataJSON(actualMetadataJSON, &actualJSONUnmarshalled)
		require.NoError(t, err)

		err = DecodeStmtStatsStatisticsJSON(actualStatisticsJSON, &actualJSONUnmarshalled.Stats)
		require.NoError(t, err)
		require.Equal(t, input, actualJSONUnmarshalled)
	})

	// When a new statistic is added to a statement payload, older versions won't have the
	// new parameter, so this test is to confirm that all other parameters will be set and
	// the new one will be empty, without breaking the decoding process.
	t.Run("statement_statistics with new parameter", func(t *testing.T) {
		data := genRandomData()
		expectedStatistics := roachpb.CollectedStatementStatistics{}

		expectedMetadataStrTemplate := `
			{
				"stmtTyp":      "{{.String}}",
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
         "nodes": [{{joinInts .IntArray}}]
         "planGists": [{{joinStrings .StringArray}}]
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
         }
       }
     }
		 `

		fillTemplate(t, expectedMetadataStrTemplate, data)
		fillTemplate(t, expectedStatisticsStrTemplate, data)
		fillObject(t, reflect.ValueOf(&expectedStatistics), &data)

		actualMetadataJSON, err := BuildStmtMetadataJSON(&expectedStatistics)
		require.NoError(t, err)
		actualStatisticsJSON, err := BuildStmtStatisticsJSON(&expectedStatistics.Stats)
		require.NoError(t, err)

		var actualJSONUnmarshalled roachpb.CollectedStatementStatistics

		err = DecodeStmtStatsMetadataJSON(actualMetadataJSON, &actualJSONUnmarshalled)
		require.NoError(t, err)

		// Remove one of the statistics on the object so its value doesn't get populated on
		// the final actualJSONUnmarshalled.Stats.
		actualStatisticsJSON, _, _ = actualStatisticsJSON.RemovePath([]string{"statistics", "numRows"})
		// Initialize the field again to remove the existing value.
		expectedStatistics.Stats.NumRows = roachpb.NumericStat{}

		err = DecodeStmtStatsStatisticsJSON(actualStatisticsJSON, &actualJSONUnmarshalled.Stats)
		require.NoError(t, err)
		require.Equal(t, expectedStatistics, actualJSONUnmarshalled)
	})

	t.Run("transaction_statistics", func(t *testing.T) {
		data := genRandomData()

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
    }
  }
}
		 `
		expectedStatisticsStr := fillTemplate(t, expectedStatisticsStrTemplate, data)
		fillObject(t, reflect.ValueOf(&input), &data)

		actualMetadataJSON, err := BuildTxnMetadataJSON(&input)
		require.NoError(t, err)
		jsonTestHelper(t, expectedMetadataStr, actualMetadataJSON)

		actualStatisticsJSON, err := BuildTxnStatisticsJSON(&input)
		require.NoError(t, err)
		jsonTestHelper(t, expectedStatisticsStr, actualStatisticsJSON)

		// Ensure that we get the same protobuf after we decode the JSON.
		var actualJSONUnmarshalled roachpb.CollectedTransactionStatistics

		err = DecodeTxnStatsMetadataJSON(actualMetadataJSON, &actualJSONUnmarshalled)
		require.NoError(t, err)

		err = DecodeTxnStatsStatisticsJSON(actualStatisticsJSON, &actualJSONUnmarshalled.Stats)
		require.NoError(t, err)
		require.Equal(t, input, actualJSONUnmarshalled)
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
				_, err = BuildStmtStatisticsJSON(&inputStmtStats.Stats)
				if err != nil {
					b.Fatal(err)
				}
			}
		})

		inputStmtStatsMetaJSON, _ := BuildStmtMetadataJSON(&inputStmtStats)
		inputStmtStatsJSON, _ := BuildStmtStatisticsJSON(&inputStmtStats.Stats)
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

		explainTreeProto, err := JSONToExplainTreePlanNode(explainTreeJSON)
		require.NoError(t, err)
		compareExplainTree(t, &testData.explainTree, explainTreeProto)
	}
}

type nodeAttrList []*roachpb.ExplainTreePlanNode_Attr

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

type nodeList []*roachpb.ExplainTreePlanNode

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

func compareExplainTree(t *testing.T, expected, actual *roachpb.ExplainTreePlanNode) {
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
