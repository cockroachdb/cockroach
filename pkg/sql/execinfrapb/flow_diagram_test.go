// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execinfrapb

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	roachpb "github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// compareDiagrams verifies that two JSON strings decode to equal diagramData
// structures. This allows the expected string to be formatted differently.
func compareDiagrams(t *testing.T, result string, expected string) {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(result))
	var resData, expData diagramData
	if err := dec.Decode(&resData); err != nil {
		t.Fatalf("error decoding '%s': %s", result, err)
	}
	dec = json.NewDecoder(strings.NewReader(expected))
	if err := dec.Decode(&expData); err != nil {
		t.Fatal(err)
	}
	fmt.Printf("res: %+v\n", resData)
	fmt.Printf("exp: %+v\n", expData)
	if !reflect.DeepEqual(resData, expData) {
		t.Errorf("\ngot:\n%s\nwant:\n%s", result, expected)
	}
}

func TestPlanDiagramIndexJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()

	flows := make(map[base.SQLInstanceID]*FlowSpec)

	tr := TableReaderSpec{
		FetchSpec: fetchpb.IndexFetchSpec{
			TableName: "Table",
			IndexName: "SomeIndex",
			FetchedColumns: []fetchpb.IndexFetchSpec_Column{
				{Name: "a"},
				{Name: "b"},
			},
		},
	}

	flows[1] = &FlowSpec{
		Processors: []ProcessorSpec{{
			Core: ProcessorCoreUnion{TableReader: &tr},
			Output: []OutputRouterSpec{{
				Type: OutputRouterSpec_PASS_THROUGH,
				Streams: []StreamEndpointSpec{
					{StreamID: 0},
				},
			}},
			StageID:     1,
			ProcessorID: 0,
		}},
	}

	flows[2] = &FlowSpec{
		Processors: []ProcessorSpec{{
			Core: ProcessorCoreUnion{TableReader: &tr},
			Output: []OutputRouterSpec{{
				Type: OutputRouterSpec_PASS_THROUGH,
				Streams: []StreamEndpointSpec{
					{StreamID: 1},
				},
			}},
			StageID:     1,
			ProcessorID: 1,
		}},
	}

	flows[3] = &FlowSpec{
		Processors: []ProcessorSpec{
			{
				Core: ProcessorCoreUnion{TableReader: &tr},
				Output: []OutputRouterSpec{{
					Type: OutputRouterSpec_PASS_THROUGH,
					Streams: []StreamEndpointSpec{
						{StreamID: 2},
					},
				}},
				StageID:     1,
				ProcessorID: 2,
			},
			{
				Input: []InputSyncSpec{{
					Type: InputSyncSpec_ORDERED,
					Ordering: Ordering{Columns: []Ordering_Column{
						{ColIdx: 1, Direction: Ordering_Column_ASC}},
					},
					Streams: []StreamEndpointSpec{
						{StreamID: 0},
						{StreamID: 1},
						{StreamID: 2},
					},
				}},
				Core: ProcessorCoreUnion{JoinReader: &JoinReaderSpec{
					FetchSpec: fetchpb.IndexFetchSpec{
						TableName: "Table",
						IndexName: "primary",
						FetchedColumns: []fetchpb.IndexFetchSpec_Column{
							{Name: "x"},
							{Name: "y"},
						},
					},
				}},
				Post: PostProcessSpec{
					Projection:    true,
					OutputColumns: []uint32{2},
				},
				Output: []OutputRouterSpec{{
					Type:    OutputRouterSpec_PASS_THROUGH,
					Streams: []StreamEndpointSpec{{Type: StreamEndpointSpec_SYNC_RESPONSE}},
				}},
				StageID:     2,
				ProcessorID: 3,
			},
		},
	}

	flags := DiagramFlags{
		ShowInputTypes: true,
	}
	json, url, err := GeneratePlanDiagramURL("SOME SQL HERE", flows, flags)
	if err != nil {
		t.Fatal(err)
	}

	// Test that deserializing the URL also results in the same plan.
	diagram, err := GeneratePlanDiagram("SOME SQL HERE", flows, flags)
	require.NoError(t, err)
	deserializedDiagram, err := FromURL(url.String())
	require.NoError(t, err)
	require.Equal(t, diagram, deserializedDiagram)

	expected := `
{
  "sql": "SOME SQL HERE",
  "nodeNames": [
    "1",
    "2",
    "3"
  ],
  "processors": [
    {
      "nodeIdx": 0,
      "inputs": [],
      "core": {
        "title": "TableReader/0",
        "details": [
          "Table@SomeIndex",
          "Columns: a, b"
        ]
      },
      "outputs": [],
      "stage": 1,
      "processorID": 0
    },
    {
      "nodeIdx": 1,
      "inputs": [],
      "core": {
        "title": "TableReader/1",
        "details": [
          "Table@SomeIndex",
          "Columns: a, b"
        ]
      },
      "outputs": [],
      "stage": 1,
      "processorID": 1
    },
    {
      "nodeIdx": 2,
      "inputs": [],
      "core": {
        "title": "TableReader/2",
        "details": [
          "Table@SomeIndex",
          "Columns: a, b"
        ]
      },
      "outputs": [],
      "stage": 1,
      "processorID": 2
    },
    {
      "nodeIdx": 2,
      "inputs": [
        {
          "title": "ordered",
          "details": [
            "@2+"
          ]
        }
      ],
      "core": {
        "title": "JoinReader/3",
        "details": [
          "Table@primary",
          "Columns: x, y",
          "Out: @3"
        ]
      },
      "outputs": [],
      "stage": 2,
      "processorID": 3
    },
    {
      "nodeIdx": 2,
      "inputs": [],
      "core": {
        "title": "Response",
        "details": []
      },
      "outputs": [],
      "stage": 0,
      "processorID": -1
    }
  ],
  "edges": [
    {
      "sourceProc": 0,
      "sourceOutput": 0,
      "destProc": 3,
      "destInput": 1,
      "streamID": 0
    },
    {
      "sourceProc": 1,
      "sourceOutput": 0,
      "destProc": 3,
      "destInput": 1,
      "streamID": 1
    },
    {
      "sourceProc": 2,
      "sourceOutput": 0,
      "destProc": 3,
      "destInput": 1,
      "streamID": 2
    },
    {
      "sourceProc": 3,
      "sourceOutput": 0,
      "destProc": 4,
      "destInput": 0,
      "streamID": 0
    }
  ],
  "flow_id": "00000000-0000-0000-0000-000000000000",
  "flags": {
    "ShowInputTypes": true,
    "MakeDeterministic": false
  }
}
	`

	compareDiagrams(t, json, expected)

	expectedURL := `https://cockroachdb.github.io/distsqlplan/decode.html#eJy0kkFr2zAUx-_7FOZ_ncoseSedAmtgGeuyJb0NM1TrNROzJU-SaUrwdx9SujUOyWgL1cHw9Jd_76dn7xB-t5BYL6_mxfrb5-LjfDUHg3WavqiOAuR3cDAIMFSoGXrvGgrB-RTt8sGF3kKWDMb2Q0zbNUPjPEHuEE1sCRLX6qalFSlN_l0JBk1RmTbjczRbu44WVtMWDB9cO3Q2yEKx4gb1yOCG-MgOUW0Ikh_ILC4hy5Ed-PCn-_DX8eETH_F0H_E6PuKsz6OG85o86anATLxFPZ6Q_uSMfXCuTjn33nTK3x8ab1mR6uUQZTGrzrqLI_fqObNcUeidDTRROtepPOp0wdNNSW9oP5ngBt_QV--afHZfLjMob2gKcZ9W-2Jhc8RTB0-q-_drHpL4S0n8mCReShLHpOq_pPcTUjm9Xc1w27q7H0ZDonxYFycefxfSC2oT0mdb_3R3GXt936ehRz8Qw5X6RZcUyXfGmhBNA3mr2kDj-OZPAAAA___R4oYQ`
	if url.String() != expectedURL {
		t.Errorf("expected `%s` got `%s`", expectedURL, url.String())
	}
}

func TestPlanDiagramJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()

	flows := make(map[base.SQLInstanceID]*FlowSpec)

	trA := TableReaderSpec{
		FetchSpec: fetchpb.IndexFetchSpec{
			TableName: "TableA",
			IndexName: "primary",
			FetchedColumns: []fetchpb.IndexFetchSpec_Column{
				{Name: "a"},
				{Name: "b"},
				{Name: "d"},
			},
		},
	}

	trB := TableReaderSpec{
		FetchSpec: fetchpb.IndexFetchSpec{
			TableName: "TableB",
			IndexName: "primary",
			FetchedColumns: []fetchpb.IndexFetchSpec_Column{
				{Name: "b"},
				{Name: "c"},
				{Name: "e"},
			},
		},
	}

	hj := HashJoinerSpec{
		LeftEqColumns:  []uint32{0, 2},
		RightEqColumns: []uint32{2, 1},
		OnExpr:         Expression{Expr: "@1+@2<@6"},
	}

	flows[1] = &FlowSpec{
		Processors: []ProcessorSpec{
			{
				Core: ProcessorCoreUnion{TableReader: &trA},
				Output: []OutputRouterSpec{{
					Type:        OutputRouterSpec_BY_HASH,
					HashColumns: []uint32{0, 1},
					Streams: []StreamEndpointSpec{
						{StreamID: 11},
						{StreamID: 12},
					},
				}},
				ProcessorID: 0,
			},
		},
	}

	flows[2] = &FlowSpec{
		Processors: []ProcessorSpec{
			{
				Core: ProcessorCoreUnion{TableReader: &trA},
				Output: []OutputRouterSpec{{
					Type:        OutputRouterSpec_BY_HASH,
					HashColumns: []uint32{0, 1},
					Streams: []StreamEndpointSpec{
						{StreamID: 21},
						{StreamID: 22},
					},
				}},
				ProcessorID: 1,
			},
			{
				Input: []InputSyncSpec{
					{
						Type: InputSyncSpec_PARALLEL_UNORDERED,
						Streams: []StreamEndpointSpec{
							{StreamID: 11},
							{StreamID: 21},
							{StreamID: 31},
						},
					},
					{
						Type: InputSyncSpec_PARALLEL_UNORDERED,
						Streams: []StreamEndpointSpec{
							{StreamID: 41},
							{StreamID: 51},
						},
					},
				},
				Core: ProcessorCoreUnion{HashJoiner: &hj},
				Post: PostProcessSpec{
					Projection:    true,
					OutputColumns: []uint32{0, 1, 2, 3, 4, 5},
				},
				Output: []OutputRouterSpec{{
					Type: OutputRouterSpec_PASS_THROUGH,
					Streams: []StreamEndpointSpec{
						{StreamID: 101},
					},
				}},
				ProcessorID: 2,
			},
			{
				Input: []InputSyncSpec{{
					Type: InputSyncSpec_PARALLEL_UNORDERED,
					Streams: []StreamEndpointSpec{
						{StreamID: 101},
						{StreamID: 102},
					},
				}},
				Core: ProcessorCoreUnion{Noop: &NoopCoreSpec{}},
				Output: []OutputRouterSpec{{
					Type:    OutputRouterSpec_PASS_THROUGH,
					Streams: []StreamEndpointSpec{{Type: StreamEndpointSpec_SYNC_RESPONSE}},
				}},
				ProcessorID: 3,
			},
		},
	}

	flows[3] = &FlowSpec{
		Processors: []ProcessorSpec{
			{
				Core: ProcessorCoreUnion{TableReader: &trA},
				Post: PostProcessSpec{
					Projection:    true,
					OutputColumns: []uint32{0, 1, 3},
				},
				Output: []OutputRouterSpec{{
					Type:        OutputRouterSpec_BY_HASH,
					HashColumns: []uint32{0, 1},
					Streams: []StreamEndpointSpec{
						{StreamID: 31},
						{StreamID: 32},
					},
				}},
				ProcessorID: 4,
			},
			{
				Core: ProcessorCoreUnion{TableReader: &trB},
				Output: []OutputRouterSpec{{
					Type:        OutputRouterSpec_BY_HASH,
					HashColumns: []uint32{2, 1},
					Streams: []StreamEndpointSpec{
						{StreamID: 41},
						{StreamID: 42},
					},
				}},
				ProcessorID: 5,
			},
			{
				Input: []InputSyncSpec{
					{
						Type: InputSyncSpec_PARALLEL_UNORDERED,
						Streams: []StreamEndpointSpec{
							{StreamID: 12},
							{StreamID: 22},
							{StreamID: 32},
						},
					},
					{
						Type: InputSyncSpec_PARALLEL_UNORDERED,
						Streams: []StreamEndpointSpec{
							{StreamID: 42},
							{StreamID: 52},
						},
					},
				},
				Core: ProcessorCoreUnion{HashJoiner: &hj},
				Output: []OutputRouterSpec{{
					Type: OutputRouterSpec_PASS_THROUGH,
					Streams: []StreamEndpointSpec{
						{StreamID: 101},
					},
				}},
				ProcessorID: 6,
			},
		},
	}

	flows[4] = &FlowSpec{
		Processors: []ProcessorSpec{{
			Core: ProcessorCoreUnion{TableReader: &trB},
			Output: []OutputRouterSpec{{
				Type:        OutputRouterSpec_BY_HASH,
				HashColumns: []uint32{2, 1},
				Streams: []StreamEndpointSpec{
					{StreamID: 51},
					{StreamID: 52},
				},
			}},
			ProcessorID: 7,
		}},
	}

	flags := DiagramFlags{
		ShowInputTypes: true,
	}
	diagram, err := GeneratePlanDiagram("SOME SQL HERE", flows, flags)
	if err != nil {
		t.Fatal(err)
	}
	s, diagramURL, err := diagram.ToURL()
	if err != nil {
		t.Fatal(err)
	}

	// Test that deserializing the URL also results in the same plan.
	deserializedDiagram, err := FromURL(diagramURL.String())
	require.NoError(t, err)
	require.Equal(t, diagram, deserializedDiagram)

	expected := `
{
  "sql": "SOME SQL HERE",
  "nodeNames": [
    "1",
    "2",
    "3",
    "4"
  ],
  "processors": [
    {
      "nodeIdx": 0,
      "inputs": [],
      "core": {
        "title": "TableReader/0",
        "details": [
          "TableA@primary",
          "Columns: a, b, d"
        ]
      },
      "outputs": [
        {
          "title": "by hash",
          "details": [
            "@1,@2"
          ]
        }
      ],
      "stage": 0,
      "processorID": 0
    },
    {
      "nodeIdx": 1,
      "inputs": [],
      "core": {
        "title": "TableReader/1",
        "details": [
          "TableA@primary",
          "Columns: a, b, d"
        ]
      },
      "outputs": [
        {
          "title": "by hash",
          "details": [
            "@1,@2"
          ]
        }
      ],
      "stage": 0,
      "processorID": 1
    },
    {
      "nodeIdx": 1,
      "inputs": [
        {
          "title": "unordered",
          "details": []
        },
        {
          "title": "unordered",
          "details": []
        }
      ],
      "core": {
        "title": "HashJoiner/2",
        "details": [
          "left(@1,@3)=right(@3,@2)",
          "ON @1+@2<@6",
          "Out: @1,@2,@3,@4,@5,@6"
        ]
      },
      "outputs": [],
      "stage": 0,
      "processorID": 2
    },
    {
      "nodeIdx": 1,
      "inputs": [
        {
          "title": "unordered",
          "details": []
        }
      ],
      "core": {
        "title": "No-op/3",
        "details": []
      },
      "outputs": [],
      "stage": 0,
      "processorID": 3
    },
    {
      "nodeIdx": 2,
      "inputs": [],
      "core": {
        "title": "TableReader/4",
        "details": [
          "TableA@primary",
          "Columns: a, b, d",
          "Out: @1,@2,@4"
        ]
      },
      "outputs": [
        {
          "title": "by hash",
          "details": [
            "@1,@2"
          ]
        }
      ],
      "stage": 0,
      "processorID": 4
    },
    {
      "nodeIdx": 2,
      "inputs": [],
      "core": {
        "title": "TableReader/5",
        "details": [
          "TableB@primary",
          "Columns: b, c, e"
        ]
      },
      "outputs": [
        {
          "title": "by hash",
          "details": [
            "@3,@2"
          ]
        }
      ],
      "stage": 0,
      "processorID": 5
    },
    {
      "nodeIdx": 2,
      "inputs": [
        {
          "title": "unordered",
          "details": []
        },
        {
          "title": "unordered",
          "details": []
        }
      ],
      "core": {
        "title": "HashJoiner/6",
        "details": [
          "left(@1,@3)=right(@3,@2)",
          "ON @1+@2<@6"
        ]
      },
      "outputs": [],
      "stage": 0,
      "processorID": 6
    },
    {
      "nodeIdx": 3,
      "inputs": [],
      "core": {
        "title": "TableReader/7",
        "details": [
          "TableB@primary",
          "Columns: b, c, e"
        ]
      },
      "outputs": [
        {
          "title": "by hash",
          "details": [
            "@3,@2"
          ]
        }
      ],
      "stage": 0,
      "processorID": 7
    },
    {
      "nodeIdx": 1,
      "inputs": [],
      "core": {
        "title": "Response",
        "details": []
      },
      "outputs": [],
      "stage": 0,
      "processorID": -1
    }
  ],
  "edges": [
    {
      "sourceProc": 0,
      "sourceOutput": 1,
      "destProc": 2,
      "destInput": 1,
      "streamID": 11
    },
    {
      "sourceProc": 0,
      "sourceOutput": 1,
      "destProc": 6,
      "destInput": 1,
      "streamID": 12
    },
    {
      "sourceProc": 1,
      "sourceOutput": 1,
      "destProc": 2,
      "destInput": 1,
      "streamID": 21
    },
    {
      "sourceProc": 1,
      "sourceOutput": 1,
      "destProc": 6,
      "destInput": 1,
      "streamID": 22
    },
    {
      "sourceProc": 2,
      "sourceOutput": 0,
      "destProc": 3,
      "destInput": 1,
      "streamID": 101
    },
    {
      "sourceProc": 3,
      "sourceOutput": 0,
      "destProc": 8,
      "destInput": 0,
      "streamID": 0
    },
    {
      "sourceProc": 4,
      "sourceOutput": 1,
      "destProc": 2,
      "destInput": 1,
      "streamID": 31
    },
    {
      "sourceProc": 4,
      "sourceOutput": 1,
      "destProc": 6,
      "destInput": 1,
      "streamID": 32
    },
    {
      "sourceProc": 5,
      "sourceOutput": 1,
      "destProc": 2,
      "destInput": 2,
      "streamID": 41
    },
    {
      "sourceProc": 5,
      "sourceOutput": 1,
      "destProc": 6,
      "destInput": 2,
      "streamID": 42
    },
    {
      "sourceProc": 6,
      "sourceOutput": 0,
      "destProc": 3,
      "destInput": 1,
      "streamID": 101
    },
    {
      "sourceProc": 7,
      "sourceOutput": 1,
      "destProc": 2,
      "destInput": 2,
      "streamID": 51
    },
    {
      "sourceProc": 7,
      "sourceOutput": 1,
      "destProc": 6,
      "destInput": 2,
      "streamID": 52
    }
  ],
  "flow_id": "00000000-0000-0000-0000-000000000000",
  "flags": {
    "ShowInputTypes": true,
    "MakeDeterministic": false
  }
}
	`

	compareDiagrams(t, s, expected)
}

func TestPlanDiagramReplicationStream(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ingestionData := StreamIngestionDataSpec{
		PartitionSpecs: map[string]StreamIngestionPartitionSpec{
			"1": {
				Spans: []roachpb.Span{{
					Key:    roachpb.Key("a"),
					EndKey: roachpb.Key("b"),
				}},
			},
			"2": {
				Spans: []roachpb.Span{{
					Key:    roachpb.Key("d"),
					EndKey: roachpb.Key("e"),
				}},
			},
			"3": {
				Spans: []roachpb.Span{
					{
						Key:    roachpb.Key("f1"),
						EndKey: roachpb.Key("g1"),
					},
					{
						Key:    roachpb.Key("f2"),
						EndKey: roachpb.Key("g2"),
					},
					{
						Key:    roachpb.Key("f3"),
						EndKey: roachpb.Key("g3"),
					},
					{
						Key:    roachpb.Key("f4"),
						EndKey: roachpb.Key("g4"),
					},
				},
			},
			"4": {
				Spans: []roachpb.Span{{
					Key:    roachpb.Key("g"),
					EndKey: roachpb.Key("h"),
				}},
			},
		},
	}
	ingestionFrontier := StreamIngestionFrontierSpec{
		StreamID: 42,
	}

	flows := map[base.SQLInstanceID]*FlowSpec{
		1: {
			Processors: []ProcessorSpec{
				{
					Core: ProcessorCoreUnion{StreamIngestionData: &ingestionData},
					Output: []OutputRouterSpec{{
						Type: OutputRouterSpec_PASS_THROUGH,
						Streams: []StreamEndpointSpec{
							{StreamID: 11},
						},
					}},
					ProcessorID: 1,
				},
				{
					Input: []InputSyncSpec{
						{
							Type: InputSyncSpec_PARALLEL_UNORDERED,
							Streams: []StreamEndpointSpec{
								{StreamID: 11},
							},
						},
					},
					Core:        ProcessorCoreUnion{StreamIngestionFrontier: &ingestionFrontier},
					ProcessorID: 2,
				},
			},
		}}

	flags := DiagramFlags{
		ShowInputTypes: true,
	}
	diagram, err := GeneratePlanDiagram("SOME SQL HERE", flows, flags)
	require.NoError(t, err)

	s, diagramURL, err := diagram.ToURL()
	t.Logf("diag URL: %s", diagramURL.String())
	require.NoError(t, err)
	expected := `
{
  "sql": "SOME SQL HERE",
  "nodeNames": [
    "1"
  ],
  "processors": [
    {
      "nodeIdx": 0,
      "inputs": [],
      "core": {
        "title": "StreamIngestionData/1",
        "details": [
          "Partitions:",
          "Source node 1, spans:",
          "{a-b}",
          "Source node 2, spans:",
          "{d-e}",
          "Source node 3, spans:",
          "{f1-g1}",
          "{f2-g2}",
          "{f3-g3}",
          "and 1 more spans",
          "and 1 more partitions"
        ]
      },
      "outputs": [],
      "stage": 0,
      "processorID": 1
    },
    {
      "nodeIdx": 0,
      "inputs": [],
      "core": {
        "title": "StreamIngestionFrontier/2",
        "details": [
          "streamID: 42"
        ]
      },
      "outputs": [],
      "stage": 0,
      "processorID": 2
    }
  ],
  "edges": [
    {
      "sourceProc": 0,
      "sourceOutput": 0,
      "destProc": 1,
      "destInput": 0,
      "streamID": 11
    }
  ],
  "flow_id": "00000000-0000-0000-0000-000000000000",
  "flags": {
    "ShowInputTypes": true,
    "MakeDeterministic": false
  }
}`
	compareDiagrams(t, s, expected)
}

func TestProcessorsImplementDiagramCellType(t *testing.T) {
	pcu := reflect.ValueOf(ProcessorCoreUnion{})
	for i := 0; i < pcu.NumField(); i++ {
		require.Implements(t, (*diagramCellType)(nil), pcu.Field(i).Interface())
	}
}

func TestChangeAggregatorSpec(t *testing.T) {
	defer leaktest.AfterTest(t)()

	testData := []struct {
		name            string
		aggregatorSpec  ChangeAggregatorSpec
		expectedWatches []string
	}{
		{
			name:            "no watches",
			aggregatorSpec:  ChangeAggregatorSpec{},
			expectedWatches: []string{""},
		},
		{
			name: "limit",
			aggregatorSpec: ChangeAggregatorSpec{
				Watches: []ChangeAggregatorSpec_Watch{
					{
						Span: roachpb.Span{Key: roachpb.Key("a")},
					},
					{
						Span: roachpb.Span{Key: roachpb.Key("b")},
					},
					{
						Span: roachpb.Span{Key: roachpb.Key("c")},
					},
				},
			},
			expectedWatches: []string{"Watches [3]: a, b, c"},
		},
		{
			name: "overlimit",
			aggregatorSpec: ChangeAggregatorSpec{
				Watches: []ChangeAggregatorSpec_Watch{
					{
						Span: roachpb.Span{Key: roachpb.Key("a")},
					},
					{
						Span: roachpb.Span{Key: roachpb.Key("b")},
					},
					{
						Span: roachpb.Span{Key: roachpb.Key("c")},
					},
					{
						Span: roachpb.Span{Key: roachpb.Key("d")},
					},
				},
			},
			expectedWatches: []string{"Watches [4]: a, b, c..."},
		},
	}

	for _, td := range testData {
		t.Run(td.name, func(t *testing.T) {
			_, details := td.aggregatorSpec.summary()
			require.Equal(t, td.expectedWatches, details)
		})
	}
}
