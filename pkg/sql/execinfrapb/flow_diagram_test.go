// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package execinfrapb

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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
		FetchSpec: descpb.IndexFetchSpec{
			TableName: "Table",
			IndexName: "SomeIndex",
			FetchedColumns: []descpb.IndexFetchSpec_Column{
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
					FetchSpec: descpb.IndexFetchSpec{
						TableName: "Table",
						IndexName: "primary",
						FetchedColumns: []descpb.IndexFetchSpec_Column{
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

	expected := `
		{
		  "sql":"SOME SQL HERE",
		  "nodeNames":["1","2","3"],
		  "processors":[
        {"nodeIdx":0,"inputs":[],"core":{"title":"TableReader/0","details":["Table@SomeIndex","Columns: a, b"]},"outputs":[],"stage":1},
				{"nodeIdx":1,"inputs":[],"core":{"title":"TableReader/1","details":["Table@SomeIndex","Columns: a, b"]},"outputs":[],"stage":1},
				{"nodeIdx":2,"inputs":[],"core":{"title":"TableReader/2","details":["Table@SomeIndex","Columns: a, b"]},"outputs":[],"stage":1},
				{"nodeIdx":2,"inputs":[{"title":"ordered","details":["@2+"]}],"core":{"title":"JoinReader/3","details":["Table@primary","Columns: x, y","Out: @3"]},"outputs":[],"stage":2},
				{"nodeIdx":2,"inputs":[],"core":{"title":"Response","details":[]},"outputs":[],"stage":0}
		  ],
		  "edges":[
		    {"sourceProc":0,"sourceOutput":0,"destProc":3,"destInput":1},
		    {"sourceProc":1,"sourceOutput":0,"destProc":3,"destInput":1},
		    {"sourceProc":2,"sourceOutput":0,"destProc":3,"destInput":1},
		    {"sourceProc":3,"sourceOutput":0,"destProc":4,"destInput":0}
		  ]
	  }
	`

	compareDiagrams(t, json, expected)

	expectedURL := "https://cockroachdb.github.io/distsqlplan/decode.html#eJy0kkFLw0AQhe_-ivCuLphsPO2pIAUrarX1Jjlss0MJJDtxdwOVkv8u2QhtoZVK6XHm7ZvvzbBb-K8aCsv5yzRZvj8nj9PFFAKWDb3qhjzUJzIISAjkKARaxyV5z26QtvHhzGygUoHKtl0Y2oVAyY6gtghVqAkKH3pV04K0IXeXQsBQ0FUdx0dpsuSGZtbQBgIPXHeN9SrRIlmh6AW4C7vZPug1QWW92ONn5_Oza_Dl-Xx5bf4Oy86QI3MInMhbFP2RkE9c2d-M-bGMrasa7b73E25EMtTzLqhkkp_MKv9zqwX5lq2ngwinJqfDImTWNC7uuXMlvTku45ccy3n0xYYhH0Y1H4uZjVI85r45u8QsLzHnf5rvD8xpX_Q3PwEAAP__4nU76g=="
	if url.String() != expectedURL {
		t.Errorf("expected `%s` got `%s`", expectedURL, url.String())
	}
}

func TestPlanDiagramJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()

	flows := make(map[base.SQLInstanceID]*FlowSpec)

	trA := TableReaderSpec{
		FetchSpec: descpb.IndexFetchSpec{
			TableName: "TableA",
			IndexName: "primary",
			FetchedColumns: []descpb.IndexFetchSpec_Column{
				{Name: "a"},
				{Name: "b"},
				{Name: "d"},
			},
		},
	}

	trB := TableReaderSpec{
		FetchSpec: descpb.IndexFetchSpec{
			TableName: "TableB",
			IndexName: "primary",
			FetchedColumns: []descpb.IndexFetchSpec_Column{
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
	s, _, err := diagram.ToURL()
	if err != nil {
		t.Fatal(err)
	}

	expected := `
		{
		  "sql":"SOME SQL HERE",
		  "nodeNames":["1","2","3","4"],
			"processors":[
			  {"nodeIdx":0,"inputs":[],"core":{"title":"TableReader/0","details":["TableA@primary","Columns: a, b, d"]},"outputs":[{"title":"by hash","details":["@1,@2"]}],"stage":0},
				{"nodeIdx":1,"inputs":[],"core":{"title":"TableReader/1","details":["TableA@primary","Columns: a, b, d"]},"outputs":[{"title":"by hash","details":["@1,@2"]}],"stage":0},
				{"nodeIdx":1,"inputs":[{"title":"unordered","details":[]},{"title":"unordered","details":[]}],"core":{"title":"HashJoiner/2","details":["left(@1,@3)=right(@3,@2)","ON @1+@2\u003c@6","Out: @1,@2,@3,@4,@5,@6"]},"outputs":[],"stage":0},
				{"nodeIdx":1,"inputs":[{"title":"unordered","details":[]}],"core":{"title":"No-op/3","details":[]},"outputs":[],"stage":0},
				{"nodeIdx":2,"inputs":[],"core":{"title":"TableReader/4","details":["TableA@primary","Columns: a, b, d","Out: @1,@2,@4"]},"outputs":[{"title":"by hash","details":["@1,@2"]}],"stage":0},
				{"nodeIdx":2,"inputs":[],"core":{"title":"TableReader/5","details":["TableB@primary","Columns: b, c, e"]},"outputs":[{"title":"by hash","details":["@3,@2"]}],"stage":0},
				{"nodeIdx":2,"inputs":[{"title":"unordered","details":[]},{"title":"unordered","details":[]}],"core":{"title":"HashJoiner/6","details":["left(@1,@3)=right(@3,@2)","ON @1+@2\u003c@6"]},"outputs":[],"stage":0},
				{"nodeIdx":3,"inputs":[],"core":{"title":"TableReader/7","details":["TableB@primary","Columns: b, c, e"]},"outputs":[{"title":"by hash","details":["@3,@2"]}],"stage":0},
				{"nodeIdx":1,"inputs":[],"core":{"title":"Response","details":[]},"outputs":[],"stage":0}
			],
			"edges":[
			  {"sourceProc":0,"sourceOutput":1,"destProc":2,"destInput":1},
				{"sourceProc":0,"sourceOutput":1,"destProc":6,"destInput":1},
				{"sourceProc":1,"sourceOutput":1,"destProc":2,"destInput":1},
				{"sourceProc":1,"sourceOutput":1,"destProc":6,"destInput":1},
				{"sourceProc":2,"sourceOutput":0,"destProc":3,"destInput":1},
				{"sourceProc":3,"sourceOutput":0,"destProc":8,"destInput":0},
				{"sourceProc":4,"sourceOutput":1,"destProc":2,"destInput":1},
				{"sourceProc":4,"sourceOutput":1,"destProc":6,"destInput":1},
				{"sourceProc":5,"sourceOutput":1,"destProc":2,"destInput":2},
				{"sourceProc":5,"sourceOutput":1,"destProc":6,"destInput":2},
				{"sourceProc":6,"sourceOutput":0,"destProc":3,"destInput":1},
				{"sourceProc":7,"sourceOutput":1,"destProc":2,"destInput":2},
				{"sourceProc":7,"sourceOutput":1,"destProc":6,"destInput":2}
			]
	  }
	`

	compareDiagrams(t, s, expected)
}

func TestProcessorsImplementDiagramCellType(t *testing.T) {
	pcu := reflect.ValueOf(ProcessorCoreUnion{})
	for i := 0; i < pcu.NumField(); i++ {
		require.Implements(t, (*diagramCellType)(nil), pcu.Field(i).Interface())
	}
}
