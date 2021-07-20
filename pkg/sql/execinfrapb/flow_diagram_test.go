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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// compareDiagrams verifies that two JSON strings decode to equal diagramData
// structures. This allows the expected string to be formatted differently.
func compareDiagrams(t *testing.T, result string, expected string) {
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

	flows := make(map[roachpb.NodeID]*FlowSpec)

	desc := &descpb.TableDescriptor{
		Name:    "Table",
		Indexes: []descpb.IndexDescriptor{{Name: "SomeIndex"}},
	}
	tr := TableReaderSpec{
		Table:    *desc,
		IndexIdx: 1,
	}

	flows[1] = &FlowSpec{
		Processors: []ProcessorSpec{{
			Core: ProcessorCoreUnion{TableReader: &tr},
			Post: PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
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
			Post: PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{0, 1},
			},
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
				Post: PostProcessSpec{
					Projection:    true,
					OutputColumns: []uint32{0, 1},
				},
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
				Core: ProcessorCoreUnion{JoinReader: &JoinReaderSpec{Table: *desc}},
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
			  {"nodeIdx":0,"inputs":[],"core":{"title":"TableReader/0","details":["Table@SomeIndex","Out: @1,@2"]},"outputs":[],"stage":1},
				{"nodeIdx":1,"inputs":[],"core":{"title":"TableReader/1","details":["Table@SomeIndex","Out: @1,@2"]},"outputs":[],"stage":1},
				{"nodeIdx":2,"inputs":[],"core":{"title":"TableReader/2","details":["Table@SomeIndex","Out: @1,@2"]},"outputs":[],"stage":1},
				{"nodeIdx":2,"inputs":[{"title":"ordered","details":["@2+"]}],"core":{"title":"JoinReader/3","details":["Table@primary","Out: @3"]},"outputs":[],"stage":2},
		    {"nodeIdx":2,"inputs":[],"core":{"title":"Response","details":[]},"outputs":[]}
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

	expectedURL := "https://cockroachdb.github.io/distsqlplan/decode.html#eJy0ksFq8zAQhO__U4S5_oJack86-RJoStu0cW_FB9VagsGWXEmGFON3L5ZLE0NSUtIed8cz33hRD_9WQyJf3y8X-dPd4ma5WYLBWE0PqiEP-QIOBgGGFAVD62xJ3ls3Sn38cKV3kAlDZdoujOuCobSOIHuEKtQEiWf1WtOGlCZ3lYBBU1BVHeOjlOW2oZXRtAPDugtykXGWCRQDg-3CPtgHtSVIPrADOD8fzn8dLs6Hiz-F75nWaXKk57RM_EcxHGl4ayvzWTA9VrB1VaPc-1e99GQ38ZPDbMi31niaIU8lJ2Nx0luaftTbzpX06GwZX940rqMvLjT5MKnpNKxMlOLxDs38ErO4xJx-a76emZOhGP59BAAA___j2TIH"
	if url.String() != expectedURL {
		t.Errorf("expected `%s` got `%s`", expectedURL, url.String())
	}
}

func TestPlanDiagramJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()

	flows := make(map[roachpb.NodeID]*FlowSpec)

	descA := &descpb.TableDescriptor{Name: "TableA"}
	descB := &descpb.TableDescriptor{Name: "TableB"}

	trA := TableReaderSpec{Table: *descA}

	trB := TableReaderSpec{Table: *descB}

	hj := HashJoinerSpec{
		LeftEqColumns:  []uint32{0, 2},
		RightEqColumns: []uint32{2, 1},
		OnExpr:         Expression{Expr: "@1+@2<@6"},
	}

	flows[1] = &FlowSpec{
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
				Post: PostProcessSpec{
					Projection:    true,
					OutputColumns: []uint32{0, 1, 3},
				},
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
				Post: PostProcessSpec{
					Projection:    true,
					OutputColumns: []uint32{1, 2, 4},
				},
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
			Post: PostProcessSpec{
				Projection:    true,
				OutputColumns: []uint32{1, 2, 4},
			},
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
				{"nodeIdx":0,"inputs":[],"core":{"title":"TableReader/0","details":["TableA@primary","Out: @1,@2,@4"]},"outputs":[{"title":"by hash","details":["@1,@2"]}],"stage":0},
				{"nodeIdx":1,"inputs":[],"core":{"title":"TableReader/1","details":["TableA@primary","Out: @1,@2,@4"]},"outputs":[{"title":"by hash","details":["@1,@2"]}],"stage":0},
				{"nodeIdx":1,"inputs":[{"title":"unordered","details":[]},{"title":"unordered","details":[]}],"core":{"title":"HashJoiner/2","details":["left(@1,@3)=right(@3,@2)","ON @1+@2\u003c@6","Out: @1,@2,@3,@4,@5,@6"]},"outputs":[],"stage":0},
				{"nodeIdx":1,"inputs":[{"title":"unordered","details":[]}],"core":{"title":"No-op/3","details":[]},"outputs":[],"stage":0},
				{"nodeIdx":2,"inputs":[],"core":{"title":"TableReader/4","details":["TableA@primary","Out: @1,@2,@4"]},"outputs":[{"title":"by hash","details":["@1,@2"]}],"stage":0},
				{"nodeIdx":2,"inputs":[],"core":{"title":"TableReader/5","details":["TableB@primary","Out: @2,@3,@5"]},"outputs":[{"title":"by hash","details":["@3,@2"]}],"stage":0},
				{"nodeIdx":2,"inputs":[{"title":"unordered","details":[]},{"title":"unordered","details":[]}],"core":{"title":"HashJoiner/6","details":["left(@1,@3)=right(@3,@2)","ON @1+@2\u003c@6"]},"outputs":[],"stage":0},
				{"nodeIdx":3,"inputs":[],"core":{"title":"TableReader/7","details":["TableB@primary","Out: @2,@3,@5"]},"outputs":[{"title":"by hash","details":["@3,@2"]}],"stage":0},
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
