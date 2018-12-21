// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package distsqlpb

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
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

	desc := &sqlbase.TableDescriptor{
		Name:    "Table",
		Indexes: []sqlbase.IndexDescriptor{{Name: "SomeIndex"}},
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
					Filter:        Expression{Expr: "@1+@2<@3"},
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

	json, url, err := GeneratePlanDiagramURL(flows)
	if err != nil {
		t.Fatal(err)
	}

	expected := `
		{
		  "nodeNames":["1","2","3"],
		  "processors":[
			  {"nodeIdx":0,"inputs":[],"core":{"title":"TableReader/0","details":["SomeIndex@Table","Out: @1,@2"]},"outputs":[],"stage":1},
				{"nodeIdx":1,"inputs":[],"core":{"title":"TableReader/1","details":["SomeIndex@Table","Out: @1,@2"]},"outputs":[],"stage":1},
				{"nodeIdx":2,"inputs":[],"core":{"title":"TableReader/2","details":["SomeIndex@Table","Out: @1,@2"]},"outputs":[],"stage":1},
				{"nodeIdx":2,"inputs":[{"title":"ordered","details":["@2+"]}],"core":{"title":"JoinReader/3","details":["primary@Table","Filter: @1+@2\u003c@3","Out: @3"]},"outputs":[],"stage":2},
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

	expectedURL := "https://cockroachdb.github.io/distsqlplan/decode.html#eJy0kjFr8zAQhvfvV3y8awS1pE6aNBXSoSlpt9aDah1B4EhGkiEl-L8XyyWJoSkpaUfd3XvP48N7-GDpwWwpQb2Ag0GAQaJm6GJoKKUQx9Y0uLQ7qIrB-a7PY7lmaEIkqD2yyy1B4dm8tbQmYyneVGCwlI1ry_qnsKWlt7TTZQgMqz6r_5ozLVAPDKHPx8Upmw1B8YGdwPnlcP7rcHE5XPwp_MgM0VIkO6dpsUA9fGF4H5z_FJTzSBfd1sT3g96dazPF0XChxWtfVbLR8qAtzzqLnxxsTakLPtFM5dzmavwgshuaDpBCHxt6jKEpf-T0XJVcKVhKeerK6bH0pVWOehrm14TFNWH5bfh2Fq6Gevj3EQAA__8R3DK5"
	if url.String() != expectedURL {
		t.Errorf("expected `%s` got `%s`", expectedURL, &url)
	}
}

func TestPlanDiagramJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()

	flows := make(map[roachpb.NodeID]*FlowSpec)

	descA := &sqlbase.TableDescriptor{Name: "TableA"}
	descB := &sqlbase.TableDescriptor{Name: "TableB"}

	trA := TableReaderSpec{Table: *descA}

	trB := TableReaderSpec{Table: *descB}

	hj := HashJoinerSpec{
		LeftEqColumns:  []uint32{0, 2},
		RightEqColumns: []uint32{2, 1},
		OnExpr:         Expression{Expr: "@1+@2<@6"},
		MergedColumns:  true,
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
						Type: InputSyncSpec_UNORDERED,
						Streams: []StreamEndpointSpec{
							{StreamID: 11},
							{StreamID: 21},
							{StreamID: 31},
						},
					},
					{
						Type: InputSyncSpec_UNORDERED,
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
					Type: InputSyncSpec_UNORDERED,
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
						Type: InputSyncSpec_UNORDERED,
						Streams: []StreamEndpointSpec{
							{StreamID: 12},
							{StreamID: 22},
							{StreamID: 32},
						},
					},
					{
						Type: InputSyncSpec_UNORDERED,
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

	diagram, err := GeneratePlanDiagram(flows)
	if err != nil {
		t.Fatal(err)
	}
	s, _, err := diagram.ToURL()
	if err != nil {
		t.Fatal(err)
	}

	expected := `
		{
			"nodeNames":["1","2","3","4"],
			"processors":[
				{"nodeIdx":0,"inputs":[],"core":{"title":"TableReader/0","details":["primary@TableA","Out: @1,@2,@4"]},"outputs":[{"title":"by hash","details":["@1,@2"]}]},
				{"nodeIdx":1,"inputs":[],"core":{"title":"TableReader/1","details":["primary@TableA","Out: @1,@2,@4"]},"outputs":[{"title":"by hash","details":["@1,@2"]}]},
				{"nodeIdx":1,"inputs":[{"title":"unordered","details":[]},{"title":"unordered","details":[]}],"core":{"title":"HashJoiner/2","details":["left(@1,@3)=right(@3,@2)","ON @1+@2\u003c@6","Merged columns: 2","Out: @1,@2,@3,@4,@5,@6"]},"outputs":[]},
				{"nodeIdx":1,"inputs":[{"title":"unordered","details":[]}],"core":{"title":"No-op/3","details":[]},"outputs":[]},
				{"nodeIdx":2,"inputs":[],"core":{"title":"TableReader/4","details":["primary@TableA","Out: @1,@2,@4"]},"outputs":[{"title":"by hash","details":["@1,@2"]}]},
				{"nodeIdx":2,"inputs":[],"core":{"title":"TableReader/5","details":["primary@TableB","Out: @2,@3,@5"]},"outputs":[{"title":"by hash","details":["@3,@2"]}]},
				{"nodeIdx":2,"inputs":[{"title":"unordered","details":[]},{"title":"unordered","details":[]}],"core":{"title":"HashJoiner/6","details":["left(@1,@3)=right(@3,@2)","ON @1+@2\u003c@6","Merged columns: 2"]},"outputs":[]},
				{"nodeIdx":3,"inputs":[],"core":{"title":"TableReader/7","details":["primary@TableB","Out: @2,@3,@5"]},"outputs":[{"title":"by hash","details":["@3,@2"]}]},
				{"nodeIdx":1,"inputs":[],"core":{"title":"Response","details":[]},"outputs":[]}
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
