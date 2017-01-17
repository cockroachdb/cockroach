// Copyright 2016 The Cockroach Authors.
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
//
// Author: Radu Berinde (radu@cockroachlabs.com)

package distsqlrun

import (
	"bytes"
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

// compareDiagrams verifies that two JSON strings decode to equal diagramData
// structures. This allows the expected string to be formatted differently.
func compareDiagrams(t *testing.T, result string, expected string) {
	dec := json.NewDecoder(strings.NewReader(result))
	var resData, expData diagramData
	if err := dec.Decode(&resData); err != nil {
		t.Fatal(err)
	}
	dec = json.NewDecoder(strings.NewReader(expected))
	if err := dec.Decode(&expData); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(resData, expData) {
		t.Errorf("\ngot:\n%s\nwant:\n%s", result, expected)
	}
}

func TestPlanDiagramIndexJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()

	desc := &sqlbase.TableDescriptor{
		Name:    "Table",
		Indexes: []sqlbase.IndexDescriptor{{Name: "SomeIndex"}},
	}
	tr := TableReaderSpec{
		Table:         *desc,
		IndexIdx:      1,
		OutputColumns: []uint32{0, 1},
	}

	jr := JoinReaderSpec{
		Table:         *desc,
		OutputColumns: []uint32{2},
		Filter:        Expression{Expr: "@1+@2<@3"},
	}

	f1 := FlowSpec{
		Processors: []ProcessorSpec{{
			Core: ProcessorCoreUnion{TableReader: &tr},
			Output: []OutputRouterSpec{{
				Type: OutputRouterSpec_PASS_THROUGH,
				Streams: []StreamEndpointSpec{
					{StreamID: 0},
				},
			}},
		}},
	}

	f2 := FlowSpec{
		Processors: []ProcessorSpec{{
			Core: ProcessorCoreUnion{TableReader: &tr},
			Output: []OutputRouterSpec{{
				Type: OutputRouterSpec_PASS_THROUGH,
				Streams: []StreamEndpointSpec{
					{StreamID: 1},
				},
			}},
		}},
	}

	f3 := FlowSpec{
		Processors: []ProcessorSpec{
			{
				Core: ProcessorCoreUnion{TableReader: &tr},
				Output: []OutputRouterSpec{{
					Type: OutputRouterSpec_PASS_THROUGH,
					Streams: []StreamEndpointSpec{
						{StreamID: 2},
					},
				}},
			},
			{
				Input: []InputSyncSpec{{
					Type:     InputSyncSpec_ORDERED,
					Ordering: Ordering{Columns: []Ordering_Column{{1, Ordering_Column_ASC}}},
					Streams: []StreamEndpointSpec{
						{StreamID: 0},
						{StreamID: 1},
						{StreamID: 2},
					},
				}},
				Core: ProcessorCoreUnion{JoinReader: &jr},
				Output: []OutputRouterSpec{{
					Type:    OutputRouterSpec_PASS_THROUGH,
					Streams: []StreamEndpointSpec{{Type: StreamEndpointSpec_SYNC_RESPONSE}},
				}}},
		},
	}

	var buf bytes.Buffer
	if err := GeneratePlanDiagram(
		[]FlowSpec{f1, f2, f3}, []string{"1", "2", "3"}, &buf,
	); err != nil {
		t.Fatal(err)
	}

	expected := `
		{"nodeNames":["1","2","3"],
			"processors":[
				{"nodeIdx":0,"inputs":[],"core":{"title":"TableReader","details":["SomeIndex@Table","@1,@2"]},"outputs":[]},
				{"nodeIdx":1,"inputs":[],"core":{"title":"TableReader","details":["SomeIndex@Table","@1,@2"]},"outputs":[]},
				{"nodeIdx":2,"inputs":[],"core":{"title":"TableReader","details":["SomeIndex@Table","@1,@2"]},"outputs":[]},
				{"nodeIdx":2,"inputs":[{"title":"ordered","details":["@2+"]}],"core":{"title":"JoinReader","details":["primary@Table","@3","@1+@2\u003c@3"]},"outputs":[]},
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

	compareDiagrams(t, buf.String(), expected)
}

func TestPlanDiagramJoin(t *testing.T) {
	defer leaktest.AfterTest(t)()

	descA := &sqlbase.TableDescriptor{Name: "TableA"}
	descB := &sqlbase.TableDescriptor{Name: "TableB"}

	trA := TableReaderSpec{
		Table:         *descA,
		OutputColumns: []uint32{0, 1, 3},
	}

	trB := TableReaderSpec{
		Table:         *descB,
		OutputColumns: []uint32{1, 2, 4},
	}

	hj := HashJoinerSpec{
		LeftEqColumns:  []uint32{0, 2},
		RightEqColumns: []uint32{2, 1},
		OutputColumns:  []uint32{0, 1, 2, 3, 4, 5},
		OnExpr:         Expression{Expr: "@1+@2<@6"},
	}

	f1 := FlowSpec{
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
			},
		},
	}

	f2 := FlowSpec{
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
				Output: []OutputRouterSpec{{
					Type: OutputRouterSpec_PASS_THROUGH,
					Streams: []StreamEndpointSpec{
						{StreamID: 101},
					},
				}},
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
				}}},
		},
	}

	f3 := FlowSpec{
		Processors: []ProcessorSpec{
			{
				Core: ProcessorCoreUnion{TableReader: &trA},
				Output: []OutputRouterSpec{{
					Type:        OutputRouterSpec_BY_HASH,
					HashColumns: []uint32{0, 1},
					Streams: []StreamEndpointSpec{
						{StreamID: 31},
						{StreamID: 32},
					},
				}},
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
			},
		},
	}

	f4 := FlowSpec{
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
		}},
	}

	var buf bytes.Buffer
	if err := GeneratePlanDiagram(
		[]FlowSpec{f1, f2, f3, f4}, []string{"1", "2", "3", "4"}, &buf,
	); err != nil {
		t.Fatal(err)
	}

	expected := `
		{
			"nodeNames":["1","2","3","4"],
			"processors":[
				{"nodeIdx":0,"inputs":[],"core":{"title":"TableReader","details":["primary@TableA","@1,@2,@4"]},"outputs":[{"title":"by hash","details":["@1,@2"]}]},
				{"nodeIdx":1,"inputs":[],"core":{"title":"TableReader","details":["primary@TableA","@1,@2,@4"]},"outputs":[{"title":"by hash","details":["@1,@2"]}]},
				{"nodeIdx":1,"inputs":[{"title":"unordered","details":[]},{"title":"unordered","details":[]}],"core":{"title":"HashJoiner","details":["ON left(@1,@3)=right(@3,@2)","@1,@2,@3,@4,@5,@6","@1+@2\u003c@6"]},"outputs":[]},
				{"nodeIdx":1,"inputs":[{"title":"unordered","details":[]}],"core":{"title":"No-op","details":[]},"outputs":[]},
				{"nodeIdx":2,"inputs":[],"core":{"title":"TableReader","details":["primary@TableA","@1,@2,@4"]},"outputs":[{"title":"by hash","details":["@1,@2"]}]},
				{"nodeIdx":2,"inputs":[],"core":{"title":"TableReader","details":["primary@TableB","@2,@3,@5"]},"outputs":[{"title":"by hash","details":["@3,@2"]}]},
				{"nodeIdx":2,"inputs":[{"title":"unordered","details":[]},{"title":"unordered","details":[]}],"core":{"title":"HashJoiner","details":["ON left(@1,@3)=right(@3,@2)","@1,@2,@3,@4,@5,@6","@1+@2\u003c@6"]},"outputs":[]},
				{"nodeIdx":3,"inputs":[],"core":{"title":"TableReader","details":["primary@TableB","@2,@3,@5"]},"outputs":[{"title":"by hash","details":["@3,@2"]}]},
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

	compareDiagrams(t, buf.String(), expected)
}
