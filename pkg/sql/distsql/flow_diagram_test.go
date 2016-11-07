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

package distsql

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestProcessorHTML(t *testing.T) {
	defer leaktest.AfterTest(t)()

	p := procDiagramInfo{
		Inputs: []cellContents{
			{},
			{Title: "ordered", ExtraLines: []string{"1+ 2-"}, Port: "in0"},
			{},
			{Title: "unordered", Port: "in1"},
			{},
		},
		Core: cellContents{
			Title:      "MergeJoin",
			ExtraLines: []string{"on 0,2", "filter: $1+$2=$3"},
		},
		NumCols: 5,
		Outputs: []cellContents{
			{},
			{},
			{Title: "mirror", Port: "out0"},
			{},
			{},
		},
	}

	var buf bytes.Buffer
	if err := p.genHTML(&buf); err != nil {
		fmt.Printf("Error: %s\n", err)
	}
	result := buf.String()
	expected := `
<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0" CELLPADDING="4">
<TR>
<TD WIDTH="20"></TD>

  <TD BORDER="1" BGCOLOR="YELLOW" port="in0"><FONT POINT-SIZE="10">ordered</FONT><BR/><FONT POINT-SIZE="9">1&#43; 2-</FONT></TD>
  <TD WIDTH="20"></TD>

  <TD BORDER="1" BGCOLOR="YELLOW" port="in1"><FONT POINT-SIZE="10">unordered</FONT></TD>
  <TD WIDTH="20"></TD>

</TR>
<TR>
  <TD BORDER="1" COLSPAN="5" BGCOLOR="SKYBLUE">MergeJoin<BR/><FONT POINT-SIZE="10">on 0,2</FONT><BR/><FONT POINT-SIZE="10">filter: $1&#43;$2=$3</FONT></TD>
</TR>
<TR>
<TD WIDTH="20"></TD>
<TD WIDTH="20"></TD>

  <TD BORDER="1" BGCOLOR="RED" port="out0"><FONT POINT-SIZE="10">mirror</FONT></TD>
  <TD WIDTH="20"></TD>
<TD WIDTH="20"></TD>

</TR>
</TABLE>
`

	if result != expected {
		t.Errorf("\ngot:\n%s\nwant:\n%s", result, expected)
	}
}

func TestPlanDiagram(t *testing.T) {
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
		Filter:        Expression{Expr: "$0+$1<$2"},
	}

	f1 := FlowSpec{
		Processors: []ProcessorSpec{{
			Core: ProcessorCoreUnion{TableReader: &tr},
			Output: []OutputRouterSpec{{
				Type: OutputRouterSpec_MIRROR,
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
				Type: OutputRouterSpec_MIRROR,
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
					Type: OutputRouterSpec_MIRROR,
					Streams: []StreamEndpointSpec{
						{StreamID: StreamID(2)},
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
					Type:    OutputRouterSpec_MIRROR,
					Streams: []StreamEndpointSpec{{Mailbox: &MailboxSpec{SimpleResponse: true}}},
				}}},
		},
	}

	var buf bytes.Buffer
	if err := GeneratePlanDiagram(
		[]FlowSpec{f1, f2, f3}, []string{"1", "2", "3"}, &buf,
	); err != nil {
		t.Fatal(err)
	}

	expected := `digraph G {
subgraph cluster_0 {
label="node 1"
p0_0 [shape=none margin=0 label=<

<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0" CELLPADDING="4">

<TR>
  <TD BORDER="1" COLSPAN="1" BGCOLOR="SKYBLUE" port="core">TableReader<BR/><FONT POINT-SIZE="10">SomeIndex@Table</FONT><BR/><FONT POINT-SIZE="10">0,1</FONT></TD>
</TR>

</TABLE>
> ]
}
subgraph cluster_1 {
label="node 2"
p1_0 [shape=none margin=0 label=<

<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0" CELLPADDING="4">

<TR>
  <TD BORDER="1" COLSPAN="1" BGCOLOR="SKYBLUE" port="core">TableReader<BR/><FONT POINT-SIZE="10">SomeIndex@Table</FONT><BR/><FONT POINT-SIZE="10">0,1</FONT></TD>
</TR>

</TABLE>
> ]
}
subgraph cluster_2 {
label="node 3"
p2_0 [shape=none margin=0 label=<

<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0" CELLPADDING="4">

<TR>
  <TD BORDER="1" COLSPAN="1" BGCOLOR="SKYBLUE" port="core">TableReader<BR/><FONT POINT-SIZE="10">SomeIndex@Table</FONT><BR/><FONT POINT-SIZE="10">0,1</FONT></TD>
</TR>

</TABLE>
> ]
p2_1 [shape=none margin=0 label=<

<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0" CELLPADDING="4">
<TR>
<TD WIDTH="20"></TD>

  <TD BORDER="1" BGCOLOR="YELLOW" port="in0"><FONT POINT-SIZE="10">ordered</FONT><BR/><FONT POINT-SIZE="9">1&#43;</FONT></TD>
  <TD WIDTH="20"></TD>

</TR>
<TR>
  <TD BORDER="1" COLSPAN="3" BGCOLOR="SKYBLUE" port="core">JoinReader<BR/><FONT POINT-SIZE="10">primary@Table</FONT><BR/><FONT POINT-SIZE="10">2</FONT><BR/><FONT POINT-SIZE="10">$0&#43;$1&lt;$2</FONT></TD>
</TR>

</TABLE>
> ]
SimpleResponse
}
p0_0:core -> p2_1:in0
p1_0:core -> p2_1:in0
p2_0:core -> p2_1:in0
p2_1:core -> SimpleResponse
}
`
	if result := buf.String(); result != expected {
		t.Errorf("\ngot:\n%s\nwant:\n%s", result, expected)
	}
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
		Expr:           Expression{Expr: "$0+$1<$5"},
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
					Type: OutputRouterSpec_MIRROR,
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
					Type:    OutputRouterSpec_MIRROR,
					Streams: []StreamEndpointSpec{{Mailbox: &MailboxSpec{SimpleResponse: true}}},
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
					Type: OutputRouterSpec_MIRROR,
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

	expected := `digraph G {
subgraph cluster_0 {
label="node 1"
p0_0 [shape=none margin=0 label=<

<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0" CELLPADDING="4">

<TR>
  <TD BORDER="1" COLSPAN="3" BGCOLOR="SKYBLUE" port="core">TableReader<BR/><FONT POINT-SIZE="10">primary@TableA</FONT><BR/><FONT POINT-SIZE="10">0,1,3</FONT></TD>
</TR>
<TR>
<TD WIDTH="20"></TD>

  <TD BORDER="1" BGCOLOR="RED" port="out0"><FONT POINT-SIZE="10">by hash</FONT><BR/><FONT POINT-SIZE="9">0,1</FONT></TD>
  <TD WIDTH="20"></TD>

</TR>
</TABLE>
> ]
}
subgraph cluster_1 {
label="node 2"
p1_0 [shape=none margin=0 label=<

<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0" CELLPADDING="4">

<TR>
  <TD BORDER="1" COLSPAN="3" BGCOLOR="SKYBLUE" port="core">TableReader<BR/><FONT POINT-SIZE="10">primary@TableA</FONT><BR/><FONT POINT-SIZE="10">0,1,3</FONT></TD>
</TR>
<TR>
<TD WIDTH="20"></TD>

  <TD BORDER="1" BGCOLOR="RED" port="out0"><FONT POINT-SIZE="10">by hash</FONT><BR/><FONT POINT-SIZE="9">0,1</FONT></TD>
  <TD WIDTH="20"></TD>

</TR>
</TABLE>
> ]
p1_1 [shape=none margin=0 label=<

<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0" CELLPADDING="4">
<TR>
<TD WIDTH="20"></TD>

  <TD BORDER="1" BGCOLOR="YELLOW" port="in0"><FONT POINT-SIZE="10">unordered</FONT></TD>
  <TD WIDTH="20"></TD>

  <TD BORDER="1" BGCOLOR="YELLOW" port="in1"><FONT POINT-SIZE="10">unordered</FONT></TD>
  <TD WIDTH="20"></TD>

</TR>
<TR>
  <TD BORDER="1" COLSPAN="5" BGCOLOR="SKYBLUE" port="core">HashJoiner<BR/><FONT POINT-SIZE="10">ON left(0,2)=right(2,1)</FONT><BR/><FONT POINT-SIZE="10">0,1,2,3,4,5</FONT><BR/><FONT POINT-SIZE="10">$0&#43;$1&lt;$5</FONT></TD>
</TR>

</TABLE>
> ]
p1_2 [shape=none margin=0 label=<

<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0" CELLPADDING="4">
<TR>
<TD WIDTH="20"></TD>

  <TD BORDER="1" BGCOLOR="YELLOW" port="in0"><FONT POINT-SIZE="10">unordered</FONT></TD>
  <TD WIDTH="20"></TD>

</TR>
<TR>
  <TD BORDER="1" COLSPAN="3" BGCOLOR="SKYBLUE" port="core">No-op</TD>
</TR>

</TABLE>
> ]
SimpleResponse
}
subgraph cluster_2 {
label="node 3"
p2_0 [shape=none margin=0 label=<

<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0" CELLPADDING="4">

<TR>
  <TD BORDER="1" COLSPAN="3" BGCOLOR="SKYBLUE" port="core">TableReader<BR/><FONT POINT-SIZE="10">primary@TableA</FONT><BR/><FONT POINT-SIZE="10">0,1,3</FONT></TD>
</TR>
<TR>
<TD WIDTH="20"></TD>

  <TD BORDER="1" BGCOLOR="RED" port="out0"><FONT POINT-SIZE="10">by hash</FONT><BR/><FONT POINT-SIZE="9">0,1</FONT></TD>
  <TD WIDTH="20"></TD>

</TR>
</TABLE>
> ]
p2_1 [shape=none margin=0 label=<

<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0" CELLPADDING="4">

<TR>
  <TD BORDER="1" COLSPAN="3" BGCOLOR="SKYBLUE" port="core">TableReader<BR/><FONT POINT-SIZE="10">primary@TableB</FONT><BR/><FONT POINT-SIZE="10">1,2,4</FONT></TD>
</TR>
<TR>
<TD WIDTH="20"></TD>

  <TD BORDER="1" BGCOLOR="RED" port="out0"><FONT POINT-SIZE="10">by hash</FONT><BR/><FONT POINT-SIZE="9">2,1</FONT></TD>
  <TD WIDTH="20"></TD>

</TR>
</TABLE>
> ]
p2_2 [shape=none margin=0 label=<

<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0" CELLPADDING="4">
<TR>
<TD WIDTH="20"></TD>

  <TD BORDER="1" BGCOLOR="YELLOW" port="in0"><FONT POINT-SIZE="10">unordered</FONT></TD>
  <TD WIDTH="20"></TD>

  <TD BORDER="1" BGCOLOR="YELLOW" port="in1"><FONT POINT-SIZE="10">unordered</FONT></TD>
  <TD WIDTH="20"></TD>

</TR>
<TR>
  <TD BORDER="1" COLSPAN="5" BGCOLOR="SKYBLUE" port="core">HashJoiner<BR/><FONT POINT-SIZE="10">ON left(0,2)=right(2,1)</FONT><BR/><FONT POINT-SIZE="10">0,1,2,3,4,5</FONT><BR/><FONT POINT-SIZE="10">$0&#43;$1&lt;$5</FONT></TD>
</TR>

</TABLE>
> ]
}
subgraph cluster_3 {
label="node 4"
p3_0 [shape=none margin=0 label=<

<TABLE BORDER="0" CELLBORDER="0" CELLSPACING="0" CELLPADDING="4">

<TR>
  <TD BORDER="1" COLSPAN="3" BGCOLOR="SKYBLUE" port="core">TableReader<BR/><FONT POINT-SIZE="10">primary@TableB</FONT><BR/><FONT POINT-SIZE="10">1,2,4</FONT></TD>
</TR>
<TR>
<TD WIDTH="20"></TD>

  <TD BORDER="1" BGCOLOR="RED" port="out0"><FONT POINT-SIZE="10">by hash</FONT><BR/><FONT POINT-SIZE="9">2,1</FONT></TD>
  <TD WIDTH="20"></TD>

</TR>
</TABLE>
> ]
}
p0_0:out0 -> p1_1:in0
p0_0:out0 -> p2_2:in0
p1_0:out0 -> p1_1:in0
p1_0:out0 -> p2_2:in0
p1_1:core -> p1_2:in0
p1_2:core -> SimpleResponse
p2_0:out0 -> p1_1:in0
p2_0:out0 -> p2_2:in0
p2_1:out0 -> p1_1:in1
p2_1:out0 -> p2_2:in1
p2_2:core -> p1_2:in0
p3_0:out0 -> p1_1:in1
p3_0:out0 -> p2_2:in1
}
`

	if result := buf.String(); result != expected {
		t.Errorf("\ngot:\n%s\nwant:\n%s", result, expected)
	}
}
