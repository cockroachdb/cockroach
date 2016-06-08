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

package sql

import (
	"fmt"
	"math"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/sql/distsql"
	"github.com/cockroachdb/cockroach/sql/parser"
	"github.com/cockroachdb/cockroach/sql/sqlbase"
	"github.com/cockroachdb/cockroach/util"
)

// distSQLNode is a planNode that receives results from a distsql flow (through
// a RowChannel).
type distSQLNode struct {
	columns  []ResultColumn
	ordering orderingInfo

	// colMapping maps columns in the RowChannel stream to result columns.
	colMapping []uint32

	flow *distsql.Flow
	c    distsql.RowChannel

	values parser.DTuple
	alloc  sqlbase.DatumAlloc

	flowStarted bool
}

var _ planNode = &distSQLNode{}

func (n *distSQLNode) ExplainTypes(func(elem string, desc string)) {}
func (n *distSQLNode) SetLimitHint(int64, bool)                    {}
func (n *distSQLNode) expandPlan() error                           { return nil }
func (n *distSQLNode) MarkDebug(explainMode)                       {}
func (n *distSQLNode) DebugValues() debugValues                    { return debugValues{} }
func (n *distSQLNode) Start() error                                { return nil }

func (n *distSQLNode) ExplainPlan(verbose bool) (name, description string, children []planNode) {
	return "distsql", "", nil
}

func (n *distSQLNode) Columns() []ResultColumn {
	return n.columns
}

func (n *distSQLNode) Ordering() orderingInfo {
	return n.ordering
}

func newDistSQLNode(
	columns []ResultColumn,
	colMapping []uint32,
	ordering orderingInfo,
) *distSQLNode {
	n := &distSQLNode{
		columns:    columns,
		ordering:   ordering,
		colMapping: colMapping,
		values:     make(parser.DTuple, len(columns)),
	}
	n.c.Init()
	return n
}

func (n *distSQLNode) Next() (bool, error) {
	if !n.flowStarted {
		n.flow.Start()
		n.flowStarted = true
	}
	d, ok := <-n.c.C
	if !ok {
		// No more data
		return false, nil
	}
	if d.Err != nil {
		return false, d.Err
	}
	if len(d.Row) != len(n.colMapping) {
		return false, util.Errorf("row length %d, expected %d", len(d.Row), len(n.colMapping))
	}
	for i := range d.Row {
		col := n.colMapping[i]
		err := d.Row[i].Decode(&n.alloc)
		if err != nil {
			return false, err
		}
		n.values[col] = d.Row[i].Datum
	}
	return true, nil
}

func (n *distSQLNode) Values() parser.DTuple {
	return n.values
}

// scanNodeToTableReaderSpec generates a TableReaderSpec that corresponds to a
// scanNode.
func scanNodeToTableReaderSpec(n *scanNode) *distsql.TableReaderSpec {
	s := &distsql.TableReaderSpec{
		Table:   n.desc,
		Reverse: n.reverse,
	}
	if n.index != &n.desc.PrimaryIndex {
		for i := range n.desc.Indexes {
			if n.index == &n.desc.Indexes[i] {
				s.IndexIdx = uint32(i + 1)
				break
			}
		}
		if s.IndexIdx == 0 {
			panic("invalid scanNode index")
		}
	}
	s.Spans = make([]distsql.TableReaderSpan, len(n.spans))
	for i, span := range n.spans {
		s.Spans[i].Span.Key = span.Start
		s.Spans[i].Span.EndKey = span.End
	}
	s.OutputColumns = make([]uint32, 0, len(n.resultColumns))
	for i := range n.resultColumns {
		if n.valNeededForCol[i] {
			s.OutputColumns = append(s.OutputColumns, uint32(i))
		}
	}
	if n.limitSoft {
		s.SoftLimit = n.limitHint
	} else {
		s.HardLimit = n.limitHint
	}

	if n.filter != nil {
		// Ugly hack to get the expression to print the way we want it.
		tmp := n.resultColumns
		n.resultColumns = make([]ResultColumn, len(tmp))
		for i, orig := range tmp {
			n.resultColumns[i].Name = fmt.Sprintf("$%d", i)
			n.resultColumns[i].Typ = orig.Typ
			n.resultColumns[i].hidden = orig.hidden
		}
		expr := n.filter.String()
		n.resultColumns = tmp
		s.Filter.Expr = expr
	}
	return s
}

// scanNodeToDistSQL creates a flow and distSQLNode that correspond to a
// scanNode.
func scanNodeToDistSQL(n *scanNode) (*distSQLNode, error) {
	req := &distsql.SetupFlowsRequest{Txn: n.p.txn.Proto}
	tr := scanNodeToTableReaderSpec(n)
	req.Flows = []distsql.FlowSpec{{
		Processors: []distsql.ProcessorSpec{{
			Core: distsql.ProcessorCoreUnion{TableReader: tr},
			Output: []distsql.OutputRouterSpec{{
				Type: distsql.OutputRouterSpec_MIRROR,
				Streams: []distsql.StreamEndpointSpec{{
					Mailbox: &distsql.MailboxSpec{SimpleResponse: true},
				}},
			}},
		}},
	}}

	dn := newDistSQLNode(n.resultColumns, tr.OutputColumns, n.ordering)

	srv := n.p.execCtx.DistSQLSrv
	flow, err := srv.SetupSimpleFlow(context.Background(), req, &dn.c)
	if err != nil {
		return nil, err
	}
	dn.flow = flow
	return dn, nil
}

// hackPlanToUseDistSQL goes through a planNode tree and replaces each scanNode with
// a distSQLNode and a corresponding flow.
func hackPlanToUseDistSQL(plan planNode) error {
	// Trigger limit propagation.
	plan.SetLimitHint(math.MaxInt64, true)

	if sel, ok := plan.(*selectNode); ok {
		if scan, ok := sel.source.plan.(*scanNode); ok {
			distNode, err := scanNodeToDistSQL(scan)
			if err != nil {
				return err
			}
			sel.source.plan = distNode
		}
	}

	_, _, children := plan.ExplainPlan(true)
	for _, c := range children {
		if err := hackPlanToUseDistSQL(c); err != nil {
			return err
		}
	}
	return nil
}
