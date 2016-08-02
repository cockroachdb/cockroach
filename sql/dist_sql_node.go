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
)

// distSQLNode is a planNode that receives results from a distsql flow (through
// a RowChannel).
type distSQLNode struct {
	columns  []ResultColumn
	ordering orderingInfo

	// syncMode indicates the mode in which we run the associated flow. If true,
	// we run in a mode used for small queries (when the number of table rows
	// needed is known). In this mode we run the processors serially and
	// accumulate the output of each processor in memory.
	syncMode bool

	// flowResult is used to access the results of the flow. It can be a
	// RowChannel or a RowBuffer (depending on syncMode).
	flowResult distsql.RowSource

	// colMapping maps columns in the RowChannel stream to result columns.
	colMapping []uint32

	flow *distsql.Flow

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
	srv *distsql.ServerImpl,
	flowReq *distsql.SetupFlowRequest,
	syncMode bool,
) (*distSQLNode, error) {
	n := &distSQLNode{
		syncMode:   syncMode,
		columns:    columns,
		ordering:   ordering,
		colMapping: colMapping,
		values:     make(parser.DTuple, len(columns)),
	}
	var recv distsql.RowReceiver
	if syncMode {
		rb := new(distsql.RowBuffer)
		n.flowResult = rb
		recv = rb
	} else {
		rc := new(distsql.RowChannel)
		rc.Init()
		n.flowResult = rc
		recv = rc
	}

	flow, err := srv.SetupSimpleFlow(context.Background(), flowReq, recv)
	if err != nil {
		return nil, err
	}
	n.flow = flow
	return n, nil
}

func (n *distSQLNode) Next() (bool, error) {
	if !n.flowStarted {
		if n.syncMode {
			n.flow.RunSync()
		} else {
			n.flow.Start()
		}
		n.flowStarted = true
	}
	row, err := n.flowResult.NextRow()
	if err != nil {
		return false, err
	}
	if row == nil {
		return false, nil
	}
	for i := range row {
		col := n.colMapping[i]
		err := row[i].Decode(&n.alloc)
		if err != nil {
			return false, err
		}
		n.values[col] = row[i].Datum
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
		//
		// The distsql Expression uses the placeholder syntax ($0, $1, $2..) to
		// refer to columns. We temporarily rename the scanNode columns to
		// (literally) "$0", "$1", ... and convert to a string.
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
// If syncMode is true, the plan does not instantiate any goroutines
// internally.
func scanNodeToDistSQL(n *scanNode, syncMode bool) (*distSQLNode, error) {
	req := distsql.SetupFlowRequest{Txn: n.p.txn.Proto}
	tr := scanNodeToTableReaderSpec(n)
	req.Flow = distsql.FlowSpec{
		Processors: []distsql.ProcessorSpec{{
			Core: distsql.ProcessorCoreUnion{TableReader: tr},
			Output: []distsql.OutputRouterSpec{{
				Type: distsql.OutputRouterSpec_MIRROR,
				Streams: []distsql.StreamEndpointSpec{{
					Mailbox: &distsql.MailboxSpec{SimpleResponse: true},
				}},
			}},
		}},
	}

	return newDistSQLNode(
		n.resultColumns, tr.OutputColumns, n.ordering, n.p.execCtx.DistSQLSrv, &req, syncMode)
}

// hackPlanToUseDistSQL goes through a planNode tree and replaces each scanNode with
// a distSQLNode and a corresponding flow.
// If syncMode is true, the plan does not instantiate any goroutines
// internally.
func hackPlanToUseDistSQL(plan planNode, syncMode bool) error {
	// Trigger limit propagation.
	plan.SetLimitHint(math.MaxInt64, true)

	if sel, ok := plan.(*selectNode); ok {
		if scan, ok := sel.source.plan.(*scanNode); ok {
			distNode, err := scanNodeToDistSQL(scan, syncMode)
			if err != nil {
				return err
			}
			sel.source.plan = distNode
		}
	}

	_, _, children := plan.ExplainPlan(true)
	for _, c := range children {
		if err := hackPlanToUseDistSQL(c, syncMode); err != nil {
			return err
		}
	}
	return nil
}
