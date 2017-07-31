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

package sql

import (
	"errors"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// traceNode is a planNode that wraps another node and uses session
// tracing to report all the database events that occur during its
// execution.
// It is used as the top-level node for SHOW TRACE FOR statements.
type traceNode struct {
	plan    planNode
	columns sqlbase.ResultColumns
	p       *planner

	execDone bool

	traceRows []traceRow
	curRow    int
}

var sessionTraceTableName = parser.TableName{
	DatabaseName: parser.Name("crdb_internal"),
	TableName:    parser.Name("session_trace"),
}

func (p *planner) makeTraceNode(plan planNode) (planNode, error) {
	desc, err := p.getVirtualTabler().getVirtualTableDesc(&sessionTraceTableName)
	if err != nil {
		return nil, err
	}
	return &traceNode{
		plan:    plan,
		p:       p,
		columns: sqlbase.ResultColumnsFromColDescs(desc.Columns),
	}, nil
}

var errTracingAlreadyEnabled = errors.New(
	"cannot run SHOW TRACE FOR on statement while session tracing is enabled" +
		" - did you mean SHOW TRACE FOR SESSION?")

func (n *traceNode) Start(params runParams) error {
	if n.p.session.Tracing.Enabled() {
		return errTracingAlreadyEnabled
	}
	if err := n.p.session.Tracing.StartTracing(tracing.SnowballRecording, true); err != nil {
		return err
	}

	startCtx, sp := tracing.ChildSpan(params.ctx, "starting plan")
	defer sp.Finish()
	params.ctx = startCtx
	return n.plan.Start(params)
}

func (n *traceNode) Close(ctx context.Context) {
	if n.plan != nil {
		n.plan.Close(ctx)
	}
	n.traceRows = nil
	if n.p.session.Tracing.Enabled() {
		// Start has already ran and enabled tracing. Stop it.
		if err := stopTracing(n.p.session); err != nil {
			log.Errorf(ctx, "error stopping tracing at end of SHOW TRACE FOR: %v", err)
		}
	}
}

func (n *traceNode) Next(params runParams) (bool, error) {
	if !n.execDone {
		// We need to run the entire statement upfront. Subsequent
		// invocations of Next() will merely return the trace.

		func() {
			consumeCtx, sp := tracing.ChildSpan(params.ctx, "consuming rows")
			defer sp.Finish()

			slowPath := true
			if a, ok := n.plan.(planNodeFastPath); ok {
				if count, res := a.FastPathResults(); res {
					log.VEventf(consumeCtx, 2, "fast path - rows affected: %d", count)
					slowPath = false
				}
			}
			if slowPath {
				for {
					hasNext, err := n.plan.Next(params)
					if err != nil {
						log.VEventf(consumeCtx, 2, "execution failed: %v", err)
						break
					}
					if !hasNext {
						break
					}

					values := n.plan.Values()
					log.VEventf(consumeCtx, 2, "output row: %s", values)
				}
			}
			log.VEventf(consumeCtx, 2, "plan completed execution")

			// Release the plan's resources early.
			n.plan.Close(consumeCtx)
			n.plan = nil

			log.VEventf(consumeCtx, 2, "resources released, stopping trace")
		}()

		if err := stopTracing(n.p.session); err != nil {
			return false, err
		}

		var err error
		n.traceRows, err = n.p.session.Tracing.generateSessionTraceVTable()
		if err != nil {
			return false, err
		}
		n.execDone = true
	}

	if n.curRow >= len(n.traceRows) {
		return false, nil
	}
	n.curRow++
	return true, nil
}

func (n *traceNode) Values() parser.Datums {
	return n.traceRows[n.curRow-1][:]
}
