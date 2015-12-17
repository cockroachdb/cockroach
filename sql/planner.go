package sql

import (
	"time"

	"github.com/cockroachdb/cockroach/sql/driver"
	"github.com/cockroachdb/cockroach/sql/parser"
)

// StatementResult prepares the given statement(s) and returns the result types.
func (e *Executor) StatementResult(user string, stmt parser.Statement, args parser.MapArgs) ([]*driver.Response_Result_Rows_Column, error) {
	planMaker := plannerPool.Get().(*planner)
	defer plannerPool.Put(planMaker)

	*planMaker = planner{
		user: user,
		evalCtx: parser.EvalContext{
			NodeID:  e.nodeID,
			ReCache: e.reCache,
			// Copy existing GetLocation closure. See plannerPool.New() for the
			// initial setting.
			GetLocation: planMaker.evalCtx.GetLocation,
			Args:        args,
		},
		leaseMgr:     e.leaseMgr,
		systemConfig: e.getSystemConfig(),
	}

	planMaker.evalCtx.StmtTimestamp = parser.DTimestamp{Time: time.Now()}
	plan, err := planMaker.makePlan(stmt)
	if err != nil {
		return nil, err
	}
	cols := make([]*driver.Response_Result_Rows_Column, len(plan.Columns()))
	for i, c := range plan.Columns() {
		d, err := makeDriverDatum(c.typ)
		if err != nil {
			return nil, err
		}
		cols[i] = &driver.Response_Result_Rows_Column{
			Name: c.name,
			Typ:  d,
		}
	}
	return cols, nil
}
