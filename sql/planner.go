package sql

import (
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/sql/driver"
	"github.com/cockroachdb/cockroach/sql/parser"
)

func (e *Executor) StatementResult(user string, stmt parser.Statement) ([]*driver.Response_Result_Rows_Column, error) {
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
		},
		leaseMgr:     e.leaseMgr,
		systemConfig: e.getSystemConfig(),
	}

	planMaker.evalCtx.StmtTimestamp = parser.DTimestamp{Time: time.Now()}
	plan, err := planMaker.makePlan(stmt)
	if err != nil {
		return nil, err
	}
	var cols []*driver.Response_Result_Rows_Column
	for _, c := range plan.Columns() {
		var d driver.Datum
		switch c.typ.(type) {
		case parser.DBool:
			d.Payload = &driver.Datum_BoolVal{}
		case parser.DInt:
			d.Payload = &driver.Datum_IntVal{}
		case parser.DFloat:
			d.Payload = &driver.Datum_FloatVal{}
		case parser.DBytes:
			d.Payload = &driver.Datum_BytesVal{}
		case parser.DString:
			d.Payload = &driver.Datum_StringVal{}
		case parser.DDate:
			d.Payload = &driver.Datum_DateVal{}
		case parser.DTimestamp:
			d.Payload = &driver.Datum_TimeVal{}
		case parser.DInterval:
			d.Payload = &driver.Datum_IntervalVal{}
		default:
			return nil, fmt.Errorf("unknown type %T", c.typ)
		}
		cols = append(cols, &driver.Response_Result_Rows_Column{
			Name: c.name,
			Typ:  d,
		})
	}
	return cols, nil
}
