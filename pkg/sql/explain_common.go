// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import "github.com/cockroachdb/cockroach/pkg/sql/opt/memo"

func (p *planner) FormatOptPlan(flags memo.ExprFmtFlags) string {
	mem := p.curPlan.mem
	catalog := &p.optPlanningCtx.catalog
	f := memo.MakeExprFmtCtx(flags, mem, catalog)
	f.FormatExpr(mem.RootExpr())
	return f.Buffer.String()
}
