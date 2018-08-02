// Copyright 2017 The Cockroach Authors.
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

package tests

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// Add a placeholder implementation to test the plan hook. It accepts statements
// of the form `SHOW planhook` and returns a single row with the string value
// 'planhook'.
func init() {
	testingPlanHook := func(
		ctx context.Context, stmt tree.Statement, state sql.PlanHookState,
	) (sql.PlanHookRowFn, sqlbase.ResultColumns, []sql.PlanNode, error) {
		show, ok := stmt.(*tree.ShowVar)
		if !ok || show.Name != "planhook" {
			return nil, nil, nil, nil
		}
		header := sqlbase.ResultColumns{
			{Name: "value", Typ: types.String},
		}
		rows := tree.Exprs{tree.NewStrVal(show.Name)}
		sel := &tree.Select{Select: &tree.ValuesClause{Rows: []tree.Exprs{rows}}}
		subPlan, err := state.Select(ctx, sel, nil)

		return func(_ context.Context, subPlans []sql.PlanNode, resultsCh chan<- tree.Datums) error {
			for {
				ok, err := subPlans[0].Next(state.RunParams(ctx))
				if err != nil {
					return err
				}
				if !ok {
					break
				}
				resultsCh <- subPlans[0].Values()
			}
			subPlans[0].Close(ctx)
			return nil
		}, header, []sql.PlanNode{subPlan}, err
	}
	sql.AddPlanHook(testingPlanHook)
}
