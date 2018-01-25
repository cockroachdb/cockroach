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
		_ context.Context, stmt tree.Statement, state sql.PlanHookState,
	) (func(context.Context, chan<- tree.Datums) error, sqlbase.ResultColumns, error) {
		show, ok := stmt.(*tree.ShowVar)
		if !ok || show.Name != "planhook" {
			return nil, nil, nil
		}
		header := sqlbase.ResultColumns{
			{Name: "value", Typ: types.String},
		}
		return func(_ context.Context, resultsCh chan<- tree.Datums) error {
			resultsCh <- tree.Datums{tree.NewDString(show.Name)}
			return nil
		}, header, nil
	}
	sql.AddPlanHook(testingPlanHook)
}
