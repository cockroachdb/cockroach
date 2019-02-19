// Copyright 2018 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

type lookupJoinNode struct {
	input planNode
	table *scanNode

	// joinType is either INNER or LEFT_OUTER.
	joinType sqlbase.JoinType

	// keyCols identifies the columns from the input which are used for the
	// lookup. These correspond to a prefix of the index columns (of the index we
	// are looking up into).
	keyCols []int

	// columns are the produced columns, namely the input clumns and the
	// columns in the table scanNode.
	columns sqlbase.ResultColumns

	// onCond is any ON condition to be used in conjunction with the implicit
	// equality condition on keyCols.
	onCond tree.TypedExpr

	props physicalProps
}

func (lj *lookupJoinNode) startExec(params runParams) error {
	panic("lookupJoinNode cannot be run in local mode")
}

func (lj *lookupJoinNode) Next(params runParams) (bool, error) {
	panic("lookupJoinNode cannot be run in local mode")
}

func (lj *lookupJoinNode) Values() tree.Datums {
	panic("lookupJoinNode cannot be run in local mode")
}

func (lj *lookupJoinNode) Close(ctx context.Context) {
	lj.input.Close(ctx)
	lj.table.Close(ctx)
}
