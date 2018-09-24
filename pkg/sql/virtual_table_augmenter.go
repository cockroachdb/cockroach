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

// virtualTableAugmenter is the function signature for the
// virtualTableAugmenterNode `next` property. This function takes in a
// tree.Datums representing the current row of the table, and augments it by
// retrieving the missing values. Each time the virtualTableAugmenter function
// is called, it returns a tree.Datums corresponding to the complete next row of
// the virtual schema table. If there is no next row (end of table is reached),
// then return (nil, nil). If there is an error, then return (nil, error).
type virtualTableAugmenter func(row tree.Datums) (tree.Datums, error)

// virtualTableAugmenterNode is a planNode that constructs its rows by invoking
// a virtualTableAugmenter function on each row returned by a corresponding
// virtualTableNode.
type virtualTableAugmenterNode struct {
	source     planDataSource
	columns    sqlbase.ResultColumns
	next       virtualTableAugmenter
	currentRow tree.Datums
}

func (p *planner) newContainerVirtualTableAugmenterNode(
	columns sqlbase.ResultColumns, next virtualTableAugmenter, source planDataSource,
) *virtualTableAugmenterNode {
	return &virtualTableAugmenterNode{
		source:  source,
		columns: columns,
		next:    next,
	}
}

func (n *virtualTableAugmenterNode) Next(params runParams) (bool, error) {
	if next, err := n.source.plan.Next(params); !next {
		return false, err
	}

	row, err := n.next(n.source.plan.Values())
	if err != nil {
		return false, err
	}
	n.currentRow = row
	return row != nil, nil
}

func (n *virtualTableAugmenterNode) Values() tree.Datums {
	return n.currentRow
}

func (n *virtualTableAugmenterNode) Close(ctx context.Context) {
}
