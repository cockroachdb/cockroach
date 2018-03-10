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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// zeroNode is a planNode with no columns and no rows and is used for nodes that
// have no results. (e.g. a table for which the filtering condition has a
// contradiction).
type zeroNode struct {
	columns sqlbase.ResultColumns
}

func newZeroNode(columns sqlbase.ResultColumns) *zeroNode {
	return &zeroNode{columns: columns}
}

// NewZeroNode is the exported version of makeZeroNode. Used by CCL.
func NewZeroNode(columns sqlbase.ResultColumns) PlanNode {
	return newZeroNode(columns)
}

func (z *zeroNode) Next(runParams) (bool, error) { return false, nil }
func (*zeroNode) Values() tree.Datums            { return nil }
func (*zeroNode) Close(context.Context)          {}
