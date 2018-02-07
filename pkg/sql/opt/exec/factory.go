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

package exec

import (
	"github.com/cockroachdb/cockroach/pkg/sql/optbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// Factory defines the interface for building an execution plan, which consists
// of a tree of execution nodes (currently a sql.planNode tree).
//
// The TypedExprs passed to these functions refer to columns of the input node
// via IndexedVars.
type Factory interface {
	// ConstructScan returns a node that represents a scan of the given table.
	// TODO(radu): support list of columns, index, index constraints
	ConstructScan(table optbase.Table) (Node, error)

	// ConstructFilter returns a node that applies a filter on the results of
	// the given input node.
	ConstructFilter(n Node, filter tree.TypedExpr) (Node, error)

	// ConstructProject returns a node that applies a projection on the results of
	// the given input node.
	ConstructProject(n Node, exprs tree.TypedExprs, colNames []string) (Node, error)
}
