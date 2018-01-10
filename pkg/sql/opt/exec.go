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

package opt

import (
	"github.com/cockroachdb/cockroach/pkg/sql/optbase"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/pkg/errors"
)

// ExecNode represents a node in the execution tree (currently maps to a
// sql.planNode).
type ExecNode interface {
	// Run() executes the plan and returns the results as a Datum table.
	Run() ([]tree.Datums, error)
}

// ExecBuilder is an interface used by the opt package to build an execution
// plan (currently a sql.planNode tree).
type ExecBuilder interface {
	// Scan returns an ExecNode that represents a scan of the given table.
	// TODO(radu): support list of columns, index, index constraints
	Scan(table optbase.Table) (ExecNode, error)
}

// ExecBuilderFactory is an interface used to generate an ExecBuilder.
type ExecBuilderFactory interface {
	// New returns an ExecBuilder.
	NewExecBuilder() ExecBuilder
}

// makeExec uses an ExecBuilder to build an execution tree.
func makeExec(e *Expr, bld ExecBuilder) (ExecNode, error) {
	switch e.op {
	case scanOp:
		return bld.Scan(e.private.(optbase.Table))
	default:
		return nil, errors.Errorf("unsupported op %s", e.op)
	}
}
