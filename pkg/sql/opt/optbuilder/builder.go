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

package optbuilder

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// Builder holds the context needed for building a memo structure from a SQL
// statement. Builder.Build() is the top-level function to perform this build
// process. As part of the build process, it performs name resolution and
// type checking on the expressions within Builder.stmt.
//
// The memo structure is the primary data structure used for query
// optimization, so building the memo is the first step required to
// optimize a query. The memo is maintained inside Builder.factory,
// which exposes methods to construct expression groups inside the memo.
//
// A memo is essentially a compact representation of a forest of logically-
// equivalent query trees. Each tree is either a logical or a physical plan
// for executing the SQL query. After the build process is complete, the memo
// forest will contain exactly one tree: the logical query plan corresponding
// to the AST of the original SQL statement with some number of "normalization"
// transformations applied. Normalization transformations include heuristics
// such as predicate push-down that should always be applied. They do not
// include "exploration" transformations whose benefit must be evaluated with
// the optimizer's cost model (e.g., join reordering).
//
// See factory.go and memo.go inside the opt/xform package for more details
// about the memo structure.
type Builder struct {
	// AllowUnsupportedExpr is a control knob: if set, when building a scalar, the
	// builder takes any TypedExpr node that it doesn't recognize and wraps that
	// expression in an UnsupportedExpr node. This is temporary; it is used for
	// interfacing with the old planning code.
	AllowUnsupportedExpr bool

	factory *norm.Factory
	stmt    tree.Statement

	ctx     context.Context
	semaCtx *tree.SemaContext
	evalCtx *tree.EvalContext
	catalog opt.Catalog

	// Skip index 0 in order to reserve it to indicate the "unknown" column.
	colMap []scopeColumn
}

// New creates a new Builder structure initialized with the given
// parsed SQL statement.
func New(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	catalog opt.Catalog,
	factory *norm.Factory,
	stmt tree.Statement,
) *Builder {
	return &Builder{
		factory: factory,
		stmt:    stmt,
		colMap:  make([]scopeColumn, 1),
		ctx:     ctx,
		semaCtx: semaCtx,
		evalCtx: evalCtx,
		catalog: catalog,
	}
}

// Build is the top-level function to build the memo structure inside
// Builder.factory from the parsed SQL statement in Builder.stmt. See the
// comment above the Builder type declaration for details.
//
// The first return value `root` is the group ID of the root memo group.
// The second return value `required` is the set of physical properties
// (e.g., row and column ordering) that are required of the root memo group.
// If any subroutines panic with a builderError as part of the build process,
// the panic is caught here and returned as an error.
func (b *Builder) Build() (root memo.GroupID, required *memo.PhysicalProps, err error) {
	defer func() {
		if r := recover(); r != nil {
			// This code allows us to propagate builder errors without adding
			// lots of checks for `if err != nil` throughout the code. This is
			// only possible because the code does not update shared state and does
			// not manipulate locks.
			if bldErr, ok := r.(builderError); ok {
				err = bldErr
			} else {
				panic(r)
			}
		}
	}()

	outScope := b.buildStmt(b.stmt, &scope{builder: b})
	root = outScope.group
	required = b.buildPhysicalProps(outScope)
	return root, required, nil
}

// builderError is used for semantic errors that occur during the build process
// and is passed as an argument to panic. These panics are caught and converted
// back to errors inside Builder.Build.
type builderError struct {
	error
}

// errorf formats according to a format specifier and returns the
// string as a builderError.
func errorf(format string, a ...interface{}) builderError {
	err := fmt.Errorf(format, a...)
	return builderError{err}
}

// buildPhysicalProps construct a set of required physical properties from the
// given scope.
func (b *Builder) buildPhysicalProps(scope *scope) *memo.PhysicalProps {
	if scope.physicalProps.Presentation == nil {
		scope.physicalProps.Presentation = makePresentation(scope.cols)
	}
	return &scope.physicalProps
}

// buildStmt builds a set of memo groups that represent the given SQL
// statement.
//
// NOTE: The following descriptions of the inScope parameter and outScope
//       return value apply for all buildXXX() functions in this directory.
//       Note that some buildXXX() functions pass outScope as a parameter
//       rather than a return value so its scopeColumns can be built up
//       incrementally across several function calls.
//
// inScope   This parameter contains the name bindings that are visible for this
//           statement/expression (e.g., passed in from an enclosing statement).
//
// outScope  This return value contains the newly bound variables that will be
//           visible to enclosing statements, as well as a pointer to any
//           "parent" scope that is still visible. The top-level memo group ID
//           for the built statement/expression is returned in outScope.group.
func (b *Builder) buildStmt(stmt tree.Statement, inScope *scope) (outScope *scope) {
	// NB: The case statements are sorted lexicographically.
	switch stmt := stmt.(type) {
	case *tree.ParenSelect:
		return b.buildSelect(stmt.Select, inScope)

	case *tree.Select:
		return b.buildSelect(stmt, inScope)

	default:
		panic(errorf("unexpected statement: %T", stmt))
	}
}
