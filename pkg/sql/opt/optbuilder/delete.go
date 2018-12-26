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
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/pkg/errors"
)

// buildDelete builds a memo group for a DeleteOp expression, which deletes all
// rows projected by the input expression. All columns from the deletion table
// are projected, including mutation columns (the optimizer may later prune the
// columns if they are not needed).
//
// Note that the ORDER BY clause can only be used if the LIMIT clause is also
// present. In that case, the ordering determines which rows are included by the
// limit. The ORDER BY makes no additional guarantees about the order in which
// mutations are applied, or the order of any returned rows (i.e. it won't
// become a physical property required of the Delete operator).
func (b *Builder) buildDelete(del *tree.Delete, inScope *scope) (outScope *scope) {
	if !b.evalCtx.SessionData.OptimizerMutations {
		panic(unimplementedf("cost-based optimizer is not planning DELETE statements"))
	}

	// UX friendliness safeguard.
	if del.Where == nil && b.evalCtx.SessionData.SafeUpdates {
		panic(builderError{pgerror.NewDangerousStatementErrorf("DELETE without WHERE clause")})
	}

	if del.OrderBy != nil && del.Limit == nil {
		panic(builderError{errors.New("DELETE statement requires LIMIT when ORDER BY is used")})
	}

	if del.With != nil {
		inScope = b.buildCTE(del.With.CTEList, inScope)
		defer b.checkCTEUsage(inScope)
	}

	// DELETE FROM xx AS yy - we want to know about xx (tn) because
	// that's what we get the descriptor with, and yy (alias) because
	// that's what RETURNING will use.
	tn, alias := getAliasedTableName(del.Table)

	// Find which table we're working on, check the permissions.
	tab := b.resolveTable(tn, privilege.DELETE)

	// Check Select permission as well, since existing values must be read.
	b.checkPrivilege(tab, privilege.SELECT)

	var mb mutationBuilder
	mb.init(b, opt.DeleteOp, tab, alias)

	// Build the input expression that selects the rows that will be deleted:
	//
	//   WITH <with>
	//   SELECT <cols> FROM <table> WHERE <where>
	//   ORDER BY <order-by> LIMIT <limit>
	//
	// All columns from the delete table will be projected.
	mb.buildInputForUpdateOrDelete(inScope, del.Where, del.Limit, del.OrderBy)

	// Build the final delete statement, including any returned expressions.
	if resultsNeeded(del.Returning) {
		mb.buildDelete(*del.Returning.(*tree.ReturningExprs))
	} else {
		mb.buildDelete(nil /* returning */)
	}

	return mb.outScope
}

// buildDelete constructs a Delete operator, possibly wrapped by a Project
// operator that corresponds to the given RETURNING clause.
func (mb *mutationBuilder) buildDelete(returning tree.ReturningExprs) {
	private := memo.MutationPrivate{
		Table:       mb.tabID,
		FetchCols:   mb.fetchColList,
		NeedResults: returning != nil,
	}
	mb.outScope.expr = mb.b.factory.ConstructDelete(mb.outScope.expr, &private)

	mb.buildReturning(returning)
}
