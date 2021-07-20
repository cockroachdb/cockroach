// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
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
	// UX friendliness safeguard.
	if del.Where == nil && b.evalCtx.SessionData.SafeUpdates {
		panic(pgerror.DangerousStatementf("DELETE without WHERE clause"))
	}

	if del.OrderBy != nil && del.Limit == nil {
		panic(pgerror.Newf(pgcode.Syntax,
			"DELETE statement requires LIMIT when ORDER BY is used"))
	}

	// Find which table we're working on, check the permissions.
	tab, depName, alias, refColumns := b.resolveTableForMutation(del.Table, privilege.DELETE)

	if refColumns != nil {
		panic(pgerror.Newf(pgcode.Syntax,
			"cannot specify a list of column IDs with DELETE"))
	}

	// Check Select permission as well, since existing values must be read.
	b.checkPrivilege(depName, tab, privilege.SELECT)

	var mb mutationBuilder
	mb.init(b, "delete", tab, alias)

	// Build the input expression that selects the rows that will be deleted:
	//
	//   WITH <with>
	//   SELECT <cols> FROM <table> WHERE <where>
	//   ORDER BY <order-by> LIMIT <limit>
	//
	// All columns from the delete table will be projected.
	mb.buildInputForDelete(inScope, del.Table, del.Where, del.Limit, del.OrderBy)

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
	mb.buildFKChecksAndCascadesForDelete()

	// Project partial index DEL boolean columns.
	mb.projectPartialIndexDelCols()

	private := mb.makeMutationPrivate(returning != nil)
	mb.outScope.expr = mb.b.factory.ConstructDelete(
		mb.outScope.expr, mb.uniqueChecks, mb.fkChecks, private,
	)

	mb.buildReturning(returning)
}
