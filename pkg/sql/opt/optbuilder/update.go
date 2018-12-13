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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/pkg/errors"
)

// buildUpdate builds a memo group for an UpdateOp expression. First, an input
// expression is constructed that outputs the existing values for all rows from
// the target table that match the WHERE clause. Additional column(s) that
// provide updated values are projected for each of the SET expressions, as well
// as for any computed columns. For example:
//
//   CREATE TABLE abc (a INT PRIMARY KEY, b INT, c INT)
//   UPDATE abc SET b=1 WHERE a=2
//
// This would create an input expression similar to this SQL:
//
//   SELECT a AS oa, b AS ob, c AS oc, 1 AS nb FROM abc WHERE a=2
//
// The execution engine evaluates this relational expression and uses the
// resulting values to form the KV keys and values.
//
// Tuple SET expressions are decomposed into individual columns:
//
//   UPDATE abc SET (b, c)=(1, 2) WHERE a=3
//   =>
//   SELECT a AS oa, b AS ob, c AS oc, 1 AS nb, 2 AS nc FROM abc WHERE a=3
//
// Subqueries become correlated left outer joins:
//
//   UPDATE abc SET b=(SELECT y FROM xyz WHERE x=a)
//   =>
//   SELECT a AS oa, b AS ob, c AS oc, y AS nb
//   FROM abc
//   LEFT JOIN LATERAL (SELECT y FROM xyz WHERE x=a)
//   ON True
//
// Computed columns result in an additional wrapper projection that can depend
// on input columns.
//
// Note that the ORDER BY clause can only be used if the LIMIT clause is also
// present. In that case, the ordering determines which rows are included by the
// limit. The ORDER BY makes no additional guarantees about the order in which
// mutations are applied, or the order of any returned rows (i.e. it won't
// become a physical property required of the Update operator).
func (b *Builder) buildUpdate(upd *tree.Update, inScope *scope) (outScope *scope) {
	if !b.evalCtx.SessionData.OptimizerUpdates {
		panic(unimplementedf("cost-based optimizer is not planning UPDATE statements"))
	}

	if upd.OrderBy != nil && upd.Limit == nil {
		panic(builderError{errors.New("UPDATE statement requires LIMIT when ORDER BY is used")})
	}

	// UX friendliness safeguard.
	if upd.Where == nil && b.evalCtx.SessionData.SafeUpdates {
		panic(builderError{pgerror.NewDangerousStatementErrorf("UPDATE without WHERE clause")})
	}

	if upd.With != nil {
		inScope = b.buildCTE(upd.With.CTEList, inScope)
		defer b.checkCTEUsage(inScope)
	}

	// UPDATE xx AS yy - we want to know about xx (tn) because
	// that's what we get the descriptor with, and yy (alias) because
	// that's what RETURNING will use.
	tn, alias := getAliasedTableName(upd.Table)

	// Find which table we're working on, check the permissions.
	tab := b.resolveTable(tn, privilege.UPDATE)

	// Check Select permission as well, since existing values must be read.
	b.checkPrivilege(tab, privilege.SELECT)

	var mb mutationBuilder
	mb.init(b, opt.UpdateOp, tab, alias)

	// Build the input expression that selects the rows that will be updated:
	//
	//   WITH <with>
	//   SELECT <cols> FROM <table> WHERE <where>
	//   ORDER BY <order-by> LIMIT <limit>
	//
	// All columns from the update table will be projected.
	mb.buildInputForUpdate(inScope, upd)

	// Derive the columns that will be updated from the SET expressions.
	mb.addTargetColsForUpdate(upd.Exprs)

	// Build each of the SET expressions.
	mb.addUpdateCols(upd.Exprs)

	// Add additional columns for computed expressions that may depend on the
	// updated columns.
	mb.addComputedColsForUpdate()

	// Build the final update statement, including any returned expressions.
	if resultsNeeded(upd.Returning) {
		mb.buildUpdate(*upd.Returning.(*tree.ReturningExprs))
	} else {
		mb.buildUpdate(nil /* returning */)
	}

	return mb.outScope
}
