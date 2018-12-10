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
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// buildInsert builds a memo group for an InsertOp expression. An input
// expression is constructed which outputs these columns:
//
//   1. Columns explicitly specified by the user in SELECT or VALUES expression.
//
//   2. Columns not specified by the user, but having a default value declared
//      in schema (or being nullable).
//
//   3. Computed columns.
//
//   4. Mutation columns which are being added or dropped by an online schema
//      change.
//
// buildInsert starts by constructing the input expression, and then wraps it
// with Project operators which add default, computed, and mutation columns. The
// final input expression will project values for all columns in the target
// table. For example, if this is the schema and INSERT statement:
//
//   CREATE TABLE abcd (
//     a INT PRIMARY KEY,
//     b INT,
//     c INT DEFAULT(10),
//     d INT AS (b+c) STORED
//   )
//   INSERT INTO abcd (a) VALUES (1)
//
// Then an input expression equivalent to this would be built:
//
//   INSERT INTO abcd (a, b, c, d)
//   SELECT aa, bb, cc, bb + cc AS dd
//   FROM (VALUES (1, NULL, 10)) AS t(aa, bb, cc)
//
func (b *Builder) buildInsert(ins *tree.Insert, inScope *scope) (outScope *scope) {
	if ins.OnConflict != nil {
		panic(unimplementedf("UPSERT is not supported"))
	}

	if ins.With != nil {
		inScope = b.buildCTE(ins.With.CTEList, inScope)
		defer b.checkCTEUsage(inScope)
	}

	// INSERT INTO xx AS yy - we want to know about xx (tn) because
	// that's what we get the descriptor with, and yy (alias) because
	// that's what RETURNING will use.
	tn, alias := getAliasedTableName(ins.Table)

	// Find which table we're working on, check the permissions.
	tab := b.resolveTable(tn, privilege.INSERT)

	// Table resolution checked the INSERT permission, but if OnConflict is
	// defined, then check the UPDATE permission as well. This has the side effect
	// of twice adding the table to the metadata dependency list.
	if ins.OnConflict != nil && !ins.OnConflict.DoNothing {
		b.checkPrivilege(tab, privilege.UPDATE)
	}

	var mb mutationBuilder
	mb.init(b, opt.InsertOp, tab, alias)

	// Compute target columns in two cases:
	//
	//   1. When explicitly specified by name:
	//
	//        INSERT INTO <table> (<col1>, <col2>, ...) ...
	//
	//   2. When implicitly targeted by VALUES expression:
	//
	//        INSERT INTO <table> VALUES (...)
	//
	// Target columns for other cases can't be derived until the input expression
	// is built, at which time the number of input columns is known. At the same
	// time, the input expression cannot be built until DEFAULT expressions are
	// replaced and named target columns are known. So this step must come first.
	if len(ins.Columns) != 0 {
		// Target columns are explicitly specified by name.
		mb.addTargetNamedColsForInsert(ins.Columns)
	} else {
		values := mb.extractValuesInput(ins.Rows)
		if values != nil {
			// Target columns are implicitly targeted by VALUES expression in the
			// same order they appear in the target table schema.
			mb.addTargetTableColsForInsert(len(values.Rows[0]))
		}
	}

	// Build the input rows expression if one was specified:
	//
	//   INSERT INTO <table> VALUES ...
	//   INSERT INTO <table> SELECT ... FROM ...
	//
	// or initialize an empty input if inserting default values (default values
	// will be added later):
	//
	//   INSERT INTO <table> DEFAULT VALUES
	//
	if !ins.DefaultValues() {
		// Replace any DEFAULT expressions in the VALUES clause, if a VALUES clause
		// exists:
		//
		//   INSERT INTO <table> VALUES (..., DEFAULT, ...)
		//
		rows := mb.replaceDefaultExprs(ins.Rows)

		mb.buildInputForInsert(inScope, rows)
	} else {
		mb.buildEmptyInput(inScope)
	}

	// Add default and computed columns that were not explicitly specified by
	// name or implicitly targeted by input columns. This includes any columns
	// undergoing write mutations, as they must always have a default or computed
	// value.
	mb.addDefaultAndComputedColsForInsert()

	// Build the final insert statement, including any returned expressions.
	if resultsNeeded(ins.Returning) {
		mb.buildInsert(*ins.Returning.(*tree.ReturningExprs))
	} else {
		mb.buildInsert(nil /* returning */)
	}

	return mb.outScope
}
