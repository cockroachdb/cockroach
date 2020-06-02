// Copyright 2020 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/errors"
)

// onDeleteCascadeBuilder is a memo.CascadeBuilder implementation for
// ON DELETE CASCADE.
//
// It provides a method to build the cascading delete in the child table,
// equivalent to a query like:
//
//   DELETE FROM child WHERE fk IN (SELECT fk FROM original_mutation_input)
//
// The input to the mutation is a semi-join of the table with the mutation
// input:
//
//   delete child
//    └── semi-join (hash)
//         ├── columns: c:5!null child.p:6!null
//         ├── scan child
//         │    └── columns: c:5!null child.p:6!null
//         ├── with-scan &1
//         │    ├── columns: p:7!null
//         │    └── mapping:
//         │         └──  parent.p:2 => p:7
//         └── filters
//              └── child.p:6 = p:7
//
// Note that NULL values in the mutation input don't require any special
// handling - they will be effectively ignored by the semi-join.
//
// See testdata/fk-on-delete-cascades for more examples.
//
type onDeleteCascadeBuilder struct {
	mutatedTable cat.Table
	// fkInboundOrdinal is the ordinal of the inbound foreign key constraint on
	// the mutated table (can be passed to mutatedTable.InboundForeignKey).
	fkInboundOrdinal int
	childTable       cat.Table
}

var _ memo.CascadeBuilder = &onDeleteCascadeBuilder{}

func newOnDeleteCascadeBuilder(
	mutatedTable cat.Table, fkInboundOrdinal int, childTable cat.Table,
) *onDeleteCascadeBuilder {
	return &onDeleteCascadeBuilder{
		mutatedTable:     mutatedTable,
		fkInboundOrdinal: fkInboundOrdinal,
		childTable:       childTable,
	}
}

// Build is part of the memo.CascadeBuilder interface.
func (cb *onDeleteCascadeBuilder) Build(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	catalog cat.Catalog,
	factoryI interface{},
	binding opt.WithID,
	bindingProps *props.Relational,
	oldValues, newValues opt.ColList,
) (_ memo.RelExpr, err error) {
	factory := factoryI.(*norm.Factory)
	b := New(ctx, semaCtx, evalCtx, catalog, factory, nil /* stmt */)

	// Enact panic handling similar to Builder.Build().
	defer func() {
		if r := recover(); r != nil {
			if ok, e := errorutil.ShouldCatch(r); ok {
				err = e
			} else {
				panic(r)
			}
		}
	}()

	fk := cb.mutatedTable.InboundForeignKey(cb.fkInboundOrdinal)

	dep := opt.DepByID(fk.OriginTableID())
	b.checkPrivilege(dep, cb.childTable, privilege.DELETE)
	b.checkPrivilege(dep, cb.childTable, privilege.SELECT)

	var mb mutationBuilder
	mb.init(b, "delete", cb.childTable, tree.MakeUnqualifiedTableName(cb.childTable.Name()))

	// Build a semi join of the table with the mutation input.
	mb.outScope = b.buildDeleteCascadeMutationInput(
		cb.childTable, &mb.alias, fk, binding, bindingProps, oldValues,
	)

	// Set list of columns that will be fetched by the input expression.
	for i := range mb.outScope.cols {
		mb.fetchOrds[i] = scopeOrdinal(i)
	}
	mb.buildDelete(nil /* returning */)
	return mb.outScope.expr, nil
}

// onDeleteSetBuilder is a memo.CascadeBuilder implementation for
// ON DELETE SET NULL and ON DELETE SET DEFAULT.
//
// It provides a method to build the cascading delete in the child table,
// equivalent to a query like:
//
//   UPDATE SET fk = NULL FROM child WHERE fk IN (SELECT fk FROM original_mutation_input)
//   or
//   UPDATE SET fk = DEFAULT FROM child WHERE fk IN (SELECT fk FROM original_mutation_input)
//
// The input to the mutation is a semi-join of the table with the mutation
// input:
//
//   update child
//    ├── columns: <none>
//    ├── fetch columns: c:5 child.p:6
//    ├── update-mapping:
//    │    └── column8:8 => child.p:4
//    └── project
//         ├── columns: column8:8 c:5!null child.p:6
//         ├── semi-join (hash)
//         │    ├── columns: c:5!null child.p:6
//         │    ├── scan child
//         │    │    └── columns: c:5!null child.p:6
//         │    ├── with-scan &1
//         │    │    ├── columns: p:7!null
//         │    │    └── mapping:
//         │    │         └──  parent.p:2 => p:7
//         │    └── filters
//         │         └── child.p:6 = p:7
//         └── projections
//                  └── NULL::INT8 [as=column8:8]
//
// Note that NULL values in the mutation input don't require any special
// handling - they will be effectively ignored by the semi-join.
//
// See testdata/fk-on-delete-set-null and fk-on-delete-set-default for more
// examples.
//
type onDeleteSetBuilder struct {
	mutatedTable cat.Table
	// fkInboundOrdinal is the ordinal of the inbound foreign key constraint on
	// the mutated table (can be passed to mutatedTable.InboundForeignKey).
	fkInboundOrdinal int
	childTable       cat.Table

	// action is either SetNull or SetDefault.
	action tree.ReferenceAction
}

var _ memo.CascadeBuilder = &onDeleteSetBuilder{}

func newOnDeleteSetBuilder(
	mutatedTable cat.Table, fkInboundOrdinal int, childTable cat.Table, action tree.ReferenceAction,
) *onDeleteSetBuilder {
	return &onDeleteSetBuilder{
		mutatedTable:     mutatedTable,
		fkInboundOrdinal: fkInboundOrdinal,
		childTable:       childTable,
		action:           action,
	}
}

// Build is part of the memo.CascadeBuilder interface.
func (cb *onDeleteSetBuilder) Build(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	catalog cat.Catalog,
	factoryI interface{},
	binding opt.WithID,
	bindingProps *props.Relational,
	oldValues, newValues opt.ColList,
) (_ memo.RelExpr, err error) {
	factory := factoryI.(*norm.Factory)
	b := New(ctx, semaCtx, evalCtx, catalog, factory, nil /* stmt */)

	// Enact panic handling similar to Builder.Build().
	defer func() {
		if r := recover(); r != nil {
			if ok, e := errorutil.ShouldCatch(r); ok {
				err = e
			} else {
				panic(r)
			}
		}
	}()

	fk := cb.mutatedTable.InboundForeignKey(cb.fkInboundOrdinal)

	dep := opt.DepByID(fk.OriginTableID())
	b.checkPrivilege(dep, cb.childTable, privilege.UPDATE)
	b.checkPrivilege(dep, cb.childTable, privilege.SELECT)

	var mb mutationBuilder
	mb.init(b, "update", cb.childTable, tree.MakeUnqualifiedTableName(cb.childTable.Name()))

	// Build a semi join of the table with the mutation input.
	mb.outScope = b.buildDeleteCascadeMutationInput(
		cb.childTable, &mb.alias, fk, binding, bindingProps, oldValues,
	)

	// Set list of columns that will be fetched by the input expression.
	for i := range mb.outScope.cols {
		mb.fetchOrds[i] = scopeOrdinal(i)
	}
	// Add target columns.
	numFKCols := fk.ColumnCount()
	for i := 0; i < numFKCols; i++ {
		tabOrd := fk.OriginColumnOrdinal(cb.childTable, i)
		mb.addTargetCol(tabOrd)
	}

	// Add the SET expressions.
	updateExprs := make(tree.UpdateExprs, numFKCols)
	for i := range updateExprs {
		updateExprs[i] = &tree.UpdateExpr{}
		if cb.action == tree.SetNull {
			updateExprs[i].Expr = tree.DNull
		} else {
			updateExprs[i].Expr = tree.DefaultVal{}
		}
	}
	mb.addUpdateCols(updateExprs)

	// TODO(radu): consider plumbing a flag to prevent building the FK check
	// against the parent we are cascading from. Need to investigate in which
	// cases this is safe (e.g. other cascades could have messed with the parent
	// table in the meantime).
	mb.buildUpdate(nil /* returning */)
	return mb.outScope.expr, nil
}

// buildDeleteCascadeMutationInput constructs a semi-join between the child
// table and a WithScan operator, selecting the rows that need to be modified by
// a cascading action.
//
// The WithScan columns that correspond to the FK columns are specified in
// oldValues.
//
// The returned scope has one column for each public table column.
//
// For example, if we have a child table with foreign key on p, the expression
// will look like this:
//
//   semi-join (hash)
//    ├── columns: c:5!null child.p:6!null
//    ├── scan child
//    │    └── columns: c:5!null child.p:6!null
//    ├── with-scan &1
//    │    ├── columns: p:7!null
//    │    └── mapping:
//    │         └──  parent.p:2 => p:7
//    └── filters
//         └── child.p:6 = p:7
//
// Note that NULL values in the mutation input don't require any special
// handling - they will be effectively ignored by the semi-join.
//
func (b *Builder) buildDeleteCascadeMutationInput(
	childTable cat.Table,
	childTableAlias *tree.TableName,
	fk cat.ForeignKeyConstraint,
	binding opt.WithID,
	bindingProps *props.Relational,
	oldValues opt.ColList,
) (outScope *scope) {
	outScope = b.buildScan(
		b.addTable(childTable, childTableAlias),
		nil, /* ordinals */
		nil, /* indexFlags */
		noRowLocking,
		excludeMutations,
		b.allocScope(),
	)

	numFKCols := fk.ColumnCount()
	if len(oldValues) != numFKCols {
		panic(errors.AssertionFailedf(
			"expected %d oldValues columns, got %d", numFKCols, len(oldValues),
		))
	}

	md := b.factory.Metadata()
	outCols := make(opt.ColList, numFKCols)
	for i := range outCols {
		c := md.ColumnMeta(oldValues[i])
		outCols[i] = md.AddColumn(c.Alias, c.Type)
	}

	mutationInput := b.factory.ConstructWithScan(&memo.WithScanPrivate{
		With:         binding,
		InCols:       oldValues,
		OutCols:      outCols,
		BindingProps: bindingProps,
		ID:           md.NextUniqueID(),
	})

	on := make(memo.FiltersExpr, numFKCols)
	for i := range on {
		tabOrd := fk.OriginColumnOrdinal(childTable, i)
		on[i] = b.factory.ConstructFiltersItem(b.factory.ConstructEq(
			b.factory.ConstructVariable(outScope.cols[tabOrd].id),
			b.factory.ConstructVariable(outCols[i]),
		))
	}
	outScope.expr = b.factory.ConstructSemiJoin(
		outScope.expr, mutationInput, on, &memo.JoinPrivate{},
	)
	return outScope
}

// onUpdateCascadeBuilder is a memo.CascadeBuilder implementation for
// ON UPDATE CASCADE / SET NULL / SET DEFAULT.
//
// It provides a method to build the cascading update in the child table,
// equivalent to a query like:
//
//   UPDATE child SET fk = fk_new_val
//   FROM (SELECT fk_old_val, fk_new_val FROM original_mutation_input)
//   WHERE fk_old_val IS DISTINCT FROM fk_new_val AND fk = fk_old_val
//
// The input to the mutation is an inner-join of the table with the mutation
// input, producing the old and new FK values for each row:
//
//   update child
//    ├── columns: <none>
//    ├── fetch columns: c:6 child.p:7
//    ├── update-mapping:
//    │    └── p_new:9 => child.p:5
//    ├── input binding: &2
//    └─── inner-join (hash)
//         ├── columns: c:6!null child.p:7!null p:8!null p_new:9!null
//         ├── scan child
//         │    └── columns: c:6!null child.p:7!null
//         ├── select
//         │    ├── columns: p:8!null p_new:9!null
//         │    ├── with-scan &1
//         │    │    ├── columns: p:8!null p_new:9!null
//         │    │    └── mapping:
//         │    │         ├──  parent.p:2 => p:8
//         │    │         └──  p_new:3 => p_new:9
//         │    └── filters
//         │         └── p:8 IS DISTINCT FROM p_new:9
//         └── filters
//              └── child.p:7 = p:8
//
// The inner join equality columns form a key in the with-scan (because they
// form a key in the parent table); so the inner-join is essentially equivalent
// to a semi-join, except that it augments the rows with other columns.
//
// Note that NULL "old" values in the mutation input don't require any special
// handling - they will be effectively ignored by the join.
//
// See testdata/fk-on-update-* for more examples.
//
type onUpdateCascadeBuilder struct {
	mutatedTable cat.Table
	// fkInboundOrdinal is the ordinal of the inbound foreign key constraint on
	// the mutated table (can be passed to mutatedTable.InboundForeignKey).
	fkInboundOrdinal int
	childTable       cat.Table

	action tree.ReferenceAction
}

var _ memo.CascadeBuilder = &onUpdateCascadeBuilder{}

func newOnUpdateCascadeBuilder(
	mutatedTable cat.Table, fkInboundOrdinal int, childTable cat.Table, action tree.ReferenceAction,
) *onUpdateCascadeBuilder {
	return &onUpdateCascadeBuilder{
		mutatedTable:     mutatedTable,
		fkInboundOrdinal: fkInboundOrdinal,
		childTable:       childTable,
		action:           action,
	}
}

// Build is part of the memo.CascadeBuilder interface.
func (cb *onUpdateCascadeBuilder) Build(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	catalog cat.Catalog,
	factoryI interface{},
	binding opt.WithID,
	bindingProps *props.Relational,
	oldValues, newValues opt.ColList,
) (_ memo.RelExpr, err error) {
	factory := factoryI.(*norm.Factory)
	b := New(ctx, semaCtx, evalCtx, catalog, factory, nil /* stmt */)

	// Enact panic handling similar to Builder.Build().
	defer func() {
		if r := recover(); r != nil {
			if ok, e := errorutil.ShouldCatch(r); ok {
				err = e
			} else {
				panic(r)
			}
		}
	}()

	fk := cb.mutatedTable.InboundForeignKey(cb.fkInboundOrdinal)

	dep := opt.DepByID(fk.OriginTableID())
	b.checkPrivilege(dep, cb.childTable, privilege.UPDATE)
	b.checkPrivilege(dep, cb.childTable, privilege.SELECT)

	var mb mutationBuilder
	mb.init(b, "update", cb.childTable, tree.MakeUnqualifiedTableName(cb.childTable.Name()))

	// Build a join of the table with the mutation input.
	mb.outScope = b.buildUpdateCascadeMutationInput(
		cb.childTable, &mb.alias, fk, binding, bindingProps, oldValues, newValues,
	)

	// The scope created by b.buildUpdateCascadeMutationInput has the table
	// columns, followed by the old FK values, followed by the new FK values.
	numFKCols := fk.ColumnCount()
	tableScopeCols := mb.outScope.cols[:len(mb.outScope.cols)-2*numFKCols]
	newValScopeCols := mb.outScope.cols[len(mb.outScope.cols)-numFKCols:]

	// Set list of columns that will be fetched by the input expression.
	for i := range tableScopeCols {
		mb.fetchOrds[i] = scopeOrdinal(i)
	}
	// Add target columns.
	for i := 0; i < numFKCols; i++ {
		tabOrd := fk.OriginColumnOrdinal(cb.childTable, i)
		mb.addTargetCol(tabOrd)
	}

	// Add the SET expressions.
	updateExprs := make(tree.UpdateExprs, numFKCols)
	for i := range updateExprs {
		updateExprs[i] = &tree.UpdateExpr{}
		switch cb.action {
		case tree.Cascade:
			updateExprs[i].Expr = &newValScopeCols[i]
		case tree.SetNull:
			updateExprs[i].Expr = tree.DNull
		case tree.SetDefault:
			updateExprs[i].Expr = tree.DefaultVal{}
		default:
			panic(errors.AssertionFailedf("unsupported action"))
		}
	}
	mb.addUpdateCols(updateExprs)

	mb.buildUpdate(nil /* returning */)
	return mb.outScope.expr, nil
}

// buildUpdateCascadeMutationInput constructs an inner-join between the child
// table and a WithScan operator, selecting the rows that need to be modified by
// a cascading action and augmenting them with the old and new FK values.
//
// For example, if we have a child table with foreign key on p, the expression
// will look like this:
//
//   inner-join (hash)
//    ├── columns: c:6!null child.p:7!null p:8!null p_new:9!null
//    ├── scan child
//    │    └── columns: c:6!null child.p:7!null
//    ├── select
//    │    ├── columns: p:8!null p_new:9!null
//    │    ├── with-scan &1
//    │    │    ├── columns: p:8!null p_new:9!null
//    │    │    └── mapping:
//    │    │         ├──  parent.p:2 => p:8
//    │    │         └──  p_new:3 => p_new:9
//    │    └── filters
//    │         └── p:8 IS DISTINCT FROM p_new:9
//    └── filters
//         └── child.p:7 = p:8
//
// The inner join equality columns form a key in the with-scan (because they
// form a key in the parent table); so the inner-join is essentially equivalent
// to a semi-join, except that it augments the rows with other columns.
//
// Note that NULL old values in the mutation input don't require any special
// handling - they will be effectively ignored by the inner-join.
//
// The WithScan columns that correspond to the FK columns are specified in
// oldValues and newValues.
//
// The returned scope has one column for each public table column, followed by
// the columns that contain the old FK values, followed by the columns that
// contain the new FK values.
//
// Note that for Upserts we need to only perform actions for rows that
// correspond to updates (and not inserts). Normally these would be selected by
// a "canaryCol IS NOT NULL" filters. However, that is not necessary because for
// inserted rows the "old" values are all NULL and won't match anything in the
// inner-join anyway. This reasoning is very similar to that of FK checks for
// Upserts (see buildFKChecksForUpsert).
//
func (b *Builder) buildUpdateCascadeMutationInput(
	childTable cat.Table,
	childTableAlias *tree.TableName,
	fk cat.ForeignKeyConstraint,
	binding opt.WithID,
	bindingProps *props.Relational,
	oldValues opt.ColList,
	newValues opt.ColList,
) (outScope *scope) {
	outScope = b.buildScan(
		b.addTable(childTable, childTableAlias),
		nil, /* ordinals */
		nil, /* indexFlags */
		noRowLocking,
		excludeMutations,
		b.allocScope(),
	)

	numFKCols := fk.ColumnCount()
	if len(oldValues) != numFKCols || len(newValues) != numFKCols {
		panic(errors.AssertionFailedf(
			"expected %d oldValues/newValues columns, got %d/%d", numFKCols, len(oldValues), len(newValues),
		))
	}

	f := b.factory
	md := f.Metadata()
	outCols := make(opt.ColList, numFKCols*2)
	outColsOld := outCols[:numFKCols]
	outColsNew := outCols[numFKCols:]
	for i := range outColsOld {
		c := md.ColumnMeta(oldValues[i])
		outColsOld[i] = md.AddColumn(c.Alias, c.Type)
	}
	for i := range outColsNew {
		c := md.ColumnMeta(newValues[i])
		outColsNew[i] = md.AddColumn(c.Alias, c.Type)
	}

	mutationInput := f.ConstructWithScan(&memo.WithScanPrivate{
		With:         binding,
		InCols:       append(oldValues[:len(oldValues):len(oldValues)], newValues...),
		OutCols:      outCols,
		BindingProps: bindingProps,
		ID:           md.NextUniqueID(),
	})

	// Filter out rows where the new values are the same as the old values. This
	// is necessary for ON UPDATE SET NULL / SET DEFAULT where we don't want the
	// action to take place if the update is a no-op. It is also important to
	// avoid infinite cascade cycles; for example:
	//   CREATE TABLE self (a INT UNIQUE REFERENCES self(a) ON UPDATE CASCADE);
	//   INSERT INTO self VALUES (1);
	//   UPDATE SELF SET a = 1 WHERE true;
	//
	// In order to perform the filtering, we use IsNot. We don't use Ne because we
	// want the action to take place when the old value is not NULL and the new
	// value is NULL.
	//
	// Note that IsNot (i.e. IS DISTINCT FROM) is false when the values are
	// composite and they are equal but not identical (e.g. 1.0 vs 1.00). It's
	// debatable what the right thing to do is. Postgres seems to use stricter
	// equality:
	//
	//   CREATE TABLE parent (p NUMERIC PRIMARY KEY);
	//   CREATE TABLE child (p NUMERIC REFERENCES parent(p) ON UPDATE SET NULL);
	//   INSERT INTO parent VALUES (1.000);
	//   INSERT INTO child VALUES (1.0);
	//
	//   UPDATE parent SET p = 1.000;
	//   SELECT * FROM child;
	//     p
	//   -----
	//    1.0   <-- action did not take place
	//   (1 row)
	//
	//   UPDATE parent SET p = 1.00;
	//   SELECT * FROM child;
	//    p
	//   ---
	//          <-- action took place
	//   (1 row)
	//
	// If we want to implement the same semantics, we would need an operator that
	// is like IsNot but implements a stricter condition.
	//
	var condition opt.ScalarExpr
	for i := 0; i < numFKCols; i++ {
		isNot := f.ConstructIsNot(
			f.ConstructVariable(outColsOld[i]),
			f.ConstructVariable(outColsNew[i]),
		)
		if condition == nil {
			condition = isNot
		} else {
			condition = f.ConstructOr(condition, isNot)
		}
	}
	mutationInput = f.ConstructSelect(
		mutationInput,
		memo.FiltersExpr{f.ConstructFiltersItem(condition)},
	)

	on := make(memo.FiltersExpr, numFKCols)
	for i := range on {
		tabOrd := fk.OriginColumnOrdinal(childTable, i)
		on[i] = f.ConstructFiltersItem(f.ConstructEq(
			f.ConstructVariable(outScope.cols[tabOrd].id),
			f.ConstructVariable(outColsOld[i]),
		))
	}
	// This should conceptually be a semi-join, however we need to retain the "new
	// value" columns from the right-hand side. Because the FK cols form a key in
	// the parent table, there will be at most one match for any left row so an
	// inner join is equivalent.
	// Note that this is very similar to the UPDATE ... FROM syntax.
	outScope.expr = f.ConstructInnerJoin(
		outScope.expr, mutationInput, on, &memo.JoinPrivate{},
	)
	// Append the columns from the right-hand side to the scope.
	for _, col := range outCols {
		colMeta := md.ColumnMeta(col)
		outScope.cols = append(outScope.cols, scopeColumn{
			name: tree.Name(colMeta.Alias),
			id:   col,
			typ:  colMeta.Type,
		})
	}
	return outScope
}
