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
// See testdata/fk-cascades-delete for more examples.
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
	mb.outScope = b.buildCascadeMutationInput(
		cb.childTable, &mb.alias, fk, binding, bindingProps, oldValues,
	)

	// Set list of columns that will be fetched by the input expression.
	// Note that the columns were populated by the scan, but the semi-join returns
	// the same columns.
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
// See testdata/fk-cascades-set-null and fk-cascades-set-default for more
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
	b.checkPrivilege(dep, cb.childTable, privilege.DELETE)
	b.checkPrivilege(dep, cb.childTable, privilege.UPDATE)

	var mb mutationBuilder
	mb.init(b, "update", cb.childTable, tree.MakeUnqualifiedTableName(cb.childTable.Name()))

	// Build a semi join of the table with the mutation input.
	mb.outScope = b.buildCascadeMutationInput(
		cb.childTable, &mb.alias, fk, binding, bindingProps, oldValues,
	)

	// Set list of columns that will be fetched by the input expression.
	// Note that the columns were populated by the scan, but the semi-join returns
	// the same columns.
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

	mb.buildUpdate(nil /* returning */)
	return mb.outScope.expr, nil
}

// buildCascadeMutationInput constructs a semi-join between the child table and
// a WithScan operator, selecting the rows that need to be modified by a
// cascading action.
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
func (b *Builder) buildCascadeMutationInput(
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
