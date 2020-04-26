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
)

// deleteCascadeBuilder is a memo.CascadeBuilder implementation for
// ON DELETE CASCADE.
//
// It provides a method to build the cascading delete in the child table,
// equivalent to a query like:
//
//   DELETE FROM child WHERE fk IN (SELECT fk FROM original_mutation_input)
//
// The expression that is built is a semi-join of the table with the mutation
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
type deleteCascadeBuilder struct {
	mutatedTable cat.Table
	// fkInboundOrdinal is the ordinal of the inbound foreign key constraint on
	// the mutated table (can be passed to mutatedTable.InboundForeignKey).
	fkInboundOrdinal int
	childTable       cat.Table
}

var _ memo.CascadeBuilder = &deleteCascadeBuilder{}

func newDeleteCascadeBuilder(
	mutatedTable cat.Table, fkInboundOrdinal int, childTable cat.Table,
) *deleteCascadeBuilder {
	return &deleteCascadeBuilder{
		mutatedTable:     mutatedTable,
		fkInboundOrdinal: fkInboundOrdinal,
		childTable:       childTable,
	}
}

// Build is part of the memo.CascadeBuilder interface.
func (cb *deleteCascadeBuilder) Build(
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
	md := factory.Metadata()
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

	// Build a semi join of the table with the mutation input. See the comment for
	// deleteCascadeBuilder.

	mb.outScope = b.buildScan(
		b.addTable(cb.childTable, &mb.alias),
		nil, /* ordinals */
		nil, /* indexFlags */
		noRowLocking,
		excludeMutations,
		b.allocScope(),
	)

	outCols := make(opt.ColList, len(oldValues))
	for i := range outCols {
		c := md.ColumnMeta(oldValues[i])
		outCols[i] = md.AddColumn(c.Alias, c.Type)
	}

	mutationInput := factory.ConstructWithScan(&memo.WithScanPrivate{
		With:         binding,
		InCols:       oldValues,
		OutCols:      outCols,
		BindingProps: bindingProps,
		ID:           md.NextUniqueID(),
	})

	on := make(memo.FiltersExpr, len(outCols))
	for i := range on {
		tabOrd := fk.OriginColumnOrdinal(cb.childTable, i)
		on[i] = factory.ConstructFiltersItem(factory.ConstructEq(
			factory.ConstructVariable(mb.scopeOrdToColID(scopeOrdinal(tabOrd))),
			factory.ConstructVariable(outCols[i]),
		))
	}

	mb.outScope.expr = factory.ConstructSemiJoin(
		mb.outScope.expr, mutationInput, on, &memo.JoinPrivate{},
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
