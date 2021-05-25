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
	"github.com/cockroachdb/cockroach/pkg/util"
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
	return buildCascadeHelper(ctx, semaCtx, evalCtx, catalog, factoryI, func(b *Builder) memo.RelExpr {
		fk := cb.mutatedTable.InboundForeignKey(cb.fkInboundOrdinal)

		dep := opt.DepByID(fk.OriginTableID())
		b.checkPrivilege(dep, cb.childTable, privilege.DELETE)
		b.checkPrivilege(dep, cb.childTable, privilege.SELECT)

		var mb mutationBuilder
		mb.init(b, "delete", cb.childTable, tree.MakeUnqualifiedTableName(cb.childTable.Name()))

		// Build a semi join of the table with the mutation input.
		//
		// The scope returned by buildDeleteCascadeMutationInput has one column
		// for each public table column, making it appropriate to set it as
		// mb.fetchScope.
		mb.fetchScope = b.buildDeleteCascadeMutationInput(
			cb.childTable, &mb.alias, fk, binding, bindingProps, oldValues,
		)
		mb.outScope = mb.fetchScope

		// Set list of columns that will be fetched by the input expression.
		mb.setFetchColIDs(mb.outScope.cols)
		mb.buildDelete(nil /* returning */)
		return mb.outScope.expr
	})
}

// onDeleteFastCascadeBuilder is a memo.CascadeBuilder implementation for
// certain cases of ON DELETE CASCADE where we are deleting the entire table or
// where we can transfer a filter from the original statement instead of
// buffering the deleted rows.
//
// It provides a method to build the cascading delete in the child table,
// equivalent to a query like:
//
//   DELETE FROM child WHERE <condition on fk column> AND fk IS NOT NULL
//
// The input to the mutation is a Select on top of a Scan. For example:
//
//── delete child
//    ├── columns: <none>
//    ├── fetch columns: c:8 child.p:9
//    └── select
//         ├── columns: c:8!null child.p:9!null
//         ├── scan child
//         │    └── columns: c:8!null child.p:9!null
//         └── filters
//              ├── child.p:9 > 1
//              └── child.p:9 IS DISTINCT FROM CAST(NULL AS INT8)
//
// See testdata/fk-on-delete-cascades for more examples.
//
type onDeleteFastCascadeBuilder struct {
	mutatedTable cat.Table
	// fkInboundOrdinal is the ordinal of the inbound foreign key constraint on
	// the mutated table (can be passed to mutatedTable.InboundForeignKey).
	fkInboundOrdinal int
	childTable       cat.Table

	origFilters memo.FiltersExpr
	origFKCols  opt.ColList
}

var _ memo.CascadeBuilder = &onDeleteFastCascadeBuilder{}

// tryNewOnDeleteFastCascadeBuilder checks if the fast path cascade is
// applicable to the given mutation, and if yes it returns an instance of
// onDeleteFastCascadeBuilder.
func tryNewOnDeleteFastCascadeBuilder(
	ctx context.Context,
	md *opt.Metadata,
	catalog cat.Catalog,
	fk cat.ForeignKeyConstraint,
	fkInboundOrdinal int,
	parentTab, childTab cat.Table,
	mutationInputScope *scope,
) (_ *onDeleteFastCascadeBuilder, ok bool) {
	fkCols := make(opt.ColList, fk.ColumnCount())
	for i := range fkCols {
		tabOrd := fk.ReferencedColumnOrdinal(parentTab, i)
		fkCols[i] = mutationInputScope.getColumnForTableOrdinal(tabOrd).id
	}
	// Check that the input expression is a full table Scan or a Select on top of
	// a Scan where the filter references only FK columns and can be transferred
	// over to the child table.
	var scan *memo.ScanExpr
	var filters memo.FiltersExpr
	switch mutationInputScope.expr.Op() {
	case opt.SelectOp:
		sel := mutationInputScope.expr.(*memo.SelectExpr)
		if sel.Input.Op() != opt.ScanOp {
			return nil, false
		}
		var p props.Shared
		memo.BuildSharedProps(&sel.Filters, &p)
		if p.VolatilitySet.HasVolatile() {
			return nil, false
		}
		scan = sel.Input.(*memo.ScanExpr)
		if !p.OuterCols.SubsetOf(fkCols.ToSet()) {
			return nil, false
		}
		if memo.CanBeCompositeSensitive(md, &sel.Filters) {
			return nil, false
		}
		if sel.Relational().HasSubquery {
			return nil, false
		}
		filters = sel.Filters

	case opt.ScanOp:
		scan = mutationInputScope.expr.(*memo.ScanExpr)

	default:
		return nil, false
	}
	// Check that the scan retrieves all table data (currently, this should always
	// be the case in a normalized expression).
	if !scan.IsUnfiltered(md) {
		return nil, false
	}

	var visited util.FastIntSet
	parentTabID := parentTab.ID()
	childTabID := childTab.ID()

	// Check that inbound FK references form a simple tree.
	//
	// TODO(radu): we could allow multiple cascade paths to the same table if
	// there are no cycles. We could also analyze cycles more deeply to see if
	// they would in fact lead to a cascade loop. It's questionable if improving
	// these cases would be useful in practice.
	//
	// checkPaths returns false if any tables are reachable from tabID through
	// multiple inbound FK paths (or if there are cycles). It uses a recursive
	// depth-first search.
	var checkPaths func(tabID cat.StableID) bool
	checkPaths = func(tabID cat.StableID) bool {
		if visited.Contains(int(tabID)) {
			return false
		}
		visited.Add(int(tabID))

		var tab cat.Table
		// Avoid calling resolveTable for tables we already resolved.
		switch tabID {
		case parentTabID:
			tab = parentTab
		case childTabID:
			tab = childTab
		default:
			tab = resolveTable(ctx, catalog, tabID)
		}
		for i, n := 0, tab.InboundForeignKeyCount(); i < n; i++ {
			if !checkPaths(tab.InboundForeignKey(i).OriginTableID()) {
				return false
			}
		}
		return true
	}
	if !checkPaths(parentTabID) {
		return nil, false
	}

	return &onDeleteFastCascadeBuilder{
		mutatedTable:     parentTab,
		fkInboundOrdinal: fkInboundOrdinal,
		childTable:       childTab,
		origFilters:      filters,
		origFKCols:       fkCols,
	}, true
}

// Build is part of the memo.CascadeBuilder interface.
func (cb *onDeleteFastCascadeBuilder) Build(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	catalog cat.Catalog,
	factoryI interface{},
	_ opt.WithID,
	_ *props.Relational,
	_, _ opt.ColList,
) (_ memo.RelExpr, err error) {
	return buildCascadeHelper(ctx, semaCtx, evalCtx, catalog, factoryI, func(b *Builder) memo.RelExpr {
		fk := cb.mutatedTable.InboundForeignKey(cb.fkInboundOrdinal)

		dep := opt.DepByID(fk.OriginTableID())
		b.checkPrivilege(dep, cb.childTable, privilege.DELETE)
		b.checkPrivilege(dep, cb.childTable, privilege.SELECT)

		var mb mutationBuilder
		mb.init(b, "delete", cb.childTable, tree.MakeUnqualifiedTableName(cb.childTable.Name()))

		// Build the input to the delete mutation, which is simply a Scan with a
		// Select on top.
		mb.fetchScope = b.buildScan(
			b.addTable(cb.childTable, &mb.alias),
			tableOrdinals(cb.childTable, columnKinds{
				includeMutations:       false,
				includeSystem:          false,
				includeVirtualInverted: false,
				includeVirtualComputed: false,
			}),
			nil, /* indexFlags */
			noRowLocking,
			b.allocScope(),
		)
		mb.outScope = mb.fetchScope

		var filters memo.FiltersExpr

		// Build the filters by copying the original filters and replacing all
		// variable references.
		if len(cb.origFilters) > 0 {
			var replaceFn norm.ReplaceFunc
			replaceFn = func(e opt.Expr) opt.Expr {
				if v, ok := e.(*memo.VariableExpr); ok {
					idx, found := cb.origFKCols.Find(v.Col)
					if !found {
						panic(errors.AssertionFailedf("non-FK variable in filter"))
					}
					tabOrd := fk.OriginColumnOrdinal(cb.childTable, idx)
					col := mb.outScope.getColumnForTableOrdinal(tabOrd)
					return b.factory.ConstructVariable(col.id)
				}
				return b.factory.CopyAndReplaceDefault(e, replaceFn)
			}
			filters = *replaceFn(&cb.origFilters).(*memo.FiltersExpr)
		}

		// We have to filter out rows that have NULL values; add an IS NOT NULL
		// filter for each FK column, unless the column is not-nullable (this is
		// a minor optimization, as normalization rules would have removed the
		// filter anyway).
		notNullCols := mb.outScope.expr.Relational().NotNullCols
		for i := range cb.origFKCols {
			tabOrd := fk.OriginColumnOrdinal(cb.childTable, i)
			col := mb.outScope.getColumnForTableOrdinal(tabOrd)
			if !notNullCols.Contains(col.id) {
				filters = append(filters, b.factory.ConstructFiltersItem(
					b.factory.ConstructIsNot(
						b.factory.ConstructVariable(col.id),
						b.factory.ConstructNull(col.typ),
					),
				))
			}
		}

		if len(filters) > 0 {
			mb.outScope.expr = b.factory.ConstructSelect(mb.outScope.expr, filters)
		}

		// Set list of columns that will be fetched by the input expression.
		mb.setFetchColIDs(mb.outScope.cols)
		mb.buildDelete(nil /* returning */)
		return mb.outScope.expr
	})
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
	return buildCascadeHelper(ctx, semaCtx, evalCtx, catalog, factoryI, func(b *Builder) memo.RelExpr {
		fk := cb.mutatedTable.InboundForeignKey(cb.fkInboundOrdinal)

		dep := opt.DepByID(fk.OriginTableID())
		b.checkPrivilege(dep, cb.childTable, privilege.UPDATE)
		b.checkPrivilege(dep, cb.childTable, privilege.SELECT)

		var mb mutationBuilder
		mb.init(b, "update", cb.childTable, tree.MakeUnqualifiedTableName(cb.childTable.Name()))

		// Build a semi join of the table with the mutation input.
		//
		// The scope returned by buildDeleteCascadeMutationInput has one column
		// for each public table column, making it appropriate to set it as
		// mb.fetchScope.
		mb.fetchScope = b.buildDeleteCascadeMutationInput(
			cb.childTable, &mb.alias, fk, binding, bindingProps, oldValues,
		)
		mb.outScope = mb.fetchScope

		// Set list of columns that will be fetched by the input expression.
		mb.setFetchColIDs(mb.outScope.cols)
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
		return mb.outScope.expr
	})
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
	// We must fetch virtual computed columns for cascades that result in an
	// update to the child table. The execution engine requires that the fetch
	// columns are a superset of the update columns. See the related panic in
	// execFactory.ConstructUpdate.
	action := fk.DeleteReferenceAction()
	fetchVirtualComputedCols := action == tree.SetNull || action == tree.SetDefault

	outScope = b.buildScan(
		b.addTable(childTable, childTableAlias),
		tableOrdinals(childTable, columnKinds{
			includeMutations:       false,
			includeSystem:          false,
			includeVirtualInverted: false,
			includeVirtualComputed: fetchVirtualComputedCols,
		}),
		nil, /* indexFlags */
		noRowLocking,
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

	// Construct a dummy operator as the binding.
	md.AddWithBinding(binding, b.factory.ConstructFakeRel(&memo.FakeRelPrivate{
		Props: bindingProps,
	}))
	mutationInput := b.factory.ConstructWithScan(&memo.WithScanPrivate{
		With:    binding,
		InCols:  oldValues,
		OutCols: outCols,
		ID:      md.NextUniqueID(),
	})

	on := make(memo.FiltersExpr, numFKCols)
	for i := range on {
		tabOrd := fk.OriginColumnOrdinal(childTable, i)
		col := outScope.getColumnForTableOrdinal(tabOrd)
		on[i] = b.factory.ConstructFiltersItem(b.factory.ConstructEq(
			b.factory.ConstructVariable(col.id),
			b.factory.ConstructVariable(outCols[i]),
		))
	}
	outScope.expr = b.factory.ConstructSemiJoin(
		outScope.expr, mutationInput, on, memo.EmptyJoinPrivate,
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
	return buildCascadeHelper(ctx, semaCtx, evalCtx, catalog, factoryI, func(b *Builder) memo.RelExpr {
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
		mb.fetchScope = b.allocScope()
		mb.fetchScope.appendColumns(tableScopeCols)

		// Set list of columns that will be fetched by the input expression.
		mb.setFetchColIDs(tableScopeCols)
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
				// TODO(radu): This requires special code in addUpdateCols to
				// prevent this scopeColumn from being duplicated in mb.outScope
				// (see the addCol anonymous function in addUpdateCols). Find a
				// cleaner way to handle this.
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
		return mb.outScope.expr
	})
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
	// We must fetch virtual computed columns for cascades. The execution engine
	// requires that the fetch columns are a superset of the update columns. See
	// the related panic in execFactory.ConstructUpdate.
	outScope = b.buildScan(
		b.addTable(childTable, childTableAlias),
		tableOrdinals(childTable, columnKinds{
			includeMutations:       false,
			includeSystem:          false,
			includeVirtualInverted: false,
			includeVirtualComputed: true,
		}),
		nil, /* indexFlags */
		noRowLocking,
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

	md.AddWithBinding(binding, b.factory.ConstructFakeRel(&memo.FakeRelPrivate{
		Props: bindingProps,
	}))
	mutationInput := f.ConstructWithScan(&memo.WithScanPrivate{
		With:    binding,
		InCols:  append(oldValues[:len(oldValues):len(oldValues)], newValues...),
		OutCols: outCols,
		ID:      md.NextUniqueID(),
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
		col := outScope.getColumnForTableOrdinal(tabOrd)
		on[i] = f.ConstructFiltersItem(f.ConstructEq(
			f.ConstructVariable(col.id),
			f.ConstructVariable(outColsOld[i]),
		))
	}
	// This should conceptually be a semi-join, however we need to retain the "new
	// value" columns from the right-hand side. Because the FK cols form a key in
	// the parent table, there will be at most one match for any left row so an
	// inner join is equivalent.
	// Note that this is very similar to the UPDATE ... FROM syntax.
	outScope.expr = f.ConstructInnerJoin(
		outScope.expr, mutationInput, on, memo.EmptyJoinPrivate,
	)
	// Append the columns from the right-hand side to the scope.
	for i, col := range outCols {
		colMeta := md.ColumnMeta(col)
		ord := fk.OriginColumnOrdinal(childTable, i%numFKCols)
		c := childTable.Column(ord)
		outScope.cols = append(outScope.cols, scopeColumn{
			name: scopeColName(c.ColName()),
			id:   col,
			typ:  colMeta.Type,
		})
	}
	return outScope
}

// buildCascadeHelper contains boilerplate for CascadeBuilder.Build
// implementations. It creates a Builder, sets up panic-to-error conversion,
// and executes the given function.
func buildCascadeHelper(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *tree.EvalContext,
	catalog cat.Catalog,
	factoryI interface{},
	fn func(b *Builder) memo.RelExpr,
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

	return fn(b), nil
}
