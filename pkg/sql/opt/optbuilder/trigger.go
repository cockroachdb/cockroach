// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optbuilder

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	plpgsql "github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// ============================================================================
// Row-level BEFORE triggers
// ============================================================================

// buildRowLevelBeforeTriggers builds any applicable row-level BEFORE triggers
// based on the event type. It returns true if triggers were built, and false
// otherwise.
func (mb *mutationBuilder) buildRowLevelBeforeTriggers(
	eventType tree.TriggerEventType, cascade bool,
) bool {
	var eventsToMatch tree.TriggerEventTypeSet
	eventsToMatch.Add(eventType)
	triggers := cat.GetRowLevelTriggers(mb.tab, tree.TriggerActionTimeBefore, eventsToMatch)
	if len(triggers) == 0 {
		return false
	}

	typeID := typedesc.TableIDToImplicitTypeOID(descpb.ID(mb.tab.ID()))
	tableTyp, err := mb.b.semaCtx.TypeResolver.ResolveTypeByOID(mb.b.ctx, typeID)
	if err != nil {
		panic(err)
	}

	// Create a mapping from the set of visible columns to their ordinals in the
	// table.
	visibleColOrds := make([]int, 0, mb.tab.ColumnCount())
	for i := 0; i < mb.tab.ColumnCount(); i++ {
		if mb.tab.Column(i).Visibility() == cat.Visible {
			visibleColOrds = append(visibleColOrds, i)
		}
	}
	if len(visibleColOrds) != len(tableTyp.TupleContents()) {
		panic(errors.AssertionFailedf("unexpected number of visible columns"))
	}

	// Keep track of the original columns, so we can remove additional columns at
	// the end. Separately track the updatable set of pass-through columns that
	// should be maintained while triggers are being built.
	triggerScope := mb.outScope.push()
	triggerScope.expr = mb.outScope.expr
	triggerScope.appendColumnsFromScope(mb.outScope)

	// Project the OLD and NEW columns, which provide access to the values of the
	// row before and after the mutation, respectively.
	oldColID, newColID := mb.buildOldAndNewCols(triggerScope, eventType, tableTyp, visibleColOrds)

	// Build each trigger function invocation in order, applying optimization
	// barriers to ensure correct evaluation order.
	f := mb.b.factory
	canModifyRows := true
	for i := range triggers {
		trigger := triggers[i]
		triggerScope.expr = f.ConstructBarrier(triggerScope.expr)

		// Resolve the trigger function and build the invocation.
		args := mb.buildTriggerFunctionArgs(trigger, eventType, oldColID, newColID)
		triggerFn, def := mb.b.buildTriggerFunction(triggers[i], mb.tab.ID(), tableTyp, args)

		// If there is a WHEN condition, wrap the trigger function invocation in a
		// CASE WHEN statement that checks the WHEN condition.
		if trigger.WhenExpr() != "" {
			elseColID := newColID
			if eventType == tree.TriggerEventDelete {
				elseColID = oldColID
			}
			triggerFn = mb.b.buildTriggerWhen(
				trigger, triggerScope, oldColID, newColID, triggerFn, f.ConstructVariable(elseColID),
			)
		}

		// For UPSERT and INSERT ON CONFLICT, UPDATE triggers should only fire for the
		// conflicting rows, which are identified by the canary column.
		if mb.canaryColID != 0 && eventType == tree.TriggerEventUpdate {
			canaryCol := f.ConstructVariable(mb.canaryColID)
			isUpdateCond := f.ConstructIsNot(canaryCol, memo.NullSingleton)
			triggerFn = f.ConstructCase(
				memo.TrueSingleton,
				memo.ScalarListExpr{f.ConstructWhen(isUpdateCond, triggerFn)},
				f.ConstructVariable(newColID),
			)
		}

		// Finally, project a column that invokes the trigger function.
		triggerFnColID := mb.b.projectColWithMetadataName(triggerScope, def.Name, tableTyp, triggerFn)

		// Don't allow the trigger to modify or filter the row if the mutation is
		// for a cascade, unless the session variable is set to allow it.
		if cascade && !mb.b.evalCtx.SessionData().UnsafeAllowTriggersModifyingCascades {
			mb.ensureNoRowsModifiedByTrigger(
				triggerScope, triggers[i].Name(), eventType, triggerFnColID, oldColID, newColID,
			)
			canModifyRows = false
		}

		// BEFORE triggers can return a NULL value to indicate that the row should
		// be skipped.
		mb.applyFilterFromTrigger(triggerScope, triggerFnColID)

		// For INSERT and UPDATE triggers, the NEW column takes on the result of the
		// trigger function. This allows subsequent trigger functions to see the
		// modified row.
		if eventType == tree.TriggerEventInsert || eventType == tree.TriggerEventUpdate {
			newColID = triggerFnColID
		}
	}
	triggerScope.expr = f.ConstructBarrier(triggerScope.expr)

	// INSERT and UPDATE triggers can modify the row to be inserted or updated
	// via the return value of the trigger function.
	if eventType == tree.TriggerEventInsert || eventType == tree.TriggerEventUpdate {
		// If the trigger cannot modify rows, avoid changing the mutation columns.
		// This is necessary to avoid adding extra checks during cascades, which
		// could cause spurious constraint-violation errors.
		//
		// For example, in a "diamond" cascade pattern, an update to one table
		// cascades to two others, which both cascade to a single grandchild table.
		// Once both cascades complete, the database is in a consistent state. If a
		// spurious check runs in between the two cascades, it could observe a
		// constraint violation.
		if canModifyRows {
			mb.applyChangesFromTriggers(triggerScope, eventType, tableTyp, visibleColOrds, newColID)
		}
	}
	mb.outScope = triggerScope

	// Since INSERT and UPDATE triggers can modify the row, we need to recompute
	// the computed columns.
	if eventType == tree.TriggerEventInsert || eventType == tree.TriggerEventUpdate {
		mb.recomputeComputedColsForTrigger(eventType)
	}
	return true
}

// buildOldAndNewCols builds the OLD and NEW column tuples for a row-level
// BEFORE trigger, if applicable. The OLD tuple contains the original values of
// the columns being updated or deleted, and the NEW tuple contains the new
// values of the columns being updated or inserted.
func (mb *mutationBuilder) buildOldAndNewCols(
	triggerScope *scope, eventType tree.TriggerEventType, tableTyp *types.T, visibleColOrds []int,
) (oldColID, newColID opt.ColumnID) {
	f := mb.b.factory
	makeTuple := func(colIDs, backupColIDs opt.OptionalColList, name string) opt.ColumnID {
		elems := make(memo.ScalarListExpr, 0, len(visibleColOrds))
		for _, i := range visibleColOrds {
			if mb.tab.Column(i).IsComputed() {
				// Row-level triggers should not observe computed columns.
				elems = append(elems, f.ConstructNull(mb.tab.Column(i).DatumType()))
				continue
			}
			col := colIDs[i]
			if col == 0 && backupColIDs != nil {
				col = backupColIDs[i]
			}
			if col == 0 {
				panic(errors.AssertionFailedf("missing column for trigger"))
			}
			elems = append(elems, f.ConstructVariable(col))
		}
		tup := f.ConstructTuple(elems, tableTyp)
		return mb.b.projectColWithMetadataName(triggerScope, name, tableTyp, tup)
	}
	if eventType == tree.TriggerEventUpdate || eventType == tree.TriggerEventDelete {
		oldColID = makeTuple(mb.fetchColIDs, nil /* backupColIDs */, triggerColOld)
	}
	if eventType == tree.TriggerEventInsert {
		newColID = makeTuple(mb.insertColIDs, mb.fetchColIDs, triggerColNew)
	} else if eventType == tree.TriggerEventUpdate {
		newColID = makeTuple(mb.updateColIDs, mb.fetchColIDs, triggerColNew)
	}
	return oldColID, newColID
}

// buildTriggerFunctionArgs builds the set of arguments that should be passed to
// the trigger function.
func (mb *mutationBuilder) buildTriggerFunctionArgs(
	trigger cat.Trigger, eventType tree.TriggerEventType, oldColID, newColID opt.ColumnID,
) memo.ScalarListExpr {
	f := mb.b.factory
	tgNew := opt.ScalarExpr(memo.NullSingleton)
	if newColID != 0 {
		tgNew = f.ConstructVariable(newColID)
	}
	tgOld := opt.ScalarExpr(memo.NullSingleton)
	if oldColID != 0 {
		tgOld = f.ConstructVariable(oldColID)
	}
	tgName := tree.NewDName(string(trigger.Name()))
	tgWhen := tree.NewDString("BEFORE")
	tgLevel := tree.NewDString("ROW")
	tgOp := tree.NewDString(eventType.String())
	tgRelID := tree.NewDOid(oid.Oid(mb.tab.ID()))
	tgTableName := tree.NewDString(string(mb.tab.Name()))
	fqName, err := mb.b.catalog.FullyQualifiedName(mb.b.ctx, mb.tab)
	if err != nil {
		panic(err)
	}
	tgTableSchema := tree.NewDString(fqName.Schema())
	tgNumArgs := tree.NewDInt(tree.DInt(len(trigger.FuncArgs())))
	tgArgV := tree.NewDArray(types.String)
	for _, arg := range trigger.FuncArgs() {
		err = tgArgV.Append(arg)
		if err != nil {
			panic(err)
		}
	}
	return memo.ScalarListExpr{
		tgNew,                                   // NEW
		tgOld,                                   // OLD
		f.ConstructConstVal(tgName, types.Name), // TG_NAME
		f.ConstructConstVal(tgWhen, types.String),        // TG_WHEN
		f.ConstructConstVal(tgLevel, types.String),       // TG_LEVEL
		f.ConstructConstVal(tgOp, types.String),          // TG_OP
		f.ConstructConstVal(tgRelID, types.Oid),          // TG_RELIID
		f.ConstructConstVal(tgTableName, types.String),   // TG_RELNAME
		f.ConstructConstVal(tgTableName, types.String),   // TG_TABLE_NAME
		f.ConstructConstVal(tgTableSchema, types.String), // TG_TABLE_SCHEMA
		f.ConstructConstVal(tgNumArgs, types.Int),        // TG_NARGS
		f.ConstructConstVal(tgArgV, types.StringArray),   // TG_ARGV
	}
}

// applyFilterFromTrigger adds a filter to the expression in triggerScope that
// removes rows for which the trigger returns NULL.
func (mb *mutationBuilder) applyFilterFromTrigger(
	triggerScope *scope, triggerFnColID opt.ColumnID,
) {
	f := mb.b.factory
	filter := f.ConstructIsNot(f.ConstructVariable(triggerFnColID), memo.NullSingleton)
	triggerScope.expr = f.ConstructSelect(triggerScope.expr, memo.FiltersExpr{f.ConstructFiltersItem(filter)})
}

// applyChangesFromTriggers updates triggerScope and mutationBuilder to reflect
// changes made by row-level BEFORE triggers. It updates triggerScope to project
// new column values and mutationBuilder to track the new column IDs. Note that
// applyChangesFromTriggers is only valid for INSERT and UPDATE triggers, since
// DELETE triggers cannot modify the row.
func (mb *mutationBuilder) applyChangesFromTriggers(
	triggerScope *scope,
	eventType tree.TriggerEventType,
	tableTyp *types.T,
	visibleColOrds []int,
	newColID opt.ColumnID,
) {
	if eventType != tree.TriggerEventInsert && eventType != tree.TriggerEventUpdate {
		panic(errors.AssertionFailedf("unexpected trigger event type: %v", eventType))
	}
	if newColID == 0 {
		panic(errors.AssertionFailedf("missing NEW column for trigger"))
	}
	f := mb.b.factory
	passThroughCols := triggerScope.colSet()
	projections := make(memo.ProjectionsExpr, 0, len(tableTyp.TupleContents()))
	for i, colTyp := range tableTyp.TupleContents() {
		c := mb.tab.Column(visibleColOrds[i])
		if c.IsComputed() {
			// Computed columns are not modified by triggers.
			continue
		}
		colNameForScope := scopeColName(c.ColName()).WithMetadataName(string(c.ColName()) + "_new")
		elem := f.ConstructColumnAccess(f.ConstructVariable(newColID), memo.TupleOrdinal(i))
		elemCol := mb.b.synthesizeColumn(triggerScope, colNameForScope, colTyp, nil /* expr */, elem)
		if eventType == tree.TriggerEventInsert {
			if existing := triggerScope.getColumn(mb.insertColIDs[visibleColOrds[i]]); existing != nil {
				// Clear the name of the previous INSERT columns, so that the
				// replacements will be resolved instead when referenced via the
				// special "excluded" data source.
				existing.clearName()
			}
			mb.insertColIDs[visibleColOrds[i]] = elemCol.id
			elemCol.table = excludedTableName
		} else {
			mb.updateColIDs[visibleColOrds[i]] = elemCol.id
		}
		projections = append(projections, f.ConstructProjectionsItem(elem, elemCol.id))
	}
	triggerScope.expr = f.ConstructProject(triggerScope.expr, projections, passThroughCols)
}

// ensureNoRowsModifiedByTrigger adds a runtime check to the scope that ensures
// that the trigger function does not modify or filter any rows. This is
// necessary for cascading operations, where modifications made by the trigger
// function could cause constraint violations.
func (mb *mutationBuilder) ensureNoRowsModifiedByTrigger(
	triggerScope *scope,
	triggerName tree.Name,
	eventType tree.TriggerEventType,
	triggerFnColID opt.ColumnID,
	oldColID, newColID opt.ColumnID,
) {
	makeConstStr := func(str string) opt.ScalarExpr {
		return mb.b.factory.ConstructConstVal(tree.NewDString(str), types.String)
	}
	expectedColID := newColID
	if eventType == tree.TriggerEventDelete {
		expectedColID = oldColID
	}
	// Construct a call to crdb_internal.plpgsql_raise with the error message.
	severity := makeConstStr("ERROR")
	message := mb.b.factory.ConstructConcat(
		makeConstStr(fmt.Sprintf(
			"trigger %s attempted to modify or filter a row in a cascade operation: ", triggerName)),
		mb.b.factory.ConstructCast(mb.b.factory.ConstructVariable(expectedColID), types.String),
	)
	detail := makeConstStr("changing the rows updated or deleted by a foreign-key cascade\n" +
		" can cause constraint violations, and therefore is not allowed")
	hint := makeConstStr("to enable this behavior (with risk of constraint violation), set\n" +
		"the session variable 'unsafe_allow_triggers_modifying_cascades' to true")
	code := makeConstStr(pgcode.TriggeredDataChangeViolation.String())
	raiseFn := mb.b.makePLpgSQLRaiseFn(memo.ScalarListExpr{severity, message, detail, hint, code})

	// Build a CASE statement to raise the error if the trigger function modified
	// the row. Add a barrier to ensure the check isn't removed or re-ordered with
	// a filter.
	//
	// TODO(#133787): consider relaxing this check, for example, to ignore updates
	// to non-FK columns.
	f := mb.b.factory
	check := f.ConstructCase(memo.TrueSingleton,
		memo.ScalarListExpr{
			f.ConstructWhen(
				f.ConstructIsNot(f.ConstructVariable(triggerFnColID), f.ConstructVariable(expectedColID)),
				raiseFn,
			),
		},
		f.ConstructNull(types.Int),
	)
	mb.b.projectColWithMetadataName(triggerScope, "check-rows", types.Int, check)
	triggerScope.expr = f.ConstructBarrier(triggerScope.expr)
}

// recomputeComputedColsForTrigger resets all computed columns and builds new
// expressions for them using the remaining columns. this is used for
// re-computing computed columns after a row-level trigger has modified the row.
func (mb *mutationBuilder) recomputeComputedColsForTrigger(eventType tree.TriggerEventType) {
	colIDs := mb.insertColIDs
	if eventType == tree.TriggerEventUpdate {
		colIDs = mb.updateColIDs
	}
	for i := range colIDs {
		if mb.tab.Column(i).IsComputed() {
			colIDs[i] = 0
		}
	}
	mb.addSynthesizedComputedCols(colIDs, false /* restrict */)
}

// ============================================================================
// Row-level AFTER triggers
// ============================================================================

// buildRowLevelAfterTriggers builds any applicable row-level AFTER triggers
// based on the mutation operator. Since AFTER triggers are a form of
// post-query, they are stored on mutationBuilder instead of being projected as
// part of the mutation input.
//
// NOTE: buildRowLevelAfterTriggers doesn't actually build the expression that
// calls the trigger functions. Instead, it stores the information needed to do
// so after the mutation executes.
func (mb *mutationBuilder) buildRowLevelAfterTriggers(mutation opt.Operator) {
	eventsToMatch := mb.getEventsToMatchForMutation(mutation)
	triggers := cat.GetRowLevelTriggers(mb.tab, tree.TriggerActionTimeAfter, eventsToMatch)
	if len(triggers) == 0 {
		return
	}
	mb.ensureWithID()

	var visibleColOrds intsets.Fast
	for i := 0; i < mb.tab.ColumnCount(); i++ {
		if mb.tab.Column(i).Visibility() == cat.Visible {
			visibleColOrds.Add(i)
		}
	}

	var fetchCols opt.ColList
	if mutation == opt.DeleteOp || mutation == opt.UpdateOp || mb.canaryColID != 0 {
		// For DELETE, UPDATE, and UPSERT/ON CONFLICT, we need to provide the old
		// values for each row.
		fetchCols = make(opt.ColList, 0, visibleColOrds.Len())
		for i, ok := visibleColOrds.Next(0); ok; i, ok = visibleColOrds.Next(i + 1) {
			if mb.fetchColIDs[i] == 0 {
				panic(errors.AssertionFailedf("fetchColID is 0"))
			}
			mb.triggerColIDs.Add(mb.fetchColIDs[i])
			fetchCols = append(fetchCols, mb.fetchColIDs[i])
		}
	}
	// makeNewCols builds a new ColList from the given ColList with only the
	// visible columns. If there are zero values, fetchColIDs will be used to
	// substitute.
	makeNewCols := func(cols opt.OptionalColList) opt.ColList {
		newCols := make(opt.ColList, 0, visibleColOrds.Len())
		for i, ok := visibleColOrds.Next(0); ok; i, ok = visibleColOrds.Next(i + 1) {
			col := cols[i]
			if col == 0 {
				col = mb.fetchColIDs[i]
			}
			if col == 0 {
				panic(errors.AssertionFailedf("col is 0"))
			}
			mb.triggerColIDs.Add(col)
			newCols = append(newCols, col)
		}
		return newCols
	}
	var updateCols, insertCols opt.ColList
	if mb.canaryColID != 0 || mutation == opt.UpdateOp {
		updateCols = makeNewCols(mb.updateColIDs)
	}
	if mb.canaryColID != 0 || mutation == opt.InsertOp {
		insertCols = makeNewCols(mb.insertColIDs)
	}
	if mb.canaryColID != 0 {
		mb.triggerColIDs.Add(mb.canaryColID)
	}
	if mb.afterTriggers != nil {
		panic(errors.AssertionFailedf("afterTriggers already set"))
	}
	mb.afterTriggers = &memo.AfterTriggers{
		Triggers: triggers,
		Builder: mb.newRowLevelAfterTriggerBuilder(
			mutation, triggers, fetchCols, updateCols, insertCols,
		),
		WithID: mb.withID,
	}
}

// getEventsToMatchForMutation returns the set of trigger events that should be
// matched for the given mutation operator.
func (mb *mutationBuilder) getEventsToMatchForMutation(
	mutation opt.Operator,
) tree.TriggerEventTypeSet {
	var eventsToMatch tree.TriggerEventTypeSet
	switch mutation {
	case opt.InsertOp:
		eventsToMatch.Add(tree.TriggerEventInsert)
		if mb.canaryColID != 0 {
			// This is an UPSERT or INSERT with ON CONFLICT, so rows can be updated in
			// addition to being inserted.
			eventsToMatch.Add(tree.TriggerEventUpdate)
		}
	case opt.UpdateOp:
		eventsToMatch.Add(tree.TriggerEventUpdate)
	case opt.DeleteOp:
		eventsToMatch.Add(tree.TriggerEventDelete)
	default:
		panic(errors.AssertionFailedf("unexpected mutation operator: %v", mutation))
	}
	return eventsToMatch
}

// rowLevelAfterTriggerBuilder is a memo.PostQueryBuilder implementation for
// row-level AFTER triggers.
//
// It provides a method to build the trigger-function invocations over the set
// of rows that were modified by the mutation.
//
// See testdata/trigger for some examples.
type rowLevelAfterTriggerBuilder struct {
	mutation     opt.Operator
	mutatedTable cat.Table
	triggers     []cat.Trigger

	// stmtTreeInitFn returns a statementTree that tracks the mutations in
	// ancestor statements. It may be unset if there are no ancestor statements.
	stmtTreeInitFn func() statementTree

	// The following fields contain the columns from the mutation input needed to
	// build the triggers. The columns must be remapped to the new memo when the
	// triggers are built. If fetchCols, updateCols, or insertCols is set, then
	// there is one entry per visible column in the table.
	//
	// fetchCols is the list of columns from the mutation input that correspond to
	// old values of the modified rows.
	fetchCols opt.ColList
	// updateCols is the list of columns from the mutation input that correspond to
	// new values of the updated rows.
	updateCols opt.ColList
	// insertCols is the list of columns from the mutation input that correspond to
	// new values of the inserted rows.
	insertCols opt.ColList
	// canaryCol is set for UPSERT and INSERT with ON CONFLICT. It is NULL to
	// indicate an inserted row, and non-NULL to indicate an updated row.
	canaryCol opt.ColumnID
}

var _ memo.PostQueryBuilder = &rowLevelAfterTriggerBuilder{}

func (mb *mutationBuilder) newRowLevelAfterTriggerBuilder(
	mutation opt.Operator, triggers []cat.Trigger, fetchCols, updateCols, insertCols opt.ColList,
) *rowLevelAfterTriggerBuilder {
	return &rowLevelAfterTriggerBuilder{
		mutation:       mutation,
		mutatedTable:   mb.tab,
		triggers:       triggers,
		stmtTreeInitFn: mb.b.stmtTree.GetInitFnForPostQuery(),
		fetchCols:      fetchCols,
		updateCols:     updateCols,
		insertCols:     insertCols,
		canaryCol:      mb.canaryColID,
	}
}

// Build is part of the memo.PostQueryBuilder interface.
func (tb *rowLevelAfterTriggerBuilder) Build(
	ctx context.Context,
	semaCtx *tree.SemaContext,
	evalCtx *eval.Context,
	catalog cat.Catalog,
	factoryI interface{},
	binding opt.WithID,
	bindingProps *props.Relational,
	colMap opt.ColMap,
) (_ memo.RelExpr, err error) {
	return buildTriggerCascadeHelper(ctx, semaCtx, evalCtx, catalog, factoryI, tb.stmtTreeInitFn,
		func(b *Builder) memo.RelExpr {
			f := b.factory
			md := f.Metadata()

			typeID := typedesc.TableIDToImplicitTypeOID(descpb.ID(tb.mutatedTable.ID()))
			tableTyp, err := semaCtx.TypeResolver.ResolveTypeByOID(ctx, typeID)
			if err != nil {
				panic(err)
			}

			// Map the columns from the original memo to the new one using colMap.
			inFetchCols := tb.fetchCols.RemapColumns(colMap)
			inUpdateCols := tb.updateCols.RemapColumns(colMap)
			inInsertCols := tb.insertCols.RemapColumns(colMap)
			colCount := len(inFetchCols) + len(inUpdateCols) + len(inInsertCols)
			if tb.canaryCol != 0 {
				// Make space for the canary column.
				colCount++
			}
			inCols := make(opt.ColList, 0, colCount)
			outCols := make(opt.ColList, 0, colCount)

			// Allocate a new scope to build the expression that will call the trigger
			// functions for each row scanned from the buffer.
			triggerScope := b.allocScope()
			var inCanaryCol, outCanaryCol opt.ColumnID
			if tb.canaryCol != 0 {
				inCanaryColID, ok := colMap.Get(int(tb.canaryCol))
				if !ok {
					panic(errors.AssertionFailedf("column %d not in mapping %s\n",
						tb.canaryCol, colMap.String()))
				}
				inCanaryCol = opt.ColumnID(inCanaryColID)
				colType := md.ColumnMeta(inCanaryCol).Type
				colName := scopeColName("").WithMetadataName("canary")
				col := b.synthesizeColumn(triggerScope, colName, colType, nil /* expr */, nil /* scalar */)
				outCanaryCol = col.id
				inCols = append(inCols, inCanaryCol)
				outCols = append(outCols, outCanaryCol)
			}
			addCols := func(cols opt.ColList, suffix string) opt.ColList {
				startIdx := len(outCols)
				for _, col := range cols {
					colMeta := md.ColumnMeta(col)
					name := scopeColName("").WithMetadataName(fmt.Sprintf("%s_%s", colMeta.Alias, suffix))
					outCol := b.synthesizeColumn(
						triggerScope, name, colMeta.Type, nil /* expr */, nil, /* scalar */
					)
					inCols = append(inCols, col)
					outCols = append(outCols, outCol.id)
				}
				return outCols[startIdx:len(outCols):len(outCols)]
			}
			outFetchCols := addCols(inFetchCols, "old")
			outUpdateCols := addCols(inUpdateCols, "new")
			outInsertCols := addCols(inInsertCols, "new")
			md.AddWithBinding(binding, b.factory.ConstructFakeRel(&memo.FakeRelPrivate{
				Props: bindingProps,
			}))
			triggerScope.expr = f.ConstructWithScan(&memo.WithScanPrivate{
				With:    binding,
				InCols:  inCols,
				OutCols: outCols,
				ID:      md.NextUniqueID(),
			})

			// Project the old and new values into tuples. These will become the OLD and
			// NEW arguments to the trigger functions.
			makeTuple := func(cols opt.ColList) opt.ScalarExpr {
				elems := make([]opt.ScalarExpr, len(cols))
				for i, col := range cols {
					elems[i] = f.ConstructVariable(col)
				}
				return f.ConstructTuple(elems, tableTyp)
			}
			var canaryCheck opt.ScalarExpr
			if tb.canaryCol != 0 {
				canaryCheck = f.ConstructIs(f.ConstructVariable(outCanaryCol), memo.NullSingleton)
			}

			// Build an expression for the old values of each row.
			oldScalar := opt.ScalarExpr(memo.NullSingleton)
			if len(outFetchCols) > 0 {
				oldScalar = makeTuple(outFetchCols)
				if outCanaryCol != 0 {
					// For an UPSERT/ON CONFLICT, the OLD column is non-null only for the
					// conflicting rows, which are identified by the canary column.
					oldScalar = f.ConstructCase(
						memo.TrueSingleton,
						memo.ScalarListExpr{f.ConstructWhen(canaryCheck, f.ConstructNull(tableTyp))},
						oldScalar,
					)
				}
			}
			// Build an expression for the new values of each row.
			newScalar := opt.ScalarExpr(memo.NullSingleton)
			if outCanaryCol != 0 {
				// For an UPSERT/ON CONFLICT, the NEW column contains either inserted or
				// updated values, depending on the canary column.
				newScalar = f.ConstructCase(
					memo.TrueSingleton,
					memo.ScalarListExpr{f.ConstructWhen(canaryCheck, makeTuple(outInsertCols))},
					makeTuple(outUpdateCols),
				)
			} else if len(outUpdateCols) > 0 {
				newScalar = makeTuple(outUpdateCols)
			} else if len(outInsertCols) > 0 {
				newScalar = makeTuple(outInsertCols)
			}
			oldColID := b.projectColWithMetadataName(triggerScope, triggerColOld, tableTyp, oldScalar)
			newColID := b.projectColWithMetadataName(triggerScope, triggerColNew, tableTyp, newScalar)
			tgWhen := tree.NewDString("AFTER")
			tgLevel := tree.NewDString("ROW")
			tgRelID := tree.NewDOid(oid.Oid(tb.mutatedTable.ID()))
			tgTableName := tree.NewDString(string(tb.mutatedTable.Name()))
			fqName, err := b.catalog.FullyQualifiedName(ctx, tb.mutatedTable)
			if err != nil {
				panic(err)
			}
			tgTableSchema := tree.NewDString(fqName.Schema())
			var tgOp opt.ScalarExpr
			switch tb.mutation {
			case opt.InsertOp:
				tgOp = f.ConstructConstVal(tree.NewDString("INSERT"), types.String)
				if outCanaryCol != 0 {
					tgOp = f.ConstructCase(
						memo.TrueSingleton,
						memo.ScalarListExpr{f.ConstructWhen(canaryCheck, tgOp)},
						f.ConstructConstVal(tree.NewDString("UPDATE"), types.String),
					)
				}
			case opt.UpdateOp:
				tgOp = f.ConstructConstVal(tree.NewDString("UPDATE"), types.String)
			case opt.DeleteOp:
				tgOp = f.ConstructConstVal(tree.NewDString("DELETE"), types.String)
			default:
				panic(errors.AssertionFailedf("unexpected mutation type: %v", tb.mutation))
			}

			for i, trigger := range tb.triggers {
				if i > 0 {
					// No need to place a barrier below the first trigger.
					triggerScope.expr = f.ConstructBarrier(triggerScope.expr)
				}

				tgName := tree.NewDName(string(trigger.Name()))
				tgNumArgs := tree.NewDInt(tree.DInt(len(trigger.FuncArgs())))
				tgArgV := tree.NewDArray(types.String)
				for _, arg := range trigger.FuncArgs() {
					err = tgArgV.Append(arg)
					if err != nil {
						panic(err)
					}
				}
				args := memo.ScalarListExpr{
					f.ConstructVariable(newColID),              // NEW
					f.ConstructVariable(oldColID),              // OLD
					f.ConstructConstVal(tgName, types.Name),    // TG_NAME
					f.ConstructConstVal(tgWhen, types.String),  // TG_WHEN
					f.ConstructConstVal(tgLevel, types.String), // TG_LEVEL
					tgOp,                                    // TG_OP
					f.ConstructConstVal(tgRelID, types.Oid), // TG_RELIID
					f.ConstructConstVal(tgTableName, types.String),   // TG_RELNAME
					f.ConstructConstVal(tgTableName, types.String),   // TG_TABLE_NAME
					f.ConstructConstVal(tgTableSchema, types.String), // TG_TABLE_SCHEMA
					f.ConstructConstVal(tgNumArgs, types.Int),        // TG_NARGS
					f.ConstructConstVal(tgArgV, types.StringArray),   // TG_ARGV
				}

				// Resolve the trigger function and build the invocation.
				triggerFn, def := b.buildTriggerFunction(trigger, tb.mutatedTable.ID(), tableTyp, args)

				// If there is a WHEN condition, wrap the trigger function invocation in a
				// CASE WHEN statement that checks the WHEN condition.
				if trigger.WhenExpr() != "" {
					triggerFn = b.buildTriggerWhen(
						trigger, triggerScope, oldColID, newColID, triggerFn, f.ConstructNull(tableTyp),
					)
				}

				// For UPSERT and INSERT ON CONFLICT, UPDATE triggers should only fire for
				// the conflicting rows, which are identified by the canary column. INSERT
				// triggers should only fire for non-conflicting rows. A trigger that
				// matches both operations can fire unconditionally.
				if outCanaryCol != 0 {
					var hasInsert, hasUpdate bool
					for j := 0; j < trigger.EventCount(); j++ {
						if trigger.Event(j).EventType == tree.TriggerEventInsert {
							hasInsert = true
						} else if trigger.Event(j).EventType == tree.TriggerEventUpdate {
							hasUpdate = true
						}
					}
					if hasInsert && !hasUpdate {
						triggerFn = f.ConstructCase(
							memo.TrueSingleton,
							memo.ScalarListExpr{f.ConstructWhen(canaryCheck, triggerFn)},
							f.ConstructNull(tableTyp),
						)
					} else if hasUpdate && !hasInsert {
						triggerFn = f.ConstructCase(
							memo.TrueSingleton,
							memo.ScalarListExpr{f.ConstructWhen(canaryCheck, f.ConstructNull(tableTyp))},
							triggerFn,
						)
					}
				}

				// Finally, project a column that invokes the trigger function.
				b.projectColWithMetadataName(triggerScope, def.Name, tableTyp, triggerFn)
			}
			// Always wrap the expression in a barrier, or else the projections will be
			// pruned and the triggers will not be executed.
			return f.ConstructBarrier(triggerScope.expr)
		})
}

// ============================================================================
// Shared logic
// ============================================================================

type cachedTriggerFunc struct {
	triggerName tree.Name
	funDef      *memo.UDFDefinition
	resolved    *tree.ResolvedFunctionDefinition
}

// buildTriggerFunction resolves and builds a trigger function invocation for
// the given trigger, using the given arguments.
func (b *Builder) buildTriggerFunction(
	trigger cat.Trigger, tableID cat.StableID, tableTyp *types.T, args memo.ScalarListExpr,
) (opt.ScalarExpr, *tree.ResolvedFunctionDefinition) {
	cached := b.builtTriggerFuncs[tableID]
	for _, cachedFunc := range cached {
		if cachedFunc.triggerName == trigger.Name() {
			private := &memo.UDFCallPrivate{Def: cachedFunc.funDef}
			return b.factory.ConstructUDFCall(args, private), cachedFunc.resolved
		}
	}

	f := b.factory
	triggerFuncScope := b.allocScope()
	funcRef := &tree.FunctionOID{OID: catid.FuncIDToOID(catid.DescID(trigger.FuncID()))}
	funcExpr := tree.FuncExpr{Func: tree.ResolvableFunctionReference{FunctionReference: funcRef}}
	triggerFuncScope.resolveType(&funcExpr, types.AnyElement)
	resolvedDef := funcExpr.Func.FunctionReference.(*tree.ResolvedFunctionDefinition)
	o := funcExpr.ResolvedOverload()

	// Build the set of parameters for the trigger function. The parameters are
	// the OLD and NEW tuples, followed by the static parameters of the trigger
	// function.
	params := append([]routineParam{
		{name: triggerColNew, typ: tableTyp, class: tree.RoutineParamIn},
		{name: triggerColOld, typ: tableTyp, class: tree.RoutineParamIn},
	}, triggerFuncStaticParams...)
	paramCols := make(opt.ColList, len(params))
	for colOrd, param := range params {
		paramColName := funcParamColName(param.name, colOrd)
		col := b.synthesizeColumn(triggerFuncScope, paramColName, param.typ, nil /* expr */, nil /* scalar */)
		col.setParamOrd(colOrd)
		paramCols[colOrd] = col.id
	}

	// Initialize and cache the UDF definition before building the function body.
	// This is necessary to handle recursive triggers.
	//
	// All triggers are called on NULL input.
	const calledOnNullInput = true
	const isTriggerFunc = true
	udfDef := &memo.UDFDefinition{
		Name:              resolvedDef.Name,
		Typ:               tableTyp,
		Volatility:        o.Volatility,
		CalledOnNullInput: calledOnNullInput,
		TriggerFunc:       isTriggerFunc,
		RoutineType:       o.Type,
		RoutineLang:       o.Language,
		Params:            paramCols,
	}
	if b.builtTriggerFuncs == nil {
		b.builtTriggerFuncs = make(map[cat.StableID][]cachedTriggerFunc)
	}
	b.builtTriggerFuncs[tableID] = append(b.builtTriggerFuncs[tableID],
		cachedTriggerFunc{
			triggerName: trigger.Name(),
			funDef:      udfDef,
			resolved:    resolvedDef,
		},
	)

	// Parse and build the function body.
	stmt, err := plpgsql.Parse(trigger.FuncBody())
	if err != nil {
		panic(err)
	}
	plBuilder := newPLpgSQLBuilder(
		b, resolvedDef.Name, stmt.AST.Label, nil /* colRefs */, params, tableTyp,
		false /* isProc */, false /* isDoBlock */, true /* buildSQL */, nil, /* outScope */
	)
	stmtScope := plBuilder.buildRootBlock(stmt.AST, triggerFuncScope, params)
	udfDef.Body = []memo.RelExpr{stmtScope.expr}
	udfDef.BodyProps = []*physical.Required{stmtScope.makePhysicalProps()}

	return f.ConstructUDFCall(args, &memo.UDFCallPrivate{Def: udfDef}), resolvedDef
}

// buildTriggerWhen wraps the trigger function invocation in a CASE WHEN
// statement that checks the WHEN condition, if one exists.
//
// elseExpr is the expression that should be returned if the WHEN condition
// evaluates to false. For BEFORE triggers, this is the OLD or NEW column
// depending on the event type. For AFTER triggers, it is NULL.
func (b *Builder) buildTriggerWhen(
	trigger cat.Trigger,
	triggerScope *scope,
	oldColID, newColID opt.ColumnID,
	triggerFn, elseExpr opt.ScalarExpr,
) opt.ScalarExpr {
	// Wrap the trigger function invocation in a CASE WHEN statement that
	// checks the WHEN condition.
	parsedWhen, err := parser.ParseExpr(trigger.WhenExpr())
	if err != nil {
		panic(err)
	}
	// The WHEN condition may reference OLD and NEW columns only.
	whenScope := triggerScope.push()
	if oldColID != 0 {
		whenScope.appendColumn(triggerScope.getColumn(oldColID))
		whenScope.getColumn(oldColID).name = scopeColName(triggerColOld)
	}
	if newColID != 0 {
		whenScope.appendColumn(triggerScope.getColumn(newColID))
		whenScope.getColumn(newColID).name = scopeColName(triggerColNew)
	}
	typedWhen := whenScope.resolveAndRequireType(parsedWhen, types.Bool)
	whenExpr := b.buildScalar(
		typedWhen, whenScope, nil /* outScope */, nil /* outCol */, nil, /* colRefs */
	)
	return b.factory.ConstructCase(
		memo.TrueSingleton,
		memo.ScalarListExpr{b.factory.ConstructWhen(whenExpr, triggerFn)},
		elseExpr,
	)
}
