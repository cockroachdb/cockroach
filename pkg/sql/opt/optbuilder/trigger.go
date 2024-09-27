// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optbuilder

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	plpgsql "github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// buildRowLevelBeforeTriggers builds any applicable row-level BEFORE triggers
// based on the event type. It returns true if triggers were built, and false
// otherwise.
func (mb *mutationBuilder) buildRowLevelBeforeTriggers(eventType tree.TriggerEventType) bool {
	f := mb.b.factory
	triggers := mb.getRowLevelBeforeTriggers(eventType)
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
	for i := range triggers {
		triggerScope.expr = f.ConstructBarrier(triggerScope.expr)

		// Project a column that invokes the trigger function.
		mb.buildTriggerFunction(
			triggers[i], triggerScope, eventType, tableTyp, oldColID, newColID,
		)
	}
	triggerScope.expr = f.ConstructBarrier(triggerScope.expr)
	mb.outScope = triggerScope
	return true
}

// getRowLevelBeforeTriggers returns the set of row-level BEFORE triggers for
// the table and given trigger event type. The triggers are returned in the
// order in which they should be executed.
func (mb *mutationBuilder) getRowLevelBeforeTriggers(
	eventType tree.TriggerEventType,
) []cat.Trigger {
	var neededTriggers intsets.Fast
	for i := 0; i < mb.tab.TriggerCount(); i++ {
		trigger := mb.tab.Trigger(i)
		if !trigger.Enabled() || !trigger.ForEachRow() ||
			trigger.ActionTime() != tree.TriggerActionTimeBefore {
			continue
		}
		for j := 0; j < trigger.EventCount(); j++ {
			if trigger.Event(j).EventType == eventType {
				// The conditions have been met for this trigger to fire.
				neededTriggers.Add(i)
				break
			}
		}
	}
	if neededTriggers.Len() == 0 {
		return nil
	}
	triggers := make([]cat.Trigger, 0, neededTriggers.Len())
	for i, ok := neededTriggers.Next(0); ok; i, ok = neededTriggers.Next(i + 1) {
		triggers = append(triggers, mb.tab.Trigger(i))
	}
	// Triggers fire in alphabetical order of the name. The names are always
	// unique within a given table, so a stable sort is not necessary.
	less := func(i, j int) bool {
		return triggers[i].Name() < triggers[j].Name()
	}
	sort.Slice(triggers, less)
	return triggers
}

// buildOldAndNewCols builds the OLD and NEW column tuples for a row-level
// BEFORE trigger, if applicable. The OLD tuple contains the original values of
// the columns being updated or deleted, and the NEW tuple contains the new
// values of the columns being updated or inserted.
func (mb *mutationBuilder) buildOldAndNewCols(
	triggerScope *scope, eventType tree.TriggerEventType, tableTyp *types.T, visibleColOrds []int,
) (oldColID, newColID opt.ColumnID) {
	f := mb.b.factory
	makeTuple := func(colIDs opt.OptionalColList, name string) opt.ColumnID {
		elems := make(memo.ScalarListExpr, 0, len(visibleColOrds))
		for _, i := range visibleColOrds {
			if colIDs[i] == 0 {
				panic(errors.AssertionFailedf("missing column for trigger"))
			}
			elems = append(elems, f.ConstructVariable(colIDs[i]))
		}
		passThroughCols := triggerScope.colSet()
		tup := f.ConstructTuple(elems, tableTyp)
		colName := scopeColName("").WithMetadataName(name)
		tupCol := mb.b.synthesizeColumn(triggerScope, colName, tableTyp, nil /* expr */, tup /* scalar */)
		proj := memo.ProjectionsExpr{f.ConstructProjectionsItem(tup, tupCol.id)}
		triggerScope.expr = f.ConstructProject(triggerScope.expr, proj, passThroughCols)
		return tupCol.id
	}
	if eventType == tree.TriggerEventUpdate || eventType == tree.TriggerEventDelete {
		oldColID = makeTuple(mb.fetchColIDs, triggerColOld)
	}
	if eventType == tree.TriggerEventInsert {
		newColID = makeTuple(mb.insertColIDs, triggerColNew)
	} else if eventType == tree.TriggerEventUpdate {
		// Build a colIDs slice using updateColIDs, filling in the missing columns
		// (which are not being updated) with the old column values.
		colIDs := make(opt.OptionalColList, len(mb.updateColIDs))
		copy(colIDs, mb.updateColIDs)
		for i, colID := range colIDs {
			if colID == 0 {
				if mb.insertColIDs[i] != 0 {
					colIDs[i] = mb.insertColIDs[i]
				} else if mb.fetchColIDs[i] != 0 {
					colIDs[i] = mb.fetchColIDs[i]
				}
			}
		}
		newColID = makeTuple(colIDs, triggerColNew)
	}
	return oldColID, newColID
}

// buildTriggerFunction builds a trigger function invocation for the given
// trigger, and returns the ID of the column that projects the result of the
// trigger function.
func (mb *mutationBuilder) buildTriggerFunction(
	trigger cat.Trigger,
	triggerScope *scope,
	eventType tree.TriggerEventType,
	tableTyp *types.T,
	oldColID, newColID opt.ColumnID,
) opt.ColumnID {
	f := mb.b.factory
	triggerFuncScope := mb.b.allocScope()
	funcRef := &tree.FunctionOID{OID: catid.FuncIDToOID(catid.DescID(trigger.FuncID()))}
	funcExpr := tree.FuncExpr{Func: tree.ResolvableFunctionReference{FunctionReference: funcRef}}
	triggerFuncScope.resolveType(&funcExpr, types.Any)
	def := funcExpr.Func.FunctionReference.(*tree.ResolvedFunctionDefinition)
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
		col := mb.b.synthesizeColumn(triggerFuncScope, paramColName, param.typ, nil /* expr */, nil /* scalar */)
		col.setParamOrd(colOrd)
		paramCols[colOrd] = col.id
	}

	// Parse and build the function body.
	stmt, err := plpgsql.Parse(trigger.FuncBody())
	if err != nil {
		panic(err)
	}
	plBuilder := newPLpgSQLBuilder(
		mb.b, def.Name, stmt.AST.Label, nil /* colRefs */, params, tableTyp,
		false /* isProc */, true /* buildSQL */, nil, /* outScope */
	)
	stmtScope := plBuilder.buildRootBlock(stmt.AST, triggerFuncScope, params)

	args := mb.buildTriggerFunctionArgs(trigger, eventType, oldColID, newColID)

	// All triggers are called on NULL input.
	const calledOnNullInput = true
	triggerFn := f.ConstructUDFCall(args,
		&memo.UDFCallPrivate{
			Def: &memo.UDFDefinition{
				Name:              def.Name,
				Typ:               tableTyp,
				Volatility:        o.Volatility,
				CalledOnNullInput: calledOnNullInput,
				RoutineType:       o.Type,
				RoutineLang:       o.Language,
				Body:              []memo.RelExpr{stmtScope.expr},
				BodyProps:         []*physical.Required{stmtScope.makePhysicalProps()},
				Params:            paramCols,
			},
		},
	)

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
	passThroughCols := triggerScope.colSet()
	colName := scopeColName("").WithMetadataName(def.Name)
	triggerFnCol := mb.b.synthesizeColumn(
		triggerScope, colName, tableTyp, nil /* expr */, triggerFn, /* scalar */
	)
	triggerScope.expr = f.ConstructProject(
		triggerScope.expr,
		memo.ProjectionsExpr{f.ConstructProjectionsItem(triggerFn, triggerFnCol.id)},
		passThroughCols,
	)
	return triggerFnCol.id
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
