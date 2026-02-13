// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optbuilder

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/plpgsql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

func (b *Builder) buildCreateTrigger(ct *tree.CreateTrigger, inScope *scope) (outScope *scope) {
	b.insideTriggerDef = true
	b.trackSchemaDeps = true
	// Make sure datasource names are qualified.
	b.qualifyDataSourceNamesInAST = true
	oldEvalCtxAnn := b.evalCtx.Annotations
	oldSemaCtxAnn := b.semaCtx.Annotations
	defer func() {
		b.insideTriggerDef = false
		b.trackSchemaDeps = false
		b.schemaDeps = nil
		b.schemaTypeDeps = intsets.Fast{}
		b.schemaFunctionDeps = intsets.Fast{}
		b.qualifyDataSourceNamesInAST = false
		b.evalCtx.Annotations = oldEvalCtxAnn
		b.semaCtx.Annotations = oldSemaCtxAnn
	}()

	// Resolve the table/view and check its privileges.
	tn := ct.TableName.ToTableName()
	if tn.ExplicitCatalog {
		if string(tn.CatalogName) != b.evalCtx.SessionData().Database {
			panic(unimplemented.New("CREATE TRIGGER", "cross-db references not supported"))
		}
	}
	ds, _, _ := b.resolveDataSource(&tn, privilege.TRIGGER)

	// Resolve the trigger function and check its privileges.
	funcExpr := tree.FuncExpr{Func: tree.ResolvableFunctionReference{FunctionReference: ct.FuncName}}
	typedExpr := inScope.resolveType(&funcExpr, types.AnyElement)
	f, ok := typedExpr.(*tree.FuncExpr)
	if !ok {
		panic(errors.AssertionFailedf("%s is not a function", funcExpr.Func.String()))
	}
	o := f.ResolvedOverload()
	if err := b.catalog.CheckExecutionPrivilege(b.ctx, o.Oid, b.checkPrivilegeUser); err != nil {
		panic(err)
	}

	var allEventTypes tree.TriggerEventTypeSet
	for i := range ct.Events {
		allEventTypes.Add(ct.Events[i].EventType)
	}

	// Validate the CREATE TRIGGER statement. Skip validation when
	// FuncBodyOverride is set, since we're analyzing an existing trigger's new
	// function body, not creating a new trigger.
	if ct.FuncBodyOverride == "" {
		if err := cat.ValidateCreateTrigger(ct, ds, allEventTypes); err != nil {
			panic(err)
		}
		checkUnsupportedCreateTrigger(ct, ds)
	}

	// Lookup the implicit table type. This must happen after the above checks,
	// since virtual/system tables do not have an implicit type.
	typeID := typedesc.TableIDToImplicitTypeOID(descpb.ID(ds.ID()))
	tableTyp, err := b.semaCtx.TypeResolver.ResolveTypeByOID(b.ctx, typeID)
	if err != nil {
		panic(err)
	}

	// Build and validate the WHEN expression.
	if ct.When != nil {
		b.buildWhenForTrigger(ct, tableTyp, allEventTypes)
	}

	// Build and validate the trigger function body.
	// TODO(#128536): pass the qualified function body through the
	// CreateTriggerPrivate instead.
	ct.FuncBody = b.buildFunctionForTrigger(ct, tableTyp, f)

	// Add the resolved and validated CREATE TRIGGER statement to the memo.
	outScope = b.allocScope()
	outScope.expr = b.factory.ConstructCreateTrigger(
		&memo.CreateTriggerPrivate{
			Syntax:   ct,
			Deps:     b.schemaDeps,
			TypeDeps: b.schemaTypeDeps,
			FuncDeps: b.schemaFunctionDeps,
		},
	)
	return outScope
}

// buildWhenForTrigger builds and validates the WHEN clause of a trigger.
func (b *Builder) buildWhenForTrigger(
	ct *tree.CreateTrigger, tableTyp *types.T, allEventTypes tree.TriggerEventTypeSet,
) {
	// The WHEN clause can reference the OLD and NEW implicit variables,
	// although only in specific contexts. The other implicit variables are not
	// allowed.
	whenScope := b.allocScope()
	whenScope.context = exprKindWhen
	tup := b.makeAllNullsTuple(tableTyp)
	newName, oldName := scopeColName(triggerColNew), scopeColName(triggerColOld)
	newCol := b.synthesizeColumn(whenScope, newName, tableTyp, nil /* expr */, tup)
	oldCol := b.synthesizeColumn(whenScope, oldName, tableTyp, nil /* expr */, tup)

	// Check that the expression is of type bool. Disallow subqueries inside the
	// WHEN clause.
	defer b.semaCtx.Properties.Restore(b.semaCtx.Properties)
	b.semaCtx.Properties.Require(exprKindWhen.String(), tree.RejectSubqueries)
	typedWhen := whenScope.resolveAndRequireType(ct.When, types.Bool)

	// Check for invalid NEW or OLD variable references. Also resolve
	// user-defined type and function reference.
	var colRefs opt.ColSet
	b.buildScalar(typedWhen, whenScope, nil /* outScope */, nil /* outCol */, &colRefs)
	if colRefs.Contains(newCol.id) {
		if ct.ForEach == tree.TriggerForEachStatement {
			panic(pgerror.New(pgcode.InvalidObjectDefinition,
				"statement trigger's WHEN condition cannot reference column values"))
		}
		if allEventTypes.Contains(tree.TriggerEventDelete) {
			panic(pgerror.New(pgcode.InvalidObjectDefinition,
				"DELETE trigger's WHEN condition cannot reference NEW values"))
		}
	}
	if colRefs.Contains(oldCol.id) {
		if ct.ForEach == tree.TriggerForEachStatement {
			panic(pgerror.New(pgcode.InvalidObjectDefinition,
				"statement trigger's WHEN condition cannot reference column values"))
		}
		if allEventTypes.Contains(tree.TriggerEventInsert) {
			panic(pgerror.New(pgcode.InvalidObjectDefinition,
				"INSERT trigger's WHEN condition cannot reference OLD values"))
		}
	}
}

// buildFunctionForTrigger builds and validates the trigger function that will
// be executed by the trigger. The validated function body will be serialized
// and returned as a string.
func (b *Builder) buildFunctionForTrigger(
	ct *tree.CreateTrigger, tableTyp *types.T, f *tree.FuncExpr,
) string {
	b.insideFuncDef = true
	defer func() {
		b.insideFuncDef = false
	}()
	o := f.ResolvedOverload()
	funcScope := b.allocScope()
	if !f.ResolvedType().Identical(types.Trigger) {
		panic(pgerror.Newf(pgcode.InvalidObjectDefinition,
			"function %s must return type trigger", ct.FuncName))
	}
	if o.Language == tree.RoutineLangSQL {
		// NOTE: Trigger functions never use SQL.
		panic(errors.AssertionFailedf("SQL language not supported for triggers"))
	}
	// The trigger always references the trigger function.
	b.schemaFunctionDeps.Add(int(funcdesc.UserDefinedFunctionOIDToID(o.Oid)))

	// The trigger function can reference the NEW and OLD transition relations,
	// aliased in the trigger definition.
	if len(ct.Transitions) > 0 {
		// Save the existing CTEs in the builder and restore them after the function
		// body is built.
		prevCTEs := b.ctes
		b.ctes = nil
		defer func() {
			b.ctes = prevCTEs
		}()
	}
	for _, transition := range ct.Transitions {
		// Build a fake relational expression with a column corresponding to each
		// column from the table.
		outCols, presentation := b.makeColsForLabeledTupleType(tableTyp)
		fakeRelPrivate := &memo.FakeRelPrivate{Props: &props.Relational{OutputCols: outCols}}
		fakeExpr := b.factory.ConstructFakeRel(fakeRelPrivate)

		// Add the fake relational expression to the memo as a CTE, and make it
		// available in the trigger function's scope.
		id := b.factory.Memo().NextWithID()
		b.factory.Metadata().AddWithBinding(id, fakeExpr)
		cte := &cteSource{
			name: tree.AliasClause{Alias: transition.Name},
			cols: presentation,
			expr: fakeExpr,
			id:   id,
			mtr:  tree.CTEMaterializeAlways,
		}
		if funcScope.ctes == nil {
			funcScope.ctes = make(map[string]*cteSource)
		}
		funcScope.ctes[string(transition.Name)] = cte
		b.addCTE(cte)
	}

	// The trigger function takes a set of implicitly-defined parameters, two of
	// which are determined by the table's record type. Add them to the trigger
	// function scope.
	triggerFuncParams := make([]routineParam, 0, len(triggerFuncStaticParams)+2)
	triggerFuncParams = append(triggerFuncParams, routineParam{name: triggerColNew, typ: tableTyp})
	triggerFuncParams = append(triggerFuncParams, routineParam{name: triggerColOld, typ: tableTyp})
	triggerFuncParams = append(triggerFuncParams, triggerFuncStaticParams...)
	for i, param := range triggerFuncParams {
		paramColName := funcParamColName(param.name, i)
		col := b.synthesizeColumn(funcScope, paramColName, param.typ, nil /* expr */, nil /* scalar */)
		col.setParamOrd(i)
	}

	// Now that the transition relations and table type are known, fully build and
	// validate the trigger function's body statements.
	//
	// We need to disable stable function folding because we want to catch the
	// volatility of stable functions. If folded, we only get a scalar and lose
	// the volatility.
	// Use FuncBodyOverride if set; this is provided during CREATE OR REPLACE
	// FUNCTION to analyze the new function body in the trigger's table context.
	body := o.Body
	if ct.FuncBodyOverride != "" {
		body = ct.FuncBodyOverride
	}
	stmt, err := parser.Parse(body)
	if err != nil {
		panic(err)
	}
	b.factory.FoldingControl().TemporarilyDisallowStableFolds(func() {
		plBuilder := newPLpgSQLBuilder(
			b, basePLOptions().WithIsTriggerFn(), ct.FuncName.String(), stmt.AST.Label,
			nil /* colRefs */, triggerFuncParams, tableTyp, nil /* outScope */, 0, /* resultBufferID */
		)
		funcScope = plBuilder.buildRootBlock(stmt.AST, funcScope, triggerFuncParams)
	})
	var vol tree.RoutineVolatility
	switch o.Volatility {
	case volatility.Leakproof, volatility.Immutable:
		vol = tree.RoutineImmutable
	case volatility.Stable:
		vol = tree.RoutineStable
	case volatility.Volatile:
		vol = tree.RoutineVolatile
	}
	checkStmtVolatility(vol, funcScope, stmt)

	// Validate that the result type of the last statement matches the
	// return type of the function.
	// TODO(mgartner): stmtScope.cols does not describe the result
	// columns of the statement. We should use physical.Presentation
	// instead.
	err = validateReturnType(b.ctx, b.semaCtx, tableTyp, funcScope.cols)
	if err != nil {
		panic(err)
	}
	if vol == tree.RoutineImmutable && len(b.schemaDeps) > 0 {
		panic(
			pgerror.Newf(
				pgcode.InvalidParameterValue,
				"referencing relations is not allowed in immutable function",
			),
		)
	}

	// Return the function body with fully-qualified names.
	fmtCtx := tree.NewFmtCtx(tree.FmtSerializable)
	fmtCtx.FormatNode(stmt.AST)
	return fmtCtx.CloseAndGetString()
}

// makeAllNullsTuple constructs a tuple with the given type, with all NULL
// elements.
func (b *Builder) makeAllNullsTuple(typ *types.T) opt.ScalarExpr {
	if len(typ.TupleContents()) == 0 {
		panic(errors.AssertionFailedf("expected nonzero tuple contents"))
	}
	elems := make(memo.ScalarListExpr, len(typ.TupleContents()))
	for i := range elems {
		elems[i] = memo.NullSingleton
	}
	return b.factory.ConstructTuple(elems, typ)
}

// makeColsForLabeledTupleType adds a column to the metadata for each element of
// the given tuple type. The elements of the tuple type must have labels. The
// set of newly constructed columns is returned, as well as a presentation for
// those columns.
func (b *Builder) makeColsForLabeledTupleType(typ *types.T) (opt.ColSet, physical.Presentation) {
	if len(typ.TupleContents()) == 0 {
		panic(errors.AssertionFailedf("expected nonzero tuple contents"))
	}
	if len(typ.TupleLabels()) != len(typ.TupleContents()) {
		panic(errors.AssertionFailedf("expected labeled tuple elements"))
	}
	var cols opt.ColSet
	presentation := make(physical.Presentation, len(typ.TupleContents()))
	for i, colTyp := range typ.TupleContents() {
		colName := typ.TupleLabels()[i]
		colID := b.factory.Metadata().AddColumn(colName, colTyp)
		cols.Add(colID)
		presentation[i] = opt.AliasedColumn{Alias: colName, ID: colID}
	}
	return cols, presentation
}

// triggerFuncStaticParams is the set of implicitly-defined parameters for a
// PL/pgSQL trigger function, excluding the NEW and OLD parameters which are
// determined by the table when a trigger is created.
var triggerFuncStaticParams = []routineParam{
	{name: "tg_name", typ: types.Name, class: tree.RoutineParamIn},
	{name: "tg_when", typ: types.String, class: tree.RoutineParamIn},
	{name: "tg_level", typ: types.String, class: tree.RoutineParamIn},
	{name: "tg_op", typ: types.String, class: tree.RoutineParamIn},
	{name: "tg_relid", typ: types.Oid, class: tree.RoutineParamIn},
	{name: "tg_relname", typ: types.Name, class: tree.RoutineParamIn},
	{name: "tg_table_name", typ: types.Name, class: tree.RoutineParamIn},
	{name: "tg_table_schema", typ: types.Name, class: tree.RoutineParamIn},
	{name: "tg_nargs", typ: types.Int, class: tree.RoutineParamIn},
	{name: "tg_argv", typ: types.StringArray, class: tree.RoutineParamIn},
}

const triggerColNew = "new"
const triggerColOld = "old"

func checkUnsupportedCreateTrigger(ct *tree.CreateTrigger, ds cat.DataSource) {
	if ct.ForEach == tree.TriggerForEachStatement {
		panic(unimplementedStatementLevelErr)
	}
	if ct.ActionTime == tree.TriggerActionTimeInsteadOf {
		panic(unimplementedInsteadOfErr)
	}
	if len(ct.Transitions) > 0 {
		panic(unimplementedReferencingErr)
	}
	for _, event := range ct.Events {
		if event.EventType == tree.TriggerEventTruncate {
			panic(unimplementedTruncateErr)
		}
		if len(event.Columns) > 0 {
			panic(unimplementedColumnListErr)
		}
	}
	if _, ok := ds.(cat.View); ok {
		panic(unimplementedViewTriggerErr)
	}
}

var (
	unimplementedStatementLevelErr = unimplemented.NewWithIssue(126362,
		"statement-level triggers are not yet supported")
	unimplementedInsteadOfErr = unimplemented.NewWithIssue(126363,
		"INSTEAD OF triggers are not yet supported")
	unimplementedReferencingErr = unimplemented.NewWithIssue(135655,
		"REFERENCING clause is not yet supported for triggers")
	unimplementedTruncateErr = unimplemented.NewWithIssue(135657,
		"TRUNCATE triggers are not yet supported")
	unimplementedColumnListErr = unimplemented.NewWithIssue(135656,
		"column lists are not yet supported for triggers")
	unimplementedViewTriggerErr = unimplemented.NewWithIssue(135658,
		"triggers on views are not yet supported")
)
