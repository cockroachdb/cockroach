// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdceval

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// sentinel value indicating we should use default builtin
// implementation.
var useDefaultBuiltin *tree.ResolvedFunctionDefinition

// cdcFunctions is a list of supported stable and immutable builtin functions.
// Some builtin functions have alternative implementation.
// This map also specifies the list of supported CDC specific function definitions.
//
// Any builtin stable (or volatile) function that accesses eval.Context will
// have to have an override due to the fact that we should not manipulate
// underlying eval.Context directly (this is due to the fact that DistSQL copies eval
// context, and, in addition, eval.Context is not thread safe).
// Instead, we have to do so through annotations object stored inside eval.Context.
var cdcFunctions = map[string]*tree.ResolvedFunctionDefinition{
	// {statement,transaction}_timestamp  functions can be supported given that we
	// set the statement and transaction timestamp to be equal to MVCC timestamp
	// of the event. However, we provide our own override which uses annotation to
	// return the MVCC timestamp of the update.
	"statement_timestamp": makeBuiltinOverride(
		tree.FunDefs["statement_timestamp"], timestampBuiltinOverloads...,
	),
	"transaction_timestamp": makeBuiltinOverride(
		tree.FunDefs["transaction_timestamp"], timestampBuiltinOverloads...,
	),

	"timezone": useDefaultBuiltin,

	// jsonb functions are stable because they depend on eval
	// context DataConversionConfig
	"jsonb_build_array":  useDefaultBuiltin,
	"jsonb_build_object": useDefaultBuiltin,
	"to_json":            useDefaultBuiltin,
	"to_jsonb":           useDefaultBuiltin,
	"row_to_json":        useDefaultBuiltin,

	// Misc functions that depend on eval context.
	"overlaps":         useDefaultBuiltin,
	"pg_collation_for": useDefaultBuiltin,
	"pg_typeof":        useDefaultBuiltin,
	"quote_literal":    useDefaultBuiltin,
	"quote_nullable":   useDefaultBuiltin,

	// TODO(yevgeniy): Support geometry.
	//"st_asgeojson",
	//"st_estimatedextent",

	"cdc_is_delete": makeCDCBuiltIn(
		"cdc_is_delete",
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, datums tree.Datums) (tree.Datum, error) {
				rowEvalCtx := rowEvalContextFromEvalContext(evalCtx)
				if rowEvalCtx.updatedRow.IsDeleted() {
					return tree.DBoolTrue, nil
				}
				return tree.DBoolFalse, nil
			},
			Info: "Returns true if the event is a deletion",
			// NB: even though some cdc functions appear to be stable (e.g. cdc_is_delete()),
			// we should not mark custom CDC functions as stable.  Doing so will cause
			// optimizer to (constant) fold this function during optimization step -- something
			// we definitely don't want to do because we need to evaluate those functions
			// for each event.
			Volatility: volatility.Volatile,
		}),
	"cdc_mvcc_timestamp": cdcTimestampBuiltin(
		"cdc_mvcc_timestamp",
		"Returns event MVCC HLC timestamp",
		func(rowEvalCtx *rowEvalContext) hlc.Timestamp {
			return rowEvalCtx.updatedRow.MvccTimestamp
		},
	),
	"cdc_updated_timestamp": cdcTimestampBuiltin(
		"cdc_updated_timestamp",
		"Returns event updated HLC timestamp",
		func(rowEvalCtx *rowEvalContext) hlc.Timestamp {
			return rowEvalCtx.updatedRow.SchemaTS
		},
	),
}

const cdcFnCategory = "CDC builtin"

var cdcFnProps = &tree.FunctionProperties{
	Category: cdcFnCategory,
}

func makeCDCBuiltIn(fnName string, overloads ...tree.Overload) *tree.ResolvedFunctionDefinition {
	def := tree.NewFunctionDefinition(fnName, cdcFnProps, overloads)
	// The schema name is actually not important since CDC doesn't use any user
	// defined functions. And, we're sure that we always return the first
	// function definition found.
	return tree.QualifyBuiltinFunctionDefinition(def, catconstants.PublicSchemaName)
}

func cdcTimestampBuiltin(
	fnName string, doc string, tsFn func(rowEvalCtx *rowEvalContext) hlc.Timestamp,
) *tree.ResolvedFunctionDefinition {
	def := tree.NewFunctionDefinition(
		fnName,
		cdcFnProps,
		[]tree.Overload{
			{
				Types:      tree.ParamTypes{},
				ReturnType: tree.FixedReturnType(types.Decimal),
				Fn: func(ctx context.Context, evalCtx *eval.Context, datums tree.Datums) (tree.Datum, error) {
					rowEvalCtx := rowEvalContextFromEvalContext(evalCtx)
					return eval.TimestampToDecimalDatum(tsFn(rowEvalCtx)), nil
				},
				Info: doc,
				// NB: even though some cdc functions appear to be stable (e.g. cdc_is_delete()),
				// we should not mark custom CDC functions as stable.  Doing so will cause
				// optimizer to (constant) fold this function during optimization step -- something
				// we definitely don't want to do because we need to evaluate those functions
				// for each event.
				Volatility: volatility.Volatile,
			},
		},
	)
	// The schema name is actually not important since CDC doesn't use any user
	// defined functions. And, we're sure that we always return the first
	// function definition found.
	return tree.QualifyBuiltinFunctionDefinition(def, catconstants.PublicSchemaName)
}

// cdcPrevType returns a types.T for the tuple corresponding to the
// event descriptor.
func cdcPrevType(desc *cdcevent.EventDescriptor) *types.T {
	numCols := len(desc.ResultColumns()) + 1 /* crdb_internal_mvcc_timestamp */
	tupleTypes := make([]*types.T, 0, numCols)
	tupleLabels := make([]string, 0, numCols)

	for _, c := range desc.ResultColumns() {
		// TODO(yevgeniy): Handle virtual columns in cdc_prev.
		// In order to do this, we have to emit default expression with
		// all named references replaced with tuple field access.
		tupleLabels = append(tupleLabels, c.Name)
		tupleTypes = append(tupleTypes, c.Typ)
	}

	// Add system columns.
	tupleLabels = append(tupleLabels, colinfo.MVCCTimestampColumnName)
	tupleTypes = append(tupleTypes, colinfo.MVCCTimestampColumnType)
	return types.MakeLabeledTuple(tupleTypes, tupleLabels)
}

// makePrevRowFn creates a function to return a tuple corresponding
// to the previous value of the row.
func makePrevRowFn(retType *types.T) *tree.ResolvedFunctionDefinition {
	return makeCDCBuiltIn(prevRowFnName.Parts[0],
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(retType),
			Fn: func(ctx context.Context, evalCtx *eval.Context, datums tree.Datums) (tree.Datum, error) {
				rec := rowEvalContextFromEvalContext(evalCtx)
				return rec.prevRowTuple, nil
			},
			Info: "Returns previous value of a row as tuple",
			// NB: even though some cdc functions appear to be stable (e.g. cdc_is_delete()),
			// we should not mark custom CDC functions as stable.  Doing so will cause
			// optimizer to (constant) fold this function during optimization step -- something
			// we definitely don't want to do because we need to evaluate those functions
			// for each event.
			Volatility:       volatility.Volatile,
			DistsqlBlocklist: true,
		},
	)
}

// The state of the previous row is made available via cdc_prev tuple.
// This tuple is generated by rewriting expressions using cdc_prev
// (i.e. row_to_json(cdc_prev.*)) to generate a table which returns
// correctly typed tuple:
//
//	SELECT ... FROM tbl, (SELECT ((crdb_internal.cdc_prev_row()).*)) AS cdc_prev
//
// The crdb_internal.cdc_prev_row() function is in turn configured to return
// previous row datums.
const prevTupleName = "cdc_prev"

var prevRowFnName = tree.MakeUnresolvedName("crdb_internal", "cdc_prev_row")

// checkFunctionSupported checks if the function (expression) is supported.
// Returns (possibly modified) function expression if supported; error otherwise.
func checkFunctionSupported(
	ctx context.Context, fnCall *tree.FuncExpr, semaCtx *tree.SemaContext,
) (*tree.FuncExpr, error) {
	if semaCtx.FunctionResolver == nil {
		return nil, errors.AssertionFailedf("function resolver must be configured for CDC")
	}

	// Returns function call expression, provided the function with specified
	// name is supported.
	cdcFunctionWithOverride := func(name string, fnCall *tree.FuncExpr) (*tree.FuncExpr, error) {
		funDef, isSafe := cdcFunctions[name]
		if !isSafe {
			return nil, pgerror.Newf(pgcode.UndefinedFunction, "function %q unsupported by CDC", name)
		}
		if funDef != useDefaultBuiltin {
			// Install our override
			fnCall.Func = tree.ResolvableFunctionReference{FunctionReference: funDef}
		}
		return fnCall, nil
	}

	switch fn := fnCall.Func.FunctionReference.(type) {
	case *tree.UnresolvedName:
		funDef, err := semaCtx.FunctionResolver.ResolveFunction(ctx, fn, semaCtx.SearchPath)
		if err != nil {
			return nil, pgerror.Newf(pgcode.UndefinedFunction, "function %q unsupported by CDC", fnCall.Func.String())
		}
		fnCall = &tree.FuncExpr{
			Func:  tree.ResolvableFunctionReference{FunctionReference: funDef},
			Type:  fnCall.Type,
			Exprs: fnCall.Exprs,
		}
		if _, isCDCFn := cdcFunctions[funDef.Name]; isCDCFn {
			return fnCall, nil
		}
		return checkFunctionSupported(ctx, fnCall, semaCtx)
	case *tree.ResolvedFunctionDefinition:
		var fnVolatility volatility.V
		for _, overload := range fn.Overloads {
			// Aggregates, generators and window functions are not supported.
			switch overload.Class {
			case tree.AggregateClass, tree.GeneratorClass, tree.WindowClass:
				return nil, pgerror.Newf(pgcode.UndefinedFunction, "function %q unsupported by CDC", fn.Name)
			}
			if overload.Volatility > fnVolatility {
				fnVolatility = overload.Volatility
			}
		}
		if fnVolatility <= volatility.Immutable {
			// Remaining immutable functions are safe.
			return fnCall, nil
		}

		return cdcFunctionWithOverride(fn.Name, fnCall)
	case *tree.FunctionDefinition:
		switch fn.Class {
		case tree.AggregateClass, tree.GeneratorClass, tree.WindowClass:
			return nil, pgerror.Newf(pgcode.UndefinedFunction, "function %q unsupported by CDC", fn.Name)
		}

		var fnVolatility volatility.V
		if fnCall.ResolvedOverload() != nil {
			if _, isCDC := cdcFunctions[fn.Name]; isCDC {
				return fnCall, nil
			}
			fnVolatility = fnCall.ResolvedOverload().Volatility
		} else {
			// Pick highest volatility overload.
			for _, o := range fn.Definition {
				if o.Volatility > fnVolatility {
					fnVolatility = o.Volatility
				}
			}
		}
		if fnVolatility <= volatility.Immutable {
			// Remaining immutable functions are safe.
			return fnCall, nil
		}

		return cdcFunctionWithOverride(fn.Name, fnCall)
	default:
		return nil, errors.AssertionFailedf("unexpected function expression of type %T", fn)
	}
}

// TestingEnableVolatileFunction allows functions with the given name (lowercase)
// to be used in expressions if their volatility level would disallow them by default.
// Used for testing.
func TestingEnableVolatileFunction(fnName string) {
	cdcFunctions[fnName] = useDefaultBuiltin
}

// For some functions (specifically the volatile ones), we do
// not want to use the provided builtin. Instead, we opt for
// our own function definition.
func makeBuiltinOverride(
	builtin *tree.FunctionDefinition, overloads ...tree.Overload,
) *tree.ResolvedFunctionDefinition {
	props := builtin.FunctionProperties
	override := tree.NewFunctionDefinition(builtin.Name, &props, overloads)
	// The schema name is actually not important since CDC doesn't use any user
	// defined functions. And, we're sure that we always return the first
	// function definition found.
	return tree.QualifyBuiltinFunctionDefinition(override, catconstants.PublicSchemaName)
}

// tree.Overload definitions for statement_timestamp and transaction_timestamp functions.
var timestampBuiltinOverloads = []tree.Overload{
	{
		Types:             tree.ParamTypes{},
		ReturnType:        tree.FixedReturnType(types.TimestampTZ),
		PreferredOverload: true,
		Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
			rowEvalCtx := rowEvalContextFromEvalContext(evalCtx)
			return tree.MakeDTimestampTZ(rowEvalCtx.updatedRow.MvccTimestamp.GoTime(), time.Microsecond)
		},
		Info: "Returns MVCC timestamp of the event",
		// NB: Default builtin implementation uses volatility.Stable
		// We override volatility to be Volatile so that function
		// is not folded.
		Volatility: volatility.Volatile,
	},
	{
		Types:      tree.ParamTypes{},
		ReturnType: tree.FixedReturnType(types.Timestamp),
		Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
			rowEvalCtx := rowEvalContextFromEvalContext(evalCtx)
			return tree.MakeDTimestamp(rowEvalCtx.updatedRow.MvccTimestamp.GoTime(), time.Microsecond)
		},
		Info: "Returns MVCC timestamp of the event",
		// NB: Default builtin implementation uses volatility.Stable
		// We override volatility to be Volatile so that function
		// is not folded.
		Volatility: volatility.Volatile,
	},
}
