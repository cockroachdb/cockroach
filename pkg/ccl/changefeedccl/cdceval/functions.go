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

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
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
	"age":                      useDefaultBuiltin,
	"array_to_json":            useDefaultBuiltin,
	"array_to_string":          useDefaultBuiltin,
	"crdb_internal.cluster_id": useDefaultBuiltin,
	"date_part":                useDefaultBuiltin,
	"date_trunc":               useDefaultBuiltin,
	"extract":                  useDefaultBuiltin,
	"format":                   useDefaultBuiltin,
	"jsonb_build_array":        useDefaultBuiltin,
	"jsonb_build_object":       useDefaultBuiltin,
	"row_to_json":              useDefaultBuiltin,
	"overlaps":                 useDefaultBuiltin,
	"pg_collation_for":         useDefaultBuiltin,
	"pg_typeof":                useDefaultBuiltin,
	"quote_literal":            useDefaultBuiltin,
	"quote_nullable":           useDefaultBuiltin,

	// {statement,transaction}_timestamp  functions can be supported given that we
	// set the statement and transaction timestamp to be equal to MVCC timestamp
	// of the event. However, we provide our own override which uses annotation to
	// return the MVCC timestamp of the update. In addition, the custom
	// implementation uses volatility.Volatile since doing so will cause optimizer
	// to (constant) fold these functions during optimization step -- something we
	// definitely don't want to do because we need to evaluate those functions for
	// each event.
	"statement_timestamp": cdcTimestampBuiltin(
		"statement_timestamp",
		"Returns MVCC timestamp of the event",
		volatility.Volatile,
		types.TimestampTZ,
		func(rowEvalCtx *rowEvalContext) hlc.Timestamp {
			return rowEvalCtx.updatedRow.MvccTimestamp
		},
	),
	"transaction_timestamp": cdcTimestampBuiltin(
		"transaction_timestamp",
		"Returns MVCC timestamp of the event",
		volatility.Volatile,
		types.TimestampTZ,
		func(rowEvalCtx *rowEvalContext) hlc.Timestamp {
			return rowEvalCtx.updatedRow.MvccTimestamp
		},
	),

	"timezone": useDefaultBuiltin,
	"to_char":  useDefaultBuiltin,
	"to_json":  useDefaultBuiltin,
	"to_jsonb": useDefaultBuiltin,

	// Misc functions that depend on eval context.
	// TODO(yevgeniy): Support geometry.
	//"st_asgeojson",
	//"st_estimatedextent",

	// NB: even though some cdc functions appear to be stable (e.g. cdc_is_delete()),
	// we should not mark custom CDC functions as stable.  Doing so will cause
	// optimizer to (constant) fold this function during optimization step -- something
	// we definitely don't want to do because we need to evaluate those functions
	// for each event.
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
			Info:       "Returns true if the event is a deletion",
			Volatility: volatility.Volatile,
		}),
	"cdc_mvcc_timestamp": cdcTimestampBuiltin(
		"cdc_mvcc_timestamp",
		"Returns MVCC timestamp of the event",
		volatility.Volatile,
		types.Decimal,
		func(rowEvalCtx *rowEvalContext) hlc.Timestamp {
			return rowEvalCtx.updatedRow.MvccTimestamp
		},
	),
	"cdc_updated_timestamp": cdcTimestampBuiltin(
		"cdc_updated_timestamp",
		"Returns schema timestamp of the event",
		volatility.Volatile,
		types.Decimal,
		func(rowEvalCtx *rowEvalContext) hlc.Timestamp {
			return rowEvalCtx.updatedRow.SchemaTS
		},
	),
	"changefeed_creation_timestamp": cdcTimestampBuiltin(
		"changefeed_creation_timestamp",
		"Returns changefeed creation time",
		volatility.Stable,
		types.Decimal,
		func(rowEvalCtx *rowEvalContext) hlc.Timestamp {
			return rowEvalCtx.startTime
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
	fnName string,
	doc string,
	v volatility.V,
	preferredOverloadReturnType *types.T,
	tsFn func(rowEvalCtx *rowEvalContext) hlc.Timestamp,
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
				Info:              doc + " as HLC timestamp",
				Volatility:        v,
				PreferredOverload: preferredOverloadReturnType == types.Decimal,
			},
			{
				Types:      tree.ParamTypes{},
				ReturnType: tree.FixedReturnType(types.TimestampTZ),
				Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
					rowEvalCtx := rowEvalContextFromEvalContext(evalCtx)
					return tree.MakeDTimestampTZ(tsFn(rowEvalCtx).GoTime(), time.Microsecond)
				},
				Info:              doc + " as TIMESTAMPTZ",
				Volatility:        v,
				PreferredOverload: preferredOverloadReturnType == types.TimestampTZ,
			},
			{
				Types:      tree.ParamTypes{},
				ReturnType: tree.FixedReturnType(types.Timestamp),
				Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
					rowEvalCtx := rowEvalContextFromEvalContext(evalCtx)
					return tree.MakeDTimestamp(tsFn(rowEvalCtx).GoTime(), time.Microsecond)
				},
				Info:              doc + " as TIMESTAMP",
				Volatility:        v,
				PreferredOverload: preferredOverloadReturnType == types.Timestamp,
			},
		},
	)

	// The schema name is actually not important since CDC doesn't use any user
	// defined functions. And, we're sure that we always return the first
	// function definition found.
	return tree.QualifyBuiltinFunctionDefinition(def, catconstants.PublicSchemaName)
}

// checkFunctionSupported checks if the function (expression) is supported.
func checkFunctionSupported(
	ctx context.Context, fnCall *tree.FuncExpr, semaCtx *tree.SemaContext,
) error {
	switch fn := fnCall.Func.FunctionReference.(type) {
	case *tree.UnresolvedName:
		// Unresolved names will be resolved later -- which performs
		// the check if the function supported by CDC.
		return nil
	case *tree.ResolvedFunctionDefinition:
		if _, denied := functionDenyList[fn.Name]; denied {
			return pgerror.Newf(pgcode.UndefinedFunction, "function %q unsupported by CDC", fn.Name)
		}
	case *tree.FunctionDefinition:
		if _, denied := functionDenyList[fn.Name]; denied {
			return pgerror.Newf(pgcode.UndefinedFunction, "function %q unsupported by CDC", fn.Name)
		}
	}
	return nil
}

// TestingDisableFunctionsBlacklist disables blacklist, thus allowing
// all functions, including volatile ones, to be used in changefeed expressions.
func TestingDisableFunctionsBlacklist() func() {
	old := functionDenyList
	functionDenyList = make(map[string]struct{})
	return func() {
		functionDenyList = old
	}
}

var functionDenyList = make(map[string]struct{})

func init() {
	for _, fnDef := range tree.ResolvedBuiltinFuncDefs {
		for _, overload := range fnDef.Overloads {
			switch overload.Volatility {
			case volatility.Volatile:
				// There is nothing that prevents a function from having
				// volatile and non-volatile overloads.  This, however, would be very
				// surprising (and, at least currently, there are no such functions).
				// Same argument applies to overload.Class -- seeing heterogeneous
				// class would be surprising.
				functionDenyList[fnDef.Name] = struct{}{}
			case volatility.Stable:
				// If the stable function is not on the white list,
				// then it is blacklisted.
				if _, whitelisted := cdcFunctions[fnDef.Name]; !whitelisted {
					functionDenyList[fnDef.Name] = struct{}{}
				}
			}

			switch overload.Class {
			case tree.AggregateClass, tree.GeneratorClass, tree.WindowClass:
				functionDenyList[fnDef.Name] = struct{}{}
			}
		}
	}
}
