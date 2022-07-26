// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cdceval

import (
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	jsonb "github.com/cockroachdb/cockroach/pkg/util/json"
)

// In general, we want to only support functions that produce the same
// value given the same data -- i.e. immutable functions.
// However, we can provide reasonable overrides to a small set of stable
// functions that make sense in the context of CDC.
var supportedVolatileBuiltinFunctions = makeStringSet(
	// These functions can be supported given that we set the statement and
	// transaction timestamp to be equal to MVCC timestamp of the event.
	"statement_timestamp",
	"transaction_timestamp",
	"timezone",

	// jsonb functions are stable because they depend on eval
	// context DataConversionConfig
	"jsonb_build_array",
	"jsonb_build_object",
	"to_json",
	"to_jsonb",
	"row_to_json",

	// Misc functions that depend on eval context.
	"overlaps",
	"pg_collation_for",
	"pg_typeof",
	"quote_literal",
	"quote_nullable",

	// TODO(yevgeniy): Support geometry.
	//"st_asgeojson",
	//"st_estimatedextent",
)

// CDC Specific functions.
// TODO(yevgeniy): Finalize function naming: e.g. cdc.is_delete() vs cdc_is_delete()
var cdcFunctions = map[string]*tree.FunctionDefinition{
	"cdc_is_delete": makeCDCBuiltIn(
		"cdc_is_delete",
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(evalCtx *eval.Context, datums tree.Datums) (tree.Datum, error) {
				rowEvalCtx := rowEvalContextFromEvalContext(evalCtx)
				if rowEvalCtx.updatedRow.IsDeleted() {
					return tree.DBoolTrue, nil
				}
				return tree.DBoolFalse, nil
			},
			Info:       "Returns true if the event is a deletion",
			Volatility: volatility.Stable,
		}),
	"cdc_mvcc_timestamp": cdcTimestampBuiltin(
		"cdc_mvcc_timestamp",
		"Returns event MVCC HLC timestamp",
		func(rowEvalCtx *rowEvalContext) hlc.Timestamp {
			return rowEvalCtx.mvccTS
		},
	),
	"cdc_updated_timestamp": cdcTimestampBuiltin(
		"cdc_updated_timestamp",
		"Returns event updated HLC timestamp",
		func(rowEvalCtx *rowEvalContext) hlc.Timestamp {
			return rowEvalCtx.updatedRow.SchemaTS
		},
	),
	"cdc_prev": makeCDCBuiltIn(
		"cdc_prev",
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Jsonb),
			Fn:         prevRowAsJSON,
			Info:       "Returns previous value of a row as JSONB",
			Volatility: volatility.Stable,
		}),
}

// TODO(yevgeniy): Implement additional functions (some ideas, not all should be implemented):
//   * cdc_is_delete is easy; what about update? does update include new events?
//   * cdc_is_new -- true if event is  a new row
//   * tuple overload (or cdc_prev_tuple) to return previous value as a tuple
//   * cdc_key -- effectively key_in_value where key columns returned as either a tuple or a json.
//   * cdc_key_cols -- return key column names;
//      * this can come in handy when working with jsonb; for example, emit previous JSONB excluding
//        key columns can be done with `SELECT cdc_prev() - cdc_key_cols()
//   * cdc_event_family_is(fam): return true if cdc event family is specified family; overload both for
//     family ID and family name.
//     function can be used to write complex conditionals when dealing with multi-family table(s)

var cdcFnProps = &tree.FunctionProperties{
	Category: "CDC builtin",
}

func makeCDCBuiltIn(fnName string, overloads ...tree.Overload) *tree.FunctionDefinition {
	return tree.NewFunctionDefinition(fnName, cdcFnProps, overloads)
}

func cdcTimestampBuiltin(
	fnName string, doc string, tsFn func(rowEvalCtx *rowEvalContext) hlc.Timestamp,
) *tree.FunctionDefinition {
	return tree.NewFunctionDefinition(
		fnName,
		cdcFnProps,
		[]tree.Overload{
			{
				Types:      tree.ArgTypes{},
				ReturnType: tree.FixedReturnType(types.Decimal),
				Fn: func(evalCtx *eval.Context, datums tree.Datums) (tree.Datum, error) {
					rowEvalCtx := rowEvalContextFromEvalContext(evalCtx)
					return eval.TimestampToDecimalDatum(tsFn(rowEvalCtx)), nil
				},
				Info:       doc,
				Volatility: volatility.Stable,
			},
		},
	)
}

func prevRowAsJSON(evalCtx *eval.Context, _ tree.Datums) (tree.Datum, error) {
	rec := rowEvalContextFromEvalContext(evalCtx)
	if rec.memo.prevJSON != nil {
		return rec.memo.prevJSON, nil
	}

	var prevJSON *tree.DJSON
	if rec.prevRow.IsInitialized() {
		b := jsonb.NewObjectBuilder(0)
		if err := rec.prevRow.ForEachColumn().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
			j, err := tree.AsJSON(d, sessiondatapb.DataConversionConfig{}, time.UTC)
			if err != nil {
				return err
			}
			b.Add(col.Name, j)
			return nil
		}); err != nil {
			return nil, err
		}
		prevJSON = tree.NewDJSON(b.Build())
	} else {
		prevJSON = tree.NewDJSON(jsonb.NullJSONValue)
	}

	rec.memo.prevJSON = prevJSON
	return prevJSON, nil
}

type cdcCustomFunctionResolver struct {
	tree.SearchPath
}

// Resolve implements tree.CustomFunctionDefinitionResolver
func (cdcCustomFunctionResolver) Resolve(name string) *tree.FunctionDefinition {
	fn, found := cdcFunctions[name]
	if found {
		return fn
	}
	fn, found = cdcFunctions[strings.ToLower(name)]
	if found {
		return fn
	}
	fn, found = tree.FunDefs[name]
	if found {
		return fn
	}
	return nil
}

func makeStringSet(vals ...string) map[string]struct{} {
	m := make(map[string]struct{}, len(vals))
	for _, v := range vals {
		m[v] = struct{}{}
	}
	return m
}
