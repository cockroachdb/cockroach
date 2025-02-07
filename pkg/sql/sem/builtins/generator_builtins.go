// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins

import (
	"bytes"
	"context"
	gojson "encoding/json"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/workloadindexrec"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/arith"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/randident"
	"github.com/cockroachdb/cockroach/pkg/util/randident/randidentcfg"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/rangedesc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// See the comments at the start of generators.go for details about
// this functionality.

var _ eval.ValueGenerator = &seriesValueGenerator{}
var _ eval.ValueGenerator = &arrayValueGenerator{}

func init() {
	for k, v := range generators {
		const enforceClass = true
		registerBuiltin(k, v, tree.GeneratorClass, enforceClass)
	}
}

const DefaultSpanStatsSpanLimit = 1000

// SpanStatsBatchLimit registers the maximum number of spans allowed in a
// span stats request payload.
var SpanStatsBatchLimit = settings.RegisterIntSetting(
	settings.ApplicationLevel,
	"server.span_stats.span_batch_limit",
	"the maximum number of spans allowed in a request payload for span statistics",
	DefaultSpanStatsSpanLimit,
	settings.PositiveInt,
)

// CheckConsistencyFatal will make crdb_internal.check_consistency() take
// storage checkpoints and terminate nodes on any inconsistencies when run with
// stats_only: false. This is the same as the background consistency checker,
// and is done by changing the mode CHECK_FULL to CHECK_VIA_QUEUE. This is for
// use in roachtest post-test assertions, to collect checkpoints on failures.
var CheckConsistencyFatal = envutil.EnvOrDefaultBool(
	"COCKROACH_INTERNAL_CHECK_CONSISTENCY_FATAL", false)

func genProps() tree.FunctionProperties {
	return tree.FunctionProperties{
		Category: builtinconstants.CategoryGenerator,
	}
}

func jsonGenPropsWithLabels(returnLabels []string) tree.FunctionProperties {
	return tree.FunctionProperties{
		Category:     builtinconstants.CategoryJSON,
		ReturnLabels: returnLabels,
	}
}

func recordGenProps() tree.FunctionProperties {
	return tree.FunctionProperties{
		Category:          builtinconstants.CategoryGenerator,
		ReturnsRecordType: true,
	}
}

var aclexplodeGeneratorType = types.MakeLabeledTuple(
	[]*types.T{types.Oid, types.Oid, types.String, types.Bool},
	[]string{"grantor", "grantee", "privilege_type", "is_grantable"},
)

// aclExplodeGenerator supports the execution of aclexplode.
type aclexplodeGenerator struct{}

func (aclexplodeGenerator) ResolvedType() *types.T                   { return aclexplodeGeneratorType }
func (aclexplodeGenerator) Start(_ context.Context, _ *kv.Txn) error { return nil }
func (aclexplodeGenerator) Close(_ context.Context)                  {}
func (aclexplodeGenerator) Next(_ context.Context) (bool, error)     { return false, nil }
func (aclexplodeGenerator) Values() (tree.Datums, error)             { return nil, nil }

// generators is a map from name to slice of Builtins for all built-in
// generators.
//
// These functions are identified with Class == tree.GeneratorClass.
// The properties are reachable via tree.FunctionDefinition.
var generators = map[string]builtinDefinition{
	// See https://www.postgresql.org/docs/9.6/static/functions-info.html.
	"aclexplode": makeBuiltin(genProps(),
		makeGeneratorOverload(
			tree.ParamTypes{{Name: "aclitems", Typ: types.StringArray}},
			aclexplodeGeneratorType,
			func(_ context.Context, _ *eval.Context, args tree.Datums) (eval.ValueGenerator, error) {
				return aclexplodeGenerator{}, nil
			},
			"Produces a virtual table containing aclitem stuff ("+
				"returns no rows as this feature is unsupported in CockroachDB)",
			volatility.Stable,
		),
	),
	"crdb_internal.scan": makeBuiltin(genProps(),
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "start_key", Typ: types.Bytes},
				{Name: "end_key", Typ: types.Bytes},
			},
			spanKeyIteratorType,
			func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (eval.ValueGenerator, error) {
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}
				startKey := []byte(tree.MustBeDBytes(args[0]))
				endKey := []byte(tree.MustBeDBytes(args[1]))
				return newSpanKeyIterator(evalCtx, roachpb.Span{
					Key:    startKey,
					EndKey: endKey,
				}), nil
			},
			"Returns the raw keys and values with their timestamp from the specified span",
			volatility.Stable,
		),
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "span", Typ: types.BytesArray},
			},
			spanKeyIteratorType,
			func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (eval.ValueGenerator, error) {
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}
				arr := tree.MustBeDArray(args[0])
				if arr.Len() != 2 {
					return nil, errors.New("expected an array of two elements")
				}
				startKey := []byte(tree.MustBeDBytes(arr.Array[0]))
				endKey := []byte(tree.MustBeDBytes(arr.Array[1]))
				return newSpanKeyIterator(evalCtx, roachpb.Span{
					Key:    startKey,
					EndKey: endKey,
				}), nil
			},
			"Returns the raw keys and values from the specified span",
			volatility.Stable,
		),
	),
	"generate_series": makeBuiltin(genProps(),
		// See https://www.postgresql.org/docs/current/static/functions-srf.html#FUNCTIONS-SRF-SERIES
		makeGeneratorOverload(
			tree.ParamTypes{{Name: "start", Typ: types.Int}, {Name: "end", Typ: types.Int}},
			seriesValueGeneratorType,
			makeSeriesGenerator,
			"Produces a virtual table containing the integer values from `start` to `end`, inclusive.",
			volatility.Immutable,
		),
		makeGeneratorOverload(
			tree.ParamTypes{{Name: "start", Typ: types.Int}, {Name: "end", Typ: types.Int}, {Name: "step", Typ: types.Int}},
			seriesValueGeneratorType,
			makeSeriesGenerator,
			"Produces a virtual table containing the integer values from `start` to `end`, inclusive, by increment of `step`.",
			volatility.Immutable,
		),
		makeGeneratorOverload(
			tree.ParamTypes{{Name: "start", Typ: types.Timestamp}, {Name: "end", Typ: types.Timestamp}, {Name: "step", Typ: types.Interval}},
			seriesTSValueGeneratorType,
			makeTSSeriesGenerator,
			"Produces a virtual table containing the timestamp values from `start` to `end`, inclusive, by increment of `step`.",
			volatility.Immutable,
		),
		makeGeneratorOverload(
			tree.ParamTypes{{Name: "start", Typ: types.TimestampTZ}, {Name: "end", Typ: types.TimestampTZ}, {Name: "step", Typ: types.Interval}},
			seriesTSTZValueGeneratorType,
			makeTSTZSeriesGenerator,
			"Produces a virtual table containing the timestampTZ values from `start` to `end`, inclusive, by increment of `step`.",
			volatility.Immutable,
		),
	),
	// crdb_internal.testing_callback is a generator function intended for internal unit tests.
	// You give it a name and it calls a callback that had to have been installed
	// on a TestServer through its eval.TestingKnobs.CallbackGenerators.
	"crdb_internal.testing_callback": makeBuiltin(genProps(),
		makeGeneratorOverload(
			tree.ParamTypes{{Name: "name", Typ: types.String}},
			types.Int,
			func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (eval.ValueGenerator, error) {
				s, ok := tree.AsDString(args[0])
				if !ok {
					return nil, errors.Newf("expected string value, got %T", args[0])
				}
				name := string(s)
				gen, ok := evalCtx.TestingKnobs.CallbackGenerators[name]
				if !ok {
					return nil, errors.Errorf("callback %q not registered", name)
				}
				return gen, nil
			},
			"For internal CRDB testing only. "+
				"The function calls a callback identified by `name` registered with the server by "+
				"the test.",
			volatility.Volatile,
		),
	),

	"pg_get_keywords": makeBuiltin(genProps(),
		// See https://www.postgresql.org/docs/10/static/functions-info.html#FUNCTIONS-INFO-CATALOG-TABLE
		makeGeneratorOverload(
			tree.ParamTypes{},
			keywordsValueGeneratorType,
			makeKeywordsGenerator,
			"Produces a virtual table containing the keywords known to the SQL parser.",
			volatility.Immutable,
		),
	),
	`pg_options_to_table`: makeBuiltin(
		genProps(),
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "options", Typ: types.MakeArray(types.String)},
			},
			optionsToOverloadGeneratorType,
			makeOptionsToTableGenerator,
			"Converts the options array format to a table.",
			// This is stable in PG, even though it's implemented immutability here.
			// It is probably related to character encodings being configurable in PG.
			volatility.Stable,
		),
	),

	"regexp_split_to_table": makeBuiltin(
		genProps(),
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "string", Typ: types.String},
				{Name: "pattern", Typ: types.String},
			},
			types.String,
			makeRegexpSplitToTableGeneratorFactory(false /* hasFlags */),
			"Split string using a POSIX regular expression as the delimiter.",
			volatility.Immutable,
		),
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "string", Typ: types.String},
				{Name: "pattern", Typ: types.String},
				{Name: "flags", Typ: types.String},
			},
			types.String,
			makeRegexpSplitToTableGeneratorFactory(true /* hasFlags */),
			"Split string using a POSIX regular expression as the delimiter with flags."+regexpFlagInfo,
			volatility.Immutable,
		),
	),

	"workload_index_recs": makeBuiltin(genProps(),
		makeGeneratorOverload(
			tree.ParamTypes{},
			types.String,
			makeWorkloadIndexRecsGeneratorFactory(false /* hasTimestamp */),
			"Returns set of index recommendations",
			volatility.Immutable,
		),
		makeGeneratorOverload(
			tree.ParamTypes{{Name: "timestamptz", Typ: types.TimestampTZ}},
			types.String,
			makeWorkloadIndexRecsGeneratorFactory(true /* hasTimestamp */),
			"Returns set of index recommendations",
			volatility.Immutable,
		),
	),

	"unnest": makeBuiltin(genProps(),
		// See https://www.postgresql.org/docs/current/static/functions-array.html
		makeGeneratorOverloadWithReturnType(
			tree.ParamTypes{{Name: "input", Typ: types.AnyArray}},
			func(args []tree.TypedExpr) *types.T {
				if len(args) == 0 || args[0].ResolvedType().Family() != types.ArrayFamily {
					return tree.UnknownReturnType
				}
				return args[0].ResolvedType().ArrayContents()
			},
			makeArrayGenerator,
			"Returns the input array as a set of rows",
			volatility.Immutable,
		),
		makeGeneratorOverloadWithReturnType(
			tree.VariadicType{
				FixedTypes: []*types.T{types.AnyArray, types.AnyArray},
				VarType:    types.AnyArray,
			},
			// TODO(rafiss): update this or docgen so that functions.md shows the
			// return type as variadic.
			func(args []tree.TypedExpr) *types.T {
				returnTypes := make([]*types.T, len(args))
				labels := make([]string, len(args))
				for i, arg := range args {
					if arg.ResolvedType().Family() != types.ArrayFamily {
						return tree.UnknownReturnType
					}
					returnTypes[i] = arg.ResolvedType().ArrayContents()
					labels[i] = "unnest"
				}
				return types.MakeLabeledTuple(returnTypes, labels)
			},
			makeVariadicUnnestGenerator,
			"Returns the input arrays as a set of rows",
			volatility.Immutable,
		),
	),

	"information_schema._pg_expandarray": makeBuiltin(genProps(),
		makeGeneratorOverloadWithReturnType(
			tree.ParamTypes{{Name: "input", Typ: types.AnyArray}},
			func(args []tree.TypedExpr) *types.T {
				if len(args) == 0 || args[0].ResolvedType().Family() != types.ArrayFamily {
					return tree.UnknownReturnType
				}
				t := args[0].ResolvedType().ArrayContents()
				return types.MakeLabeledTuple([]*types.T{t, types.Int}, expandArrayValueGeneratorLabels)
			},
			makeExpandArrayGenerator,
			"Returns the input array as a set of rows with an index",
			volatility.Immutable,
		),
	),

	"crdb_internal.unary_table": makeBuiltin(genProps(),
		makeGeneratorOverload(
			tree.ParamTypes{},
			unaryValueGeneratorType,
			makeUnaryGenerator,
			"Produces a virtual table containing a single row with no values.\n\n"+
				"This function is used only by CockroachDB's developers for testing purposes.",
			volatility.Volatile,
		),
	),

	"generate_subscripts": makeBuiltin(genProps(),
		// See https://www.postgresql.org/docs/current/static/functions-srf.html#FUNCTIONS-SRF-SUBSCRIPTS
		makeGeneratorOverload(
			tree.ParamTypes{{Name: "array", Typ: types.AnyArray}},
			subscriptsValueGeneratorType,
			makeGenerateSubscriptsGenerator,
			"Returns a series comprising the given array's subscripts.",
			volatility.Immutable,
		),
		makeGeneratorOverload(
			tree.ParamTypes{{Name: "array", Typ: types.AnyArray}, {Name: "dim", Typ: types.Int}},
			subscriptsValueGeneratorType,
			makeGenerateSubscriptsGenerator,
			"Returns a series comprising the given array's subscripts.",
			volatility.Immutable,
		),
		makeGeneratorOverload(
			tree.ParamTypes{{Name: "array", Typ: types.AnyArray}, {Name: "dim", Typ: types.Int}, {Name: "reverse", Typ: types.Bool}},
			subscriptsValueGeneratorType,
			makeGenerateSubscriptsGenerator,
			"Returns a series comprising the given array's subscripts.\n\n"+
				"When reverse is true, the series is returned in reverse order.",
			volatility.Immutable,
		),
	),

	"json_array_elements":       makeBuiltin(jsonGenPropsWithLabels(jsonArrayGeneratorLabels), jsonArrayElementsImpl),
	"jsonb_array_elements":      makeBuiltin(jsonGenPropsWithLabels(jsonArrayGeneratorLabels), jsonArrayElementsImpl),
	"json_array_elements_text":  makeBuiltin(jsonGenPropsWithLabels(jsonArrayGeneratorLabels), jsonArrayElementsTextImpl),
	"jsonb_array_elements_text": makeBuiltin(jsonGenPropsWithLabels(jsonArrayGeneratorLabels), jsonArrayElementsTextImpl),
	"json_object_keys":          makeBuiltin(genProps(), jsonObjectKeysImpl),
	"jsonb_object_keys":         makeBuiltin(genProps(), jsonObjectKeysImpl),
	"json_each":                 makeBuiltin(jsonGenPropsWithLabels(jsonEachGeneratorLabels), jsonEachImpl),
	"jsonb_each":                makeBuiltin(jsonGenPropsWithLabels(jsonEachGeneratorLabels), jsonEachImpl),
	"json_each_text":            makeBuiltin(jsonGenPropsWithLabels(jsonEachGeneratorLabels), jsonEachTextImpl),
	"jsonb_each_text":           makeBuiltin(jsonGenPropsWithLabels(jsonEachGeneratorLabels), jsonEachTextImpl),
	"json_populate_record": makeBuiltin(jsonPopulateProps, makeJSONPopulateImpl(makeJSONPopulateRecordGenerator,
		"Expands the object in from_json to a row whose columns match the record type defined by base.",
	)),
	"jsonb_populate_record": makeBuiltin(jsonPopulateProps, makeJSONPopulateImpl(makeJSONPopulateRecordGenerator,
		"Expands the object in from_json to a row whose columns match the record type defined by base.",
	)),
	"json_populate_recordset": makeBuiltin(jsonPopulateProps, makeJSONPopulateImpl(makeJSONPopulateRecordSetGenerator,
		"Expands the outermost array of objects in from_json to a set of rows whose columns match the record type defined by base")),
	"jsonb_populate_recordset": makeBuiltin(jsonPopulateProps, makeJSONPopulateImpl(makeJSONPopulateRecordSetGenerator,
		"Expands the outermost array of objects in from_json to a set of rows whose columns match the record type defined by base")),

	"json_to_record":     makeBuiltin(recordGenProps(), jsonToRecordImpl),
	"jsonb_to_record":    makeBuiltin(recordGenProps(), jsonToRecordImpl),
	"json_to_recordset":  makeBuiltin(recordGenProps(), jsonToRecordSetImpl),
	"jsonb_to_recordset": makeBuiltin(recordGenProps(), jsonToRecordSetImpl),

	"crdb_internal.check_consistency": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategorySystemInfo,
			DistsqlBlocklist: true, // see #88222
		},
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "stats_only", Typ: types.Bool},
				{Name: "start_key", Typ: types.Bytes},
				{Name: "end_key", Typ: types.Bytes},
			},
			checkConsistencyGeneratorType,
			makeCheckConsistencyGenerator,
			"Runs a consistency check on ranges touching the specified key range. "+
				"an empty start or end key is treated as the minimum and maximum possible, "+
				"respectively. stats_only should only be set to false when targeting a "+
				"small number of ranges to avoid overloading the cluster. Each returned row "+
				"contains the range ID, the status (a roachpb.CheckConsistencyResponse_Status), "+
				"and verbose detail.\n\n"+
				"Example usage:\n\n"+
				"`SELECT * FROM crdb_internal.check_consistency(true, b'\\x02', b'\\x04')`",
			volatility.Volatile,
		),
	),

	"crdb_internal.list_sql_keys_in_range": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "range_id", Typ: types.Int},
			},
			rangeKeyIteratorType,
			makeRangeKeyIterator,
			"Returns all SQL K/V pairs within the requested range.",
			volatility.Volatile,
		),
	),

	"crdb_internal.payloads_for_span": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "span_id", Typ: types.Int},
			},
			payloadsForSpanGeneratorType,
			makePayloadsForSpanGenerator,
			"Returns the payload(s) of the requested span and all its children.",
			volatility.Volatile,
		),
	),
	"crdb_internal.payloads_for_trace": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "trace_id", Typ: types.Int},
			},
			payloadsForTraceGeneratorType,
			makePayloadsForTraceGenerator,
			"Returns the payload(s) of the requested trace.",
			volatility.Volatile,
		),
	),
	"crdb_internal.show_create_all_schemas": makeBuiltin(
		tree.FunctionProperties{},
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "database_name", Typ: types.String},
			},
			showCreateAllSchemasGeneratorType,
			makeShowCreateAllSchemasGenerator,
			`Returns rows of CREATE schema statements.
The output can be used to recreate a database.'
`,
			volatility.Volatile,
		),
	),
	"crdb_internal.show_create_all_tables": makeBuiltin(
		tree.FunctionProperties{},
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "database_name", Typ: types.String},
			},
			showCreateAllTablesGeneratorType,
			makeShowCreateAllTablesGenerator,
			`Returns rows of CREATE table statements followed by
ALTER table statements that add table constraints. The rows are ordered
by dependencies. All foreign keys are added after the creation of the table
in the alter statements.
It is not recommended to perform this operation on a database with many
tables.
The output can be used to recreate a database.'
`,
			volatility.Volatile,
		),
	),
	"crdb_internal.show_create_all_types": makeBuiltin(
		tree.FunctionProperties{},
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "database_name", Typ: types.String},
			},
			showCreateAllTypesGeneratorType,
			makeShowCreateAllTypesGenerator,
			`Returns rows of CREATE type statements.
The output can be used to recreate a database.'
`,
			volatility.Volatile,
		),
	),
	"crdb_internal.decode_plan_gist": makeBuiltin(
		tree.FunctionProperties{},
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "gist", Typ: types.String},
			},
			decodePlanGistGeneratorType,
			makeDecodePlanGistGenerator,
			`Returns rows of output similar to EXPLAIN from a gist such as those found in planGists element of the statistics column of the statement_statistics table.
			`,
			volatility.Volatile,
		),
	),
	"crdb_internal.decode_external_plan_gist": makeBuiltin(
		tree.FunctionProperties{},
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "gist", Typ: types.String},
			},
			decodePlanGistGeneratorType,
			makeDecodeExternalPlanGistGenerator,
			`Returns rows of output similar to EXPLAIN from a gist such as those found in planGists element of the statistics column of the statement_statistics table without attempting to resolve tables or indexes.
			`,
			volatility.Volatile,
		),
	),
	"crdb_internal.gen_rand_ident": makeBuiltin(
		tree.FunctionProperties{},
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "name_pattern", Typ: types.String},
				{Name: "count", Typ: types.Int},
			},
			types.String,
			func(ctx context.Context, evalCtx *eval.Context, args tree.Datums,
			) (eval.ValueGenerator, error) {
				return makeIdentGenerator(ctx, evalCtx, args[0], args[1], nil)
			},
			`Returns random SQL identifiers.

gen_rand_ident(pattern, count) is an alias for gen_rand_ident(pattern, count, '').
See the documentation of the other gen_rand_ident overload for details.
`,
			volatility.Volatile,
		),
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "name_pattern", Typ: types.String},
				{Name: "count", Typ: types.Int},
				{Name: "parameters", Typ: types.Jsonb},
			},
			types.String,
			func(ctx context.Context, evalCtx *eval.Context, args tree.Datums,
			) (eval.ValueGenerator, error) {
				return makeIdentGenerator(ctx, evalCtx, args[0], args[1], args[2])
			},
			`Returns count random SQL identifiers that resemble the name_pattern.

The last argument is a JSONB object containing the following optional fields:
- "seed": the seed to use for the pseudo-random generator (default: random).`+
				randidentcfg.ConfigDoc,
			volatility.Volatile,
		),
	),
	"crdb_internal.tenant_span_stats": makeBuiltin(genProps(),
		// This overload defines a built-in that returns the range count,
		// approximate disk size, live range bytes, total range bytes,
		// and live range byte percentage for all tables that belong to the
		// tenant executing the statement. It is invoked without arguments.
		// e.g. `SELECT * FROM crdb_internal.tenant_span_stats();`
		makeGeneratorOverload(
			tree.ParamTypes{},
			tableSpanStatsGeneratorType,
			makeTableSpanStatsGenerator,
			"Returns statistics (range count, disk size, live range bytes, total range bytes, live range byte percentage) for all of the tenant's tables.",
			volatility.Stable,
		),
		// This overload defines a built-in that returns the range count,
		// approximate disk size, live range bytes, total range bytes,
		// and live range byte percentage for all tables that belong to the
		// database specified. The database is specified by its descriptor id.
		// e.g. `SELECT * FROM crdb_internal.tenant_span_stats(104);`
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "database_id", Typ: types.Int},
			},
			tableSpanStatsGeneratorType,
			makeTableSpanStatsGenerator,
			"Returns statistics (range count, disk size, live range bytes, total range bytes, live range byte percentage) for tables of the provided database id.",
			volatility.Stable,
		),
		// This overload defines a built-in that returns the range count,
		// approximate disk size, live range bytes, total range bytes,
		// and live range byte percentage for all tables that belong to the
		// database and table specified. The database and table are specified
		// by their descriptor ids.
		// e.g. `SELECT * FROM crdb_internal.tenant_span_stats(104, 106);`
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "database_id", Typ: types.Int},
				{Name: "table_id", Typ: types.Int},
			},
			tableSpanStatsGeneratorType,
			makeTableSpanStatsGenerator,
			"Returns statistics (range count, disk size, live range bytes, total range bytes, live range byte percentage) for the provided table id.",
			volatility.Stable,
		),
		// This overload defines a built-in that returns roachpb.SpanStats for
		// the spans provided.
		// e.g. `SELECT * FROM crdb_internal.tenant_span_stats(ARRAY(SELECT('\xfe8a'::bytes, '\xfe8b'::bytes)));`
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "spans", Typ: types.AnyTupleArray},
			},
			spanStatsGeneratorType,
			makeSpanStatsGenerator,
			"Returns SpanStats for the provided spans.",
			volatility.Stable,
		),
	),
	"crdb_internal.sstable_metrics": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "node_id", Typ: types.Int},
				{Name: "store_id", Typ: types.Int},
				{Name: "start_key", Typ: types.Bytes},
				{Name: "end_key", Typ: types.Bytes},
			},
			tableMetricsGeneratorType,
			makeTableMetricsGenerator,
			"Returns statistics for the sstables containing keys in the range start_key and end_key for the provided node id.",
			volatility.Stable,
		),
	),
	"crdb_internal.scan_storage_internal_keys": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "node_id", Typ: types.Int},
				{Name: "store_id", Typ: types.Int},
				{Name: "start_key", Typ: types.Bytes},
				{Name: "end_key", Typ: types.Bytes},
			},
			storageInternalKeysGeneratorType,
			makeStorageInternalKeysGenerator,
			"Scans a store's storage engine, computing statistics describing the internal keys within the span [start_key, end_key). This function is rate limited to 10 megabytes per second.",
			volatility.Volatile,
		),
		makeGeneratorOverload(
			tree.ParamTypes{
				{Name: "node_id", Typ: types.Int},
				{Name: "store_id", Typ: types.Int},
				{Name: "start_key", Typ: types.Bytes},
				{Name: "end_key", Typ: types.Bytes},
				{Name: "mb_per_second", Typ: types.Int4},
			},
			storageInternalKeysGeneratorType,
			makeStorageInternalKeysGenerator,
			"Scans a store's storage engine, computing statistics describing the internal keys within the span [start_key, end_key).",
			volatility.Volatile,
		),
	),
	"crdb_internal.execute_internally": makeBuiltin(
		tree.FunctionProperties{
			Undocumented: true,
			Category:     builtinconstants.CategoryGenerator,
		},
		makeInternallyExecutedQueryGeneratorOverload(false /* withSessionBound */, false /* withOverrides */, false /* withTxn */),
		makeInternallyExecutedQueryGeneratorOverload(true /* withSessionBound */, false /* withOverrides */, false /* withTxn */),
		makeInternallyExecutedQueryGeneratorOverload(false /* withSessionBound */, true /* withOverrides */, false /* withTxn */),
		makeInternallyExecutedQueryGeneratorOverload(true /* withSessionBound */, true /* withOverrides */, false /* withTxn */),
		makeInternallyExecutedQueryGeneratorOverload(false /* withSessionBound */, true /* withOverrides */, true /* withTxn */),
		makeInternallyExecutedQueryGeneratorOverload(true /* withSessionBound */, true /* withOverrides */, true /* withTxn */),
	),
}

var decodePlanGistGeneratorType = types.String

type gistPlanGenerator struct {
	gist     string
	index    int
	rows     []string
	evalCtx  *eval.Context
	external bool
}

var _ eval.ValueGenerator = &gistPlanGenerator{}

func (g *gistPlanGenerator) ResolvedType() *types.T {
	return types.String
}

func (g *gistPlanGenerator) Start(ctx context.Context, _ *kv.Txn) error {
	rows, err := g.evalCtx.Planner.DecodeGist(ctx, g.gist, g.external)
	if err != nil {
		return err
	}
	g.rows = rows
	g.index = -1
	return nil
}

func (g *gistPlanGenerator) Next(context.Context) (bool, error) {
	g.index++
	return g.index < len(g.rows), nil
}

func (g *gistPlanGenerator) Close(context.Context) {}

// Values implements the eval.ValueGenerator interface.
func (g *gistPlanGenerator) Values() (tree.Datums, error) {
	return tree.Datums{tree.NewDString(g.rows[g.index])}, nil
}

func makeDecodePlanGistGenerator(
	ctx context.Context, evalCtx *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	gist := string(tree.MustBeDString(args[0]))
	return &gistPlanGenerator{gist: gist, evalCtx: evalCtx, external: false}, nil
}

func makeDecodeExternalPlanGistGenerator(
	ctx context.Context, evalCtx *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	gist := string(tree.MustBeDString(args[0]))
	return &gistPlanGenerator{gist: gist, evalCtx: evalCtx, external: true}, nil
}

func makeGeneratorOverload(
	in tree.TypeList, ret *types.T, g eval.GeneratorOverload, info string, volatility volatility.V,
) tree.Overload {
	return makeGeneratorOverloadWithReturnType(in, tree.FixedReturnType(ret), g, info, volatility)
}

var unsuitableUseOfGeneratorFn = func(_ context.Context, _ *eval.Context, _ tree.Datums) (tree.Datum, error) {
	return nil, errors.AssertionFailedf("generator functions cannot be evaluated as scalars")
}

var unsuitableUseOfGeneratorFnWithExprs eval.FnWithExprsOverload = func(
	_ context.Context, _ *eval.Context, _ tree.Exprs,
) (tree.Datum, error) {
	return nil, errors.AssertionFailedf("generator functions cannot be evaluated as scalars")
}

func makeGeneratorOverloadWithReturnType(
	in tree.TypeList,
	retType tree.ReturnTyper,
	g eval.GeneratorOverload,
	info string,
	volatility volatility.V,
) tree.Overload {
	return tree.Overload{
		Types:      in,
		ReturnType: retType,
		Generator:  g,
		Class:      tree.GeneratorClass,
		Info:       info,
		Volatility: volatility,
	}
}

// regexpSplitToTableGenerator supports regexp_split_to_table.
type regexpSplitToTableGenerator struct {
	words []string
	curr  int
}

func makeRegexpSplitToTableGeneratorFactory(hasFlags bool) eval.GeneratorOverload {
	return func(
		ctx context.Context, evalCtx *eval.Context, args tree.Datums,
	) (eval.ValueGenerator, error) {
		words, err := regexpSplit(evalCtx, args, hasFlags)
		if err != nil {
			return nil, err
		}
		return &regexpSplitToTableGenerator{
			words: words,
			curr:  -1,
		}, nil
	}
}

// ResolvedType implements the eval.ValueGenerator interface.
func (*regexpSplitToTableGenerator) ResolvedType() *types.T { return types.String }

// Close implements the eval.ValueGenerator interface.
func (*regexpSplitToTableGenerator) Close(_ context.Context) {}

// Start implements the eval.ValueGenerator interface.
func (g *regexpSplitToTableGenerator) Start(_ context.Context, _ *kv.Txn) error {
	g.curr = -1
	return nil
}

// Next implements the eval.ValueGenerator interface.
func (g *regexpSplitToTableGenerator) Next(_ context.Context) (bool, error) {
	g.curr++
	return g.curr < len(g.words), nil
}

// Values implements the eval.ValueGenerator interface.
func (g *regexpSplitToTableGenerator) Values() (tree.Datums, error) {
	return tree.Datums{tree.NewDString(g.words[g.curr])}, nil
}

type optionsToTableGenerator struct {
	arr *tree.DArray
	idx int
}

func makeOptionsToTableGenerator(
	_ context.Context, _ *eval.Context, d tree.Datums,
) (eval.ValueGenerator, error) {
	arr := tree.MustBeDArray(d[0])
	return &optionsToTableGenerator{arr: arr, idx: -1}, nil
}

var optionsToOverloadGeneratorType = types.MakeLabeledTuple(
	[]*types.T{types.String, types.String},
	[]string{"option_name", "option_value"},
)

// ResolvedType implements the eval.ValueGenerator interface.
func (*optionsToTableGenerator) ResolvedType() *types.T {
	return optionsToOverloadGeneratorType
}

// Close implements the eval.ValueGenerator interface.
func (*optionsToTableGenerator) Close(_ context.Context) {}

// Start implements the eval.ValueGenerator interface.
func (g *optionsToTableGenerator) Start(_ context.Context, _ *kv.Txn) error {
	return nil
}

// Next implements the eval.ValueGenerator interface.
func (g *optionsToTableGenerator) Next(_ context.Context) (bool, error) {
	g.idx++
	if g.idx >= g.arr.Len() {
		return false, nil
	}
	return true, nil
}

// Values implements the eval.ValueGenerator interface.
func (g *optionsToTableGenerator) Values() (tree.Datums, error) {
	elem := g.arr.Array[g.idx]

	if elem == tree.DNull {
		return nil, pgerror.Newf(
			pgcode.InvalidParameterValue,
			"null array element not allowed in this context",
		)
	}
	s := string(tree.MustBeDString(elem))
	split := strings.SplitN(s, "=", 2)
	ret := make(tree.Datums, 2)

	ret[0] = tree.NewDString(split[0])
	if len(split) == 2 {
		ret[1] = tree.NewDString(split[1])
	} else {
		ret[1] = tree.DNull
	}
	return ret, nil
}

// keywordsValueGenerator supports the execution of pg_get_keywords().
type keywordsValueGenerator struct {
	curKeyword int
}

var keywordsValueGeneratorType = types.MakeLabeledTuple(
	[]*types.T{types.String, types.String, types.String},
	[]string{"word", "catcode", "catdesc"},
)

func makeKeywordsGenerator(
	_ context.Context, _ *eval.Context, _ tree.Datums,
) (eval.ValueGenerator, error) {
	return &keywordsValueGenerator{}, nil
}

// ResolvedType implements the eval.ValueGenerator interface.
func (*keywordsValueGenerator) ResolvedType() *types.T { return keywordsValueGeneratorType }

// Close implements the eval.ValueGenerator interface.
func (*keywordsValueGenerator) Close(_ context.Context) {}

// Start implements the eval.ValueGenerator interface.
func (k *keywordsValueGenerator) Start(_ context.Context, _ *kv.Txn) error {
	k.curKeyword = -1
	return nil
}

// Next implements the eval.ValueGenerator interface.
func (k *keywordsValueGenerator) Next(_ context.Context) (bool, error) {
	k.curKeyword++
	return k.curKeyword < len(lexbase.KeywordNames), nil
}

// Values implements the eval.ValueGenerator interface.
func (k *keywordsValueGenerator) Values() (tree.Datums, error) {
	kw := lexbase.KeywordNames[k.curKeyword]
	cat := lexbase.KeywordsCategories[kw]
	desc := keywordCategoryDescriptions[cat]
	return tree.Datums{tree.NewDString(kw), tree.NewDString(cat), tree.NewDString(desc)}, nil
}

var keywordCategoryDescriptions = map[string]string{
	"R": "reserved",
	"C": "unreserved (cannot be function or type name)",
	"T": "reserved (can be function or type name)",
	"U": "unreserved",
}

// seriesValueGenerator supports the execution of generate_series()
// with integer bounds.
type seriesValueGenerator struct {
	origStart, value, start, stop, step interface{}
	nextOK                              bool
	genType                             *types.T
	next                                func(*seriesValueGenerator) (bool, error)
	genValue                            func(*seriesValueGenerator) (tree.Datums, error)
}

var seriesValueGeneratorType = types.Int

var seriesTSValueGeneratorType = types.Timestamp

var seriesTSTZValueGeneratorType = types.TimestampTZ

var errStepCannotBeZero = pgerror.New(pgcode.InvalidParameterValue, "step cannot be 0")

func seriesIntNext(s *seriesValueGenerator) (bool, error) {
	step := s.step.(int64)
	start := s.start.(int64)
	stop := s.stop.(int64)

	if !s.nextOK {
		return false, nil
	}
	if step < 0 && (start < stop) {
		return false, nil
	}
	if step > 0 && (stop < start) {
		return false, nil
	}
	s.value = start
	s.start, s.nextOK = arith.AddWithOverflow(start, step)
	return true, nil
}

func seriesGenIntValue(s *seriesValueGenerator) (tree.Datums, error) {
	return tree.Datums{tree.NewDInt(tree.DInt(s.value.(int64)))}, nil
}

// seriesTSNext performs calendar-aware math.
func seriesTSNext(s *seriesValueGenerator) (bool, error) {
	step := s.step.(duration.Duration)
	start := s.start.(time.Time)
	stop := s.stop.(time.Time)

	if !s.nextOK {
		return false, nil
	}

	stepForward := step.Compare(duration.Duration{}) > 0
	if !stepForward && (start.Before(stop)) {
		return false, nil
	}
	if stepForward && (stop.Before(start)) {
		return false, nil
	}

	s.value = start
	s.start = duration.Add(start, step)
	return true, nil
}

func seriesGenTSValue(s *seriesValueGenerator) (tree.Datums, error) {
	ts, err := tree.MakeDTimestamp(s.value.(time.Time), time.Microsecond)
	if err != nil {
		return nil, err
	}
	return tree.Datums{ts}, nil
}

func seriesGenTSTZValue(s *seriesValueGenerator) (tree.Datums, error) {
	ts, err := tree.MakeDTimestampTZ(s.value.(time.Time), time.Microsecond)
	if err != nil {
		return nil, err
	}
	return tree.Datums{ts}, nil
}

func makeSeriesGenerator(
	_ context.Context, _ *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	start := int64(tree.MustBeDInt(args[0]))
	stop := int64(tree.MustBeDInt(args[1]))
	step := int64(1)
	if len(args) > 2 {
		step = int64(tree.MustBeDInt(args[2]))
	}
	if step == 0 {
		return nil, errStepCannotBeZero
	}
	return &seriesValueGenerator{
		origStart: start,
		stop:      stop,
		step:      step,
		genType:   seriesValueGeneratorType,
		genValue:  seriesGenIntValue,
		next:      seriesIntNext,
	}, nil
}

func makeTSSeriesGenerator(
	_ context.Context, _ *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	start := args[0].(*tree.DTimestamp).Time
	stop := args[1].(*tree.DTimestamp).Time
	step := args[2].(*tree.DInterval).Duration

	if step.Compare(duration.Duration{}) == 0 {
		return nil, errStepCannotBeZero
	}

	return &seriesValueGenerator{
		origStart: start,
		stop:      stop,
		step:      step,
		genType:   seriesTSValueGeneratorType,
		genValue:  seriesGenTSValue,
		next:      seriesTSNext,
	}, nil
}

func makeTSTZSeriesGenerator(
	_ context.Context, _ *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	start := args[0].(*tree.DTimestampTZ).Time
	stop := args[1].(*tree.DTimestampTZ).Time
	step := args[2].(*tree.DInterval).Duration

	if step.Compare(duration.Duration{}) == 0 {
		return nil, errStepCannotBeZero
	}

	return &seriesValueGenerator{
		origStart: start,
		stop:      stop,
		step:      step,
		genType:   seriesTSTZValueGeneratorType,
		genValue:  seriesGenTSTZValue,
		next:      seriesTSNext,
	}, nil
}

// ResolvedType implements the eval.ValueGenerator interface.
func (s *seriesValueGenerator) ResolvedType() *types.T {
	return s.genType
}

// Start implements the eval.ValueGenerator interface.
func (s *seriesValueGenerator) Start(_ context.Context, _ *kv.Txn) error {
	s.nextOK = true
	s.start = s.origStart
	s.value = s.origStart
	return nil
}

// Close implements the eval.ValueGenerator interface.
func (s *seriesValueGenerator) Close(_ context.Context) {}

// Next implements the eval.ValueGenerator interface.
func (s *seriesValueGenerator) Next(_ context.Context) (bool, error) {
	return s.next(s)
}

// Values implements the eval.ValueGenerator interface.
func (s *seriesValueGenerator) Values() (tree.Datums, error) {
	return s.genValue(s)
}

func makeVariadicUnnestGenerator(
	_ context.Context, _ *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	var arrays []*tree.DArray
	for _, a := range args {
		arrays = append(arrays, tree.MustBeDArray(a))
	}
	g := &multipleArrayValueGenerator{arrays: arrays}
	return g, nil
}

// multipleArrayValueGenerator is a value generator that returns each element of
// a list of arrays.
type multipleArrayValueGenerator struct {
	arrays    []*tree.DArray
	nextIndex int
	datums    tree.Datums
}

// ResolvedType implements the eval.ValueGenerator interface.
func (s *multipleArrayValueGenerator) ResolvedType() *types.T {
	arraysN := len(s.arrays)
	returnTypes := make([]*types.T, arraysN)
	labels := make([]string, arraysN)
	for i, arr := range s.arrays {
		returnTypes[i] = arr.ParamTyp
		labels[i] = "unnest"
	}
	return types.MakeLabeledTuple(returnTypes, labels)
}

// Start implements the eval.ValueGenerator interface.
func (s *multipleArrayValueGenerator) Start(_ context.Context, _ *kv.Txn) error {
	s.datums = make(tree.Datums, len(s.arrays))
	s.nextIndex = -1
	return nil
}

// Close implements the eval.ValueGenerator interface.
func (s *multipleArrayValueGenerator) Close(_ context.Context) {}

// Next implements the eval.ValueGenerator interface.
func (s *multipleArrayValueGenerator) Next(_ context.Context) (bool, error) {
	s.nextIndex++
	for _, arr := range s.arrays {
		if s.nextIndex < arr.Len() {
			return true, nil
		}
	}
	return false, nil
}

// Values implements the eval.ValueGenerator interface.
func (s *multipleArrayValueGenerator) Values() (tree.Datums, error) {
	for i, arr := range s.arrays {
		if s.nextIndex < arr.Len() {
			s.datums[i] = arr.Array[s.nextIndex]
		} else {
			s.datums[i] = tree.DNull
		}
	}
	return s.datums, nil
}

// makeWorkloadIndexRecsGeneratorFactory uses the arrayValueGenerator to return
// all the index recommendations as an array of strings. The hasTimestamp
// represents whether there is a timestamp filter.
func makeWorkloadIndexRecsGeneratorFactory(hasTimestamp bool) eval.GeneratorOverload {
	return func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (eval.ValueGenerator, error) {
		var ts tree.DTimestampTZ
		var err error

		if hasTimestamp {
			ts = tree.MustBeDTimestampTZ(args[0])
		} else {
			ts = tree.DTimestampTZ{Time: tree.MinSupportedTime}
		}

		var indexRecs []string
		indexRecs, err = workloadindexrec.FindWorkloadRecs(ctx, evalCtx, &ts)
		if err != nil {
			return &arrayValueGenerator{}, err
		}

		arr := tree.NewDArray(types.String)
		for _, indexRec := range indexRecs {
			if err = arr.Append(tree.NewDString(indexRec)); err != nil {
				return nil, err
			}
		}
		return &arrayValueGenerator{array: arr}, nil
	}
}

func makeArrayGenerator(
	_ context.Context, _ *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	arr := tree.MustBeDArray(args[0])
	return &arrayValueGenerator{array: arr}, nil
}

// arrayValueGenerator is a value generator that returns each element of an
// array.
type arrayValueGenerator struct {
	array     *tree.DArray
	nextIndex int
}

// ResolvedType implements the eval.ValueGenerator interface.
func (s *arrayValueGenerator) ResolvedType() *types.T {
	return s.array.ParamTyp
}

// Start implements the eval.ValueGenerator interface.
func (s *arrayValueGenerator) Start(_ context.Context, _ *kv.Txn) error {
	s.nextIndex = -1
	return nil
}

// Close implements the eval.ValueGenerator interface.
func (s *arrayValueGenerator) Close(_ context.Context) {}

// Next implements the eval.ValueGenerator interface.
func (s *arrayValueGenerator) Next(_ context.Context) (bool, error) {
	s.nextIndex++
	if s.nextIndex >= s.array.Len() {
		return false, nil
	}
	return true, nil
}

// Values implements the eval.ValueGenerator interface.
func (s *arrayValueGenerator) Values() (tree.Datums, error) {
	return tree.Datums{s.array.Array[s.nextIndex]}, nil
}

func makeExpandArrayGenerator(
	_ context.Context, _ *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	arr := tree.MustBeDArray(args[0])
	g := &expandArrayValueGenerator{avg: arrayValueGenerator{array: arr}}
	g.buf[1] = tree.NewDInt(tree.DInt(-1))
	return g, nil
}

// expandArrayValueGenerator is a value generator that returns each element of
// an array and an index for it.
type expandArrayValueGenerator struct {
	avg arrayValueGenerator
	buf [2]tree.Datum
}

var expandArrayValueGeneratorLabels = []string{"x", "n"}

// ResolvedType implements the eval.ValueGenerator interface.
func (s *expandArrayValueGenerator) ResolvedType() *types.T {
	return types.MakeLabeledTuple(
		[]*types.T{s.avg.array.ParamTyp, types.Int},
		expandArrayValueGeneratorLabels,
	)
}

// Start implements the eval.ValueGenerator interface.
func (s *expandArrayValueGenerator) Start(_ context.Context, _ *kv.Txn) error {
	s.avg.nextIndex = -1
	return nil
}

// Close implements the eval.ValueGenerator interface.
func (s *expandArrayValueGenerator) Close(_ context.Context) {}

// Next implements the eval.ValueGenerator interface.
func (s *expandArrayValueGenerator) Next(_ context.Context) (bool, error) {
	s.avg.nextIndex++
	return s.avg.nextIndex < s.avg.array.Len(), nil
}

// Values implements the eval.ValueGenerator interface.
func (s *expandArrayValueGenerator) Values() (tree.Datums, error) {
	// Expand array's index is 1 based.
	s.buf[0] = s.avg.array.Array[s.avg.nextIndex]
	s.buf[1] = tree.NewDInt(tree.DInt(s.avg.nextIndex + 1))
	return s.buf[:], nil
}

func makeGenerateSubscriptsGenerator(
	_ context.Context, _ *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	var arr *tree.DArray
	dim := 1
	if len(args) > 1 {
		dim = int(tree.MustBeDInt(args[1]))
	}
	// We sadly only support 1D arrays right now.
	if dim == 1 {
		arr = tree.MustBeDArray(args[0])
	} else {
		arr = &tree.DArray{}
	}
	var reverse bool
	if len(args) == 3 {
		reverse = bool(tree.MustBeDBool(args[2]))
	}
	g := &subscriptsValueGenerator{
		avg:     arrayValueGenerator{array: arr},
		reverse: reverse,
	}
	g.buf[0] = tree.NewDInt(tree.DInt(-1))
	return g, nil
}

// subscriptsValueGenerator is a value generator that returns a series
// comprising the given array's subscripts.
type subscriptsValueGenerator struct {
	avg arrayValueGenerator
	buf [1]tree.Datum
	// firstIndex is normally 1, since arrays are normally 1-indexed. But the
	// special Postgres vector types are 0-indexed.
	firstIndex int
	reverse    bool
}

var subscriptsValueGeneratorType = types.Int

// ResolvedType implements the eval.ValueGenerator interface.
func (s *subscriptsValueGenerator) ResolvedType() *types.T {
	return subscriptsValueGeneratorType
}

// Start implements the eval.ValueGenerator interface.
func (s *subscriptsValueGenerator) Start(_ context.Context, _ *kv.Txn) error {
	if s.reverse {
		s.avg.nextIndex = s.avg.array.Len()
	} else {
		s.avg.nextIndex = -1
	}
	// Most arrays are 1-indexed, but not all.
	s.firstIndex = s.avg.array.FirstIndex()
	return nil
}

// Close implements the eval.ValueGenerator interface.
func (s *subscriptsValueGenerator) Close(_ context.Context) {}

// Next implements the eval.ValueGenerator interface.
func (s *subscriptsValueGenerator) Next(_ context.Context) (bool, error) {
	if s.reverse {
		s.avg.nextIndex--
		return s.avg.nextIndex >= 0, nil
	}
	s.avg.nextIndex++
	return s.avg.nextIndex < s.avg.array.Len(), nil
}

// Values implements the eval.ValueGenerator interface.
func (s *subscriptsValueGenerator) Values() (tree.Datums, error) {
	s.buf[0] = tree.NewDInt(tree.DInt(s.avg.nextIndex + s.firstIndex))
	return s.buf[:], nil
}

// EmptyGenerator returns a new, empty generator. Used when a SRF
// evaluates to NULL.
func EmptyGenerator() eval.ValueGenerator {
	return &arrayValueGenerator{array: tree.NewDArray(types.AnyElement)}
}

// NullGenerator returns a new generator that returns a single row of nulls
// corresponding to the types stored in the tuple typ.
func NullGenerator(typ *types.T) (eval.ValueGenerator, error) {
	if typ.Family() != types.TupleFamily {
		return nil, errors.AssertionFailedf("generator expected to return multiple columns")
	}
	arrs := make([]*tree.DArray, len(typ.TupleContents()))
	for i := range typ.TupleContents() {
		arrs[i] = &tree.DArray{}
		arrs[i].Array = tree.Datums{tree.DNull}
	}
	return &multipleArrayValueGenerator{arrays: arrs}, nil
}

// unaryValueGenerator supports the execution of crdb_internal.unary_table().
type unaryValueGenerator struct {
	done bool
}

var unaryValueGeneratorType = types.EmptyTuple

func makeUnaryGenerator(
	_ context.Context, _ *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	return &unaryValueGenerator{}, nil
}

// ResolvedType implements the eval.ValueGenerator interface.
func (*unaryValueGenerator) ResolvedType() *types.T { return unaryValueGeneratorType }

// Start implements the eval.ValueGenerator interface.
func (s *unaryValueGenerator) Start(_ context.Context, _ *kv.Txn) error {
	s.done = false
	return nil
}

// Close implements the eval.ValueGenerator interface.
func (s *unaryValueGenerator) Close(_ context.Context) {}

// Next implements the eval.ValueGenerator interface.
func (s *unaryValueGenerator) Next(_ context.Context) (bool, error) {
	if !s.done {
		s.done = true
		return true, nil
	}
	return false, nil
}

var noDatums tree.Datums

// Values implements the eval.ValueGenerator interface.
func (s *unaryValueGenerator) Values() (tree.Datums, error) { return noDatums, nil }

func jsonAsText(j json.JSON) (tree.Datum, error) {
	text, err := j.AsText()
	if err != nil {
		return nil, err
	}
	if text == nil {
		return tree.DNull, nil
	}
	return tree.NewDString(*text), nil
}

var (
	errJSONObjectKeysOnArray         = pgerror.New(pgcode.InvalidParameterValue, "cannot call json_object_keys on an array")
	errJSONObjectKeysOnScalar        = pgerror.Newf(pgcode.InvalidParameterValue, "cannot call json_object_keys on a scalar")
	errJSONDeconstructArrayAsObject  = pgerror.New(pgcode.InvalidParameterValue, "cannot deconstruct an array as an object")
	errJSONDeconstructScalarAsObject = pgerror.Newf(pgcode.InvalidParameterValue, "cannot deconstruct a scalar")
)

var jsonArrayElementsImpl = makeGeneratorOverload(
	tree.ParamTypes{{Name: "input", Typ: types.Jsonb}},
	jsonArrayGeneratorType,
	makeJSONArrayAsJSONGenerator,
	"Expands a JSON array to a set of JSON values.",
	volatility.Immutable,
)

var jsonArrayElementsTextImpl = makeGeneratorOverload(
	tree.ParamTypes{{Name: "input", Typ: types.Jsonb}},
	jsonArrayTextGeneratorType,
	makeJSONArrayAsTextGenerator,
	"Expands a JSON array to a set of text values.",
	volatility.Immutable,
)

var jsonArrayGeneratorLabels = []string{"value"}
var jsonArrayGeneratorType = types.Jsonb

var jsonArrayTextGeneratorType = types.String

type jsonArrayGenerator struct {
	json      tree.DJSON
	nextIndex int
	asText    bool
	buf       [1]tree.Datum
}

var errJSONCallOnNonArray = pgerror.New(pgcode.InvalidParameterValue,
	"cannot be called on a non-array")

func makeJSONArrayAsJSONGenerator(
	_ context.Context, _ *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	return makeJSONArrayGenerator(args, false)
}

func makeJSONArrayAsTextGenerator(
	_ context.Context, _ *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	return makeJSONArrayGenerator(args, true)
}

func makeJSONArrayGenerator(args tree.Datums, asText bool) (eval.ValueGenerator, error) {
	target := tree.MustBeDJSON(args[0])
	if target.Type() != json.ArrayJSONType {
		return nil, errJSONCallOnNonArray
	}
	return &jsonArrayGenerator{
		json:   target,
		asText: asText,
	}, nil
}

// ResolvedType implements the eval.ValueGenerator interface.
func (g *jsonArrayGenerator) ResolvedType() *types.T {
	if g.asText {
		return jsonArrayTextGeneratorType
	}
	return jsonArrayGeneratorType
}

// Start implements the eval.ValueGenerator interface.
func (g *jsonArrayGenerator) Start(_ context.Context, _ *kv.Txn) error {
	g.nextIndex = -1
	g.json.JSON = g.json.JSON.MaybeDecode()
	g.buf[0] = nil
	return nil
}

// Close implements the eval.ValueGenerator interface.
func (g *jsonArrayGenerator) Close(_ context.Context) {}

// Next implements the eval.ValueGenerator interface.
func (g *jsonArrayGenerator) Next(_ context.Context) (bool, error) {
	g.nextIndex++
	next, err := g.json.FetchValIdx(g.nextIndex)
	if err != nil || next == nil {
		return false, err
	}
	if g.asText {
		if g.buf[0], err = jsonAsText(next); err != nil {
			return false, err
		}
	} else {
		g.buf[0] = tree.NewDJSON(next)
	}
	return true, nil
}

// Values implements the eval.ValueGenerator interface.
func (g *jsonArrayGenerator) Values() (tree.Datums, error) {
	return g.buf[:], nil
}

// jsonObjectKeysImpl is a key generator of a JSON object.
var jsonObjectKeysImpl = makeGeneratorOverload(
	tree.ParamTypes{{Name: "input", Typ: types.Jsonb}},
	jsonObjectKeysGeneratorType,
	makeJSONObjectKeysGenerator,
	"Returns sorted set of keys in the outermost JSON object.",
	volatility.Immutable,
)

var jsonObjectKeysGeneratorType = types.String

type jsonObjectKeysGenerator struct {
	iter *json.ObjectIterator
}

func makeJSONObjectKeysGenerator(
	_ context.Context, _ *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	target := tree.MustBeDJSON(args[0])
	iter, err := target.ObjectIter()
	if err != nil {
		return nil, err
	}
	if iter == nil {
		switch target.Type() {
		case json.ArrayJSONType:
			return nil, errJSONObjectKeysOnArray
		default:
			return nil, errJSONObjectKeysOnScalar
		}
	}
	return &jsonObjectKeysGenerator{
		iter: iter,
	}, nil
}

// ResolvedType implements the eval.ValueGenerator interface.
func (g *jsonObjectKeysGenerator) ResolvedType() *types.T {
	return jsonObjectKeysGeneratorType
}

// Start implements the eval.ValueGenerator interface.
func (g *jsonObjectKeysGenerator) Start(_ context.Context, _ *kv.Txn) error { return nil }

// Close implements the eval.ValueGenerator interface.
func (g *jsonObjectKeysGenerator) Close(_ context.Context) {}

// Next implements the eval.ValueGenerator interface.
func (g *jsonObjectKeysGenerator) Next(_ context.Context) (bool, error) {
	return g.iter.Next(), nil
}

// Values implements the eval.ValueGenerator interface.
func (g *jsonObjectKeysGenerator) Values() (tree.Datums, error) {
	return tree.Datums{tree.NewDString(g.iter.Key())}, nil
}

var jsonEachImpl = makeGeneratorOverload(
	tree.ParamTypes{{Name: "input", Typ: types.Jsonb}},
	jsonEachGeneratorType,
	makeJSONEachImplGenerator,
	"Expands the outermost JSON or JSONB object into a set of key/value pairs.",
	volatility.Immutable,
)

var jsonEachTextImpl = makeGeneratorOverload(
	tree.ParamTypes{{Name: "input", Typ: types.Jsonb}},
	jsonEachTextGeneratorType,
	makeJSONEachTextImplGenerator,
	"Expands the outermost JSON or JSONB object into a set of key/value pairs. "+
		"The returned values will be of type text.",
	volatility.Immutable,
)

var jsonToRecordImpl = makeGeneratorOverload(
	tree.ParamTypes{{Name: "input", Typ: types.Jsonb}},
	// NOTE: this type will never actually get used. It is replaced in the
	// optimizer by looking at the most recent AS alias clause.
	types.EmptyTuple,
	makeJSONRecordGenerator,
	"Builds an arbitrary record from a JSON object.",
	volatility.Stable,
)

var jsonToRecordSetImpl = makeGeneratorOverload(
	tree.ParamTypes{{Name: "input", Typ: types.Jsonb}},
	// NOTE: this type will never actually get used. It is replaced in the
	// optimizer by looking at the most recent AS alias clause.
	types.EmptyTuple,
	makeJSONRecordSetGenerator,
	"Builds an arbitrary set of records from a JSON array of objects.",
	volatility.Stable,
)

var jsonEachGeneratorLabels = []string{"key", "value"}

var jsonEachGeneratorType = types.MakeLabeledTuple(
	[]*types.T{types.String, types.Jsonb},
	jsonEachGeneratorLabels,
)

var jsonEachTextGeneratorType = types.MakeLabeledTuple(
	[]*types.T{types.String, types.String},
	jsonEachGeneratorLabels,
)

type jsonEachGenerator struct {
	target tree.DJSON
	iter   *json.ObjectIterator
	key    tree.Datum
	value  tree.Datum
	asText bool
}

func makeJSONEachImplGenerator(
	_ context.Context, _ *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	return makeJSONEachGenerator(args, false)
}

func makeJSONEachTextImplGenerator(
	_ context.Context, _ *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	return makeJSONEachGenerator(args, true)
}

func makeJSONEachGenerator(args tree.Datums, asText bool) (eval.ValueGenerator, error) {
	target := tree.MustBeDJSON(args[0])
	return &jsonEachGenerator{
		target: target,
		key:    nil,
		value:  nil,
		asText: asText,
	}, nil
}

// ResolvedType implements the eval.ValueGenerator interface.
func (g *jsonEachGenerator) ResolvedType() *types.T {
	if g.asText {
		return jsonEachTextGeneratorType
	}
	return jsonEachGeneratorType
}

// Start implements the eval.ValueGenerator interface.
func (g *jsonEachGenerator) Start(_ context.Context, _ *kv.Txn) error {
	iter, err := g.target.ObjectIter()
	if err != nil {
		return err
	}
	if iter == nil {
		switch g.target.Type() {
		case json.ArrayJSONType:
			return errJSONDeconstructArrayAsObject
		default:
			return errJSONDeconstructScalarAsObject
		}
	}
	g.iter = iter
	return nil
}

// Close implements the eval.ValueGenerator interface.
func (g *jsonEachGenerator) Close(_ context.Context) {}

// Next implements the eval.ValueGenerator interface.
func (g *jsonEachGenerator) Next(_ context.Context) (bool, error) {
	if !g.iter.Next() {
		return false, nil
	}
	g.key = tree.NewDString(g.iter.Key())
	if g.asText {
		var err error
		if g.value, err = jsonAsText(g.iter.Value()); err != nil {
			return false, err
		}
	} else {
		g.value = tree.NewDJSON(g.iter.Value())
	}
	return true, nil
}

// Values implements the eval.ValueGenerator interface.
func (g *jsonEachGenerator) Values() (tree.Datums, error) {
	return tree.Datums{g.key, g.value}, nil
}

var jsonPopulateProps = tree.FunctionProperties{
	Category: builtinconstants.CategoryJSON,
}

func makeJSONPopulateImpl(gen eval.GeneratorWithExprsOverload, info string) tree.Overload {
	return tree.Overload{
		// The json{,b}_populate_record{,set} builtins all have a 2 argument
		// structure. The first argument is an arbitrary tuple type, which is used
		// to set the columns of the output when the builtin is used as a FROM
		// source, or used as-is when it's used as an ordinary projection. To match
		// PostgreSQL, the argument actually is types.AnyElement, and its tuple-ness is
		// checked at execution time.
		// The second argument is a JSON object or array of objects. The builtin
		// transforms the JSON in the second argument into the tuple in the first
		// argument, field by field, casting fields in key "k" to the type in the
		// tuple slot "k". Any tuple fields that were missing in the JSON will be
		// left as they are in the input argument.
		// The first argument can be of the form NULL::<tupletype>, in which case
		// the default values of each field will be NULL.
		// The second argument can also be null, in which case the first argument
		// is returned as-is.
		Types:              tree.ParamTypes{{Name: "base", Typ: types.AnyElement}, {Name: "from_json", Typ: types.Jsonb}},
		ReturnType:         tree.IdentityReturnType(0),
		GeneratorWithExprs: gen,
		Class:              tree.GeneratorClass,
		Info:               info,
		Volatility:         volatility.Stable,
		// The typical way to call json_populate_record is to send NULL::atype
		// as the first argument, so we have to call the function with NULL
		// inputs.
		CalledOnNullInput: true,
	}
}

func makeJSONPopulateRecordGenerator(
	ctx context.Context, evalCtx *eval.Context, args tree.Exprs,
) (eval.ValueGenerator, error) {
	tuple, j, err := jsonPopulateRecordEvalArgs(ctx, evalCtx, args)
	if err != nil {
		return nil, err
	}

	if j != nil {
		if j.Type() != json.ObjectJSONType {
			return nil, pgerror.Newf(pgcode.InvalidParameterValue, "argument of json_populate_record must be an object")
		}
	} else {
		j = json.NewObjectBuilder(0).Build()
	}
	return &jsonPopulateRecordGenerator{
		evalCtx: evalCtx,
		input:   tuple,
		target:  j,
	}, nil
}

// jsonPopulateRecordEvalArgs evaluates the first 2 expression arguments to
// one of the jsonPopulateRecord variants, and returns the correctly-typed
// tuple of default values, and the JSON input or nil if it was SQL NULL.
func jsonPopulateRecordEvalArgs(
	ctx context.Context, evalCtx *eval.Context, args tree.Exprs,
) (tuple *tree.DTuple, jsonInputOrNil json.JSON, err error) {
	evalled := make(tree.Datums, len(args))
	for i := range args {
		var err error
		evalled[i], err = eval.Expr(ctx, evalCtx, args[i].(tree.TypedExpr))
		if err != nil {
			return nil, nil, err
		}
	}
	tupleType := args[0].(tree.TypedExpr).ResolvedType()
	if tupleType.Family() != types.TupleFamily && tupleType.Family() != types.UnknownFamily {
		return nil, nil, pgerror.New(
			pgcode.DatatypeMismatch,
			"first argument of json{b}_populate_record{set} must be a record type",
		)
	}
	var defaultElems tree.Datums
	if evalled[0] == tree.DNull {
		defaultElems = make(tree.Datums, len(tupleType.TupleLabels()))
		for i := range defaultElems {
			defaultElems[i] = tree.DNull
		}
	} else {
		defaultElems = tree.MustBeDTuple(evalled[0]).D
	}
	var j json.JSON
	if evalled[1] != tree.DNull {
		j = tree.MustBeDJSON(evalled[1]).JSON
	}
	return tree.NewDTuple(tupleType, defaultElems...), j, nil
}

type jsonPopulateRecordGenerator struct {
	input  *tree.DTuple
	target json.JSON

	wasCalled bool
	ctx       context.Context
	evalCtx   *eval.Context
}

// ResolvedType is part of the eval.ValueGenerator interface.
func (j jsonPopulateRecordGenerator) ResolvedType() *types.T {
	return j.input.ResolvedType()
}

// Start is part of the eval.ValueGenerator interface.
func (j *jsonPopulateRecordGenerator) Start(ctx context.Context, _ *kv.Txn) error {
	j.ctx = ctx
	return nil
}

// Close is part of the eval.ValueGenerator interface.
func (j *jsonPopulateRecordGenerator) Close(_ context.Context) {}

// Next is part of the eval.ValueGenerator interface.
func (j *jsonPopulateRecordGenerator) Next(ctx context.Context) (bool, error) {
	j.ctx = ctx
	if !j.wasCalled {
		j.wasCalled = true
		return true, nil
	}
	return false, nil
}

// Values is part of the eval.ValueGenerator interface.
func (j jsonPopulateRecordGenerator) Values() (tree.Datums, error) {
	output := tree.NewDTupleWithLen(j.input.ResolvedType(), j.input.D.Len())
	copy(output.D, j.input.D)
	if err := eval.PopulateRecordWithJSON(j.ctx, j.evalCtx, j.target, j.input.ResolvedType(), output); err != nil {
		return nil, err
	}
	return output.D, nil
}

func makeJSONPopulateRecordSetGenerator(
	ctx context.Context, evalCtx *eval.Context, args tree.Exprs,
) (eval.ValueGenerator, error) {
	tuple, j, err := jsonPopulateRecordEvalArgs(ctx, evalCtx, args)
	if err != nil {
		return nil, err
	}

	if j != nil {
		if j.Type() != json.ArrayJSONType {
			return nil, pgerror.Newf(pgcode.InvalidParameterValue, "argument of json_populate_recordset must be an array")
		}
	} else {
		j = json.NewArrayBuilder(0).Build()
	}

	return &jsonPopulateRecordSetGenerator{
		jsonPopulateRecordGenerator: jsonPopulateRecordGenerator{
			evalCtx: evalCtx,
			input:   tuple,
			target:  j,
		},
	}, nil
}

type jsonPopulateRecordSetGenerator struct {
	jsonPopulateRecordGenerator

	nextIdx int
}

// ResolvedType is part of the eval.ValueGenerator interface.
func (j jsonPopulateRecordSetGenerator) ResolvedType() *types.T { return j.input.ResolvedType() }

// Start is part of the eval.ValueGenerator interface.
func (j jsonPopulateRecordSetGenerator) Start(ctx context.Context, _ *kv.Txn) error {
	j.ctx = ctx
	return nil
}

// Close is part of the eval.ValueGenerator interface.
func (j jsonPopulateRecordSetGenerator) Close(_ context.Context) {}

// Next is part of the eval.ValueGenerator interface.
func (j *jsonPopulateRecordSetGenerator) Next(ctx context.Context) (bool, error) {
	j.ctx = ctx
	if j.nextIdx >= j.target.Len() {
		return false, nil
	}
	j.nextIdx++
	return true, nil
}

// Values is part of the eval.ValueGenerator interface.
func (j *jsonPopulateRecordSetGenerator) Values() (tree.Datums, error) {
	obj, err := j.target.FetchValIdx(j.nextIdx - 1)
	if err != nil {
		return nil, err
	}
	if obj.Type() != json.ObjectJSONType {
		return nil, pgerror.Newf(pgcode.InvalidParameterValue, "argument of json_populate_recordset must be an array of objects")
	}
	output := tree.NewDTupleWithLen(j.input.ResolvedType(), j.input.D.Len())
	copy(output.D, j.input.D)
	if err := eval.PopulateRecordWithJSON(j.ctx, j.evalCtx, obj, j.input.ResolvedType(), output); err != nil {
		return nil, err
	}
	return output.D, nil
}

func makeJSONRecordGenerator(
	ctx context.Context, evalCtx *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	target := tree.MustBeDJSON(args[0])
	return &jsonRecordGenerator{
		evalCtx: evalCtx,
		target:  target.JSON,
	}, nil
}

type jsonRecordGenerator struct {
	evalCtx *eval.Context
	target  json.JSON

	wasCalled bool
	values    tree.Datums
	types     []*types.T
	labels    []string
	// labelToRowIndexMap maps the column label to its position within the row.
	labelToRowIndexMap map[string]int
}

func (j *jsonRecordGenerator) SetAlias(types []*types.T, labels []string) error {
	j.types = types
	j.labels = labels
	j.labelToRowIndexMap = make(map[string]int)
	for i := range types {
		j.labelToRowIndexMap[j.labels[i]] = i
	}
	if len(types) != len(labels) {
		return errors.AssertionFailedf("unexpected mismatched types/labels list in json record generator %v %v", types, labels)
	}
	return nil
}

func (j jsonRecordGenerator) ResolvedType() *types.T {
	return types.AnyTuple
}

func (j *jsonRecordGenerator) Start(ctx context.Context, _ *kv.Txn) error {
	j.values = make(tree.Datums, len(j.types))
	if j.target.Type() != json.ObjectJSONType {
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"invalid non-object argument to json_to_record")
	}
	return nil
}

func (j *jsonRecordGenerator) Next(ctx context.Context) (bool, error) {
	if j.wasCalled {
		return false, nil
	}
	for i := range j.values {
		j.values[i] = tree.DNull
	}
	iter, err := j.target.ObjectIter()
	if err != nil {
		return false, err
	}
	for iter.Next() {
		idx, ok := j.labelToRowIndexMap[iter.Key()]
		if !ok {
			continue
		}
		v := iter.Value()
		datum, err := eval.PopulateDatumWithJSON(ctx, j.evalCtx, v, j.types[idx])
		if err != nil {
			return false, err
		}
		j.values[idx] = datum
	}

	j.wasCalled = true
	return true, nil
}

func (j jsonRecordGenerator) Values() (tree.Datums, error) {
	return j.values, nil
}

func (j jsonRecordGenerator) Close(ctx context.Context) {}

type jsonRecordSetGenerator struct {
	jsonRecordGenerator

	arr       tree.DJSON
	nextIndex int
}

func makeJSONRecordSetGenerator(
	ctx context.Context, evalCtx *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	arr := tree.MustBeDJSON(args[0])
	return &jsonRecordSetGenerator{
		arr: arr,
		jsonRecordGenerator: jsonRecordGenerator{
			evalCtx: evalCtx,
		},
	}, nil
}

func (j *jsonRecordSetGenerator) Start(ctx context.Context, _ *kv.Txn) error {
	j.values = make(tree.Datums, len(j.types))
	if j.arr.Type() != json.ArrayJSONType {
		return pgerror.Newf(pgcode.InvalidParameterValue,
			"argument to json_to_recordset must be an array of objects")
	}
	j.nextIndex = -1
	return nil
}

func (j *jsonRecordSetGenerator) Next(ctx context.Context) (bool, error) {
	j.nextIndex++
	next, err := j.arr.FetchValIdx(j.nextIndex)
	if err != nil || next == nil {
		return false, err
	}
	if next.Type() != json.ObjectJSONType {
		return false, pgerror.Newf(pgcode.InvalidParameterValue,
			"argument to json_to_recordset must be an array of objects")
	}
	j.target = next
	j.wasCalled = false
	_, err = j.jsonRecordGenerator.Next(ctx)
	if err != nil {
		return false, err
	}
	return true, nil
}

type checkConsistencyGenerator struct {
	txn                *kv.Txn // to load range descriptors
	consistencyChecker eval.ConsistencyCheckRunner
	rangeDescIterator  rangedesc.Iterator
	mode               kvpb.ChecksumMode

	// The descriptors for which we haven't yet emitted rows. Rows are consumed
	// from this field and produce one (or more, in the case of splits not reflected
	// in the descriptor) rows in `next`.
	descs []roachpb.RangeDescriptor
	// The current row, emitted by Values().
	cur kvpb.CheckConsistencyResponse_Result
	// The time it took to produce the current row, i.e. how long it took to run
	// the consistency check that produced the row. When a consistency check
	// produces more than one row (i.e. after a split), all of the duration will
	// be attributed to the first row.
	dur time.Duration
	// next are the potentially prefetched subsequent rows. This is usually empty
	// (as one consistency check produces one result which immediately moves to
	// `cur`) except when a descriptor we use doesn't reflect subsequent splits.
	next []kvpb.CheckConsistencyResponse_Result
}

var _ eval.ValueGenerator = &checkConsistencyGenerator{}

func makeCheckConsistencyGenerator(
	ctx context.Context, evalCtx *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	if err := evalCtx.SessionAccessor.CheckPrivilege(
		ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
	); err != nil {
		return nil, err
	}

	keyFrom := roachpb.Key(*args[1].(*tree.DBytes))
	keyTo := roachpb.Key(*args[2].(*tree.DBytes))

	minKey := evalCtx.Codec.TenantPrefix()
	maxKey := minKey.PrefixEnd()
	if minKey.Compare(keys.LocalMax) < 0 {
		// Consistency checks cannot run on the local keyspace [/Min, /LocalMax).
		// The keys in the local keyspace are "virtual" - they exist in the logical
		// keyspace, but are invisible to the KV replication layer because they
		// correspond to per-node local storage and are therefore not consistent.
		minKey = keys.LocalMax
	}

	if len(keyFrom) == 0 {
		keyFrom = minKey
	} else if bytes.Compare(keyFrom, minKey) < 0 {
		return nil, errors.Errorf("start key must be >= %q", []byte(minKey))
	}

	if len(keyTo) == 0 {
		keyTo = maxKey
	} else if bytes.Compare(keyTo, maxKey) > 0 {
		return nil, errors.Errorf("end key must be <= %q", []byte(maxKey))
	}

	if bytes.Compare(keyFrom, keyTo) >= 0 {
		return nil, errors.New("start key must be less than end key")
	}

	mode := kvpb.ChecksumMode_CHECK_FULL
	if statsOnly := bool(*args[0].(*tree.DBool)); statsOnly {
		mode = kvpb.ChecksumMode_CHECK_STATS
	} else if CheckConsistencyFatal {
		mode = kvpb.ChecksumMode_CHECK_VIA_QUEUE
	}

	if evalCtx.ConsistencyChecker == nil {
		return nil, errors.WithIssueLink(
			errors.AssertionFailedf("no consistency checker configured"),
			errors.IssueLink{IssueURL: build.MakeIssueURL(88222)},
		)
	}

	rangeDescIterator, err := evalCtx.Planner.GetRangeDescIterator(ctx, roachpb.Span{
		Key:    keyFrom,
		EndKey: keyTo,
	})
	if err != nil {
		return nil, err
	}
	return &checkConsistencyGenerator{
		txn:                evalCtx.Txn,
		consistencyChecker: evalCtx.ConsistencyChecker,
		rangeDescIterator:  rangeDescIterator,
		mode:               mode,
	}, nil
}

var checkConsistencyGeneratorType = types.MakeLabeledTuple(
	[]*types.T{types.Int, types.Bytes, types.String, types.String, types.String, types.Interval},
	[]string{"range_id", "start_key", "start_key_pretty", "status", "detail", "duration"},
)

// ResolvedType is part of the eval.ValueGenerator interface.
func (*checkConsistencyGenerator) ResolvedType() *types.T {
	return checkConsistencyGeneratorType
}

// Start is part of the eval.ValueGenerator interface.
func (c *checkConsistencyGenerator) Start(ctx context.Context, _ *kv.Txn) error {
	for c.rangeDescIterator.Valid() {
		desc := c.rangeDescIterator.CurRangeDescriptor()
		if len(desc.StartKey) == 0 {
			desc.StartKey = keys.MustAddr(keys.LocalMax)
		}
		c.descs = append(c.descs, desc)
		c.rangeDescIterator.Next()
	}
	return nil
}

// maybeRefillRows checks whether c.next is empty and if so, consumes the first
// element of c.descs for a consistency check. This populates c.next with at
// least one result (even on error). Returns the duration of the consistency
// check, if any, and zero otherwise.
func (c *checkConsistencyGenerator) maybeRefillRows(ctx context.Context) time.Duration {
	if len(c.next) > 0 || len(c.descs) == 0 {
		// We have a row to produce or no more ranges to check, so we're done
		// for now or for good, respectively.
		return 0
	}
	tBegin := timeutil.Now()
	// NB: peeling off the spans one by one allows this generator to produce
	// rows in a streaming manner. If we called CheckConsistency(c.from, c.to)
	// we would only get the result once all checks have completed and it will
	// generally be a lot more brittle since an error will completely wipe out
	// the result set.
	desc := c.descs[0]
	c.descs = c.descs[1:]
	resp, err := c.consistencyChecker.CheckConsistency(
		ctx, desc.StartKey.AsRawKey(), desc.EndKey.AsRawKey(), c.mode,
	)
	if err != nil {
		resp = &kvpb.CheckConsistencyResponse{Result: []kvpb.CheckConsistencyResponse_Result{{
			RangeID:  desc.RangeID,
			StartKey: desc.StartKey,
			Status:   kvpb.CheckConsistencyResponse_RANGE_INDETERMINATE,
			Detail:   err.Error(),
		}}}
	}

	// NB: this could have more than one entry, if a range split in the
	// meantime.
	c.next = resp.Result
	return timeutil.Since(tBegin)
}

// Next is part of the eval.ValueGenerator interface.
func (c *checkConsistencyGenerator) Next(ctx context.Context) (bool, error) {
	dur := c.maybeRefillRows(ctx)
	if len(c.next) == 0 {
		return false, nil
	}
	c.dur, c.cur, c.next = dur, c.next[0], c.next[1:]
	return true, nil
}

// Values is part of the eval.ValueGenerator interface.
func (c *checkConsistencyGenerator) Values() (tree.Datums, error) {
	row := c.cur
	intervalMeta := types.IntervalTypeMetadata{
		DurationField: types.IntervalDurationField{
			DurationType: types.IntervalDurationType_MILLISECOND,
		},
	}
	return tree.Datums{
		tree.NewDInt(tree.DInt(row.RangeID)),
		tree.NewDBytes(tree.DBytes(row.StartKey)),
		tree.NewDString(roachpb.Key(row.StartKey).String()),
		tree.NewDString(row.Status.String()),
		tree.NewDString(row.Detail),
		tree.NewDInterval(duration.MakeDuration(c.dur.Nanoseconds(), 0 /* days */, 0 /* months */), intervalMeta),
	}, nil
}

// Close is part of the eval.ValueGenerator interface.
func (c *checkConsistencyGenerator) Close(_ context.Context) {}

// spanKeyIteratorChunkKeys is the number of K/V pairs that the
// keyIterator requests at a time. If this changes, make sure
// to update the test in sql_keys.
//
// TODO(ssd): I increased this from 256, but it still seems a bit low.
const spanKeyIteratorChunkKeys = 2048

// spanKeyIteratorChunkBytes is the maximum size in bytes that a
// keyIterator will request at a time.
const spanKeyIteratorChunkBytes = 8 << 20 // 8MiB

var rangeKeyIteratorType = types.MakeLabeledTuple(
	// TODO(rohany): These could be bytes if we don't want to display the
	//  prettified versions of the key and value.
	[]*types.T{types.String, types.String, types.String},
	[]string{"key", "value", "ts"},
)

var spanKeyIteratorType = types.MakeLabeledTuple(
	[]*types.T{types.Bytes, types.Bytes, types.String},
	[]string{"key", "value", "ts"},
)

// spanKeyIterator is an eval.ValueGenerator that iterates over all
// SQL keys in a target span.
type spanKeyIterator struct {
	// The span to iterate
	span roachpb.Span

	// The transaction to use.
	txn *kv.Txn
	acc mon.BoundAccount

	// kvs is a set of K/V pairs currently accessed by the iterator.
	// It is not all of the K/V pairs in the target span. Instead,
	// the iterator maintains a small set of K/V pairs in the span,
	// and accesses more in a streaming fashion.
	kvs []roachpb.KeyValue

	// resumeSpan is the resume span from the last ScanRequest.
	resumeSpan *roachpb.Span

	// index maintains the current position of the iterator in kvs.
	index int
	// A buffer to avoid allocating an array on every call to Values().
	buf [3]tree.Datum
}

func newSpanKeyIterator(evalCtx *eval.Context, span roachpb.Span) *spanKeyIterator {
	return &spanKeyIterator{
		acc:  evalCtx.Planner.Mon().MakeBoundAccount(),
		span: span,
	}
}

// Start implements the eval.ValueGenerator interface.
func (sp *spanKeyIterator) Start(ctx context.Context, txn *kv.Txn) error {
	if err := sp.acc.Grow(ctx, spanKeyIteratorChunkBytes); err != nil {
		return err
	}
	sp.txn = txn
	return sp.scan(ctx, sp.span.Key, sp.span.EndKey)
}

// Next implements the eval.ValueGenerator interface.
func (sp *spanKeyIterator) Next(ctx context.Context) (bool, error) {
	sp.index++
	// If index is within rk.kvs, then we have buffered K/V pairs to return.
	// Otherwise, we might have to request another chunk of K/V pairs.
	if sp.index < len(sp.kvs) {
		return true, nil
	}

	// If we don't have a resume span, then we're out of results.
	if sp.resumeSpan == nil {
		return false, nil
	}

	// If we had some K/V pairs already, use the last key to constrain
	// the result of the next scan.
	err := sp.scan(ctx, sp.resumeSpan.Key, sp.span.EndKey)
	if err != nil {
		return false, err
	}

	return sp.Next(ctx)
}

func (sp *spanKeyIterator) scan(
	ctx context.Context, startKey roachpb.Key, endKey roachpb.Key,
) error {
	ba := &kvpb.BatchRequest{}
	ba.TargetBytes = spanKeyIteratorChunkBytes
	ba.MaxSpanRequestKeys = spanKeyIteratorChunkKeys
	ba.Add(&kvpb.ScanRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    startKey,
			EndKey: endKey,
		},
		ScanFormat: kvpb.KEY_VALUES,
	})
	br, pErr := sp.txn.Send(ctx, ba)
	if pErr != nil {
		return pErr.GoError()
	}
	resp := br.Responses[0].GetScan()
	sp.kvs = resp.Rows
	sp.resumeSpan = resp.ResumeSpan
	// The user of the generator first calls Next(), then Values(), so the index
	// managing the iterator's position needs to start at -1 instead of 0.
	sp.index = -1
	return nil
}

// Values implements the eval.ValueGenerator interface.
func (sp *spanKeyIterator) Values() (tree.Datums, error) {
	kv := sp.kvs[sp.index]
	sp.buf[0] = tree.NewDBytes(tree.DBytes(kv.Key))
	sp.buf[1] = tree.NewDBytes(tree.DBytes(kv.Value.RawBytes))
	sp.buf[2] = tree.NewDString(kv.Value.Timestamp.String())
	return sp.buf[:], nil
}

// Close implements the eval.ValueGenerator interface.
func (sp *spanKeyIterator) Close(ctx context.Context) {
	sp.acc.Close(ctx)
}

// ResolvedType implements the eval.ValueGenerator interface.
func (sp *spanKeyIterator) ResolvedType() *types.T {
	return spanKeyIteratorType
}

type rangeKeyIterator struct {
	// rangeID is the ID of the range to iterate over. rangeID is set
	// by the constructor of the rangeKeyIterator.
	rangeID roachpb.RangeID
	spanKeyIterator
	planner eval.Planner
}

var _ eval.ValueGenerator = &rangeKeyIterator{}
var _ eval.ValueGenerator = &spanKeyIterator{}

func makeRangeKeyIterator(
	ctx context.Context, evalCtx *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	if err := evalCtx.SessionAccessor.CheckPrivilege(
		ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
	); err != nil {
		return nil, err
	}
	planner := evalCtx.Planner
	rangeID := roachpb.RangeID(tree.MustBeDInt(args[0]))
	return &rangeKeyIterator{
		spanKeyIterator: spanKeyIterator{
			acc: planner.Mon().MakeBoundAccount(),
		},
		rangeID: rangeID,
		planner: planner,
	}, nil
}

// ResolvedType implements the eval.ValueGenerator interface.
func (rk *rangeKeyIterator) ResolvedType() *types.T {
	return rangeKeyIteratorType
}

// Start implements the eval.ValueGenerator interface.
func (rk *rangeKeyIterator) Start(ctx context.Context, txn *kv.Txn) (err error) {
	// Scan the range meta K/V's to find the target range. We do this in a
	// chunk-wise fashion to avoid loading all ranges into memory.
	rangeDesc, err := rk.planner.GetRangeDescByID(ctx, rk.rangeID)
	if err != nil {
		return err
	}
	rk.span = rangeDesc.KeySpan().AsRawSpanWithNoLocals()
	return rk.spanKeyIterator.Start(ctx, txn)
}

// Values implements the eval.ValueGenerator interface.
func (rk *rangeKeyIterator) Values() (tree.Datums, error) {
	kv := rk.kvs[rk.index]
	rk.buf[0] = tree.NewDString(kv.Key.String())
	rk.buf[1] = tree.NewDString(kv.Value.PrettyPrint())
	rk.buf[2] = tree.NewDString(kv.Value.Timestamp.String())
	return rk.buf[:], nil
}

var payloadsForSpanGeneratorLabels = []string{"payload_type", "payload_jsonb"}

var payloadsForSpanGeneratorType = types.MakeLabeledTuple(
	[]*types.T{types.String, types.Jsonb},
	payloadsForSpanGeneratorLabels,
)

// payloadsForSpanGenerator is a value generator that iterates over all payloads
// in a Span's recording. The recording includes the span's children.
type payloadsForSpanGenerator struct {
	// The span to iterate over.
	span tracing.RegistrySpan

	// payloads represents all of span's structured records. It's set at Start()
	// time.
	payloads []json.JSON

	// payloadIndex maintains the current position of the index of the iterator
	// in the list of `payloads` associated with a given recording.
	payloadIndex int
}

func makePayloadsForSpanGenerator(
	ctx context.Context, evalCtx *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	if err := evalCtx.SessionAccessor.CheckPrivilege(
		ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
	); err != nil {
		return nil, err
	}
	spanID := tracingpb.SpanID(*(args[0].(*tree.DInt)))
	span := evalCtx.Tracer.GetActiveSpanByID(spanID)
	if span == nil {
		return nil, nil
	}

	return &payloadsForSpanGenerator{span: span}, nil
}

// ResolvedType implements the eval.ValueGenerator interface.
func (p *payloadsForSpanGenerator) ResolvedType() *types.T {
	return payloadsForSpanGeneratorType
}

// Start implements the eval.ValueGenerator interface.
func (p *payloadsForSpanGenerator) Start(_ context.Context, _ *kv.Txn) error {
	// The user of the generator first calls Next(), then Values(), so the index
	// managing the iterator's position needs to start at -1 instead of 0.
	p.payloadIndex = -1

	rec := p.span.GetFullRecording(tracingpb.RecordingStructured)
	if rec.Empty() {
		// No structured records.
		return nil
	}
	p.payloads = make([]json.JSON, len(rec.Root.StructuredRecords))
	for i, sr := range rec.Root.StructuredRecords {
		var err error
		p.payloads[i], err = protoreflect.MessageToJSON(sr.Payload, protoreflect.FmtFlags{EmitDefaults: true})
		if err != nil {
			return err
		}
	}

	return nil
}

// Next implements the eval.ValueGenerator interface.
func (p *payloadsForSpanGenerator) Next(_ context.Context) (bool, error) {
	p.payloadIndex++
	return p.payloadIndex < len(p.payloads), nil
}

// Values implements the eval.ValueGenerator interface.
func (p *payloadsForSpanGenerator) Values() (tree.Datums, error) {
	payload := p.payloads[p.payloadIndex]
	payloadTypeAsJSON, err := payload.FetchValKey("@type")
	if err != nil {
		return nil, err
	}

	// We trim the proto type prefix as well as the enclosing double quotes
	// leftover from JSON value conversion.
	payloadTypeAsString := strings.TrimSuffix(
		strings.TrimPrefix(
			strings.TrimPrefix(
				payloadTypeAsJSON.String(),
				"\"type.googleapis.com/",
			),
			"cockroach."),
		"\"",
	)

	return tree.Datums{
		tree.NewDString(payloadTypeAsString),
		tree.NewDJSON(payload),
	}, nil
}

// Close implements the eval.ValueGenerator interface.
func (p *payloadsForSpanGenerator) Close(_ context.Context) {}

var payloadsForTraceGeneratorLabels = []string{"span_id", "payload_type", "payload_jsonb"}

var payloadsForTraceGeneratorType = types.MakeLabeledTuple(
	[]*types.T{types.Int, types.String, types.Jsonb},
	payloadsForTraceGeneratorLabels,
)

// payloadsForTraceGenerator is a value generator that iterates over all payloads
// of a given Trace.
type payloadsForTraceGenerator struct {
	traceID uint64
	planner eval.Planner
	// Iterator over all internal rows of a query that retrieves all payloads
	// of a trace.
	it eval.InternalRows
}

func makePayloadsForTraceGenerator(
	ctx context.Context, evalCtx *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	if err := evalCtx.SessionAccessor.CheckPrivilege(
		ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
	); err != nil {
		return nil, err
	}
	traceID := uint64(*(args[0].(*tree.DInt)))
	return &payloadsForTraceGenerator{traceID: traceID, planner: evalCtx.Planner}, nil
}

// ResolvedType implements the eval.ValueGenerator interface.
func (p *payloadsForTraceGenerator) ResolvedType() *types.T {
	return payloadsForSpanGeneratorType
}

// Start implements the eval.ValueGenerator interface.
func (p *payloadsForTraceGenerator) Start(ctx context.Context, _ *kv.Txn) error {
	const query = `WITH spans AS(
									SELECT span_id
  	 							FROM crdb_internal.node_inflight_trace_spans
 		 							WHERE trace_id = $1
									) SELECT *
										FROM spans, LATERAL crdb_internal.payloads_for_span(spans.span_id)`

	it, err := p.planner.QueryIteratorEx(
		ctx,
		"crdb_internal.payloads_for_trace",
		sessiondata.NoSessionDataOverride,
		query,
		p.traceID,
	)
	if err != nil {
		return err
	}
	p.it = it
	return nil
}

// Next implements the eval.ValueGenerator interface.
func (p *payloadsForTraceGenerator) Next(ctx context.Context) (bool, error) {
	if p.it == nil {
		return false, errors.AssertionFailedf("Start must be called before Next")
	}
	return p.it.Next(ctx)
}

// Values implements the eval.ValueGenerator interface.
func (p *payloadsForTraceGenerator) Values() (tree.Datums, error) {
	if p.it == nil {
		return nil, errors.AssertionFailedf("Start must be called before Values")
	}
	return p.it.Cur(), nil
}

// Close implements the eval.ValueGenerator interface.
func (p *payloadsForTraceGenerator) Close(_ context.Context) {
	if p.it != nil {
		err := p.it.Close()
		if err != nil {
			// TODO(angelapwen, yuzefovich): The iterator's error should be surfaced here.
			return
		}
	}
}

var showCreateAllSchemasGeneratorType = types.String
var showCreateAllTypesGeneratorType = types.String
var showCreateAllTablesGeneratorType = types.String

// Phase is used to determine if CREATE statements or ALTER statements
// are being generated for showCreateAllTables.
type Phase int

const (
	create Phase = iota
	alterAddFks
	alterValidateFks
)

// showCreateAllSchemasGenerator supports the execution of
// crdb_internal.show_create_all_schemas(dbName).
type showCreateAllSchemasGenerator struct {
	evalPlanner eval.Planner
	txn         *kv.Txn
	ids         []int64
	dbName      string
	acc         mon.BoundAccount

	// The following variables are updated during
	// calls to Next() and change throughout the lifecycle of
	// showCreateAllSchemasGenerator.
	curr tree.Datum
	idx  int
}

// ResolvedType implements the eval.ValueGenerator interface.
func (s *showCreateAllSchemasGenerator) ResolvedType() *types.T {
	return showCreateAllSchemasGeneratorType
}

// Start implements the eval.ValueGenerator interface.
func (s *showCreateAllSchemasGenerator) Start(ctx context.Context, txn *kv.Txn) error {
	ids, err := getSchemaIDs(
		ctx, s.evalPlanner, txn, s.dbName, &s.acc,
	)
	if err != nil {
		return err
	}

	s.ids = ids

	s.txn = txn
	s.idx = -1
	return nil
}

func (s *showCreateAllSchemasGenerator) Next(ctx context.Context) (bool, error) {
	s.idx++
	if s.idx >= len(s.ids) {
		return false, nil
	}

	createStmt, err := getSchemaCreateStatement(
		ctx, s.evalPlanner, s.txn, s.ids[s.idx], s.dbName,
	)
	if err != nil {
		return false, err
	}
	createStmtStr := string(tree.MustBeDString(createStmt))
	s.curr = tree.NewDString(createStmtStr + ";")

	return true, nil
}

// Values implements the eval.ValueGenerator interface.
func (s *showCreateAllSchemasGenerator) Values() (tree.Datums, error) {
	return tree.Datums{s.curr}, nil
}

// Close implements the eval.ValueGenerator interface.
func (s *showCreateAllSchemasGenerator) Close(ctx context.Context) {
	s.acc.Close(ctx)
}

// makeShowCreateAllSchemasGenerator creates a generator to support the
// crdb_internal.show_create_all_schemas(dbName) builtin.
// We use the timestamp of when the generator is created as the
// timestamp to pass to AS OF SYSTEM TIME for looking up the create schema
func makeShowCreateAllSchemasGenerator(
	ctx context.Context, evalCtx *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	dbName := string(tree.MustBeDString(args[0]))
	return &showCreateAllSchemasGenerator{
		evalPlanner: evalCtx.Planner,
		dbName:      dbName,
		acc:         evalCtx.Planner.Mon().MakeBoundAccount(),
	}, nil
}

// showCreateAllTablesGenerator supports the execution of
// crdb_internal.show_create_all_tables(dbName).
type showCreateAllTablesGenerator struct {
	evalPlanner eval.Planner
	txn         *kv.Txn
	ids         []int64
	dbName      string
	acc         mon.BoundAccount
	sessionData *sessiondata.SessionData

	// The following variables are updated during
	// calls to Next() and change throughout the lifecycle of
	// showCreateAllTablesGenerator.
	curr           tree.Datum
	idx            int
	shouldValidate bool
	alterArr       tree.Datums
	alterArrIdx    int
	phase          Phase
}

// ResolvedType implements the eval.ValueGenerator interface.
func (s *showCreateAllTablesGenerator) ResolvedType() *types.T {
	return showCreateAllTablesGeneratorType
}

// Start implements the eval.ValueGenerator interface.
func (s *showCreateAllTablesGenerator) Start(ctx context.Context, txn *kv.Txn) error {
	// Note: All the table ids are accumulated in ram before the generator
	// starts generating values.
	// This is reasonable under the assumption that:
	// This uses approximately the same amount of memory as required when
	// generating the vtable crdb_internal.show_create_statements. If generating
	// and reading from the vtable succeeds which we do to retrieve the ids, then
	// it is reasonable to use the same amount of memory to hold the ids in
	// ram during the lifecycle of showCreateAllTablesGenerator.
	//
	// We also account for the memory in the BoundAccount memory monitor in
	// showCreateAllTablesGenerator.
	ids, err := getTopologicallySortedTableIDs(
		ctx, s.evalPlanner, txn, s.dbName, &s.acc,
	)
	if err != nil {
		return err
	}

	s.ids = ids

	s.txn = txn
	s.idx = -1
	s.phase = create
	return nil
}

func (s *showCreateAllTablesGenerator) Next(ctx context.Context) (bool, error) {
	switch s.phase {
	case create:
		s.idx++
		if s.idx >= len(s.ids) {
			// Were done generating the create statements, start generating alters.
			s.phase = alterAddFks
			s.idx = -1
			return s.Next(ctx)
		}

		createStmt, err := getCreateStatement(
			ctx, s.evalPlanner, s.txn, s.ids[s.idx], s.dbName,
		)
		if err != nil {
			return false, err
		}
		createStmtStr := string(tree.MustBeDString(createStmt))
		s.curr = tree.NewDString(createStmtStr + ";")
	case alterAddFks, alterValidateFks:
		// We have existing alter statements to generate for the current
		// table id.
		s.alterArrIdx++
		if s.alterArrIdx < len(s.alterArr) {
			alterStmtStr := string(tree.MustBeDString(s.alterArr[s.alterArrIdx]))
			s.curr = tree.NewDString(alterStmtStr + ";")

			// At least one FK was added, we must validate the FK.
			s.shouldValidate = true
			return true, nil
		}
		// We need to generate the alter statements for the next table.
		s.idx++
		if s.idx >= len(s.ids) {
			if s.phase == alterAddFks {
				// Were done generating the alter fk statements,
				// start generating alter validate fk statements.
				s.phase = alterValidateFks
				s.idx = -1

				if s.shouldValidate {
					// Add a warning about the possibility of foreign key
					// validation failing.
					s.curr = tree.NewDString(foreignKeyValidationWarning)
					return true, nil
				}
				return s.Next(ctx)
			}
			// We're done if were on phase alterValidateFks and we
			// finish going through all the table ids.
			return false, nil
		}

		statementReturnType := alterAddFKStatements
		if s.phase == alterValidateFks {
			statementReturnType = alterValidateFKStatements
		}
		alterStmt, err := getAlterStatements(
			ctx, s.evalPlanner, s.txn, s.ids[s.idx], s.dbName, statementReturnType,
		)
		if err != nil {
			return false, err
		}
		if alterStmt == nil {
			// There can be no ALTER statements for a given id, in this case
			// we go next.
			return s.Next(ctx)
		}
		s.alterArr = tree.MustBeDArray(alterStmt).Array
		s.alterArrIdx = -1
		return s.Next(ctx)
	}

	return true, nil
}

// Values implements the eval.ValueGenerator interface.
func (s *showCreateAllTablesGenerator) Values() (tree.Datums, error) {
	return tree.Datums{s.curr}, nil
}

// Close implements the eval.ValueGenerator interface.
func (s *showCreateAllTablesGenerator) Close(ctx context.Context) {
	s.acc.Close(ctx)
}

// makeShowCreateAllTablesGenerator creates a generator to support the
// crdb_internal.show_create_all_tables(dbName) builtin.
// We use the timestamp of when the generator is created as the
// timestamp to pass to AS OF SYSTEM TIME for looking up the create table
// and alter table statements.
func makeShowCreateAllTablesGenerator(
	ctx context.Context, evalCtx *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	dbName := string(tree.MustBeDString(args[0]))
	return &showCreateAllTablesGenerator{
		evalPlanner: evalCtx.Planner,
		dbName:      dbName,
		acc:         evalCtx.Planner.Mon().MakeBoundAccount(),
		sessionData: evalCtx.SessionData(),
	}, nil
}

// showCreateAllTypesGenerator supports the execution of
// crdb_internal.show_create_all_types(dbName).
type showCreateAllTypesGenerator struct {
	evalPlanner eval.Planner
	txn         *kv.Txn
	ids         []int64
	dbName      string
	acc         mon.BoundAccount

	// The following variables are updated during
	// calls to Next() and change throughout the lifecycle of
	// showCreateAllTypesGenerator.
	curr tree.Datum
	idx  int
}

// ResolvedType implements the eval.ValueGenerator interface.
func (s *showCreateAllTypesGenerator) ResolvedType() *types.T {
	return showCreateAllTypesGeneratorType
}

// Start implements the eval.ValueGenerator interface.
func (s *showCreateAllTypesGenerator) Start(ctx context.Context, txn *kv.Txn) error {
	ids, err := getTypeIDs(
		ctx, s.evalPlanner, txn, s.dbName, &s.acc,
	)
	if err != nil {
		return err
	}

	s.ids = ids

	s.txn = txn
	s.idx = -1
	return nil
}

func (s *showCreateAllTypesGenerator) Next(ctx context.Context) (bool, error) {
	s.idx++
	if s.idx >= len(s.ids) {
		return false, nil
	}

	createStmt, err := getTypeCreateStatement(
		ctx, s.evalPlanner, s.txn, s.ids[s.idx], s.dbName,
	)
	if err != nil {
		return false, err
	}
	createStmtStr := string(tree.MustBeDString(createStmt))
	s.curr = tree.NewDString(createStmtStr + ";")

	return true, nil
}

// Values implements the eval.ValueGenerator interface.
func (s *showCreateAllTypesGenerator) Values() (tree.Datums, error) {
	return tree.Datums{s.curr}, nil
}

// Close implements the eval.ValueGenerator interface.
func (s *showCreateAllTypesGenerator) Close(ctx context.Context) {
	s.acc.Close(ctx)
}

// makeShowCreateAllTypesGenerator creates a generator to support the
// crdb_internal.show_create_all_types(dbName) builtin.
// We use the timestamp of when the generator is created as the
// timestamp to pass to AS OF SYSTEM TIME for looking up the create type
func makeShowCreateAllTypesGenerator(
	ctx context.Context, evalCtx *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	dbName := string(tree.MustBeDString(args[0]))
	return &showCreateAllTypesGenerator{
		evalPlanner: evalCtx.Planner,
		dbName:      dbName,
		acc:         evalCtx.Planner.Mon().MakeBoundAccount(),
	}, nil
}

// identGenerator supports the execution of
// crdb_internal.gen_rand_ident().
type identGenerator struct {
	gen randident.NameGenerator
	acc mon.BoundAccount

	// The following variables are updated during
	// calls to Next() and change throughout the lifecycle of
	// identGenerator.
	curr  tree.Datum
	idx   int
	count int
}

// ResolvedType implements the eval.ValueGenerator interface.
func (s *identGenerator) ResolvedType() *types.T {
	return types.String
}

// Start implements the eval.ValueGenerator interface.
func (s *identGenerator) Start(ctx context.Context, txn *kv.Txn) error {
	return nil
}

func (s *identGenerator) Next(ctx context.Context) (bool, error) {
	s.idx++
	if s.idx > s.count {
		return false, nil
	}

	name := s.gen.GenerateOne(strconv.Itoa(s.idx))
	if err := s.acc.Grow(ctx, int64(len(name))); err != nil {
		return false, err
	}
	s.curr = tree.NewDString(name)

	return true, nil
}

// Values implements the eval.ValueGenerator interface.
func (s *identGenerator) Values() (tree.Datums, error) {
	return tree.Datums{s.curr}, nil
}

// Close implements the eval.ValueGenerator interface.
func (s *identGenerator) Close(ctx context.Context) {
	s.acc.Close(ctx)
}

// makeIdentGenerator creates a generator to support the
// crdb_internal.gen_rand_ident() builtin.
func makeIdentGenerator(
	ctx context.Context, evalCtx *eval.Context, namePatDatum, countDatum, cfgDatum tree.Datum,
) (eval.ValueGenerator, error) {
	pattern := string(tree.MustBeDString(namePatDatum))
	count := int(tree.MustBeDInt(countDatum))
	cfg := randident.DefaultNameGeneratorConfig()
	seed := randutil.NewPseudoSeed()
	if cfgDatum != nil {
		customCfg := struct {
			// Seed is the random seed to use. We expose this so that tests
			// can use this function and obtain deterministic output.
			Seed *int64

			// The other name config parameters.
			randidentcfg.Config `json:",inline"`
		}{
			Seed:   nil,
			Config: cfg,
		}
		userInputCfg := cfgDatum.(*tree.DJSON).JSON.String()
		d := gojson.NewDecoder(strings.NewReader(userInputCfg))
		d.DisallowUnknownFields()
		if err := d.Decode(&customCfg); err != nil {
			return nil, pgerror.WithCandidateCode(err, pgcode.Syntax)
		}
		if customCfg.Seed != nil {
			seed = *customCfg.Seed
		}
		cfg = customCfg.Config
	}
	cfg.Finalize()
	rand := rand.New(rand.NewSource(seed))
	return &identGenerator{
		gen:   randident.NewNameGenerator(&cfg, rand, pattern),
		acc:   evalCtx.Planner.Mon().MakeBoundAccount(),
		count: count,
	}, nil
}

type spanStatsDetails struct {
	dbId    int
	tableId int
}

type tableSpanStatsIterator struct {
	argDbId             int
	argTableId          int
	it                  eval.InternalRows
	codec               keys.SQLCodec
	p                   eval.Planner
	spanStatsBatchLimit int
	// Each iter
	iterSpanIdx       int
	spanStatsDetails  []spanStatsDetails
	currStatsResponse *roachpb.SpanStatsResponse
}

func newTableSpanStatsIterator(
	eval *eval.Context, dbId int, tableId int, spanBatchLimit int,
) *tableSpanStatsIterator {
	return &tableSpanStatsIterator{codec: eval.Codec, p: eval.Planner, argDbId: dbId, argTableId: tableId, spanStatsBatchLimit: spanBatchLimit}
}

// Start implements the eval.ValueGenerator interface.
func (tssi *tableSpanStatsIterator) Start(ctx context.Context, _ *kv.Txn) error {
	var err error = nil
	tssi.it, err = tssi.p.GetDetailsForSpanStats(ctx, tssi.argDbId, tssi.argTableId)
	return err
}

// Next implements the eval.ValueGenerator interface.
func (tssi *tableSpanStatsIterator) Next(ctx context.Context) (bool, error) {
	if tssi.it == nil {
		return false, errors.AssertionFailedf("Start must be called before Next")
	}
	// Check if we can iterate through the span details buffer.
	if tssi.iterSpanIdx+1 < len(tssi.spanStatsDetails) {
		tssi.iterSpanIdx++
		return true, nil
	}

	// There are no more span details to iterate in the buffer.
	// Instead, we continue to fetch more span stats (if possible).
	hasMoreRows, err := tssi.fetchSpanStats(ctx)
	if err != nil {
		return false, err
	}
	// After fetching new span stats, reset the index.
	tssi.iterSpanIdx = 0
	return hasMoreRows || len(tssi.spanStatsDetails) != 0, nil
}

func (tssi *tableSpanStatsIterator) fetchSpanStats(ctx context.Context) (bool, error) {
	// Reset span details.
	tssi.spanStatsDetails = tssi.spanStatsDetails[:0]

	var ok bool
	var err error
	var spans []roachpb.Span
	// While we have more rows
	for ok, err = tssi.it.Next(ctx); ok; ok, err = tssi.it.Next(ctx) {

		// Pull the current row.
		row := tssi.it.Cur()
		dbId := int(tree.MustBeDInt(row[0]))
		tableId := int(tree.MustBeDInt(row[1]))

		// Add the row data to span stats details
		tssi.spanStatsDetails = append(tssi.spanStatsDetails, spanStatsDetails{
			dbId:    dbId,
			tableId: tableId,
		})

		// Gather the span for the current span stats request.
		tableStartKey := tssi.codec.TablePrefix(uint32(tableId))
		spans = append(spans, roachpb.Span{
			Key:    tableStartKey,
			EndKey: tableStartKey.PrefixEnd(),
		})

		// Exit the loop if we're reached our limit of spans
		// for the span stats request.
		if len(tssi.spanStatsDetails) >= tssi.spanStatsBatchLimit {
			break
		}
	}

	// If we encounter an error while iterating over rows,
	// return error before fetching span stats.
	if err != nil {
		return false, err
	}

	// If we have spans, request span stats
	if len(spans) > 0 {
		tssi.currStatsResponse, err = tssi.p.SpanStats(ctx, spans)
	}

	if err != nil {
		return false, err
	}
	return ok, err
}

// Values implements the eval.ValueGenerator interface.
func (tssi *tableSpanStatsIterator) Values() (tree.Datums, error) {
	// Get the current span details.
	spanDetails := tssi.spanStatsDetails[tssi.iterSpanIdx]
	startKey := tssi.codec.TablePrefix(uint32(spanDetails.tableId))
	tableSpan := roachpb.Span{Key: startKey, EndKey: startKey.PrefixEnd()}
	// Get the current span stats.
	spanStats, found := tssi.currStatsResponse.SpanToStats[tableSpan.String()]
	if !found {
		return nil, errors.Errorf("could not find span stats for table span: %s", tableSpan.String())
	}

	totalBytes := spanStats.TotalStats.KeyBytes +
		spanStats.TotalStats.ValBytes +
		spanStats.TotalStats.RangeKeyBytes +
		spanStats.TotalStats.RangeValBytes
	livePercentage := float64(0)
	if totalBytes > 0 {
		livePercentage = float64(spanStats.TotalStats.LiveBytes) / float64(totalBytes)
	}
	return []tree.Datum{
		tree.NewDInt(tree.DInt(spanDetails.dbId)),
		tree.NewDInt(tree.DInt(spanDetails.tableId)),
		tree.NewDInt(tree.DInt(spanStats.RangeCount)),
		tree.NewDInt(tree.DInt(spanStats.ApproximateDiskBytes)),
		tree.NewDInt(tree.DInt(spanStats.TotalStats.LiveBytes)),
		tree.NewDInt(tree.DInt(totalBytes)),
		tree.NewDFloat(tree.DFloat(livePercentage)),
	}, nil
}

// Close implements the eval.ValueGenerator interface.
func (tssi *tableSpanStatsIterator) Close(_ context.Context) {}

// ResolvedType implements the eval.ValueGenerator interface.
func (tssi *tableSpanStatsIterator) ResolvedType() *types.T {
	return tableSpanStatsGeneratorType
}

var tableMetricsGeneratorType = types.MakeLabeledTuple(
	[]*types.T{types.Int, types.Int, types.Int, types.Int, types.Int, types.Json},
	[]string{"node_id", "store_id", "level", "file_num", "approximate_span_bytes", "metrics"},
)

// tableMetricsIterator implements eval.ValueGenerator; it returns a set of
// SSTable metrics (one per row).
type tableMetricsIterator struct {
	metrics []enginepb.SSTableMetricsInfo
	evalCtx *eval.Context

	iterIdx int
	nodeID  int32
	storeID int32
	start   []byte
	end     []byte
}

var _ eval.ValueGenerator = (*tableMetricsIterator)(nil)

func newTableMetricsIterator(
	evalCtx *eval.Context, nodeID, storeID int32, start, end []byte,
) *tableMetricsIterator {
	return &tableMetricsIterator{evalCtx: evalCtx, nodeID: nodeID, storeID: storeID, start: start, end: end}
}

// Start implements the eval.ValueGenerator interface.
func (tmi *tableMetricsIterator) Start(ctx context.Context, _ *kv.Txn) error {
	var err error
	tmi.metrics, err = tmi.evalCtx.GetTableMetrics(ctx, tmi.nodeID, tmi.storeID, tmi.start, tmi.end)
	if err != nil {
		err = errors.Wrapf(err, "getting table metrics for node %d store %d", tmi.nodeID, tmi.storeID)
	}

	sort.SliceStable(tmi.metrics, func(i, j int) bool {
		a, b := tmi.metrics[i], tmi.metrics[j]
		return a.Level < b.Level || (a.Level == b.Level && a.TableID < b.TableID)
	})

	return err
}

// Next implements the eval.ValueGenerator interface.
func (tmi *tableMetricsIterator) Next(_ context.Context) (bool, error) {
	tmi.iterIdx++
	return tmi.iterIdx <= len(tmi.metrics), nil
}

// Values implements the eval.ValueGenerator interface.
func (tmi *tableMetricsIterator) Values() (tree.Datums, error) {
	metricsInfo := tmi.metrics[tmi.iterIdx-1]

	metricsJson, err := tree.ParseDJSON(string(metricsInfo.TableInfoJSON))
	if err != nil {
		return nil, err
	}

	return tree.Datums{
		tree.NewDInt(tree.DInt(tmi.nodeID)),
		tree.NewDInt(tree.DInt(tmi.storeID)),
		tree.NewDInt(tree.DInt(metricsInfo.Level)),
		tree.NewDInt(tree.DInt(metricsInfo.TableID)),
		tree.NewDInt(tree.DInt(metricsInfo.ApproximateSpanBytes)),
		metricsJson,
	}, nil
}

// Close implements the eval.ValueGenerator interface.
func (tmi *tableMetricsIterator) Close(_ context.Context) {}

// ResolvedType implements the eval.ValueGenerator interface.
func (tmi *tableMetricsIterator) ResolvedType() *types.T {
	return tableMetricsGeneratorType
}

func makeTableMetricsGenerator(
	ctx context.Context, evalCtx *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	if err := evalCtx.SessionAccessor.CheckPrivilege(
		ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
	); err != nil {
		return nil, err
	}
	nodeID := int32(tree.MustBeDInt(args[0]))
	storeID := int32(tree.MustBeDInt(args[1]))
	start := []byte(tree.MustBeDBytes(args[2]))
	end := []byte(tree.MustBeDBytes(args[3]))
	// We use the keys as-is if they are valid engine keys, otherwise we encode
	// them as a version-less key. This preserves the ability to pass in
	// manually constructed engine keys, but also allows for the common practice
	// of passing in keys extracted from the ranges table. It's possible
	// (although somewhat unlikely) for a user key to validate as an engine key.
	if ek, ok := storage.DecodeEngineKey(start); !ok || ek.Validate() != nil {
		start = storage.EncodeMVCCKey(storage.MVCCKey{Key: start})
	}
	if ek, ok := storage.DecodeEngineKey(end); !ok || ek.Validate() != nil {
		end = storage.EncodeMVCCKey(storage.MVCCKey{Key: start})
	}
	return newTableMetricsIterator(evalCtx, nodeID, storeID, start, end), nil
}

type storageInternalKeysIterator struct {
	metrics []enginepb.StorageInternalKeysMetrics
	evalCtx *eval.Context

	iterIdx            int
	nodeID             int32
	storeID            int32
	megabytesPerSecond int64
	start              []byte
	end                []byte
}

var storageInternalKeysGeneratorType = types.MakeLabeledTuple(
	[]*types.T{types.Int, types.Int, types.Int, types.Int, types.Int, types.Int, types.Int, types.Int,
		types.Int, types.Int, types.Int, types.Int},
	[]string{
		"level",
		"node_id",
		"store_id",
		"snapshot_pinned_keys",
		"snapshot_pinned_keys_bytes",
		"point_key_delete_is_latest_count",
		"point_key_delete_count",
		"point_key_set_is_latest_count",
		"point_key_set_count",
		"range_delete_count",
		"range_key_set_count",
		"range_key_delete_count",
	},
)

var _ eval.ValueGenerator = (*storageInternalKeysIterator)(nil)

func newStorageInternalKeysGenerator(
	evalCtx *eval.Context, nodeID, storeID int32, start, end []byte, megaBytesPerSecond int64,
) *storageInternalKeysIterator {
	return &storageInternalKeysIterator{evalCtx: evalCtx, nodeID: nodeID, storeID: storeID, start: start, end: end, megabytesPerSecond: megaBytesPerSecond}
}

// Start implements the eval.ValueGenerator interface.
func (s *storageInternalKeysIterator) Start(ctx context.Context, _ *kv.Txn) error {
	var err error
	s.metrics, err = s.evalCtx.ScanStorageInternalKeys(ctx, s.nodeID, s.storeID, s.start, s.end, s.megabytesPerSecond)
	if err != nil {
		err = errors.Wrapf(err, "getting table metrics for node %d store %d", s.nodeID, s.storeID)
	}

	return err
}

// Next implements the eval.ValueGenerator interface.
func (s *storageInternalKeysIterator) Next(_ context.Context) (bool, error) {
	s.iterIdx++
	return s.iterIdx <= len(s.metrics), nil
}

// Values implements the eval.ValueGenerator interface.
func (s *storageInternalKeysIterator) Values() (tree.Datums, error) {
	metricsInfo := s.metrics[s.iterIdx-1]
	levelDatum := tree.DNull

	if metricsInfo.Level != -1 {
		levelDatum = tree.NewDInt(tree.DInt(metricsInfo.Level))
	}

	return tree.Datums{
		levelDatum,
		tree.NewDInt(tree.DInt(s.nodeID)),
		tree.NewDInt(tree.DInt(s.storeID)),
		tree.NewDInt(tree.DInt(metricsInfo.SnapshotPinnedKeys)),
		tree.NewDInt(tree.DInt(metricsInfo.SnapshotPinnedKeysBytes)),
		tree.NewDInt(tree.DInt(metricsInfo.PointKeyDeleteIsLatestCount)),
		tree.NewDInt(tree.DInt(metricsInfo.PointKeyDeleteCount)),
		tree.NewDInt(tree.DInt(metricsInfo.PointKeySetIsLatestCount)),
		tree.NewDInt(tree.DInt(metricsInfo.PointKeySetCount)),
		tree.NewDInt(tree.DInt(metricsInfo.RangeDeleteCount)),
		tree.NewDInt(tree.DInt(metricsInfo.RangeKeySetCount)),
		tree.NewDInt(tree.DInt(metricsInfo.RangeKeyDeleteCount)),
	}, nil
}

// Close implements the eval.ValueGenerator interface.
func (tmi *storageInternalKeysIterator) Close(_ context.Context) {}

// ResolvedType implements the eval.ValueGenerator interface.
func (tmi *storageInternalKeysIterator) ResolvedType() *types.T {
	return storageInternalKeysGeneratorType
}

func makeStorageInternalKeysGenerator(
	ctx context.Context, evalCtx *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	if err := evalCtx.SessionAccessor.CheckPrivilege(
		ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
	); err != nil {
		return nil, err
	}
	nodeID := int32(tree.MustBeDInt(args[0]))
	storeID := int32(tree.MustBeDInt(args[1]))
	start := []byte(tree.MustBeDBytes(args[2]))
	end := []byte(tree.MustBeDBytes(args[3]))
	// We use the keys as-is if they are valid engine keys, otherwise we encode
	// them as a version-less key. This preserves the ability to pass in
	// manually constructed engine keys, but also allows for the common practice
	// of passing in keys extracted from the ranges table. It's possible
	// (although somewhat unlikely) for a user key to validate as an engine key.
	if ek, ok := storage.DecodeEngineKey(start); !ok || ek.Validate() != nil {
		start = storage.EncodeMVCCKey(storage.MVCCKey{Key: start})
	}
	if ek, ok := storage.DecodeEngineKey(end); !ok || ek.Validate() != nil {
		end = storage.EncodeMVCCKey(storage.MVCCKey{Key: end})
	}

	var megabytesPerSecond int64
	if len(args) > 4 {
		megabytesPerSecond = int64(tree.MustBeDInt(args[4]))
	} else {
		megabytesPerSecond = int64(10)
	}

	return newStorageInternalKeysGenerator(evalCtx, nodeID, storeID, start, end, megabytesPerSecond), nil
}

var tableSpanStatsGeneratorType = types.MakeLabeledTuple(
	[]*types.T{types.Int, types.Int, types.Int, types.Int, types.Int, types.Int, types.Float},
	[]string{"database_id", "table_id", "range_count", "approximate_disk_bytes", "live_bytes", "total_bytes", "live_percentage"},
)

func makeTableSpanStatsGenerator(
	ctx context.Context, evalCtx *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	// The user must have ADMIN role or VIEWACTIVITY/VIEWACTIVITYREDACTED permission to use this builtin.
	hasViewActivity, _, err := evalCtx.SessionAccessor.HasViewActivityOrViewActivityRedactedRole(ctx)
	if err != nil {
		return nil, err
	}
	if !hasViewActivity {
		return nil, pgerror.Newf(pgcode.InsufficientPrivilege, "user needs ADMIN role or the VIEWACTIVITY/VIEWACTIVITYREDACTED permission to view span statistics")
	}
	dbId := 0
	tableId := 0
	if len(args) > 0 {
		dbId = int(tree.MustBeDInt(args[0]))
		if dbId <= 0 {
			return nil, errors.New("provided database id must be greater than or equal to 1")
		}
	}
	if len(args) > 1 {
		tableId = int(tree.MustBeDInt(args[1]))
		if tableId <= 0 {
			return nil, errors.New("provided table id must be greater than or equal to 1")
		}
	}

	spanBatchLimit := SpanStatsBatchLimit.Get(&evalCtx.Settings.SV)
	return newTableSpanStatsIterator(evalCtx, dbId, tableId,
		int(spanBatchLimit)), nil
}

var spanStatsGeneratorType = types.MakeLabeledTuple(
	[]*types.T{types.Bytes, types.Bytes, types.Json},
	[]string{"start_key", "end_key", "stats"},
)

type spanStatsValueGenerator struct {
	spans     roachpb.Spans // spans are provided as an argument.
	res       *roachpb.SpanStatsResponse
	currStats *roachpb.SpanStats
	currSpan  roachpb.Span
	idx       int
	p         eval.Planner
}

func (s *spanStatsValueGenerator) ResolvedType() *types.T {
	return spanStatsGeneratorType
}

func (s *spanStatsValueGenerator) Start(ctx context.Context, txn *kv.Txn) error {
	res, err := s.p.SpanStats(ctx, s.spans)
	s.res = res
	return err
}

func (s *spanStatsValueGenerator) Next(ctx context.Context) (bool, error) {
	// We must stop iterating after emitting values for all spans.
	if s.idx == len(s.spans) {
		return false, nil
	}
	sp := s.spans[s.idx]
	s.currStats = s.res.SpanToStats[sp.String()]
	s.currSpan = sp
	s.idx++
	return true, nil
}

func (s *spanStatsValueGenerator) Values() (tree.Datums, error) {
	jsonStr, err := gojson.Marshal(s.currStats)
	if err != nil {
		return nil, err
	}
	jsonDatum, err := tree.ParseDJSON(string(jsonStr))
	if err != nil {
		return nil, err
	}
	return []tree.Datum{
		tree.NewDBytes(tree.DBytes(s.currSpan.Key)),
		tree.NewDBytes(tree.DBytes(s.currSpan.EndKey)),
		jsonDatum,
	}, nil
}

func (s *spanStatsValueGenerator) Close(ctx context.Context) {}

func makeSpanStatsGenerator(
	ctx context.Context, evalCtx *eval.Context, args tree.Datums,
) (eval.ValueGenerator, error) {
	argSpans := tree.MustBeDArray(args[0])
	spans := make([]roachpb.Span, 0, argSpans.Len())
	for _, span := range argSpans.Array {
		s := tree.MustBeDTuple(span)
		if len(s.D) != 2 || s.D[0] == tree.DNull || s.D[1] == tree.DNull {
			continue
		}
		startKey := roachpb.Key(tree.MustBeDBytes(s.D[0]))
		endKey := roachpb.Key(tree.MustBeDBytes(s.D[1]))
		spans = append(spans, roachpb.Span{
			Key:    startKey,
			EndKey: endKey,
		})
	}

	return &spanStatsValueGenerator{p: evalCtx.Planner, spans: spans}, nil
}

var internallyExecutedQueryGeneratorType = types.String

func makeInternallyExecutedQueryGeneratorOverload(
	withSessionBound, withOverrides, withTxn bool,
) tree.Overload {
	inputTypes := tree.ParamTypes{{Name: "query", Typ: types.String}}
	if withSessionBound {
		inputTypes = append(inputTypes, tree.ParamType{Name: "session_bound", Typ: types.Bool})
	}
	if withOverrides {
		inputTypes = append(inputTypes, tree.ParamType{Name: "overrides", Typ: types.String})
	}
	if withTxn {
		if !withOverrides {
			// In order to not confuse two boolean arguments we require that
			// whenever 'use_session_txn' is specified, 'overrides' must be
			// specified too.
			panic(errors.AssertionFailedf("'use_session_txn' requires 'overrides' to be used"))
		}
		inputTypes = append(inputTypes, tree.ParamType{Name: "use_session_txn", Typ: types.Bool})
	}
	return makeGeneratorOverload(
		inputTypes,
		internallyExecutedQueryGeneratorType,
		func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (eval.ValueGenerator, error) {
			if err := evalCtx.SessionAccessor.CheckPrivilege(
				ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
			); err != nil {
				return nil, err
			}
			numExpectedArgs, queryIdx, sessionBoundIdx, overridesIdx, txnIdx := 1, 0, 1, 2, 3
			if withSessionBound {
				numExpectedArgs++
			} else {
				overridesIdx--
				txnIdx--
			}
			if withOverrides {
				numExpectedArgs++
			}
			if withTxn {
				numExpectedArgs++
			}
			if len(args) != numExpectedArgs {
				return nil, errors.Newf("expected %d argument, got %d", numExpectedArgs, len(args))
			}
			q, ok := args[queryIdx].(*tree.DString)
			if !ok {
				return nil, errors.Newf("expected string argument for 'query', got %s", args[queryIdx].ResolvedType())
			}
			query := string(*q)
			// Sanity check that a single statement was provided. Note that the
			// internal executor will parse the query too, but we'd get an
			// assertion failure error if we gave it more than one statement -
			// we catch those cases here.
			stmts, err := parser.Parse(query)
			if err != nil {
				return nil, err
			}
			if len(stmts) != 1 {
				return nil, errors.Newf("only one statement is supported, %d were given", len(stmts))
			}
			if stmts[0].AST.StatementReturnType() == tree.Ack {
				// We want to disallow statements that modify txn state (like
				// BEGIN and COMMIT). Such statements (as well as some others
				// like changing cluster settings and dealing with prepared
				// statements) have the Ack return type, so we'll lean on the
				// safe side and prohibit them all.
				return nil, errors.New("this statement is disallowed")
			}
			var sessionBound bool
			if withSessionBound {
				s, ok := args[sessionBoundIdx].(*tree.DBool)
				if !ok {
					return nil, errors.Newf("expected bool argument for 'session_bound', got %s", args[sessionBoundIdx].ResolvedType())
				}
				sessionBound = bool(*s)
			}
			var overrides string
			if withOverrides {
				o, ok := args[overridesIdx].(*tree.DString)
				if !ok {
					return nil, errors.Newf("expected string argument for 'overrides', got %s", args[overridesIdx].ResolvedType())
				}
				overrides = string(*o)
			}
			var useTxn bool
			if withTxn {
				t, ok := args[txnIdx].(*tree.DBool)
				if !ok {
					return nil, errors.Newf("expected string argument for 'use_session_txn', got %s", args[txnIdx].ResolvedType())
				}
				useTxn = bool(*t)
				if sessionBound && useTxn {
					return nil, errors.New("when session bound internal executor is used, it always uses the session txn - omit the last argument")
				}
			}
			return newInternallyExecutedQueryIterator(evalCtx, query, sessionBound, overrides, useTxn), nil
		},
		"Executes the provided query via the Internal Executor and prints "+
			"out the result, in which each row is converted to a single string. "+
			"First argument is the single query to be executed. Optional "+
			"'session_bound' argument specifies whether the Internal Executor "+
			"should be bound to the current session ('false' by default). "+
			"Optional 'overrides' argument is a comma-separated list of session "+
			"variable overrides. Optional 'use_session_txn' argument is a boolean "+
			"indicating whether the Internal Executor should use the session's txn "+
			"(this is only applicable when 'session_bound' is 'false', and usage of "+
			"this option requires specifying 'overrides' argument).",
		volatility.Volatile,
	)
}

type internallyExecutedQueryIterator struct {
	evalCtx      *eval.Context
	query        string
	sessionBound bool
	overrides    string
	useTxn       bool
	formatter    *tree.FmtCtx

	rows eval.InternalRows
	a    tree.DatumAlloc
	buf  [1]tree.Datum
}

func newInternallyExecutedQueryIterator(
	evalCtx *eval.Context, query string, sessionBound bool, overrides string, useTxn bool,
) *internallyExecutedQueryIterator {
	return &internallyExecutedQueryIterator{
		evalCtx:      evalCtx,
		query:        query,
		sessionBound: sessionBound,
		overrides:    overrides,
		useTxn:       useTxn,
		formatter:    tree.NewFmtCtx(tree.FmtExport),
	}
}

// ExecuteQueryViaJobExecContext executes the provided query via the JobExecCtx
// of the eval.Context. The method is initialized in the sql package to avoid
// import cycles.
var ExecuteQueryViaJobExecContext func(*eval.Context, context.Context, redact.RedactableString, *kv.Txn, sessiondata.InternalExecutorOverride, string, ...interface{}) (eval.InternalRows, error)

// Start implements the eval.ValueGenerator interface.
func (qi *internallyExecutedQueryIterator) Start(ctx context.Context, txn *kv.Txn) error {
	var opName redact.RedactableString = "internally-executed-query-builtin"
	var ieo sessiondata.InternalExecutorOverride
	// Always use the session's user, even in "jobs-like" mode.
	ieo.User = qi.evalCtx.SessionData().User()
	ieo.MultiOverride = qi.overrides
	var rows eval.InternalRows
	var err error
	if qi.sessionBound {
		rows, err = qi.evalCtx.Planner.QueryIteratorEx(ctx, opName, ieo, qi.query)
	} else {
		var txnArg *kv.Txn
		if qi.useTxn {
			txnArg = txn
		}
		rows, err = ExecuteQueryViaJobExecContext(qi.evalCtx, ctx, opName, txnArg, ieo, qi.query)
	}
	if err != nil {
		return err
	}
	qi.rows = rows
	return nil
}

// Next implements the eval.ValueGenerator interface.
func (qi *internallyExecutedQueryIterator) Next(ctx context.Context) (bool, error) {
	return qi.rows.Next(ctx)
}

// Values implements the eval.ValueGenerator interface.
func (qi *internallyExecutedQueryIterator) Values() (tree.Datums, error) {
	datums := qi.rows.Cur()
	qi.formatter.Reset()
	for i, v := range datums {
		if i > 0 {
			qi.formatter.WriteString(", ")
		}
		qi.formatter.FormatNode(v)
	}
	qi.buf[0] = qi.a.NewDString(tree.DString(qi.formatter.String()))
	return qi.buf[:], nil
}

// Close implements the eval.ValueGenerator interface.
func (qi *internallyExecutedQueryIterator) Close(context.Context) {
	if qi.rows != nil {
		_ = qi.rows.Close()
	}
}

// ResolvedType implements the eval.ValueGenerator interface.
func (qi *internallyExecutedQueryIterator) ResolvedType() *types.T {
	return internallyExecutedQueryGeneratorType
}
