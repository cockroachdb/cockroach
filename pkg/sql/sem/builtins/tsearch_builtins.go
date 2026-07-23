// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/tsearch"
)

func init() {
	for k, v := range tsearchBuiltins {
		v.props.Category = builtinconstants.CategoryFullTextSearch
		v.props.AvailableOnPublicSchema = true
		// Most builtins in this file are of the Normal class, but there is one
		// (at the time of writing) of the Generator class.
		const enforceClass = false
		registerBuiltin(k, v, tree.NormalClass, enforceClass)
	}
}

type tsParseGenerator struct {
	input     string
	tokens    []string
	nextToken string
}

func (t tsParseGenerator) ResolvedType() *types.T {
	return tsParseType
}

func (t *tsParseGenerator) Start(_ context.Context, _ *kv.Txn) error {
	t.tokens = tsearch.TSParse(t.input)
	return nil
}

func (t *tsParseGenerator) Next(_ context.Context) (bool, error) {
	if len(t.tokens) == 0 {
		return false, nil
	}
	t.nextToken, t.tokens = t.tokens[0], t.tokens[1:]
	return true, nil
}

func (t tsParseGenerator) Values() (tree.Datums, error) {
	return tree.Datums{tree.NewDInt(1), tree.NewDString(t.nextToken)}, nil
}

func (t tsParseGenerator) Close(_ context.Context) {}

var tsParseType = types.MakeLabeledTuple(
	[]*types.T{types.Int, types.String},
	[]string{"tokid", "token"},
)

var tsearchBuiltins = map[string]builtinDefinition{
	"ts_parse": makeBuiltin(genProps(),
		makeGeneratorOverload(
			tree.ParamTypes{{Name: "parser_name", Typ: types.String}, {Name: "document", Typ: types.String}},
			types.MakeLabeledTuple(
				[]*types.T{types.Int, types.String},
				[]string{"tokid", "token"},
			),
			func(_ context.Context, _ *eval.Context, args tree.Datums) (eval.ValueGenerator, error) {
				parserName := string(tree.MustBeDString(args[0]))
				if parserName != "default" {
					return nil, pgerror.Newf(pgcode.UndefinedObject, "text search parser %q does not exist", parserName)
				}
				return &tsParseGenerator{input: string(tree.MustBeDString(args[1]))}, nil
			},
			"ts_parse parses the given document and returns a series of records, "+
				"one for each token produced by parsing. "+
				"Each record includes a tokid showing the assigned token type and a token which is the text of the token.",
			volatility.Stable,
		),
	),
	// Full text search functions.
	"to_tsvector": makeBuiltin(
		tree.FunctionProperties{},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "config", Typ: types.String}, {Name: "text", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.TSVector),
			Fn: func(_ context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				// Parse, stem, and stopword the input.
				config := string(tree.MustBeDString(args[0]))
				document := string(tree.MustBeDString(args[1]))
				vector, err := tsearch.DocumentToTSVector(config, document)
				if err != nil {
					return nil, err
				}
				return &tree.DTSVector{TSVector: vector}, nil
			},
			Info: "Converts text to a tsvector, normalizing words according to the specified configuration. " +
				"Position information is included in the result.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "text", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.TSVector),
			Fn: func(_ context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				config := tsearch.GetConfigKey(evalCtx.SessionData().DefaultTextSearchConfig)
				document := string(tree.MustBeDString(args[0]))
				vector, err := tsearch.DocumentToTSVector(config, document)
				if err != nil {
					return nil, err
				}
				return &tree.DTSVector{TSVector: vector}, nil
			},
			Info: "Converts text to a tsvector, normalizing words according to the default configuration. " +
				"Position information is included in the result.",
			Volatility: volatility.Stable,
		},
	),
	"to_tsquery": makeBuiltin(
		tree.FunctionProperties{},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "config", Typ: types.String}, {Name: "text", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.TSQuery),
			Fn: func(_ context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				config := string(tree.MustBeDString(args[0]))
				input := string(tree.MustBeDString(args[1]))
				query, err := tsearch.ToTSQuery(config, input)
				if err != nil {
					return nil, err
				}
				return &tree.DTSQuery{TSQuery: query}, nil
			},
			Info: "Converts the input text into a tsquery by normalizing each word in the input according to " +
				"the specified configuration. The input must already be formatted like a tsquery, in other words, " +
				"subsequent tokens must be connected by a tsquery operator (&, |, <->, !).",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "text", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.TSQuery),
			Fn: func(_ context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				config := tsearch.GetConfigKey(evalCtx.SessionData().DefaultTextSearchConfig)
				input := string(tree.MustBeDString(args[0]))
				query, err := tsearch.ToTSQuery(config, input)
				if err != nil {
					return nil, err
				}
				return &tree.DTSQuery{TSQuery: query}, nil
			},
			Info: "Converts the input text into a tsquery by normalizing each word in the input according to " +
				"the default configuration. The input must already be formatted like a tsquery, in other words, " +
				"subsequent tokens must be connected by a tsquery operator (&, |, <->, !).",
			Volatility: volatility.Stable,
		},
	),
	"plainto_tsquery": makeBuiltin(
		tree.FunctionProperties{},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "config", Typ: types.String}, {Name: "text", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.TSQuery),
			Fn: func(_ context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				config := string(tree.MustBeDString(args[0]))
				input := string(tree.MustBeDString(args[1]))
				query, err := tsearch.PlainToTSQuery(config, input)
				if err != nil {
					return nil, err
				}
				return &tree.DTSQuery{TSQuery: query}, nil
			},
			Info: "Converts text to a tsquery, normalizing words according to the specified configuration." +
				" The & operator is inserted between each token in the input.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "text", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.TSQuery),
			Fn: func(_ context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				config := tsearch.GetConfigKey(evalCtx.SessionData().DefaultTextSearchConfig)
				input := string(tree.MustBeDString(args[0]))
				query, err := tsearch.PlainToTSQuery(config, input)
				if err != nil {
					return nil, err
				}
				return &tree.DTSQuery{TSQuery: query}, nil
			},
			Info: "Converts text to a tsquery, normalizing words according to the default configuration." +
				" The & operator is inserted between each token in the input.",
			Volatility: volatility.Stable,
		},
	),
	"phraseto_tsquery": makeBuiltin(
		tree.FunctionProperties{},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "config", Typ: types.String}, {Name: "text", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.TSQuery),
			Fn: func(_ context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				config := string(tree.MustBeDString(args[0]))
				input := string(tree.MustBeDString(args[1]))
				query, err := tsearch.PhraseToTSQuery(config, input)
				if err != nil {
					return nil, err
				}
				return &tree.DTSQuery{TSQuery: query}, nil
			},
			Info: "Converts text to a tsquery, normalizing words according to the specified configuration." +
				" The <-> operator is inserted between each token in the input.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "text", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.TSQuery),
			Fn: func(_ context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				config := tsearch.GetConfigKey(evalCtx.SessionData().DefaultTextSearchConfig)
				input := string(tree.MustBeDString(args[0]))
				query, err := tsearch.PhraseToTSQuery(config, input)
				if err != nil {
					return nil, err
				}
				return &tree.DTSQuery{TSQuery: query}, nil
			},
			Info: "Converts text to a tsquery, normalizing words according to the default configuration." +
				" The <-> operator is inserted between each token in the input.",
			Volatility: volatility.Stable,
		},
	),
	"ts_rank": makeBuiltin(
		tree.FunctionProperties{},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "weights", Typ: types.FloatArray},
				{Name: "vector", Typ: types.TSVector},
				{Name: "query", Typ: types.TSQuery},
				{Name: "normalization", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Float4),
			Fn: func(_ context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				weights, err := getWeights(tree.MustBeDArray(args[0]))
				if err != nil {
					return nil, err
				}
				rank, err := tsearch.Rank(
					weights,
					tree.MustBeDTSVector(args[1]).TSVector,
					tree.MustBeDTSQuery(args[2]).TSQuery,
					int(tree.MustBeDInt(args[3])),
				)
				if err != nil {
					return nil, err
				}
				return tree.NewDFloat(tree.DFloat(rank)), nil
			},
			Info:       "Ranks vectors based on the frequency of their matching lexemes.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "weights", Typ: types.FloatArray},
				{Name: "vector", Typ: types.TSVector},
				{Name: "query", Typ: types.TSQuery},
			},
			ReturnType: tree.FixedReturnType(types.Float4),
			Fn: func(_ context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				weights, err := getWeights(tree.MustBeDArray(args[0]))
				if err != nil {
					return nil, err
				}
				rank, err := tsearch.Rank(
					weights,
					tree.MustBeDTSVector(args[1]).TSVector,
					tree.MustBeDTSQuery(args[2]).TSQuery,
					0,
				)
				if err != nil {
					return nil, err
				}
				return tree.NewDFloat(tree.DFloat(rank)), nil
			},
			Info:       "Ranks vectors based on the frequency of their matching lexemes.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "vector", Typ: types.TSVector},
				{Name: "query", Typ: types.TSQuery},
				{Name: "normalization", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Float4),
			Fn: func(_ context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				rank, err := tsearch.Rank(
					nil, /* weights */
					tree.MustBeDTSVector(args[0]).TSVector,
					tree.MustBeDTSQuery(args[1]).TSQuery,
					int(tree.MustBeDInt(args[2])),
				)
				if err != nil {
					return nil, err
				}
				return tree.NewDFloat(tree.DFloat(rank)), nil
			},
			Info:       "Ranks vectors based on the frequency of their matching lexemes.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "vector", Typ: types.TSVector},
				{Name: "query", Typ: types.TSQuery},
			},
			ReturnType: tree.FixedReturnType(types.Float4),
			Fn: func(_ context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				rank, err := tsearch.Rank(
					nil, /* weights */
					tree.MustBeDTSVector(args[0]).TSVector,
					tree.MustBeDTSQuery(args[1]).TSQuery,
					0, /* method */
				)
				if err != nil {
					return nil, err
				}
				return tree.NewDFloat(tree.DFloat(rank)), nil
			},
			Info:       "Ranks vectors based on the frequency of their matching lexemes.",
			Volatility: volatility.Immutable,
		},
	),
}

func getWeights(arr *tree.DArray) ([]float32, error) {
	ret := make([]float32, 4)
	if arr.Len() < len(ret) {
		return ret, pgerror.New(pgcode.ArraySubscript, "array of weight is too short (must be at least 4)")
	}
	for i, d := range arr.Array[:len(ret)] {
		if d == tree.DNull {
			return ret, pgerror.New(pgcode.NullValueNotAllowed, "array of weight must not contain null")
		}
		ret[i] = float32(tree.MustBeDFloat(d))
	}
	return ret, nil
}
