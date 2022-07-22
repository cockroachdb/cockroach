// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins

import (
	"context"

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
		registerBuiltin(k, v)
	}
}

var tsearchBuiltins = map[string]builtinDefinition{
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
			Info: "Converts text to a tsvector, normalizing words according to the specified or default configuration. " +
				"Position information is included in the result.",
			Volatility: volatility.Immutable,
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
				"the specified or default configuration. The input must already be formatted like a tsquery, in other words, " +
				"subsequent tokens must be connected by a tsquery operator (&, |, <->, !).",
			Volatility: volatility.Immutable,
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
			Info: "Converts text to a tsquery, normalizing words according to the specified or default configuration." +
				" The & operator is inserted between each token in the input.",
			Volatility: volatility.Immutable,
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
			Info: "Converts text to a tsquery, normalizing words according to the specified or default configuration." +
				" The <-> operator is inserted between each token in the input.",
			Volatility: volatility.Immutable,
		},
	),
}
