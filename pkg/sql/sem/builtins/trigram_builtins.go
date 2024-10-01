// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/trigram"
)

func init() {
	for k, v := range trigramBuiltins {
		v.props.Category = builtinconstants.CategoryTrigram
		v.props.AvailableOnPublicSchema = true
		const enforceClass = true
		registerBuiltin(k, v, tree.NormalClass, enforceClass)
	}
}

var trigramBuiltins = map[string]builtinDefinition{
	// Trigram functions.
	"similarity": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryTrigram},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "left", Typ: types.String}, {Name: "right", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				l, r := string(tree.MustBeDString(args[0])), string(tree.MustBeDString(args[1]))
				f := trigram.Similarity(l, r)
				return tree.NewDFloat(tree.DFloat(f)), nil
			},
			Info: "Returns a number that indicates how similar the two arguments" +
				" are. The range of the result is zero (indicating that the two strings are" +
				" completely dissimilar) to one (indicating that the two strings are" +
				" identical).",
			Volatility: volatility.Immutable,
		},
	),
	"show_trgm": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryTrigram},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "input", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.StringArray),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				arr := trigram.MakeTrigrams(s, true /* pad */)
				ret := tree.NewDArray(types.String)
				ret.Array = make(tree.Datums, 0, len(arr))
				for i := range arr {
					if err := ret.Append(tree.NewDString(arr[i])); err != nil {
						return nil, err
					}
				}
				return ret, nil
			},
			Info:       "Returns an array of all the trigrams in the given string.",
			Volatility: volatility.Immutable,
		},
	),
	"word_similarity":        makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 41285, Category: builtinconstants.CategoryTrigram}),
	"strict_word_similarity": makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 41285, Category: builtinconstants.CategoryTrigram}),
	"show_limit":             makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 41285, Category: builtinconstants.CategoryTrigram}),
	"set_limit":              makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 41285, Category: builtinconstants.CategoryTrigram}),
}
