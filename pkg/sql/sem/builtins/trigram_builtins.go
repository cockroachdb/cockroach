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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/trigram"
)

func initTrigramBuiltins() {
	for k, v := range trigramBuiltins {
		if _, exists := builtins[k]; exists {
			panic("duplicate builtin: " + k)
		}
		v.props.Category = categoryTrigram
		v.props.AvailableOnPublicSchema = true
		builtins[k] = v
	}
}

var trigramBuiltins = map[string]builtinDefinition{
	// Trigram functions.
	"similarity": makeBuiltin(
		tree.FunctionProperties{Category: categoryTrigram},
		tree.Overload{
			Types:      tree.ArgTypes{{"left", types.String}, {"right", types.String}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				l, r := string(tree.MustBeDString(args[0])), string(tree.MustBeDString(args[1]))
				lTrigrams, rTrigrams := trigram.MakeTrigrams(l, true /* pad */), trigram.MakeTrigrams(r, true /* pad */)

				// To calculate the similarity, we count the number of shared trigrams
				// in the strings, and divide by the number of non-shared trigrams.
				// See the CALCSML macro in Postgres contrib/pg_trgm/trgm.h.

				i, j := 0, 0
				nShared := 0
				for i < len(lTrigrams) && j < len(rTrigrams) {
					lTrigram, rTrigram := lTrigrams[i], rTrigrams[j]
					if lTrigram < rTrigram {
						i++
					} else if lTrigram > rTrigram {
						j++
					} else {
						nShared++
						i++
						j++
					}
				}
				shared := float32(nShared)
				return tree.NewDFloat(tree.DFloat(shared / (float32(len(lTrigrams)+len(rTrigrams)) - shared))), nil
			},
			Info: "Returns a number that indicates how similar the two arguments" +
				" are. The range of the result is zero (indicating that the two strings are" +
				" completely dissimilar) to one (indicating that the two strings are" +
				" identical).",
			Volatility: volatility.Immutable,
		},
	),
	"show_trgm": makeBuiltin(
		tree.FunctionProperties{Category: categoryTrigram},
		tree.Overload{
			Types:      tree.ArgTypes{{"input", types.String}},
			ReturnType: tree.FixedReturnType(types.StringArray),
			Fn: func(evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				return trigram.MakeTrigramsDatum(s), nil
			},
			Info:       "Returns an array of all the trigrams in the given string.",
			Volatility: volatility.Immutable,
		},
	),
	"word_similarity":        makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 41285, Category: categoryTrigram}),
	"strict_word_similarity": makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 41285, Category: categoryTrigram}),
	"show_limit":             makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 41285, Category: categoryTrigram}),
	"set_limit":              makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 41285, Category: categoryTrigram}),
}
