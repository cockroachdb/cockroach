// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/embedding"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/vector"
)

func init() {
	for k, v := range embeddingBuiltins {
		v.props.Category = builtinconstants.CategoryEmbedding
		v.props.AvailableOnPublicSchema = true
		const enforceClass = true
		registerBuiltin(k, v, tree.NormalClass, enforceClass)
	}
}

var embeddingBuiltins = map[string]builtinDefinition{
	"embed": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "text", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.PGVector),
			Fn: func(
				_ context.Context, _ *eval.Context, args tree.Datums,
			) (tree.Datum, error) {
				eng, err := embedding.GetEngine()
				if err != nil {
					return nil, err
				}
				text := string(tree.MustBeDString(args[0]))
				vec, err := eng.Embed(text)
				if err != nil {
					return nil, err
				}
				return tree.NewDPGVector(vector.T(vec)), nil
			},
			Info: "Returns the vector embedding of the input text using the " +
				"all-MiniLM-L6-v2 model (384 dimensions). The model is " +
				"downloaded automatically on first use. Requires the ONNX " +
				"Runtime library (--embedding-libs).",
			Volatility: volatility.Stable,
		},
	),
}
