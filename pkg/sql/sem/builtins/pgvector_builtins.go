// Copyright 2024 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/util/vector"
)

func init() {
	for k, v := range pgvectorBuiltins {
		v.props.Category = builtinconstants.CategoryPGVector
		v.props.AvailableOnPublicSchema = true
		const enforceClass = true
		registerBuiltin(k, v, tree.NormalClass, enforceClass)
	}
}

var pgvectorBuiltins = map[string]builtinDefinition{
	"cosine_distance": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "v1", Typ: types.PGVector},
				{Name: "v2", Typ: types.PGVector},
			},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				v1 := tree.MustBeDPGVector(args[0])
				v2 := tree.MustBeDPGVector(args[1])
				distance, err := vector.CosDistance(v1.T, v2.T)
				if err != nil {
					return nil, err
				}
				return tree.NewDFloat(tree.DFloat(distance)), nil
			},
			Info:       "Returns the cosine distance between the two vectors.",
			Volatility: volatility.Immutable,
		},
	),
	"inner_product": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "v1", Typ: types.PGVector},
				{Name: "v2", Typ: types.PGVector},
			},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				v1 := tree.MustBeDPGVector(args[0])
				v2 := tree.MustBeDPGVector(args[1])
				distance, err := vector.InnerProduct(v1.T, v2.T)
				if err != nil {
					return nil, err
				}
				return tree.NewDFloat(tree.DFloat(distance)), nil
			},
			Info:       "Returns the inner product between the two vectors.",
			Volatility: volatility.Immutable,
		},
	),
	"l1_distance": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "v1", Typ: types.PGVector},
				{Name: "v2", Typ: types.PGVector},
			},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				v1 := tree.MustBeDPGVector(args[0])
				v2 := tree.MustBeDPGVector(args[1])
				distance, err := vector.L1Distance(v1.T, v2.T)
				if err != nil {
					return nil, err
				}
				return tree.NewDFloat(tree.DFloat(distance)), nil
			},
			Info:       "Returns the Manhattan distance between the two vectors.",
			Volatility: volatility.Immutable,
		},
	),
	"l2_distance": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "v1", Typ: types.PGVector},
				{Name: "v2", Typ: types.PGVector},
			},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				v1 := tree.MustBeDPGVector(args[0])
				v2 := tree.MustBeDPGVector(args[1])
				distance, err := vector.L2Distance(v1.T, v2.T)
				if err != nil {
					return nil, err
				}
				return tree.NewDFloat(tree.DFloat(distance)), nil
			},
			Info:       "Returns the Euclidean distance between the two vectors.",
			Volatility: volatility.Immutable,
		},
	),
	"vector_dims": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "vector", Typ: types.PGVector},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				v1 := tree.MustBeDPGVector(args[0])
				return tree.NewDInt(tree.DInt(len(v1.T))), nil
			},
			Info:       "Returns the number of the dimensions in the vector.",
			Volatility: volatility.Immutable,
		},
	),
	"vector_norm": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "vector", Typ: types.PGVector},
			},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				v1 := tree.MustBeDPGVector(args[0])
				return tree.NewDFloat(tree.DFloat(vector.Norm(v1.T))), nil
			},
			Info:       "Returns the Euclidean norm of the vector.",
			Volatility: volatility.Immutable,
		},
	),
}
