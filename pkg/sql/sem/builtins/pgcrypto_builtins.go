// Copyright 2015 The Cockroach Authors.
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
	"crypto/hmac"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func initPgcryptoBuiltins() {
	// Add all pgcryptoBuiltins to the builtins map after a sanity check.
	for k, v := range pgcryptoBuiltins {
		if _, exists := builtins[k]; exists {
			panic("duplicate builtin: " + k)
		}
		builtins[k] = v
	}
}

var pgcryptoBuiltins = map[string]builtinDefinition{
	"digest": makeBuiltin(
		tree.FunctionProperties{Category: categoryCrypto},
		tree.Overload{
			Types:      tree.ArgTypes{{"data", types.String}, {"type", types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				alg := tree.MustBeDString(args[1])
				hashFunc, err := getHashFunc(string(alg))
				if err != nil {
					return nil, err
				}
				h := hashFunc()
				if ok, err := feedHash(h, args[:1]); !ok || err != nil {
					return tree.DNull, err
				}
				return tree.NewDBytes(tree.DBytes(h.Sum(nil))), nil
			},
			Info: "Computes a binary hash of the given `data`. `type` is the algorithm " +
				"to use (md5, sha1, sha224, sha256, sha384, or sha512).",
			Volatility: volatility.LeakProof,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"data", types.Bytes}, {"type", types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				alg := tree.MustBeDString(args[1])
				hashFunc, err := getHashFunc(string(alg))
				if err != nil {
					return nil, err
				}
				h := hashFunc()
				if ok, err := feedHash(h, args[:1]); !ok || err != nil {
					return tree.DNull, err
				}
				return tree.NewDBytes(tree.DBytes(h.Sum(nil))), nil
			},
			Info: "Computes a binary hash of the given `data`. `type` is the algorithm " +
				"to use (md5, sha1, sha224, sha256, sha384, or sha512).",
			Volatility: volatility.Immutable,
		},
	),

	"gen_random_uuid": generateRandomUUID4Impl,

	"hmac": makeBuiltin(
		tree.FunctionProperties{Category: categoryCrypto},
		tree.Overload{
			Types:      tree.ArgTypes{{"data", types.String}, {"key", types.String}, {"type", types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				key := tree.MustBeDString(args[1])
				alg := tree.MustBeDString(args[2])
				hashFunc, err := getHashFunc(string(alg))
				if err != nil {
					return nil, err
				}
				h := hmac.New(hashFunc, []byte(key))
				if ok, err := feedHash(h, args[:1]); !ok || err != nil {
					return tree.DNull, err
				}
				return tree.NewDBytes(tree.DBytes(h.Sum(nil))), nil
			},
			Info:       "Calculates hashed MAC for `data` with key `key`. `type` is the same as in `digest()`.",
			Volatility: volatility.LeakProof,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"data", types.Bytes}, {"key", types.Bytes}, {"type", types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				key := tree.MustBeDBytes(args[1])
				alg := tree.MustBeDString(args[2])
				hashFunc, err := getHashFunc(string(alg))
				if err != nil {
					return nil, err
				}
				h := hmac.New(hashFunc, []byte(key))
				if ok, err := feedHash(h, args[:1]); !ok || err != nil {
					return tree.DNull, err
				}
				return tree.NewDBytes(tree.DBytes(h.Sum(nil))), nil
			},
			Info:       "Calculates hashed MAC for `data` with key `key`. `type` is the same as in `digest()`.",
			Volatility: volatility.Immutable,
		},
	),
}
