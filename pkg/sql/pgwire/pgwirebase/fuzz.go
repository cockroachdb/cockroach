// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

//go:build gofuzz

package pgwirebase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

var (
	// Compile a slice of all typs.
	typs = func() []*types.T {
		var ret []*types.T
		for _, typ := range types.OidToType {
			ret = append(ret, typ)
		}
		return ret
	}()
)

func FuzzDecodeDatum(data []byte) int {
	if len(data) < 2 {
		return 0
	}

	typ := typs[int(data[1])%len(typs)]
	code := FormatCode(data[0]) % (FormatBinary + 1)
	b := data[2:]

	evalCtx := eval.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())

	_, err := DecodeDatum(context.Background(), evalCtx, typ, code, b, &tree.DatumAlloc{})
	if err != nil {
		return 0
	}
	return 1
}
