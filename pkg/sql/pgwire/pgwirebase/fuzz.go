// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build gofuzz

package pgwirebase

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
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

	evalCtx := tree.NewTestingEvalContext(cluster.MakeTestingClusterSettings())
	defer evalCtx.Stop(context.Background())

	_, err := DecodeDatum(evalCtx, typ, code, b)
	if err != nil {
		return 0
	}
	return 1
}
