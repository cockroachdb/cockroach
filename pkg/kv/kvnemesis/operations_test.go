// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvnemesis

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/assert"
)

func TestOperationsFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()

	tests := []struct {
		step     Step
		expected string
	}{
		{step: step(get(`a`)), expected: `db0.Get(ctx, "a")`},
		{step: step(batch(get(`b`), get(`c`))), expected: `
			{
			  b := &Batch{}
			  b.Get(ctx, "b")
			  b.Get(ctx, "c")
			  db0.Run(ctx, b)
			}
		`},
		{
			step: step(closureTxn(ClosureTxnType_Commit, batch(get(`d`), get(`e`)), put(`f`, `g`))),
			expected: `
			db0.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			  {
			    b := &Batch{}
			    b.Get(ctx, "d")
			    b.Get(ctx, "e")
			    txn.Run(ctx, b)
			  }
			  txn.Put(ctx, "f", g)
			  return nil
			})
			`,
		},
	}

	for _, test := range tests {
		expected := strings.TrimSpace(test.expected)
		var actual strings.Builder
		test.step.format(&actual, formatCtx{indent: "\t\t\t"})
		assert.Equal(t, expected, strings.TrimSpace(actual.String()))
	}
}
