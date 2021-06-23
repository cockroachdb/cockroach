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
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
)

func TestOperationsFormat(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		step     Step
		expected string
	}{
		{step: step(get(`a`)), expected: `db0.Get(ctx, "a")`},
		{step: step(batch(get(`b`), reverseScanForUpdate(`c`, `e`), get(`f`))), expected: `
			{
			  b := &Batch{}
			  b.Get(ctx, "b")
			  b.ReverseScanForUpdate(ctx, "c", "e")
			  b.Get(ctx, "f")
			  db0.Run(ctx, b)
			}
		`},
		{
			step: step(closureTxn(ClosureTxnType_Commit, batch(get(`g`), get(`h`)), put(`i`, `j`))),
			expected: `
			db0.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			  {
			    b := &Batch{}
			    b.Get(ctx, "g")
			    b.Get(ctx, "h")
			    txn.Run(ctx, b)
			  }
			  txn.Put(ctx, "i", j)
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
