// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvnemeses

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestOperationsFormat(t *testing.T) {
	// TODO(dan): Flesh this out.
	s := step(
		get(`a`),
		beginTxn(`1`),
		batch(get(`b`), get(`c`)),
		useTxn(`2`, batch(get(`d`), get(`e`)), put(`f`, `g`)),
	)
	expected := `
		Concurrent
		  db.Get(ctx, "a")
		  txn1 := db.NewTxn(ctx)
		  {
		    b := &Batch{}
		    b.Get(ctx, "b")
		    b.Get(ctx, "c")
		    db.Run(ctx, b)
		  }
		  {
		    {
		      b := &Batch{}
		      b.Get(ctx, "d")
		      b.Get(ctx, "e")
		      txn2.Run(ctx, b)
		    }
		    txn2.Put(ctx, "f", "g")
		  }
	`
	expected = "\n\t\t" + strings.TrimSpace(expected)
	var actual strings.Builder
	s.format(&actual, "\t\t")
	require.Equal(t, expected, actual.String())
}
