// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package btree

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/btree/internal/ordered"
)

func TestBTree(t *testing.T) {
	assertEq := func(t *testing.T, exp, got int) {
		t.Helper()
		if exp != got {
			t.Fatalf("expected %d, got %d", exp, got)
		}
	}

	tree := NewSetConfig(ordered.Compare[int]).MakeSet()
	tree.Upsert(2)
	tree.Upsert(12)
	tree.Upsert(1)

	it := tree.MakeIter()
	it.First()
	expected := []int{1, 2, 12}
	for _, exp := range expected {
		assertEq(t, exp, it.Cur())
		it.Next()
	}
}
