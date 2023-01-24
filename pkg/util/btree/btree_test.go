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

	"github.com/cockroachdb/cockroach/pkg/util/btree/ordered"
	"github.com/stretchr/testify/require"
)

// The below test, for reasons I do not understand, cause an internal
// compiler error.
func TestBTree(t *testing.T) {

	// You might think that this could be defined:
	//
	//  var tree Set[WithOrdered[int], int]
	//
	// However, in go1.19.4, without GOEXPERIMENT=unified, this hits a compiler
	// error.
	tree := MakeSetWithFunc(ordered.Compare[int])
	tree.Upsert(2)
	tree.Upsert(12)
	tree.Upsert(1)

	it := tree.MakeIter()
	it.First()
	expected := []int{1, 2, 12}
	for _, exp := range expected {
		require.Equal(t, exp, it.Cur())
		it.Next()
	}
}
