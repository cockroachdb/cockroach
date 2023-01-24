// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package btree_test

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/btree"
	"github.com/cockroachdb/cockroach/pkg/util/btree/ordered"
)

func ExampleMap() {
	m := btree.MakeMapWithFunc[string, int](
		ordered.Compare[string],
	)
	m.Upsert("foo", 1)
	m.Upsert("bar", 2)
	fmt.Println(m.Get("foo"))
	fmt.Println(m.Get("bar"))
	fmt.Println(m.Get("baz"))
	it := m.MakeIter()
	for it.First(); it.Valid(); it.Next() {
		fmt.Println(it.Cur(), it.Value())
	}

	// Output:
	// foo 1 true
	// bar 2 true
	//  0 false
	// bar 2
	// foo 1
}
