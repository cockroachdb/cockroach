// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package protoutil

import "testing"

type C struct {
	Target *int `cockroachdb:"randnullable"`
}

type B struct {
	F *C `cockroachdb:"noop"`
}

type A struct {
	F *B `cockroachdb:"noop"`
}

func TestWalkReplace(t *testing.T) {
	a1 := &C{}
	a2 := &A{F: &B{F: &C{}}}

	Walk(a1, ZeroInsertingVisitor)
	if a1.Target == nil {
		t.Fatal("target is nil")
	}

	Walk(a2, ZeroInsertingVisitor)
	if a2.F.F.Target == nil {
		t.Fatal("target is nil")
	}

	n := 5
	a1.Target = &n
	Walk(a2, ZeroInsertingVisitor)
	if *a1.Target != n {
		t.Fatal("target got overwritten")
	}
}
