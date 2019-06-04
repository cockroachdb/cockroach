// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License included
// in the file licenses/BSL.txt and at www.mariadb.com/bsl11.
//
// Change Date: 2022-10-01
//
// On the date above, in accordance with the Business Source License, use
// of this software will be governed by the Apache License, Version 2.0,
// included in the file licenses/APL.txt and at
// https://www.apache.org/licenses/LICENSE-2.0

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
