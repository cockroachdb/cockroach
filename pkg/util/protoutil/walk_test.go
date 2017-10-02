// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package protoutil

import (
	"testing"
)

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
