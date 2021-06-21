// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package roachpb

import (
	"reflect"
	"testing"
)

func TestSpanGroup(t *testing.T) {
	g := &SpanGroup{}

	be := makeSpan("b-e")

	if g.Contains(Key("a")) {
		t.Fatal("empty group should not contain a")
	}
	if g.Sub(be) {
		t.Fatalf("removing b-e from empty group should not expand it")
	}
	if !g.Add(be) {
		t.Fatal("adding b-e to empty should expand it")
	}
	if !g.Sub(be) {
		t.Fatalf("removing b-e from b-e should expand it")
	}
	g.Add(be)
	if g.Add(be) {
		t.Fatal("adding  b-e to b-e should not expand it")
	}
	if g.Add(makeSpan("c-d")) {
		t.Fatal("adding c-d to b-e should not expand it")
	}
	if g.Add(makeSpan("b-d")) {
		t.Fatal("adding b-d to b-e should not expand it")
	}
	if g.Add(makeSpan("d-e")) {
		t.Fatal("adding d-e to b-e should not expand it")
	}
	if got, expected := g.Len(), 1; got != expected {
		t.Fatalf("got %d, expected %d", got, expected)
	}
	if got, expected := g.Slice(), be; len(got) != 1 || !reflect.DeepEqual(got[0], expected) {
		t.Fatalf("got %v, expected %v", got, expected)
	}
	for _, k := range []string{"b", "c", "d"} {
		if !g.Contains(Key(k)) {
			t.Fatalf("span b-e should contain %q", k)
		}
	}
	for _, k := range []string{"a", "e", "f"} {
		if g.Contains(Key(k)) {
			t.Fatalf("span b-e should not contain %q", k)
		}
	}
	if !g.Sub(makeSpan("d-e"), makeSpan("b-c")) {
		t.Fatalf("removing b-c and d-e from b-e should expand it")
	}
	if got, expected := g.Slice(), makeSpan("c-d"); len(got) != 1 || !reflect.DeepEqual(got[0], expected) {
		t.Fatalf("got %v, expected %v", got, expected)
	}
}
