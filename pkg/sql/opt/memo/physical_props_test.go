// Copyright 2018 The Cockroach Authors.
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

package memo_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
)

func TestPhysicalProps(t *testing.T) {
	// Empty props.
	props := &memo.PhysicalProps{}
	testPhysicalProps(t, props, "")

	if props.Defined() {
		t.Error("no props should be defined")
	}

	// Presentation props.
	presentation := memo.Presentation{
		opt.LabeledColumn{Label: "a", ID: 1},
		opt.LabeledColumn{Label: "b", ID: 2},
	}
	props = &memo.PhysicalProps{Presentation: presentation}
	testPhysicalProps(t, props, "p:a:1,b:2")

	if !presentation.Defined() {
		t.Error("presentation should be defined")
	}

	if !presentation.Equals(presentation) {
		t.Error("presentation should equal itself")
	}

	if presentation.Equals(memo.Presentation{}) {
		t.Error("presentation should not equal the empty presentation")
	}

	// Add Ordering props.
	ordering := memo.Ordering{1, 5}
	props.Ordering = ordering
	testPhysicalProps(t, props, "p:a:1,b:2 o:+1,+5")

	if !ordering.Defined() {
		t.Error("ordering should be defined")
	}

	if !ordering.Provides(ordering) {
		t.Error("ordering should provide itself")
	}

	if !ordering.Provides(memo.Ordering{1}) {
		t.Error("ordering should provide the prefix ordering")
	}

	if (memo.Ordering{}).Provides(ordering) {
		t.Error("empty ordering should not provide ordering")
	}

	if !ordering.Provides(memo.Ordering{}) {
		t.Error("ordering should provide the empty ordering")
	}

	if !ordering.Equals(ordering) {
		t.Error("ordering should be equal with itself")
	}

	if ordering.Equals(memo.Ordering{}) {
		t.Error("ordering should not equal the empty ordering")
	}

	if (memo.Ordering{}).Equals(ordering) {
		t.Error("empty ordering should not equal ordering")
	}
}

func testPhysicalProps(t *testing.T, physProps *memo.PhysicalProps, expected string) {
	actual := physProps.Fingerprint()
	if actual != expected {
		t.Errorf("\nexpected: %s\nactual: %s", expected, actual)
	}
}
