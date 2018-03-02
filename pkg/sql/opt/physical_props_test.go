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

package opt

import (
	"testing"
)

func TestPhysicalProps(t *testing.T) {
	// Empty props.
	props := &PhysicalProps{}
	testPhysicalProps(t, props, "")

	if props.Defined() {
		t.Error("no props should be defined")
	}

	// Presentation props.
	presentation := Presentation{
		LabeledColumn{Label: "a", Index: 1},
		LabeledColumn{Label: "b", Index: 2},
	}
	props = &PhysicalProps{Presentation: presentation}
	testPhysicalProps(t, props, "p:a:1,b:2")

	if !presentation.Defined() {
		t.Error("presentation should be defined")
	}

	if !presentation.Provides(presentation) {
		t.Error("presentation should provide itself")
	}

	if presentation.Provides(Presentation{}) {
		t.Error("presentation should not provide the empty presentation")
	}

	// Add Ordering props.
	ordering := Ordering{ColumnIndex(1), ColumnIndex(5)}
	props.Ordering = ordering
	testPhysicalProps(t, props, "p:a:1,b:2 o:+1,+5")

	if !ordering.Defined() {
		t.Error("ordering should be defined")
	}

	if !ordering.Provides(ordering) {
		t.Error("ordering should provide itself")
	}

	if !ordering.Provides(Ordering{ColumnIndex(1)}) {
		t.Error("ordering should provide the prefix ordering")
	}

	if (Ordering{}).Provides(ordering) {
		t.Error("empty ordering should not provide ordering")
	}

	if !ordering.Provides(Ordering{}) {
		t.Error("ordering should provide the empty ordering")
	}
}

func testPhysicalProps(t *testing.T, physProps *PhysicalProps, expected string) {
	actual := physProps.Fingerprint()
	if actual != expected {
		t.Errorf("\nexpected: %s\nactual: %s", expected, actual)
	}
}
