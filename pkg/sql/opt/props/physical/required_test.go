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

package physical_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
)

func TestRequiredProps(t *testing.T) {
	// Empty props.
	phys := &physical.Required{}
	testRequiredProps(t, phys, "[]")

	if phys.Defined() {
		t.Error("no props should be defined")
	}

	// Presentation props.
	presentation := physical.Presentation{
		opt.AliasedColumn{Alias: "a", ID: 1},
		opt.AliasedColumn{Alias: "b", ID: 2},
	}
	phys = &physical.Required{Presentation: presentation}
	testRequiredProps(t, phys, "[presentation: a:1,b:2]")

	if presentation.Any() {
		t.Error("presentation should not be empty")
	}

	if !presentation.Equals(presentation) {
		t.Error("presentation should equal itself")
	}

	if presentation.Equals(physical.Presentation{}) {
		t.Error("presentation should not equal the empty presentation")
	}

	// Add ordering props.
	ordering := physical.ParseOrderingChoice("+1,+5")
	phys.Ordering = ordering
	testRequiredProps(t, phys, "[presentation: a:1,b:2] [ordering: +1,+5]")
}

func testRequiredProps(t *testing.T, physProps *physical.Required, expected string) {
	t.Helper()
	actual := physProps.String()
	if actual != expected {
		t.Errorf("\nexpected: %s\nactual: %s", expected, actual)
	}
}
