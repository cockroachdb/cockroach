// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package physical_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
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

	if presentation.Equals(physical.Presentation(nil)) {
		t.Error("presentation should not equal the empty presentation")
	}

	if presentation.Equals(physical.Presentation{}) {
		t.Error("presentation should not equal the 0 column presentation")
	}

	if (physical.Presentation{}).Equals(physical.Presentation(nil)) {
		t.Error("0 column presentation should not equal the empty presentation")
	}

	// Add ordering props.
	ordering := props.ParseOrderingChoice("+1,+5")
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
