// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package opt_test

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils/testcat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

func TestOrdering(t *testing.T) {
	// Add Ordering props.
	ordering := opt.Ordering{1, 5}

	if ordering.Empty() {
		t.Error("ordering not empty")
	}

	if !ordering.ColSet().Equals(opt.MakeColSet(1, 5)) {
		t.Error("ordering colset should equal the ordering columns")
	}

	if !(opt.Ordering{}).ColSet().Equals(opt.ColSet{}) {
		t.Error("empty ordering should have empty column set")
	}

	if !ordering.Equals(ordering) {
		t.Error("ordering should be equal with itself")
	}

	if ordering.Equals(opt.Ordering{}) {
		t.Error("ordering should not equal the empty ordering")
	}

	if (opt.Ordering{}).Equals(ordering) {
		t.Error("empty ordering should not equal ordering")
	}
}

func TestOrderingColumn_RemapColumn(t *testing.T) {
	var md opt.Metadata
	catalog := testcat.New()
	_, err := catalog.ExecuteDDL("CREATE TABLE tab (a INT PRIMARY KEY, b INT, c INT, d INT);")
	if err != nil {
		t.Fatal(err)
	}
	tn := tree.NewUnqualifiedTableName("tab")
	tab := catalog.Table(tn)

	from := md.AddTable(tab, &tree.TableName{})
	to := md.AddTable(tab, &tree.TableName{})

	col1 := opt.MakeOrderingColumn(from.ColumnID(0), false)
	col2 := opt.MakeOrderingColumn(from.ColumnID(3), true)

	remappedCol1 := col1.RemapColumn(from, to)
	remappedCol2 := col2.RemapColumn(from, to)

	expected := "+1"
	if col1.String() != expected {
		t.Errorf("\ncol1 was changed: %s\n", col1.String())
	}

	expected = "-4"
	if col2.String() != expected {
		t.Errorf("\ncol2 was changed: %s\n", col2.String())
	}

	expected = "+7"
	if remappedCol1.String() != expected {
		t.Errorf("\nexpected: %s\nactual: %s\n", expected, remappedCol1.String())
	}

	expected = "-10"
	if remappedCol2.String() != expected {
		t.Errorf("\nexpected: %s\nactual: %s\n", expected, remappedCol2.String())
	}
}
