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
	"reflect"
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

	if !ordering.Provides(ordering) {
		t.Error("ordering should provide itself")
	}

	if !ordering.Provides(opt.Ordering{1}) {
		t.Error("ordering should provide the prefix ordering")
	}

	if (opt.Ordering{}).Provides(ordering) {
		t.Error("empty ordering should not provide ordering")
	}

	if !ordering.Provides(opt.Ordering{}) {
		t.Error("ordering should provide the empty ordering")
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

	common := ordering.CommonPrefix(opt.Ordering{1})
	if exp := (opt.Ordering{1}); !reflect.DeepEqual(common, exp) {
		t.Errorf("expected common prefix %s, got %s", exp, common)
	}
	common = ordering.CommonPrefix(opt.Ordering{1, 2, 3})
	if exp := (opt.Ordering{1}); !reflect.DeepEqual(common, exp) {
		t.Errorf("expected common prefix %s, got %s", exp, common)
	}
	common = ordering.CommonPrefix(opt.Ordering{1, 5, 6})
	if exp := (opt.Ordering{1, 5}); !reflect.DeepEqual(common, exp) {
		t.Errorf("expected common prefix %s, got %s", exp, common)
	}
}

func TestOrderingSet(t *testing.T) {
	expect := func(s opt.OrderingSet, exp string) {
		t.Helper()
		if actual := s.String(); actual != exp {
			t.Errorf("expected %s; got %s", exp, actual)
		}
	}
	var s opt.OrderingSet
	expect(s, "")
	s.Add(opt.Ordering{1, 2})
	expect(s, "(+1,+2)")
	s.Add(opt.Ordering{1, -2, 3})
	expect(s, "(+1,+2) (+1,-2,+3)")
	// Add an ordering that already exists.
	s.Add(opt.Ordering{1, -2, 3})
	expect(s, "(+1,+2) (+1,-2,+3)")
	// Add an ordering that is a prefix of an existing ordering.
	s.Add(opt.Ordering{1, -2})
	expect(s, "(+1,+2) (+1,-2,+3)")
	// Add an ordering that has an existing ordering as a prefix.
	s.Add(opt.Ordering{1, 2, 5})
	expect(s, "(+1,+2,+5) (+1,-2,+3)")

	s2 := s.Copy()
	s2.RestrictToPrefix(opt.Ordering{1})
	expect(s2, "(+1,+2,+5) (+1,-2,+3)")
	s2 = s.Copy()
	s2.RestrictToPrefix(opt.Ordering{1, 2})
	expect(s2, "(+1,+2,+5)")
	s2 = s.Copy()
	s2.RestrictToPrefix(opt.Ordering{2})
	expect(s2, "")

	s2 = s.Copy()
	s2.RestrictToCols(opt.MakeColSet(1, 2, 3, 5))
	expect(s2, "(+1,+2,+5) (+1,-2,+3)")

	s2 = s.Copy()
	s2.RestrictToCols(opt.MakeColSet(1, 2, 3))
	expect(s2, "(+1,+2) (+1,-2,+3)")

	s2 = s.Copy()
	s2.RestrictToCols(opt.MakeColSet(1, 2))
	expect(s2, "(+1,+2) (+1,-2)")

	s2 = s.Copy()
	s2.RestrictToCols(opt.MakeColSet(1, 3))
	expect(s2, "(+1)")

	s2 = s.Copy()
	s2.RestrictToCols(opt.MakeColSet(2, 3))
	expect(s2, "")
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

	expected = "+5"
	if remappedCol1.String() != expected {
		t.Errorf("\nexpected: %s\nactual: %s\n", expected, remappedCol1.String())
	}

	expected = "-8"
	if remappedCol2.String() != expected {
		t.Errorf("\nexpected: %s\nactual: %s\n", expected, remappedCol2.String())
	}
}
