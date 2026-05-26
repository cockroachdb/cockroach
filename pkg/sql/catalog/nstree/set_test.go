// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package nstree

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/datadriven"
)

// TestSetDataDriven tests the Set using a data-driven
// exposition format. The tests support the following commands:
//
//	add [parent-id=...] [parent-schema-id=...] name=...
//	  Calls the add method with an entry matching the spec.
//	  Prints the entry.
//
//	contains [parent-id=...] [parent-schema-id=...] name=...
//	  Calls the Remove method on the specified id.
//	  Prints whether it is contained removed.
//
//	clear
//	  Clears the tree.
func TestSetDataDriven(t *testing.T) {
	datadriven.Walk(t, datapathutils.TestDataPath(t, "set"), func(t *testing.T, path string) {
		var tr Set
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			return testSetDataDriven(t, d, &tr)
		})
	})
}

func testSetDataDriven(t *testing.T, d *datadriven.TestData, tr *Set) string {
	switch d.Cmd {
	case "add":
		a := parseArgs(t, d, argName, argParentID|argParentSchemaID)
		entry := makeNameInfo(a)
		tr.Add(entry)
		return formatNameInfo(entry)
	case "contains":
		a := parseArgs(t, d, argName, argParentID|argParentSchemaID)
		return strconv.FormatBool(tr.Contains(makeNameInfo(a)))
	case "clear":
		tr.Clear()
		return ""
	default:
		t.Fatalf("unknown command %q", d.Cmd)
		panic("unreachable")
	}
}

func makeNameInfo(a args) descpb.NameInfo {
	return descpb.NameInfo{
		ParentID:       a.parentID,
		ParentSchemaID: a.parentSchemaID,
		Name:           a.name,
	}
}

func formatNameInfo(ni catalog.NameKey) string {
	return fmt.Sprintf("(%d, %d, %s)",
		ni.GetParentID(), ni.GetParentSchemaID(), ni.GetName())
}
