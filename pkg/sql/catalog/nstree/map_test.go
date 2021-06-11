// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package nstree

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/datadriven"
)

// TestMapDataDriven tests the Map using a data-driven
// exposition format. The tests support the following commands:
//
//   add [parent-id=...] [parent-schema-id=...] name=... id=...
//     Calls the add method with an entry matching the spec.
//     Prints the entry.
//
//   remove id=...
//     Calls the Remove method on the specified id.
//     Prints whether it was removed.
//
//   iterate-by-id [stop-after=<int>]
//     Iterates and prints the entries, ordered by ID.
//     If stop-after is specified, after that many entries have been
//     iterated, then an error will be returned. If there is an input,
//     it will be used as the error message, otherwise, the error will
//     be iterutil.StopIteration.
//
//   clear
//     Clears the tree.
//
//   get-by-id id=...
//     Gets the entry with the given ID and prints its entry.
//     If no such entry exists, "not found" will be printed.
//
//   get-by-name [parent-id=...] [parent-schema-id=...] name=...
//     Gets the entry with the given name and prints its entry.
//     If no such entry exists, "not found" will be printed.
//
func TestMapDataDriven(t *testing.T) {
	datadriven.Walk(t, "testdata/map", func(t *testing.T, path string) {
		tr := MakeMap()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			return testMapDataDriven(t, d, tr)
		})
	})
}

func testMapDataDriven(t *testing.T, d *datadriven.TestData, tr Map) string {
	switch d.Cmd {
	case "add":
		a := parseArgs(t, d, argID|argName, argParentID|argParentSchemaID)
		entry := makeNameEntryFromArgs(a)
		tr.Upsert(entry)
		return formatNameEntry(entry)
	case "get-by-id":
		a := parseArgs(t, d, argID, 0)
		got := tr.GetByID(a.id)
		if got == nil {
			return notFound
		}
		return formatNameEntry(got)
	case "get-by-name":
		a := parseArgs(t, d, argName, argParentID|argParentSchemaID)
		got := tr.GetByName(a.parentID, a.parentSchemaID, a.name)
		if got == nil {
			return notFound
		}
		return formatNameEntry(got)
	case "iterate-by-id":
		a := parseArgs(t, d, 0, argStopAfter)
		var buf strings.Builder
		var i int
		err := tr.IterateByID(func(entry catalog.NameEntry) error {
			defer func() { i++ }()
			if a.set&argStopAfter != 0 && i == a.stopAfter {
				if d.Input != "" {
					return errors.New(d.Input)
				}
				return iterutil.StopIteration()
			}
			buf.WriteString(formatNameEntry(entry))
			buf.WriteString("\n")
			return nil
		})
		if err != nil {
			fmt.Fprintf(&buf, "error: %v", err)
		}
		return buf.String()
	case "len":
		return strconv.Itoa(tr.Len())
	case "clear":
		tr.Clear()
		return ""
	case "remove":
		a := parseArgs(t, d, argID, 0)
		removed := tr.Remove(a.id)
		return strconv.FormatBool(removed != nil)
	default:
		t.Fatalf("unknown command %q", d.Cmd)
		panic("unreachable")
	}
}

type nameEntry struct {
	descpb.NameInfo
	id descpb.ID
}

func (ne nameEntry) GetID() descpb.ID { return ne.id }

func makeNameEntryFromArgs(a args) nameEntry {
	ne := nameEntry{}
	ne.ParentID = a.parentID
	ne.ParentSchemaID = a.parentSchemaID
	ne.Name = a.name
	ne.id = a.id
	return ne
}

func formatNameEntry(ne catalog.NameEntry) string {
	return fmt.Sprintf("%s: %d", formatNameInfo(ne), ne.GetID())
}
