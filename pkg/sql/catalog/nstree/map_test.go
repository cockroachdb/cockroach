// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package nstree

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/datadriven"
)

// TestNameMapDataDriven tests the NameMap using a data-driven
// exposition format. The tests support the following commands:
//
//	add [parent-id=...] [parent-schema-id=...] name=... id=...
//	  Calls the add method with an entry matching the spec.
//	  Prints the entry.
//
//	remove id=...
//	  Calls the Remove method on the specified id.
//	  Prints whether it was removed.
//
//	iterate-by-id [stop-after=<int>]
//	  Iterates and prints the entries, ordered by ID.
//	  If stop-after is specified, after that many entries have been
//	  iterated, then an error will be returned. If there is an input,
//	  it will be used as the error message, otherwise, the error will
//	  be iterutil.StopIteration.
//
//	clear
//	  Clears the tree.
//
//	get-by-id id=...
//	  Gets the entry with the given ID and prints its entry.
//	  If no such entry exists, "not found" will be printed.
func TestNameMapDataDriven(t *testing.T) {
	datadriven.Walk(t, datapathutils.TestDataPath(t, "name_map"), func(t *testing.T, path string) {
		var nm NameMap
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			return testMapDataDriven(t, d, nil /* im */, &nm)
		})
	})
}

// TestIDMapDataDriven is like TestNameMapDataDriven but for IDMap.
func TestIDMapDataDriven(t *testing.T) {
	datadriven.Walk(t, datapathutils.TestDataPath(t, "id_map"), func(t *testing.T, path string) {
		var im IDMap
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			return testMapDataDriven(t, d, &im, nil /* nm */)
		})
	})
}

func testMapDataDriven(t *testing.T, d *datadriven.TestData, im *IDMap, nm *NameMap) string {
	switch d.Cmd {
	case "add":
		a := parseArgs(t, d, argID|argName, argParentID|argParentSchemaID)
		entry := makeNameEntryFromArgs(a)
		if im != nil {
			im.Upsert(entry)
		} else {
			nm.Upsert(entry, false)
		}
		return formatNameEntry(entry)
	case "add-without-name":
		a := parseArgs(t, d, argID|argName, argParentID|argParentSchemaID)
		entry := makeNameEntryFromArgs(a)
		if im != nil {
			return fmt.Sprintf("error: %s not valid for IDMap", d.Cmd)
		}
		nm.Upsert(entry, true)
		return formatNameEntry(entry)
	case "get-by-id":
		a := parseArgs(t, d, argID, 0)
		var got catalog.NameEntry
		if im != nil {
			got = im.Get(a.id)
		} else {
			got = nm.GetByID(a.id)
		}
		if got == nil {
			return notFound
		}
		return formatNameEntry(got)
	case "get-by-name":
		a := parseArgs(t, d, argName, argParentID|argParentSchemaID)
		if im != nil {
			return fmt.Sprintf("error: %s not valid for IDMap", d.Cmd)
		}
		got := nm.GetByName(a.parentID, a.parentSchemaID, a.name)
		if got == nil {
			return notFound
		}
		return formatNameEntry(got)
	case "iterate-by-id":
		a := parseArgs(t, d, 0, argStopAfter)
		var buf strings.Builder
		var i int
		iterator := func(entry catalog.NameEntry) error {
			defer func() { i++ }()
			if a.set&argStopAfter != 0 && i == a.stopAfter {
				if d.Input != "" {
					return fmt.Errorf("error: %s", d.Input)
				}
				return iterutil.StopIteration()
			}
			buf.WriteString(formatNameEntry(entry))
			buf.WriteString("\n")
			return nil
		}
		var err error
		if im != nil {
			err = im.Iterate(iterator)
		} else {
			err = nm.IterateByID(iterator)
		}
		if err != nil {
			fmt.Fprintf(&buf, "%v", err)
		}
		return buf.String()
	case "iterate-by-name":
		a := parseArgs(t, d, 0, argStopAfter)
		if im != nil {
			return fmt.Sprintf("error: %s not valid for IDMap", d.Cmd)
		}
		var buf strings.Builder
		var i int
		err := nm.iterateByName(func(entry catalog.NameEntry) error {
			defer func() { i++ }()
			if a.set&argStopAfter != 0 && i == a.stopAfter {
				if d.Input != "" {
					return fmt.Errorf("error: %s", d.Input)
				}
				return iterutil.StopIteration()
			}
			buf.WriteString(formatNameEntry(entry))
			buf.WriteString("\n")
			return nil
		})
		if err != nil {
			fmt.Fprintf(&buf, "%v", err)
		}
		return buf.String()
	case "len":
		var n int
		if im != nil {
			n = im.Len()
		} else {
			n = nm.Len()
		}
		return strconv.Itoa(n)
	case "clear":
		if im != nil {
			im.Clear()
		} else {
			nm.Clear()
		}
		return ""
	case "remove":
		a := parseArgs(t, d, argID, 0)
		var removed catalog.NameEntry
		if im != nil {
			removed = im.Remove(a.id)
		} else {
			removed = nm.Remove(a.id)
		}
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
