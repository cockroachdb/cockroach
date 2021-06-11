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

type nameEntry struct {
	descpb.NameInfo
	id descpb.ID
}

func (ne nameEntry) GetID() descpb.ID { return ne.id }

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
	type argType int
	const (
		argParentID = 1 << iota
		argParentSchemaID
		argID
		argName
		argStopAfter
	)
	type args struct {
		set                          argType
		parentID, parentSchemaID, id descpb.ID
		name                         string
		stopAfter                    int
	}
	type setFunc func(t *testing.T, d *datadriven.TestData, key string, a *args) bool
	setDescIDFunc := func(f func(a *args) *descpb.ID) setFunc {
		return func(t *testing.T, d *datadriven.TestData, key string, a *args) bool {
			t.Helper()
			var id int
			d.ScanArgs(t, key, &id)
			*f(a) = descpb.ID(id)
			return true
		}
	}
	setStringFunc := func(f func(a *args) *string) setFunc {
		return func(t *testing.T, d *datadriven.TestData, key string, a *args) bool {
			t.Helper()
			d.ScanArgs(t, key, f(a))
			return true
		}
	}
	setIntFunc := func(f func(a *args) *int) setFunc {
		return func(t *testing.T, d *datadriven.TestData, key string, a *args) bool {
			t.Helper()
			d.ScanArgs(t, key, f(a))
			return true
		}
	}

	argParser := map[argType]struct {
		key string
		sf  setFunc
	}{
		argParentID: {
			"parent-id",
			setDescIDFunc(func(a *args) *descpb.ID { return &a.parentID }),
		},
		argParentSchemaID: {
			"parent-schema-id",
			setDescIDFunc(func(a *args) *descpb.ID { return &a.parentSchemaID }),
		},
		argID: {
			"id",
			setDescIDFunc(func(a *args) *descpb.ID { return &a.id }),
		},
		argName: {
			"name",
			setStringFunc(func(a *args) *string { return &a.name }),
		},
		argStopAfter: {
			"stop-after",
			setIntFunc(func(a *args) *int { return &a.stopAfter }),
		},
	}
	parseArgs := func(t *testing.T, d *datadriven.TestData, required, allowed argType) args {
		allowed = allowed | required // all required are allowed
		var a args
		for at, p := range argParser {
			if ok := d.HasArg(p.key); ok {
				if allowed&at == 0 {
					d.Fatalf(t, "%s: illegal argument %s", d.Cmd, p.key)
				}
				p.sf(t, d, p.key, &a)
				a.set |= at
			} else if required&at != 0 {
				d.Fatalf(t, "%s: missing required argument %s", d.Cmd, p.key)
			}
		}
		return a
	}
	constructDescriptorFromArgs := func(t *testing.T, a args) catalog.NameEntry {
		ne := nameEntry{}
		ne.ParentID = a.parentID
		ne.ParentSchemaID = a.parentSchemaID
		ne.Name = a.name
		ne.id = a.id
		return ne
	}
	formatEntry := func(d catalog.NameEntry) string {
		return fmt.Sprintf("(%d, %d, %s): %d",
			d.GetParentID(), d.GetParentSchemaID(), d.GetName(), d.GetID())
	}
	datadriven.Walk(t, "testdata/desc_tree", func(t *testing.T, path string) {
		tr := MakeMap()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			const notFound = "not found"
			switch d.Cmd {
			case "add":
				a := parseArgs(t, d, argID|argName, argParentID|argParentSchemaID)
				desc := constructDescriptorFromArgs(t, a)
				tr.Upsert(desc)
				return formatEntry(desc)
			case "get-by-id":
				a := parseArgs(t, d, argID, 0)
				got := tr.GetByID(a.id)
				if got == nil {
					return notFound
				}
				return formatEntry(got.(catalog.NameEntry))
			case "get-by-name":
				a := parseArgs(t, d, argName, argParentID|argParentSchemaID)
				got := tr.GetByName(a.parentID, a.parentSchemaID, a.name)
				if got == nil {
					return notFound
				}
				return formatEntry(got.(catalog.NameEntry))
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
					buf.WriteString(formatEntry(entry))
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
				_, removed := tr.Remove(a.id)
				return strconv.FormatBool(removed)
			default:
				t.Fatalf("unknown command %q", d.Cmd)
				panic("unreachable")
			}
		})
	})
}
