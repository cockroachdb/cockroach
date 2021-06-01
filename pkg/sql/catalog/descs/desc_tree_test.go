// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descs

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/datadriven"
)

// TestDescTreeDataDriven tests the descriptorTree using a data-driven
// exposition format. The tests support the following commands:
//
//   add [parent-id=...] [parent-schema-id=...] name=... id=... owner=...
//     Calls the add method with a descriptor matching the spec
//     Prints the entry
//
//   remove id=...
//     Calls the remove method on the specified id.
//     Prints whether it was removed.
//
//   iterate-by-id [stop-after=<int>]
//     Iterates and prints the descriptors, ordered by ID.
//     If stop-after is specified, after that many descriptors have been
//     iterated, then an error will be returned. If there is an input,
//     it will be used as the error message, otherwise, the error will
//     be iterutil.StopIteration.
//
//   clear
//     Clears the tree.
//
//   get-by-id id=...
//     Gets the descriptor with the given ID and prints its entry.
//     If no such descriptor exists, "not found" will be printed.
//
//   get-by-name [parent-id=...] [parent-schema-id=...] name=...
//     Gets the descriptor with the given name and prints its entry.
//     If no such descriptor exists, "not found" will be printed.
//
func TestDescTreeDataDriven(t *testing.T) {
	type argType int
	const (
		argParentID = 1 << iota
		argParentSchemaID
		argID
		argName
		argOwner
		argStopAfter
	)
	type args struct {
		set                          argType
		parentID, parentSchemaID, id descpb.ID
		name, owner                  string
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
		argOwner: {
			"owner",
			setStringFunc(func(a *args) *string { return &a.owner }),
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
	constructDescriptorFromArgs := func(t *testing.T, a args) catalog.Descriptor {
		owner := security.MakeSQLUsernameFromPreNormalizedString(a.owner)
		switch {
		case a.parentID != descpb.InvalidID &&
			a.parentSchemaID != descpb.InvalidID:
			// Object case, create a table.
			return tabledesc.NewBuilder(&descpb.TableDescriptor{
				Name:                    a.name,
				ID:                      a.id,
				ParentID:                a.parentID,
				UnexposedParentSchemaID: a.parentSchemaID,
				Privileges:              descpb.NewPrivilegeDescriptor(owner, privilege.TablePrivileges, owner),
			}).BuildCreatedMutable()
		case a.parentID != descpb.InvalidID:
			// Schema case, create a schema.
			return schemadesc.NewBuilder(&descpb.SchemaDescriptor{
				Name:       a.name,
				ID:         a.id,
				Version:    1,
				ParentID:   a.parentID,
				Privileges: descpb.NewPrivilegeDescriptor(owner, privilege.SchemaPrivileges, owner),
			}).BuildCreatedMutable()
		default:
			return dbdesc.NewInitial(a.id, a.name, owner)
		}
	}
	formatDesc := func(d catalog.Descriptor) string {
		return fmt.Sprintf("(%d, %d, %s): %d(%s)",
			d.GetParentID(), d.GetParentSchemaID(), d.GetName(), d.GetID(), d.GetPrivileges().Owner())
	}
	datadriven.Walk(t, "testdata/desc_tree", func(t *testing.T, path string) {
		tr := makeDescriptorTree()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			const notFound = "not found"
			switch d.Cmd {
			case "add":
				a := parseArgs(t, d, argID|argName|argOwner, argParentID|argParentSchemaID)
				desc := constructDescriptorFromArgs(t, a)
				tr.add(desc)
				return formatDesc(desc)
			case "get-by-id":
				a := parseArgs(t, d, argID, 0)
				got, ok := tr.getByID(a.id)
				if !ok {
					return notFound
				}
				return formatDesc(got)
			case "get-by-name":
				a := parseArgs(t, d, argName, argParentID|argParentSchemaID)
				got, ok := tr.getByName(a.parentID, a.parentSchemaID, a.name)
				if !ok {
					return notFound
				}
				return formatDesc(got)
			case "iterate-by-id":
				a := parseArgs(t, d, 0, argStopAfter)
				var buf strings.Builder
				var i int
				err := tr.iterateByID(func(descriptor catalog.Descriptor) error {
					defer func() { i++ }()
					if a.set&argStopAfter != 0 && i == a.stopAfter {
						if d.Input != "" {
							return errors.New(d.Input)
						} else {
							return iterutil.StopIteration()
						}
					}
					buf.WriteString(formatDesc(descriptor))
					buf.WriteString("\n")
					return nil
				})
				if err != nil {
					fmt.Fprintf(&buf, "error: %v", err)
				}
				return buf.String()
			case "len":
				return strconv.Itoa(tr.len())
			case "clear":
				tr.clear()
				return ""
			case "remove":
				a := parseArgs(t, d, argID, 0)
				return strconv.FormatBool(tr.remove(a.id))
			default:
				t.Fatalf("unknown command %q", d.Cmd)
				panic("unreachable")
			}
		})
	})
}
