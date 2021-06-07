// Copyright 2016 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package backupresolver

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestDescriptorsMatchingTargets(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// TODO(ajwerner): There should be a constructor for an immutable
	// and really all of the leasable descriptor types which includes its initial
	// DescriptorMeta. This refactoring precedes the actual adoption of
	// DescriptorMeta.
	var descriptors []catalog.Descriptor
	{
		// Make shorthand type names for syntactic sugar.
		type scDesc = descpb.SchemaDescriptor
		type tbDesc = descpb.TableDescriptor
		type typDesc = descpb.TypeDescriptor
		ts1 := hlc.Timestamp{WallTime: 1}
		mkTable := func(descriptor tbDesc) catalog.Descriptor {
			descProto := tabledesc.NewBuilder(&descriptor).BuildImmutable().DescriptorProto()
			return catalogkv.NewBuilderWithMVCCTimestamp(descProto, ts1).BuildImmutable()
		}
		mkDB := func(id descpb.ID, name string) catalog.Descriptor {
			return dbdesc.NewInitial(id, name, security.AdminRoleName())
		}
		mkTyp := func(desc typDesc) catalog.Descriptor {
			// Set a default parent schema for the type descriptors.
			if desc.ParentSchemaID == descpb.InvalidID {
				desc.ParentSchemaID = keys.PublicSchemaID
			}
			return typedesc.NewBuilder(&desc).BuildImmutable()
		}
		mkSchema := func(desc scDesc) catalog.Descriptor {
			return schemadesc.NewBuilder(&desc).BuildImmutable()
		}
		toOid := typedesc.TypeIDToOID
		typeExpr := "'hello'::@100015 = 'hello'::@100015"
		typeArrExpr := "'hello'::@100016 = 'hello'::@100016"
		descriptors = []catalog.Descriptor{
			mkDB(0, "system"),
			mkTable(tbDesc{ID: 1, Name: "foo", ParentID: 0}),
			mkTable(tbDesc{ID: 2, Name: "bar", ParentID: 0}),
			mkTable(tbDesc{ID: 4, Name: "baz", ParentID: 3}),
			mkTable(tbDesc{ID: 6, Name: "offline", ParentID: 0, State: descpb.DescriptorState_OFFLINE}),
			mkDB(3, "data"),
			mkDB(5, "empty"),
			// Create some user defined types and tables that reference them.
			mkDB(7, "udts"),
			// Type descriptors represent different kinds of types. ENUM means
			// that the type descriptor references an enum type. ALIAS means that
			// the descriptor is a type alias for an existing type. ALIAS is only
			// used for managing the implicit array type for each user defined type.
			// Every user defined type also has an ALIAS type that represents an
			// array of the user defined type, and that is tracked by the ArrayTypeID
			// field on the type descriptor.
			mkTyp(descpb.TypeDescriptor{ParentID: 7, ID: 8, Name: "enum1", ArrayTypeID: 9, Kind: descpb.TypeDescriptor_ENUM}),
			mkTyp(descpb.TypeDescriptor{ParentID: 7, ID: 9, Name: "_enum1", Kind: descpb.TypeDescriptor_ALIAS, Alias: types.MakeEnum(toOid(8), toOid(9))}),
			mkTable(descpb.TableDescriptor{ParentID: 7, ID: 10, Name: "enum_tbl", Columns: []descpb.ColumnDescriptor{{ID: 0, Type: types.MakeEnum(toOid(8), toOid(9))}}}),
			mkTable(descpb.TableDescriptor{ParentID: 7, ID: 11, Name: "enum_arr_tbl", Columns: []descpb.ColumnDescriptor{{ID: 0, Type: types.MakeArray(types.MakeEnum(toOid(8), toOid(9)))}}}),
			mkTyp(descpb.TypeDescriptor{ParentID: 7, ID: 12, Name: "enum2", ArrayTypeID: 13, Kind: descpb.TypeDescriptor_ENUM}),
			mkTyp(descpb.TypeDescriptor{ParentID: 7, ID: 13, Name: "_enum2", Kind: descpb.TypeDescriptor_ALIAS, Alias: types.MakeEnum(toOid(12), toOid(13))}),
			// Create some user defined types that are used in table expressions.
			mkDB(14, "udts_expr"),
			mkTyp(descpb.TypeDescriptor{ParentID: 14, ID: 15, Name: "enum1", ArrayTypeID: 16, Kind: descpb.TypeDescriptor_ENUM}),
			mkTyp(descpb.TypeDescriptor{ParentID: 14, ID: 16, Name: "_enum1", Kind: descpb.TypeDescriptor_ALIAS, Alias: types.MakeEnum(toOid(15), toOid(16))}),
			// Create a table with a default expression.
			mkTable(tbDesc{
				ID:       17,
				Name:     "def",
				ParentID: 14,
				Columns: []descpb.ColumnDescriptor{
					{
						Name:        "a",
						DefaultExpr: &typeExpr,
						Type:        types.Bool,
					},
				},
			}),
			// Create a table with a computed column.
			mkTable(tbDesc{
				ID:       18,
				Name:     "comp",
				ParentID: 14,
				Columns: []descpb.ColumnDescriptor{
					{
						Name:        "a",
						DefaultExpr: &typeExpr,
						Type:        types.Bool,
					},
				},
			}),
			// Create a table with a partial index.
			mkTable(tbDesc{
				ID:       19,
				Name:     "pi",
				ParentID: 14,
				Indexes: []descpb.IndexDescriptor{
					{
						Name:      "idx",
						Predicate: typeExpr,
					},
				},
			}),
			// Create a table with a check expression.
			mkTable(tbDesc{
				ID:       20,
				Name:     "checks",
				ParentID: 14,
				Checks: []*descpb.TableDescriptor_CheckConstraint{
					{
						Expr: typeExpr,
					},
				},
			}),
			mkTable(tbDesc{
				ID:       21,
				Name:     "def_arr",
				ParentID: 14,
				Columns: []descpb.ColumnDescriptor{
					{
						Name:        "a",
						DefaultExpr: &typeArrExpr,
						Type:        types.Bool,
					},
				},
			}),
			mkDB(22, "uds"),
			mkSchema(scDesc{ParentID: 22, ID: 23, Name: "sc"}),
			mkTable(tbDesc{ParentID: 22, UnexposedParentSchemaID: 23, ID: 24, Name: "tb1"}),
		}
	}

	tests := []struct {
		sessionDatabase string
		pattern         string
		expected        []string
		expectedDBs     []string
		err             string
	}{
		{"", "DATABASE system", []string{"system", "foo", "bar"}, []string{"system"}, ``},
		{"", "DATABASE system, noexist", nil, nil, `database "noexist" does not exist`},
		{"", "DATABASE system, system", []string{"system", "foo", "bar"}, []string{"system"}, ``},
		{"", "DATABASE data", []string{"data", "baz"}, []string{"data"}, ``},
		{"", "DATABASE system, data", []string{"system", "foo", "bar", "data", "baz"}, []string{"data", "system"}, ``},
		{"", "DATABASE system, data, noexist", nil, nil, `database "noexist" does not exist`},
		{"system", "DATABASE system", []string{"system", "foo", "bar"}, []string{"system"}, ``},
		{"system", "DATABASE system, noexist", nil, nil, `database "noexist" does not exist`},
		{"system", "DATABASE data", []string{"data", "baz"}, []string{"data"}, ``},
		{"system", "DATABASE system, data", []string{"system", "foo", "bar", "data", "baz"}, []string{"data", "system"}, ``},
		{"system", "DATABASE system, data, noexist", nil, nil, `database "noexist" does not exist`},

		{"", "TABLE foo", nil, nil, `table "foo" does not exist`},
		{"system", "TABLE foo", []string{"system", "foo"}, nil, ``},
		{"system", "TABLE foo, foo", []string{"system", "foo"}, nil, ``},
		{"data", "TABLE foo", nil, nil, `table "foo" does not exist`},

		{"", "TABLE *", nil, nil, `"\*" does not match any valid database or schema`},
		{"", "TABLE *, system.public.foo", nil, nil, `"\*" does not match any valid database or schema`},
		{"noexist", "TABLE *", nil, nil, `"\*" does not match any valid database or schema`},
		{"system", "TABLE *", []string{"system", "foo", "bar"}, nil, ``},
		{"data", "TABLE *", []string{"data", "baz"}, nil, ``},
		{"empty", "TABLE *", []string{"empty"}, nil, ``},

		{"", "TABLE foo, baz", nil, nil, `table "(foo|baz)" does not exist`},
		{"system", "TABLE foo, baz", nil, nil, `table "baz" does not exist`},
		{"data", "TABLE foo, baz", nil, nil, `table "foo" does not exist`},

		{"", "TABLE system.foo", []string{"system", "foo"}, nil, ``},
		{"", "TABLE system.foo, foo", []string{"system", "foo"}, nil, `table "foo" does not exist`},
		{"", "TABLE system.public.foo", []string{"system", "foo"}, nil, ``},
		{"", "TABLE system.public.foo, foo", []string{"system", "foo"}, nil, `table "foo" does not exist`},

		{"", "TABLE system.public.foo, bar", []string{"system", "foo"}, nil, `table "bar" does not exist`},
		{"", "TABLE system.foo, bar", []string{"system", "foo"}, nil, `table "bar" does not exist`},
		{"system", "TABLE system.public.foo, bar", []string{"system", "foo", "bar"}, nil, ``},
		{"system", "TABLE system.foo, bar", []string{"system", "foo", "bar"}, nil, ``},

		{"", "TABLE noexist.*", nil, nil, `"noexist\.\*" does not match any valid database or schema`},
		{"", "TABLE empty.*", []string{"empty"}, nil, ``},
		{"", "TABLE system.*", []string{"system", "foo", "bar"}, nil, ``},
		{"", "TABLE system.public.*", []string{"system", "foo", "bar"}, nil, ``},
		{"", "TABLE system.public.*, foo, baz", nil, nil, `table "(foo|baz)" does not exist`},
		{"system", "TABLE system.public.*, foo, baz", nil, nil, `table "baz" does not exist`},
		{"data", "TABLE system.public.*, baz", []string{"system", "foo", "bar", "data", "baz"}, nil, ``},
		{"data", "TABLE system.public.*, foo, baz", nil, nil, `table "(foo|baz)" does not exist`},

		{"", "TABLE SyStEm.FoO", []string{"system", "foo"}, nil, ``},
		{"", "TABLE SyStEm.pUbLic.FoO", []string{"system", "foo"}, nil, ``},
		{"", `TABLE system."FoO"`, nil, nil, `table "system.FoO" does not exist`},
		{"system", `TABLE "FoO"`, nil, nil, `table "FoO" does not exist`},

		{"", `TABLE system."foo"`, []string{"system", "foo"}, nil, ``},
		{"", `TABLE system.public."foo"`, []string{"system", "foo"}, nil, ``},
		{"system", `TABLE "foo"`, []string{"system", "foo"}, nil, ``},

		{"system", `TABLE offline`, nil, nil, `table "offline" does not exist`},
		{"", `TABLE system.offline`, []string{"system", "foo"}, nil, `table "system.public.offline" does not exist`},
		{"system", `TABLE *`, []string{"system", "foo", "bar"}, nil, ``},
		// If we backup udts, then all tables and types (even unused) should be present.
		{"", "DATABASE udts", []string{"udts", "enum1", "_enum1", "enum2", "_enum2", "enum_tbl", "enum_arr_tbl"}, []string{"udts"}, ``},
		// Backing up enum_tbl should pull in both the enum and its array type.
		{"", "TABLE udts.enum_tbl", []string{"udts", "enum1", "_enum1", "enum_tbl"}, nil, ``},
		// Backing up enum_arr_tbl should also pull in both the enum and its array type.
		{"", "TABLE udts.enum_arr_tbl", []string{"udts", "enum1", "_enum1", "enum_arr_tbl"}, nil, ``},
		// Test collecting expressions that are present in table expressions.
		{"", "TABLE udts_expr.def", []string{"udts_expr", "enum1", "_enum1", "def"}, nil, ``},
		{"", "TABLE udts_expr.def_arr", []string{"udts_expr", "enum1", "_enum1", "def_arr"}, nil, ``},
		{"", "TABLE udts_expr.comp", []string{"udts_expr", "enum1", "_enum1", "comp"}, nil, ``},
		{"", "TABLE udts_expr.pi", []string{"udts_expr", "enum1", "_enum1", "pi"}, nil, ``},
		{"", "TABLE udts_expr.checks", []string{"udts_expr", "enum1", "_enum1", "checks"}, nil, ``},
		// Test that the user defined schema shows up in the descriptors.
		{"", "DATABASE uds", []string{"uds", "sc", "tb1"}, []string{"uds"}, ``},
		{"", "TABLE uds.sc.tb1", []string{"uds", "sc", "tb1"}, nil, ``},
	}
	searchPath := sessiondata.MakeSearchPath([]string{"public", "pg_catalog"})
	for i, test := range tests {
		t.Run(fmt.Sprintf("%d/%s/%s", i, test.sessionDatabase, test.pattern), func(t *testing.T) {
			sql := fmt.Sprintf(`GRANT ALL ON %s TO ignored`, test.pattern)
			stmt, err := parser.ParseOne(sql)
			if err != nil {
				t.Fatal(err)
			}
			targets := stmt.AST.(*tree.Grant).Targets

			matched, err := DescriptorsMatchingTargets(context.Background(),
				test.sessionDatabase, searchPath, descriptors, targets, hlc.Timestamp{})
			if test.err != "" {
				if !testutils.IsError(err, test.err) {
					t.Fatalf("expected error matching '%v', but got '%v'", test.err, err)
				}
			} else if err != nil {
				t.Fatal(err)
			} else {
				var matchedNames []string
				for _, m := range matched.Descs {
					matchedNames = append(matchedNames, m.GetName())
				}
				var matchedDBNames []string
				for _, m := range matched.RequestedDBs {
					matchedDBNames = append(matchedDBNames, m.GetName())
				}
				sort.Strings(test.expected)
				sort.Strings(test.expectedDBs)
				sort.Strings(matchedNames)
				sort.Strings(matchedDBNames)
				if !reflect.DeepEqual(test.expected, matchedNames) {
					t.Fatalf("expected %q got %q", test.expected, matchedNames)
				}
				if !reflect.DeepEqual(test.expectedDBs, matchedDBNames) {
					t.Fatalf("expected %q got %q", test.expectedDBs, matchedDBNames)
				}
			}
		})
	}
}
