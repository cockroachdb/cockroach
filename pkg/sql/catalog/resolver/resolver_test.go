// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package resolver_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/resolver"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// fakeMetadata represents a fake table resolution environment for tests.
type fakeMetadata struct {
	t             *testing.T
	knownVSchemas []knownSchema
	knownCatalogs []knownCatalog
}

type knownSchema struct {
	scName tree.Name
	tables []tree.Name
}

type knownCatalog struct {
	ctName  tree.Name
	schemas []knownSchema
}

func makeResolvedObjectPrefix(dbName, scName string) catalog.ResolvedObjectPrefix {
	return catalog.ResolvedObjectPrefix{
		Database: dbdesc.NewBuilder(&descpb.DatabaseDescriptor{Name: dbName}).
			BuildImmutableDatabase(),
		Schema: schemadesc.NewBuilder(&descpb.SchemaDescriptor{Name: scName}).
			BuildImmutableSchema(),
	}
}

// LookupSchema implements the TableNameResolver interface.
func (f *fakeMetadata) LookupSchema(
	ctx context.Context, dbName, scName string,
) (found bool, scMeta catalog.ResolvedObjectPrefix, err error) {
	defer func() {
		f.t.Logf("LookupSchema(%s, %s) -> found %v meta %v err %v",
			dbName, scName, found, scMeta, err)
	}()
	if scName == catconstants.PgTempSchemaName {
		scName = "pg_temp_123"
	}
	for i := range f.knownVSchemas {
		v := &f.knownVSchemas[i]
		if scName == string(v.scName) {
			// Virtual schema found, check that the db exists.
			// The empty database is valid.
			if dbName == "" {
				return true, makeResolvedObjectPrefix(dbName, scName), nil
			}
			for j := range f.knownCatalogs {
				c := &f.knownCatalogs[j]
				if dbName == string(c.ctName) {
					return true, makeResolvedObjectPrefix(dbName, scName), nil
				}
			}
			// No valid database, schema is invalid.
			return false, catalog.ResolvedObjectPrefix{}, nil
		}
	}
	for i := range f.knownCatalogs {
		c := &f.knownCatalogs[i]
		if dbName == string(c.ctName) {
			for j := range c.schemas {
				s := &c.schemas[j]
				if scName == string(s.scName) {
					return true, makeResolvedObjectPrefix(dbName, scName), nil
				}
			}
			break
		}
	}
	return false, catalog.ResolvedObjectPrefix{}, nil
}

// LookupObject implements the TableNameResolver interface.
func (f *fakeMetadata) LookupObject(
	ctx context.Context, flags tree.ObjectLookupFlags, dbName, scName, obName string,
) (found bool, prefix catalog.ResolvedObjectPrefix, objMeta catalog.Descriptor, err error) {
	defer func() {
		f.t.Logf("LookupObject(%s, %s, %s) -> found %v prefix %v meta %v err %v",
			dbName, scName, obName, found, prefix, objMeta, err)
	}()
	if scName == catconstants.PgTempSchemaName {
		scName = "pg_temp_123"
	}
	foundV := false
	for i := range f.knownVSchemas {
		v := &f.knownVSchemas[i]
		if scName == string(v.scName) {
			// Virtual schema found, check that the db exists.
			// The empty database is valid.
			if dbName != "" {
				hasDb := false
				for j := range f.knownCatalogs {
					c := &f.knownCatalogs[j]
					if dbName == string(c.ctName) {
						hasDb = true
						break
					}
				}
				if !hasDb {
					return false, prefix, nil, nil
				}
			}
			// Db valid, check the table name.
			for tbIdx, tb := range v.tables {
				if obName == string(tb) {
					return true, makeResolvedObjectPrefix(dbName, scName), makeFakeDescriptor(tbIdx), nil
				}
			}
			foundV = true
			break
		}
	}
	if foundV {
		// Virtual schema matched, but there was no table. Fail.
		return false, prefix, nil, nil
	}

	for i := range f.knownCatalogs {
		c := &f.knownCatalogs[i]
		if dbName == string(c.ctName) {
			for j := range c.schemas {
				s := &c.schemas[j]
				if scName == string(s.scName) {
					for tbIdx, tb := range s.tables {
						if obName == string(tb) {
							return true, makeResolvedObjectPrefix(dbName, scName), makeFakeDescriptor(tbIdx), nil
						}
					}
					break
				}
			}
			break
		}
	}
	return false, prefix, nil, nil
}

// makeFakeDescriptor makes an empty table descriptor with the given ID.
// Only the ID is accessed during testing.
func makeFakeDescriptor(tbIdx int) catalog.Descriptor {
	return tabledesc.NewBuilder(&descpb.TableDescriptor{ID: descpb.ID(tbIdx)}).BuildImmutable()
}

func newFakeMetadata() *fakeMetadata {
	return &fakeMetadata{
		knownVSchemas: []knownSchema{
			{"pg_catalog", []tree.Name{"pg_tables"}},
		},
		knownCatalogs: []knownCatalog{
			{"db1", []knownSchema{{"public", []tree.Name{"foo", "kv"}}}},
			{"db2", []knownSchema{
				{"public", []tree.Name{"foo"}},
				{"extended", []tree.Name{"bar", "pg_tables"}},
			}},
			{"db3", []knownSchema{
				{"public", []tree.Name{"foo", "bar"}},
				{"pg_temp_123", []tree.Name{"foo", "baz"}},
			}},
			{"system", []knownSchema{{"public", []tree.Name{"users"}}}},
		},
	}
}

func TestResolveTablePatternOrName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	type spath = sessiondata.SearchPath

	var mpath = func(args ...string) spath {
		return sessiondata.MakeSearchPath(args)
	}

	var tpath = func(tempSchemaName string, args ...string) spath {
		return sessiondata.MakeSearchPath(args).WithTemporarySchemaName(tempSchemaName)
	}

	testCases := []struct {
		// Test inputs.
		in         string // The table name or pattern.
		curDb      string // The current database.
		searchPath spath  // The current search path.
		expected   bool   // If non-star, whether the object is expected to exist already.
		// Expected outputs.
		out      string // The prefix after resolution.
		expanded string // The prefix after resolution, with hidden fields revealed.
		scName   string // The schema name after resolution.
		err      string // Error, if expected.
	}{
		//
		// Tests for table names.
		//

		// Names of length 1.

		{`kv`, `db1`, mpath("public", "pg_catalog"), true, `kv`, `db1.public.kv`, `db1.public[1]`, ``},
		{`foo`, `db1`, mpath("public", "pg_catalog"), true, `foo`, `db1.public.foo`, `db1.public[0]`, ``},
		{`blix`, `db1`, mpath("public", "pg_catalog"), true, ``, ``, ``, `prefix or object not found`},
		{`pg_tables`, `db1`, mpath("public", "pg_catalog"), true, `pg_tables`, `db1.pg_catalog.pg_tables`, `db1.pg_catalog[0]`, ``},

		{`blix`, `db1`, mpath("public", "pg_catalog"), false, `blix`, `db1.public.blix`, `db1.public`, ``},

		// A valid table is invisible if "public" is not in the search path.
		{`kv`, `db1`, mpath(), true, ``, ``, ``, `prefix or object not found`},

		// But pg_catalog is magic and "always there".
		{`pg_tables`, `db1`, mpath(), true, `pg_tables`, `db1.pg_catalog.pg_tables`, `db1.pg_catalog[0]`, ``},
		{`blix`, `db1`, mpath(), false, ``, ``, ``, `prefix or object not found`},

		// If there's a table with the same name as a pg_catalog table, then search path order matters.
		{`pg_tables`, `db2`, mpath("extended", "pg_catalog"), true, `pg_tables`, `db2.extended.pg_tables`, `db2.extended[1]`, ``},
		{`pg_tables`, `db2`, mpath("pg_catalog", "extended"), true, `pg_tables`, `db2.pg_catalog.pg_tables`, `db2.pg_catalog[0]`, ``},
		// When pg_catalog is not explicitly mentioned in the search path, it is searched first.
		{`pg_tables`, `db2`, mpath("foo"), true, `pg_tables`, `db2.pg_catalog.pg_tables`, `db2.pg_catalog[0]`, ``},

		// Names of length 2.

		{`public.kv`, `db1`, mpath("public", "pg_catalog"), true, `public.kv`, `db1.public.kv`, `db1.public[1]`, ``},
		{`public.foo`, `db1`, mpath("public", "pg_catalog"), true, `public.foo`, `db1.public.foo`, `db1.public[0]`, ``},
		{`public.blix`, `db1`, mpath("public", "pg_catalog"), true, ``, ``, ``, `prefix or object not found`},
		{`public.pg_tables`, `db1`, mpath("public", "pg_catalog"), true, ``, ``, ``, `prefix or object not found`},
		{`extended.pg_tables`, `db2`, mpath("public", "pg_catalog"), true, `extended.pg_tables`, `db2.extended.pg_tables`, `db2.extended[1]`, ``},
		{`pg_catalog.pg_tables`, `db1`, mpath("public", "pg_catalog"), true, `pg_catalog.pg_tables`, `db1.pg_catalog.pg_tables`, `db1.pg_catalog[0]`, ``},

		{`public.blix`, `db1`, mpath("public", "pg_catalog"), false, `public.blix`, `db1.public.blix`, `db1.public`, ``},

		// Compat with CockroachDB v1.x.
		{`db1.kv`, `db1`, mpath("public", "pg_catalog"), true, `db1.public.kv`, `db1.public.kv`, `db1.public[1]`, ``},

		{`blix.foo`, `db1`, mpath("public", "pg_catalog"), true, ``, ``, ``, `prefix or object not found`},
		{`blix.pg_tables`, `db1`, mpath("public", "pg_catalog"), true, ``, ``, ``, `prefix or object not found`},

		// Names of length 3.

		{`db1.public.foo`, `db1`, mpath("public", "pg_catalog"), true, `db1.public.foo`, `db1.public.foo`, `db1.public[0]`, ``},
		{`db1.public.kv`, `db1`, mpath(), true, `db1.public.kv`, `db1.public.kv`, `db1.public[1]`, ``},
		{`db1.public.blix`, `db1`, mpath(), false, `db1.public.blix`, `db1.public.blix`, `db1.public`, ``},

		{`blix.public.foo`, `db1`, mpath("public"), true, ``, ``, ``, `prefix or object not found`},
		{`blix.public.foo`, `db1`, mpath("public"), false, ``, ``, ``, `prefix or object not found`},

		// Beware: vtables only exist in valid databases and the empty database name.
		{`db1.pg_catalog.pg_tables`, `db1`, mpath(), true, `db1.pg_catalog.pg_tables`, `db1.pg_catalog.pg_tables`, `db1.pg_catalog[0]`, ``},
		{`"".pg_catalog.pg_tables`, `db1`, mpath(), true, `"".pg_catalog.pg_tables`, `"".pg_catalog.pg_tables`, `.pg_catalog[0]`, ``},
		{`blix.pg_catalog.pg_tables`, `db1`, mpath("public"), true, ``, ``, ``, `prefix or object not found`},
		{`blix.pg_catalog.pg_tables`, `db1`, mpath("public"), false, ``, ``, ``, `prefix or object not found`},
		{`"".pg_catalog.blix`, `db1`, mpath(), false, `"".pg_catalog.blix`, `"".pg_catalog.blix`, `.pg_catalog`, ``},

		//
		// Tests for table names with no current database.
		//

		{`kv`, ``, mpath("public", "pg_catalog"), true, ``, ``, ``, `prefix or object not found`},
		{`pg_tables`, ``, mpath("public", "pg_catalog"), true, `pg_tables`, `"".pg_catalog.pg_tables`, `.pg_catalog[0]`, ``},
		{`pg_tables`, ``, mpath(), true, `pg_tables`, `"".pg_catalog.pg_tables`, `.pg_catalog[0]`, ``},
		{`system.users`, ``, mpath(), true, `system.public.users`, `system.public.users`, `system.public[0]`, ``},
		{`"".system.users`, ``, mpath(), true, ``, ``, ``, `prefix or object not found`},
		{`"".system.users`, ``, mpath(), false, ``, ``, ``, `prefix or object not found`},

		{`blix`, ``, mpath("public"), false, ``, ``, ``, `prefix or object not found`},
		{`blix`, ``, mpath("public", "pg_catalog"), false, `blix`, `"".pg_catalog.blix`, `.pg_catalog`, ``},

		// Names of length 2.

		{`public.kv`, ``, mpath("public", "pg_catalog"), true, ``, ``, ``, `prefix or object not found`},
		{`pg_catalog.pg_tables`, ``, mpath("public", "pg_catalog"), true, `pg_catalog.pg_tables`, `"".pg_catalog.pg_tables`, `.pg_catalog[0]`, ``},

		// Compat with CockroachDB v1.x.
		{`db1.kv`, ``, mpath("public", "pg_catalog"), true, `db1.public.kv`, `db1.public.kv`, `db1.public[1]`, ``},
		{`db1.blix`, ``, mpath("public", "pg_catalog"), true, ``, ``, ``, `prefix or object not found`},

		{`blix.pg_tables`, ``, mpath("public", "pg_catalog"), true, ``, ``, ``, `prefix or object not found`},

		// Names of length 3.

		{`db1.public.foo`, ``, mpath("public", "pg_catalog"), true, `db1.public.foo`, `db1.public.foo`, `db1.public[0]`, ``},
		{`db1.public.kv`, ``, mpath(), true, `db1.public.kv`, `db1.public.kv`, `db1.public[1]`, ``},
		{`db1.public.blix`, ``, mpath(), false, `db1.public.blix`, `db1.public.blix`, `db1.public`, ``},

		{`blix.public.foo`, ``, mpath("public"), true, ``, ``, ``, `prefix or object not found`},
		{`blix.public.foo`, ``, mpath("public"), false, ``, ``, ``, `prefix or object not found`},

		// Beware: vtables only exist in valid databases and the empty database name.
		{`db1.pg_catalog.pg_tables`, ``, mpath(), true, `db1.pg_catalog.pg_tables`, `db1.pg_catalog.pg_tables`, `db1.pg_catalog[0]`, ``},
		{`"".pg_catalog.pg_tables`, ``, mpath(), true, `"".pg_catalog.pg_tables`, `"".pg_catalog.pg_tables`, `.pg_catalog[0]`, ``},
		{`blix.pg_catalog.pg_tables`, ``, mpath("public"), true, ``, ``, ``, `prefix or object not found`},
		{`blix.pg_catalog.pg_tables`, ``, mpath("public"), false, ``, ``, ``, `prefix or object not found`},
		{`"".pg_catalog.blix`, ``, mpath(), false, `"".pg_catalog.blix`, `"".pg_catalog.blix`, `.pg_catalog`, ``},

		//
		// Tests for table patterns.
		//

		// Patterns of length 1.

		{`*`, `db1`, mpath("public", "pg_catalog"), false, `*`, `db1.public.*`, `db1.public`, ``},

		// Patterns of length 2.
		{`public.*`, `db1`, mpath("public"), false, `public.*`, `db1.public.*`, `db1.public`, ``},
		{`public.*`, `db1`, mpath("public", "pg_catalog"), false, `public.*`, `db1.public.*`, `db1.public`, ``},
		{`public.*`, `db1`, mpath(), false, `public.*`, `db1.public.*`, `db1.public`, ``},

		{`blix.*`, `db1`, mpath("public"), false, ``, ``, ``, `prefix or object not found`},

		{`pg_catalog.*`, `db1`, mpath("public"), false, `pg_catalog.*`, `db1.pg_catalog.*`, `db1.pg_catalog`, ``},
		{`pg_catalog.*`, `db1`, mpath("public", "pg_catalog"), false, `pg_catalog.*`, `db1.pg_catalog.*`, `db1.pg_catalog`, ``},
		{`pg_catalog.*`, `db1`, mpath(), false, `pg_catalog.*`, `db1.pg_catalog.*`, `db1.pg_catalog`, ``},

		//
		// Tests for table patterns with no current database.
		//

		// Patterns of length 1.

		{`*`, ``, mpath("public"), false, ``, ``, ``, `prefix or object not found`},
		{`*`, ``, mpath("public", "pg_catalog"), false, `*`, `"".pg_catalog.*`, `.pg_catalog`, ``},

		// Patterns of length 2.

		{`public.*`, ``, mpath("public", "pg_catalog"), false, ``, ``, ``, `prefix or object not found`},
		// vtables exist also in the empty database.
		{`pg_catalog.*`, ``, mpath("public", "pg_catalog"), false, `pg_catalog.*`, `"".pg_catalog.*`, `.pg_catalog`, ``},
		{`pg_catalog.*`, ``, mpath(), false, `pg_catalog.*`, `"".pg_catalog.*`, `.pg_catalog`, ``},

		// Compat with CockroachDB v1.x.
		{`db1.*`, ``, mpath("public", "pg_catalog"), false, `db1.public.*`, `db1.public.*`, `db1.public`, ``},

		{`blix.*`, ``, mpath("public"), false, ``, ``, ``, `prefix or object not found`},
		{`blix.*`, ``, mpath("public", "pg_catalog"), false, ``, ``, ``, `prefix or object not found`},
		{`blix.*`, ``, mpath(), false, ``, ``, ``, `prefix or object not found`},

		// Patterns of length 3.

		{`db1.public.*`, ``, mpath("public", "pg_catalog"), false, `db1.public.*`, `db1.public.*`, `db1.public`, ``},
		{`db1.public.*`, ``, mpath(), false, `db1.public.*`, `db1.public.*`, `db1.public`, ``},

		{`blix.public.*`, ``, mpath("public"), false, ``, ``, ``, `prefix or object not found`},
		{`blix.public.*`, ``, mpath("public", "pg_catalog"), false, ``, ``, ``, `prefix or object not found`},

		// Beware: vtables only exist in valid databases and the empty database name.
		{`db1.pg_catalog.*`, ``, mpath(), false, `db1.pg_catalog.*`, `db1.pg_catalog.*`, `db1.pg_catalog`, ``},
		{`"".pg_catalog.*`, ``, mpath(), false, `"".pg_catalog.*`, `"".pg_catalog.*`, `.pg_catalog`, ``},
		{`blix.pg_catalog.*`, ``, mpath("public"), false, ``, ``, ``, `prefix or object not found`},

		//
		// Tests for temporary table resolution
		//

		// Names of length 1

		{`foo`, `db3`, tpath("pg_temp_123", "public"), true, `foo`, `db3.pg_temp_123.foo`, `db3.pg_temp_123[0]`, ``},
		{`foo`, `db3`, tpath("pg_temp_123", "public", "pg_temp"), true, `foo`, `db3.public.foo`, `db3.public[0]`, ``},
		{`baz`, `db3`, tpath("pg_temp_123", "public"), true, `baz`, `db3.pg_temp_123.baz`, `db3.pg_temp_123[1]`, ``},
		{`bar`, `db3`, tpath("pg_temp_123", "public"), true, `bar`, `db3.public.bar`, `db3.public[1]`, ``},
		{`bar`, `db3`, tpath("pg_temp_123", "public", "pg_temp"), true, `bar`, `db3.public.bar`, `db3.public[1]`, ``},

		// Names of length 2

		{`public.foo`, `db3`, tpath("pg_temp_123", "public"), true, `public.foo`, `db3.public.foo`, `db3.public[0]`, ``},
		{`pg_temp.foo`, `db3`, tpath("pg_temp_123", "public"), true, `pg_temp_123.foo`, `db3.pg_temp_123.foo`, `db3.pg_temp_123[0]`, ``},
		{`pg_temp_123.foo`, `db3`, tpath("pg_temp_123", "public"), true, `pg_temp_123.foo`, `db3.pg_temp_123.foo`, `db3.pg_temp_123[0]`, ``},

		// Wrongly qualifying a TT/PT as a PT/TT results in an error.
		{`pg_temp.bar`, `db3`, tpath("pg_temp_123", "public"), true, ``, ``, ``, `prefix or object not found`},
		{`public.baz`, `db3`, tpath("pg_temp_123", "public"), true, ``, ``, ``, `prefix or object not found`},

		// Case where the temporary table being created has the same name as an
		// existing persistent table.
		{`pg_temp.bar`, `db3`, tpath("pg_temp_123", "public"), false, `pg_temp_123.bar`, `db3.pg_temp_123.bar`, `db3.pg_temp_123`, ``},

		// Case where the persistent table being created has the same name as an
		// existing temporary table.
		{`public.baz`, `db3`, tpath("pg_temp_123", "public"), false, `public.baz`, `db3.public.baz`, `db3.public`, ``},

		// Names of length 3

		{`db3.public.foo`, `db3`, tpath("pg_temp_123", "public"), true, `db3.public.foo`, `db3.public.foo`, `db3.public[0]`, ``},
		{`db3.pg_temp.foo`, `db3`, tpath("pg_temp_123", "public"), true, `db3.pg_temp_123.foo`, `db3.pg_temp_123.foo`, `db3.pg_temp_123[0]`, ``},
		{`db3.pg_temp_123.foo`, `db3`, tpath("pg_temp_123", "public"), true, `db3.pg_temp_123.foo`, `db3.pg_temp_123.foo`, `db3.pg_temp_123[0]`, ``},

		// Wrongly qualifying a TT/PT as a PT/TT results in an error.
		{`db3.pg_temp.bar`, `db3`, tpath("pg_temp_123", "public"), true, ``, ``, ``, `prefix or object not found`},
		{`db3.public.baz`, `db3`, tpath("pg_temp_123", "public"), true, ``, ``, ``, `prefix or object not found`},

		// Case where the temporary table being created has the same name as an
		// existing persistent table.
		{`db3.pg_temp.bar`, `db3`, tpath("pg_temp_123", "public"), false, `db3.pg_temp_123.bar`, `db3.pg_temp_123.bar`, `db3.pg_temp_123`, ``},

		// Case where the persistent table being created has the same name as an
		// existing temporary table.
		{`db3.public.baz`, `db3`, tpath("pg_temp_123", "public"), false, `db3.public.baz`, `db3.public.baz`, `db3.public`, ``},
	}

	fakeResolver := newFakeMetadata()
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s/%s/%s/%v", tc.in, tc.curDb, tc.searchPath, tc.expected), func(t *testing.T) {
			fakeResolver.t = t
			tp, sc, err := func() (tree.TablePattern, string, error) {
				stmt, err := parser.ParseOne(fmt.Sprintf("GRANT SELECT ON TABLE %s TO foo", tc.in))
				if err != nil {
					return nil, "", err
				}
				tp, err := stmt.AST.(*tree.Grant).Targets.Tables[0].NormalizeTablePattern()
				if err != nil {
					return nil, "", err
				}

				var found bool
				var scPrefix, ctPrefix string
				var scMeta catalog.ResolvedObjectPrefix
				var obMeta catalog.Descriptor
				ctx := context.Background()
				switch tpv := tp.(type) {
				case *tree.AllTablesSelector:
					found, scMeta, err = resolver.ResolveObjectNamePrefix(ctx, fakeResolver, tc.curDb, tc.searchPath, &tpv.ObjectNamePrefix)
					scPrefix = tpv.Schema()
					ctPrefix = tpv.Catalog()
				case *tree.TableName:
					if tc.expected {
						flags := tree.ObjectLookupFlags{}
						// TODO: As part of work for #34240, we should be operating on
						//  UnresolvedObjectNames here, rather than TableNames.
						un := tpv.ToUnresolvedObjectName()
						found, scMeta, obMeta, err = resolver.ResolveExisting(ctx, un, fakeResolver, flags, tc.curDb, tc.searchPath)
					} else {
						// TODO: As part of work for #34240, we should be operating on
						//  UnresolvedObjectNames here, rather than TableNames.
						un := tpv.ToUnresolvedObjectName()
						found, _, scMeta, err = resolver.ResolveTarget(ctx, un, fakeResolver, tc.curDb, tc.searchPath)
					}
					tpv.ObjectNamePrefix = scMeta.NamePrefix()
					scPrefix = tpv.Schema()
					ctPrefix = tpv.Catalog()
				default:
					t.Fatalf("%s: unknown pattern type: %T", t.Name(), tp)
				}
				if err != nil {
					return nil, "", err
				}

				var scRes string
				if scMeta != (catalog.ResolvedObjectPrefix{}) {
					scRes = fmt.Sprintf("%s.%s", ctPrefix, scPrefix)
				}
				if obMeta != nil {
					obIdx := obMeta.GetID()
					scRes = fmt.Sprintf("%s.%s[%d]", ctPrefix, scPrefix, obIdx)
				}

				if !found {
					return nil, "", fmt.Errorf("prefix or object not found")
				}
				return tp, scRes, nil
			}()

			if !testutils.IsError(err, tc.err) {
				t.Fatalf("%s: expected %s, but found %v", t.Name(), tc.err, err)
			}
			if tc.err != "" {
				return
			}
			if out := tp.String(); tc.out != out {
				t.Errorf("%s: expected %s, but found %s", t.Name(), tc.out, out)
			}
			switch tpv := tp.(type) {
			case *tree.AllTablesSelector:
				tpv.ObjectNamePrefix.ExplicitCatalog = true
				tpv.ObjectNamePrefix.ExplicitSchema = true
			case *tree.TableName:
				tpv.ObjectNamePrefix.ExplicitCatalog = true
				tpv.ObjectNamePrefix.ExplicitSchema = true
			}
			if out := tp.String(); tc.expanded != out {
				t.Errorf("%s: expected full %s, but found %s", t.Name(), tc.expanded, out)
			}
			if tc.scName != sc {
				t.Errorf("%s: expected schema %s, but found %s", t.Name(), tc.scName, sc)
			}
		})
	}
}
