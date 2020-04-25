// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestClassifyTablePattern(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		in, out  string
		expanded string
		err      string
	}{
		{`a`, `a`, `""."".a`, ``},
		{`a.b`, `a.b`, `"".a.b`, ``},
		{`a.b.c`, `a.b.c`, `a.b.c`, ``},
		{`a.b.c.d`, ``, ``, `at or near "\.": syntax error`},
		{`a.""`, ``, ``, `invalid table name: a\.""`},
		{`a.b.""`, ``, ``, `invalid table name: a\.b\.""`},
		{`a.b.c.""`, ``, ``, `at or near "\.": syntax error`},
		{`a."".c`, ``, ``, `invalid table name: a\.""\.c`},
		// CockroachDB extension: empty catalog name.
		{`"".b.c`, `"".b.c`, `"".b.c`, ``},

		// Check keywords: disallowed in first position, ok afterwards.
		{`user.x.y`, ``, ``, `syntax error`},
		{`"user".x.y`, `"user".x.y`, `"user".x.y`, ``},
		{`x.user.y`, `x."user".y`, `x."user".y`, ``},
		{`x.user`, `x."user"`, `"".x."user"`, ``},

		{`*`, `*`, `""."".*`, ``},
		{`a.*`, `a.*`, `"".a.*`, ``},
		{`a.b.*`, `a.b.*`, `a.b.*`, ``},
		{`a.b.c.*`, ``, ``, `at or near "\.": syntax error`},
		{`a.b.*.c`, ``, ``, `at or near "\.": syntax error`},
		{`a.*.b`, ``, ``, `at or near "\.": syntax error`},
		{`*.b`, ``, ``, `at or near "\.": syntax error`},
		{`"".*`, ``, ``, `invalid table name: "".\*`},
		{`a."".*`, ``, ``, `invalid table name: a\.""\.\*`},
		{`a.b."".*`, ``, ``, `invalid table name: a.b.""`},
		// CockroachDB extension: empty catalog name.
		{`"".b.*`, `"".b.*`, `"".b.*`, ``},

		// Check keywords: disallowed in first position, ok afterwards.
		{`user.x.*`, ``, ``, `syntax error`},
		{`"user".x.*`, `"user".x.*`, `"user".x.*`, ``},
		{`x.user.*`, `x."user".*`, `x."user".*`, ``},

		{`foo@bar`, ``, ``, `at or near "@": syntax error`},
	}

	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			tp, err := func() (tree.TablePattern, error) {
				stmt, err := parser.ParseOne(fmt.Sprintf("GRANT SELECT ON %s TO foo", tc.in))
				if err != nil {
					return nil, err
				}
				tp, err := stmt.AST.(*tree.Grant).Targets.Tables[0].NormalizeTablePattern()
				if err != nil {
					return nil, err
				}
				return tp, nil
			}()
			if !testutils.IsError(err, tc.err) {
				t.Fatalf("%s: expected %s, but found %v", tc.in, tc.err, err)
			}
			if tc.err != "" {
				return
			}
			if out := tp.String(); tc.out != out {
				t.Fatalf("%s: expected %s, but found %s", tc.in, tc.out, out)
			}
			switch tpv := tp.(type) {
			case *tree.AllTablesSelector:
				tpv.ExplicitSchema = true
				tpv.ExplicitCatalog = true
			case *tree.TableName:
				tpv.ExplicitSchema = true
				tpv.ExplicitCatalog = true
			default:
				t.Fatalf("%s: unknown pattern type: %T", tc.in, tp)
			}

			if out := tp.String(); tc.expanded != out {
				t.Fatalf("%s: expected full %s, but found %s", tc.in, tc.expanded, out)
			}
		})
	}
}

func TestClassifyColumnName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	testCases := []struct {
		in, out string
		err     string
	}{
		{`a`, `a`, ``},
		{`a.b`, `a.b`, ``},
		{`a.b.c`, `a.b.c`, ``},
		{`a.b.c.d`, `a.b.c.d`, ``},
		{`a.b.c.d.e`, ``, `at or near "\.": syntax error`},
		{`""`, ``, `invalid column name: ""`},
		{`a.""`, ``, `invalid column name: a\.""`},
		{`a.b.""`, ``, `invalid column name: a\.b\.""`},
		{`a.b.c.""`, ``, `invalid column name: a\.b\.c\.""`},
		{`a.b.c.d.""`, ``, `at or near "\.": syntax error`},
		{`"".a`, ``, `invalid column name: ""\.a`},
		{`"".a.b`, ``, `invalid column name: ""\.a\.b`},
		// CockroachDB extension: empty catalog name.
		{`"".a.b.c`, `"".a.b.c`, ``},

		{`a.b."".d`, ``, `invalid column name: a\.b\.""\.d`},
		{`a."".c.d`, ``, `invalid column name: a\.""\.c\.d`},

		// Check keywords: disallowed in first position, ok afterwards.
		{`user.x.y`, ``, `syntax error`},
		{`"user".x.y`, `"user".x.y`, ``},
		{`x.user.y`, `x.user.y`, ``},
		{`x.user`, `x."user"`, ``},

		{`*`, `*`, ``},
		{`a.*`, `a.*`, ``},
		{`a.b.*`, `a.b.*`, ``},
		{`a.b.c.*`, `a.b.c.*`, ``},
		{`a.b.c.d.*`, ``, `at or near "\.": syntax error`},
		{`a.b.*.c`, ``, `at or near "\.": syntax error`},
		{`a.*.b`, ``, `at or near "\.": syntax error`},
		{`*.b`, ``, `at or near "\.": syntax error`},
		{`"".*`, ``, `invalid column name: "".\*`},
		{`a."".*`, ``, `invalid column name: a\.""\.\*`},
		{`a.b."".*`, ``, `invalid column name: a\.b\.""\.\*`},
		{`a.b.c."".*`, ``, `at or near "\.": syntax error`},

		{`"".a.*`, ``, `invalid column name: ""\.a.*`},
		// CockroachDB extension: empty catalog name.
		{`"".a.b.*`, `"".a.b.*`, ``},

		{`a."".c.*`, ``, `invalid column name: a\.""\.c\.*`},

		// Check keywords: disallowed in first position, ok afterwards.
		{`user.x.*`, ``, `syntax error`},
		{`"user".x.*`, `"user".x.*`, ``},
		{`x.user.*`, `x.user.*`, ``},

		{`foo@bar`, ``, `at or near "@": syntax error`},
	}

	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			v, err := func() (tree.VarName, error) {
				stmt, err := parser.ParseOne(fmt.Sprintf("SELECT %s", tc.in))
				if err != nil {
					return nil, err
				}
				v := stmt.AST.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(tree.VarName)
				return v.NormalizeVarName()
			}()
			if !testutils.IsError(err, tc.err) {
				t.Fatalf("%s: expected %s, but found %v", tc.in, tc.err, err)
			}
			if tc.err != "" {
				return
			}
			if out := v.String(); tc.out != out {
				t.Fatalf("%s: expected %s, but found %s", tc.in, tc.out, out)
			}
			switch v.(type) {
			case *tree.AllColumnsSelector:
			case tree.UnqualifiedStar:
			case *tree.ColumnItem:
			default:
				t.Fatalf("%s: unknown var type: %T", tc.in, v)
			}
		})
	}
}

// fakeSource represents a fake column resolution environment for tests.
type fakeSource struct {
	t           *testing.T
	knownTables []knownTable
}

type knownTable struct {
	srcName tree.TableName
	columns []tree.Name
}

type colsRes tree.NameList

func (c colsRes) ColumnSourceMeta() {}

// FindSourceMatchingName is part of the ColumnItemResolver interface.
func (f *fakeSource) FindSourceMatchingName(
	_ context.Context, tn tree.TableName,
) (
	res tree.NumResolutionResults,
	prefix *tree.TableName,
	srcMeta tree.ColumnSourceMeta,
	err error,
) {
	defer func() {
		f.t.Logf("FindSourceMatchingName(%s) -> res %d prefix %s meta %v err %v",
			&tn, res, prefix, srcMeta, err)
	}()
	found := false
	var columns colsRes
	for i := range f.knownTables {
		t := &f.knownTables[i]
		if t.srcName.ObjectName != tn.ObjectName {
			continue
		}
		if tn.ExplicitSchema {
			if !t.srcName.ExplicitSchema || t.srcName.SchemaName != tn.SchemaName {
				continue
			}
			if tn.ExplicitCatalog {
				if !t.srcName.ExplicitCatalog || t.srcName.CatalogName != tn.CatalogName {
					continue
				}
			}
		}
		if found {
			return tree.MoreThanOne, nil, nil, fmt.Errorf("ambiguous source name: %q", &tn)
		}
		found = true
		prefix = &t.srcName
		columns = colsRes(t.columns)
	}
	if !found {
		return tree.NoResults, nil, nil, nil
	}
	return tree.ExactlyOne, prefix, columns, nil
}

// FindSourceProvidingColumn is part of the ColumnItemResolver interface.
func (f *fakeSource) FindSourceProvidingColumn(
	_ context.Context, col tree.Name,
) (prefix *tree.TableName, srcMeta tree.ColumnSourceMeta, colHint int, err error) {
	defer func() {
		f.t.Logf("FindSourceProvidingColumn(%s) -> prefix %s meta %v hint %d err %v",
			col, prefix, srcMeta, colHint, err)
	}()
	found := false
	var columns colsRes
	for i := range f.knownTables {
		t := &f.knownTables[i]
		for c, cn := range t.columns {
			if cn != col {
				continue
			}
			if found {
				return nil, nil, -1, f.ambiguousColumnErr(col)
			}
			found = true
			colHint = c
			columns = colsRes(t.columns)
			prefix = &t.srcName
			break
		}
	}
	if !found {
		return nil, nil, -1, fmt.Errorf("column %q does not exist", &col)
	}
	return prefix, columns, colHint, nil
}

func (f *fakeSource) ambiguousColumnErr(col tree.Name) error {
	var candidates bytes.Buffer
	sep := ""
	for i := range f.knownTables {
		t := &f.knownTables[i]
		for _, cn := range t.columns {
			if cn == col {
				fmt.Fprintf(&candidates, "%s%s.%s", sep, tree.ErrString(&t.srcName), cn)
				sep = ", "
			}
		}
	}
	return fmt.Errorf("column reference %q is ambiguous (candidates: %s)", &col, candidates.String())
}

type colRes string

func (c colRes) ColumnResolutionResult() {}

// Resolve is part of the ColumnItemResolver interface.
func (f *fakeSource) Resolve(
	_ context.Context,
	prefix *tree.TableName,
	srcMeta tree.ColumnSourceMeta,
	colHint int,
	col tree.Name,
) (tree.ColumnResolutionResult, error) {
	f.t.Logf("in Resolve: prefix %s meta %v colHint %d col %s",
		prefix, srcMeta, colHint, col)
	columns, ok := srcMeta.(colsRes)
	if !ok {
		return nil, fmt.Errorf("programming error: srcMeta invalid")
	}
	if colHint >= 0 {
		// Resolution succeeded. Let's do some sanity checking.
		if columns[colHint] != col {
			return nil, fmt.Errorf("programming error: invalid colHint %d", colHint)
		}
		return colRes(fmt.Sprintf("%s.%s", prefix, col)), nil
	}
	for _, cn := range columns {
		if col == cn {
			// Resolution succeeded.
			return colRes(fmt.Sprintf("%s.%s", prefix, col)), nil
		}
	}
	return nil, fmt.Errorf("unknown column name: %s", &col)
}

var _ sqlutils.ColumnItemResolverTester = &fakeSource{}

// GetColumnItemResolver is part of the sqlutils.ColumnItemResolverTester
// interface.
func (f *fakeSource) GetColumnItemResolver() tree.ColumnItemResolver {
	return f
}

// AddTable is part of the sqlutils.ColumnItemResolverTester interface.
func (f *fakeSource) AddTable(tabName tree.TableName, colNames []tree.Name) {
	f.knownTables = append(f.knownTables, knownTable{srcName: tabName, columns: colNames})
}

// ResolveQualifiedStarTestResults is part of the
// sqlutils.ColumnItemResolverTester interface.
func (f *fakeSource) ResolveQualifiedStarTestResults(
	srcName *tree.TableName, srcMeta tree.ColumnSourceMeta,
) (string, string, error) {
	cs, ok := srcMeta.(colsRes)
	if !ok {
		return "", "", fmt.Errorf("fake resolver did not return colsRes, found %T instead", srcMeta)
	}
	nl := tree.NameList(cs)
	return srcName.String(), nl.String(), nil
}

// ResolveColumnItemTestResults is part of the
// sqlutils.ColumnItemResolverTester interface.
func (f *fakeSource) ResolveColumnItemTestResults(res tree.ColumnResolutionResult) (string, error) {
	c, ok := res.(colRes)
	if !ok {
		return "", fmt.Errorf("fake resolver did not return colRes, found %T instead", res)
	}
	return string(c), nil
}

func TestResolveQualifiedStar(t *testing.T) {
	defer leaktest.AfterTest(t)()
	f := &fakeSource{t: t}
	sqlutils.RunResolveQualifiedStarTest(t, f)
}

func TestResolveColumnItem(t *testing.T) {
	defer leaktest.AfterTest(t)()
	f := &fakeSource{t: t}
	sqlutils.RunResolveColumnItemTest(t, f)
}

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

func (*knownSchema) SchemaMeta() {}

type knownCatalog struct {
	ctName  tree.Name
	schemas []knownSchema
}

// LookupSchema implements the TableNameResolver interface.
func (f *fakeMetadata) LookupSchema(
	_ context.Context, dbName, scName string,
) (found bool, scMeta tree.SchemaMeta, err error) {
	defer func() {
		f.t.Logf("LookupSchema(%s, %s) -> found %v meta %v err %v",
			dbName, scName, found, scMeta, err)
	}()
	for i := range f.knownVSchemas {
		v := &f.knownVSchemas[i]
		if scName == string(v.scName) {
			// Virtual schema found, check that the db exists.
			// The empty database is valid.
			if dbName == "" {
				return true, v, nil
			}
			for j := range f.knownCatalogs {
				c := &f.knownCatalogs[j]
				if dbName == string(c.ctName) {
					return true, v, nil
				}
			}
			// No valid database, schema is invalid.
			return false, nil, nil
		}
	}
	for i := range f.knownCatalogs {
		c := &f.knownCatalogs[i]
		if dbName == string(c.ctName) {
			for j := range c.schemas {
				s := &c.schemas[j]
				if scName == string(s.scName) {
					return true, s, nil
				}
			}
			break
		}
	}
	return false, nil, nil
}

type fakeResResult int

func (fakeResResult) NameResolutionResult() {}

// LookupObject implements the TableNameResolver interface.
func (f *fakeMetadata) LookupObject(
	_ context.Context, lookupFlags tree.ObjectLookupFlags, dbName, scName, tbName string,
) (found bool, obMeta tree.NameResolutionResult, err error) {
	defer func() {
		f.t.Logf("LookupObject(%s, %s, %s) -> found %v meta %v err %v",
			dbName, scName, tbName, found, obMeta, err)
	}()
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
					return false, nil, nil
				}
			}
			// Db valid, check the table name.
			for tbIdx, tb := range v.tables {
				if tbName == string(tb) {
					return true, fakeResResult(tbIdx), nil
				}
			}
			foundV = true
			break
		}
	}
	if foundV {
		// Virtual schema matched, but there was no table. Fail.
		return false, nil, nil
	}

	for i := range f.knownCatalogs {
		c := &f.knownCatalogs[i]
		if dbName == string(c.ctName) {
			for j := range c.schemas {
				s := &c.schemas[j]
				if scName == string(s.scName) {
					for tbIdx, tb := range s.tables {
						if tbName == string(tb) {
							return true, fakeResResult(tbIdx), nil
						}
					}
					break
				}
			}
			break
		}
	}
	return false, nil, nil
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
		},
	}
}

func TestResolveTablePatternOrName(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
		{`pg_temp.foo`, `db3`, tpath("pg_temp_123", "public"), true, `pg_temp.foo`, `db3.pg_temp.foo`, `db3.pg_temp[0]`, ``},
		{`pg_temp_123.foo`, `db3`, tpath("pg_temp_123", "public"), true, `pg_temp_123.foo`, `db3.pg_temp_123.foo`, `db3.pg_temp_123[0]`, ``},

		// Wrongly qualifying a TT/PT as a PT/TT results in an error.
		{`pg_temp.bar`, `db3`, tpath("pg_temp_123", "public"), true, ``, ``, ``, `prefix or object not found`},
		{`public.baz`, `db3`, tpath("pg_temp_123", "public"), true, ``, ``, ``, `prefix or object not found`},

		// Cases where a session tries to access a temporary table of another session.
		{`pg_temp_111.foo`, `db3`, tpath("pg_temp_123", "public"), true, ``, ``, ``, `cannot access temporary tables of other sessions`},
		{`pg_temp_111.foo`, `db3`, tpath("pg_temp_123", "public"), false, ``, ``, ``, `cannot access temporary tables of other sessions`},

		// Case where the temporary table being created has the same name as an
		// existing persistent table.
		{`pg_temp.bar`, `db3`, tpath("pg_temp_123", "public"), false, `pg_temp.bar`, `db3.pg_temp.bar`, `db3.pg_temp_123`, ``},

		// Case where the persistent table being created has the same name as an
		// existing temporary table.
		{`public.baz`, `db3`, tpath("pg_temp_123", "public"), false, `public.baz`, `db3.public.baz`, `db3.public`, ``},

		// Cases where the temporary schema has not been created yet
		{`pg_temp.foo`, `db3`, mpath("public"), false, ``, ``, ``, `prefix or object not found`},

		// Names of length 3

		{`db3.public.foo`, `db3`, tpath("pg_temp_123", "public"), true, `db3.public.foo`, `db3.public.foo`, `db3.public[0]`, ``},
		{`db3.pg_temp.foo`, `db3`, tpath("pg_temp_123", "public"), true, `db3.pg_temp.foo`, `db3.pg_temp.foo`, `db3.pg_temp[0]`, ``},
		{`db3.pg_temp_123.foo`, `db3`, tpath("pg_temp_123", "public"), true, `db3.pg_temp_123.foo`, `db3.pg_temp_123.foo`, `db3.pg_temp_123[0]`, ``},

		// Wrongly qualifying a TT/PT as a PT/TT results in an error.
		{`db3.pg_temp.bar`, `db3`, tpath("pg_temp_123", "public"), true, ``, ``, ``, `prefix or object not found`},
		{`db3.public.baz`, `db3`, tpath("pg_temp_123", "public"), true, ``, ``, ``, `prefix or object not found`},

		// Cases where a session tries to access a temporary table of another session.
		{`db3.pg_temp_111.foo`, `db3`, tpath("pg_temp_123", "public"), true, ``, ``, ``, `cannot access temporary tables of other sessions`},
		{`db3.pg_temp_111.foo`, `db3`, tpath("pg_temp_123", "public"), false, ``, ``, ``, `cannot access temporary tables of other sessions`},

		// Case where the temporary table being created has the same name as an
		// existing persistent table.
		{`db3.pg_temp.bar`, `db3`, tpath("pg_temp_123", "public"), false, `db3.pg_temp.bar`, `db3.pg_temp.bar`, `db3.pg_temp_123`, ``},

		// Case where the persistent table being created has the same name as an
		// existing temporary table.
		{`db3.public.baz`, `db3`, tpath("pg_temp_123", "public"), false, `db3.public.baz`, `db3.public.baz`, `db3.public`, ``},

		// Cases where the temporary schema has not been created yet
		{`db3.pg_temp.foo`, `db3`, mpath("public"), false, ``, ``, ``, `prefix or object not found`},
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
				var scMeta interface{}
				var obMeta interface{}
				ctx := context.Background()
				switch tpv := tp.(type) {
				case *tree.AllTablesSelector:
					found, scMeta, err = tpv.ObjectNamePrefix.Resolve(ctx, fakeResolver, tc.curDb, tc.searchPath)
					scPrefix = tpv.Schema()
					ctPrefix = tpv.Catalog()
				case *tree.TableName:
					var prefix tree.ObjectNamePrefix
					if tc.expected {
						flags := tree.ObjectLookupFlags{}
						// TODO: As part of work for #34240, we should be operating on
						//  UnresolvedObjectNames here, rather than TableNames.
						un := tpv.ToUnresolvedObjectName()
						found, prefix, obMeta, err = tree.ResolveExisting(ctx, un, fakeResolver, flags, tc.curDb, tc.searchPath)
					} else {
						// TODO: As part of work for #34240, we should be operating on
						//  UnresolvedObjectNames here, rather than TableNames.
						un := tpv.ToUnresolvedObjectName()
						found, prefix, scMeta, err = tree.ResolveTarget(ctx, un, fakeResolver, tc.curDb, tc.searchPath)
					}
					tpv.ObjectNamePrefix = prefix
					scPrefix = tpv.Schema()
					ctPrefix = tpv.Catalog()
				default:
					t.Fatalf("%s: unknown pattern type: %T", t.Name(), tp)
				}
				if err != nil {
					return nil, "", err
				}

				var scRes string
				if scMeta != nil {
					sc, ok := scMeta.(*knownSchema)
					if !ok {
						t.Fatalf("%s: scMeta not of correct type: %v", t.Name(), scMeta)
					}
					scRes = fmt.Sprintf("%s.%s", ctPrefix, sc.scName)
				}
				if obMeta != nil {
					obIdx, ok := obMeta.(fakeResResult)
					if !ok {
						t.Fatalf("%s: obMeta not of correct type: %v", t.Name(), obMeta)
					}
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
