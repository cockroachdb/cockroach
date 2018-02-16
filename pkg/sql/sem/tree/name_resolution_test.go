// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package tree_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/testutils"
)

func TestNormalizeTableName(t *testing.T) {
	testCases := []struct {
		in, out  string
		expanded string
		err      string
	}{
		{`a`, `a`, `""."".a`, ``},
		{`a.b`, `a.b`, `"".a.b`, ``},
		{`a.b.c`, `a.b.c`, `a.b.c`, ``},
		{`a.b.c.d`, ``, ``, `syntax error at or near "\."`},
		{`a.""`, ``, ``, `invalid table name: a\.""`},
		{`a.b.""`, ``, ``, `invalid table name: a\.b\.""`},
		{`a.b.c.""`, ``, ``, `syntax error at or near "\."`},
		{`a."".c`, ``, ``, `invalid table name: a\.""\.c`},

		// CockroachDB extension: empty catalog name.
		{`"".b.c`, `"".b.c`, `"".b.c`, ``},

		// Check keywords: disallowed in first position, ok afterwards.
		{`user.x.y`, ``, ``, `syntax error`},
		{`"user".x.y`, `"user".x.y`, `"user".x.y`, ``},
		{`x.user.y`, `x."user".y`, `x."user".y`, ``},
		{`x.user`, `x."user"`, `"".x."user"`, ``},

		{`foo@bar`, ``, ``, `syntax error at or near "@"`},
		{`test.*`, ``, ``, `syntax error at or near "\*"`},
	}

	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			tn, err := func() (*tree.TableName, error) {
				stmt, err := parser.ParseOne(fmt.Sprintf("ALTER TABLE %s RENAME TO x", tc.in))
				if err != nil {
					return nil, err
				}
				tn, err := stmt.(*tree.RenameTable).Name.Normalize()
				if err != nil {
					return nil, err
				}
				return tn, nil
			}()
			if !testutils.IsError(err, tc.err) {
				t.Fatalf("%s: expected %s, but found %v", tc.in, tc.err, err)
			}
			if tc.err != "" {
				return
			}
			if out := tn.String(); tc.out != out {
				t.Fatalf("%s: expected %s, but found %s", tc.in, tc.out, out)
			}
			tn.ExplicitSchema = true
			tn.ExplicitCatalog = true
			if out := tn.String(); tc.expanded != out {
				t.Fatalf("%s: expected full %s, but found %s", tc.in, tc.expanded, out)
			}
		})
	}
}

func TestClassifyTablePattern(t *testing.T) {
	testCases := []struct {
		in, out  string
		expanded string
		err      string
	}{
		{`a`, `a`, `""."".a`, ``},
		{`a.b`, `a.b`, `"".a.b`, ``},
		{`a.b.c`, `a.b.c`, `a.b.c`, ``},
		{`a.b.c.d`, ``, ``, `syntax error at or near "\."`},
		{`a.""`, ``, ``, `invalid table name: a\.""`},
		{`a.b.""`, ``, ``, `invalid table name: a\.b\.""`},
		{`a.b.c.""`, ``, ``, `syntax error at or near "\."`},
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
		{`a.b.c.*`, ``, ``, `syntax error at or near "\."`},
		{`a.b.*.c`, ``, ``, `syntax error at or near "\."`},
		{`a.*.b`, ``, ``, `syntax error at or near "\."`},
		{`*.b`, ``, ``, `syntax error at or near "\."`},
		{`"".*`, ``, ``, `invalid table name: "".\*`},
		{`a."".*`, ``, ``, `invalid table name: a\.""\.\*`},
		{`a.b."".*`, ``, ``, `syntax error at or near "\."`},
		// CockroachDB extension: empty catalog name.
		{`"".b.*`, `"".b.*`, `"".b.*`, ``},

		// Check keywords: disallowed in first position, ok afterwards.
		{`user.x.*`, ``, ``, `syntax error`},
		{`"user".x.*`, `"user".x.*`, `"user".x.*`, ``},
		{`x.user.*`, `x."user".*`, `x."user".*`, ``},

		{`foo@bar`, ``, ``, `syntax error at or near "@"`},
	}

	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			tp, err := func() (tree.TablePattern, error) {
				stmt, err := parser.ParseOne(fmt.Sprintf("GRANT SELECT ON %s TO foo", tc.in))
				if err != nil {
					return nil, err
				}
				tp, err := stmt.(*tree.Grant).Targets.Tables[0].NormalizeTablePattern()
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
	testCases := []struct {
		in, out string
		err     string
	}{
		{`a`, `a`, ``},
		{`a.b`, `a.b`, ``},
		{`a.b.c`, `a.b.c`, ``},
		{`a.b.c.d`, `a.b.c.d`, ``},
		{`a.b.c.d.e`, ``, `syntax error at or near "\."`},
		{`""`, ``, `invalid column name: ""`},
		{`a.""`, ``, `invalid column name: a\.""`},
		{`a.b.""`, ``, `invalid column name: a\.b\.""`},
		{`a.b.c.""`, ``, `invalid column name: a\.b\.c\.""`},
		{`a.b.c.d.""`, ``, `syntax error at or near "\."`},
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
		{`a.b.c.d.*`, ``, `syntax error at or near "\."`},
		{`a.b.*.c`, ``, `syntax error at or near "\."`},
		{`a.*.b`, ``, `syntax error at or near "\."`},
		{`*.b`, ``, `syntax error at or near "\."`},
		{`"".*`, ``, `invalid column name: "".\*`},
		{`a."".*`, ``, `invalid column name: a\.""\.\*`},
		{`a.b."".*`, ``, `invalid column name: a\.b\.""\.\*`},
		{`a.b.c."".*`, ``, `syntax error at or near "\."`},

		{`"".a.*`, ``, `invalid column name: ""\.a.*`},
		// CockroachDB extension: empty catalog name.
		{`"".a.b.*`, `"".a.b.*`, ``},

		{`a."".c.*`, ``, `invalid column name: a\.""\.c\.*`},

		// Check keywords: disallowed in first position, ok afterwards.
		{`user.x.*`, ``, `syntax error`},
		{`"user".x.*`, `"user".x.*`, ``},
		{`x.user.*`, `x.user.*`, ``},

		{`foo@bar`, ``, `syntax error at or near "@"`},
	}

	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			v, err := func() (tree.VarName, error) {
				stmt, err := parser.ParseOne(fmt.Sprintf("SELECT %s", tc.in))
				if err != nil {
					return nil, err
				}
				v := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(tree.VarName)
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
	knownTables knownTableList
}

type knownTableList []struct {
	srcName tree.TableName
	columns []tree.Name
}

type colsRes tree.NameList

func (c colsRes) ColumnSourceMeta() {}

// FindSourceMatchingName implements the ColumnItemResolver interface.
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
		if t.srcName.TableName != tn.TableName {
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
			return tree.MoreThanOne, nil, nil, fmt.Errorf("ambiguous table name: %s", &tn)
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

// FindSourceProvidingColumn implements the ColumnItemResolver interface.
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
				return nil, nil, -1, fmt.Errorf("ambiguous column name: %s", &col)
			}
			found = true
			colHint = c
			columns = colsRes(t.columns)
			prefix = &t.srcName
			break
		}
	}
	if !found {
		return nil, nil, -1, fmt.Errorf("unknown column name: %s", &col)
	}
	return prefix, columns, colHint, nil
}

type colRes string

func (c colRes) ColumnResolutionResult() {}

// Resolve implements the ColumnItemResolver interface.
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
		return colRes(fmt.Sprintf("%s.%s(%d)", prefix, col, colHint)), nil
	}
	for c, cn := range columns {
		if col == cn {
			// Resolution succeeded.
			return colRes(fmt.Sprintf("%s.%s(%d)", prefix, col, c)), nil
		}
	}
	return nil, fmt.Errorf("unknown column name: %s", &col)
}

func newFakeSource() *fakeSource {
	return &fakeSource{
		knownTables: knownTableList{
			{tree.MakeTableNameWithSchema("", "crdb_internal", "tables"), []tree.Name{"table_name"}},
			{tree.MakeTableName("db1", "foo"), []tree.Name{"x"}},
			{tree.MakeTableName("db2", "foo"), []tree.Name{"x"}},
			{tree.MakeUnqualifiedTableName("bar"), []tree.Name{"x"}},
			{tree.MakeTableName("db1", "kv"), []tree.Name{"k", "v"}},
		},
	}
}

func TestResolveColumnItem(t *testing.T) {
	testCases := []struct {
		in  string
		out string
		err string
	}{
		{`a`, ``, `unknown column name`},
		{`x`, ``, `ambiguous column name`},
		{`k`, `db1.public.kv.k(0)`, ``},
		{`v`, `db1.public.kv.v(1)`, ``},
		{`table_name`, `"".crdb_internal.tables.table_name(0)`, ``},

		{`blix.x`, ``, `no data source matches prefix: blix`},
		{`"".x`, ``, `invalid column name: ""\.x`},
		{`foo.x`, ``, `ambiguous table name`},
		{`kv.k`, `db1.public.kv.k(0)`, ``},
		{`bar.x`, `bar.x(0)`, ``},
		{`tables.table_name`, `"".crdb_internal.tables.table_name(0)`, ``},

		{`a.b.x`, ``, `no data source matches prefix: a\.b`},
		{`crdb_internal.tables.table_name`, `"".crdb_internal.tables.table_name(0)`, ``},
		{`public.foo.x`, ``, `ambiguous table name`},
		{`public.kv.k`, `db1.public.kv.k(0)`, ``},

		// CockroachDB extension: d.t.x -> d.public.t.x
		{`db1.foo.x`, `db1.public.foo.x(0)`, ``},
		{`db2.foo.x`, `db2.public.foo.x(0)`, ``},

		{`a.b.c.x`, ``, `no data source matches prefix: a\.b\.c`},
		{`"".crdb_internal.tables.table_name`, `"".crdb_internal.tables.table_name(0)`, ``},
		{`db1.public.foo.x`, `db1.public.foo.x(0)`, ``},
		{`db2.public.foo.x`, `db2.public.foo.x(0)`, ``},
		{`db1.public.kv.v`, `db1.public.kv.v(1)`, ``},
	}

	fakeFrom := newFakeSource()
	for _, tc := range testCases {
		t.Run(tc.in, func(t *testing.T) {
			fakeFrom.t = t
			out, err := func() (string, error) {
				stmt, err := parser.ParseOne(fmt.Sprintf("SELECT %s", tc.in))
				if err != nil {
					return "", err
				}
				v := stmt.(*tree.Select).Select.(*tree.SelectClause).Exprs[0].Expr.(tree.VarName)
				c, err := v.NormalizeVarName()
				if err != nil {
					return "", err
				}
				ci, ok := c.(*tree.ColumnItem)
				if !ok {
					return "", fmt.Errorf("var name %s (%T) did not resolve to ColumnItem, found %T instead",
						v, v, c)
				}
				res, err := ci.Resolve(context.Background(), fakeFrom)
				if err != nil {
					return "", err
				}
				s, ok := res.(colRes)
				if !ok {
					return "", fmt.Errorf("fake resolver did not return colRes, found %T instead", res)
				}
				return string(s), nil
			}()
			if !testutils.IsError(err, tc.err) {
				t.Fatalf("%s: expected %s, but found %v", tc.in, tc.err, err)
			}
			if tc.err != "" {
				return
			}

			if tc.out != out {
				t.Fatalf("%s: expected %s, but found %s", tc.in, tc.out, out)
			}
		})
	}
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
	_ context.Context, dbName, scName, tbName string,
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
		},
	}
}

func TestResolveTablePatternOrName(t *testing.T) {
	type spath = sessiondata.SearchPath

	var mpath = func(args ...string) spath { return sessiondata.MakeSearchPath(args) }

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

		{`blix`, ``, mpath("public", "pg_catalog"), false, ``, ``, ``, `prefix or object not found`},

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

		{`*`, ``, mpath("public", "pg_catalog"), false, ``, ``, ``, `prefix or object not found`},

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
				tp, err := stmt.(*tree.Grant).Targets.Tables[0].NormalizeTablePattern()
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
					found, scMeta, err = tpv.TableNamePrefix.Resolve(ctx, fakeResolver, tc.curDb, tc.searchPath)
					scPrefix = tpv.Schema()
					ctPrefix = tpv.Catalog()
				case *tree.TableName:
					if tc.expected {
						found, obMeta, err = tpv.ResolveExisting(ctx, fakeResolver, tc.curDb, tc.searchPath)
					} else {
						found, scMeta, err = tpv.ResolveTarget(ctx, fakeResolver, tc.curDb, tc.searchPath)
					}
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
				tpv.TableNamePrefix.ExplicitCatalog = true
				tpv.TableNamePrefix.ExplicitSchema = true
			case *tree.TableName:
				tpv.TableNamePrefix.ExplicitCatalog = true
				tpv.TableNamePrefix.ExplicitSchema = true
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
