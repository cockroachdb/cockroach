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
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func TestClassifyTablePattern(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
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
	defer log.Scope(t).Close(t)
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
	defer log.Scope(t).Close(t)
	f := &fakeSource{t: t}
	sqlutils.RunResolveQualifiedStarTest(t, f)
}

func TestResolveColumnItem(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	f := &fakeSource{t: t}
	sqlutils.RunResolveColumnItemTest(t, f)
}
