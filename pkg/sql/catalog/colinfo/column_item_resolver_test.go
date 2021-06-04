// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package colinfo_test

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo/colinfotestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

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
	res colinfo.NumResolutionResults,
	prefix *tree.TableName,
	srcMeta colinfo.ColumnSourceMeta,
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
			return colinfo.MoreThanOne, nil, nil, fmt.Errorf("ambiguous source name: %q", &tn)
		}
		found = true
		prefix = &t.srcName
		columns = colsRes(t.columns)
	}
	if !found {
		return colinfo.NoResults, nil, nil, nil
	}
	return colinfo.ExactlyOne, prefix, columns, nil
}

// FindSourceProvidingColumn is part of the ColumnItemResolver interface.
func (f *fakeSource) FindSourceProvidingColumn(
	_ context.Context, col tree.Name,
) (prefix *tree.TableName, srcMeta colinfo.ColumnSourceMeta, colHint int, err error) {
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
	srcMeta colinfo.ColumnSourceMeta,
	colHint int,
	col tree.Name,
) (colinfo.ColumnResolutionResult, error) {
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

var _ colinfotestutils.ColumnItemResolverTester = &fakeSource{}

// GetColumnItemResolver is part of the sqlutils.ColumnItemResolverTester
// interface.
func (f *fakeSource) GetColumnItemResolver() colinfo.ColumnItemResolver {
	return f
}

// AddTable is part of the sqlutils.ColumnItemResolverTester interface.
func (f *fakeSource) AddTable(tabName tree.TableName, colNames []tree.Name) {
	f.knownTables = append(f.knownTables, knownTable{srcName: tabName, columns: colNames})
}

// ResolveQualifiedStarTestResults is part of the
// sqlutils.ColumnItemResolverTester interface.
func (f *fakeSource) ResolveQualifiedStarTestResults(
	srcName *tree.TableName, srcMeta colinfo.ColumnSourceMeta,
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
func (f *fakeSource) ResolveColumnItemTestResults(
	res colinfo.ColumnResolutionResult,
) (string, error) {
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
	colinfotestutils.RunResolveQualifiedStarTest(t, f)
}

func TestResolveColumnItem(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	f := &fakeSource{t: t}
	colinfotestutils.RunResolveColumnItemTest(t, f)
}
