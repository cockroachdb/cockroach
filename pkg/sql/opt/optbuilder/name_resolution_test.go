// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package optbuilder

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
)

var _ sqlutils.ColumnItemResolverTester = &scope{}

// GetColumnItemResolver is part of the sqlutils.ColumnItemResolverTester
// interface.
func (s *scope) GetColumnItemResolver() tree.ColumnItemResolver {
	return s
}

// AddTable is part of the sqlutils.ColumnItemResolverTester interface.
func (s *scope) AddTable(tabName tree.TableName, colNames []tree.Name) {
	for _, col := range colNames {
		s.cols = append(s.cols, scopeColumn{name: col, table: tabName})
	}
}

// ResolveQualifiedStarTestResults is part of the
// sqlutils.ColumnItemResolverTester interface.
func (s *scope) ResolveQualifiedStarTestResults(
	srcName *tree.TableName, srcMeta tree.ColumnSourceMeta,
) (string, string, error) {
	s, ok := srcMeta.(*scope)
	if !ok {
		return "", "", fmt.Errorf("resolver did not return *scope, found %T instead", srcMeta)
	}
	nl := make(tree.NameList, 0, len(s.cols))
	for i := range s.cols {
		col := s.cols[i]
		if col.table == *srcName && !col.hidden {
			nl = append(nl, col.name)
		}
	}
	return srcName.String(), nl.String(), nil
}

// ResolveColumnItemTestResults is part of the
// sqlutils.ColumnItemResolverTester interface.
func (s *scope) ResolveColumnItemTestResults(colRes tree.ColumnResolutionResult) (string, error) {
	col, ok := colRes.(*scopeColumn)
	if !ok {
		return "", fmt.Errorf("resolver did not return *scopeColumn, found %T instead", colRes)
	}
	return fmt.Sprintf("%s.%s", col.table.String(), col.name), nil
}

func TestResolveQualifiedStar(t *testing.T) {
	s := &scope{}
	sqlutils.RunResolveQualifiedStarTest(t, s)
}

func TestResolveColumnItem(t *testing.T) {
	s := &scope{}
	sqlutils.RunResolveColumnItemTest(t, s)
}
