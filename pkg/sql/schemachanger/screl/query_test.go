// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package screl_test

import (
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/rel"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scpb"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/screl"
	"github.com/stretchr/testify/require"
)

func TestQueryBasic(t *testing.T) {
	mkType := func(id descpb.ID) *scpb.Target {
		return scpb.NewTarget(scpb.Status_PUBLIC, &scpb.Type{TypeID: id}, nil /* metadata */)
	}
	mkTypeRef := func(typID, descID descpb.ID) *scpb.Target {
		return scpb.NewTarget(scpb.Status_PUBLIC, &scpb.ViewDependsOnType{
			TypeID:  typID,
			TableID: descID,
		}, nil /* metadata */)
	}
	mkTable := func(id descpb.ID) *scpb.Target {
		return scpb.NewTarget(scpb.Status_PUBLIC, &scpb.Table{TableID: id}, nil /* metadata */)
	}
	concatNodes := func(nodes ...[]*scpb.Node) []*scpb.Node {
		var ret []*scpb.Node
		for _, n := range nodes {
			ret = append(ret, n...)
		}
		return ret
	}
	mkNodes := func(status scpb.Status, targets ...*scpb.Target) []*scpb.Node {
		var ret []*scpb.Node
		for _, t := range targets {
			ret = append(ret, &scpb.Node{Status: status, Target: t})
		}
		return ret
	}
	mkTableTypeRef := func(typID, tabID descpb.ID) []*scpb.Target {
		return []*scpb.Target{
			mkType(typID),
			mkTable(tabID),
			mkTypeRef(typID, tabID),
		}
	}
	var (
		tableEl, tableTarget, tableNode rel.Var = "table-el", "table-target", "table-node"
		refEl, refTarget, refNode       rel.Var = "ref-el", "ref-target", "ref-node"
		typeEl, typeTarget, typeNode    rel.Var = "type-el", "type-target", "type-node"
		tableID, typeID, dir, status    rel.Var = "table-id", "type-id", "dir", "status"
		pathJoinQuery                           = screl.MustQuery(
			tableEl.Type((*scpb.Table)(nil)),
			refEl.Type((*scpb.ViewDependsOnType)(nil)),
			typeEl.Type((*scpb.Type)(nil)),

			tableEl.AttrEqVar(screl.DescID, tableID),
			refEl.AttrEqVar(screl.DescID, tableID),
			refEl.AttrEqVar(screl.ReferencedDescID, typeID),
			typeEl.AttrEqVar(screl.DescID, typeID),

			screl.JoinTargetNode(tableEl, tableTarget, tableNode),
			screl.JoinTargetNode(refEl, refTarget, refNode),
			screl.JoinTargetNode(typeEl, typeTarget, typeNode),

			dir.Entities(screl.TargetStatus, tableTarget, refTarget, typeTarget),
			status.Entities(screl.Status, tableNode, refNode, typeNode),
		)
	)
	type queryExpectations struct {
		query *rel.Query
		nodes []rel.Var
		exp   []string
	}
	for _, c := range []struct {
		nodes   []*scpb.Node
		queries []queryExpectations
	}{
		{
			nodes: concatNodes(
				mkNodes(scpb.Status_ABSENT, mkTableTypeRef(1, 2)...),
				mkNodes(scpb.Status_PUBLIC, mkTableTypeRef(1, 2)...),
				mkNodes(scpb.Status_ABSENT, mkTableTypeRef(3, 4)...),
				mkNodes(scpb.Status_PUBLIC,
					mkType(5),
					mkTable(6),
					mkTypeRef(6, 5)),
			),
			queries: []queryExpectations{
				{
					query: pathJoinQuery,
					nodes: []rel.Var{tableNode, typeNode},
					exp: []string{`
[Table:{DescID: 2}, PUBLIC; ABSENT]
[Type:{DescID: 1}, PUBLIC; ABSENT]`, `
[Table:{DescID: 2}, PUBLIC; PUBLIC]
[Type:{DescID: 1}, PUBLIC; PUBLIC]`, `
[Table:{DescID: 4}, PUBLIC; ABSENT]
[Type:{DescID: 3}, PUBLIC; ABSENT]`,
					},
				},
				{
					query: pathJoinQuery,
					nodes: []rel.Var{tableNode, typeNode, refNode},
					exp: []string{`
[Table:{DescID: 2}, PUBLIC; ABSENT]
[Type:{DescID: 1}, PUBLIC; ABSENT]
[ViewDependsOnType:{DescID: 2, ReferencedDescID: 1}, PUBLIC; ABSENT]`, `
[Table:{DescID: 2}, PUBLIC; PUBLIC]
[Type:{DescID: 1}, PUBLIC; PUBLIC]
[ViewDependsOnType:{DescID: 2, ReferencedDescID: 1}, PUBLIC; PUBLIC]`, `
[Table:{DescID: 4}, PUBLIC; ABSENT]
[Type:{DescID: 3}, PUBLIC; ABSENT]
[ViewDependsOnType:{DescID: 4, ReferencedDescID: 3}, PUBLIC; ABSENT]`,
					},
				},
			},
		},
	} {
		t.Run("", func(t *testing.T) {
			tr, err := rel.NewDatabase(screl.Schema, [][]rel.Attr{
				{screl.ColumnID},
			})
			require.NoError(t, err)
			for _, n := range c.nodes {
				require.NoError(t, tr.Insert(n))
			}
			for _, q := range c.queries {
				t.Run("", func(t *testing.T) {
					var results []string
					require.NoError(t, q.query.Iterate(tr, func(r rel.Result) error {
						results = append(results, formatResults(r, q.nodes))
						return nil
					}))
					sort.Strings(results)
					require.Equal(t, q.exp, results)
				})
			}
		})
	}
}

func formatResults(r rel.Result, nodes []rel.Var) string {
	var buf strings.Builder
	for _, n := range nodes {
		buf.WriteString("\n")
		buf.WriteString(screl.NodeString(r.Var(n).(*scpb.Node)))
	}
	return buf.String()
}
