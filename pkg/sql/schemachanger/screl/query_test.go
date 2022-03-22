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
	mkSeq := func(id descpb.ID) *scpb.Target {
		t := scpb.MakeTarget(
			scpb.ToPublic,
			&scpb.Sequence{SequenceID: id},
			nil, /* metadata */
		)
		return &t
	}
	mkFK := func(seqID, tblID descpb.ID) *scpb.Target {
		t := scpb.MakeTarget(
			scpb.ToPublic,
			&scpb.ForeignKeyConstraint{
				TableID:           tblID,
				ReferencedTableID: seqID},
			nil, /* metadata */
		)
		return &t
	}
	mkTable := func(id descpb.ID) *scpb.Target {
		t := scpb.MakeTarget(
			scpb.ToPublic,
			&scpb.Table{TableID: id},
			nil, /* metadata */
		)
		return &t
	}
	concatNodes := func(nodes ...[]*screl.Node) []*screl.Node {
		var ret []*screl.Node
		for _, n := range nodes {
			ret = append(ret, n...)
		}
		return ret
	}
	mkNodes := func(status scpb.Status, targets ...*scpb.Target) []*screl.Node {
		var ret []*screl.Node
		for _, t := range targets {
			ret = append(ret, &screl.Node{CurrentStatus: status, Target: t})
		}
		return ret
	}
	mkTargets := func(seqID, tabID descpb.ID) []*scpb.Target {
		return []*scpb.Target{
			mkSeq(seqID),
			mkTable(tabID),
			mkFK(seqID, tabID),
		}
	}
	var (
		tableEl, tableTarget, tableNode rel.Var = "table-el", "table-target", "table-node"
		refEl, refTarget, refNode       rel.Var = "ref-el", "ref-target", "ref-node"
		seqEl, seqTarget, seqNode       rel.Var = "seq-el", "seq-target", "seq-node"
		tableID, seqID, dir, status     rel.Var = "table-id", "seq-id", "dir", "status"
		pathJoinQuery                           = screl.MustQuery(
			tableEl.Type((*scpb.Table)(nil)),
			refEl.Type((*scpb.ForeignKeyConstraint)(nil)),
			seqEl.Type((*scpb.Sequence)(nil)),

			tableEl.AttrEqVar(screl.DescID, tableID),
			refEl.AttrEqVar(screl.DescID, tableID),
			refEl.AttrEqVar(screl.ReferencedDescID, seqID),
			seqEl.AttrEqVar(screl.DescID, seqID),

			screl.JoinTargetNode(tableEl, tableTarget, tableNode),
			screl.JoinTargetNode(refEl, refTarget, refNode),
			screl.JoinTargetNode(seqEl, seqTarget, seqNode),

			dir.Entities(screl.TargetStatus, tableTarget, refTarget, seqTarget),
			status.Entities(screl.CurrentStatus, tableNode, refNode, seqNode),
		)
	)
	type queryExpectations struct {
		query *rel.Query
		nodes []rel.Var
		exp   []string
	}
	for _, c := range []struct {
		nodes   []*screl.Node
		queries []queryExpectations
	}{
		{
			nodes: concatNodes(
				mkNodes(scpb.Status_ABSENT, mkTargets(1, 2)...),
				mkNodes(scpb.Status_PUBLIC, mkTargets(1, 2)...),
				mkNodes(scpb.Status_ABSENT, mkTargets(3, 4)...),
				mkNodes(scpb.Status_PUBLIC,
					mkSeq(5),
					mkTable(6),
					mkFK(6, 5)),
			),
			queries: []queryExpectations{
				{
					query: pathJoinQuery,
					nodes: []rel.Var{tableNode, seqNode},
					exp: []string{`
[[Table:{DescID: 2}, PUBLIC], ABSENT]
[[Sequence:{DescID: 1}, PUBLIC], ABSENT]`, `
[[Table:{DescID: 2}, PUBLIC], PUBLIC]
[[Sequence:{DescID: 1}, PUBLIC], PUBLIC]`, `
[[Table:{DescID: 4}, PUBLIC], ABSENT]
[[Sequence:{DescID: 3}, PUBLIC], ABSENT]`,
					},
				},
				{
					query: pathJoinQuery,
					nodes: []rel.Var{tableNode, seqNode, refNode},
					exp: []string{`
[[Table:{DescID: 2}, PUBLIC], ABSENT]
[[Sequence:{DescID: 1}, PUBLIC], ABSENT]
[[ForeignKeyConstraint:{DescID: 2, ConstraintID: 0, ReferencedDescID: 1}, PUBLIC], ABSENT]`, `
[[Table:{DescID: 2}, PUBLIC], PUBLIC]
[[Sequence:{DescID: 1}, PUBLIC], PUBLIC]
[[ForeignKeyConstraint:{DescID: 2, ConstraintID: 0, ReferencedDescID: 1}, PUBLIC], PUBLIC]`, `
[[Table:{DescID: 4}, PUBLIC], ABSENT]
[[Sequence:{DescID: 3}, PUBLIC], ABSENT]
[[ForeignKeyConstraint:{DescID: 4, ConstraintID: 0, ReferencedDescID: 3}, PUBLIC], ABSENT]`,
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
		buf.WriteString(screl.NodeString(r.Var(n).(*screl.Node)))
	}
	return buf.String()
}
