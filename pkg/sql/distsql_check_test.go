// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/inverted"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/stretchr/testify/require"
)

// TestDistSQLBlockers verifies that most distSQLBlockers are accumulated
// correctly in checkSupportForPlanNode. Note that the testing of actual
// physical planner heuristics is done via the logic test framework.
func TestDistSQLBlockers(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	var v distSQLExprCheckVisitor
	sd := &sessiondata.SessionData{}
	desc, err := CreateTestTableDescriptor(
		ctx, 1 /* parentID */, 104 /* id */, "CREATE TABLE t (k INT PRIMARY KEY)",
		catpb.NewBasePrivilegeDescriptor(username.AdminRoleName()), nil /* txn */, nil, /* collection */
	)
	require.NoError(t, err)
	mvccCol, err := catalog.MustFindColumnByID(desc, colinfo.MVCCTimestampColumnID)
	require.NoError(t, err)
	funcExpr := tree.NewTypedFuncExpr(
		tree.WrapFunction("crdb_internal.force_retry"), 0, /* aggQualifier */
		nil /* exprs */, nil /* filter */, nil /* windowDef */, types.Bool,
		&tree.FunctionProperties{DistsqlBlocklist: true}, nil, /* overload */
	)

	for _, tc := range []struct {
		name              string // might be left unset - expected.String() will be used instead
		expected          distSQLBlockers
		makePlan          func() planNode
		txnBufferedWrites bool
	}{
		{
			name:     "routine and subquery with oid",
			expected: distSQLBlockers(routineProhibited | oidProhibited),
			makePlan: func() planNode {
				subquery := &tree.Subquery{}
				subquery.SetType(types.Oid)
				rn := &renderNode{
					Render: []tree.TypedExpr{
						&tree.RoutineExpr{},
						subquery,
					},
				}
				rn.Source = &zeroNode{}
				return rn
			},
		},
		{
			expected: distSQLBlockers(oidProhibited),
			makePlan: func() planNode {
				rn := &renderNode{
					Render: []tree.TypedExpr{&tree.DOid{}},
				}
				rn.Source = &zeroNode{}
				return rn
			},
		},
		{
			expected: distSQLBlockers(arrayOfUntypedTuplesProhibited),
			makePlan: func() planNode {
				rn := &renderNode{
					Render: []tree.TypedExpr{
						&tree.DArray{ParamTyp: types.AnyTuple, Array: tree.Datums{}},
					},
				}
				rn.Source = &zeroNode{}
				return rn
			},
		},
		{
			expected: distSQLBlockers(untypedTupleProhibited),
			makePlan: func() planNode {
				rn := &renderNode{
					Render: []tree.TypedExpr{tree.NewDTuple(types.AnyTuple)},
				}
				rn.Source = &zeroNode{}
				return rn
			},
		},
		{
			expected: distSQLBlockers(funcDistSQLBlocklist | jsonpathProhibited),
			makePlan: func() planNode {
				rn := &renderNode{
					Render: []tree.TypedExpr{
						funcExpr,
						&tree.DJsonpath{},
					},
				}
				rn.Source = &zeroNode{}
				return rn
			},
		},
		{
			expected: distSQLBlockers(unsupportedPlanNode | ordinalityProhibited),
			makePlan: func() planNode {
				node := &ordinalityNode{}
				node.Source = &scanBufferNode{}
				return node
			},
		},
		{
			expected: distSQLBlockers(rowLevelLockingProhibited | systemColumnsAndBufferedWritesProhibited),
			makePlan: func() planNode {
				input := &scanNode{}
				input.fetchPlanningInfo.catalogCols = []catalog.Column{mvccCol}
				node := &indexJoinNode{}
				node.Source = input
				node.fetch.lockingStrength = descpb.ScanLockingStrength_FOR_UPDATE
				return node
			},
			txnBufferedWrites: true,
		},
		{
			name:     "mvcc col without buffered writes",
			expected: 0,
			makePlan: func() planNode {
				node := &indexJoinNode{}
				node.Source = &zeroNode{}
				node.fetch.catalogCols = []catalog.Column{mvccCol}
				return node
			},
		},
		{
			expected: distSQLBlockers(funcDistSQLBlocklist | invertedFilterProhibited | unsupportedPlanNode),
			makePlan: func() planNode {
				spanExpr := &inverted.SpanExpression{
					Left: &inverted.SpanExpression{},
				}
				node := &invertedFilterNode{
					InvertedFilterPlanningInfo: invertedFilterPlanningInfo{
						Expression:      spanExpr,
						PreFiltererExpr: funcExpr,
					},
				}
				node.Source = &scanBufferNode{}
				return node
			},
		},
		{
			expected: distSQLBlockers(localityOptimizedOpProhibited),
			makePlan: func() planNode {
				return &scanNode{localityOptimized: true}
			},
		},
		{
			expected: distSQLBlockers(localityOptimizedOpProhibited | unsupportedPlanNode | systemColumnsAndBufferedWritesProhibited),
			makePlan: func() planNode {
				scanInput := &scanNode{
					fetchPlanningInfo: fetchPlanningInfo{
						catalogCols: []catalog.Column{mvccCol},
					},
				}
				ljNode := &lookupJoinNode{
					lookupJoinPlanningInfo: lookupJoinPlanningInfo{
						remoteOnlyLookups: true,
					},
				}
				ljNode.Source = scanInput
				bufNode := &bufferNode{}
				bufNode.Source = ljNode
				return bufNode
			},
			txnBufferedWrites: true,
		},
		{
			expected: distSQLBlockers(valuesNodeProhibited),
			makePlan: func() planNode {
				return &valuesNode{specifiedInQuery: false}
			},
		},
		{
			expected: distSQLBlockers(vectorSearchProhibited),
			makePlan: func() planNode {
				return &vectorSearchNode{}
			},
		},
		{
			expected: distSQLBlockers(vectorSearchProhibited),
			makePlan: func() planNode {
				node := &vectorMutationSearchNode{}
				node.Source = &zeroNode{}
				return node
			},
		},
	} {
		name := tc.name
		if name == "" {
			name = tc.expected.String()
		}
		t.Run(name, func(t *testing.T) {
			node := tc.makePlan()
			_, blockers := checkSupportForPlanNode(ctx, node, &v, sd, tc.txnBufferedWrites)
			require.Equal(t, tc.expected, blockers)
		})
	}
}
