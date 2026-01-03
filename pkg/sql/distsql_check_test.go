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

// TestDistSQLProhibitedCauses verifies that most distSQLProhibitedCauses are
// accumulated correctly in checkSupportForPlanNode. Note that the testing of
// actual physical planner heuristics is done via the logic test framework.
func TestDistSQLProhibitedCauses(t *testing.T) {
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
		expected          distSQLProhibitedCauses
		makePlan          func() planNode
		txnBufferedWrites bool
	}{
		{
			name:     "routine and subquery with oid",
			expected: distSQLProhibitedCauses(routineProhibited) | distSQLProhibitedCauses(oidProhibited),
			makePlan: func() planNode {
				subquery := &tree.Subquery{}
				subquery.SetType(types.Oid)
				return &renderNode{
					singleInputPlanNode: singleInputPlanNode{input: &zeroNode{}},
					render: []tree.TypedExpr{
						&tree.RoutineExpr{},
						subquery,
					},
				}
			},
		},
		{
			expected: distSQLProhibitedCauses(oidProhibited),
			makePlan: func() planNode {
				return &renderNode{
					singleInputPlanNode: singleInputPlanNode{input: &zeroNode{}},
					render:              []tree.TypedExpr{&tree.DOid{}},
				}
			},
		},
		{
			expected: distSQLProhibitedCauses(arrayOfUntypedTuplesProhibited),
			makePlan: func() planNode {
				return &renderNode{
					singleInputPlanNode: singleInputPlanNode{input: &zeroNode{}},
					render: []tree.TypedExpr{
						&tree.DArray{ParamTyp: types.AnyTuple, Array: tree.Datums{}},
					},
				}
			},
		},
		{
			expected: distSQLProhibitedCauses(untypedTupleProhibited),
			makePlan: func() planNode {
				return &renderNode{
					singleInputPlanNode: singleInputPlanNode{input: &zeroNode{}},
					render:              []tree.TypedExpr{tree.NewDTuple(types.AnyTuple)},
				}
			},
		},
		{
			expected: distSQLProhibitedCauses(funcDistSQLBlocklist) | distSQLProhibitedCauses(jsonpathProhibited),
			makePlan: func() planNode {
				return &renderNode{
					singleInputPlanNode: singleInputPlanNode{input: &zeroNode{}},
					render: []tree.TypedExpr{
						funcExpr,
						&tree.DJsonpath{},
					},
				}
			},
		},
		{
			expected: distSQLProhibitedCauses(unsupportedPlanNode) | distSQLProhibitedCauses(ordinalityProhibited),
			makePlan: func() planNode {
				return &ordinalityNode{
					singleInputPlanNode: singleInputPlanNode{input: &scanBufferNode{}},
				}
			},
		},
		{
			expected: distSQLProhibitedCauses(rowLevelLockingProhibited) | distSQLProhibitedCauses(systemColumnsAndBufferedWritesProhibited),
			makePlan: func() planNode {
				input := &scanNode{}
				input.fetchPlanningInfo.catalogCols = []catalog.Column{mvccCol}
				node := &indexJoinNode{
					singleInputPlanNode: singleInputPlanNode{input: input},
				}
				node.fetch.lockingStrength = descpb.ScanLockingStrength_FOR_UPDATE
				return node
			},
			txnBufferedWrites: true,
		},
		{
			name:     "mvcc col without buffered writes",
			expected: 0,
			makePlan: func() planNode {
				node := &indexJoinNode{
					singleInputPlanNode: singleInputPlanNode{input: &zeroNode{}},
				}
				node.fetch.catalogCols = []catalog.Column{mvccCol}
				return node
			},
		},
		{
			expected: distSQLProhibitedCauses(funcDistSQLBlocklist) | distSQLProhibitedCauses(invertedFilterProhibited) | distSQLProhibitedCauses(unsupportedPlanNode),
			makePlan: func() planNode {
				spanExpr := &inverted.SpanExpression{
					Left: &inverted.SpanExpression{},
				}
				return &invertedFilterNode{
					singleInputPlanNode: singleInputPlanNode{input: &scanBufferNode{}},
					invertedFilterPlanningInfo: invertedFilterPlanningInfo{
						expression:      spanExpr,
						preFiltererExpr: funcExpr,
					},
				}
			},
		},
		{
			expected: distSQLProhibitedCauses(localityOptimizedOpProhibited),
			makePlan: func() planNode {
				return &scanNode{localityOptimized: true}
			},
		},
		{
			expected: distSQLProhibitedCauses(localityOptimizedOpProhibited) | distSQLProhibitedCauses(unsupportedPlanNode) | distSQLProhibitedCauses(systemColumnsAndBufferedWritesProhibited),
			makePlan: func() planNode {
				return &bufferNode{
					singleInputPlanNode: singleInputPlanNode{
						input: &lookupJoinNode{
							singleInputPlanNode: singleInputPlanNode{
								input: &scanNode{
									fetchPlanningInfo: fetchPlanningInfo{
										catalogCols: []catalog.Column{mvccCol},
									},
								},
							},
							lookupJoinPlanningInfo: lookupJoinPlanningInfo{
								remoteOnlyLookups: true,
							},
						},
					},
				}
			},
			txnBufferedWrites: true,
		},
		{
			expected: distSQLProhibitedCauses(valuesNodeProhibited),
			makePlan: func() planNode {
				return &valuesNode{specifiedInQuery: false}
			},
		},
		{
			expected: distSQLProhibitedCauses(vectorSearchProhibited),
			makePlan: func() planNode {
				return &vectorSearchNode{}
			},
		},
		{
			expected: distSQLProhibitedCauses(vectorSearchProhibited),
			makePlan: func() planNode {
				return &vectorMutationSearchNode{
					singleInputPlanNode: singleInputPlanNode{input: &zeroNode{}},
				}
			},
		},
	} {
		name := tc.name
		if name == "" {
			name = tc.expected.String()
		}
		t.Run(name, func(t *testing.T) {
			node := tc.makePlan()
			_, causes := checkSupportForPlanNode(ctx, node, &v, sd, tc.txnBufferedWrites)
			require.Equal(t, tc.expected, causes)
		})
	}
}
