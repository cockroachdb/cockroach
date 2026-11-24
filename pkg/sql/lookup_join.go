// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/exec"
	"github.com/cockroachdb/cockroach/pkg/sql/physicalplan"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

type lookupJoinNode struct {
	singleInputPlanNode
	lookupJoinPlanningInfo

	// columns are the produced columns, namely the input columns and (unless the
	// join type is semi or anti join) the columns fetched from the table.
	// It includes an additional continuation column when IsFirstJoinInPairedJoin
	// is true.
	columns colinfo.ResultColumns
}

type lookupJoinPlanningInfo struct {
	fetch fetchPlanningInfo

	// joinType is either INNER, LEFT_OUTER, LEFT_SEMI, or LEFT_ANTI.
	joinType descpb.JoinType

	// eqCols represents the part of the join condition used to perform
	// the lookup into the index. It should only be set when lookupExpr is empty.
	// eqCols identifies the columns from the input which are used for the
	// lookup. These correspond to a prefix of the index columns (of the index we
	// are looking up into).
	eqCols []exec.NodeColumnOrdinal

	// eqColsAreKey is true when each lookup can return at most one row.
	eqColsAreKey bool

	// lookupExpr represents the part of the join condition used to perform
	// the lookup into the index. It should only be set when eqCols is empty.
	// lookupExpr is used instead of eqCols when the lookup condition is
	// more complicated than a simple equality between input columns and index
	// columns. In this case, lookupExpr specifies the expression that will be
	// used to construct the spans for each lookup.
	lookupExpr tree.TypedExpr

	// If remoteLookupExpr is set, this is a locality optimized lookup join. In
	// this case, lookupExpr contains the lookup join conditions targeting ranges
	// located on local nodes (relative to the gateway region), and
	// remoteLookupExpr contains the lookup join conditions targeting remote
	// nodes. The optimizer will only plan a locality optimized lookup join if it
	// is known that each lookup returns at most one row. This fact allows the
	// execution engine to use the local conditions in lookupExpr first, and if a
	// match is found locally for each input row, there is no need to search
	// remote nodes. If a local match is not found for all input rows, the
	// execution engine uses remoteLookupExpr to search remote nodes.
	remoteLookupExpr tree.TypedExpr

	// onCond is any ON condition to be used in conjunction with the implicit
	// equality condition on eqCols or the conditions in lookupExpr.
	onCond tree.TypedExpr

	// At most one of is{First,Second}JoinInPairedJoiner can be true.
	isFirstJoinInPairedJoiner  bool
	isSecondJoinInPairedJoiner bool

	reqOrdering ReqOrdering

	limitHint int64

	// remoteOnlyLookups is true when this join is defined with only lookups
	// that read into remote regions, though the lookups are defined in
	// lookupExpr, not remoteLookupExpr.
	remoteOnlyLookups bool

	// If true, reverseScans indicates that the lookups should use ReverseScan
	// requests instead of Scan requests. This causes lookups *for each input row*
	// to return results in reverse order. This is only useful when each lookup
	// can return more than one row.
	reverseScans bool

	// If set, indicates that the DistSender-level cross-range parallelism
	// should be enabled (which means that the TargetBytes limit cannot be used
	// by the fetcher). The caller is responsible for ensuring this is safe
	// (from OOM perspective). Note that this field has no effect when the
	// Streamer API is used.
	parallelize bool

	// finalizeLastStageCb will be nil in the spec factory.
	finalizeLastStageCb func(*physicalplan.PhysicalPlan)
}

func (lj *lookupJoinNode) startExec(params runParams) error {
	panic("lookupJoinNode cannot be run in local mode")
}

func (lj *lookupJoinNode) Next(params runParams) (bool, error) {
	panic("lookupJoinNode cannot be run in local mode")
}

func (lj *lookupJoinNode) Values() tree.Datums {
	panic("lookupJoinNode cannot be run in local mode")
}

func (lj *lookupJoinNode) Close(ctx context.Context) {
	lj.input.Close(ctx)
}
