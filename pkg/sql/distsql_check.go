// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// DistributionType is an enum defining when a plan should be distributed.
type DistributionType int

const (
	// LocalDistribution does not distribute a plan across multiple SQL
	// instances.
	LocalDistribution = iota
	// FullDistribution distributes a plan across multiple SQL instances whether
	// it is a system tenant or non-system tenant.
	FullDistribution
)

// distSQLExprCheckVisitor is a tree.Visitor that checks if expressions
// contain things not supported by distSQL, like distSQL-blocklisted functions.
type distSQLExprCheckVisitor struct {
	err error
}

var _ tree.Visitor = &distSQLExprCheckVisitor{}

// NB: when modifying this, consider whether reducedLeafExprVisitor needs to be
// adjusted accordingly.
func (v *distSQLExprCheckVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	if v.err != nil {
		return false, expr
	}
	switch t := expr.(type) {
	case *tree.FuncExpr:
		if t.IsDistSQLBlocklist() {
			v.err = funcBlocklistErr
			return false, expr
		}
	case *tree.RoutineExpr:
		v.err = routineUnsupportedErr
		return false, expr
	case *tree.DOid:
		v.err = oidUnsupportedErr
		return false, expr
	case *tree.Subquery:
		if hasOidType(t.ResolvedType()) {
			// If a subquery results in a DOid datum, the datum will get a type
			// annotation (because DOids are ambiguous) when serializing the
			// render expression involving the result of the subquery. As a
			// result, we might need to perform a cast on a remote node which
			// might fail, thus we prohibit the distribution of the main query.
			v.err = oidUnsupportedErr
			return false, expr
		}
	case *tree.CastExpr:
		// TODO (rohany): I'm not sure why this CastExpr doesn't have a type
		//  annotation at this stage of processing...
		if typ, ok := tree.GetStaticallyKnownType(t.Type); ok {
			switch typ.Family() {
			case types.OidFamily:
				v.err = castToOidUnsupportedErr
				return false, expr
			}
		}
	case *tree.DArray:
		// We need to check for arrays of untyped tuples here since constant-folding
		// on builtin functions sometimes produces this. DecodeUntaggedDatum
		// requires that all the types of the tuple contents are known.
		if t.ResolvedType().ArrayContents().Identical(types.AnyTuple) {
			v.err = arrayOfUntypedTuplesUnsupportedErr
			return false, expr
		}
	case *tree.DTuple:
		if t.ResolvedType().Identical(types.AnyTuple) {
			v.err = untypedTuplesUnsupportedErr
			return false, expr
		}
	case *tree.DJsonpath:
		// TODO(#22513): We currently do not have an encoding for jsonpath
		// thus do not support it within distsql.
		v.err = jsonpathUnsupportedErr
		return false, expr
	}
	return true, expr
}

func (v *distSQLExprCheckVisitor) VisitPost(expr tree.Expr) tree.Expr { return expr }

// hasOidType returns whether t or its contents include an OID type.
func hasOidType(t *types.T) bool {
	switch t.Family() {
	case types.OidFamily:
		return true
	case types.ArrayFamily:
		return hasOidType(t.ArrayContents())
	case types.TupleFamily:
		for _, typ := range t.TupleContents() {
			if hasOidType(typ) {
				return true
			}
		}
	}
	return false
}

// checkExprForDistSQL verifies that an expression doesn't contain things that
// are not yet supported by distSQL, like distSQL-blocklisted functions.
func checkExprForDistSQL(expr tree.Expr, distSQLVisitor *distSQLExprCheckVisitor) error {
	if expr == nil {
		return nil
	}
	distSQLVisitor.err = nil
	tree.WalkExprConst(distSQLVisitor, expr)
	return distSQLVisitor.err
}

type distRecommendation int

const (
	// cannotDistribute indicates that a plan cannot be distributed.
	cannotDistribute distRecommendation = iota

	// canDistribute indicates that a plan can be distributed, but it's not
	// clear whether it'll benefit from that.
	canDistribute

	// shouldDistribute indicates that a plan will likely benefit if distributed.
	shouldDistribute
)

// compose returns the recommendation for a plan given recommendations for two
// parts of it.
func (a distRecommendation) compose(b distRecommendation) distRecommendation {
	if a == cannotDistribute || b == cannotDistribute {
		return cannotDistribute
	}
	if a == shouldDistribute || b == shouldDistribute {
		return shouldDistribute
	}
	return canDistribute
}

type queryNotSupportedError struct {
	msg string
}

func (e *queryNotSupportedError) Error() string {
	return e.msg
}

func newQueryNotSupportedError(msg string) error {
	return &queryNotSupportedError{msg: msg}
}

var (
	funcBlocklistErr                   = newQueryNotSupportedError("function cannot be executed with distsql")
	routineUnsupportedErr              = newQueryNotSupportedError("user-defined routine cannot be executed with distsql")
	oidUnsupportedErr                  = newQueryNotSupportedError("OID expressions are not supported by distsql")
	castToOidUnsupportedErr            = newQueryNotSupportedError("cast to OID is not supported by distsql")
	arrayOfUntypedTuplesUnsupportedErr = newQueryNotSupportedError("array of untyped tuples cannot be executed with distsql")
	untypedTuplesUnsupportedErr        = newQueryNotSupportedError("untyped tuple cannot be executed with distsql")
	jsonpathUnsupportedErr             = newQueryNotSupportedError("jsonpath cannot be executed with distsql")
	// planNodeNotSupportedErr is the catch-all error value returned from
	// checkSupportForPlanNode when a planNode type does not support distributed
	// execution.
	planNodeNotSupportedErr            = newQueryNotSupportedError("unsupported node")
	aggBlocklistErr                    = newQueryNotSupportedError("aggregate cannot be executed with distsql")
	cannotDistributeRowLevelLockingErr = newQueryNotSupportedError(
		"scans with row-level locking are not supported by distsql",
	)
	invertedFilterNotDistributableErr = newQueryNotSupportedError(
		"inverted filter is only distributable when it's a union of spans",
	)
	localityOptimizedOpNotDistributableErr = newQueryNotSupportedError(
		"locality-optimized operation cannot be distributed",
	)
	ordinalityNotDistributableErr = newQueryNotSupportedError(
		"ordinality operation cannot be distributed",
	)
	cannotDistributeVectorSearchErr = newQueryNotSupportedError(
		"vector search operation cannot be distributed",
	)
	cannotDistributeSystemColumnsAndBufferedWrites = newQueryNotSupportedError(
		"system column (that requires MVCC decoding) is requested when writes have been buffered",
	)
	unsupportedValuesNode = newQueryNotSupportedError("unsupported valuesNode, not specified in query")
)

// checkSupportForPlanNode returns a distRecommendation (as described above) or
// cannotDistribute and an error if the plan subtree is not distributable.
// The error doesn't indicate complete failure - it's instead the reason that
// this plan couldn't be distributed.
// TODO(radu): add tests for this.
func checkSupportForPlanNode(
	ctx context.Context,
	node planNode,
	distSQLVisitor *distSQLExprCheckVisitor,
	sd *sessiondata.SessionData,
	txnHasBufferedWrites bool,
) (retRec distRecommendation, retErr error) {
	if buildutil.CrdbTestBuild {
		defer func() {
			if retRec == cannotDistribute && retErr == nil {
				panic(errors.AssertionFailedf("all 'cannotDistribute' recommendations must be accompanied by an error"))
			}
		}()
	}
	switch n := node.(type) {
	// Keep these cases alphabetized, please!
	case *createStatsNode:
		if n.runAsJob {
			return cannotDistribute, planNodeNotSupportedErr
		}
		return shouldDistribute, nil

	case *distinctNode:
		return checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)

	case *exportNode:
		return checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)

	case *filterNode:
		if err := checkExprForDistSQL(n.filter, distSQLVisitor); err != nil {
			return cannotDistribute, err
		}
		return checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)

	case *groupNode:
		rec, err := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)
		if err != nil {
			return cannotDistribute, err
		}
		for _, agg := range n.funcs {
			if agg.distsqlBlocklist {
				return cannotDistribute, aggBlocklistErr
			}
		}
		// Don't force distribution if we expect to process small number of
		// rows.
		aggRec := canDistribute
		if n.estimatedInputRowCount == 0 {
			log.VEventf(ctx, 2, "aggregation (no stats) recommends plan distribution")
			aggRec = shouldDistribute
		} else if n.estimatedInputRowCount >= sd.DistributeGroupByRowCountThreshold {
			log.VEventf(ctx, 2, "large aggregation recommends plan distribution")
			aggRec = shouldDistribute
		}
		return rec.compose(aggRec), nil

	case *indexJoinNode:
		if n.fetch.lockingStrength != descpb.ScanLockingStrength_FOR_NONE {
			// Index joins that are performing row-level locking cannot
			// currently be distributed because their locks would not be
			// propagated back to the root transaction coordinator.
			// TODO(nvanbenschoten): lift this restriction.
			return cannotDistribute, cannotDistributeRowLevelLockingErr
		}
		if txnHasBufferedWrites && n.fetch.requiresMVCCDecoding() {
			// TODO(#144166): relax this.
			return cannotDistribute, cannotDistributeSystemColumnsAndBufferedWrites
		}
		return checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)

	case *invertedFilterNode:
		rec, err := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)
		if err != nil {
			return cannotDistribute, err
		}
		if err := checkExprForDistSQL(n.preFiltererExpr, distSQLVisitor); err != nil {
			return cannotDistribute, err
		}
		// When filtering is a union of inverted spans, it is distributable: place
		// an inverted filterer on each node, which produce the primary keys in
		// arbitrary order, and de-duplicate the PKs at the next stage.
		// The expression is a union of inverted spans iff all the spans have been
		// promoted to FactoredUnionSpans, in which case the Left and Right
		// inverted.Expressions are nil.
		//
		// TODO(sumeer): Even if the filtering cannot be distributed, the
		// placement of the inverted filter could be optimized. Specifically, when
		// the input is a single processor (because the TableReader is reading
		// span(s) that are all on the same node), we can place the inverted
		// filterer on that input node. Currently, this approach fails because we
		// don't know whether the input is a single processor at this stage, and if
		// we blindly returned shouldDistribute, we encounter situations where
		// remote TableReaders are feeding an inverted filterer which runs into an
		// encoding problem with inverted columns. The remote code tries to decode
		// the inverted column as the original type (e.g. for geospatial, tries to
		// decode the int cell-id as a geometry) which obviously fails -- this is
		// related to #50659. Fix this in the distSQLSpecExecFactory.
		if n.expression.Left != nil || n.expression.Right != nil {
			return cannotDistribute, invertedFilterNotDistributableErr
		}
		// TODO(yuzefovich): we might want to be smarter about this and don't force
		// distribution with small inputs.
		log.VEventf(ctx, 2, "inverted filter (union of inverted spans) recommends plan distribution")
		return rec.compose(shouldDistribute), nil

	case *invertedJoinNode:
		if n.fetch.lockingStrength != descpb.ScanLockingStrength_FOR_NONE {
			// Inverted joins that are performing row-level locking cannot
			// currently be distributed because their locks would not be
			// propagated back to the root transaction coordinator.
			// TODO(nvanbenschoten): lift this restriction.
			return cannotDistribute, cannotDistributeRowLevelLockingErr
		}
		if txnHasBufferedWrites && n.fetch.requiresMVCCDecoding() {
			// TODO(#144166): relax this.
			return cannotDistribute, cannotDistributeSystemColumnsAndBufferedWrites
		}
		if err := checkExprForDistSQL(n.onExpr, distSQLVisitor); err != nil {
			return cannotDistribute, err
		}
		rec, err := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)
		if err != nil {
			return cannotDistribute, err
		}
		// TODO(yuzefovich): we might want to be smarter about this and don't
		// force distribution with small inputs.
		log.VEventf(ctx, 2, "inverted join recommends plan distribution")
		return rec.compose(shouldDistribute), nil

	case *joinNode:
		if err := checkExprForDistSQL(n.pred.onCond, distSQLVisitor); err != nil {
			return cannotDistribute, err
		}
		recLeft, err := checkSupportForPlanNode(ctx, n.left, distSQLVisitor, sd, txnHasBufferedWrites)
		if err != nil {
			return cannotDistribute, err
		}
		recRight, err := checkSupportForPlanNode(ctx, n.right, distSQLVisitor, sd, txnHasBufferedWrites)
		if err != nil {
			return cannotDistribute, err
		}
		rec := recLeft.compose(recRight)
		if len(n.pred.leftEqualityIndices) > 0 {
			// We can partition both streams on the equality columns.
			if n.estimatedLeftRowCount == 0 && n.estimatedRightRowCount == 0 {
				log.VEventf(ctx, 2, "join (no stats) recommends plan distribution")
				rec = rec.compose(shouldDistribute)
			} else if n.estimatedLeftRowCount+n.estimatedRightRowCount >= sd.DistributeJoinRowCountThreshold {
				log.VEventf(ctx, 2, "large join recommends plan distribution")
				rec = rec.compose(shouldDistribute)
			}
		}
		return rec, nil

	case *limitNode:
		// Note that we don't need to check whether we support distribution of
		// n.countExpr or n.offsetExpr because those expressions are evaluated
		// locally, during the physical planning.
		return checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)

	case *lookupJoinNode:
		if n.remoteLookupExpr != nil || n.remoteOnlyLookups {
			// This is a locality optimized join.
			return cannotDistribute, localityOptimizedOpNotDistributableErr
		}
		if n.fetch.lockingStrength != descpb.ScanLockingStrength_FOR_NONE {
			// Lookup joins that are performing row-level locking cannot
			// currently be distributed because their locks would not be
			// propagated back to the root transaction coordinator.
			// TODO(nvanbenschoten): lift this restriction.
			return cannotDistribute, cannotDistributeRowLevelLockingErr
		}
		if txnHasBufferedWrites && n.fetch.requiresMVCCDecoding() {
			// TODO(#144166): relax this.
			return cannotDistribute, cannotDistributeSystemColumnsAndBufferedWrites
		}
		if err := checkExprForDistSQL(n.lookupExpr, distSQLVisitor); err != nil {
			return cannotDistribute, err
		}
		if err := checkExprForDistSQL(n.remoteLookupExpr, distSQLVisitor); err != nil {
			return cannotDistribute, err
		}
		if err := checkExprForDistSQL(n.onCond, distSQLVisitor); err != nil {
			return cannotDistribute, err
		}
		rec, err := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)
		if err != nil {
			return cannotDistribute, err
		}
		return rec.compose(canDistribute), nil

	case *ordinalityNode:
		// WITH ORDINALITY never gets distributed so that the gateway node can
		// always number each row in order.
		return cannotDistribute, ordinalityNotDistributableErr

	case *projectSetNode:
		for i := range n.exprs {
			if err := checkExprForDistSQL(n.exprs[i], distSQLVisitor); err != nil {
				return cannotDistribute, err
			}
		}
		return checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)

	case *renderNode:
		for _, e := range n.render {
			if err := checkExprForDistSQL(e, distSQLVisitor); err != nil {
				return cannotDistribute, err
			}
		}
		return checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)

	case *scanNode:
		if n.lockingStrength != descpb.ScanLockingStrength_FOR_NONE {
			// Scans that are performing row-level locking cannot currently be
			// distributed because their locks would not be propagated back to
			// the root transaction coordinator.
			// TODO(nvanbenschoten): lift this restriction.
			return cannotDistribute, cannotDistributeRowLevelLockingErr
		}
		if n.localityOptimized {
			// This is a locality optimized scan.
			return cannotDistribute, localityOptimizedOpNotDistributableErr
		}
		if txnHasBufferedWrites && n.fetchPlanningInfo.requiresMVCCDecoding() {
			// TODO(#144166): relax this.
			return cannotDistribute, cannotDistributeSystemColumnsAndBufferedWrites
		}
		scanRec := canDistribute
		if n.estimatedRowCount != 0 {
			var suffix string
			estimate := n.estimatedRowCount
			if n.softLimit != 0 && sd.UseSoftLimitForDistributeScan {
				estimate = n.softLimit
				suffix = " (using soft limit)"
			}
			if estimate >= sd.DistributeScanRowCountThreshold {
				log.VEventf(ctx, 2, "large scan recommends plan distribution%s", suffix)
				scanRec = shouldDistribute
			} else if n.softLimit != 0 && n.estimatedRowCount >= sd.DistributeScanRowCountThreshold {
				log.VEventf(
					ctx, 2, `estimated row count would consider the scan "large" `+
						`while soft limit hint makes it "small"`,
				)
			}
		}
		if n.isFull && (n.estimatedRowCount == 0 || sd.AlwaysDistributeFullScans) {
			// In the absence of table stats, we default to always distributing
			// full scans.
			log.VEventf(ctx, 2, "full scan recommends plan distribution")
			scanRec = shouldDistribute
		}
		return scanRec, nil

	case *sortNode:
		rec, err := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)
		if err != nil {
			return cannotDistribute, err
		}
		// Don't force distribution if we expect to process small number of
		// rows.
		sortRec := canDistribute
		if n.estimatedInputRowCount == 0 {
			log.VEventf(ctx, 2, "sort (no stats) recommends plan distribution")
			sortRec = shouldDistribute
		} else if n.estimatedInputRowCount >= sd.DistributeSortRowCountThreshold {
			log.VEventf(ctx, 2, "large sort recommends plan distribution")
			sortRec = shouldDistribute
		}
		return rec.compose(sortRec), nil

	case *topKNode:
		rec, err := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)
		if err != nil {
			return cannotDistribute, err
		}
		// Don't force distribution if we expect to process small number of
		// rows.
		topKRec := canDistribute
		if n.estimatedInputRowCount == 0 {
			log.VEventf(ctx, 2, "top k (no stats) recommends plan distribution")
			topKRec = shouldDistribute
		} else if n.estimatedInputRowCount >= sd.DistributeSortRowCountThreshold {
			log.VEventf(ctx, 2, "large top k recommends plan distribution")
			topKRec = shouldDistribute
		}
		return rec.compose(topKRec), nil

	case *unaryNode:
		return canDistribute, nil

	case *unionNode:
		recLeft, err := checkSupportForPlanNode(ctx, n.left, distSQLVisitor, sd, txnHasBufferedWrites)
		if err != nil {
			return cannotDistribute, err
		}
		recRight, err := checkSupportForPlanNode(ctx, n.right, distSQLVisitor, sd, txnHasBufferedWrites)
		if err != nil {
			return cannotDistribute, err
		}
		return recLeft.compose(recRight), nil

	case *valuesNode:
		if !n.specifiedInQuery {
			// This condition indicates that the valuesNode was created by planning,
			// not by the user, like the way vtables are expanded into valuesNodes. We
			// don't want to distribute queries like this across the network.
			return cannotDistribute, unsupportedValuesNode
		}

		for _, tuple := range n.tuples {
			for _, expr := range tuple {
				if err := checkExprForDistSQL(expr, distSQLVisitor); err != nil {
					return cannotDistribute, err
				}
			}
		}
		return canDistribute, nil

	case *vectorSearchNode, *vectorMutationSearchNode:
		// Don't allow distribution for vector search operators, for now.
		// TODO(yuzefovich): if we start distributing plans with these nodes,
		// we'll need to ensure to collect LeafTxnFinalInfo metadata.
		return cannotDistribute, cannotDistributeVectorSearchErr

	case *windowNode:
		rec, err := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)
		if err != nil {
			return cannotDistribute, err
		}
		if len(n.partitionIdxs) > 0 {
			// If the window has a PARTITION BY clause, then we should distribute the
			// execution.
			// TODO(yuzefovich): we might want to be smarter about this and don't
			// force distribution with small inputs.
			log.VEventf(ctx, 2, "window with PARTITION BY recommends plan distribution")
			return rec.compose(shouldDistribute), nil
		}
		return rec.compose(canDistribute), nil

	case *zeroNode:
		return canDistribute, nil

	case *zigzagJoinNode:
		for _, side := range n.sides {
			if side.fetch.lockingStrength != descpb.ScanLockingStrength_FOR_NONE {
				// ZigZag joins that are performing row-level locking cannot
				// currently be distributed because their locks would not be
				// propagated back to the root transaction coordinator.
				// TODO(nvanbenschoten): lift this restriction.
				return cannotDistribute, cannotDistributeRowLevelLockingErr
			}
			if txnHasBufferedWrites && side.fetch.requiresMVCCDecoding() {
				// TODO(#144166): relax this.
				return cannotDistribute, cannotDistributeSystemColumnsAndBufferedWrites
			}
		}
		if err := checkExprForDistSQL(n.onCond, distSQLVisitor); err != nil {
			return cannotDistribute, err
		}
		// TODO(yuzefovich): we might want to be smarter about this and don't
		// force distribution with small inputs.
		log.VEventf(ctx, 2, "zigzag join recommends plan distribution")
		return shouldDistribute, nil

	default:
		return cannotDistribute, planNodeNotSupportedErr
	}
}
