// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"
	"strings"

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

// distSQLBlocker is an enum of all reasons for why a query couldn't be
// distributed.
//
// It is separate from distSQLBlockers so that we can generate the stringer for
// each blocker.
type distSQLBlocker uint64

//go:generate stringer -type=distSQLBlocker

const (
	funcDistSQLBlocklist distSQLBlocker = 1 << iota
	routineProhibited
	oidProhibited
	castToOidProhibited
	arrayOfUntypedTuplesProhibited
	untypedTupleProhibited
	jsonpathProhibited
	unsupportedPlanNode
	aggDistSQLBlocklist
	rowLevelLockingProhibited
	invertedFilterProhibited
	localityOptimizedOpProhibited
	ordinalityProhibited
	vectorSearchProhibited
	systemColumnsAndBufferedWritesProhibited
	valuesNodeProhibited
	numDistSQLBlockers
)

// distSQLBlockers is a bit-map describing all reasons for why a query couldn't
// be distributed.
type distSQLBlockers uint64

func (b *distSQLBlockers) addSingle(blocker distSQLBlocker) {
	*b |= distSQLBlockers(blocker)
}

func (b *distSQLBlockers) addMultiple(blockers distSQLBlockers) {
	*b |= blockers
}

func (b distSQLBlockers) String() string {
	if b == 0 {
		return ""
	}
	var sb strings.Builder
	var comma string
	for blocker := distSQLBlocker(1); blocker < numDistSQLBlockers; blocker <<= 1 {
		if b&distSQLBlockers(blocker) != 0 {
			sb.WriteString(comma)
			comma = ", "
			sb.WriteString(blocker.String())
		}
	}
	return sb.String()
}

// distSQLExprCheckVisitor is a tree.Visitor that checks if expressions
// contain things not supported by distSQL, like distSQL-blocklisted functions.
//
// Note that in order to find all blockers, this visitor does _not_
// short-circuit and always fully walks the tree.
type distSQLExprCheckVisitor struct {
	blockers distSQLBlockers
}

var _ tree.Visitor = &distSQLExprCheckVisitor{}

// NB: when modifying this, consider whether reducedLeafExprVisitor needs to be
// adjusted accordingly.
func (v *distSQLExprCheckVisitor) VisitPre(expr tree.Expr) (recurse bool, newExpr tree.Expr) {
	switch t := expr.(type) {
	case *tree.FuncExpr:
		if t.IsDistSQLBlocklist() {
			v.blockers.addSingle(funcDistSQLBlocklist)
		}
	case *tree.RoutineExpr:
		v.blockers.addSingle(routineProhibited)
	case *tree.DOid:
		v.blockers.addSingle(oidProhibited)
	case *tree.Subquery:
		if hasOidType(t.ResolvedType()) {
			// If a subquery results in a DOid datum, the datum will get a type
			// annotation (because DOids are ambiguous) when serializing the
			// render expression involving the result of the subquery. As a
			// result, we might need to perform a cast on a remote node which
			// might fail, thus we prohibit the distribution of the main query.
			v.blockers.addSingle(oidProhibited)
		}
	case *tree.CastExpr:
		// TODO (rohany): I'm not sure why this CastExpr doesn't have a type
		//  annotation at this stage of processing...
		if typ, ok := tree.GetStaticallyKnownType(t.Type); ok {
			switch typ.Family() {
			case types.OidFamily:
				v.blockers.addSingle(castToOidProhibited)
			}
		}
	case *tree.DArray:
		// We need to check for arrays of untyped tuples here since constant-folding
		// on builtin functions sometimes produces this. DecodeUntaggedDatum
		// requires that all the types of the tuple contents are known.
		if t.ResolvedType().ArrayContents().Identical(types.AnyTuple) {
			v.blockers.addSingle(arrayOfUntypedTuplesProhibited)
		}
	case *tree.DTuple:
		if t.ResolvedType().Identical(types.AnyTuple) {
			v.blockers.addSingle(untypedTupleProhibited)
		}
	case *tree.DJsonpath:
		// TODO(#22513): We currently do not have an encoding for jsonpath
		// thus do not support it within distsql.
		v.blockers.addSingle(jsonpathProhibited)
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
// are not yet supported by distSQL, like distSQL-blocklisted functions. Zero
// value indicates that everything is supported.
func checkExprForDistSQL(expr tree.Expr, distSQLVisitor *distSQLExprCheckVisitor) distSQLBlockers {
	if expr == nil {
		return 0
	}
	distSQLVisitor.blockers = 0
	tree.WalkExprConst(distSQLVisitor, expr)
	return distSQLVisitor.blockers
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

// checkSupportForPlanNode returns a distRecommendation (as described above) or
// cannotDistribute and distSQLBlockers if the plan subtree is not
// distributable. The blockers don't indicate complete failure - it's instead
// the reason that this plan couldn't be distributed.
//
// Note that in order to find all blockers that prohibit DistSQL, this method
// does _not_ short-circuit and always fully walks the tree.
func checkSupportForPlanNode(
	ctx context.Context,
	node planNode,
	distSQLVisitor *distSQLExprCheckVisitor,
	sd *sessiondata.SessionData,
	txnHasBufferedWrites bool,
) (retRec distRecommendation, retCauses distSQLBlockers) {
	defer func() {
		// In order to simplify the method, the recommendation returned might be
		// "incomplete" - without fully accounting for DistSQL blockers, so we
		// reconcile that in the defer unconditionally if at least one blocker
		// is present.
		if retCauses != 0 {
			retRec = cannotDistribute
		}
	}()

	switch n := node.(type) {
	// Keep these cases alphabetized, please!
	case *createStatsNode:
		if n.runAsJob {
			return cannotDistribute, distSQLBlockers(unsupportedPlanNode)
		}
		return shouldDistribute, 0

	case *distinctNode:
		return checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)

	case *exportNode:
		return checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)

	case *filterNode:
		rec, blockers := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)
		blockers.addMultiple(checkExprForDistSQL(n.filter, distSQLVisitor))
		return rec, blockers

	case *groupNode:
		rec, blockers := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)
		for _, agg := range n.funcs {
			if agg.distsqlBlocklist {
				blockers.addSingle(aggDistSQLBlocklist)
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
		return rec.compose(aggRec), blockers

	case *indexJoinNode:
		rec, blockers := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)
		if n.fetch.lockingStrength != descpb.ScanLockingStrength_FOR_NONE {
			// Index joins that are performing row-level locking cannot
			// currently be distributed because their locks would not be
			// propagated back to the root transaction coordinator.
			// TODO(nvanbenschoten): lift this restriction.
			blockers.addSingle(rowLevelLockingProhibited)
		}
		if txnHasBufferedWrites && n.fetch.requiresMVCCDecoding() {
			// TODO(#144166): relax this.
			blockers.addSingle(systemColumnsAndBufferedWritesProhibited)
		}
		return rec, blockers

	case *invertedFilterNode:
		rec, blockers := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)
		blockers.addMultiple(checkExprForDistSQL(n.preFiltererExpr, distSQLVisitor))
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
			blockers.addSingle(invertedFilterProhibited)
		}
		// TODO(yuzefovich): we might want to be smarter about this and don't force
		// distribution with small inputs.
		log.VEventf(ctx, 2, "inverted filter (union of inverted spans) recommends plan distribution")
		return rec.compose(shouldDistribute), blockers

	case *invertedJoinNode:
		rec, blockers := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)
		if n.fetch.lockingStrength != descpb.ScanLockingStrength_FOR_NONE {
			// Inverted joins that are performing row-level locking cannot
			// currently be distributed because their locks would not be
			// propagated back to the root transaction coordinator.
			// TODO(nvanbenschoten): lift this restriction.
			blockers.addSingle(rowLevelLockingProhibited)
		}
		if txnHasBufferedWrites && n.fetch.requiresMVCCDecoding() {
			// TODO(#144166): relax this.
			blockers.addSingle(systemColumnsAndBufferedWritesProhibited)
		}
		blockers.addMultiple(checkExprForDistSQL(n.onExpr, distSQLVisitor))
		// TODO(yuzefovich): we might want to be smarter about this and don't
		// force distribution with small inputs.
		log.VEventf(ctx, 2, "inverted join recommends plan distribution")
		return rec.compose(shouldDistribute), blockers

	case *joinNode:
		rec, blockers := checkSupportForPlanNode(ctx, n.left, distSQLVisitor, sd, txnHasBufferedWrites)
		blockers.addMultiple(checkExprForDistSQL(n.pred.onCond, distSQLVisitor))
		recRight, blockersRight := checkSupportForPlanNode(ctx, n.right, distSQLVisitor, sd, txnHasBufferedWrites)
		blockers.addMultiple(blockersRight)
		rec = rec.compose(recRight)
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
		return rec, blockers

	case *limitNode:
		// Note that we don't need to check whether we support distribution of
		// n.countExpr or n.offsetExpr because those expressions are evaluated
		// locally, during the physical planning.
		return checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)

	case *lookupJoinNode:
		rec, blockers := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)
		if n.remoteLookupExpr != nil || n.remoteOnlyLookups {
			// This is a locality optimized join.
			blockers.addSingle(localityOptimizedOpProhibited)
		}
		if n.fetch.lockingStrength != descpb.ScanLockingStrength_FOR_NONE {
			// Lookup joins that are performing row-level locking cannot
			// currently be distributed because their locks would not be
			// propagated back to the root transaction coordinator.
			// TODO(nvanbenschoten): lift this restriction.
			blockers.addSingle(rowLevelLockingProhibited)
		}
		if txnHasBufferedWrites && n.fetch.requiresMVCCDecoding() {
			// TODO(#144166): relax this.
			blockers.addSingle(systemColumnsAndBufferedWritesProhibited)
		}
		for _, expr := range []tree.TypedExpr{
			n.lookupExpr,
			n.remoteLookupExpr,
			n.onCond,
		} {
			blockers.addMultiple(checkExprForDistSQL(expr, distSQLVisitor))
		}
		return rec.compose(canDistribute), blockers

	case *ordinalityNode:
		_, blockers := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)
		// WITH ORDINALITY never gets distributed so that the gateway node can
		// always number each row in order.
		blockers.addSingle(ordinalityProhibited)
		return cannotDistribute, blockers

	case *projectSetNode:
		rec, blockers := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)
		for i := range n.exprs {
			blockers.addMultiple(checkExprForDistSQL(n.exprs[i], distSQLVisitor))
		}
		return rec, blockers

	case *renderNode:
		rec, blockers := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)
		for _, e := range n.render {
			blockers.addMultiple(checkExprForDistSQL(e, distSQLVisitor))
		}
		return rec, blockers

	case *scanNode:
		rec, blockers := canDistribute, distSQLBlockers(0)
		if n.lockingStrength != descpb.ScanLockingStrength_FOR_NONE {
			// Scans that are performing row-level locking cannot currently be
			// distributed because their locks would not be propagated back to
			// the root transaction coordinator.
			// TODO(nvanbenschoten): lift this restriction.
			blockers.addSingle(rowLevelLockingProhibited)
		}
		if n.localityOptimized {
			// This is a locality optimized scan.
			blockers.addSingle(localityOptimizedOpProhibited)
		}
		if txnHasBufferedWrites && n.fetchPlanningInfo.requiresMVCCDecoding() {
			// TODO(#144166): relax this.
			blockers.addSingle(systemColumnsAndBufferedWritesProhibited)
		}
		if n.estimatedRowCount != 0 {
			var suffix string
			estimate := n.estimatedRowCount
			if n.softLimit != 0 && sd.UseSoftLimitForDistributeScan {
				estimate = n.softLimit
				suffix = " (using soft limit)"
			}
			if estimate >= sd.DistributeScanRowCountThreshold {
				log.VEventf(ctx, 2, "large scan recommends plan distribution%s", suffix)
				rec = shouldDistribute
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
			rec = shouldDistribute
		}
		return rec, blockers

	case *sortNode:
		rec, blockers := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)
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
		return rec.compose(sortRec), blockers

	case *topKNode:
		rec, blockers := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)
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
		return rec.compose(topKRec), blockers

	case *unaryNode:
		return canDistribute, 0

	case *unionNode:
		rec, blockers := checkSupportForPlanNode(ctx, n.left, distSQLVisitor, sd, txnHasBufferedWrites)
		recRight, blockersRight := checkSupportForPlanNode(ctx, n.right, distSQLVisitor, sd, txnHasBufferedWrites)
		blockers.addMultiple(blockersRight)
		return rec.compose(recRight), blockers

	case *valuesNode:
		rec, blockers := canDistribute, distSQLBlockers(0)
		if !n.specifiedInQuery {
			// This condition indicates that the valuesNode was created by planning,
			// not by the user, like the way vtables are expanded into valuesNodes. We
			// don't want to distribute queries like this across the network.
			blockers.addSingle(valuesNodeProhibited)
		}
		for _, tuple := range n.tuples {
			for _, expr := range tuple {
				blockers.addMultiple(checkExprForDistSQL(expr, distSQLVisitor))
			}
		}
		return rec, blockers

	case *vectorMutationSearchNode:
		// Don't allow distribution for vector search operators, for now.
		// TODO(yuzefovich): if we start distributing plans with these nodes,
		// we'll need to ensure to collect LeafTxnFinalInfo metadata.
		_, blockers := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)
		blockers.addSingle(vectorSearchProhibited)
		return cannotDistribute, blockers

	case *vectorSearchNode:
		// Don't allow distribution for vector search operators, for now.
		// TODO(yuzefovich): if we start distributing plans with these nodes,
		// we'll need to ensure to collect LeafTxnFinalInfo metadata.
		return cannotDistribute, distSQLBlockers(vectorSearchProhibited)

	case *windowNode:
		rec, blockers := checkSupportForPlanNode(ctx, n.input, distSQLVisitor, sd, txnHasBufferedWrites)
		windowRec := canDistribute
		if len(n.partitionIdxs) > 0 {
			// If the window has a PARTITION BY clause, then we should distribute the
			// execution.
			// TODO(yuzefovich): we might want to be smarter about this and don't
			// force distribution with small inputs.
			log.VEventf(ctx, 2, "window with PARTITION BY recommends plan distribution")
			windowRec = shouldDistribute
		}
		return rec.compose(windowRec), blockers

	case *zeroNode:
		return canDistribute, 0

	case *zigzagJoinNode:
		// TODO(yuzefovich): we might want to be smarter about this and don't
		// force distribution with small inputs.
		log.VEventf(ctx, 2, "zigzag join recommends plan distribution")
		rec, blockers := shouldDistribute, distSQLBlockers(0)
		for _, side := range n.sides {
			if side.fetch.lockingStrength != descpb.ScanLockingStrength_FOR_NONE {
				// ZigZag joins that are performing row-level locking cannot
				// currently be distributed because their locks would not be
				// propagated back to the root transaction coordinator.
				// TODO(nvanbenschoten): lift this restriction.
				blockers.addSingle(rowLevelLockingProhibited)
			}
			if txnHasBufferedWrites && side.fetch.requiresMVCCDecoding() {
				// TODO(#144166): relax this.
				blockers.addSingle(systemColumnsAndBufferedWritesProhibited)
			}
		}
		blockers.addMultiple(checkExprForDistSQL(n.onCond, distSQLVisitor))
		return rec, blockers

	default:
		switch n.InputCount() {
		case 0:
			return cannotDistribute, distSQLBlockers(unsupportedPlanNode)
		case 1:
			input, err := n.Input(0)
			if buildutil.CrdbTestBuild && err != nil {
				panic(err)
			}
			_, blockers := checkSupportForPlanNode(ctx, input, distSQLVisitor, sd, txnHasBufferedWrites)
			blockers.addSingle(unsupportedPlanNode)
			return cannotDistribute, blockers
		default:
			if buildutil.CrdbTestBuild {
				// All (exactly two of them) multi-input planNodes are handled
				// above.
				panic(errors.AssertionFailedf("unexpected multi-input planNode %T", n))
			}
			return cannotDistribute, distSQLBlockers(unsupportedPlanNode)
		}
	}
}
