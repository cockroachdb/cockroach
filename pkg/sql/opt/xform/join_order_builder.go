// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package xform

import (
	"math"
	"math/bits"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/errors"
)

// OnReorderFunc defines the callback function for the NotifyOnReorder
// event supported by the optimizer and factory. OnReorderFunc is called once
// the join graph has been assembled. The callback parameters give the root join
// as well as the vertexes and edges of the join graph.
type OnReorderFunc func(
	join memo.RelExpr,
	vertexes []memo.RelExpr,
	edges []memo.FiltersExpr,
	edgeOps []opt.Operator,
)

// OnAddJoinFunc defines the callback function for the NotifyOnAddJoin event
// supported by JoinOrderBuilder. OnAddJoinFunc is called when JoinOrderBuilder
// attempts to add a join to the memo via addJoin. The callback parameters give
// the base relations of the left and right inputs of the join, the set of all
// base relations currently being considered, the base relations referenced by
// the join's ON condition, and the type of join.
type OnAddJoinFunc func(left, right, all, refs []memo.RelExpr, op opt.Operator)

// JoinOrderBuilder is used to add valid orderings of a given join tree to the
// memo during exploration.
//
// Motivation
// ----------
//
// For any given join tree, it is possible to enumerate all possible orderings
// through exhaustive application of transformation rules. However, enumerating
// join orderings this way leads to a large number of duplicates, which in turn
// significantly impacts planning time. Ideally, each unique join ordering would
// only be enumerated once.
//
// In addition, in the vast majority of cases, the optimal plan for a query does
// not involve introducing cross joins that were not present in the original
// version of the plan. This is because cross joins produce |m| x |n| output
// tuples, where |m| is left input cardinality and |n| is right input
// cardinality. With a query like this:
//
//    SELECT *
//    FROM (SELECT * FROM xy INNER JOIN ab ON x = a)
//    INNER JOIN uv ON x = u
//
// An ordering like the following is valid but not desirable, since the cross
// join will likely be very expensive compared to a join with a predicate:
//
//    SELECT *
//    FROM (SELECT * FROM uv INNER JOIN ab ON True)
//    INNER JOIN xy ON x = a AND x = u
//
// Avoiding cross joins significantly decreases the search space (and therefore
// planning time) without preventing the best plan from being found in most
// cases.
//
// Finally, join trees that incorporate non-inner joins should be handled as
// well as inner join trees. This is non-trivial, because non-inner joins are
// much more restrictive than inner joins. For example, the associative property
// does not apply to an inner join in the right input of a left join. Any join
// enumeration method must take care not to introduce invalid orderings.
//
// How JoinOrderBuilder works
// --------------------------
//
// First, the initial join tree is traversed and encoded as a hypergraph (which
// is a graph in which any edge can reference two or more vertexes). The
// 'leaves' of the join tree (base relations) become the vertexes of the graph,
// and the edges are built using the ON conditions of the joins.
//
// Taking this query as an example:
//
//    SELECT *
//    FROM (SELECT * FROM xy LEFT JOIN ab ON x = a)
//    INNER JOIN uv ON x = u AND (y = b OR b IS NULL)
//
// The vertexes of the graph would represent the base relations xy, ab and uv.
// The three edges would be:
//
//   x = a [left]
//   x = u [inner]
//   y = b OR b IS NULL [inner]
//
// Then, the DPSube algorithm is executed (see citations: [8]). DPSube
// enumerates all disjoint pairs of subsets of base relations such as
// ({xy}, {uv}) or ({xy, uv}, {cd}). This is done efficiently using bit sets.
// For each pair of relation subsets, the edges are iterated over. For each
// edge, if certain edge conditions (to be described in the next section) are
// satisfied, a new join is created using the filters from that edge and the
// relation subsets as inputs.
//
// Avoiding invalid orderings
// --------------------------
//
// Earlier, it was mentioned that edges are restricted in their ability to form
// new joins. How is this accomplished?
//
// The paper 'On the correct and complete enumeration of the core search space'
// introduces the concept of a total eligibility set (TES) (citations: [8]). The
// TES is an extension of the syntactic eligibility set (SES). The SES models
// the producer-consumer relationship of joins and base relations; any relations
// contained in the SES of a join must be present in the join's input. For
// example, take the following query:
//
//    SELECT *
//    FROM xy
//    LEFT JOIN (SELECT * FROM ab INNER JOIN uv ON a = u)
//    ON x = u
//
// The SES for the left join will contain relations xy and uv because both are
// referenced by the join's predicate. Therefore, both must be in the input of
// this join for any ordering to be valid.
//
// The TES of an edge is initialized with the SES, and then expanded during
// execution of the CD-C algorithm (see citations: [8] section 5.4). For each
// 'child' join under the current join, associative, left-asscom and
// right-asscom properties are provided by lookup tables (the asscom properties
// are derived from a combination of association and commutation). Depending on
// these properties, the TES will be expanded to take into account whether
// certain transformations of the join tree are valid. During execution of the
// DPSube algorithm, the TES is used to decide whether a given edge can be used
// to construct a new join operator.
//
// Consider the following (invalid) reordering of the above example):
//
//    SELECT *
//    FROM ab
//    INNER JOIN (SELECT * FROM xy LEFT JOIN uv ON x = u)
//    ON a = u
//
// The left join's TES will include relations xy and uv because they are in the
// SES. The TES will also contain ab because the right-asscom property does not
// hold for a left join and an inner join. Violation of the right-asscom
// property in this context means that the xy and ab relations cannot switch
// places. Because all relations in the join's TES must be a part of its inputs,
// ab cannot be pulled out of the left join. This prevents the invalid plan from
// being considered.
//
// In addition to the TES, 'conflict rules' are also required to detect invalid
// plans. For details, see the methods: calculateTES, addJoins,
// checkNonInnerJoin, and checkInnerJoin.
//
// Special handling of inner joins
// -------------------------------
//
// In general, a join's ON condition must be treated as a single entity, because
// join filter conjuncts cannot usually be pulled out of (or pushed down from)
// the ON condition. However, this restriction can be relaxed for inner joins
// because inner join trees have a unique property: they can be modeled as a
// series of cross joins followed by a series of selects with the inner join
// conjuncts. This allows inner join conjuncts to be treated as 'detached' from
// their original operator, free to be combined with conjuncts from other inner
// joins. For example, take this query:
//
//    SELECT *
//    FROM (SELECT * FROM xy INNER JOIN ab ON x = a)
//    INNER JOIN uv ON x = u AND a = u
//
// Treating the ON conditions of these joins as a conglomerate (as we do with
// non-inner joins), a join between base relations xy and uv would not be
// possible, because the a = u conjunct requires that the ab base relation also
// be under that edge. However, creating separate edges for each inner join
// conjunct solves this problem, allowing a reordering like the following
// (the ab and uv relations are switched, along with the filters):
//
//    SELECT *
//    FROM (SELECT * FROM xy INNER JOIN uv ON x = u)
//    INNER JOIN ab ON x = a AND a = u
//
// In fact, this idea can be taken even further. Take this query as an example:
//
//    SELECT *
//    FROM xy
//    INNER JOIN (SELECT * FROM ab LEFT JOIN uv ON b = v)
//    ON x = a AND (y = u OR u IS NULL)
//
// The following is a valid reformulation:
//
//    SELECT *
//    FROM (SELECT * FROM xy INNER JOIN ab ON x = a)
//    LEFT JOIN uv ON b = v
//    WHERE y = u OR u IS NULL
//
// Notice the new Select operation that now carries the inner join conjunct that
// references the right side of the left join. We can model the process that
// leads to this reformulation as follows:
//   1. The inner join is rewritten as a cross join and two selects, each
//      carrying a conjunct: (x = a) for one and (y = u OR u IS NULL) for the
//      other.
//   2. The Select operators are pulled above the inner join.
//   3. The left join and inner join are reordered according to the associative
//      property (see citations: [8] table 2).
//   4. Finally, the inner join conjuncts are pushed back down the reordered
//      join tree as far as possible. The x = a conjunct can be pushed to the
//      inner join, but the (y = u OR u IS NULL) conjunct must remain on the
//      Select.
// JoinOrderBuilder is able to effect this transformation (though it is not
// accomplished in so many steps).
//
// Note that it would be correct to handle inner joins in the same way as
// non-inner joins, by never splitting up predicates. However, this would
// diminish the search space for plans involving inner joins, and in many cases
// prevent the best plan from being found. It is for this reason that inner
// joins are handled separately.
//
// Also note that this approach to handling inner joins is not discussed in [8].
// Rather, it is an extension of the ideas of [8] motivated by the fact that the
// best plans for many real-world queries require inner joins to be handled in
// this way.
//
// Transitive closure
// ------------------
//
// Treating inner join conjuncts as separate edges allows yet another addition:
// we can add new edges that are implied by the transitive closure of the inner
// join edges. For example, take this query:
//
//    SELECT * FROM xy
//    INNER JOIN ab ON x = a
//    INNER JOIN uv ON a = u
//
// The two edges x = a and a = u are explicit in this join tree. However, there
// is the additional implicit edge x = u which can be added to the join graph.
// Adding this edge allows xy to be joined to uv without introducing a cross
// join. This step of ensuring transitive closure is often crucial to finding
// the best plan; for example, the plan for TPC-H query 9 is much slower without
// it (try commenting out the call to ensureClosure()).
//
// Citations: [8]
type JoinOrderBuilder struct {
	f       *norm.Factory
	evalCtx *tree.EvalContext

	// vertexes is the set of base relations that form the vertexes of the join
	// graph. Any RelExpr can be a vertex, including a join (e.g. in case where
	// join has flags that prevent reordering).
	vertexes []memo.RelExpr

	// edges is the set of all edges of the join graph, including both inner and
	// non-inner join edges. As noted in the struct comment, each conjunct in an
	// inner join predicate becomes its own edge. By contrast, the entire
	// predicate becomes a single edge in the non-inner join case.
	edges []edge

	// innerEdges is the set of edges which were constructed using the ON
	// condition of an inner join, represented as indexes into the 'edges' slice.
	// It is useful to keep them separate because inner edges are permissive of
	// more orderings than non-inner edges. One or more edges can be formed from
	// any given inner join.
	innerEdges edgeSet

	// nonInnerEdges is the set of edges which were constructed using the ON
	// condition of a non-inner join, represented as indexes into the 'edges'
	// slice. There is a one-to-one correspondence between non-inner joins and
	// their respective edges.
	nonInnerEdges edgeSet

	// plans maps from a set of base relations to the memo group for the join tree
	// that contains those relations (and only those relations). As an example,
	// the group for [xy, ab, uv] might contain the join trees (xy, (ab, uv)),
	// ((xy, ab), uv), (ab, (xy, uv)), etc.
	//
	// The group for a single base relation is simply the base relation itself.
	plans map[vertexSet]memo.RelExpr

	// joinCount counts the number of joins that have been added to the join
	// graph. It is used to ensure that the number of joins that are reordered at
	// once does not exceed the session limit.
	joinCount int

	onReorderFunc OnReorderFunc

	onAddJoinFunc OnAddJoinFunc
}

// Init initializes a new JoinOrderBuilder with the given factory. The join
// graph is reset, so a JoinOrderBuilder can be reused. Callback functions are
// not reset.
func (jb *JoinOrderBuilder) Init(f *norm.Factory, evalCtx *tree.EvalContext) {
	// This initialization pattern ensures that fields are not unwittingly
	// reused. Field reuse must be explicit.
	*jb = JoinOrderBuilder{
		f:             f,
		evalCtx:       evalCtx,
		plans:         make(map[vertexSet]memo.RelExpr),
		onReorderFunc: jb.onReorderFunc,
		onAddJoinFunc: jb.onAddJoinFunc,
	}
}

// Reorder adds all valid orderings of the given join to the memo.
func (jb *JoinOrderBuilder) Reorder(join memo.RelExpr) {
	switch t := join.(type) {
	case *memo.InnerJoinExpr, *memo.SemiJoinExpr, *memo.AntiJoinExpr,
		*memo.LeftJoinExpr, *memo.FullJoinExpr:
		flags := join.Private().(*memo.JoinPrivate).Flags
		if !flags.Empty() {
			panic(errors.AssertionFailedf("join with hints cannot be reordered"))
		}

		// Populate the vertexes and edges of the join hypergraph.
		jb.populateGraph(join)

		// Ensure equivalence closure for the edges. This can be crucial for finding
		// the best plan.
		jb.ensureClosure(join)

		if jb.onReorderFunc != nil {
			// Hook for testing purposes.
			jb.callOnReorderFunc(join)
		}

		// Execute the DPSube algorithm. Enumerate all join orderings and add any
		// valid ones to the memo.
		jb.dpSube()

	default:
		panic(errors.AssertionFailedf("%v cannot be reordered", t.Op()))
	}
}

// populateGraph traverses the given subtree up to ReorderJoinsLimit and
// initializes the vertexes and edges of the join hypergraph. populateGraph
// returns the sets of vertexes and edges that were added to the graph during
// traversal of the subtree.
func (jb *JoinOrderBuilder) populateGraph(rel memo.RelExpr) (vertexSet, edgeSet) {
	// Remember starting set of vertexes and edges so that the vertexes and
	// edges added during the traversal of the tree rooted at this node can be
	// determined by checking the set difference.
	startVertexes := jb.allVertexes()
	startEdges := jb.allEdges()

	switch t := rel.(type) {
	case *memo.InnerJoinExpr, *memo.SemiJoinExpr, *memo.AntiJoinExpr,
		*memo.LeftJoinExpr, *memo.FullJoinExpr:
		jb.joinCount++

		flags := t.Private().(*memo.JoinPrivate).Flags
		if !flags.Empty() || jb.joinCount > jb.evalCtx.SessionData.ReorderJoinsLimit {
			// If the join has flags or the join limit has been reached, we can't
			// reorder. Simply treat the join as a base relation.
			jb.addBaseRelation(t)
			break
		}

		left := t.Child(0).(memo.RelExpr)
		right := t.Child(1).(memo.RelExpr)
		on := *t.Child(2).(*memo.FiltersExpr)

		// Traverse the left input of the join, initializing the join graph in the
		// process.
		leftVertexes, leftEdges := jb.populateGraph(left)

		// Traverse the right input of the join, initializing the join graph in the
		// process.
		rightVertexes, rightEdges := jb.populateGraph(right)

		// Construct an operator that will be referred to during TES calculation by
		// any joins higher up in the join tree. A pointer to the operator will be
		// stored on the edge(s) that are constructed with it.
		op := &operator{
			joinType:      t.Op(),
			leftVertexes:  leftVertexes,
			rightVertexes: rightVertexes,
			leftEdges:     leftEdges,
			rightEdges:    rightEdges,
		}

		// Create hyperedges for the join graph using this join's ON condition.
		if t.Op() == opt.InnerJoinOp {
			jb.makeInnerEdge(op, on)
		} else {
			jb.makeNonInnerEdge(op, on)
		}

		// Initialize the plan for this join. This allows any new joins with the
		// same set of input relations to be added to the same memo group.
		jb.plans[leftVertexes.union(rightVertexes)] = t

	default:
		jb.addBaseRelation(t)
	}

	// Use set difference operations to return all vertexes and edges added to the
	// graph during traversal of this subtree.
	return jb.allVertexes().difference(startVertexes), jb.allEdges().Difference(startEdges)
}

// ensureClosure ensures that the edges considered during join reordering
// reflect the transitive closure of all equality filters between columns.
// As an example, take a query like the following:
//
//    SELECT * FROM xy INNER JOIN ab ON x = a INNER JOIN uv ON u = a
//
// Contains the explicit edges x = a and u = a, and the implicit edge x = u.
// This implicit edge will be added by ensureClosure.
func (jb *JoinOrderBuilder) ensureClosure(join memo.RelExpr) {
	// Use the equivalencies of the root join to ensure transitive closure.
	equivFDs := &join.Relational().FuncDeps

	// Enumerate all distinct pairs of equivalent columns.
	reps := equivFDs.EquivReps()
	for col, ok := reps.Next(0); ok; col, ok = reps.Next(col + 1) {
		// Get all columns which are known to be equal to this column.
		equivGroup := equivFDs.ComputeEquivGroup(col)

		// Ensure that there exists an edge for each distinct pair of equivalent
		// columns.
		for col1, ok1 := equivGroup.Next(0); ok1; col1, ok1 = equivGroup.Next(col1 + 1) {
			for col2, ok2 := equivGroup.Next(col1 + 1); ok2; col2, ok2 = equivGroup.Next(col2 + 1) {
				if !jb.hasEqEdge(col1, col2) {
					// This equality is not yet represented among the edges.
					jb.makeTransitiveEdge(col1, col2)
				}
			}
		}
	}
}

// dpSube carries out the DPSube algorithm (citations: [8] figure 4). All
// disjoint pairs of subsets of base relations are enumerated and checked for
// validity. If valid, the pair of subsets is used along with the edges
// connecting them to create a new join operator, which is added to the memo.
// TODO(drewk): implement DPHyp (or a similar algorithm).
func (jb *JoinOrderBuilder) dpSube() {
	subsets := jb.allVertexes()
	for subset := vertexSet(1); subset <= subsets; subset++ {
		if subset.isSingleton() {
			// This subset has only one set bit, which means it only represents one
			// relation. We need at least two relations in order to create a new join.
			continue
		}
		// Enumerate all possible pairwise-disjoint binary partitions of the subset,
		// s1 AND s2. These represent sets of relations that may be joined together.
		//
		// Only iterate s1 up to subset/2 to avoid enumerating duplicate partitions.
		// This works because s1 and s2 are always disjoint, and subset will always
		// be equal to s1 + s2. Therefore, any pair of subsets where s1 > s2 will
		// already have been handled when s2 < s1. Also note that for subset = 1111
		// (in binary), subset / 2 = 0111 (integer division).
		for s1 := vertexSet(1); s1 <= subset/2; s1++ {
			if !s1.isSubsetOf(subset) {
				continue
			}
			s2 := subset.difference(s1)
			jb.addJoins(s1, s2)
		}
	}
}

// addJoins iterates through the edges of the join graph and checks whether any
// joins can be constructed between the memo groups for the two given sets of
// base relations without creating an invalid plan or introducing cross joins.
// If any valid joins are found, they are added to the memo.
func (jb *JoinOrderBuilder) addJoins(s1, s2 vertexSet) {
	if jb.plans[s1] == nil || jb.plans[s2] == nil {
		// Both inputs must have plans.
		return
	}

	var fds props.FuncDepSet
	fds.AddEquivFrom(&jb.plans[s1].Relational().FuncDeps)
	fds.AddEquivFrom(&jb.plans[s2].Relational().FuncDeps)

	// Gather all inner edges that connect the left and right relation sets.
	var innerJoinFilters memo.FiltersExpr
	var addInnerJoin bool
	var joinIsRedundant bool
	for i, ok := jb.innerEdges.Next(0); ok; i, ok = jb.innerEdges.Next(i + 1) {
		e := &jb.edges[i]

		// Ensure that this edge forms a valid connection between the two sets. See
		// the checkNonInnerJoin and checkInnerJoin comments for more information.
		if e.checkInnerJoin(s1, s2) {
			if areFiltersRedundant(&fds, e.filters) {
				// Avoid adding redundant filters.
				continue
			}
			if !joinIsRedundant {
				// If this edge was originally part of a join between relation sets s1 and
				// s2, any other edges that apply will also be part of that original join.
				joinIsRedundant = e.joinIsRedundant(s1, s2)
			}
			getEquivFDs(&fds, e.filters)
			innerJoinFilters = append(innerJoinFilters, e.filters...)
			addInnerJoin = true
		}
	}

	// Iterate through the non-inner edges and attempt to construct joins from
	// them.
	for i, ok := jb.nonInnerEdges.Next(0); ok; i, ok = jb.nonInnerEdges.Next(i + 1) {
		e := &jb.edges[i]

		// Ensure that this edge forms a valid connection between the two sets. See
		// the checkNonInnerJoin and checkInnerJoin comments for more information.
		if e.checkNonInnerJoin(s1, s2) {
			// Construct a non-inner join. If any inner join filters also apply to the
			// pair of relationSets, construct a select on top of the join with the
			// inner join filters.
			jb.addJoin(e.op.joinType, s1, s2, e.filters, innerJoinFilters, e.joinIsRedundant(s1, s2))
			return
		}
		if e.checkNonInnerJoin(s2, s1) {
			// If joining s1, s2 is not valid, try s2, s1. We only do this if the
			// s1, s2 join fails, because commutation is handled by the addJoin
			// function. This is necessary because we only iterate s1 up to subset / 2
			// in DPSube(). Take this transformation as an example:
			//
			//    SELECT *
			//    FROM (SELECT * FROM xy LEFT JOIN ab ON x = a)
			//    INNER JOIN uv ON x = u
			//    =>
			//    SELECT *
			//    FROM (SELECT * FROM xy INNER JOIN uv ON x = u)
			//    LEFT JOIN ab ON x = a
			//
			// Bitset encodings for the base relations:
			// xy: 001
			// ab: 010
			// uv: 100
			//
			// The left join in the new plan is between set 101 on the left, and set
			// 010 on the right. 101 is larger than 111 / 2, so we will not enumerate
			// this plan unless we consider a join with s2 on the left and s1 on the
			// right.
			jb.addJoin(e.op.joinType, s2, s1, e.filters, innerJoinFilters, e.joinIsRedundant(s2, s1))
			return
		}
	}

	if addInnerJoin {
		// Construct an inner join. Don't add in the case when a non-inner join has
		// already been constructed, because doing so can lead to a case where a
		// non-inner join operator 'disappears' because an inner join has replaced
		// it.
		jb.addJoin(opt.InnerJoinOp, s1, s2, innerJoinFilters, nil /* selectFilters */, joinIsRedundant)
	}
}

// makeInnerEdge constructs edges from the ON condition of an inner join. If the
// inner join has one or more conjuncts, an edge is created for each conjunct.
// If the inner join has no filters, an edge with an empty SES is created.
func (jb *JoinOrderBuilder) makeInnerEdge(op *operator, filters memo.FiltersExpr) {
	if len(filters) == 0 {
		// This is a cross join. Create a single edge for the empty FiltersExpr.
		jb.edges = append(jb.edges, *jb.makeEdge(op, filters))
		jb.innerEdges.Add(len(jb.edges) - 1)
		return
	}
	for i := range filters {
		// Create an edge for each conjunct.
		jb.edges = append(jb.edges, *jb.makeEdge(op, filters[i:i+1]))
		jb.innerEdges.Add(len(jb.edges) - 1)
	}
}

// makeNonInnerEdge constructs an edge from the ON condition of a non-inner
// join. For any given non-inner join, exactly one edge is constructed.
func (jb *JoinOrderBuilder) makeNonInnerEdge(op *operator, filters memo.FiltersExpr) {
	// Always create a single edge from a non-inner join.
	jb.edges = append(jb.edges, *jb.makeEdge(op, filters))
	jb.nonInnerEdges.Add(len(jb.edges) - 1)
}

// makeTransitiveEdge constructs an edge using an equality between two columns
// that results from calculating transitive closure over the edges of the join
// graph.
func (jb *JoinOrderBuilder) makeTransitiveEdge(col1, col2 opt.ColumnID) {
	var op *operator

	// Find the inner join down to which this filter would be pushed if it had
	// been made explicit in the original query. This operator will be used to
	// construct the TES for this edge. Note that this is possible because each
	// edge contains a record of all base relations that were in the left and
	// right inputs of the original join operators from which they were formed.
	relations := jb.getRelations(opt.MakeColSet(col1, col2))
	for i, ok := jb.innerEdges.Next(0); ok; i, ok = jb.innerEdges.Next(i + 1) {
		currEdge := &jb.edges[i]
		if relations.isSubsetOf(currEdge.op.leftVertexes.union(currEdge.op.rightVertexes)) &&
			relations.intersects(currEdge.op.leftVertexes) &&
			relations.intersects(currEdge.op.rightVertexes) {
			op = currEdge.op
			break
		}
	}
	if op == nil {
		// No valid operator was found. This can happen (for example) when an
		// equivalence comes from within a base relation, in which case filters
		// would have been pushed down beyond the original join tree.
		return
	}

	// Construct the edge.
	var1 := jb.f.ConstructVariable(col1)
	var2 := jb.f.ConstructVariable(col2)
	condition := jb.f.ConstructEq(var1, var2)
	filters := memo.FiltersExpr{jb.f.ConstructFiltersItem(condition)}

	// Add the edge to the join graph.
	jb.edges = append(jb.edges, *jb.makeEdge(op, filters))
	jb.innerEdges.Add(len(jb.edges) - 1)
}

// makeEdge returns a new edge given an operator and set of filters.
func (jb *JoinOrderBuilder) makeEdge(op *operator, filters memo.FiltersExpr) (e *edge) {
	e = &edge{op: op, filters: filters}
	e.calcNullRejectedRels(jb)
	e.calcSES(jb)
	e.calcTES(jb.edges)
	return e
}

// getFreeVars returns the set of columns referenced by the given predicate.
func (jb *JoinOrderBuilder) getFreeVars(predicate memo.FiltersExpr) opt.ColSet {
	return predicate.OuterCols()
}

// hasEqEdge returns true if the inner edges include a direct equality between
// the two given columns (e.g. x = a).
func (jb *JoinOrderBuilder) hasEqEdge(leftCol, rightCol opt.ColumnID) bool {
	for idx, ok := jb.innerEdges.Next(0); ok; idx, ok = jb.innerEdges.Next(idx + 1) {
		for i := range jb.edges[idx].filters {
			if jb.edges[idx].filters[i].ScalarProps().FuncDeps.AreColsEquiv(leftCol, rightCol) {
				return true
			}
		}
	}
	return false
}

// addJoin adds a join between the given left and right subsets of relations on
// the given set of edges. If the group containing joins between this set of
// relations is already contained in the plans field, the new join is added to
// the memo group. Otherwise, the join is memoized (possibly constructing a new
// group). If the join being considered existed in the originally matched join
// tree, no join is added (though its commuted version may be).
func (jb *JoinOrderBuilder) addJoin(
	op opt.Operator,
	s1, s2 vertexSet,
	joinFilters, selectFilters memo.FiltersExpr,
	joinIsRedundant bool,
) {
	if s1.intersects(s2) {
		panic(errors.AssertionFailedf("sets are not disjoint"))
	}
	if jb.onAddJoinFunc != nil {
		// Hook for testing purposes.
		jb.callOnAddJoinFunc(s1, s2, joinFilters, op)
	}

	left := jb.plans[s1]
	right := jb.plans[s2]
	union := s1.union(s2)
	if !joinIsRedundant {
		if jb.plans[union] != nil {
			jb.addToGroup(op, left, right, joinFilters, selectFilters, jb.plans[union])
		} else {
			jb.plans[union] = jb.memoize(op, left, right, joinFilters, selectFilters)
		}
	}

	if commute(op) {
		// Also add the commuted version of the join to the memo. Note that if the
		// join is redundant (a join between base relation sets s1 and s2 existed in
		// the matched join tree) then jb.plans[union] will already have the
		// original join group.
		if jb.plans[union] == nil {
			panic(errors.AssertionFailedf("expected existing join plan"))
		}
		jb.addToGroup(op, right, left, joinFilters, selectFilters, jb.plans[union])

		if jb.onAddJoinFunc != nil {
			// Hook for testing purposes.
			jb.callOnAddJoinFunc(s2, s1, joinFilters, op)
		}
	}
}

// areFiltersRedundant returns true if the given FiltersExpr contains a single
// equality filter that is already represented by the given FuncDepSet.
func areFiltersRedundant(fds *props.FuncDepSet, filters memo.FiltersExpr) bool {
	if len(filters) != 1 {
		return false
	}
	eq, ok := filters[0].Condition.(*memo.EqExpr)
	if !ok {
		return false
	}
	var1, ok1 := eq.Left.(*memo.VariableExpr)
	var2, ok2 := eq.Right.(*memo.VariableExpr)
	if !ok1 || !ok2 {
		return false
	}
	return fds.AreColsEquiv(var1.Col, var2.Col)
}

// getEquivFDs adds all equivalencies from the given filters to the given
// FuncDepSet.
func getEquivFDs(fds *props.FuncDepSet, filters memo.FiltersExpr) {
	for i := range filters {
		fds.AddEquivFrom(&filters[i].ScalarProps().FuncDeps)
	}
}

// addToGroup adds a join of the given type and with the given inputs to the
// given memo group. If selectFilters is not empty, the join is memoized instead
// and used as input to a select, which is added to the join group.
func (jb *JoinOrderBuilder) addToGroup(
	op opt.Operator, left, right memo.RelExpr, on, selectFilters memo.FiltersExpr, grp memo.RelExpr,
) {
	if len(selectFilters) > 0 {
		joinExpr := jb.memoize(op, left, right, on, nil)
		selectExpr := &memo.SelectExpr{
			Input:   joinExpr,
			Filters: selectFilters,
		}
		jb.f.Memo().AddSelectToGroup(selectExpr, grp)
		return
	}

	// Set SkipReorderJoins to true in order to avoid duplicate reordering on this
	// join.
	newJoinPrivate := memo.JoinPrivate{SkipReorderJoins: true}
	switch op {
	case opt.InnerJoinOp:
		newJoin := &memo.InnerJoinExpr{
			Left:        left,
			Right:       right,
			On:          on,
			JoinPrivate: newJoinPrivate,
		}
		jb.f.Memo().AddInnerJoinToGroup(newJoin, grp)

	case opt.SemiJoinOp:
		newJoin := &memo.SemiJoinExpr{
			Left:        left,
			Right:       right,
			On:          on,
			JoinPrivate: newJoinPrivate,
		}
		jb.f.Memo().AddSemiJoinToGroup(newJoin, grp)

	case opt.AntiJoinOp:
		newJoin := &memo.AntiJoinExpr{
			Left:        left,
			Right:       right,
			On:          on,
			JoinPrivate: newJoinPrivate,
		}
		jb.f.Memo().AddAntiJoinToGroup(newJoin, grp)

	case opt.LeftJoinOp:
		newJoin := &memo.LeftJoinExpr{
			Left:        left,
			Right:       right,
			On:          on,
			JoinPrivate: newJoinPrivate,
		}
		jb.f.Memo().AddLeftJoinToGroup(newJoin, grp)

	case opt.FullJoinOp:
		newJoin := &memo.FullJoinExpr{
			Left:        left,
			Right:       right,
			On:          on,
			JoinPrivate: newJoinPrivate,
		}
		jb.f.Memo().AddFullJoinToGroup(newJoin, grp)

	default:
		panic(errors.AssertionFailedf("invalid operator: %v", op))
	}
}

// memoize adds a join of the given type and with the given inputs to the memo
// and returns it. If selectFilters is not empty, the join becomes the input of
// a select with those filters, which is also added to the memo and returned.
func (jb *JoinOrderBuilder) memoize(
	op opt.Operator, left, right memo.RelExpr, on, selectFilters memo.FiltersExpr,
) memo.RelExpr {
	var join memo.RelExpr

	// Set SkipReorderJoins to true in order to avoid duplicate reordering on this
	// join.
	newJoinPrivate := &memo.JoinPrivate{SkipReorderJoins: true}
	switch op {
	case opt.InnerJoinOp:
		join = jb.f.Memo().MemoizeInnerJoin(left, right, on, newJoinPrivate)

	case opt.SemiJoinOp:
		join = jb.f.Memo().MemoizeSemiJoin(left, right, on, newJoinPrivate)

	case opt.AntiJoinOp:
		join = jb.f.Memo().MemoizeAntiJoin(left, right, on, newJoinPrivate)

	case opt.LeftJoinOp:
		join = jb.f.Memo().MemoizeLeftJoin(left, right, on, newJoinPrivate)

	case opt.FullJoinOp:
		join = jb.f.Memo().MemoizeFullJoin(left, right, on, newJoinPrivate)

	default:
		panic(errors.AssertionFailedf("invalid operator: %v", op))
	}
	if len(selectFilters) > 0 {
		return jb.f.Memo().MemoizeSelect(join, selectFilters)
	}
	return join
}

// getRelations returns a vertexSet containing all relations with output
// columns in the given ColSet.
func (jb *JoinOrderBuilder) getRelations(cols opt.ColSet) (rels vertexSet) {
	if cols.Empty() {
		return rels
	}
	for i := range jb.vertexes {
		if jb.vertexes[i].Relational().OutputCols.Intersects(cols) {
			rels = rels.add(vertexIndex(i))
		}
	}
	return rels
}

// allVertexes returns a vertexSet that represents all relations that are
// currently part of the join graph
func (jb *JoinOrderBuilder) allVertexes() vertexSet {
	// If the join graph has 3 vertexes, 1 << len(jb.vertexes) will give the bit
	// set '1000'. Subtracting 1 will give the final set '111', which represents
	// all relations referenced by the join graph.
	return vertexSet((1 << len(jb.vertexes)) - 1)
}

// allEdges returns an edgeSet that represents indices to all edges in the join
// graph.
func (jb *JoinOrderBuilder) allEdges() edgeSet {
	allEdgeSet := edgeSet{}
	for i := range jb.edges {
		allEdgeSet.Add(i)
	}
	return allEdgeSet
}

// addBaseRelation adds the given RelExpr to the vertexes of the join graph.
// Before a relation is added, the number of vertexes is checked to ensure that
// the number of vertexes can be represented by an int64 bit set.
func (jb *JoinOrderBuilder) addBaseRelation(rel memo.RelExpr) {
	jb.checkSize()
	jb.vertexes = append(jb.vertexes, rel)

	// Initialize the plan for this vertex.
	idx := vertexIndex(len(jb.vertexes) - 1)
	relSet := vertexSet(0).add(idx)
	jb.plans[relSet] = rel
}

// checkSize panics if the number of relations is greater than or equal to
// MaxReorderJoinsLimit. checkSize should be called before a vertex is added to
// the join graph.
func (jb *JoinOrderBuilder) checkSize() {
	if len(jb.vertexes) >= opt.MaxReorderJoinsLimit {
		panic(
			errors.AssertionFailedf(
				"cannot reorder a join tree with more than %v relations",
				opt.MaxReorderJoinsLimit,
			),
		)
	}
}

// NotifyOnReorder sets a callback function that is called when a join is passed
// to JoinOrderBuilder to be reordered.
func (jb *JoinOrderBuilder) NotifyOnReorder(reorderFunc OnReorderFunc) {
	jb.onReorderFunc = reorderFunc
}

// NotifyOnAddJoin sets a callback function that is called when a join is added
// to the memo.
func (jb *JoinOrderBuilder) NotifyOnAddJoin(onAddJoin OnAddJoinFunc) {
	jb.onAddJoinFunc = onAddJoin
}

// callOnReorderFunc calls the onReorderFunc callback function. Panics if the
// function is nil.
func (jb *JoinOrderBuilder) callOnReorderFunc(join memo.RelExpr) {
	// Get a slice with all edges of the join graph.
	edgeSlice := make([]memo.FiltersExpr, 0, len(jb.edges))
	edgeOps := make([]opt.Operator, 0, len(jb.edges))
	for i := range jb.edges {
		edgeSlice = append(edgeSlice, jb.edges[i].filters)
		edgeOps = append(edgeOps, jb.edges[i].op.joinType)
	}

	jb.onReorderFunc(join, jb.getRelationSlice(jb.allVertexes()), edgeSlice, edgeOps)
}

// callOnAddJoinFunc calls the onAddJoinFunc callback function. Panics if the
// function is nil.
func (jb *JoinOrderBuilder) callOnAddJoinFunc(
	s1, s2 vertexSet, edges memo.FiltersExpr, op opt.Operator,
) {
	jb.onAddJoinFunc(
		jb.getRelationSlice(s1),
		jb.getRelationSlice(s2),
		jb.getRelationSlice(s1.union(s2)),
		jb.getRelationSlice(jb.getRelations(jb.getFreeVars(edges))),
		op,
	)
}

// getRelationSlice returns the base relations represented by the given
// vertexSet.
func (jb *JoinOrderBuilder) getRelationSlice(relations vertexSet) (relSlice []memo.RelExpr) {
	relSlice = make([]memo.RelExpr, 0, relations.len())
	for idx, ok := relations.next(0); ok; idx, ok = relations.next(idx + 1) {
		relSlice = append(relSlice, jb.vertexes[idx])
	}
	return relSlice
}

// edge represents an edge in the join graph. It holds the information needed
// to determine whether a new join between two sets of vertexes can be
// constructed.
type edge struct {
	// op holds properties of the original join operator from which this edge was
	// constructed. See the operator comments for more information.
	op *operator

	// filters is the set of join filters that will be used to construct new join
	// ON conditions.
	filters memo.FiltersExpr

	// nullRejectedRels is the set of vertexes on which nulls are rejected by the
	// filters.
	nullRejectedRels vertexSet

	// ses is the syntactic eligibility set of the edge; in other words, it is the
	// set of base relations referenced by the filters field.
	ses vertexSet

	// tes is the total eligibility set of the edge. The TES gives the set of base
	// relations (vertexes) that must be in the input of any join that uses the
	// filters from this edge in its ON condition. The TES is initialized with the
	// SES, and then expanded by the conflict detection algorithm.
	tes vertexSet

	// rules is a set of conflict rules which must evaluate to true in order for
	// a join between two sets of vertexes to be valid. See the conflictRule
	// comments for more information.
	rules []conflictRule
}

// operator contains the properties of a join operator from the original join
// tree. It is used in calculating the total eligibility sets for edges from any
// 'parent' joins which were originally above this one in the tree.
type operator struct {
	// joinType is the operator type of the original join operator.
	joinType opt.Operator

	// leftVertexes is the set of vertexes (base relations) that were in the left
	// input of the original join operator.
	leftVertexes vertexSet

	// rightVertexes is the set of vertexes (base relations) that were in the
	// right input of the original join operator.
	rightVertexes vertexSet

	// leftEdges is the set of edges that were constructed from join operators
	// that were in the left input of the original join operator.
	leftEdges edgeSet

	// rightEdgers is the set of edges that were constructed from join operators
	// that were in the right input of the original join operator.
	rightEdges edgeSet
}

// conflictRule is a pair of vertex sets which carry the requirement that if the
// 'from' set intersects a set of prospective join input relations, then the
// 'to' set must be a subset of the input relations (from -> to). Take the
// following query as an example:
//
//    SELECT * FROM xy
//    INNER JOIN (SELECT * FROM ab LEFT JOIN uv ON a = u)
//    ON x = a
//
// During execution of the CD-C algorithm, the following conflict rule would
// be added to inner join edge: [uv -> ab]. This means that, for any join that
// uses this edge, the presence of uv in the set of input relations implies the
// presence of ab. This prevents an inner join between relations xy and uv
// (since then ab would not be an input relation). Note that, in practice, this
// conflict rule would be absorbed into the TES because ab is a part of the
// inner join edge's SES (see the addRule func).
type conflictRule struct {
	from vertexSet
	to   vertexSet
}

// calcNullRejectedRels initializes the notNullRels vertex set of the edge with
// all relations on which nulls are rejected by the edge's filters.
func (e *edge) calcNullRejectedRels(jb *JoinOrderBuilder) {
	var nullRejectedCols opt.ColSet
	for i := range e.filters {
		if constraints := e.filters[i].ScalarProps().Constraints; constraints != nil {
			nullRejectedCols.UnionWith(constraints.ExtractNotNullCols(jb.evalCtx))
		}
	}
	e.nullRejectedRels = jb.getRelations(nullRejectedCols)
}

// calcSES initializes the ses vertex set of the edge with all relations
// referenced by the edge's predicate (syntactic eligibility set from paper). If
// a join uses a predicate in its ON condition, all relations in the SES must be
// part of the join's inputs. For example, in this query:
//
//    SELECT *
//    FROM xy
//    INNER JOIN (SELECT * FROM ab INNER JOIN uv ON b = (u*2))
//    ON x = a
//
// The SES for the x = a edge would contain relations xy and ab. The SES for the
// b = u*2 edge would contain ab and uv. Therefore, this query could be
// reordered like so:
//
//    SELECT *
//    FROM (SELECT * FROM xy INNER JOIN ab ON x = a)
//    INNER JOIN uv
//    ON b = (u*2)
//
// While still satisfying the syntactic eligibility sets of the edges.
func (e *edge) calcSES(jb *JoinOrderBuilder) {
	// Get the columns referenced by the predicate.
	freeVars := jb.getFreeVars(e.filters)

	// Add all relations referenced by the predicate to the SES vertexSet.
	// Columns that do not come from a base relation (outer to the join tree) will
	// be treated as constant, and therefore will not be referenced in the SES.
	e.ses = jb.getRelations(freeVars)
}

// calcTES initializes the total eligibility set of this edge using the
// syntactic eligibility set as well as the properties given by the lookup
// tables. calcTES should only be called when the SES, leftVertexes and
// rightVertexes have already been initialized.
func (e *edge) calcTES(edges []edge) {
	// Initialize the TES with the SES.
	e.tes = e.ses

	// In the case of a degenerate predicate (doesn't reference one or both
	// sides), add relations from the unreferenced side(s). This 'freezes' the
	// predicate so that its join cannot be pushed down the join tree. Example:
	//
	//    SELECT * FROM uv
	//    INNER JOIN (
	//   			SELECT * FROM xy
	//    		INNER JOIN ab
	//   	  	ON x = a
	//   	)
	//    ON True
	//
	// In this example, the total eligibility set for the cross join predicate
	// would contain all three relations, so the cross join could not be pushed
	// down the join tree. This behavior is desirable because (1) it is rare that
	// the best plan involves creating new cross joins and (2) it constrains the
	// search space (which decreases planning time). This behavior is necessary
	// because it prevents cases where joins from separate branches of the
	// join tree conflict with one another (see citations: [8] section 6.2).
	//
	// TODO(drewk): in some cases, it is beneficial to introduce cross joins. We
	//  can do this selectively by adding edges between relations for which we
	//  want to consider cross joins.
	if !e.tes.intersects(e.op.leftVertexes) {
		e.tes = e.tes.union(e.op.leftVertexes)
	}
	if !e.tes.intersects(e.op.rightVertexes) {
		e.tes = e.tes.union(e.op.rightVertexes)
	}

	// Execute the CD-C algorithm (see citations: [8] section 5.4).
	//
	// CD-C stands for 'Conflict Detection C'. It is the last (and most complete)
	// of three algorithms given by [8] which allow invalid orderings of a join
	// tree to be detected and prevented based on the properties of the original.
	// For every 'child' operator that is in the left input of a given operator,
	// lookup tables are used to establish whether various transformations are
	// possible given the types of the two joins.
	//
	// If it is found that a transformation property does not hold for the two
	// joins, the TES may be expanded to reflect the 'conflict', or
	// 'conflict rules' may be introduced (see the struct comment). Later, during
	// plan enumeration, the TES and conflict rules of each edge are used to
	// determine whether the edge can be used to form a join between two given
	// sets of relations.
	//
	// Iterate through the join operators in the left input of the operator from
	// which this edge was constructed. Whenever a conflict is detected, expand
	// the total eligibility set accordingly.
	for idx, ok := e.op.leftEdges.Next(0); ok; idx, ok = e.op.leftEdges.Next(idx + 1) {
		if e.op.leftVertexes.isSubsetOf(e.tes) {
			// Fast path to break out early: the TES includes all relations from the
			// left input.
			break
		}
		child := &edges[idx]
		if !assoc(child, e) {
			// The edges are not associative, so add a conflict rule mapping from the
			// right input relations of the child to its left input relations.
			rule := conflictRule{from: child.op.rightVertexes}
			if child.op.leftVertexes.intersects(child.ses) {
				// A less restrictive conflict rule can be added in this case.
				rule.to = child.op.leftVertexes.intersection(child.ses)
			} else {
				rule.to = child.op.leftVertexes
			}
			e.addRule(rule)
		}
		if !leftAsscom(child, e) {
			// Left-asscom does not hold, so add a conflict rule mapping from the
			// left input relations of the child to its right input relations.
			rule := conflictRule{from: child.op.leftVertexes}
			if child.op.rightVertexes.intersects(child.ses) {
				// A less restrictive conflict rule can be added in this case.
				rule.to = child.op.rightVertexes.intersection(child.ses)
			} else {
				rule.to = child.op.rightVertexes
			}
			e.addRule(rule)
		}
	}

	// Iterate through the operators in the right input of the operator from which
	// this edge was constructed, adjusting the TES along the way.
	for idx, ok := e.op.rightEdges.Next(0); ok; idx, ok = e.op.rightEdges.Next(idx + 1) {
		if e.op.rightVertexes.isSubsetOf(e.tes) {
			// Fast path to break out early: the TES includes all relations from the
			// right input.
			break
		}
		child := &edges[idx]
		if !assoc(e, child) {
			// The edges are not associative, so add a conflict rule mapping from the
			// left input relations of the child to its right input relations.
			rule := conflictRule{from: child.op.leftVertexes}
			if child.op.rightVertexes.intersects(child.ses) {
				// A less restrictive conflict rule can be added in this case.
				rule.to = child.op.rightVertexes.intersection(child.ses)
			} else {
				rule.to = child.op.rightVertexes
			}
			e.addRule(rule)
		}
		if !rightAsscom(e, child) {
			// Right-asscom does not hold, so add a conflict rule mapping from the
			// right input relations of the child to its left input relations.
			rule := conflictRule{from: child.op.rightVertexes}
			if child.op.leftVertexes.intersects(child.ses) {
				// A less restrictive conflict rule can be added in this case.
				rule.to = child.op.leftVertexes.intersection(child.ses)
			} else {
				rule.to = child.op.leftVertexes
			}
			e.addRule(rule)
		}
	}
}

// addRule adds the given conflict rule to the edge. Before the rule is added to
// the rules set, an effort is made to eliminate the need for the rule.
func (e *edge) addRule(rule conflictRule) {
	if rule.from.intersects(e.tes) {
		// If the 'from' relation set intersects the total eligibility set, simply
		// add the 'to' set to the TES because the rule will always be triggered.
		e.tes = e.tes.union(rule.to)
		return
	}
	if rule.to.isSubsetOf(e.tes) {
		// If the 'to' relation set is a subset of the total eligibility set, the
		// rule is a do-nothing.
		return
	}
	e.rules = append(e.rules, rule)
}

// checkNonInnerJoin performs an applicability check for a non-inner join
// between the two given sets of base relations. If it returns true, a non-inner
// join can be constructed using the filters from this edge and the two given
// relation sets. checkNonInnerJoin is more restrictive than checkInnerJoin
// because it handles the general case.
func (e *edge) checkNonInnerJoin(s1, s2 vertexSet) bool {
	if !e.checkRules(s1, s2) {
		// The conflict rules for this edge are not satisfied for a join between s1
		// and s2.
		return false
	}

	// The left TES must be a subset of the s1 relations, and the right TES must
	// be a subset of the s2 relations. In addition, the TES must intersect both
	// s1 and s2 (the edge must connect the two vertex sets).
	//
	// The subset checks ensure that (1) all relations referenced by the predicate
	// are in s1 or s2 as well as that (2) relations cannot move from one side of
	// the join to the other (without explicit allowance). The intersection checks
	// ensure that the two sets are connected by the edge; this prevents new cross
	// joins from being constructed. Take this query as an example:
	//
	//    SELECT *
	//    FROM (SELECT * FROM ab LEFT JOIN uv ON a = u)
	//    INNER JOIN xy
	//    ON (x = v OR v IS NULL)
	//
	// In this query, the xy relation cannot be pushed into the right side of the
	// left join despite the edge between xy and uv because doing so would change
	// the query semantics. How would the TES and conflict rules of the inner join
	// edge prevent this?
	//   1. The TES would first be initialized with the SES: relations xy and uv.
	//   2. Application of CD-C would expand the TES to include ab because
	//      assoc(left-join, inner-join) is false.
	//   3. The TES now includes all three relations, meaning that the inner join
	//      cannot join any two relations together (including xy and uv).
	return e.tes.intersection(e.op.leftVertexes).isSubsetOf(s1) &&
		e.tes.intersection(e.op.rightVertexes).isSubsetOf(s2) &&
		e.tes.intersects(s1) && e.tes.intersects(s2)
}

// checkInnerJoin performs an applicability check for an inner join between the
// two given sets of base relations. If it returns true, an inner join can be
// constructed using the filters from this edge and the two given relation sets.
//
// Why is the inner join check different from the non-inner join check?
// In effect, the difference between the inner and non-inner edge checks is that
// for inner joins, relations can be moved 'across' the join relative to their
// positions in the original join tree. This is necessary in order to allow
// inner join conjuncts from different joins to be combined into new join
// operators. For example, take this perfectly valid (and desirable)
// transformation:
//
//    SELECT * FROM xy
//    INNER JOIN (SELECT * FROM ab INNER JOIN uv ON a = u)
//    ON x = a AND x = u
//    =>
//    SELECT * FROM ab
//    INNER JOIN (SELECT * FROM xy INNER JOIN uv ON x = u)
//    ON x = a AND a = u
//
// Note that, from the perspective of the x = a edge, it looks like the join has
// been commuted (the xy and ab relations switched sides). From the perspective
// of the a = u edge, however, all relations that were previously on the left
// are still on the left, and all relations that were on the right are still on
// the right. The stricter requirements of checkNonInnerJoin would not allow
// this transformation to take place.
func (e *edge) checkInnerJoin(s1, s2 vertexSet) bool {
	if !e.checkRules(s1, s2) {
		// The conflict rules for this edge are not satisfied for a join between s1
		// and s2.
		return false
	}

	// The TES must be a subset of the relations of the candidate join inputs. In
	// addition, the TES must intersect both s1 and s2 (the edge must connect the
	// two vertex sets).
	return e.tes.isSubsetOf(s1.union(s2)) && e.tes.intersects(s1) && e.tes.intersects(s2)
}

// checkRules iterates through the edge's rules and returns false if a conflict
// is detected for the given sets of join input relations. Otherwise, returns
// true.
func (e *edge) checkRules(s1, s2 vertexSet) bool {
	s := s1.union(s2)
	for _, rule := range e.rules {
		if rule.from.intersects(s) && !rule.to.isSubsetOf(s) {
			// The join is invalid because it does not obey this conflict rule.
			return false
		}
	}
	return true
}

// joinIsRedundant returns true if a join between the two sets of base relations
// was already present in the original join tree. If so, enumerating this join
// would be redundant, so it should be skipped.
func (e *edge) joinIsRedundant(s1, s2 vertexSet) bool {
	return e.op.leftVertexes == s1 && e.op.rightVertexes == s2
}

// commute returns true if the given join operator type is commutable.
func commute(op opt.Operator) bool {
	return op == opt.InnerJoinOp || op == opt.FullJoinOp
}

// assoc returns true if two joins with the operator types and filters described
// by the given edges are associative with each other. An example of an
// application of the associative property:
//
//    SELECT * FROM
//    (
//      SELECT * FROM xy
//      INNER JOIN ab ON x = a
//    )
//    INNER JOIN uv ON a = u
//    =>
//    SELECT * FROM xy
//    INNER JOIN
//    (
//      SELECT * FROM ab
//      INNER JOIN uv ON a = u
//    )
//    ON x = a
//
func assoc(edgeA, edgeB *edge) bool {
	if edgeB.ses.intersects(edgeA.op.leftVertexes) || edgeA.ses.intersects(edgeB.op.rightVertexes) {
		// Ensure that application of the associative property would not lead to
		// 'orphaned' predicates, where one or more referenced relations are not in
		// the resulting join's inputs. Take as an example this reordering that
		// results from applying the associative property:
		//
		//    SELECT * FROM (SELECT * FROM xy INNER JOIN ab ON y = a)
		//    INNER JOIN uv
		//    ON x = u
		//    =>
		//    SELECT * FROM xy
		//    INNER JOIN (SELECT * FROM ab INNER JOIN uv ON x = u)
		//    ON y = a
		//
		// Note that the x = u predicate references the xy relation, which is not
		// in that join's inputs. Therefore, this transformation is invalid.
		return false
	}
	return checkProperty(assocTable, edgeA, edgeB)
}

// leftAsscom returns true if two joins with the operator types and filters
// described by the given edges allow the left-asscom property. An example of
// an application of the left-asscom property:
//
//    SELECT * FROM
//    (
//      SELECT * FROM xy
//      INNER JOIN ab ON x = a
//    )
//    INNER JOIN uv ON x = u
//    =>
//    SELECT * FROM
//    (
//      SELECT * FROM xy
//      INNER JOIN uv ON x = u
//    )
//    INNER JOIN ab ON x = a
//
func leftAsscom(edgeA, edgeB *edge) bool {
	if edgeB.ses.intersects(edgeA.op.rightVertexes) || edgeA.ses.intersects(edgeB.op.rightVertexes) {
		// Ensure that application of the left-asscom property would not lead to
		// 'orphaned' predicates. See the assoc() comment for why this is necessary.
		return false
	}
	return checkProperty(leftAsscomTable, edgeA, edgeB)
}

// rightAsscom returns true if two joins with the operator types and filters
// described by the given edges allow the right-asscom property. An example of
// an application of the right-asscom property:
//
//    SELECT * FROM uv
//    INNER JOIN
//    (
//      SELECT * FROM xy
//      INNER JOIN ab ON x = a
//    )
//    ON a = u
//    =>
//    SELECT * FROM xy
//    INNER JOIN
//    (
//      SELECT * FROM uv
//      INNER JOIN ab ON a = u
//    )
//    ON x = a
//
func rightAsscom(edgeA, edgeB *edge) bool {
	if edgeB.ses.intersects(edgeA.op.leftVertexes) || edgeA.ses.intersects(edgeB.op.leftVertexes) {
		// Ensure that application of the right-asscom property would not lead to
		// 'orphaned' predicates. See the assoc() comment for why this is necessary.
		return false
	}
	return checkProperty(rightAsscomTable, edgeA, edgeB)
}

// lookupTableEntry is an entry in one of the join property lookup tables
// defined below (associative, left-asscom and right-asscom properties). A
// lookupTableEntry can be unconditionally true or false, as well as true
// conditional on the null-rejecting properties of the edge filters.
type lookupTableEntry uint8

const (
	// never indicates that the transformation represented by the table entry is
	// unconditionally incorrect.
	never lookupTableEntry = 0

	// always indicates that the transformation represented by the table entry is
	// unconditionally correct.
	always lookupTableEntry = 1 << (iota - 1)

	// filterA indicates that the filters of the "A" join operator must reject
	// nulls for the set of vertexes specified by rejectsOnLeftA, rejectsOnRightA,
	// etc.
	filterA

	// filterB indicates that the filters of the "B" join operator must reject
	// nulls for the set of vertexes specified by rejectsOnLeftA, rejectsOnRightA,
	// etc.
	filterB

	// rejectsOnLeftA indicates that the filters must reject nulls for the left
	// input relations of operator "A".
	rejectsOnLeftA

	// rejectsOnLeftA indicates that the filters must reject nulls for the right
	// input relations of operator "A".
	rejectsOnRightA

	// rejectsOnLeftA indicates that the filters must reject nulls for the right
	// input relations of operator "B".
	rejectsOnRightB

	// table2Note1 indicates that the filters of operator "B" must reject nulls on
	// the relations of the right input of operator "A".
	// Citations: [8] Table 2 Footnote 1.
	table2Note1 = filterB | rejectsOnRightA

	// table2Note2 indicates that the filters of operators "A" and "B" must reject
	// nulls on the relations of the right input of operator "A".
	// Citations: [8] Table 2 Footnote 2.
	table2Note2 = (filterA | filterB) | rejectsOnRightA

	// table3Note1 indicates that the filters of operator "A" must reject nulls on
	// the relations of the left input of operator "A".
	// Citations: [8] Table 3 Footnote 1.
	table3Note1 = filterA | rejectsOnLeftA

	// table3Note2 indicates that the filters of operator "B" must reject nulls on
	// the relations of the right input of operator "B".
	// Citations: [8] Table 3 Footnote 1]2.
	table3Note2 = filterB | rejectsOnRightB

	// table3Note3 indicates that the filters of operators "A" and "B" must reject
	// nulls on the relations of the left input of operator "A".
	// Citations: [8] Table 3 Footnote 3.
	table3Note3 = (filterA | filterB) | rejectsOnLeftA

	// table3Note4 indicates that the filters of operators "A" and "B" must reject
	// nulls on the relations of the right input of operator "B".
	// Citations: [8] Table 3 Footnote 4.
	table3Note4 = (filterA | filterB) | rejectsOnRightB
)

// assocTable is a lookup table indicating whether it is correct to apply the
// associative transformation to pairs of join operators.
// citations: [8] table 2
var assocTable = [5][5]lookupTableEntry{
	//             inner-B semi-B  anti-B  left-B  full-B
	/* inner-A */ {always, always, always, always, never},
	/* semi-A  */ {never, never, never, never, never},
	/* anti-A  */ {never, never, never, never, never},
	/* left-A  */ {never, never, never, table2Note1, never},
	/* full-A  */ {never, never, never, table2Note1, table2Note2},
}

// leftAsscomTable is a lookup table indicating whether it is correct to apply
// the left-asscom transformation to pairs of join operators.
// citations: [8] table 3
var leftAsscomTable = [5][5]lookupTableEntry{
	//             inner-B semi-B  anti-B  left-B  full-B
	/* inner-A */ {always, always, always, always, never},
	/* semi-A  */ {always, always, always, always, never},
	/* anti-A  */ {always, always, always, always, never},
	/* left-A  */ {always, always, always, always, table3Note1},
	/* full-A  */ {never, never, never, table3Note2, table3Note3},
}

// rightAsscomTable is a lookup table indicating whether it is correct to apply
// the right-asscom transformation to pairs of join operators.
// citations: [8] table 3
var rightAsscomTable = [5][5]lookupTableEntry{
	//             inner-B semi-B anti-B left-B full-B
	/* inner-A */ {always, never, never, never, never},
	/* semi-A  */ {never, never, never, never, never},
	/* anti-A  */ {never, never, never, never, never},
	/* left-A  */ {never, never, never, never, never},
	/* full-A  */ {never, never, never, never, table3Note4},
}

// checkProperty returns true if the transformation represented by the given
// property lookup table is allowed for the two given edges. Note that while
// most table entries are either true or false, some are conditionally true,
// depending on the null-rejecting properties of the edge filters (for example,
// association for two full joins).
func checkProperty(table [5][5]lookupTableEntry, edgeA, edgeB *edge) bool {
	entry := table[getOpIdx(edgeA)][getOpIdx(edgeB)]

	if entry == never {
		// Application of this transformation property is unconditionally incorrect.
		return false
	}
	if entry == always {
		// Application of this transformation property is unconditionally correct.
		return true
	}

	// This property is conditionally applicable. Get the relations that must be
	// null-rejected by the filters.
	var candidateNullRejectRels vertexSet
	if entry&rejectsOnLeftA != 0 {
		// Filters must null-reject on the left input vertexes of edgeA.
		candidateNullRejectRels = edgeA.op.leftVertexes
	} else if entry&rejectsOnRightA != 0 {
		// Filters must null-reject on the right input vertexes of edgeA.
		candidateNullRejectRels = edgeA.op.rightVertexes
	} else if entry&rejectsOnRightB != 0 {
		// Filters must null-reject on the right input vertexes of edgeB.
		candidateNullRejectRels = edgeB.op.rightVertexes
	}

	// Check whether the edge filters reject nulls on nullRejectRelations.
	if entry&filterA != 0 {
		// The filters of edgeA must reject nulls on one or more of the relations in
		// nullRejectRelations.
		if !edgeA.nullRejectedRels.intersects(candidateNullRejectRels) {
			return false
		}
	}
	if entry&filterB != 0 {
		// The filters of edgeB must reject nulls on one or more of the relations in
		// nullRejectRelations.
		if !edgeB.nullRejectedRels.intersects(candidateNullRejectRels) {
			return false
		}
	}
	return true
}

// getOpIdx returns an index into the join property lookup tables given an edge
// with an associated operator type.
func getOpIdx(e *edge) int {
	switch e.op.joinType {
	case opt.InnerJoinOp:
		return 0

	case opt.SemiJoinOp:
		return 1

	case opt.AntiJoinOp:
		return 2

	case opt.LeftJoinOp:
		return 3

	case opt.FullJoinOp:
		return 4

	default:
		panic(errors.AssertionFailedf("invalid operator: %v", e.op.joinType))
	}
}

type edgeSet = util.FastIntSet

type bitSet uint64

// vertexSet represents a set of base relations that form the vertexes of the
// join graph.
type vertexSet = bitSet

const maxSetSize = 63

// vertexIndex represents the ordinal position of a base relation in the
// JoinOrderBuilder vertexes field. vertexIndex must be less than maxSetSize.
type vertexIndex = uint64

// add returns a copy of the bitSet with the given element added.
func (s bitSet) add(idx uint64) bitSet {
	if idx > maxSetSize {
		panic(errors.AssertionFailedf("cannot insert %d into bitSet", idx))
	}
	return s | (1 << idx)
}

// union returns the set union of this set with the given set.
func (s bitSet) union(o bitSet) bitSet {
	return s | o
}

// intersection returns the set intersection of this set with the given set.
func (s bitSet) intersection(o bitSet) bitSet {
	return s & o
}

// difference returns the set difference of this set with the given set.
func (s bitSet) difference(o bitSet) bitSet {
	return s & ^o
}

// intersects returns true if this set and the given set intersect.
func (s bitSet) intersects(o bitSet) bool {
	return s.intersection(o) != 0
}

// isSubsetOf returns true if this set is a subset of the given set.
func (s bitSet) isSubsetOf(o bitSet) bool {
	return s.union(o) == o
}

// isSingleton returns true if the set has exactly one element.
func (s bitSet) isSingleton() bool {
	return s > 0 && (s&(s-1)) == 0
}

// next returns the next element in the set after the given start index, and
// a bool indicating whether such an element exists.
func (s bitSet) next(startVal uint64) (elem uint64, ok bool) {
	if startVal < maxSetSize {
		if ntz := bits.TrailingZeros64(uint64(s >> startVal)); ntz < 64 {
			return startVal + uint64(ntz), true
		}
	}
	return uint64(math.MaxInt64), false
}

// len returns the number of elements in the set.
func (s bitSet) len() int {
	return bits.OnesCount64(uint64(s))
}
