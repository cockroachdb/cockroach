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
	"github.com/cockroachdb/errors"
)

// JoinOrderBuilder is used to add valid orderings of a given join tree to the
// memo during exploration.
//
// How JoinOrderBuilder works
// --------------------------
//
// First, the initial join tree is traversed and encoded as a graph (technically
// a hypergraph because predicates can reference more than two relations). The
// 'leaves' of the join tree become the vertexes of the graph, and the edges are
// built using the ON conditions of the joins. For example, take this query:
//
//		SELECT *
//		FROM ab,
//		     (SELECT * FROM cd WHERE d > 0),
//		     uv,
//		     xy
//		WHERE a = x AND
//		      u = x AND
//		      u = c
//
// ab rowcount: 5
// cd rowcount: 10
// uv rowcount: 10,000,000
// xy rowcount: 10,000,000,000
//
// The vertexes of the graph would be initialized with base relations ab,
// (SELECT * FROM cd WHERE d > 0), uv, and xy. The edges correspond to
// relationships between those base relations. For example, an edge exists
// between the ab and xy vertexes, because of the a = x filter. Note that a
// filter like a = u + x would result in a hyperedge between relations ab, uv
// and xy.
//
// Next, equivalence closure is computed for the graph edges. In this example,
// the filter a = u (among others) would be added to the graph, since a = x and
// u = x. At this point, if the graph were translated back into query form it
// might look something like this:
//
//		SELECT *
//		FROM ab,
//		     (SELECT * FROM cd WHERE d > 0),
//		     uv,
//		     xy
//		WHERE a = x AND
//		      u = x AND
//		      u = c AND
//		      c = x AND
//		      a = c AND
//		      a = u
//
// Finally, the DPSube algorithm is executed. DPSube enumerates all disjoint
// pairs of subsets of base relations such as ([xy], [uv]) or ([xy, uv], [cd]).
// This is done efficiently using bit sets. For each pair of relation subsets, a
// join is added to the memo as long as there exists at least one edge that
// connects the two subsets. All connecting edges are added to the new join's ON
// condition.
//
// The following is an example of a plan for the above query that takes
// advantage of join reordering:
//
//		inner-join (merge)
//		 ├── cost: 76.409
//		 ├── select
//		 │    ├── cost: 10.53
//		 │    ├── scan cd
//		 │    │    └── cost: 10.42
//		 │    └── filters
//		 │         └── d > 0
//		 ├── inner-join (lookup xy)
//		 │    ├── lookup columns are key
//		 │    ├── cost: 65.65
//		 │    ├── inner-join (lookup uv)
//		 │    │    ├── lookup columns are key
//		 │    │    ├── cost: 35.43
//		 │    │    ├── scan ab
//		 │    │    │    └── cost: 5.22
//		 │    │    └── filters (true)
//		 │    └── filters
//		 │         └── u = x
//		 └── filters
//		      ├── c = x
//		      └── a = c
//
// Note that the uv lookup join uses an a = u edge that wasn't present in the
// original query. For comparison, here's the plan for this query that results
// from setting the join reorder limit to zero:
//
//		inner-join (merge)
//		 ├── cost: 70900016.2
//		 ├── scan ab
//		 │    └── cost: 5.22
//		 ├── inner-join (merge)
//		 │    ├── cost: 70900010.8
//		 │    ├── select
//		 │    │    ├── cost: 10.53
//		 │    │    ├── scan cd
//		 │    │    │    └── cost: 10.42
//		 │    │    └── filters
//		 │    │         └── d > 0
//		 │    ├── inner-join (lookup xy)
//		 │    │    ├── lookup columns are key
//		 │    │    ├── cost: 70800000
//		 │    │    ├── scan uv
//		 │    │    │    └── cost: 10400000
//		 │    │    └── filters (true)
//		 │    └── filters (true)
//		 └── filters (true)
//
// Advantages of using a join order enumeration algorithm like DPSube as opposed
// to combining associative and commutative transformation rules:
//   1. Each join order is enumerated only once, so there is significantly less
//      duplication of effort.
//   2. An ordering is not added to the memo if it involves creating a cross
//      join where there wasn't one before. Cross join plans are rarely the best
//      plan, and avoiding them significantly decreases the search space and
//      therefore planning time.
//   3. The transformation rules (as well as some normalization rules) may
//      produce equivalence closure in some cases, but only by accident. Join
//      order enumeration, on the other hand, computes complete equivalence
//      closure for the entire join tree.
//
// For more information on the enumeration of valid join orderings, see the
// paper: 'On the correct and complete enumeration of the core search space'.
//
// Currently, JoinOrderBuilder can only reorder join trees containing
// InnerJoins. However, the algorithm is extensible to non-inner joins.
// TODO(drewk): add support for other join operators.
//
// Citations: [8]
type JoinOrderBuilder struct {
	f *norm.Factory

	// vertexes is the set of base relations that form the vertexes of the join
	// graph. Any RelExpr can be a vertex, including a join (e.g. in case where
	// join has flags that prevent reordering).
	vertexes []memo.RelExpr

	// edges represents the hyperedges of the join graph. The relationSet key
	// represents the hyperedge itself - it contains all relations which are
	// referenced by the FiltersExpr value. All relations in the relationSet must
	// be part of the input of any join that uses the corresponding FiltersExpr in
	// its ON condition. The predicate xy.x = ab.a + uv.u would have the
	// relationSet [xy, ab, uv].
	edges map[relationSet]memo.FiltersExpr

	// plans maps from a set of base relations to the memo group for the join tree
	// that contains those relations (and only those relations). As an example,
	// the group for [xy, ab, uv] might contain the join trees (xy, (ab, uv)),
	// ((xy, ab), uv), (ab, (xy, uv)), etc.
	//
	// The group for a single base relation is simply the base relation itself.
	plans map[relationSet]memo.RelExpr

	// edgeFDs is a set of equivalencies used to compute equivalence closure for
	// the edges of the join graph.
	edgeFDs props.FuncDepSet

	// onReorderFunc is called once the join graph has been assembled. The
	// callback parameters give the root join as well as the vertexes and edges of
	// the join graph.
	onReorderFunc func(join memo.RelExpr, vertexes []memo.RelExpr, edges memo.FiltersExpr)

	// onAddJoinFunc is called when JoinOrderBuilder attempts to add a join to the
	// memo via addJoin. The callback parameters give the base relations of the
	// left and right inputs of the join, the set of all base relations currently
	// being considered, and the base relations referenced by the join's ON
	// condition.
	onAddJoinFunc func(left, right, all, refs []memo.RelExpr)
}

// Init initializes a new JoinOrderBuilder with the given factory. The join
// graph is reset, so a JoinOrderBuilder can be reused. Callback functions are
// not reset.
func (jb *JoinOrderBuilder) Init(f *norm.Factory) {
	jb.f = f

	jb.vertexes = []memo.RelExpr{}
	jb.edges = map[relationSet]memo.FiltersExpr{}
	jb.plans = map[relationSet]memo.RelExpr{}
	jb.edgeFDs = props.FuncDepSet{}
}

// Reorder adds all valid (non-commutative) orderings of the given join to the
// memo. Currently, only InnerJoins are reordered.
func (jb *JoinOrderBuilder) Reorder(join memo.RelExpr) {
	switch t := join.(type) {
	case *memo.InnerJoinExpr:
		if !t.Flags.Empty() {
			panic(errors.AssertionFailedf("join with hints cannot be reordered"))
		}

		// Populate the vertexes and edges of the join hypergraph.
		jb.populateGraph(join)

		// Ensure equivalence closure for edges that don't reference outer columns.
		jb.ensureClosure(t.Relational().OutputCols.Difference(t.Relational().OuterCols))

		if jb.onReorderFunc != nil {
			// Hook for testing purposes.
			jb.callOnReorderFunc(join)
		}

		// Execute the DPSube algorithm. Enumerate all allowed join orderings and
		// add them to the memo.
		jb.dpSube()

	default:
		panic(errors.AssertionFailedf("%v cannot be reordered", t.Op()))
	}
}

// populateGraph traverses the given expression and initializes the vertexes
// and edges of the join hypergraph.
// TODO(drewk): Rather than only matching at or below the join order limit,
// iteratively optimize shallow sub-sections of the tree using the limit.
func (jb *JoinOrderBuilder) populateGraph(rel memo.RelExpr) {
	switch t := rel.(type) {
	case *memo.InnerJoinExpr:
		if !t.Flags.Empty() {
			// If the join has flags, we can't reorder it. Simply treat it as a base
			// relation.
			jb.addBaseRelation(t)
			break
		}
		startIdx := relationIndex(len(jb.vertexes))

		// Traverse the left and right inputs of this join, initializing the join
		// graph in the process.
		jb.populateGraph(t.Left)
		jb.populateGraph(t.Right)

		endIdx := relationIndex(len(jb.vertexes))

		// Create hyperedges for the join graph using this join's ON condition.
		jb.makeEdges(t)

		// Initialize the plan for this join. All base relations in the input of the
		// join are in the range [startIdx, endIdx) because populateGraph was called
		// on the join's left and right inputs, so all base relations under the join
		// were added to jb.vertexes.
		relSet := relationSet(((1 << (endIdx - startIdx)) - 1) << startIdx)
		jb.plans[relSet] = t

	default:
		jb.addBaseRelation(t)

		// Initialize the plan for this vertex.
		idx := relationIndex(len(jb.vertexes) - 1)
		relSet := relationSet(1 << idx)
		jb.plans[relSet] = t
	}
}

// ensureClosure ensures that the edges considered during join reordering
// reflect the transitive closure of all equality filters between columns.
// As an example, take a query like the following:
//
//    SELECT * FROM xy INNER JOIN ab ON x = a INNER JOIN uv ON u = a
//
// Contains the explicit edges x = a and u = a, and the implicit edge x = u.
// This implicit edge will be added by ensureClosure.
func (jb *JoinOrderBuilder) ensureClosure(nonOuterCols opt.ColSet) {
	// Remove outer column references.
	jb.edgeFDs.ProjectCols(nonOuterCols)

	// Enumerate all distinct pairs of equivalent columns.
	reps := jb.edgeFDs.EquivReps()
	for col, ok := reps.Next(0); ok; col, ok = reps.Next(col + 1) {
		// Get all columns which are known to be equal to this column.
		equivGroup := jb.edgeFDs.ComputeEquivGroup(col)

		// Ensure that there exists an edge for each distinct pair of equivalent
		// columns.
		for col1, ok1 := equivGroup.Next(0); ok1; col1, ok1 = equivGroup.Next(col1 + 1) {
			for col2, ok2 := equivGroup.Next(col1 + 1); ok2; col2, ok2 = equivGroup.Next(col2 + 1) {
				relation1, relation2 := jb.getRelationIndexes(col1, col2)
				relations := relationSet(1 << relation1).union(relationSet(1 << relation2))
				var1 := jb.f.Memo().MemoizeVariable(col1)
				var2 := jb.f.Memo().MemoizeVariable(col2)
				if !jb.hasColEq(jb.edges[relations], col1, col2) {
					// This equality is not yet represented among the edges.
					condition := jb.f.Memo().MemoizeEq(var1, var2)
					filter := jb.f.ConstructFiltersItem(condition)
					jb.edges[relations] = append(jb.edges[relations], filter)
				}
			}
		}
	}
}

// dpSube carries out the DPSube algorithm from the paper. All disjoint pairs of
// subsets of base relations are enumerated and checked for validity. If valid,
// the pair of subsets is used along with the edges connecting them to create a
// new join operator, which is added to the memo.
// TODO(drewk): implement DPHyp (or a similar algorithm).
func (jb *JoinOrderBuilder) dpSube() {
	subsets := relationSet((1 << len(jb.vertexes)) - 1)
	for subset := relationSet(1); subset <= subsets; subset++ {
		if subset.isSingleton() {
			// This subset has only one set bit, which means it only represents one
			// relation. We need at least two relations in order to create a new join.
			continue
		}
		// Enumerate all possible pairwise-disjoint binary partitions of the subset,
		// s1 AND s2. These represent sets of relations that may be joined together.
		// Only iterate s1 up to subset/2 to avoid enumerating duplicate partitions.
		for s1 := relationSet(1); s1 <= subset/2; s1++ {
			if !s1.isSubset(subset) {
				continue
			}
			s2 := subset.difference(s1)
			if edges, ok := jb.getEdges(s1, s2); ok {
				jb.addJoin(opt.InnerJoinOp, s1, s2, edges)
			}
		}
	}
}

// makeEdges creates a set of hyper-graph edges for the given join and adds them
// to the edges field. Each edge is mapped to its SES (syntactic eligibility
// set, defined in the getSES comment). For InnerJoins, an edge is created for
// each conjunct in the join's ON condition.
func (jb *JoinOrderBuilder) makeEdges(join memo.RelExpr) {
	addEdge := func(ses relationSet, predicate memo.FiltersExpr) {
		if _, ok := jb.edges[ses]; ok {
			jb.edges[ses] = append(jb.edges[ses], predicate...)
		} else {
			jb.edges[ses] = predicate
		}
	}

	switch t := join.(type) {
	case *memo.InnerJoinExpr:
		on := t.On
		leftCols := t.Left.Relational().OutputCols
		rightCols := t.Right.Relational().OutputCols
		leftRels := jb.getRelations(leftCols)
		rightRels := jb.getRelations(rightCols)
		if on.IsTrue() {
			// Create a faux edge with all relations that produce the input columns of
			// this join. This 'freezes' the join so that only commutation is allowed.
			addEdge(jb.getSES(leftRels, rightRels, on), on)
		} else {
			for i := range on {
				// Add all conjuncts as edges.
				edgeFilters := on[i : i+1]
				addEdge(jb.getSES(leftRels, rightRels, edgeFilters), edgeFilters)

				// Add the equivalence FDs to the edgeFDs field. These will be used to
				// calculate the equivalence closure for the edges.
				jb.edgeFDs.AddEquivFrom(&on[i].ScalarProps().FuncDeps)
			}
		}

	default:
		panic(errors.AssertionFailedf("invalid operator: %v", t.Op()))
	}
}

// getSES returns a relationSet containing all relations referenced by the join
// predicate (syntactic eligibility set from paper). SES is simply the set of
// base relations that are referenced by the join predicate. If a join uses a
// predicate in its ON condition, all relations in the SES must be part of the
// join's inputs. For example, in this query:
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
func (jb *JoinOrderBuilder) getSES(
	leftRels, rightRels relationSet, predicate memo.FiltersExpr,
) relationSet {
	// Get the columns referenced by the predicate.
	freeVars := jb.getFreeVars(predicate)

	// Add all relations referenced by the predicate to the SES relationSet.
	// Columns that do not come from a base relation (outer to the join tree) will
	// be treated as constant, and therefore will not be referenced in the SES.
	ses := jb.getRelations(freeVars)

	// In the case of a degenerate predicate (only references relations from one
	// side of the join) or a cross join, add relations from the unreferenced
	// side(s). This 'freezes' the predicate so that its join can only be
	// commuted. Example:
	//
	//    SELECT * FROM uv
	//    INNER JOIN (
	//   			SELECT * FROM xy
	//    		INNER JOIN ab
	//   	  	ON x = a
	//   	)
	//    ON True
	//
	// In this example, the syntactic eligibility set for the cross join predicate
	// would contain all three relations, so the cross join could not be pushed
	// down the join tree. This behavior is desirable because (1) it is rare that
	// the best plan involves creating new cross joins and (2) it constrains the
	// search space (which decreases planning time).
	if !ses.intersects(leftRels) {
		ses = ses.union(leftRels)
	}
	if !ses.intersects(rightRels) {
		ses = ses.union(rightRels)
	}

	return ses
}

// getFreeVars returns the set of columns referenced by the given predicate.
func (jb *JoinOrderBuilder) getFreeVars(predicate memo.FiltersExpr) opt.ColSet {
	var freeVars opt.ColSet
	for i := range predicate {
		freeVars.UnionWith(predicate[i].ScalarProps().OuterCols)
	}
	return freeVars
}

// hasColEq returns true if the given FiltersExpr contains a direct equality
// between the two given columns (e.g. x = a).
func (jb *JoinOrderBuilder) hasColEq(
	filters memo.FiltersExpr, leftCol, rightCol opt.ColumnID,
) bool {
	for i := range filters {
		if filters[i].ScalarProps().FuncDeps.AreColsEquiv(leftCol, rightCol) {
			return true
		}
	}
	return false
}

// getRelationIndexes returns a pair of relationIndexes representing the base
// relations that output the given pair of columns.
func (jb *JoinOrderBuilder) getRelationIndexes(col1, col2 opt.ColumnID) (idx1, idx2 relationIndex) {
	var found1, found2 bool
	for i := range jb.vertexes {
		if !found1 && jb.vertexes[i].Relational().OutputCols.Contains(col1) {
			idx1 = relationIndex(i)
			found1 = true
		}
		if !found2 && jb.vertexes[i].Relational().OutputCols.Contains(col2) {
			idx2 = relationIndex(i)
			found2 = true
		}
		if found1 && found2 {
			return idx1, idx2
		}
	}
	panic(errors.AssertionFailedf(
		"one or both columns not from a base relation: %v, %v", col1, col2))
}

// addJoin adds a join between the given left and right subsets of relations on
// the given set of edges. If the group containing joins between this set of
// relations is already contained in the plans field, the new join is added to
// the memo group. Otherwise, the join is memoized (possibly constructing a new
// group).
func (jb *JoinOrderBuilder) addJoin(op opt.Operator, s1, s2 relationSet, edges memo.FiltersExpr) {
	if s1.intersects(s2) {
		panic(errors.AssertionFailedf("sets are not disjoint"))
	}
	if jb.onAddJoinFunc != nil {
		// Hook for testing purposes.
		jb.callOnAddJoinFunc(s1, s2, edges)
	}
	left := jb.plans[s1]
	right := jb.plans[s2]
	union := s1.union(s2)
	if jb.plans[union] != nil {
		jb.addJoinToGroup(op, left, right, edges, jb.plans[union])
	} else {
		jb.plans[union] = jb.memoizeJoin(op, left, right, edges)
	}
}

// getEdges returns all edges that (1) reference only relations from the two
// given sets of base relations and (2) reference relations from both sets of
// base relations. It also returns a boolean that indicates whether the two sets
// are connected by at least one edge.
func (jb *JoinOrderBuilder) getEdges(s1, s2 relationSet) (edges memo.FiltersExpr, connected bool) {
	if jb.plans[s1] == nil || jb.plans[s2] == nil {
		// Both inputs must have plans.
		return nil, false
	}
	for ses := range jb.edges {
		// All relations in the edge's syntactic eligibility set must be in s1 or
		// s2. In addition, s1 and s2 must be connected by the edge.
		if ses.isSubset(s1.union(s2)) && ses.intersects(s1) && ses.intersects(s2) {
			edges = append(edges, jb.edges[ses]...)
			connected = true
		}
	}
	if connected && edges != nil {
		// Sort the edges because the map will randomize order. Then, remove
		// duplicate filters.
		edges.Sort()
		var fds props.FuncDepSet
		fds.AddEquivFrom(&jb.plans[s1].Relational().FuncDeps)
		fds.AddEquivFrom(&jb.plans[s2].Relational().FuncDeps)
		newEdges := make(memo.FiltersExpr, 0, len(edges))
		for i := range edges {
			if col1, col2, ok := extractEquivCols(&edges[i]); ok {
				if fds.AreColsEquiv(col1, col2) {
					// This filter is redundant.
					continue
				}
			}
			newEdges = append(newEdges, edges[i])
			fds.AddEquivFrom(&edges[i].ScalarProps().FuncDeps)
		}
		edges = newEdges
	}
	return edges, connected
}

func (jb *JoinOrderBuilder) addJoinToGroup(
	op opt.Operator, left, right memo.RelExpr, on memo.FiltersExpr, grp memo.RelExpr,
) {
	switch op {
	case opt.InnerJoinOp:
		newJoin := &memo.InnerJoinExpr{
			Left:        left,
			Right:       right,
			On:          on,
			JoinPrivate: memo.JoinPrivate{},
		}
		jb.f.Memo().AddInnerJoinToGroup(newJoin, grp)

	default:
		panic(errors.AssertionFailedf("invalid operator: %v", op))
	}
}

func (jb *JoinOrderBuilder) memoizeJoin(
	op opt.Operator, left, right memo.RelExpr, on memo.FiltersExpr,
) memo.RelExpr {
	switch op {
	case opt.InnerJoinOp:
		grp := jb.f.Memo().MemoizeInnerJoin(left, right, on, &memo.JoinPrivate{WasReordered: true})
		return grp

	default:
		panic(errors.AssertionFailedf("invalid operator: %v", op))
	}
}

// getRelations returns a relationSet containing all relations with output
// columns in the given ColSet.
func (jb *JoinOrderBuilder) getRelations(cols opt.ColSet) relationSet {
	var rels relationSet
	if cols.Empty() {
		return rels
	}
	for i := range jb.vertexes {
		if jb.vertexes[i].Relational().OutputCols.Intersects(cols) {
			rels = rels.add(relationIndex(i))
		}
	}
	return rels
}

// addBaseRelation adds the given RelExpr to the vertexes of the join graph.
// Before a relation is added, the number of vertexes is checked to ensure that
// the number of vertexes can be represented by an int64 bit set.
func (jb *JoinOrderBuilder) addBaseRelation(rel memo.RelExpr) {
	jb.checkSize()
	jb.vertexes = append(jb.vertexes, rel)
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

// extractEquivCols returns the left and right equivalent columns if the given
// FiltersItem is a single equality between those columns, as well as a
// boolean indicating whether the operation was successful.
func extractEquivCols(filter *memo.FiltersItem) (col1, col2 opt.ColumnID, ok bool) {
	eq, ok := filter.Condition.(*memo.EqExpr)
	if !ok {
		return 0, 0, false
	}
	var1, ok := eq.Left.(*memo.VariableExpr)
	if !ok {
		return 0, 0, false
	}
	var2, ok := eq.Right.(*memo.VariableExpr)
	if !ok {
		return 0, 0, false
	}
	return var1.Col, var2.Col, true
}

// NotifyOnReorder sets a callback function that is called when a join is passed
// to JoinOrderBuilder to be reordered.
func (jb *JoinOrderBuilder) NotifyOnReorder(
	onReorder func(memo.RelExpr, []memo.RelExpr, memo.FiltersExpr),
) {
	jb.onReorderFunc = onReorder
}

// NotifyOnAddJoin sets a callback function that is called when a join is added
// to the memo.
func (jb *JoinOrderBuilder) NotifyOnAddJoin(onAddJoin func(left, right, all, refs []memo.RelExpr)) {
	jb.onAddJoinFunc = onAddJoin
}

// callOnReorderFunc calls the onReorderFunc callback function. Panics if the
// function is nil.
func (jb *JoinOrderBuilder) callOnReorderFunc(join memo.RelExpr) {
	// Get a slice with all edges of the join graph.
	edgeSlice := make(memo.FiltersExpr, 0, len(jb.edges))
	for _, filters := range jb.edges {
		edgeSlice = append(edgeSlice, filters...)
	}

	// Sort because the map randomizes the order of the edges.
	edgeSlice.Sort()

	jb.onReorderFunc(join, jb.vertexes, edgeSlice)
}

// callOnAddJoinFunc calls the onAddJoinFunc callback function. Panics if the
// function is nil.
func (jb *JoinOrderBuilder) callOnAddJoinFunc(s1, s2 relationSet, edges memo.FiltersExpr) {
	jb.onAddJoinFunc(
		jb.getRelationSlice(s1),
		jb.getRelationSlice(s2),
		jb.getRelationSlice(s1.union(s2)),
		jb.getRelationSlice(jb.getRelations(jb.getFreeVars(edges))),
	)
}

// getRelationSlice returns the base relations represented by the given
// relationSet.
func (jb *JoinOrderBuilder) getRelationSlice(relations relationSet) (relSlice []memo.RelExpr) {
	relSlice = make([]memo.RelExpr, 0, relations.len())
	for idx, ok := relations.next(0); ok; idx, ok = relations.next(idx + 1) {
		relSlice = append(relSlice, jb.vertexes[idx])
	}
	return relSlice
}

// relationIndex represents the ordinal position of a base relation in the
// JoinOrderBuilder vertexes field. relationIndex must be less than maxSetSize.
type relationIndex uint64

// relationSet is a bitset of relationIndex values.
type relationSet uint64

const maxSetSize = 64

// add returns a copy of the relationSet with the given element added.
func (s relationSet) add(idx relationIndex) relationSet {
	if idx > maxSetSize-1 {
		panic(errors.AssertionFailedf("cannot insert %d into relationSet", idx))
	}
	return s | (1 << idx)
}

// union returns the set union of this set with the given set.
func (s relationSet) union(o relationSet) relationSet {
	return s | o
}

// intersection returns the set intersection of this set with the given set.
func (s relationSet) intersection(o relationSet) relationSet {
	return s & o
}

// difference returns the set difference of this set with the given set.
func (s relationSet) difference(o relationSet) relationSet {
	return s & ^o
}

// intersects returns true if this set and the given set intersect.
func (s relationSet) intersects(o relationSet) bool {
	return s.intersection(o) != 0
}

// isSubset returns true if this set is a subset of the given set.
func (s relationSet) isSubset(o relationSet) bool {
	return s.union(o) == o
}

// isSingleton returns true if the set has exactly one element.
func (s relationSet) isSingleton() bool {
	return s > 0 && (s&(s-1)) == 0
}

// next returns the next element in the set after the given start index, and
// a bool indicating whether such an element exists.
func (s relationSet) next(startVal relationIndex) (elem relationIndex, ok bool) {
	if startVal < maxSetSize {
		if ntz := bits.TrailingZeros64(uint64(s >> startVal)); ntz < 64 {
			return startVal + relationIndex(ntz), true
		}
	}
	return relationIndex(math.MaxInt64), false
}

// len returns the number of elements in the set.
func (s relationSet) len() int64 {
	return int64(bits.OnesCount64(uint64(s)))
}
