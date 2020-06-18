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
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/errors"
)

// joinOrderBuilder is used to add valid orderings of a given join tree to the
// memo during exploration.
//
// How joinOrderBuilder works
// --------------------------
//
// First, the initial join tree is traversed and encoded as a graph (technically
// a hypergraph). The 'leaves' of the join tree become the vertexes of the
// graph, and the edges are built using the ON conditions of the joins. For
// example, take this query:
//
//		SELECT *
//		FROM
//	  	ab,
//			cd,
//			(SELECT * FROM uv WHERE u = 5),
//			xy
//		WHERE
//	  	x = u AND
//			b = c AND
//			a = u
//
// xy has 1,000,000 rows. The other tables have 1,000 rows each.
//
// The vertexes of the graph would be initialized with base relations ab, cd,
// (SELECT * FROM uv WHERE u = 5), and xy. The edges correspond to relationships
// between those base relations. For example, an edge exists between the xy and
// uv vertexes, because of the x = u relationship.
//
// Next, equivalence closure is computed for the graph edges. In this example,
// the filter a = x would be added to the graph, since a = u and x = u. At this
// point, if the graph were translated back into query form it would look
// something like this:
//
//		SELECT *
//		FROM
//	  	ab,
//			cd,
//			(SELECT * FROM uv WHERE u = 5),
//			xy
//		WHERE
//	  	x = u AND
//			b = c AND
//			a = u AND
//			a = x
//
// Finally, the DPSube algorithm is executed. DPSube enumerates all disjoint
// pairs of subsets of base relations such as ([xy], [ab]) or ([xy, ab], [ub]).
// This is done efficiently using bit sets. For each pair of relation subsets, a
// join is added to the memo as long as there exists at least one edge that
// connects the two subsets. All connecting edges are added to the new join's ON
// condition.
//
// The following is an example of a plan for the above query that takes
// advantage of join reordering:
//
//		inner-join (lookup xy)
//			├── lookup columns are key
//			├── inner-join (lookup cd)
//			│    ├── lookup columns are key
//			│    ├── inner-join (lookup ab)
//			│    │    ├── lookup columns are key
//			│    │    ├── scan uv
//			│    │    │    └── constraint: /5: [/5 - /5]
//			│    │    └── filters (true)
//			│    └── filters (true)
//			└── filters
//           └── a = x
//
// Note that the implicit a = x edge that wasn't present in the original query
// is used in the merge join between xy and uv. Also note that the xy and ab
// scans have become constrained by the implicit x = 5 and a = 5 filters,
// respectively. For comparison, here's the plan for this query that results
// from setting the join reorder limit to zero:
//
//		inner-join (hash)
//			├── inner-join (hash)
//			│    ├── scan ab
//			│    ├── scan cd
//			│    └── filters
//			│         └── b = c
//			├── inner-join (lookup xy)
//			│    ├── lookup columns are key
//			│    ├── scan uv
//			│    │    └── constraint: /5: [/5 - /5]
//			│    └── filters (true)
//			└── filters
//           └── a = u
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
// Currently, joinOrderBuilder can only reorder join trees containing
// InnerJoins. However, the algorithm is extensible to non-inner joins.
// TODO(drewk): add support for other join operators.
//
// Citations: [8]
type joinOrderBuilder struct {
	mem     *memo.Memo
	f       *norm.Factory
	funcs   CustomFuncs
	evalCtx *tree.EvalContext

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

	// Plans maps from a set of base relations to the memo group for the join tree
	// that contains those relations (and only those relations). As an example,
	// the group for [xy, ab, uv] might contain the join trees (xy, (ab, uv)),
	// ((xy, ab), uv), (ab, (xy, uv)), etc.
	//
	// The group for a single base relation is simply the base relation itself.
	plans map[relationSet]memo.RelExpr

	// edgeFDs is a set of equivalencies used to compute equivalence closure for
	// the edges of the join graph.
	edgeFDs props.FuncDepSet
}

func (jb *joinOrderBuilder) Init(
	mem *memo.Memo, f *norm.Factory, funcs CustomFuncs, evalCtx *tree.EvalContext,
) {
	jb.mem = mem
	jb.f = f
	jb.funcs = funcs
	jb.evalCtx = evalCtx

	jb.vertexes = []memo.RelExpr{}
	jb.edges = map[relationSet]memo.FiltersExpr{}
	jb.plans = map[relationSet]memo.RelExpr{}
	jb.edgeFDs = props.FuncDepSet{}
}

// Reorder adds all valid (non-commutative) orderings of the given join to the
// memo. Currently, only InnerJoins are reordered.
func (jb *joinOrderBuilder) Reorder(join memo.RelExpr) {
	switch t := join.(type) {
	case *memo.InnerJoinExpr:
		if !t.Flags.Empty() {
			panic(errors.AssertionFailedf("join with hints cannot be reordered"))
		}

		// Populate the vertexes and edges of the join hypergraph.
		jb.populateGraph(join)

		// Ensure equivalence closure for edges that don't reference outer columns.
		jb.ensureClosure(t.Relational().OutputCols.Difference(t.Relational().OuterCols))

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
func (jb *joinOrderBuilder) populateGraph(rel memo.RelExpr) {
	switch t := rel.(type) {
	case *memo.InnerJoinExpr:
		if !t.Flags.Empty() {
			// If the join has flags, we can't reorder it. Simply treat it as a base
			// relation.
			jb.checkSize()
			jb.vertexes = append(jb.vertexes, t)
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
		relSet := relationSet((1<<(endIdx-startIdx) - 1) << startIdx)
		jb.plans[relSet] = t

	default:
		jb.checkSize()
		jb.vertexes = append(jb.vertexes, t)

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
func (jb *joinOrderBuilder) ensureClosure(nonOuterCols opt.ColSet) {
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
				var1 := jb.mem.MemoizeVariable(col1)
				var2 := jb.mem.MemoizeVariable(col2)
				if !jb.hasColEq(jb.edges[relations], col1, col2) {
					// This equality is not yet represented among the edges.
					condition := jb.mem.MemoizeEq(var1, var2)
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
func (jb *joinOrderBuilder) dpSube() {
	subsets := relationSet((1 << len(jb.vertexes)) - 1)
	for subset := relationSet(1); subset <= subsets; subset++ {
		if subset.isSingleton() {
			// This subset has only one set bit, which means it only represents one
			// relation. We need at least two relations in order to create a new join.
			continue
		}
		// Enumerate all possible pairwise-disjoint binary partitions of the subset,
		// s1 AND s2. These represent sets of relations that may be joined together.
		for s1 := relationSet(1); s1 <= subset/2; s1++ {
			if !s1.isSubset(subset) {
				continue
			}
			s2 := subset.difference(s1)
			if edges, ok := jb.getEdges(s1, s2); ok {
				// First sort the ON condition to ensure that plans are deterministic
				// (the edges are stored in a map, so the order is randomized).
				edges.Sort()
				jb.addJoin(opt.InnerJoinOp, s1, s2, edges)
			}
		}
	}
}

// makeEdges creates a set of hyper-graph edges for the given join and adds them
// to the edges field. Each edge is mapped to its SES (syntactic eligibility
// set, defined in the getSES comment). For InnerJoins, an edge is created for
// each conjunct in the join's ON condition.
func (jb *joinOrderBuilder) makeEdges(join memo.RelExpr) {
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
func (jb *joinOrderBuilder) getSES(
	leftRels, rightRels relationSet, predicate memo.FiltersExpr,
) relationSet {
	// Get the columns referenced by the predicate.
	var freeVars opt.ColSet
	for i := range predicate {
		freeVars.UnionWith(predicate[i].ScalarProps().OuterCols)
	}

	// Add all relations referenced by the predicate to the SES relationSet. Outer
	// columns will be ignored. This is correct because outer columns can be
	// treated as constant.
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

// hasColEq returns true if the given FiltersExpr contains a direct equality
// between the two given columns (e.g. x = a).
func (jb *joinOrderBuilder) hasColEq(
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
func (jb *joinOrderBuilder) getRelationIndexes(col1, col2 opt.ColumnID) (idx1, idx2 relationIndex) {
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
func (jb *joinOrderBuilder) addJoin(op opt.Operator, s1, s2 relationSet, edges memo.FiltersExpr) {
	if s1.intersects(s2) {
		panic(errors.AssertionFailedf("sets are not disjoint"))
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
func (jb *joinOrderBuilder) getEdges(s1, s2 relationSet) (edges memo.FiltersExpr, connected bool) {
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
	return edges, connected
}

func (jb *joinOrderBuilder) addJoinToGroup(
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
		jb.mem.AddInnerJoinToGroup(newJoin, grp)

	default:
		panic(errors.AssertionFailedf("invalid operator: %v", op))
	}
}

func (jb *joinOrderBuilder) memoizeJoin(
	op opt.Operator, left, right memo.RelExpr, on memo.FiltersExpr,
) memo.RelExpr {
	switch op {
	case opt.InnerJoinOp:
		grp := jb.mem.MemoizeInnerJoin(left, right, on, &memo.JoinPrivate{})
		return grp

	default:
		panic(errors.AssertionFailedf("invalid operator: %v", op))
	}
}

// getRelations returns a relationSet containing all relations with output columns
// in the given ColSet.
func (jb *joinOrderBuilder) getRelations(cols opt.ColSet) relationSet {
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

// checkSize panics if the number of relations is greater than or equal to
// MaxReorderJoinsLimit. checkSize should be called before a vertex is added to
// the join graph.
func (jb *joinOrderBuilder) checkSize() {
	if len(jb.vertexes) >= opt.MaxReorderJoinsLimit {
		panic(
			errors.AssertionFailedf(
				"cannot reorder a join tree with more than %v relations",
				opt.MaxReorderJoinsLimit,
			),
		)
	}
}

// relationIndex represents the ordinal position of a base relation in the
// joinOrderBuilder vertexes field. relationIndex must be less than maxSetSize.
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
