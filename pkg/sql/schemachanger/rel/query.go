// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rel

import (
	"sort"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// Query searches for sets of entities which uphold a set of constraints.
type Query struct {
	schema *Schema
	// clauses are the original clauses. They exist for debugging.
	clauses []Clause
	// variables is the set of variables used in the query
	// stored in the order in which they appear.
	variables []Var
	// variableSlots is the mapping of names to slots.
	variableSlots map[Var]slotIdx
	// entities is the mapping of entities to slots.
	entities []slotIdx
	// slots store the data and metadata about the slots.
	slots []slot
	// facts are the set of facts which must be unified.
	facts []fact
	// filters are the set of predicate filters to evaluate.
	filters []filter
	// notJoins are sub-queries which, if successfully unified, imply a
	// contradiction in the outer query.
	notJoins []subQuery

	// cache one evalContext for reuse to accelerate benchmarks and deal with
	// the common case.
	mu struct {
		syncutil.Mutex
		cached *evalContext
	}
}

// queryDepth is a depth in the join order of a query.
type queryDepth uint16

type subQuery struct {
	query             *Query
	depth             queryDepth
	inputSlotMappings util.FastIntMap
}

// Result represents A setting of entities which fulfills the
// constraints of its corresponding query. It is a rather low-level
// interface.
type Result interface {

	// Var returns the value bound to the given variable.
	// If the variable does not exist in the query, nil will be
	// returned.
	Var(name Var) interface{}
}

// ResultIterator is used to iterate results of A query.
// Iteration can be halted with the use of iterutils.StopIteration.
type ResultIterator func(r Result) error

// NewQuery construct a new query with the provided clauses forming the
// conjunction of constraints on the results of the query when it is
// evaluated against a database.
func NewQuery(sc *Schema, clauses ...Clause) (_ *Query, err error) {
	defer func() {
		switch r := recover().(type) {
		case nil:
			return
		case error:
			err = errors.Wrap(r, "failed to construct query")
		default:
			err = errors.AssertionFailedf("failed to construct query: %v", r)
		}
	}()
	q := newQuery(sc, clauses)
	return q, nil
}

// Iterate will call the result iterator for every valid binding of each
// distinct entity variable such that all the variables in the query are
// bound and all filters passing.
func (q *Query) Iterate(db *Database, ri ResultIterator) error {
	ec := q.getEvalContext()
	defer q.putEvalContext(ec)
	return ec.Iterate(db, ri)
}

// getEvalContext grabs a cached evalContext from the query
// if one exists, otherwise it creates a new one.
func (q *Query) getEvalContext() *evalContext {
	getCachedEvalContext := func() (ec *evalContext) {
		q.mu.Lock()
		defer q.mu.Unlock()
		ec, q.mu.cached = q.mu.cached, ec
		return ec
	}
	if ec := getCachedEvalContext(); ec != nil {
		return ec
	}
	return newEvalContext(q)
}

// putEvalContext puts the evalContext in the cache if there is not one.
func (q *Query) putEvalContext(ec *evalContext) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.mu.cached == nil {
		q.mu.cached = ec
	}
}

// Entities returns the entities in the query in their join order.
// This method exists primarily for introspection.
func (q *Query) Entities() []Var {
	var entitySlots intsets.Fast
	for _, slotIdx := range q.entities {
		entitySlots.Add(int(slotIdx))
	}
	vars := make([]Var, 0, len(q.entities))
	for v, slotIdx := range q.variableSlots {
		if !entitySlots.Contains(int(slotIdx)) {
			continue
		}
		vars = append(vars, v)
	}
	sort.Slice(vars, func(i, j int) bool {
		return q.variableSlots[vars[i]] < q.variableSlots[vars[j]]
	})
	return vars
}

// Clauses returns the query's Clauses.
func (q *Query) Clauses() Clauses {
	return q.clauses
}
