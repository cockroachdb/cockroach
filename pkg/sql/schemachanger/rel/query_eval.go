// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rel

import (
	"reflect"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/intsets"
	"github.com/cockroachdb/errors"
)

// evalContext implements Result and accumulates the state during the
// evaluation of A query.
type evalContext struct {
	q  *Query
	db *Database
	ri ResultIterator

	facts []fact

	// depth and cur relate to the join depth in the entities list.
	depth, cur queryDepth

	// Keeps track of stats for the query. Is reset each time a new query is run.
	stats QueryStats

	slots             []slot
	slotResetCount    []int8 // Keeps track of the number of times a slot has been reset
	filterSliceCaches map[int][]reflect.Value
	curSubQuery       int
}

type QueryStats struct {
	// StartTime is the timestamp when the query started.
	StartTime time.Time
	// ResultsFound tracks the number of results returned by the query.
	ResultsFound int
	// FirstUnsatisfiedClause is the index of the first clause in the query that
	// wasn't satisfied. This is set when a query runs and no results are found
	// (ResultsFound is zero). The index refers to an entry in []Query.clauses.
	FirstUnsatisfiedClause int
	// FiltersCheckStarted indicates whether the query completed all slots and
	// began applying filters.
	FiltersCheckStarted bool
	// LastFilterChecked is the index of the last filter applied. This is valid
	// only if FiltersCheckStarted is true.
	LastFilterChecked int
}

func newEvalContext(q *Query) *evalContext {
	return &evalContext{
		q:              q,
		depth:          queryDepth(len(q.entities)),
		slots:          cloneSlots(q.slots),
		slotResetCount: make([]int8, len(q.slots)),
		facts:          q.facts,
	}
}

// cloneSlots clones the slots of a query for use in an evalContext.
func cloneSlots(slots []slot) []slot {
	clone := append(make([]slot, 0, len(slots)), slots...)
	for i := range clone {
		// If there are any slots which map to a set of allowed values, we need
		// to clone those values because during query evaluation, we'll fill in
		// inline values in the context of the current entity set. This matters
		// in particular for constraints related to entities or strings; their
		// inline values depend on the entitySet.
		if clone[i].any != nil {
			vals := clone[i].any
			clone[i].any = append(make([]typedValue, 0, len(vals)), vals...)
		}
		if clone[i].not != nil {
			cloned := *clone[i].not
			clone[i].not = &cloned
		}
	}
	return clone
}

type evalResult evalContext

func (ec *evalResult) Var(name Var) interface{} {
	n, ok := ec.q.variableSlots[name]
	if !ok {
		return nil
	}
	return ec.slots[n].toInterface()
}

// Iterate is part of the PreparedQuery interface.
func (ec *evalContext) Iterate(db *Database, ri ResultIterator) error {
	if db.schema != ec.q.schema {
		return errors.Errorf(
			"query and database are not from the same schema: %s != %s",
			db.schema.name, ec.q.schema.name,
		)
	}
	defer func() { ec.db, ec.ri = nil, nil }()
	ec.db, ec.ri = db, ri

	// TODO(ajwerner): Decide if we should allow depth-zero queries to exist.
	if ec.depth == 0 {
		return nil
	}
	return ec.iterateNext()
}

// iterateNext steps down in the join to either decide that we've
// iterated through all the entity variables and need to process
// filters and pass along the result or that we need to go on and
// join the next entity.
func (ec *evalContext) iterateNext() error {
	nextSubQuery, done, err := ec.maybeVisitSubqueries()
	if done || err != nil {
		return err
	}
	if curSubQuery := ec.curSubQuery; nextSubQuery != curSubQuery {
		defer func() { ec.curSubQuery = curSubQuery }()
		ec.curSubQuery = nextSubQuery
	}
	// We're at the bottom of the iteration, check if all conditions have
	// been satisfied, and then invoke the iterator.
	if ec.cur == ec.depth {
		if ec.haveUnboundSlots() || ec.checkFilters() {
			return nil
		}
		ec.stats.ResultsFound++
		return ec.ri((*evalResult)(ec))
	}

	// If we've already populated the next entity in the join as variable,
	// skip iterating the database.
	if done, err := ec.maybeVisitAlreadyBoundEntity(); done || err != nil {
		return err
	}

	// If there exists an any clause, which forms a disjunction, over some
	// attribute for the next entity, iterate independently over each of the
	// values.
	where, hasAttrs, anyAttr, anyValues, err := ec.buildWhere()
	if err != nil {
		return err
	}
	if len(anyValues) > 0 {
		for i := range anyValues {
			// Interact with the slice directly in order to potentially set
			// the inline value.
			v := &anyValues[i]
			iv, err := v.inlineValue(&ec.db.entitySet, anyAttr)
			if err != nil {
				return err
			}
			if !where.add(anyAttr, iv) {
				return errors.AssertionFailedf(
					"failed to create predicate with more than %d attributes", numAttrs,
				)
			}
			if err := ec.db.iterate(where, hasAttrs, ec); err != nil {
				return err
			}
		}
		return nil
	}
	// If there's no anyValues, directly iterate the database.
	return ec.db.iterate(where, hasAttrs, ec)
}

func (ec *evalContext) visit(e entity) error {
	// Keep track of which slots were filled as part of this step in the
	// evaluation and then unset them when we pop out of this stack frame.
	var slotsFilled intsets.Fast
	defer func() {
		slotsFilled.ForEach(func(i int) {
			ec.slotResetCount[i]++
			ec.slots[i].reset()
		})
	}()

	// Fill in the slot corresponding to this entity. It should not be filled
	// because we wouldn't have done an iteration here to discover such a
	// conflict if it were. See (*evalContext).maybeVisitAlreadyBoundEntity().
	if foundContradiction := maybeSet(
		ec.slots, ec.q.entities[ec.cur],
		typedValue{
			typ:   e.getTypeInfo(&ec.db.entitySet).typ,
			value: e.getSelf(&ec.db.entitySet),
		},
		&slotsFilled,
	); foundContradiction {
		return errors.AssertionFailedf(
			"unexpectedly found contradiction setting entity",
		)
	}

	// Fill in the slots corresponding to this entity and its attributes.
	setEntitySlots := func() (foundContradiction bool) {
		// TODO(ajwerner): Constrain to just the facts about this variable.
		for _, f := range ec.facts {
			if f.variable != ec.q.entities[ec.cur] {
				continue
			}

			tv, ok := e.getTypedValue(&ec.db.entitySet, f.attr)
			if !ok {
				return true // we have no value for this attribute, contradiction
			}
			if contradiction := maybeSet(
				ec.slots, f.value, tv, &slotsFilled,
			); contradiction {
				return true
			}
		}
		// TODO(ajwerner): Could recognize at this point that if any fact about
		// this entity don't have values it's because the entity does not have
		// a defined value for the corresponding attribute and thus we've found a
		// contradiction. At time of writing, this is deferred until later.
		return false
	}

	// Propagate the new information and see if there's a contradiction.
	if contradiction := setEntitySlots() || unify(
		ec.facts, ec.slots, &slotsFilled,
	); contradiction {
		return nil
	}

	// Step down to the next variable, or, if at the bottom, ensure that
	// all the required slots are filled and pass the result to the caller.
	ec.cur++
	defer func() { ec.cur-- }()
	return ec.iterateNext()
}

// Check to see if all the variableSlots have been assigned a value.
// If not, then we did not successfully unify everything.
func (ec *evalContext) haveUnboundSlots() bool {
	for _, v := range ec.q.variableSlots {
		if ec.slots[v].value == nil {
			return true
		}
	}
	return false
}

func (ec *evalContext) checkFilters() (done bool) {
	ec.stats.FiltersCheckStarted = true
	ec.stats.LastFilterChecked = 0
	for i := range ec.q.filters {
		if done = ec.checkFilter(i); done {
			return true
		}
		ec.stats.LastFilterChecked++
	}
	return false
}

func (ec *evalContext) checkFilter(i int) bool {
	f := ec.q.filters[i]
	ins := ec.getFilterInput(i)
	defer func() {
		for i := range f.input {
			ins[i] = reflect.Value{}
		}
	}()
	for i, idx := range f.input {
		inI := ec.slots[idx].typedValue.toInterface()
		in := reflect.ValueOf(inI)
		// Note that this will enforce that the type of the input to the filter
		// matches the expectation by omitting results of the wrong type. This
		// may or may not be the right behavior.
		//
		// TODO(ajwerner): Enforce the typing at a lower layer. See the TODO
		// where this filter was built.
		inType := f.predicate.Type().In(i)
		if in.Type() != inType {
			if in.Type().ConvertibleTo(inType) {
				in = in.Convert(inType)
			} else {
				return true
			}
		}
		ins[i] = in
	}
	// TODO(ajwerner): Catch panics here and convert them to errors.
	outs := f.predicate.Call(ins)
	return !outs[0].Bool()
}

// Construct a where clause with all the bound values known for the next
// entity in the join. In the face of an existing any clause for the current
// entity, the corresponding attribute and values will be returned for use
// breaking the disjunction into separate indexed searches for each value.
func (ec *evalContext) buildWhere() (
	where values,
	hasAttrs ordinalSet,
	anyAttr ordinal,
	anyValues []typedValue,
	_ error,
) {
	hasAttrs = hasAttrs.
		add(ec.db.schema.typeOrdinal).
		add(ec.db.schema.selfOrdinal)
	// The logic here is that if there's an any for a slotIdx with a fact for the
	// current entity, maybe we want to use it to bound our search. In general,
	// it will help if we have an index that covers the current facts plus this
	// value. There may be more than one any, in which case, this is not going to
	// be very smart.
	//
	// TODO(ajwerner): Make this filter push-down smarter based on the indexes
	// which exist.
	for _, f := range ec.facts {
		if f.variable != ec.q.entities[ec.cur] {
			continue
		}
		hasAttrs = hasAttrs.add(f.attr)
		if s := &ec.slots[f.value]; !s.empty() {
			p, err := s.inlineValue(&ec.db.entitySet, f.attr)
			if err != nil {
				return values{}, 0, 0, nil, err
			}
			if !where.add(f.attr, p) {
				return values{}, 0, 0, nil, errors.AssertionFailedf(
					"failed to create predicate with more than %d attributes", numAttrs,
				)
			}
		} else if anyValues == nil && s.any != nil {
			anyAttr, anyValues = f.attr, s.any
		}
	}
	return where, hasAttrs, anyAttr, anyValues, nil
}

// unify is like unifyReturningContradiction but it does not return the fact.
func unify(facts []fact, s []slot, slotsFilled *intsets.Fast) (contradictionFound bool) {
	contradictionFound, _ = unifyReturningContradiction(facts, s, slotsFilled)
	return contradictionFound
}

// unifyReturningContradiction attempts to unify away facts by assigning
// values to slots. If a contradiction is found, a fact involved in the
// contradiction is returned. Any slots set in the process of unification
// are recorded into the set.
func unifyReturningContradiction(
	facts []fact, s []slot, slotsFilled *intsets.Fast,
) (contradictionFound bool, contradicted fact) {
	// TODO(ajwerner): As we unify we could determine that some facts are no
	// longer relevant. When we do that we could move them to the front and keep
	// track of some offset. In principle, we could do this and then each time
	// we step back up a frame, merge sort the fact back into sorted order and,
	// in that way, reduce the slotsFilled of facts which need to be searched while
	// still requiring only linear operations in the number of facts. As it
	// stands, this algorithm is quadratic in the number of facts.
	for {
		var somethingChanged bool
		var prev, cur *fact
		for i := 1; i < len(facts); i++ {
			prev, cur = &facts[i-1], &facts[i]
			if prev.variable != cur.variable ||
				prev.attr != cur.attr ||
				// If the slots are the same, they've already been unified.
				// TODO(ajwerner): We could probably prune equal facts elsewhere.
				prev.value == cur.value ||
				// Similarly, we could avoid looking at fully unified facts.
				s[prev.value].eq(s[cur.value]) {
				continue
			}

			var unified bool
			for _, v := range []struct {
				dst, src slotIdx
			}{
				{prev.value, cur.value},
				{cur.value, prev.value},
			} {
				if s[v.dst].empty() {
					if contradictionFound = maybeSet(
						s, v.dst, s[v.src].typedValue, slotsFilled,
					); contradictionFound {
						return true, *cur
					}
					unified = true
					break
				}
			}
			if unified {
				somethingChanged = true
			} else {
				return true, *cur
			}
		}
		if !somethingChanged {
			return false, fact{}
		}
	}
}

// Check if the slot is already filled with a value because it was
// already bound. If it does not exist in the database, then there's
// a contraction, and we can return. If it does, then we can visit
// the entity as opposed to needing to find it.
func (ec *evalContext) maybeVisitAlreadyBoundEntity() (done bool, _ error) {
	s := &ec.slots[ec.q.entities[ec.cur]]
	if s.empty() {
		return false, nil
	}
	eid, ok := ec.db.hash[s.value]

	// This is the case where we've somehow bound the entity to a
	// value that does not exist in the database. This could happen
	// if, say, the caller provided an entity as a value. I don't know
	// if we need to permit this, but it's definitely an edge. The claim
	// of the API is that we are going to iterate the set of entities in
	// the database which conform to the query.
	if !ok {
		return true, nil // contradiction
	}
	return true, ec.visit(ec.db.entities[eid])
}

func (ec *evalContext) getFilterInput(i int) (ins []reflect.Value) {
	if ec.filterSliceCaches == nil {
		ec.filterSliceCaches = make(map[int][]reflect.Value)
	}
	c, ok := ec.filterSliceCaches[i]
	if !ok {
		c = make([]reflect.Value, len(ec.q.filters[i].input))
		ec.filterSliceCaches[i] = c
	}
	return c
}

func (ec *evalContext) maybeVisitSubqueries() (nextSubQuery int, done bool, error error) {
	nextSubQuery = ec.curSubQuery
	for nextSubQuery < len(ec.q.notJoins) &&
		ec.q.notJoins[nextSubQuery].depth <= ec.cur {
		if done, err := ec.visitSubquery(nextSubQuery); done || err != nil {
			return ec.curSubQuery, done, err
		}
		nextSubQuery++
	}
	return nextSubQuery, false, nil
}

func (ec *evalContext) visitSubquery(query int) (done bool, _ error) {
	sub := ec.q.notJoins[query]
	sec := sub.query.getEvalContext()
	defer sub.query.putEvalContext(sec)
	defer func() { // reset the slots populated to run the subquery
		sub.inputSlotMappings.ForEach(func(_, subSlot int) {
			sec.slotResetCount[subSlot]++
			sec.slots[subSlot].reset()
		})
	}()
	if err := ec.bindSubQuerySlots(sub.inputSlotMappings, sec); err != nil {
		return false, err
	}
	err := sec.Iterate(ec.db, func(r Result) error {
		return errResultSetNotEmpty
	})
	switch {
	case err == nil:
		return false, nil
	case errors.Is(err, errResultSetNotEmpty):
		return true, nil
	default:
		return false, err
	}
}

func (ec *evalContext) bindSubQuerySlots(mapping util.FastIntMap, sec *evalContext) (err error) {
	mapping.ForEach(func(src, dst int) {
		if err != nil {
			return
		}
		if ec.slots[src].empty() {
			// TODO(ajwerner): Find a way to prove statically that this cannot
			// happen and make it an assertion failure.
			err = errors.Errorf(
				"subquery invocation references unbound variable %q",
				ec.findSlotVariable(src),
			)
		}
		sec.slots[dst].typedValue = ec.slots[src].typedValue
	})
	return err
}

func (ec *evalContext) findSlotVariable(src int) Var {
	for v, slot := range ec.q.variableSlots {
		if src == int(slot) {
			return v
		}
	}
	return ""
}

var errResultSetNotEmpty = errors.New("result set not empty")

// findFirstClauseNotSatisfied returns a clause that wasn't satisfied in the
// last query. There could be multiple clauses unsatisfied, this function
// returns the earliest one, which should be investigated first when debugging
// the rule. The index of the clause is returned, which can be used to lookup in
// Query.Clauses().
func (ec *evalContext) findFirstClauseNotSatisfied() (int, error) {
	minUnsatisfiedClauseInx := len(ec.q.clauses)
	for i := range ec.slots {
		if ec.slots[i].empty() && ec.slotResetCount[i] == 0 {
			minUnsatisfiedClauseInx = min(minUnsatisfiedClauseInx, ec.q.clauseIDs[i])
		}
	}

	if minUnsatisfiedClauseInx < len(ec.q.clauses) {
		return minUnsatisfiedClauseInx, nil
	}

	// If we found satisfied entries in all slots, it indicates that the filters,
	// which are evaluated at the end, did not satisfy the query. Locate the last
	// filter that was applied.
	if !ec.stats.FiltersCheckStarted || ec.stats.LastFilterChecked >= len(ec.q.filters) {
		return -1,
			errors.AssertionFailedf("cound not find a clause that was not satisfied: "+
				"FiltesCheckStarted=%t, LastFilterChecked=%d, len(filters)=%d",
				ec.stats.FiltersCheckStarted, ec.stats.LastFilterChecked, len(ec.q.filters))
	}
	return ec.q.filters[ec.stats.LastFilterChecked].clauseID, nil
}
