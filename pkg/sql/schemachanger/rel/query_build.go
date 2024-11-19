// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rel

import (
	"reflect"
	"sort"

	"github.com/cockroachdb/errors"
	"gopkg.in/yaml.v2"
)

type queryBuilder struct {
	sc            *Schema
	variables     []Var
	variableSlots map[Var]slotIdx
	facts         []fact
	slots         []slot
	filters       []filter

	// Track whether the slotIdx holds an entity separately. We want to
	// know this in planning, but it'll be implicit during execution.
	// This might be badly named. What we really mean here is that the
	// slotIdx is a join target.
	slotIsEntity []bool

	// This slice mirrors the slots slice. It identifies the clause that caused
	// each slot to be created. It is separate from the slots directory since its
	// constant from query to query, so it doesn't need to be cloned like the slots
	// directory is.
	clauseIDs []int

	// curClauseID is the ID of the clause to use for any new slots that are
	// created.
	curClauseID int

	notJoins []subQuery
}

// newQuery constructs a query. Errors are panicked and caught
// in the calling NewQuery function.
func newQuery(sc *Schema, clauses Clauses, cib *clauseIDBuilder) *Query {
	p := &queryBuilder{
		sc:            sc,
		variableSlots: map[Var]slotIdx{},
	}
	// Flatten away nested and clauses. We may need them at some point
	// if we add something like or-join or not-join. At time of writing,
	// the and case in processClause is an assertion failure.
	forDisplay := flattened(clauses)
	for _, displayClause := range forDisplay {
		// Advance the clause ID only for clauses in forDisplay. The clauseID is used
		// for debugging to report clauses that didn't match, so it must correspond
		// to the clauses shown for display purposes.
		p.curClauseID = cib.nextID()

		for _, t := range expanded([]Clause{displayClause}) {
			p.processClause(t, cib)
		}
	}
	for _, s := range p.variableSlots {
		p.facts = append(p.facts, fact{
			variable: s,
			attr:     sc.selfOrdinal,
			value:    s,
		})
	}

	// Order the facts for unification. The ordering is first by variable
	// and then by attribute.
	//
	// TODO(ajwerner): For disjunctions using Any, the code currently uses
	// the index to constrain the search for each value in the "first"
	// such fact for the variable. Maybe we should trust the user order of
	// facts for a given variable rather than sorting by attribute ordinal.
	// However, we do need all the facts with the same variable and attribute
	// to be adjacent for the unification fixed point evaluation to work.
	entities := p.findEntitySlots()
	p.setSubQueryDepths(entities)
	sort.SliceStable(p.facts, func(i, j int) bool {
		if p.facts[i].variable == p.facts[j].variable {
			return p.facts[i].attr < p.facts[j].attr
		}
		return p.facts[i].variable < p.facts[j].variable
	})

	// Remove any redundant facts.
	truncated := p.facts[:0]
	for i, f := range p.facts {
		if i == 0 || f != p.facts[i-1] {
			truncated = append(truncated, f)
		}
	}
	p.facts = truncated

	// Ensure that the query does not already contain a contradiction as that
	// is almost definitely a bug.
	if contradictionFound, contradiction := unifyReturningContradiction(
		p.facts, p.slots, nil,
	); contradictionFound {
		panic(errors.Errorf(
			"query contains contradiction on %v", sc.attrs[contradiction.attr],
		))
	}
	return &Query{
		schema:        sc,
		variables:     p.variables,
		variableSlots: p.variableSlots,
		clauses:       forDisplay,
		entities:      entities,
		facts:         p.facts,
		slots:         p.slots,
		filters:       p.filters,
		notJoins:      p.notJoins,
		clauseIDs:     p.clauseIDs,
	}
}

func (p *queryBuilder) processClause(t Clause, cib *clauseIDBuilder) {
	defer func() {
		if r := recover(); r != nil {
			rErr, ok := r.(error)
			if !ok {
				rErr = errors.AssertionFailedf("processClause: panic: %v", r)
			}
			encoded, err := yaml.Marshal(t)
			if err != nil {
				panic(errors.CombineErrors(rErr, errors.Wrap(
					err, "failed to encode clause",
				)))
			}
			panic(errors.Wrapf(
				rErr, "failed to process invalid clause %s", encoded,
			))
		}
	}()
	switch t := t.(type) {
	case tripleDecl:
		p.processTripleDecl(t)
	case eqDecl:
		p.processEqDecl(t)
	case filterDecl:
		p.processFilterDecl(t)
	case ruleInvocation:
		if !t.rule.isNotJoin {
			panic(errors.AssertionFailedf("rule invocations which aren't not-joins" +
				" should have been flattened away"))
		}
		p.processNotJoin(t, cib)
	case and:
		panic(errors.AssertionFailedf("and clauses should be flattened away"))
	default:
		panic(errors.AssertionFailedf("unknown clause type %T", t))
	}
}

func (p *queryBuilder) processTripleDecl(fd tripleDecl) {
	ord := p.sc.mustGetOrdinal(fd.attribute)
	if p.maybeHandleContains(fd, ord) {
		return
	}
	f := fact{
		variable: p.maybeAddVar(fd.entity, true /* entity */),
		attr:     ord,
		value:    p.processValueExpr(fd.value),
	}
	p.typeCheck(f)
	p.facts = append(p.facts, f)
}

func (p *queryBuilder) maybeHandleContains(fd tripleDecl, ord ordinal) (isContains bool) {
	contains, isContains := fd.value.(containsExpr)
	isContainAttr := p.sc.sliceOrdinals.contains(ord)
	switch {
	case !isContains && !isContainAttr:
		return false
	case !isContainAttr:
		panic(errors.Errorf("cannot use Contains for non-slice attribute %v", fd.attribute))
	case !isContains:
		panic(errors.Errorf("cannot use attribute %v for operations other than Contains", fd.attribute))
	default: // isContains && isContainsAttr
		p.handleContains(fd.entity, ord, contains.v)
		return true
	}
}

// handleContains will add facts to join the source to the slice member
// value by joining the source and a slice member entity.
//
// Note that we declare the variable slot for the slice member entity
// before we declare the variable slot for the source entity. This means
// that if the source entity has not been previously referenced in the
// query, it will be joined after the slice member. This gives the user
// the ability to find the source entity via its slice membership. If we
// were to declare the source entity first, we'd have to way to perform
// efficient lookups via the slice member's value. To make this concrete,
// consider the following single-clause query:
//
//	Var("e").AttrContains(SliceAttr, 1)
//
// This query will first find the slice members which have values of
// 1 and then will join those to the sources (which will be constant)
// as opposed to searching all entities and then seeing if they contain
// 1. The following query will do the less efficient join:
//
//	Var("e").AttrEqVar(rel.Self, "e"),
//	Var("e").AttrContains(SliceAttr, 1)
//
// This second query will first find all attributes, and then it will join
// them with slice members which are 1 and have the entity as its source.
func (p *queryBuilder) handleContains(source Var, valOrd ordinal, val expr) {
	sliceMember := p.fillSlot(slot{}, true /* isEntity */)
	p.maybeAddVar(source, true /* isEntity */)
	srcOrd := p.sc.sliceSourceOrdinal
	valValue := p.processValueExpr(val)
	srcValue := p.processValueExpr(source)
	for _, f := range []fact{
		{variable: sliceMember, attr: valOrd, value: valValue},
		{variable: sliceMember, attr: srcOrd, value: srcValue},
	} {
		p.typeCheck(f)
		p.facts = append(p.facts, f)
	}
}

func (p *queryBuilder) processEqDecl(t eqDecl) {
	varIdx := p.maybeAddVar(t.v, false)
	valueIdx := p.processValueExpr(t.expr)
	// This is somewhat inefficient but what it does is it lets
	// us state that the variable is equal to itself and that it
	// is equal to the value. It should be obvious that a variable
	// is equal to itself, but we want to have the normal contradiction
	// discovery machinery run.
	//
	// Note that there's no need to typeCheck because Self accepts all types.
	p.facts = append(p.facts,
		fact{
			variable: varIdx,
			attr:     p.sc.mustGetOrdinal(Self),
			value:    valueIdx,
		})
}

func (p *queryBuilder) processFilterDecl(t filterDecl) {
	fv := reflect.ValueOf(t.predicateFunc)
	// Type check the function.
	if err := checkNotNil(fv); err != nil {
		panic(errors.Wrapf(err, "nil filter function for variables %s", t.vars))
	}
	if fv.Kind() != reflect.Func {
		panic(errors.Errorf(
			"non-function %T filter function for variables %s",
			t.predicateFunc, t.vars,
		))
	}
	ft := fv.Type()
	if ft.NumOut() != 1 || ft.Out(0) != boolType {
		panic(errors.Errorf(
			"invalid non-bool return from %T filter function for variables %s",
			t.predicateFunc, t.vars,
		))
	}
	if ft.NumIn() != len(t.vars) {
		panic(errors.Errorf(
			"invalid %T filter function for variables %s accepts %d inputs",
			t.predicateFunc, t.vars, ft.NumIn(),
		))
	}

	slots := make([]slotIdx, len(t.vars))
	for i, v := range t.vars {
		slots[i] = p.maybeAddVar(v, false /* isEntity */)
		// TODO(ajwerner): This should end up constraining the slot type, but
		// it currently doesn't. In fact, we have no way of constraining the
		// type for a non-entity variable. Probably the way this should go is
		// that the slots should carry constraints like types and any values.
		// Then, when we go to populate them, we can enforce the constraints.
		//
		// Instead, as a hack, we've got a runtime check on the types to fail
		// out if any of the types are not right.
		checkSlotType(&p.slots[slots[i]], ft.In(i))
	}
	p.filters = append(p.filters, filter{
		input:     slots,
		predicate: fv,
		clauseID:  p.curClauseID,
	})
}

func (p *queryBuilder) processValueExpr(rawValue expr) slotIdx {
	switch v := rawValue.(type) {
	case Var:
		return p.maybeAddVar(v, false /* isEntity */)
	case anyExpr:
		sd := slot{
			any: make([]typedValue, len(v)),
		}
		for i, vv := range v {
			tv, err := makeComparableValue(vv)
			if err != nil {
				panic(err)
			}
			sd.any[i] = tv
		}
		return p.fillSlot(sd, false /* isEntity */)
	case valueExpr:
		tv, err := makeComparableValue(v.value)
		if err != nil {
			panic(err)
		}
		return p.fillSlot(slot{typedValue: tv}, false /* isEntity */)
	case notValueExpr:
		tv, err := makeComparableValue(v.value)
		if err != nil {
			panic(err)
		}
		return p.fillSlot(slot{not: &tv}, false /* isEntity */)
	case containsExpr:
		return p.processValueExpr(v.v)
	default:
		panic(errors.AssertionFailedf("unknown expr type %T", rawValue))
	}
}

func (p *queryBuilder) maybeAddVar(v Var, isEntity bool) slotIdx {
	if v == Blank {
		if isEntity {
			panic(errors.AssertionFailedf("cannot use _ as an entity"))
		}
		return p.fillSlot(slot{}, isEntity)
	}
	id, exists := p.variableSlots[v]
	if exists {
		if isEntity && !p.slotIsEntity[id] {
			p.slotIsEntity[id] = isEntity
		}
		return id
	}
	id = p.fillSlot(slot{}, isEntity)
	p.variables = append(p.variables, v)
	p.variableSlots[v] = id
	return id
}

func (p *queryBuilder) fillSlot(sd slot, isEntity bool) slotIdx {
	s := slotIdx(len(p.slots))
	p.slots = append(p.slots, sd)
	p.slotIsEntity = append(p.slotIsEntity, isEntity)
	p.clauseIDs = append(p.clauseIDs, p.curClauseID)
	return s
}

// findEntitySlots finds the slots which correspond to entity variableSlots in
// the order in which they appear. This will imply the user-requested join
// order.
func (p *queryBuilder) findEntitySlots() (entitySlots []slotIdx) {
	for i := range p.slots {
		if p.slotIsEntity[i] {
			entitySlots = append(entitySlots, slotIdx(i))
		}
	}
	return entitySlots
}

// typeCheck asserts that the value types for the fact are sane given the
// attribute.
func (p *queryBuilder) typeCheck(f fact) {
	s := &p.slots[f.value]
	if s.empty() && s.any == nil {
		return
	}
	switch f.attr {
	case p.sc.mustGetOrdinal(Type):
		checkSlotType(s, reflectTypeType)
	default:
		checkSlotType(s, p.sc.attrTypes[f.attr])
	}
}

func (p *queryBuilder) processNotJoin(t ruleInvocation, cib *clauseIDBuilder) {
	// If we have a not-join, then we need to find the slots for the inputs,
	// and we have to build the sub-query, which is a whole new query, and
	// we have to then figure out its depth. At this point, we build the
	// subquery and ensure that its inputs are bound variables. We'll
	// populate the depth at which we'll execute the subquery later, after
	// we've built the outer query.
	var sub subQuery
	// We want to ensure that the facts for the injected entities are joined
	// first in the query evaluation. We do this by injecting facts to the
	// front of the set of clauses.
	var clauses Clauses
	for i, v := range t.args {
		src, ok := p.variableSlots[v]
		if !ok {
			panic(errors.Errorf("variable %q used to invoke not-join rule %s not bound",
				v, t.rule.Name))
		}
		if p.slotIsEntity[src] {
			clauses = append(clauses, tripleDecl{
				entity:    t.rule.paramVars[i],
				attribute: Self,
				value:     t.rule.paramVars[i],
			})
		}
	}
	clauses = append(clauses, t.rule.clauses...)
	sub.query = newQuery(p.sc, clauses, cib.newBuilderForSubquery())
	for i, v := range t.args {
		src := p.variableSlots[v]
		dst, ok := sub.query.variableSlots[t.rule.paramVars[i]]
		if !ok {
			panic(errors.AssertionFailedf("variable %q used in not-join rule %s not bound",
				t.rule.paramNames[i], t.rule.Name))
		}
		sub.inputSlotMappings.Set(int(src), int(dst))
	}
	p.notJoins = append(p.notJoins, sub)
}

func (p *queryBuilder) setSubQueryDepths(entitySlots []slotIdx) {
	for i := range p.notJoins {
		p.setSubqueryDepth(&p.notJoins[i], entitySlots)
	}
}

func (p *queryBuilder) setSubqueryDepth(s *subQuery, entitySlots []slotIdx) {
	var max int
	s.inputSlotMappings.ForEach(func(key, _ int) {
		if p.slotIsEntity[key] && key > max {
			max = key
		}
	})
	got := sort.Search(len(entitySlots), func(i int) bool {
		return int(entitySlots[i]) >= max
	})
	if got == len(entitySlots) {
		panic(errors.AssertionFailedf("failed to find maximum entity in entitySlots: %v not in %v",
			max, entitySlots))
	}
	s.depth = queryDepth(got + 1)
}

var boolType = reflect.TypeOf((*bool)(nil)).Elem()

func checkSlotType(s *slot, exp reflect.Type) {
	if !s.empty() {
		if err := checkType(s.typ, exp); err != nil {
			panic(err)
		}
	}
	for i := range s.any {
		if err := checkType(s.any[i].typ, exp); err != nil {
			panic(err)
		}
	}
}
