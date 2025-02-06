// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rel

import (
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/util/intsets"
)

type slotIdx uint16

type fact struct {
	variable slotIdx
	attr     ordinal
	value    slotIdx
}

// slot represents an potentially unbound value referenced in a query.
type slot struct {
	typedValue

	// any holds the acceptable valuesMap which may occupy this slot as
	// indicated from an Any value.
	// TODO(ajwerner): Consider pulling this out into a set of constraints
	// on the slot. Then we could potentially also have the type act as a
	// constraint on the value. This mostly arises in the context of the
	// filter functions but conceivably could arise also when we have
	// equality filters and then variables used in attributes which require
	// types.
	any []typedValue

	// not holds a value which this slot must not be equal to. Additionally,
	// the value which fills this slot must have the same type as the value
	// in the not container.
	not *typedValue
}

// typedValue is a value in its comparable form, which is to say, it is a
// pointer to a primitive type. The type is the type from which the value
// was derived and which should be presented to the user.
type typedValue struct {
	typ   reflect.Type
	value interface{}

	// inline can be populated for all data types. It is set lazily.
	// Not all data types are directly comparable based solely on the inline
	// value.
	inline    uintptr
	inlineSet bool
}

func (tv typedValue) toValue() reflect.Value {
	if tv.typ == reflectTypeType {
		return reflect.ValueOf(tv.value)
	}
	if tv.typ.Kind() == reflect.Ptr {
		if tv.typ.Elem().Kind() == reflect.Struct {
			return reflect.ValueOf(tv.value)
		}
		return reflect.ValueOf(tv.value).Convert(tv.typ)
	}
	return reflect.ValueOf(tv.value).Convert(reflect.PointerTo(tv.typ)).Elem()
}

func (tv typedValue) toInterface() interface{} {
	return tv.toValue().Interface()
}

// inlineValue populates the inline value for the typedValue. The inline
// value is a single scalar which can be used to efficiently compare
// values, but it only has meaning in the context of the current entitySet.
// It must be cleared when moving to a new entity set.
func (tv *typedValue) inlineValue(es *entitySet, attr ordinal) (uintptr, error) {
	if tv.inlineSet {
		return tv.inline, nil
	}
	v, err := es.makeInlineValue(attr, tv.toValue())
	if err != nil {
		return 0, err
	}
	tv.inline, tv.inlineSet = v, true
	return tv.inline, nil
}

// resetInline clears the inline value.
func (tv *typedValue) resetInline() {
	tv.inlineSet, tv.inline = false, 0
}

func (s *slot) eq(other slot) bool {
	// TODO(ajwerner): Deal with types. We may have two slots which both have
	// nil values but they differ in terms of types.
	switch {
	case s.value == nil && other.value == nil:
		return true
	case s.value == nil:
		return false
	case other.value == nil:
		return false
	default:
		_, eq := compareNotNil(s.value, other.value)
		return eq
	}
}

func (s *slot) empty() bool {
	return s.value == nil
}

func (s *slot) reset() {
	s.typedValue = typedValue{}
	if s.any != nil {
		for i := 0; i < len(s.any); i++ {
			s.any[i].resetInline()
		}
	}
	if s.not != nil {
		s.not.resetInline()
	}
}

func maybeSet(
	slots []slot, idx slotIdx, tv typedValue, set *intsets.Fast,
) (foundContradiction bool) {
	s := &slots[idx]

	eqNotNil := func(a, b interface{}) bool {
		_, eq := compareNotNil(a, b)
		return eq
	}
	findMatchInAny := func(haystack []typedValue) bool {
		for _, v := range s.any {
			if tv.typ == v.typ && eqNotNil(v.value, tv.value) {
				return true
			}
		}
		return false
	}
	check := func() (shouldSet, foundContradiction bool) {
		if !s.empty() {
			if _, eq := compareNotNil(s.value, tv.value); !eq {
				return false, true
			}
			return false, false
		}
		if s.not != nil {
			if tv.typ != s.not.typ || eqNotNil(s.not.value, tv.value) {
				return false, true
			}
		}
		if s.any != nil && !findMatchInAny(s.any) {
			return false, true // contradiction
		}
		return true, false
	}
	shouldSet, contradiction := check()
	if !shouldSet || contradiction {
		return contradiction
	}
	s.typedValue = tv
	if set != nil {
		set.Add(int(idx))
	}
	return false
}

// filter is a user-provided predicate over some set of variables.
type filter struct {
	input     []slotIdx
	predicate reflect.Value
	clauseID  int
}
