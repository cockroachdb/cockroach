// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scpb

import (
	"fmt"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/util/debugutil"
	"github.com/cockroachdb/errors"
)

// ElementCollection represents an ordered set of
// (current status, target status, element) tuples,
// looked up in `g` by iterating through `indexes`.
type ElementCollection[E Element] struct {
	g       ElementCollectionGetter
	indexes []int

	// debugInfo is only populated when using a debugger.
	debugInfo []debugInfo
}

type debugInfo struct {
	element Element
	target  TargetStatus
	status  Status
}

// ElementCollectionGetter abstracts the actual mechanism for obtaining
// (current status, target status, element) tuples in an element collection.
type ElementCollectionGetter interface {

	// Get returns values for the given index in [0, Size()[.
	Get(index int) (current Status, target TargetStatus, e Element)

	// Size returns the cardinality of the collection.
	Size() int
}

// NewElementCollection constructs a generic element collection
// containing the elements in `g` listed in the `indexes` slice.
// The slice is owned by the returned struct.
func NewElementCollection(g ElementCollectionGetter, indexes []int) *ElementCollection[Element] {
	if g == nil || len(indexes) == 0 {
		return nil
	}
	ret := &ElementCollection[Element]{
		g:       g,
		indexes: indexes,
	}
	return ret.decorate()
}

func (c *ElementCollection[E]) decorate() *ElementCollection[E] {
	if c != nil && debugutil.IsLaunchedByDebugger() {
		c.debugInfo = make([]debugInfo, len(c.indexes))
		for i, idx := range c.indexes {
			s, t, e := c.g.Get(idx)
			c.debugInfo[i] = debugInfo{
				element: e,
				target:  t,
				status:  s,
			}
		}
	}
	return c
}

// ElementCollection is an ElementCollectionGetter.
var _ ElementCollectionGetter = &ElementCollection[Element]{}

// Get implements the ElementCollectionGetter interface.
func (c *ElementCollection[E]) Get(ordinal int) (current Status, target TargetStatus, e Element) {
	if ordinal < 0 || ordinal >= c.Size() {
		panic(errors.AssertionFailedf("ordinal %d is out of bounds [0, %d[", ordinal, c.Size()))
	}
	return c.g.Get(c.indexes[ordinal])
}

// Size implements the ElementCollectionGetter interface.
func (c *ElementCollection[E]) Size() int {
	if c == nil {
		return 0
	}
	return len(c.indexes)
}

// Filter generates a new ElementCollection by filtering
// its values using the predicate.
func (c *ElementCollection[E]) Filter(
	predicate func(current Status, target TargetStatus, e E) bool,
) *ElementCollection[E] {
	return c.genericFilter(func(current Status, target TargetStatus, genericElement Element) bool {
		e, ok := genericElement.(E)
		return ok && predicate(current, target, e)
	})
}

// FilterTarget is like Filter but ignoring current status.
func (c *ElementCollection[E]) FilterTarget(
	predicate func(target TargetStatus, e E) bool,
) *ElementCollection[E] {
	return c.genericFilter(func(current Status, target TargetStatus, genericElement Element) bool {
		e, ok := genericElement.(E)
		return ok && predicate(target, e)
	})
}

// FilterElement is like Filter but ignoring current and target statuses.
func (c *ElementCollection[E]) FilterElement(predicate func(e E) bool) *ElementCollection[E] {
	return c.genericFilter(func(current Status, target TargetStatus, genericElement Element) bool {
		e, ok := genericElement.(E)
		return ok && predicate(e)
	})
}

func (c *ElementCollection[E]) genericFilter(
	predicate func(current Status, target TargetStatus, e Element) bool,
) *ElementCollection[E] {
	if c == nil || len(c.indexes) == 0 {
		return nil
	}
	ret := &ElementCollection[E]{
		g:       c.g,
		indexes: make([]int, 0, len(c.indexes)),
	}
	for _, j := range c.indexes {
		if predicate(c.g.Get(j)) {
			ret.indexes = append(ret.indexes, j)
		}
	}
	if len(ret.indexes) == 0 {
		return nil
	}
	return ret.decorate()
}

// ForEach iterates through the collection and applies fn
// on each tuple.
func (c *ElementCollection[E]) ForEach(fn func(current Status, target TargetStatus, e E)) {
	if c == nil {
		return
	}
	for _, j := range c.indexes {
		current, target, e := c.g.Get(j)
		fn(current, target, e.(E))
	}
}

// ForEachTarget is like ForEach but without current status.
func (c *ElementCollection[E]) ForEachTarget(fn func(target TargetStatus, e E)) {
	c.ForEach(func(current Status, target TargetStatus, e E) {
		fn(target, e)
	})
}

// Elements returns the elements in the collection as a slice.
func (c *ElementCollection[E]) Elements() []E {
	if c.IsEmpty() {
		return nil
	}
	ret := make([]E, c.Size())
	for i, j := range c.indexes {
		_, _, e := c.g.Get(j)
		ret[i] = e.(E)
	}
	return ret
}

// IsEmpty returns true iff empty.
func (c *ElementCollection[E]) IsEmpty() bool {
	return c.Size() == 0
}

// IsSingleton returns true iff Size() is one.
func (c *ElementCollection[E]) IsSingleton() bool {
	return c.Size() == 1
}

// IsMany returns true iff Size() is more than one.
func (c *ElementCollection[E]) IsMany() bool {
	return c.Size() > 1
}

// ToPublic filters by elements targeting the PUBLIC state.
func (c *ElementCollection[E]) ToPublic() *ElementCollection[E] {
	return c.FilterTarget(func(target TargetStatus, _ E) bool {
		return target == ToPublic
	})
}

// NotToPublic filters by elements not targeting the PUBLIC state.
func (c *ElementCollection[E]) NotToPublic() *ElementCollection[E] {
	return c.FilterTarget(func(target TargetStatus, _ E) bool {
		return target != ToPublic
	})
}

// ToAbsent filters by elements targeting the ABSENT state.
func (c *ElementCollection[E]) ToAbsent() *ElementCollection[E] {
	return c.FilterTarget(func(target TargetStatus, _ E) bool {
		return target == ToAbsent
	})
}

// NotToAbsent filters by elements not targeting the ABSENT state.
func (c *ElementCollection[E]) NotToAbsent() *ElementCollection[E] {
	return c.FilterTarget(func(target TargetStatus, _ E) bool {
		return target != ToAbsent
	})
}

// Transient filters by elements targeting the TRANSIENT_ABSENT state.
func (c *ElementCollection[E]) Transient() *ElementCollection[E] {
	return c.FilterTarget(func(target TargetStatus, _ E) bool {
		return target == Transient
	})
}

// NotTransient filters by elements not targeting the TRANSIENT_ABSENT state.
func (c *ElementCollection[E]) NotTransient() *ElementCollection[E] {
	return c.FilterTarget(func(target TargetStatus, _ E) bool {
		return target != Transient
	})
}

// WithTarget filters by elements targeting any state.
func (c *ElementCollection[E]) WithTarget() *ElementCollection[E] {
	return c.FilterTarget(func(target TargetStatus, _ E) bool {
		return target != InvalidTarget
	})
}

// WithoutTarget filters by elements not targeting any state.
func (c *ElementCollection[E]) WithoutTarget() *ElementCollection[E] {
	return c.FilterTarget(func(target TargetStatus, _ E) bool {
		return target == InvalidTarget
	})
}

// MustGetOneElement asserts that the collection contains exactly
// one element and returns it.
func (c *ElementCollection[E]) MustGetOneElement() E {
	_, _, e := c.MustHaveOne().Get(0)
	return e.(E)
}

// MustHaveOne asserts that the collection contains exactly one element.
func (c *ElementCollection[E]) MustHaveOne() *ElementCollection[E] {
	if c.Size() != 1 {
		panic(errors.AssertionFailedf("expected element collection size 1, not %d", c.Size()))
	}
	return c

}

// MustGetZeroOrOneElement asserts that the collection contains at most
// one element and returns it if it exists (nil otherwise).
func (c *ElementCollection[E]) MustGetZeroOrOneElement() (ret E) {
	if c.Size() == 0 {
		return ret
	}
	_, _, e := c.MustHaveZeroOrOne().Get(0)
	return e.(E)
}

// MustHaveZeroOrOne asserts that the collection contains at most
// one element
func (c *ElementCollection[E]) MustHaveZeroOrOne() *ElementCollection[E] {
	if c.Size() > 1 {
		panic(errors.AssertionFailedf("expected element collection size 0 or 1, not %d", c.Size()))
	}
	return c
}

func (c *ElementCollection[E]) String() string {
	var result string
	for _, element := range c.Elements() {
		elemType := reflect.TypeOf(element).Elem()
		result += fmt.Sprintf("%s:{%+v}\n", elemType.Name(), element)
	}
	return result
}
