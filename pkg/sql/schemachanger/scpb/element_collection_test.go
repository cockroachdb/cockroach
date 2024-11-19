// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package scpb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type testGetter []struct {
	current Status
	target  TargetStatus
	element Element
}

var _ ElementCollectionGetter = testGetter(nil)

// Get implements ElementCollectionGetter.
func (t testGetter) Get(index int) (current Status, target TargetStatus, e Element) {
	tuple := t[index]
	return tuple.current, tuple.target, tuple.element
}

// Size implements ElementCollectionGetter.
func (t testGetter) Size() int {
	return len(t)
}

// TestElementCollection has unit test on ElementCollection methods.
func TestElementCollection(t *testing.T) {
	g := testGetter([]struct {
		current Status
		target  TargetStatus
		element Element
	}{
		{
			current: Status_ABSENT,
			target:  ToPublic,
			element: &Namespace{
				DatabaseID:   100,
				SchemaID:     101,
				DescriptorID: 104,
				Name:         "new_name",
			},
		},
		{
			current: Status_PUBLIC,
			target:  ToAbsent,
			element: &Namespace{
				DatabaseID:   100,
				SchemaID:     101,
				DescriptorID: 104,
				Name:         "old_name",
			},
		},
		{
			current: Status_PUBLIC,
			target:  InvalidTarget,
			element: &Schema{
				SchemaID: 101,
			},
		},
	})
	c := newTestCollection(g)
	t.Run("getter", func(t *testing.T) {
		require.Equal(t, 3, c.Size())
		current, target, element := c.Get(1)
		require.Equal(t, g[1].current, current)
		require.Equal(t, g[1].target, target)
		require.Equal(t, g[1].element.String(), element.String())
	})
	t.Run("filters", func(t *testing.T) {
		require.Equal(t, len(g), c.ToPublic().Size()+c.NotToPublic().Size())
		require.Equal(t, len(g), c.ToAbsent().Size()+c.NotToAbsent().Size())
		require.Equal(t, len(g), c.Transient().Size()+c.NotTransient().Size())
		require.Equal(t, len(g), c.WithTarget().Size()+c.WithoutTarget().Size())
		require.Equal(t, 2, c.FilterNamespace().Size())
		require.Equal(t, 1, c.FilterNamespace().NotToAbsent().Size())
	})
	t.Run("slice", func(t *testing.T) {
		require.Len(t, c.FilterNamespace().NotToAbsent().Elements(), 1)
		require.Equal(t, g[0].element.String(), c.FilterNamespace().NotToAbsent().Elements()[0].String())
	})
	t.Run("for_each", func(t *testing.T) {
		var ranAlready bool
		c.FilterSchema().ForEachTarget(func(target TargetStatus, e *Schema) {
			require.False(t, ranAlready)
			ranAlready = true
			require.Equal(t, g[2].target, target)
			require.Equal(t, g[2].element.String(), e.String())
		})
		require.True(t, ranAlready)
	})
	t.Run("predicates", func(t *testing.T) {
		require.False(t, c.IsEmpty())
		require.False(t, c.FilterSchema().IsEmpty())
		require.True(t, c.FilterSecondaryIndexPartial().IsEmpty())

		require.False(t, c.IsSingleton())
		require.True(t, c.FilterSchema().IsSingleton())
		require.False(t, c.FilterSecondaryIndexPartial().IsSingleton())

		require.True(t, c.IsMany())
		require.False(t, c.FilterSchema().IsMany())
		require.False(t, c.FilterSecondaryIndexPartial().IsMany())
	})
	t.Run("assertions", func(t *testing.T) {
		e, err := recoverFromPanic(c.MustGetZeroOrOneElement)
		require.Error(t, err)
		require.Nil(t, e)
		e, err = recoverFromPanic(c.MustGetOneElement)
		require.Error(t, err)
		require.Nil(t, e)

		e, err = recoverFromPanic(c.FilterSchema().MustGetZeroOrOneElement)
		require.NoError(t, err)
		require.NotNil(t, e)
		e, err = recoverFromPanic(c.FilterSchema().MustGetOneElement)
		require.NoError(t, err)
		require.NotNil(t, e)

		e, err = recoverFromPanic(c.FilterSecondaryIndexPartial().MustGetZeroOrOneElement)
		require.NoError(t, err)
		require.Nil(t, e)
		e, err = recoverFromPanic(c.FilterSecondaryIndexPartial().MustGetOneElement)
		require.Error(t, err)
		require.Nil(t, e)
	})
	t.Run("deprecated", func(t *testing.T) {
		var ranAlready bool
		ForEachSchema(c, func(current Status, target TargetStatus, e *Schema) {
			require.False(t, ranAlready)
			ranAlready = true
			require.Equal(t, g[2].target, target)
			require.Equal(t, g[2].element.String(), e.String())
		})
		require.True(t, ranAlready)
		current, target, ns := FindNamespace(c)
		require.Equal(t, g[0].current, current)
		require.Equal(t, g[0].target, target)
		require.Equal(t, g[0].element.String(), ns.String())
	})
}

func newTestCollection(g testGetter) *ElementCollection[Element] {
	indexes := make([]int, len(g))
	for i := range g {
		indexes[i] = i
	}
	return NewElementCollection(g, indexes)
}

func recoverFromPanic[E Element](fn func() E) (e E, err error) {
	defer func() {
		switch recErr := recover().(type) {
		case error:
			err = recErr
		}
	}()
	return fn(), nil
}
