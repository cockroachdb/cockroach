// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package syncutil

// Set is like a Go map[V]struct{} but is safe for concurrent use by multiple
// goroutines without additional locking or coordination.
// Loads, stores, and deletes run in amortized constant time.
//
// See the Map type for more details.
type Set[V comparable] struct {
	m Map[V, struct{}]
}

// Contains returns whether the value is stored in the set.
//
// Both Add and Remove return whether the value previously existed in the set,
// so Contains is typically not needed when later calling one of those methods.
// In fact, its use in logic that later mutates the set may be racy if not
// carefully considered, as there is no synchronization between the Contains
// call and the subsequent Add or Remove call.
func (s *Set[V]) Contains(value V) bool {
	_, ok := s.m.Load(value)
	return ok
}

// dummyValue is a placeholder value for all values in the map.
var dummyValue = new(struct{})

// Add adds the value to the set.
//
// Returns whether the value was added (true) or was already present (false).
func (s *Set[V]) Add(value V) bool {
	_, loaded := s.m.LoadOrStore(value, dummyValue)
	return !loaded
}

// Remove removes the value from the set.
//
// Returns whether the value was present and removed (true) or was not present
// and not removed (false).
func (s *Set[V]) Remove(value V) bool {
	_, loaded := s.m.LoadAndDelete(value)
	return loaded
}

// Range calls f sequentially for each value present in the set.
// If f returns false, range stops the iteration.
//
// See Map.Range for more details.
func (s *Set[V]) Range(f func(value V) bool) {
	s.m.Range(func(value V, _ *struct{}) bool {
		return f(value)
	})
}
