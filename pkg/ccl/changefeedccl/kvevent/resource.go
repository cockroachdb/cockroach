// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvevent

// Resource describes the resource allocated by on behalf of an event.
type Resource interface {
	// Release releases the resources.
	Release()
}

// noResource is "nil" resource.
type noResource struct{}

var _ Resource = &noResource{}

func (n *noResource) Release() {
}

// NoResource is a nil resource.
var NoResource Resource = (*noResource)(nil)

// ScopedResource is a "scoped object" holding the underlying resource.
// It is designed to assist with the correct release of the resources,
// when use in functions with multiple return points.
//
// NB: ScopedResource should not be allocated on the heap, and it should never be passed
// as a function argument.  Use MakeScopedResource function to construct  scoped resource.
//
// Example usage:
//    r := callReturningResource()
//    // Immediately, create scoped resource, and defer release it.
//    sr := MakeScopedResource(r)
//    defer sr.Release()
//    ... code ...
//    if err != nil {
//      return err;   // Early termination is fine: we deferred release above.
//    }
//    return passResourceToAnotherFunction(sr.Move())
//
// Resource can be Move()d only once.  Calling Move multiple times panics.
//
type ScopedResource struct {
	r Resource
}

// MakeScopedResource returns initialized ScopedResource.
func MakeScopedResource(r Resource) ScopedResource {
	return ScopedResource{r}
}

// Release releases the underlying resource, provided it has not been moved.
func (s *ScopedResource) Release() {
	if s.r != nil {
		s.r.Release()
	}
}

// Move transfers the responsibility of releasing the resource somewhere else.
func (s *ScopedResource) Move() Resource {
	if s.r == nil {
		panic("can't move multiple times")
	}
	r := s.r
	s.r = nil
	return r
}
