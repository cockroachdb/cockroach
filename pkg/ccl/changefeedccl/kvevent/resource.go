// Copyright 2021 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package kvevent

// Resource describes the resources allocated by an event.
// Resource must be released, or it must be "moved" -- i.e. the responsibility
// to release the resource is moved to some other call site.
// Example:
//   r := getResource()
//   defer r.Release()  // Immediately defer resource release.
//   ... more code ...
//   if err != nil {
//      return err  // Early returns are fine -- resource is released by defer.
//   }
//   ... more code ...
//   return transferResource(r.Move())
// Note: after the resource is transferred, the deferred release continues to work correctly.
type Resource interface {
	// Release releases the resources.  Should normally be invoked via defer.
	Release()
	// Move transfers the responsibility of releasing the resources downstream.
	Move() Resource
}

// callbackResource is a resource that invokes callback to release resources.
type callbackResource struct {
	cb func()
}

var _ Resource = &callbackResource{}

// Release implements Resource interface.
func (c *callbackResource) Release() {
	if c.cb != nil {
		c.cb()
	}
}

// Move implements Resource Interface
func (c *callbackResource) Move() Resource {
	if c.cb == nil {
		panic("cannot move resource which was already moved")
	}
	movedResource := &callbackResource{cb: c.cb}
	c.cb = nil
	return movedResource
}

// noResource is "nil" resource.
type noResource struct{}

var _ Resource = &noResource{}

func (n *noResource) Release() {
}

func (n noResource) Move() Resource {
	return NoResource
}

// NoResource is a nil resource.
var NoResource = &noResource{}
