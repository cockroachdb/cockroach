// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package execreleasable

// Releasable is an interface for objects than can be Released back into a
// memory pool when finished.
type Releasable interface {
	// Release allows this object to be returned to a memory pool. Objects must
	// not be used after Release is called.
	Release()
}
