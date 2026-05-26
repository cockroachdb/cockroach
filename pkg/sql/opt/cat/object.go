// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cat

import "github.com/cockroachdb/cockroach/pkg/sql/sem/catid"

// Object is implemented by all objects in the catalog.
type Object interface {
	// ID is the unique, stable identifier for this object. See the comment for
	// StableID for more detail.
	ID() StableID

	// PostgresDescriptorID is the descriptor ID for this object. This can differ
	// from the ID of this object in some cases. This is only important for
	// reporting the Postgres-compatible identifiers for objects in the various
	// object catalogs. In the vast majority of cases, you should use ID()
	// instead.
	PostgresDescriptorID() catid.DescID

	// Equals returns true if this object is identical to the given Object.
	//
	// Two objects are identical if they have the same identifier and there were
	// no changes to schema or table statistics between the times the two objects
	// were resolved.
	//
	// Used for invalidating cached plans.
	Equals(other Object) bool

	// Version returns the underlying version of the descriptor backing this object.
	Version() uint64
}
