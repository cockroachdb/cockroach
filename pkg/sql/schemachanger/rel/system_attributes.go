// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rel

// systemAttribute is a type which represents attributes offered by the
// system for all entities stored in a database. In particular they capture
// the type and address of the variable. Unlike other attributes which only
// apply to entities, the systemAttributes Type and Self apply to all values.
//
// The system attribute may be extended to cover other structural attributes.
// TODO(ajwerner): Add support for slices, arrays, and maps and then provide
// system attributes to access slice/array indexes and map keys and valuesMap.
type systemAttribute int8

//go:generate stringer -type systemAttribute

const (
	// Note that the value of the system attribute here is not relevant because
	// inside a given schema, each attribute is mapped internally to an ordinal
	// independently.
	_ systemAttribute = iota

	// Type is an attribute which stores a value's type.
	Type

	// Self is an attribute which stores the variable itself.
	Self

	// sliceSource is the attribute used internally when building
	// slice member entities to reference the source entity of the slice member.
	sliceSource
	// sliceIndex is the attributed used internally when building
	// slice member entities to reference the index of the slice member in the
	// slice. It is used to give slice member entities unique identities and to
	// order slice entries according to their index. At time of writing, there
	// is no way to access this property in queries.
	sliceIndex
)

var _ Attr = systemAttribute(0)

// maxOrdinal is the maximum ordinal value an attribute in a schema may use.
const maxOrdinal = ordinalSetMaxOrdinal
