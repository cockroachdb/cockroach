// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	_ systemAttribute = 64 - iota

	// Type is an attribute which stores a value's type.
	Type

	// Self is an attribute which stores the variable itself.
	Self

	maxUserAttribute ordinal = 64 - iota
)

var _ Attr = systemAttribute(0)
