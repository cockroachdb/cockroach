// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rel

// Attr is used to define a property of entities. Attributes in a given
// schema have a type.
type Attr interface {

	// String is used when formatting the attribute.
	String() string
}
