// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package jsonpath

// This file contains node definitions for methods that are available in JSONPath.

type Size struct{}

var _ Path = Size{}

func (s Size) String() string {
	return ".size()"
}

type Type struct{}

var _ Path = Type{}

func (t Type) String() string {
	return ".type()"
}
