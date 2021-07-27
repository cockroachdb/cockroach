// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package eav

import "strconv"

// Type is an enumerations of the types exported by this package.
type Type int

//go:generate stringer --type Type --trimprefix Type

// Enumeration of the types exported by this package.
const (
	_ Type = iota
	TypeInt64
	TypeInt32
	TypeUint32
	TypeString

	NumTypes int = iota
)

// Types exported by this package.
type (
	// Int64 is a value representing an int64.
	Int64 int64
	// Int32 is a value representing an int32.
	Int32 int32
	// Uint32 is a value representing a uint32.
	Uint32 uint32
	// String is a value representing a string.
	String string
)

var _ = []Value{
	(*Int64)(nil),
	(*Int32)(nil),
	(*String)(nil),
	(*Uint32)(nil),
}

// String encodes the Uint32 as a string.
func (u Uint32) String() string { return strconv.FormatUint(uint64(u), 10) }

// Type returns TypeUint32.
func (u *Uint32) Type() Type { return TypeUint32 }

// Compare compares this to another value.
func (u *Uint32) Compare(other Value) (ok, less, eq bool) {
	o, ok := other.(*Uint32)
	if !ok {
		return false, false, false
	}
	switch {
	case *u < *o:
		return ok, true, false
	case *u > *o:
		return ok, false, false
	default:
		return ok, false, true
	}
}

// String format as a decimal string.
func (i *Int64) String() string {
	return strconv.FormatInt(int64(*i), 10)
}

// Type returns TypeInt64.
func (i *Int64) Type() Type { return TypeInt64 }

// Compare compares this to another value.
func (i *Int64) Compare(other Value) (ok, less, eq bool) {
	o, ok := other.(*Int64)
	if !ok {
		return false, false, false
	}
	switch {
	case *i < *o:
		return ok, true, false
	case *i > *o:
		return ok, false, false
	default:
		return ok, false, true
	}
}

// String format as a decimal string.
func (i *Int32) String() string {
	return strconv.FormatInt(int64(*i), 10)
}

// Type is TypeInt32.
func (i *Int32) Type() Type { return TypeInt32 }

// Compare compares this to another value.
func (i *Int32) Compare(other Value) (ok, less, eq bool) {
	o, ok := other.(*Int32)
	if !ok {
		return false, false, false
	}
	switch {
	case *i < *o:
		return ok, true, false
	case *i > *o:
		return ok, false, false
	default:
		return ok, false, true
	}
}

// String format as a decimal string.
func (s *String) String() string {
	return strconv.Quote(string(*s))
}

// Type is TypeString.
func (s *String) Type() Type { return TypeString }

// Compare compares this to another value.
func (s *String) Compare(other Value) (ok, less, eq bool) {
	o, ok := other.(*String)
	if !ok {
		return false, false, false
	}
	switch {
	case *s < *o:
		return ok, true, false
	case *s > *o:
		return ok, false, false
	default:
		return ok, false, true
	}
}
