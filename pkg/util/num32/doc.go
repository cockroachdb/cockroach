// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/*
Package num32 contains basic numeric functions that operate on scalar, vector,
and matrix float32 values. Inputs and outputs deliberately use simple float
types so that they can be used in multiple contexts. It uses the gonum library
when possible, since it offers assembly language implementations of various
useful primitives.

Using the same convention as gonum, when a slice is being modified in place, it
has the name dst and the function does not return a value. In addition, many of
the functions have the same name and semantics as those in gonum.

Where possible, functions in this package are written with the assumption that
the caller prevents bad input. They will panic with assertion errors if this is
not the case, rather than returning error values. Callers should generally have
panic recovery logic further up the stack to gracefully handle these assertions,
as they indicate buggy code.
*/
package num32
