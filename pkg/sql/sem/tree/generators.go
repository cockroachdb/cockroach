// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package tree

import "github.com/cockroachdb/cockroach/pkg/sql/sem/types"

// Table generators, also called "set-generating functions", are
// special functions that return an entire table.
//
// Overview of the concepts:
//
// - generators are implemented as regular built-in functions that
//   return values of type DTable (this is a Datum type).
//
// - the return type of generators is a types.TTable. This describes objects
//   that are conceptually sets of rows. A types.TTable type is
//   characterized by its column types (no names).
//
// - a DTable doesn't carry the contents of a table directly; instead
//   it carries a ValueGenerator reference.
//
// - ValueGenerator is an interface that offers a
//   Start/Next/Values/Stop API similar to sql.planNode.
//
// - because generators are regular functions, it is possible to use
//   them in any expression context. This is useful to e.g
//   pass an entire table as argument to the ARRAY( ) conversion
//   function.
//
// - the data source mechanism in the sql package has a special case
//   for generators appearing in FROM contexts and knows how to
//   construct a special row source from them (the valueGenerator
//   planNode).

// ValueGenerator is the interface provided by the object held by a
// DTable; objects that implement this interface are able to produce
// rows of values in a streaming fashion (like Go iterators or
// generators in Python).
type ValueGenerator interface {
	// ResolvedType returns the type signature of this value generator.
	// Used by DTable.ResolvedType().
	ResolvedType() types.TTable

	// Start initializes the generator. Must be called once before
	// Next() and Values().
	Start() error

	// Next determines whether there is a row of data available.
	Next() (bool, error)

	// Values retrieves the current row of data.
	Values() Datums

	// Close must be called after Start() before disposing of the
	// ValueGenerator. It does not need to be called if Start() has not
	// been called yet.
	Close()
}
