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

package parser

import "github.com/cockroachdb/cockroach/pkg/sql/sem/types"

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
