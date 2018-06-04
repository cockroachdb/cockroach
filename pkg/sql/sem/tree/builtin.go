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

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// Builtin is a built-in function.
type Builtin struct {
	FunctionProperties

	Types      TypeList
	ReturnType ReturnTyper

	// PreferredOverload determines overload resolution as follows.
	// When multiple overloads are eligible based on types even after all of of
	// the heuristics to pick one have been used, if one of the overloads is a
	// Builtin with the `PreferredOverload` flag set to true it can be selected
	// rather than returning a no-such-method error.
	// This should generally be avoided -- avoiding introducing ambiguous
	// overloads in the first place is a much better solution -- and only done
	// after consultation with @knz @nvanbenschoten.
	PreferredOverload bool

	// Info is a description of the function, which is surfaced on the CockroachDB
	// docs site on the "Functions and Operators" page. Descriptions typically use
	// third-person with the function as an implicit subject (e.g. "Calculates
	// infinity"), but should focus more on ease of understanding so other structures
	// might be more appropriate.
	Info string

	AggregateFunc func([]types.T, *EvalContext) AggregateFunc
	WindowFunc    func([]types.T, *EvalContext) WindowFunc
	Fn            func(*EvalContext, Datums) (Datum, error)
}

// params implements the overloadImpl interface.
func (b Builtin) params() TypeList { return b.Types }

// returnType implements the overloadImpl interface.
func (b Builtin) returnType() ReturnTyper { return b.ReturnType }

// preferred implements the overloadImpl interface.
func (b Builtin) preferred() bool { return b.PreferredOverload }

// FixedReturnType returns a fixed type that the function returns, returning Any
// if the return type is based on the function's arguments.
func (b Builtin) FixedReturnType() types.T {
	if b.ReturnType == nil {
		return nil
	}
	return returnTypeToFixedType(b.ReturnType)
}

// Signature returns a human-readable signature.
func (b Builtin) Signature() string {
	return fmt.Sprintf("(%s) -> %s", b.Types.String(), b.FixedReturnType())
}
