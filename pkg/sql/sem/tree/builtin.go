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

// FunctionClass specifies the class of the builtin function.
type FunctionClass int

const (
	// NormalClass is a standard builtin function.
	NormalClass FunctionClass = iota
	// AggregateClass is a builtin aggregate function.
	AggregateClass
	// WindowClass is a builtin window function.
	WindowClass
	// GeneratorClass is a builtin generator function.
	GeneratorClass
)

// Avoid vet warning about unused enum value.
var _ = NormalClass

// Builtin is a built-in function.
type Builtin struct {
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

	// Impure is set to true when a function potentially returns a
	// different value when called in the same statement with the same
	// parameters. e.g.: random(), clock_timestamp(). Some functions
	// like now() return the same value in the same statement, but
	// different values in separate statements, and should not be marked
	// as impure.
	Impure bool

	// DistsqlBlacklist is set to true when a function depends on
	// members of the EvalContext that are not marshaled by DistSQL
	// (e.g. planner). Currently used for DistSQL to determine if
	// expressions can be evaluated on a different node without sending
	// over the EvalContext.
	//
	// TODO(andrei): Get rid of the planner from the EvalContext and then we can
	// get rid of this blacklist.
	DistsqlBlacklist bool

	// NullableArgs is set to true when a function's definition can
	// handle NULL arguments. When set, the function will be given the
	// chance to see NULL arguments. When not, the function will
	// evaluate directly to NULL in the presence of any NULL arguments.
	//
	// NOTE: when set, a function should be prepared for any of its arguments to
	// be NULL and should act accordingly.
	NullableArgs bool

	// NeedsRepeatedEvaluation is set to true when a function may change
	// at every row whether or not it is applied to an expression that
	// contains row-dependent variables. Used e.g. by `random` and
	// aggregate functions.
	NeedsRepeatedEvaluation bool

	// Privileged is set to true when the built-in can only be used by
	// security.RootUser.
	Privileged bool

	// Class is the kind of built-in function (normal/aggregate/window/etc.)
	Class FunctionClass

	// Category is used to generate documentation strings.
	Category string

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
