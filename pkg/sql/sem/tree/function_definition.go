// Copyright 2016 The Cockroach Authors.
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

// FunctionDefinition implements a reference to the (possibly several)
// overloads for a built-in function.
type FunctionDefinition struct {
	// Name is the short name of the function.
	Name string

	// Definition is the set of overloads for this function name.
	// We use []overloadImpl here although all the uses of this struct
	// could actually write a []Overload, because we want to share
	// the code with typeCheckOverloadedExprs().
	Definition []overloadImpl

	// FunctionProperties are the properties common to all overloads.
	FunctionProperties
}

// FunctionProperties defines the properties of the built-in
// functions that are common across all overloads.
type FunctionProperties struct {
	// NullableArgs is set to true when a function's definition can
	// handle NULL arguments. When set, the function will be given the
	// chance to see NULL arguments. When not, the function will
	// evaluate directly to NULL in the presence of any NULL arguments.
	//
	// NOTE: when set, a function should be prepared for any of its arguments to
	// be NULL and should act accordingly.
	NullableArgs bool

	// Private, when set to true, indicates the built-in function is not
	// available for use by user queries. This is currently used by some
	// aggregates due to issue #10495.
	Private bool

	// NeedsRepeatedEvaluation is set to true when a function may change
	// at every row whether or not it is applied to an expression that
	// contains row-dependent variables. Used e.g. by `random` and
	// aggregate functions.
	NeedsRepeatedEvaluation bool

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

	// Privileged is set to true when the built-in can only be used by
	// security.RootUser.
	Privileged bool

	// Class is the kind of built-in function (normal/aggregate/window/etc.)
	Class FunctionClass

	// Category is used to generate documentation strings.
	Category string

	// ReturnLabels is used by transformSRF until the transform
	// is properly migrated to a point past type checking.
	// TODO(knz): remove this field once it becomes unneeded.
	ReturnLabels []string

	// AmbiguousReturnType is true if the builtin's return type can't be
	// determined without extra context. This is used for formatting builtins
	// with the FmtParsable directive.
	AmbiguousReturnType bool
}

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

// NewFunctionDefinition allocates a function definition corresponding
// to the given built-in definition.
func NewFunctionDefinition(
	name string, props *FunctionProperties, def []Overload,
) *FunctionDefinition {
	overloads := make([]overloadImpl, len(def))

	for i := range def {
		if def[i].PreferredOverload {
			// Builtins with a preferred overload are always ambiguous.
			props.AmbiguousReturnType = true
		}
		overloads[i] = &def[i]
	}
	return &FunctionDefinition{
		Name:               name,
		Definition:         overloads,
		FunctionProperties: *props,
	}
}

// FunDefs holds pre-allocated FunctionDefinition instances
// for every builtin function. Initialized by builtins.init().
var FunDefs map[string]*FunctionDefinition

// Format implements the NodeFormatter interface.
func (fd *FunctionDefinition) Format(ctx *FmtCtx) {
	ctx.WriteString(fd.Name)
}
func (fd *FunctionDefinition) String() string { return AsString(fd) }
