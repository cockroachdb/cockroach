// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tree

import (
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/lib/pq/oid"
)

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
	// UnsupportedWithIssue, if non-zero indicates the built-in is not
	// really supported; the name is a placeholder. Value -1 just says
	// "not supported" without an issue to link; values > 0 provide an
	// issue number to link.
	UnsupportedWithIssue int

	// Undocumented, when set to true, indicates that the built-in function is
	// hidden from documentation. This is currently used to hide experimental
	// functionality as it is being developed.
	Undocumented bool

	// Private, when set to true, indicates the built-in function is not
	// available for use by user queries. This is currently used by some
	// aggregates due to issue #10495. Private functions are implicitly
	// considered undocumented.
	Private bool

	// NullableArgs is set to true when a function's definition can handle NULL
	// arguments. When set to true, the function will be given the chance to see NULL
	// arguments.
	//
	// When set to false, the function will directly result in NULL in the
	// presence of any NULL arguments without evaluating the function's
	// implementation defined in Overload.Fn. Therefore, if the function is
	// expected to produce side-effects with a NULL argument, NullableArgs must
	// be true. Note that if this behavior changes so that NullableArgs=false
	// functions can produce side-effects, the FoldFunctionWithNullArg optimizer
	// rule must be changed to avoid folding those functions.
	//
	// NOTE: when set, a function should be prepared for any of its arguments to
	// be NULL and should act accordingly.
	NullableArgs bool

	// DistsqlBlocklist is set to true when a function depends on
	// members of the EvalContext that are not marshaled by DistSQL
	// (e.g. planner). Currently used for DistSQL to determine if
	// expressions can be evaluated on a different node without sending
	// over the EvalContext.
	//
	// TODO(andrei): Get rid of the planner from the EvalContext and then we can
	// get rid of this blocklist.
	DistsqlBlocklist bool

	// Class is the kind of built-in function (normal/aggregate/window/etc.)
	Class FunctionClass

	// Category is used to generate documentation strings.
	Category string

	// AvailableOnPublicSchema indicates whether the function can be resolved
	// if it is found on the public schema.
	AvailableOnPublicSchema bool

	// ReturnLabels can be used to override the return column name of a
	// function in a FROM clause.
	// This satisfies a Postgres quirk where some json functions have
	// different return labels when used in SELECT or FROM clause.
	ReturnLabels []string

	// AmbiguousReturnType is true if the builtin's return type can't be
	// determined without extra context. This is used for formatting builtins
	// with the FmtParsable directive.
	AmbiguousReturnType bool

	// HasSequenceArguments is true if the builtin function takes in a sequence
	// name (string) and can be used in a scalar expression.
	// TODO(richardjcai): When implicit casting is supported, these builtins
	// should take RegClass as the arg type for the sequence name instead of
	// string, we will add a dependency on all RegClass types used in a view.
	HasSequenceArguments bool
}

// ShouldDocument returns whether the built-in function should be included in
// external-facing documentation.
func (fp *FunctionProperties) ShouldDocument() bool {
	return !(fp.Undocumented || fp.Private)
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
	// SQLClass is a builtin function that executes a SQL statement as a side
	// effect of the function call.
	//
	// For example, AddGeometryColumn is a SQLClass function that executes an
	// ALTER TABLE ... ADD COLUMN statement to add a geometry column to an
	// existing table. It returns metadata about the column added.
	//
	// All builtin functions of this class should include a definition for
	// Overload.SQLFn, which returns the SQL statement to be executed. They
	// should also include a definition for Overload.Fn, which is executed
	// like a NormalClass function and returns a Datum.
	SQLClass
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
		// Produce separate telemetry for each overload.
		def[i].counter = sqltelemetry.BuiltinCounter(name, def[i].Signature(false))

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

// OidToBuiltinName contains a map from the hashed OID of all builtin functions
// to their name. We populate this from the pg_catalog.go file in the sql
// package because of dependency issues: we can't use oidHasher from this file.
var OidToBuiltinName map[oid.Oid]string

// Format implements the NodeFormatter interface.
func (fd *FunctionDefinition) Format(ctx *FmtCtx) {
	ctx.WriteString(fd.Name)
}
func (fd *FunctionDefinition) String() string { return AsString(fd) }
