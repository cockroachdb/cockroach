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
//
// Author: Raphael 'kena' Poss (knz@cockroachlabs.com)

package parser

import (
	"errors"
	"fmt"
)

// Table generators, also called "set-generating functions", are
// special functions that return an entire table.
//
// Overview of the concepts:
//
// - generators are implemented as regular built-in functions that
//   return values of type DTable (this is a Datum type).
//
// - the return type of generators is a TTable. This describes objects
//   that are conceptually sets of rows. A TTable type is
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

// generatorFactory is the type of constructor functions for
// ValueGenerator objects suitable for use with DTable.
type generatorFactory func(ctx *EvalContext, args Datums) (ValueGenerator, error)

// ValueGenerator is the interface provided by the object held by a
// DTable; objects that implement this interface are able to produce
// rows of values in a streaming fashion (like Go iterators or
// generators in Python).
type ValueGenerator interface {
	// ColumnTypes returns the type signature of this value generator.
	// Used by DTable.ResolvedType().
	ColumnTypes() TTuple

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

var _ ValueGenerator = &seriesValueGenerator{}
var _ ValueGenerator = &arrayValueGenerator{}

func initGeneratorBuiltins() {
	// Add all windows to the Builtins map after a few sanity checks.
	for k, v := range generators {
		for _, g := range v {
			if !g.impure {
				panic(fmt.Sprintf("generator functions should all be impure, found %v", g))
			}
			if g.class != GeneratorClass {
				panic(fmt.Sprintf("generator functions should be marked with the GeneratorClass "+
					"function class, found %v", g))
			}
		}
		Builtins[k] = v
	}
}

var generators = map[string][]Builtin{
	"generate_series": {
		makeGeneratorBuiltin(
			ArgTypes{{"start", TypeInt}, {"end", TypeInt}},
			TTuple{TypeInt},
			makeSeriesGenerator,
			"Produces a virtual table containing the integer values from `start` to `end`, inclusive.",
		),
		makeGeneratorBuiltin(
			ArgTypes{{"start", TypeInt}, {"end", TypeInt}, {"step", TypeInt}},
			TTuple{TypeInt},
			makeSeriesGenerator,
			"Produces a virtual table containing the integer values from `start` to `end`, inclusive, by increment of `step`.",
		),
	},
	"unnest": {
		makeGeneratorBuiltinWithReturnType(
			ArgTypes{{"input", TypeAnyArray}},
			func(args []TypedExpr) Type {
				if len(args) == 0 {
					return unknownReturnType
				}
				return TTable{Cols: TTuple{args[0].ResolvedType().(TArray).Typ}}
			},
			makeArrayGenerator,
			"Returns the input array as a set of rows",
		),
	},
}

func makeGeneratorBuiltin(in ArgTypes, ret TTuple, g generatorFactory, info string) Builtin {
	return makeGeneratorBuiltinWithReturnType(in, fixedReturnType(TTable{Cols: ret}), g, info)
}

func makeGeneratorBuiltinWithReturnType(
	in ArgTypes, retType returnTyper, g generatorFactory, info string,
) Builtin {
	return Builtin{
		impure:     true,
		class:      GeneratorClass,
		Types:      in,
		ReturnType: retType,
		fn: func(ctx *EvalContext, args Datums) (Datum, error) {
			gen, err := g(ctx, args)
			if err != nil {
				return nil, err
			}
			return &DTable{gen}, nil
		},
		category: categoryCompatibility,
		Info:     info,
	}
}

// seriesValueGenerator supports the execution of generate_series()
// with integer bounds.
type seriesValueGenerator struct {
	value, start, stop, step int64
}

var errStepCannotBeZero = errors.New("step cannot be 0")

func makeSeriesGenerator(_ *EvalContext, args Datums) (ValueGenerator, error) {
	start := int64(MustBeDInt(args[0]))
	stop := int64(MustBeDInt(args[1]))
	step := int64(1)
	if len(args) > 2 {
		step = int64(MustBeDInt(args[2]))
	}
	if step == 0 {
		return nil, errStepCannotBeZero
	}
	return &seriesValueGenerator{start, start, stop, step}, nil
}

// ColumnTypes implements the ValueGenerator interface.
func (s *seriesValueGenerator) ColumnTypes() TTuple { return TTuple{TypeInt} }

// Start implements the ValueGenerator interface.
func (s *seriesValueGenerator) Start() error { return nil }

// Close implements the ValueGenerator interface.
func (s *seriesValueGenerator) Close() {}

// Next implements the ValueGenerator interface.
func (s *seriesValueGenerator) Next() (bool, error) {
	if s.step < 0 && (s.start < s.stop) {
		return false, nil
	}
	if s.step > 0 && (s.stop < s.start) {
		return false, nil
	}
	s.value = s.start
	s.start += s.step
	return true, nil
}

// Values implements the ValueGenerator interface.
func (s *seriesValueGenerator) Values() Datums {
	return Datums{NewDInt(DInt(s.value))}
}

func makeArrayGenerator(_ *EvalContext, args Datums) (ValueGenerator, error) {
	arr := MustBeDArray(args[0])
	return &arrayValueGenerator{array: arr}, nil
}

// arrayValueGenerator is a value generator that returns each element of an
// array.
type arrayValueGenerator struct {
	array     *DArray
	nextIndex int
}

// ColumnTypes implements the ValueGenerator interface.
func (s *arrayValueGenerator) ColumnTypes() TTuple { return TTuple{s.array.ParamTyp} }

// Start implements the ValueGenerator interface.
func (s *arrayValueGenerator) Start() error {
	s.nextIndex = -1
	return nil
}

// Close implements the ValueGenerator interface.
func (s *arrayValueGenerator) Close() {}

// Next implements the ValueGenerator interface.
func (s *arrayValueGenerator) Next() (bool, error) {
	s.nextIndex++
	if s.nextIndex >= s.array.Len() {
		return false, nil
	}
	return true, nil
}

// Values implements the ValueGenerator interface.
func (s *arrayValueGenerator) Values() Datums {
	return Datums{s.array.Array[s.nextIndex]}
}
