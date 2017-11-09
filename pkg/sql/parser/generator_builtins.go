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

package parser

import (
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// See the comments at the start of generators.go for details about
// this functionality.

// generatorFactory is the type of constructor functions for
// ValueGenerator objects suitable for use with DTable.
type generatorFactory func(ctx *EvalContext, args Datums) (ValueGenerator, error)

var _ ValueGenerator = &seriesValueGenerator{}
var _ ValueGenerator = &arrayValueGenerator{}

func initGeneratorBuiltins() {
	// Add all windows to the Builtins map after a few sanity checks.
	for k, v := range Generators {
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

// Generators is a map from name to slice of Builtins for all built-in
// generators.
var Generators = map[string][]Builtin{
	"generate_series": {
		makeGeneratorBuiltin(
			ArgTypes{{"start", types.Int}, {"end", types.Int}},
			seriesValueGeneratorType,
			makeSeriesGenerator,
			"Produces a virtual table containing the integer values from `start` to `end`, inclusive.",
		),
		makeGeneratorBuiltin(
			ArgTypes{{"start", types.Int}, {"end", types.Int}, {"step", types.Int}},
			seriesValueGeneratorType,
			makeSeriesGenerator,
			"Produces a virtual table containing the integer values from `start` to `end`, inclusive, by increment of `step`.",
		),
	},
	"pg_get_keywords": {
		makeGeneratorBuiltin(
			ArgTypes{},
			keywordsValueGeneratorType,
			makeKeywordsGenerator,
			"Produces a virtual table containing the keywords known to the SQL parser.",
		),
	},
	"unnest": {
		makeGeneratorBuiltinWithReturnType(
			ArgTypes{{"input", types.AnyArray}},
			func(args []TypedExpr) types.T {
				if len(args) == 0 {
					return unknownReturnType
				}
				return types.TTable{
					Cols:   types.TTuple{args[0].ResolvedType().(types.TArray).Typ},
					Labels: arrayValueGeneratorLabels,
				}
			},
			makeArrayGenerator,
			"Returns the input array as a set of rows",
		),
	},
	"crdb_internal.unary_table": {
		makeGeneratorBuiltin(
			ArgTypes{},
			unaryValueGeneratorType,
			makeUnaryGenerator,
			"Produces a virtual table containing a single row with no values.\n\n"+
				"This function is used only by CockroachDB's developers for testing purposes.",
		),
	},
}

func makeGeneratorBuiltin(in ArgTypes, ret types.TTable, g generatorFactory, info string) Builtin {
	return makeGeneratorBuiltinWithReturnType(in, fixedReturnType(ret), g, info)
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

// keywordsValueGenerator supports the execution of pg_get_keywords().
type keywordsValueGenerator struct {
	curKeyword int
}

var keywordsValueGeneratorType = types.TTable{
	Cols:   types.TTuple{types.String, types.String, types.String},
	Labels: []string{"word", "catcode", "catdesc"},
}

func makeKeywordsGenerator(_ *EvalContext, _ Datums) (ValueGenerator, error) {
	return &keywordsValueGenerator{}, nil
}

// ResolvedType implements the ValueGenerator interface.
func (*keywordsValueGenerator) ResolvedType() types.TTable { return keywordsValueGeneratorType }

// Close implements the ValueGenerator interface.
func (*keywordsValueGenerator) Close() {}

// Start implements the ValueGenerator interface.
func (k *keywordsValueGenerator) Start() error {
	k.curKeyword = -1
	return nil
}
func (k *keywordsValueGenerator) Next() (bool, error) {
	k.curKeyword++
	if k.curKeyword >= len(keywordNames) {
		return false, nil
	}
	return true, nil
}

// Values implements the ValueGenerator interface.
func (k *keywordsValueGenerator) Values() Datums {
	kw := keywordNames[k.curKeyword]
	info := lex.Keywords[kw]
	cat := info.Cat
	desc := keywordCategoryDescriptions[cat]
	return Datums{NewDString(kw), NewDString(cat), NewDString(desc)}
}

var keywordCategoryDescriptions = map[string]string{
	"R": "reserved",
	"C": "unreserved (cannot be function or type name)",
	"T": "reserved (can be function or type name)",
	"U": "unreserved",
}

// keywordNames contains all the keys in the `keywords` map, sorted so
// that pg_get_keywords returns deterministic results.
var keywordNames = func() []string {
	ret := make([]string, 0, len(lex.Keywords))
	for k := range lex.Keywords {
		ret = append(ret, k)
	}
	sort.Strings(ret)
	return ret
}()

// seriesValueGenerator supports the execution of generate_series()
// with integer bounds.
type seriesValueGenerator struct {
	value, start, stop, step int64
	nextOK                   bool
}

var seriesValueGeneratorType = types.TTable{
	Cols:   types.TTuple{types.Int},
	Labels: []string{"generate_series"},
}

var errStepCannotBeZero = pgerror.NewError(pgerror.CodeInvalidParameterValueError, "step cannot be 0")

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
	return &seriesValueGenerator{
		value:  start,
		start:  start,
		stop:   stop,
		step:   step,
		nextOK: true,
	}, nil
}

// ResolvedType implements the ValueGenerator interface.
func (*seriesValueGenerator) ResolvedType() types.TTable { return seriesValueGeneratorType }

// Start implements the ValueGenerator interface.
func (s *seriesValueGenerator) Start() error { return nil }

// Close implements the ValueGenerator interface.
func (s *seriesValueGenerator) Close() {}

// Next implements the ValueGenerator interface.
func (s *seriesValueGenerator) Next() (bool, error) {
	if !s.nextOK {
		return false, nil
	}
	if s.step < 0 && (s.start < s.stop) {
		return false, nil
	}
	if s.step > 0 && (s.stop < s.start) {
		return false, nil
	}
	s.value = s.start
	s.start, s.nextOK = addWithOverflow(s.start, s.step)
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

var arrayValueGeneratorLabels = []string{"unnest"}

// ResolvedType implements the ValueGenerator interface.
func (s *arrayValueGenerator) ResolvedType() types.TTable {
	return types.TTable{
		Cols:   types.TTuple{s.array.ParamTyp},
		Labels: arrayValueGeneratorLabels,
	}
}

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

// unaryValueGenerator supports the execution of crdb_internal.unary_table().
type unaryValueGenerator struct {
	done bool
}

var unaryValueGeneratorType = types.TTable{
	Cols:   types.TTuple{},
	Labels: []string{"unary_table"},
}

func makeUnaryGenerator(_ *EvalContext, args Datums) (ValueGenerator, error) {
	return &unaryValueGenerator{}, nil
}

// ResolvedType implements the ValueGenerator interface.
func (*unaryValueGenerator) ResolvedType() types.TTable { return unaryValueGeneratorType }

// Start implements the ValueGenerator interface.
func (s *unaryValueGenerator) Start() error { return nil }

// Close implements the ValueGenerator interface.
func (s *unaryValueGenerator) Close() {}

// Next implements the ValueGenerator interface.
func (s *unaryValueGenerator) Next() (bool, error) {
	if !s.done {
		s.done = true
		return true, nil
	}
	return false, nil
}

// Values implements the ValueGenerator interface.
func (s *unaryValueGenerator) Values() Datums { return Datums{} }
