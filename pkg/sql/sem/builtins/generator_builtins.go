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

package builtins

import (
	"fmt"
	"sort"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// See the comments at the start of generators.go for details about
// this functionality.

// generatorFactory is the type of constructor functions for
// parser.ValueGenerator objects suitable for use with parser.DTable.
type generatorFactory func(ctx *parser.EvalContext, args parser.Datums) (parser.ValueGenerator, error)

var _ parser.ValueGenerator = &seriesValueGenerator{}
var _ parser.ValueGenerator = &arrayValueGenerator{}

func initGeneratorBuiltins() {
	// Add all windows to the Builtins map after a few sanity checks.
	for k, v := range Generators {
		for _, g := range v {
			if !g.Impure {
				panic(fmt.Sprintf("generator functions should all be impure, found %v", g))
			}
			if g.Class != parser.GeneratorClass {
				panic(fmt.Sprintf("generator functions should be marked with the parser.GeneratorClass "+
					"function class, found %v", g))
			}
		}
		Builtins[k] = v
	}
}

// Generators is a map from name to slice of Builtins for all built-in
// generators.
var Generators = map[string][]parser.Builtin{
	"generate_series": {
		makeGeneratorBuiltin(
			parser.ArgTypes{{"start", types.Int}, {"end", types.Int}},
			seriesValueGeneratorType,
			makeSeriesGenerator,
			"Produces a virtual table containing the integer values from `start` to `end`, inclusive.",
		),
		makeGeneratorBuiltin(
			parser.ArgTypes{{"start", types.Int}, {"end", types.Int}, {"step", types.Int}},
			seriesValueGeneratorType,
			makeSeriesGenerator,
			"Produces a virtual table containing the integer values from `start` to `end`, inclusive, by increment of `step`.",
		),
	},
	"pg_get_keywords": {
		makeGeneratorBuiltin(
			parser.ArgTypes{},
			keywordsValueGeneratorType,
			makeKeywordsGenerator,
			"Produces a virtual table containing the keywords known to the SQL parser.",
		),
	},
	"unnest": {
		makeGeneratorBuiltinWithReturnType(
			parser.ArgTypes{{"input", types.AnyArray}},
			func(args []parser.TypedExpr) types.T {
				if len(args) == 0 {
					return parser.UnknownReturnType
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
			parser.ArgTypes{},
			unaryValueGeneratorType,
			makeUnaryGenerator,
			"Produces a virtual table containing a single row with no values.\n\n"+
				"This function is used only by CockroachDB's developers for testing purposes.",
		),
	},
}

func makeGeneratorBuiltin(
	in parser.ArgTypes, ret types.TTable, g generatorFactory, info string,
) parser.Builtin {
	return makeGeneratorBuiltinWithReturnType(in, parser.FixedReturnType(ret), g, info)
}

func makeGeneratorBuiltinWithReturnType(
	in parser.ArgTypes, retType parser.ReturnTyper, g generatorFactory, info string,
) parser.Builtin {
	return parser.Builtin{
		Impure:     true,
		Class:      parser.GeneratorClass,
		Types:      in,
		ReturnType: retType,
		Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
			gen, err := g(ctx, args)
			if err != nil {
				return nil, err
			}
			return &parser.DTable{ValueGenerator: gen}, nil
		},
		Category: categoryCompatibility,
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

func makeKeywordsGenerator(_ *parser.EvalContext, _ parser.Datums) (parser.ValueGenerator, error) {
	return &keywordsValueGenerator{}, nil
}

// ResolvedType implements the parser.ValueGenerator interface.
func (*keywordsValueGenerator) ResolvedType() types.TTable { return keywordsValueGeneratorType }

// Close implements the parser.ValueGenerator interface.
func (*keywordsValueGenerator) Close() {}

// Start implements the parser.ValueGenerator interface.
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

// Values implements the parser.ValueGenerator interface.
func (k *keywordsValueGenerator) Values() parser.Datums {
	kw := keywordNames[k.curKeyword]
	info := lex.Keywords[kw]
	cat := info.Cat
	desc := keywordCategoryDescriptions[cat]
	return parser.Datums{parser.NewDString(kw), parser.NewDString(cat), parser.NewDString(desc)}
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

func makeSeriesGenerator(_ *parser.EvalContext, args parser.Datums) (parser.ValueGenerator, error) {
	start := int64(parser.MustBeDInt(args[0]))
	stop := int64(parser.MustBeDInt(args[1]))
	step := int64(1)
	if len(args) > 2 {
		step = int64(parser.MustBeDInt(args[2]))
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

// ResolvedType implements the parser.ValueGenerator interface.
func (*seriesValueGenerator) ResolvedType() types.TTable { return seriesValueGeneratorType }

// Start implements the parser.ValueGenerator interface.
func (s *seriesValueGenerator) Start() error { return nil }

// Close implements the parser.ValueGenerator interface.
func (s *seriesValueGenerator) Close() {}

// Next implements the parser.ValueGenerator interface.
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
	s.start, s.nextOK = parser.AddWithOverflow(s.start, s.step)
	return true, nil
}

// Values implements the parser.ValueGenerator interface.
func (s *seriesValueGenerator) Values() parser.Datums {
	return parser.Datums{parser.NewDInt(parser.DInt(s.value))}
}

func makeArrayGenerator(_ *parser.EvalContext, args parser.Datums) (parser.ValueGenerator, error) {
	arr := parser.MustBeDArray(args[0])
	return &arrayValueGenerator{array: arr}, nil
}

// arrayValueGenerator is a value generator that returns each element of an
// array.
type arrayValueGenerator struct {
	array     *parser.DArray
	nextIndex int
}

var arrayValueGeneratorLabels = []string{"unnest"}

// ResolvedType implements the parser.ValueGenerator interface.
func (s *arrayValueGenerator) ResolvedType() types.TTable {
	return types.TTable{
		Cols:   types.TTuple{s.array.ParamTyp},
		Labels: arrayValueGeneratorLabels,
	}
}

// Start implements the parser.ValueGenerator interface.
func (s *arrayValueGenerator) Start() error {
	s.nextIndex = -1
	return nil
}

// Close implements the parser.ValueGenerator interface.
func (s *arrayValueGenerator) Close() {}

// Next implements the parser.ValueGenerator interface.
func (s *arrayValueGenerator) Next() (bool, error) {
	s.nextIndex++
	if s.nextIndex >= s.array.Len() {
		return false, nil
	}
	return true, nil
}

// Values implements the parser.ValueGenerator interface.
func (s *arrayValueGenerator) Values() parser.Datums {
	return parser.Datums{s.array.Array[s.nextIndex]}
}

// EmptyDTable returns a new, empty parser.DTable.
func EmptyDTable() *parser.DTable {
	return &parser.DTable{ValueGenerator: &arrayValueGenerator{array: parser.NewDArray(types.Any)}}
}

// unaryValueGenerator supports the execution of crdb_internal.unary_table().
type unaryValueGenerator struct {
	done bool
}

var unaryValueGeneratorType = types.TTable{
	Cols:   types.TTuple{},
	Labels: []string{"unary_table"},
}

func makeUnaryGenerator(_ *parser.EvalContext, args parser.Datums) (parser.ValueGenerator, error) {
	return &unaryValueGenerator{}, nil
}

// ResolvedType implements the parser.ValueGenerator interface.
func (*unaryValueGenerator) ResolvedType() types.TTable { return unaryValueGeneratorType }

// Start implements the parser.ValueGenerator interface.
func (s *unaryValueGenerator) Start() error { return nil }

// Close implements the parser.ValueGenerator interface.
func (s *unaryValueGenerator) Close() {}

// Next implements the parser.ValueGenerator interface.
func (s *unaryValueGenerator) Next() (bool, error) {
	if !s.done {
		s.done = true
		return true, nil
	}
	return false, nil
}

// Values implements the parser.ValueGenerator interface.
func (s *unaryValueGenerator) Values() parser.Datums { return parser.Datums{} }
