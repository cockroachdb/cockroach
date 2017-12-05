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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/json"
)

// See the comments at the start of generators.go for details about
// this functionality.

// generatorFactory is the type of constructor functions for
// tree.ValueGenerator objects suitable for use with tree.DTable.
type generatorFactory func(ctx *tree.EvalContext, args tree.Datums) (tree.ValueGenerator, error)

var _ tree.ValueGenerator = &seriesValueGenerator{}
var _ tree.ValueGenerator = &arrayValueGenerator{}

func initGeneratorBuiltins() {
	// Add all windows to the Builtins map after a few sanity checks.
	for k, v := range Generators {
		for _, g := range v {
			if !g.Impure {
				panic(fmt.Sprintf("generator functions should all be impure, found %v", g))
			}
			if g.Class != tree.GeneratorClass {
				panic(fmt.Sprintf("generator functions should be marked with the tree.GeneratorClass "+
					"function class, found %v", g))
			}
		}
		Builtins[k] = v
	}
}

// Generators is a map from name to slice of Builtins for all built-in
// generators.
var Generators = map[string][]tree.Builtin{
	"generate_series": {
		makeGeneratorBuiltin(
			tree.ArgTypes{{"start", types.Int}, {"end", types.Int}},
			seriesValueGeneratorType,
			makeSeriesGenerator,
			"Produces a virtual table containing the integer values from `start` to `end`, inclusive.",
		),
		makeGeneratorBuiltin(
			tree.ArgTypes{{"start", types.Int}, {"end", types.Int}, {"step", types.Int}},
			seriesValueGeneratorType,
			makeSeriesGenerator,
			"Produces a virtual table containing the integer values from `start` to `end`, inclusive, by increment of `step`.",
		),
	},
	"pg_get_keywords": {
		makeGeneratorBuiltin(
			tree.ArgTypes{},
			keywordsValueGeneratorType,
			makeKeywordsGenerator,
			"Produces a virtual table containing the keywords known to the SQL parser.",
		),
	},
	"unnest": {
		makeGeneratorBuiltinWithReturnType(
			tree.ArgTypes{{"input", types.AnyArray}},
			func(args []tree.TypedExpr) types.T {
				if len(args) == 0 {
					return tree.UnknownReturnType
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
			tree.ArgTypes{},
			unaryValueGeneratorType,
			makeUnaryGenerator,
			"Produces a virtual table containing a single row with no values.\n\n"+
				"This function is used only by CockroachDB's developers for testing purposes.",
		),
	},
	"json_array_elements":       {jsonArrayElementsImpl},
	"jsonb_array_elements":      {jsonArrayElementsImpl},
	"json_array_elements_text":  {jsonArrayElementsTextImpl},
	"jsonb_array_elements_text": {jsonArrayElementsTextImpl},
	"json_object_keys":          {jsonObjectKeysImpl},
	"jsonb_object_keys":         {jsonObjectKeysImpl},
}

func makeGeneratorBuiltin(
	in tree.ArgTypes, ret types.TTable, g generatorFactory, info string,
) tree.Builtin {
	return makeGeneratorBuiltinWithReturnType(in, tree.FixedReturnType(ret), g, info)
}

func makeGeneratorBuiltinWithReturnType(
	in tree.ArgTypes, retType tree.ReturnTyper, g generatorFactory, info string,
) tree.Builtin {
	return tree.Builtin{
		Impure:     true,
		Class:      tree.GeneratorClass,
		Types:      in,
		ReturnType: retType,
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			gen, err := g(ctx, args)
			if err != nil {
				return nil, err
			}
			return &tree.DTable{ValueGenerator: gen}, nil
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

func makeKeywordsGenerator(_ *tree.EvalContext, _ tree.Datums) (tree.ValueGenerator, error) {
	return &keywordsValueGenerator{}, nil
}

// ResolvedType implements the tree.ValueGenerator interface.
func (*keywordsValueGenerator) ResolvedType() types.TTable { return keywordsValueGeneratorType }

// Close implements the tree.ValueGenerator interface.
func (*keywordsValueGenerator) Close() {}

// Start implements the tree.ValueGenerator interface.
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

// Values implements the tree.ValueGenerator interface.
func (k *keywordsValueGenerator) Values() tree.Datums {
	kw := keywordNames[k.curKeyword]
	info := lex.Keywords[kw]
	cat := info.Cat
	desc := keywordCategoryDescriptions[cat]
	return tree.Datums{tree.NewDString(kw), tree.NewDString(cat), tree.NewDString(desc)}
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

func makeSeriesGenerator(_ *tree.EvalContext, args tree.Datums) (tree.ValueGenerator, error) {
	start := int64(tree.MustBeDInt(args[0]))
	stop := int64(tree.MustBeDInt(args[1]))
	step := int64(1)
	if len(args) > 2 {
		step = int64(tree.MustBeDInt(args[2]))
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

// ResolvedType implements the tree.ValueGenerator interface.
func (*seriesValueGenerator) ResolvedType() types.TTable { return seriesValueGeneratorType }

// Start implements the tree.ValueGenerator interface.
func (s *seriesValueGenerator) Start() error { return nil }

// Close implements the tree.ValueGenerator interface.
func (s *seriesValueGenerator) Close() {}

// Next implements the tree.ValueGenerator interface.
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
	s.start, s.nextOK = tree.AddWithOverflow(s.start, s.step)
	return true, nil
}

// Values implements the tree.ValueGenerator interface.
func (s *seriesValueGenerator) Values() tree.Datums {
	return tree.Datums{tree.NewDInt(tree.DInt(s.value))}
}

func makeArrayGenerator(_ *tree.EvalContext, args tree.Datums) (tree.ValueGenerator, error) {
	arr := tree.MustBeDArray(args[0])
	return &arrayValueGenerator{array: arr}, nil
}

// arrayValueGenerator is a value generator that returns each element of an
// array.
type arrayValueGenerator struct {
	array     *tree.DArray
	nextIndex int
}

var arrayValueGeneratorLabels = []string{"unnest"}

// ResolvedType implements the tree.ValueGenerator interface.
func (s *arrayValueGenerator) ResolvedType() types.TTable {
	return types.TTable{
		Cols:   types.TTuple{s.array.ParamTyp},
		Labels: arrayValueGeneratorLabels,
	}
}

// Start implements the tree.ValueGenerator interface.
func (s *arrayValueGenerator) Start() error {
	s.nextIndex = -1
	return nil
}

// Close implements the tree.ValueGenerator interface.
func (s *arrayValueGenerator) Close() {}

// Next implements the tree.ValueGenerator interface.
func (s *arrayValueGenerator) Next() (bool, error) {
	s.nextIndex++
	if s.nextIndex >= s.array.Len() {
		return false, nil
	}
	return true, nil
}

// Values implements the tree.ValueGenerator interface.
func (s *arrayValueGenerator) Values() tree.Datums {
	return tree.Datums{s.array.Array[s.nextIndex]}
}

// EmptyDTable returns a new, empty tree.DTable.
func EmptyDTable() *tree.DTable {
	return &tree.DTable{ValueGenerator: &arrayValueGenerator{array: tree.NewDArray(types.Any)}}
}

// unaryValueGenerator supports the execution of crdb_internal.unary_table().
type unaryValueGenerator struct {
	done bool
}

var unaryValueGeneratorType = types.TTable{
	Cols:   types.TTuple{},
	Labels: []string{"unary_table"},
}

func makeUnaryGenerator(_ *tree.EvalContext, args tree.Datums) (tree.ValueGenerator, error) {
	return &unaryValueGenerator{}, nil
}

// ResolvedType implements the tree.ValueGenerator interface.
func (*unaryValueGenerator) ResolvedType() types.TTable { return unaryValueGeneratorType }

// Start implements the tree.ValueGenerator interface.
func (s *unaryValueGenerator) Start() error { return nil }

// Close implements the tree.ValueGenerator interface.
func (s *unaryValueGenerator) Close() {}

// Next implements the tree.ValueGenerator interface.
func (s *unaryValueGenerator) Next() (bool, error) {
	if !s.done {
		s.done = true
		return true, nil
	}
	return false, nil
}

// Values implements the tree.ValueGenerator interface.
func (s *unaryValueGenerator) Values() tree.Datums { return tree.Datums{} }

var jsonArrayElementsImpl = makeGeneratorBuiltin(
	tree.ArgTypes{{"input", types.JSON}},
	jsonArrayGeneratorType,
	makeJSONArrayAsJSONGenerator,
	"Expands a JSON array to a set of JSON values.",
)

var jsonArrayElementsTextImpl = makeGeneratorBuiltin(
	tree.ArgTypes{{"input", types.JSON}},
	jsonArrayTextGeneratorType,
	makeJSONArrayAsTextGenerator,
	"Expands a JSON array to a set of text values.",
)

var jsonArrayGeneratorType = types.TTable{
	Cols:   types.TTuple{types.JSON},
	Labels: []string{"value"},
}

var jsonArrayTextGeneratorType = types.TTable{
	Cols:   types.TTuple{types.String},
	Labels: []string{"value"},
}

type jsonArrayGenerator struct {
	json      tree.DJSON
	nextIndex int
	asText    bool
}

var errJSONCallOnNonArray = pgerror.NewError(pgerror.CodeInvalidParameterValueError,
	"cannot be called on a non-array")

func makeJSONArrayAsJSONGenerator(
	_ *tree.EvalContext, args tree.Datums,
) (tree.ValueGenerator, error) {
	return makeJSONArrayGenerator(args, false)
}

func makeJSONArrayAsTextGenerator(
	_ *tree.EvalContext, args tree.Datums,
) (tree.ValueGenerator, error) {
	return makeJSONArrayGenerator(args, true)
}

func makeJSONArrayGenerator(args tree.Datums, asText bool) (tree.ValueGenerator, error) {
	target := tree.MustBeDJSON(args[0])
	if target.Type() != json.ArrayJSONType {
		return nil, errJSONCallOnNonArray
	}
	return &jsonArrayGenerator{
		json:   target,
		asText: asText,
	}, nil
}

// ResolvedType implements the tree.ValueGenerator interface.
func (g *jsonArrayGenerator) ResolvedType() types.TTable {
	if g.asText {
		return jsonArrayTextGeneratorType
	}
	return jsonArrayGeneratorType
}

// Start implements the tree.ValueGenerator interface.
func (g *jsonArrayGenerator) Start() error {
	g.nextIndex = -1
	return nil
}

// Close implements the tree.ValueGenerator interface.
func (g *jsonArrayGenerator) Close() {}

// Next implements the tree.ValueGenerator interface.
func (g *jsonArrayGenerator) Next() (bool, error) {
	g.nextIndex++
	return g.json.FetchValIdx(g.nextIndex) != nil, nil
}

// Values implements the tree.ValueGenerator interface.
func (g *jsonArrayGenerator) Values() tree.Datums {
	val := g.json.FetchValIdx(g.nextIndex)
	if g.asText {
		text := val.AsText()
		if text == nil {
			return tree.Datums{
				tree.DNull,
			}
		}
		return tree.Datums{
			tree.NewDString(*text),
		}
	}
	return tree.Datums{
		&tree.DJSON{
			JSON: val,
		},
	}
}

// jsonObjectKeysImpl is a key generator of a JSON object.
var jsonObjectKeysImpl = makeGeneratorBuiltin(
	tree.ArgTypes{{"input", types.JSON}},
	jsonObjectKeysGeneratorType,
	makeJSONObjectKeysGenerator,
	"Returns sorted set of keys in the outermost JSON object.",
)

var jsonObjectKeysGeneratorType = types.TTable{
	Cols:   types.TTuple{types.String},
	Labels: []string{"json_object_keys"},
}

type jsonObjectKeysGenerator struct {
	iter    *json.ObjectKeyIterator
	nextVal string
}

func makeJSONObjectKeysGenerator(
	_ *tree.EvalContext, args tree.Datums,
) (tree.ValueGenerator, error) {
	target := tree.MustBeDJSON(args[0])
	iter, err := target.IterObjectKey()
	if err != nil {
		return nil, err
	}
	return &jsonObjectKeysGenerator{
		iter: iter,
	}, nil
}

// ResolvedType implements the tree.ValueGenerator interface.
func (g *jsonObjectKeysGenerator) ResolvedType() types.TTable {
	return jsonObjectKeysGeneratorType
}

// Start implements the tree.ValueGenerator interface.
func (g *jsonObjectKeysGenerator) Start() error { return nil }

// Close implements the tree.ValueGenerator interface.
func (g *jsonObjectKeysGenerator) Close() {}

// Next implements the tree.ValueGenerator interface.
func (g *jsonObjectKeysGenerator) Next() (bool, error) {
	ok, val := g.iter.Next()
	g.nextVal = val
	return ok, nil
}

// Values implements the tree.ValueGenerator interface.
func (g *jsonObjectKeysGenerator) Values() tree.Datums {
	return tree.Datums{tree.NewDString(g.nextVal)}
}
