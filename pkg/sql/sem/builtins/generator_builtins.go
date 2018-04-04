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
	"time"

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
		makeGeneratorBuiltin(
			tree.ArgTypes{{"start", types.Timestamp}, {"end", types.Timestamp}, {"step", types.Interval}},
			seriesTSValueGeneratorType,
			makeTSSeriesGenerator,
			"Produces a virtual table containing the timestamp values from `start` to `end`, inclusive, by increment of `step`.",
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
				t := types.UnwrapType(args[0].ResolvedType()).(types.TArray).Typ
				return types.TTable{
					Cols:   types.TTuple{t},
					Labels: arrayValueGeneratorLabels,
				}
			},
			makeArrayGenerator,
			"Returns the input array as a set of rows",
		),
	},
	"information_schema._pg_expandarray": {
		makeGeneratorBuiltinWithReturnType(
			tree.ArgTypes{{"input", types.AnyArray}},
			func(args []tree.TypedExpr) types.T {
				if len(args) == 0 {
					return tree.UnknownReturnType
				}
				t := types.UnwrapType(args[0].ResolvedType()).(types.TArray).Typ
				return types.TTable{
					Cols:   types.TTuple{t, types.Int},
					Labels: expandArrayValueGeneratorLabels,
				}
			},
			makeExpandArrayGenerator,
			"Returns the input array as a set of rows with an index",
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
	"json_each":                 {jsonEachImpl},
	"jsonb_each":                {jsonEachImpl},
	"json_each_text":            {jsonEachTextImpl},
	"jsonb_each_text":           {jsonEachTextImpl},
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
	value, start, stop, step interface{}
	nextOK                   bool
	genType                  types.TTable
	next                     func(*seriesValueGenerator) (bool, error)
	genValue                 func(*seriesValueGenerator) tree.Datums
}

var seriesValueGeneratorType = types.TTable{
	Cols:   types.TTuple{types.Int},
	Labels: []string{"generate_series"},
}

var seriesTSValueGeneratorType = types.TTable{
	Cols:   types.TTuple{types.Timestamp},
	Labels: []string{"generate_series"},
}

var errStepCannotBeZero = pgerror.NewError(pgerror.CodeInvalidParameterValueError, "step cannot be 0")

func seriesIntNext(s *seriesValueGenerator) (bool, error) {
	step := s.step.(int64)
	start := s.start.(int64)
	stop := s.stop.(int64)

	if !s.nextOK {
		return false, nil
	}
	if step < 0 && (start < stop) {
		return false, nil
	}
	if step > 0 && (stop < start) {
		return false, nil
	}
	s.value = start
	s.start, s.nextOK = tree.AddWithOverflow(start, step)
	return true, nil
}

func seriesGenIntValue(s *seriesValueGenerator) tree.Datums {
	return tree.Datums{tree.NewDInt(tree.DInt(s.value.(int64)))}
}

func seriesTSNext(s *seriesValueGenerator) (bool, error) {
	step := s.step.(time.Duration)
	start := s.start.(time.Time)
	stop := s.stop.(time.Time)

	if !s.nextOK {
		return false, nil
	}

	if step < 0 && (start.Before(stop)) {
		return false, nil
	}
	if step > 0 && (stop.Before(start)) {
		return false, nil
	}

	s.value = start
	s.start = start.Add(step)
	return true, nil
}

func seriesGenTSValue(s *seriesValueGenerator) tree.Datums {
	return tree.Datums{tree.MakeDTimestamp(s.value.(time.Time), time.Microsecond)}
}

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
		value:    start,
		start:    start,
		stop:     stop,
		step:     step,
		nextOK:   true,
		genType:  seriesValueGeneratorType,
		genValue: seriesGenIntValue,
		next:     seriesIntNext,
	}, nil
}

func makeTSSeriesGenerator(_ *tree.EvalContext, args tree.Datums) (tree.ValueGenerator, error) {
	start := args[0].(*tree.DTimestamp).Time
	stop := args[1].(*tree.DTimestamp).Time
	step := time.Duration(args[2].(*tree.DInterval).Nanos) * time.Nanosecond

	if step == 0 {
		return nil, errStepCannotBeZero
	}

	return &seriesValueGenerator{
		value:    start,
		start:    start,
		stop:     stop,
		step:     step,
		nextOK:   true,
		genType:  seriesTSValueGeneratorType,
		genValue: seriesGenTSValue,
		next:     seriesTSNext,
	}, nil
}

// ResolvedType implements the tree.ValueGenerator interface.
func (s *seriesValueGenerator) ResolvedType() types.TTable {
	return s.genType
}

// Start implements the tree.ValueGenerator interface.
func (s *seriesValueGenerator) Start() error { return nil }

// Close implements the tree.ValueGenerator interface.
func (s *seriesValueGenerator) Close() {}

// Next implements the tree.ValueGenerator interface.
func (s *seriesValueGenerator) Next() (bool, error) {
	return s.next(s)
}

// Values implements the tree.ValueGenerator interface.
func (s *seriesValueGenerator) Values() tree.Datums {
	return s.genValue(s)
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

func makeExpandArrayGenerator(
	evalCtx *tree.EvalContext, args tree.Datums,
) (tree.ValueGenerator, error) {
	arr := tree.MustBeDArray(args[0])
	return &expandArrayValueGenerator{avg: arrayValueGenerator{array: arr}}, nil
}

// expandArrayValueGenerator is a value generator that returns each element of
// an array and an index for it.
type expandArrayValueGenerator struct {
	avg arrayValueGenerator
}

var expandArrayValueGeneratorLabels = []string{"x", "n"}

// ResolvedType implements the tree.ValueGenerator interface.
func (s *expandArrayValueGenerator) ResolvedType() types.TTable {
	return types.TTable{
		Cols:   types.TTuple{s.avg.array.ParamTyp, types.Int},
		Labels: expandArrayValueGeneratorLabels,
	}
}

// Start implements the tree.ValueGenerator interface.
func (s *expandArrayValueGenerator) Start() error {
	s.avg.nextIndex = -1
	return nil
}

// Close implements the tree.ValueGenerator interface.
func (s *expandArrayValueGenerator) Close() {}

// Next implements the tree.ValueGenerator interface.
func (s *expandArrayValueGenerator) Next() (bool, error) {
	s.avg.nextIndex++
	if s.avg.nextIndex >= s.avg.array.Len() {
		return false, nil
	}
	return true, nil
}

// Values implements the tree.ValueGenerator interface.
func (s *expandArrayValueGenerator) Values() tree.Datums {
	// Expand array's index is 1 based.
	return tree.Datums{
		s.avg.array.Array[s.avg.nextIndex],
		tree.NewDInt(tree.DInt(s.avg.nextIndex + 1)),
	}
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

func jsonAsText(j json.JSON) (tree.Datum, error) {
	text, err := j.AsText()
	if err != nil {
		return nil, err
	}
	if text == nil {
		return tree.DNull, nil
	}
	return tree.NewDString(*text), nil
}

var (
	errJSONObjectKeysOnArray         = pgerror.NewError(pgerror.CodeInvalidParameterValueError, "cannot call json_object_keys on an array")
	errJSONObjectKeysOnScalar        = pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError, "cannot call json_object_keys on a scalar")
	errJSONDeconstructArrayAsObject  = pgerror.NewError(pgerror.CodeInvalidParameterValueError, "cannot deconstruct an array as an object")
	errJSONDeconstructScalarAsObject = pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError, "cannot deconstruct a scalar")
)

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
	value     tree.Datum
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
	g.json.JSON = g.json.JSON.MaybeDecode()
	g.value = nil
	return nil
}

// Close implements the tree.ValueGenerator interface.
func (g *jsonArrayGenerator) Close() {}

// Next implements the tree.ValueGenerator interface.
func (g *jsonArrayGenerator) Next() (bool, error) {
	g.nextIndex++
	next, err := g.json.FetchValIdx(g.nextIndex)
	if err != nil || next == nil {
		return false, err
	}
	if g.asText {
		if g.value, err = jsonAsText(next); err != nil {
			return false, err
		}
	} else {
		g.value = tree.NewDJSON(next)
	}
	return true, nil
}

// Values implements the tree.ValueGenerator interface.
func (g *jsonArrayGenerator) Values() tree.Datums {
	return tree.Datums{g.value}
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
	iter *json.ObjectIterator
}

func makeJSONObjectKeysGenerator(
	_ *tree.EvalContext, args tree.Datums,
) (tree.ValueGenerator, error) {
	target := tree.MustBeDJSON(args[0])
	iter, err := target.ObjectIter()
	if err != nil {
		return nil, err
	}
	if iter == nil {
		switch target.Type() {
		case json.ArrayJSONType:
			return nil, errJSONObjectKeysOnArray
		default:
			return nil, errJSONObjectKeysOnScalar
		}
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
	return g.iter.Next(), nil
}

// Values implements the tree.ValueGenerator interface.
func (g *jsonObjectKeysGenerator) Values() tree.Datums {
	return tree.Datums{tree.NewDString(g.iter.Key())}
}

var jsonEachImpl = makeGeneratorBuiltin(
	tree.ArgTypes{{"input", types.JSON}},
	jsonEachGeneratorType,
	makeJSONEachImplGenerator,
	"Expands the outermost JSON or JSONB object into a set of key/value pairs.",
)

var jsonEachTextImpl = makeGeneratorBuiltin(
	tree.ArgTypes{{"input", types.JSON}},
	jsonEachTextGeneratorType,
	makeJSONEachTextImplGenerator,
	"Expands the outermost JSON or JSONB object into a set of key/value pairs. "+
		"The returned values will be of type text.",
)

var jsonEachGeneratorType = types.TTable{
	Cols:   types.TTuple{types.String, types.JSON},
	Labels: []string{"key", "value"},
}

var jsonEachTextGeneratorType = types.TTable{
	Cols:   types.TTuple{types.String, types.String},
	Labels: []string{"key", "value"},
}

type jsonEachGenerator struct {
	iter   *json.ObjectIterator
	key    tree.Datum
	value  tree.Datum
	asText bool
}

func makeJSONEachImplGenerator(_ *tree.EvalContext, args tree.Datums) (tree.ValueGenerator, error) {
	return makeJSONEachGenerator(args, false)
}

func makeJSONEachTextImplGenerator(
	_ *tree.EvalContext, args tree.Datums,
) (tree.ValueGenerator, error) {
	return makeJSONEachGenerator(args, true)
}

func makeJSONEachGenerator(args tree.Datums, asText bool) (tree.ValueGenerator, error) {
	target := tree.MustBeDJSON(args[0])
	iter, err := target.ObjectIter()
	if err != nil {
		return nil, err
	}
	if iter == nil {
		switch target.Type() {
		case json.ArrayJSONType:
			return nil, errJSONDeconstructArrayAsObject
		default:
			return nil, errJSONDeconstructScalarAsObject
		}
	}
	return &jsonEachGenerator{
		iter:   iter,
		key:    nil,
		value:  nil,
		asText: asText,
	}, nil
}

// ResolvedType implements the tree.ValueGenerator interface.
func (g *jsonEachGenerator) ResolvedType() types.TTable {
	if g.asText {
		return jsonEachTextGeneratorType
	}
	return jsonEachGeneratorType
}

// Start implements the tree.ValueGenerator interface.
func (g *jsonEachGenerator) Start() error { return nil }

// Close implements the tree.ValueGenerator interface.
func (g *jsonEachGenerator) Close() {}

// Next implements the tree.ValueGenerator interface.
func (g *jsonEachGenerator) Next() (bool, error) {
	if !g.iter.Next() {
		return false, nil
	}
	g.key = tree.NewDString(g.iter.Key())
	if g.asText {
		var err error
		if g.value, err = jsonAsText(g.iter.Value()); err != nil {
			return false, err
		}
	} else {
		g.value = tree.NewDJSON(g.iter.Value())
	}
	return true, nil
}

// Values implements the tree.ValueGenerator interface.
func (g *jsonEachGenerator) Values() tree.Datums {
	return tree.Datums{g.key, g.value}
}
