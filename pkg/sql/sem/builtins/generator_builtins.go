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
	"github.com/cockroachdb/cockroach/pkg/util/arith"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/json"
)

// See the comments at the start of generators.go for details about
// this functionality.

var _ tree.ValueGenerator = &seriesValueGenerator{}
var _ tree.ValueGenerator = &arrayValueGenerator{}

func initGeneratorBuiltins() {
	// Add all windows to the Builtins map after a few sanity checks.
	for k, v := range generators {
		if !v.props.Impure {
			panic(fmt.Sprintf("generator functions should all be impure, found %v", v))
		}
		if v.props.Class != tree.GeneratorClass {
			panic(fmt.Sprintf("generator functions should be marked with the tree.GeneratorClass "+
				"function class, found %v", v))
		}

		builtins[k] = v
	}
}

func genProps(labels []string) tree.FunctionProperties {
	return tree.FunctionProperties{
		Impure:       true,
		Class:        tree.GeneratorClass,
		Category:     categoryGenerator,
		ReturnLabels: labels,
	}
}

// generators is a map from name to slice of Builtins for all built-in
// generators.
//
// These functions are identified with Class == tree.GeneratorClass.
// The properties are reachable via tree.FunctionDefinition.
var generators = map[string]builtinDefinition{
	"generate_series": makeBuiltin(genProps(seriesValueGeneratorLabels),
		// See https://www.postgresql.org/docs/current/static/functions-srf.html#FUNCTIONS-SRF-SERIES
		makeGeneratorOverload(
			tree.ArgTypes{{"start", types.Int}, {"end", types.Int}},
			seriesValueGeneratorType,
			makeSeriesGenerator,
			"Produces a virtual table containing the integer values from `start` to `end`, inclusive.",
		),
		makeGeneratorOverload(
			tree.ArgTypes{{"start", types.Int}, {"end", types.Int}, {"step", types.Int}},
			seriesValueGeneratorType,
			makeSeriesGenerator,
			"Produces a virtual table containing the integer values from `start` to `end`, inclusive, by increment of `step`.",
		),
		makeGeneratorOverload(
			tree.ArgTypes{{"start", types.Timestamp}, {"end", types.Timestamp}, {"step", types.Interval}},
			seriesTSValueGeneratorType,
			makeTSSeriesGenerator,
			"Produces a virtual table containing the timestamp values from `start` to `end`, inclusive, by increment of `step`.",
		),
	),

	"pg_get_keywords": makeBuiltin(genProps(keywordsValueGeneratorType.Labels),
		// See https://www.postgresql.org/docs/10/static/functions-info.html#FUNCTIONS-INFO-CATALOG-TABLE
		makeGeneratorOverload(
			tree.ArgTypes{},
			keywordsValueGeneratorType,
			makeKeywordsGenerator,
			"Produces a virtual table containing the keywords known to the SQL parser.",
		),
	),

	"unnest": makeBuiltin(genProps(arrayValueGeneratorLabels),
		// See https://www.postgresql.org/docs/current/static/functions-array.html
		makeGeneratorOverloadWithReturnType(
			tree.ArgTypes{{"input", types.AnyArray}},
			func(args []tree.TypedExpr) types.T {
				if len(args) == 0 || args[0].ResolvedType() == types.Unknown {
					return tree.UnknownReturnType
				}
				return types.UnwrapType(args[0].ResolvedType()).(types.TArray).Typ
			},
			makeArrayGenerator,
			"Returns the input array as a set of rows",
		),
	),

	"information_schema._pg_expandarray": makeBuiltin(genProps(expandArrayValueGeneratorLabels),
		makeGeneratorOverloadWithReturnType(
			tree.ArgTypes{{"input", types.AnyArray}},
			func(args []tree.TypedExpr) types.T {
				if len(args) == 0 || args[0].ResolvedType() == types.Unknown {
					return tree.UnknownReturnType
				}
				t := types.UnwrapType(args[0].ResolvedType()).(types.TArray).Typ
				return types.TTuple{
					Types:  []types.T{t, types.Int},
					Labels: expandArrayValueGeneratorLabels,
				}
			},
			makeExpandArrayGenerator,
			"Returns the input array as a set of rows with an index",
		),
	),

	"crdb_internal.unary_table": makeBuiltin(genProps(nil /* no labels */),
		makeGeneratorOverload(
			tree.ArgTypes{},
			unaryValueGeneratorType,
			makeUnaryGenerator,
			"Produces a virtual table containing a single row with no values.\n\n"+
				"This function is used only by CockroachDB's developers for testing purposes.",
		),
	),

	"generate_subscripts": makeBuiltin(genProps(subscriptsValueGeneratorLabels),
		// See https://www.postgresql.org/docs/current/static/functions-srf.html#FUNCTIONS-SRF-SUBSCRIPTS
		makeGeneratorOverload(
			tree.ArgTypes{{"array", types.AnyArray}},
			subscriptsValueGeneratorType,
			makeGenerateSubscriptsGenerator,
			"Returns a series comprising the given array's subscripts.",
		),
		makeGeneratorOverload(
			tree.ArgTypes{{"array", types.AnyArray}, {"dim", types.Int}},
			subscriptsValueGeneratorType,
			makeGenerateSubscriptsGenerator,
			"Returns a series comprising the given array's subscripts.",
		),
		makeGeneratorOverload(
			tree.ArgTypes{{"array", types.AnyArray}, {"dim", types.Int}, {"reverse", types.Bool}},
			subscriptsValueGeneratorType,
			makeGenerateSubscriptsGenerator,
			"Returns a series comprising the given array's subscripts.\n\n"+
				"When reverse is true, the series is returned in reverse order.",
		),
	),

	"json_array_elements":       makeBuiltin(genProps(jsonArrayGeneratorLabels), jsonArrayElementsImpl),
	"jsonb_array_elements":      makeBuiltin(genProps(jsonArrayGeneratorLabels), jsonArrayElementsImpl),
	"json_array_elements_text":  makeBuiltin(genProps(jsonArrayGeneratorLabels), jsonArrayElementsTextImpl),
	"jsonb_array_elements_text": makeBuiltin(genProps(jsonArrayGeneratorLabels), jsonArrayElementsTextImpl),
	"json_object_keys":          makeBuiltin(genProps(jsonObjectKeysGeneratorLabels), jsonObjectKeysImpl),
	"jsonb_object_keys":         makeBuiltin(genProps(jsonObjectKeysGeneratorLabels), jsonObjectKeysImpl),
	"json_each":                 makeBuiltin(genProps(jsonEachGeneratorLabels), jsonEachImpl),
	"jsonb_each":                makeBuiltin(genProps(jsonEachGeneratorLabels), jsonEachImpl),
	"json_each_text":            makeBuiltin(genProps(jsonEachGeneratorLabels), jsonEachTextImpl),
	"jsonb_each_text":           makeBuiltin(genProps(jsonEachGeneratorLabels), jsonEachTextImpl),
}

func makeGeneratorOverload(
	in tree.ArgTypes, ret types.T, g tree.GeneratorFactory, info string,
) tree.Overload {
	return makeGeneratorOverloadWithReturnType(in, tree.FixedReturnType(ret), g, info)
}

func newUnsuitableUseOfGeneratorError() error {
	return pgerror.NewAssertionErrorf("generator functions cannot be evaluated as scalars")
}

func makeGeneratorOverloadWithReturnType(
	in tree.ArgTypes, retType tree.ReturnTyper, g tree.GeneratorFactory, info string,
) tree.Overload {
	return tree.Overload{
		Types:      in,
		ReturnType: retType,
		Generator:  g,
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return nil, newUnsuitableUseOfGeneratorError()
		},
		Info: info,
	}
}

// keywordsValueGenerator supports the execution of pg_get_keywords().
type keywordsValueGenerator struct {
	curKeyword int
}

var keywordsValueGeneratorType = types.TTuple{
	Types:  []types.T{types.String, types.String, types.String},
	Labels: []string{"word", "catcode", "catdesc"},
}

func makeKeywordsGenerator(_ *tree.EvalContext, _ tree.Datums) (tree.ValueGenerator, error) {
	return &keywordsValueGenerator{}, nil
}

// ResolvedType implements the tree.ValueGenerator interface.
func (*keywordsValueGenerator) ResolvedType() types.T { return keywordsValueGeneratorType }

// Close implements the tree.ValueGenerator interface.
func (*keywordsValueGenerator) Close() {}

// Start implements the tree.ValueGenerator interface.
func (k *keywordsValueGenerator) Start() error {
	k.curKeyword = -1
	return nil
}
func (k *keywordsValueGenerator) Next() (bool, error) {
	k.curKeyword++
	return k.curKeyword < len(keywordNames), nil
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
	ctx                                 *tree.EvalContext
	origStart, value, start, stop, step interface{}
	nextOK                              bool
	genType                             types.T
	next                                func(*seriesValueGenerator) (bool, error)
	genValue                            func(*seriesValueGenerator) tree.Datums
}

var seriesValueGeneratorLabels = []string{"generate_series"}

var seriesValueGeneratorType = types.Int

var seriesTSValueGeneratorType = types.Timestamp

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
	s.start, s.nextOK = arith.AddWithOverflow(start, step)
	return true, nil
}

func seriesGenIntValue(s *seriesValueGenerator) tree.Datums {
	return tree.Datums{tree.NewDInt(tree.DInt(s.value.(int64)))}
}

// seriesTSNext performs calendar-aware math.
func seriesTSNext(s *seriesValueGenerator) (bool, error) {
	step := s.step.(duration.Duration)
	start := s.start.(time.Time)
	stop := s.stop.(time.Time)

	if !s.nextOK {
		return false, nil
	}

	stepForward := step.Compare(duration.Duration{}) > 0
	if !stepForward && (start.Before(stop)) {
		return false, nil
	}
	if stepForward && (stop.Before(start)) {
		return false, nil
	}

	s.value = start
	s.start = duration.Add(s.ctx, start, step)
	return true, nil
}

func seriesGenTSValue(s *seriesValueGenerator) tree.Datums {
	return tree.Datums{tree.MakeDTimestamp(s.value.(time.Time), time.Microsecond)}
}

func makeSeriesGenerator(ctx *tree.EvalContext, args tree.Datums) (tree.ValueGenerator, error) {
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
		ctx:       ctx,
		origStart: start,
		stop:      stop,
		step:      step,
		genType:   seriesValueGeneratorType,
		genValue:  seriesGenIntValue,
		next:      seriesIntNext,
	}, nil
}

func makeTSSeriesGenerator(ctx *tree.EvalContext, args tree.Datums) (tree.ValueGenerator, error) {
	start := args[0].(*tree.DTimestamp).Time
	stop := args[1].(*tree.DTimestamp).Time
	step := args[2].(*tree.DInterval).Duration

	if step.Compare(duration.Duration{}) == 0 {
		return nil, errStepCannotBeZero
	}

	return &seriesValueGenerator{
		ctx:       ctx,
		origStart: start,
		stop:      stop,
		step:      step,
		genType:   seriesTSValueGeneratorType,
		genValue:  seriesGenTSValue,
		next:      seriesTSNext,
	}, nil
}

// ResolvedType implements the tree.ValueGenerator interface.
func (s *seriesValueGenerator) ResolvedType() types.T {
	return s.genType
}

// Start implements the tree.ValueGenerator interface.
func (s *seriesValueGenerator) Start() error {
	s.nextOK = true
	s.start = s.origStart
	s.value = s.origStart
	return nil
}

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
func (s *arrayValueGenerator) ResolvedType() types.T {
	return s.array.ParamTyp
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
	g := &expandArrayValueGenerator{avg: arrayValueGenerator{array: arr}}
	g.buf[1] = tree.NewDInt(tree.DInt(-1))
	return g, nil
}

// expandArrayValueGenerator is a value generator that returns each element of
// an array and an index for it.
type expandArrayValueGenerator struct {
	avg arrayValueGenerator
	buf [2]tree.Datum
}

var expandArrayValueGeneratorLabels = []string{"x", "n"}

// ResolvedType implements the tree.ValueGenerator interface.
func (s *expandArrayValueGenerator) ResolvedType() types.T {
	return types.TTuple{
		Types:  []types.T{s.avg.array.ParamTyp, types.Int},
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
	return s.avg.nextIndex < s.avg.array.Len(), nil
}

// Values implements the tree.ValueGenerator interface.
func (s *expandArrayValueGenerator) Values() tree.Datums {
	// Expand array's index is 1 based.
	s.buf[0] = s.avg.array.Array[s.avg.nextIndex]
	s.buf[1] = tree.NewDInt(tree.DInt(s.avg.nextIndex + 1))
	return s.buf[:]
}

func makeGenerateSubscriptsGenerator(
	evalCtx *tree.EvalContext, args tree.Datums,
) (tree.ValueGenerator, error) {
	var arr *tree.DArray
	dim := 1
	if len(args) > 1 {
		dim = int(tree.MustBeDInt(args[1]))
	}
	// We sadly only support 1D arrays right now.
	if dim == 1 {
		arr = tree.MustBeDArray(args[0])
	} else {
		arr = &tree.DArray{}
	}
	var reverse bool
	if len(args) == 3 {
		reverse = bool(tree.MustBeDBool(args[2]))
	}
	g := &subscriptsValueGenerator{
		avg:     arrayValueGenerator{array: arr},
		reverse: reverse,
	}
	g.buf[0] = tree.NewDInt(tree.DInt(-1))
	return g, nil
}

// subscriptsValueGenerator is a value generator that returns a series
// comprising the given array's subscripts.
type subscriptsValueGenerator struct {
	avg     arrayValueGenerator
	buf     [1]tree.Datum
	reverse bool
}

var subscriptsValueGeneratorLabels = []string{"generate_subscripts"}

var subscriptsValueGeneratorType = types.Int

// ResolvedType implements the tree.ValueGenerator interface.
func (s *subscriptsValueGenerator) ResolvedType() types.T {
	return subscriptsValueGeneratorType
}

// Start implements the tree.ValueGenerator interface.
func (s *subscriptsValueGenerator) Start() error {
	if s.reverse {
		s.avg.nextIndex = s.avg.array.Len()
	} else {
		s.avg.nextIndex = -1
	}
	return nil
}

// Close implements the tree.ValueGenerator interface.
func (s *subscriptsValueGenerator) Close() {}

// Next implements the tree.ValueGenerator interface.
func (s *subscriptsValueGenerator) Next() (bool, error) {
	if s.reverse {
		s.avg.nextIndex--
		return s.avg.nextIndex >= 0, nil
	}
	s.avg.nextIndex++
	return s.avg.nextIndex < s.avg.array.Len(), nil
}

// Values implements the tree.ValueGenerator interface.
func (s *subscriptsValueGenerator) Values() tree.Datums {
	// Generate Subscript's indexes are 1 based.
	s.buf[0] = tree.NewDInt(tree.DInt(s.avg.nextIndex + 1))
	return s.buf[:]
}

// EmptyGenerator returns a new, empty generator. Used when a SRF
// evaluates to NULL.
func EmptyGenerator() tree.ValueGenerator {
	return &arrayValueGenerator{array: tree.NewDArray(types.Any)}
}

// unaryValueGenerator supports the execution of crdb_internal.unary_table().
type unaryValueGenerator struct {
	done bool
}

var unaryValueGeneratorType = types.TTuple{}

func makeUnaryGenerator(_ *tree.EvalContext, args tree.Datums) (tree.ValueGenerator, error) {
	return &unaryValueGenerator{}, nil
}

// ResolvedType implements the tree.ValueGenerator interface.
func (*unaryValueGenerator) ResolvedType() types.T { return unaryValueGeneratorType }

// Start implements the tree.ValueGenerator interface.
func (s *unaryValueGenerator) Start() error {
	s.done = false
	return nil
}

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

var noDatums tree.Datums

// Values implements the tree.ValueGenerator interface.
func (s *unaryValueGenerator) Values() tree.Datums { return noDatums }

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

var jsonArrayElementsImpl = makeGeneratorOverload(
	tree.ArgTypes{{"input", types.JSON}},
	jsonArrayGeneratorType,
	makeJSONArrayAsJSONGenerator,
	"Expands a JSON array to a set of JSON values.",
)

var jsonArrayElementsTextImpl = makeGeneratorOverload(
	tree.ArgTypes{{"input", types.JSON}},
	jsonArrayTextGeneratorType,
	makeJSONArrayAsTextGenerator,
	"Expands a JSON array to a set of text values.",
)

var jsonArrayGeneratorLabels = []string{"value"}

var jsonArrayGeneratorType = types.JSON

var jsonArrayTextGeneratorType = types.String

type jsonArrayGenerator struct {
	json      tree.DJSON
	nextIndex int
	asText    bool
	buf       [1]tree.Datum
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
func (g *jsonArrayGenerator) ResolvedType() types.T {
	if g.asText {
		return jsonArrayTextGeneratorType
	}
	return jsonArrayGeneratorType
}

// Start implements the tree.ValueGenerator interface.
func (g *jsonArrayGenerator) Start() error {
	g.nextIndex = -1
	g.json.JSON = g.json.JSON.MaybeDecode()
	g.buf[0] = nil
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
		if g.buf[0], err = jsonAsText(next); err != nil {
			return false, err
		}
	} else {
		g.buf[0] = tree.NewDJSON(next)
	}
	return true, nil
}

// Values implements the tree.ValueGenerator interface.
func (g *jsonArrayGenerator) Values() tree.Datums {
	return g.buf[:]
}

// jsonObjectKeysImpl is a key generator of a JSON object.
var jsonObjectKeysImpl = makeGeneratorOverload(
	tree.ArgTypes{{"input", types.JSON}},
	jsonObjectKeysGeneratorType,
	makeJSONObjectKeysGenerator,
	"Returns sorted set of keys in the outermost JSON object.",
)

var jsonObjectKeysGeneratorLabels = []string{"json_object_keys"}

var jsonObjectKeysGeneratorType = types.String

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
func (g *jsonObjectKeysGenerator) ResolvedType() types.T {
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

var jsonEachImpl = makeGeneratorOverload(
	tree.ArgTypes{{"input", types.JSON}},
	jsonEachGeneratorType,
	makeJSONEachImplGenerator,
	"Expands the outermost JSON or JSONB object into a set of key/value pairs.",
)

var jsonEachTextImpl = makeGeneratorOverload(
	tree.ArgTypes{{"input", types.JSON}},
	jsonEachTextGeneratorType,
	makeJSONEachTextImplGenerator,
	"Expands the outermost JSON or JSONB object into a set of key/value pairs. "+
		"The returned values will be of type text.",
)

var jsonEachGeneratorLabels = []string{"key", "value"}

var jsonEachGeneratorType = types.TTuple{
	Types:  []types.T{types.String, types.JSON},
	Labels: jsonEachGeneratorLabels,
}

var jsonEachTextGeneratorType = types.TTuple{
	Types:  []types.T{types.String, types.String},
	Labels: jsonEachGeneratorLabels,
}

type jsonEachGenerator struct {
	target tree.DJSON
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
	return &jsonEachGenerator{
		target: target,
		key:    nil,
		value:  nil,
		asText: asText,
	}, nil
}

// ResolvedType implements the tree.ValueGenerator interface.
func (g *jsonEachGenerator) ResolvedType() types.T {
	if g.asText {
		return jsonEachTextGeneratorType
	}
	return jsonEachGeneratorType
}

// Start implements the tree.ValueGenerator interface.
func (g *jsonEachGenerator) Start() error {
	iter, err := g.target.ObjectIter()
	if err != nil {
		return err
	}
	if iter == nil {
		switch g.target.Type() {
		case json.ArrayJSONType:
			return errJSONDeconstructArrayAsObject
		default:
			return errJSONDeconstructScalarAsObject
		}
	}
	g.iter = iter
	return nil
}

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
