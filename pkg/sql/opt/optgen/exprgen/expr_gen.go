// Copyright 2019 The Cockroach Authors.
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

package exprgen

import (
	"fmt"
	"io"
	"reflect"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/norm"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/optgen/lang"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/testutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// Build generates an expression from an optgen string (the kind of expression
// that would show up in the replace side of a rule).
//
// For example, if the input is "(Eq (Const 1) (Const 2))", the output is the
// corresponding expression tree:
//   eq [type=bool]
//    ├── const: 1 [type=int]
//    └── const: 2 [type=int]
//
// There are some peculiarities compared to the usual opt-gen replace syntax:
//
//  - Filters are specified as simply [ <condition> ... ]; no FiltersItem is
//    necessary.
//
//  - Various implicit conversions are allowed for convenience, e.g. list of
//    columns to ColList/ColSet.
//
//  - Operation privates (e.g. ScanPrivate) are specified as lists of fields
//    of the form [ (FiledName <value>) ]. For example:
//      [ (Table "abc") (Index "abc@ab") (Cols "a,b") ]
//    Implicit conversions are allowed here for column lists, orderings, etc.
//
// For more examples, see the various testdata/ files.
//
func Build(catalog cat.Catalog, factory *norm.Factory, input string) (_ opt.Expr, err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(exprGenErr); ok {
				err = e.error
			} else {
				panic(r)
			}
		}
	}()

	eg := exprGen{
		customFuncs: customFuncs{
			f:   factory,
			mem: factory.Memo(),
			cat: catalog,
		},
	}

	// To create a valid optgen "file", we create a rule with a bogus match.
	contents := "[FakeRule] (True) => " + input
	parsed := eg.parse(contents)
	res := eg.eval(parsed.Rules[0].Replace)

	// TODO(radu): fill in best props (cost, physical props etc).
	return res.(opt.Expr), nil
}

type exprGen struct {
	customFuncs
}

type exprGenErr struct {
	error
}

func errorf(format string, a ...interface{}) error {
	return exprGenErr{fmt.Errorf(format, a...)}
}

// Parses the input (with replace-side rule syntax) into an optgen expression
// tree.
func (eg *exprGen) parse(contents string) *lang.RootExpr {
	p := lang.NewParser("fake.opt")
	p.SetFileResolver(func(name string) (io.Reader, error) {
		if name == "fake.opt" {
			return strings.NewReader(contents), nil
		}
		return nil, fmt.Errorf("unknown file '%s'", name)
	})
	parsed := p.Parse()
	if parsed == nil {
		errStr := "Errors during parsing:\n"
		for _, err := range p.Errors() {
			errStr = fmt.Sprintf("%s%s\n", errStr, err.Error())
		}
		panic(errorf("%s", errStr))
	}
	return parsed
}

// Evaluates the corresponding expression.
func (eg *exprGen) eval(expr lang.Expr) interface{} {
	switch expr := expr.(type) {
	case *lang.FuncExpr:
		if expr.HasDynamicName() {
			panic(errorf("dynamic functions not supported: %s", expr))
		}

		name := expr.SingleName()
		// Search for a factory function.
		method := reflect.ValueOf(eg.f).MethodByName("Construct" + name)
		if !method.IsValid() {
			// Search for a custom function.
			method = reflect.ValueOf(&eg.customFuncs).MethodByName(name)
			if !method.IsValid() {
				panic(errorf("unknown operator or function %s", name))
			}
		}
		return eg.call(name, method, expr.Args)

	case *lang.ListExpr:
		// Return a list expression as []interface{}.
		list := make([]interface{}, len(expr.Items))
		for i, e := range expr.Items {
			list[i] = eg.eval(e)
		}
		return list

	case *lang.NumberExpr:
		return tree.NewDInt(tree.DInt(*expr))

	case *lang.StringExpr:
		return string(*expr)

	default:
		panic(errorf("unsupported expression %s", expr.Op()))
	}
}

// Calls a function (custom function or factory method) with the given
// arguments. Various implicit conversions are allowed.
func (eg *exprGen) call(name string, method reflect.Value, args lang.SliceExpr) interface{} {
	fnTyp := method.Type()
	if fnTyp.NumIn() != len(args) {
		panic(errorf("%s expects %d arguments", name, fnTyp.NumIn()))
	}
	argVals := make([]reflect.Value, len(args))
	for i := range args {
		desiredType := fnTyp.In(i)

		// Special case for privates.
		if desiredType.Kind() == reflect.Ptr && desiredType.Elem().Kind() == reflect.Struct &&
			strings.HasSuffix(desiredType.Elem().Name(), "Private") {
			argVals[i] = reflect.ValueOf(eg.evalPrivate(desiredType.Elem(), args[i]))
			continue
		}

		arg := eg.eval(args[i])
		argVals[i] = eg.castToDesiredType(arg, desiredType)
		if !argVals[i].IsValid() {
			panic(errorf("%s: using %T as type %s", name, arg, desiredType))
		}
	}
	return method.Call(argVals)[0].Interface()
}

// castToDesiredType tries to convert the given argument to a value of the given
// type.
func (eg *exprGen) castToDesiredType(arg interface{}, desiredType reflect.Type) reflect.Value {
	actualType := reflect.TypeOf(arg)
	if actualType.AssignableTo(desiredType) {
		return reflect.ValueOf(arg)
	}

	if slice, ok := arg.([]interface{}); ok {
		// Special case for converting slice of ColumnIDs to a ColSet.
		if desiredType == reflect.TypeOf(opt.ColSet{}) {
			var set opt.ColSet
			for i := range slice {
				col, ok := slice[i].(opt.ColumnID)
				if !ok {
					return reflect.Value{}
				}
				set.Add(int(col))
			}
			return reflect.ValueOf(set)
		}

		if desiredType.Kind() != reflect.Slice {
			return reflect.Value{}
		}

		// See if we can convert all elements to the desired slice element type.
		converted := convertSlice(slice, desiredType, func(v interface{}) reflect.Value {
			if val := reflect.ValueOf(v); val.Type().AssignableTo(desiredType.Elem()) {
				return val.Convert(desiredType.Elem())
			}
			return reflect.Value{}
		})
		if converted.IsValid() {
			return converted
		}

		// Special case for converting slice of values implementing ScalarExpr to a
		// FiltersExpr.
		if desiredType == reflect.TypeOf(memo.FiltersExpr{}) {
			converted := convertSlice(slice, desiredType, func(v interface{}) reflect.Value {
				expr, ok := v.(opt.ScalarExpr)
				if !ok {
					return reflect.Value{}
				}
				return reflect.ValueOf(memo.FiltersItem{Condition: expr})
			})
			if converted.IsValid() {
				return converted
			}
		}
	}

	if str, ok := arg.(string); ok {
		// String to type.
		if desiredType == reflect.TypeOf((*types.T)(nil)).Elem() {
			typ, err := testutils.ParseType(str)
			if err != nil {
				panic(exprGenErr{err})
			}
			return reflect.ValueOf(typ)
		}

		// String to OrderingChoice.
		if desiredType == reflect.TypeOf(physical.OrderingChoice{}) {
			return reflect.ValueOf(physical.ParseOrderingChoice(eg.substituteCols(str)))
		}
	}
	return reflect.Value{}
}

// convertSlice tries to create a slice of the given type; each element is the
// result of calling mapFn on the input slice element.
//
// If mapFn returns an invalid Value, convertSlice also returns an invalid
// Value.
func convertSlice(
	slice []interface{}, toType reflect.Type, mapFn func(v interface{}) reflect.Value,
) reflect.Value {
	res := reflect.MakeSlice(toType, len(slice), len(slice))

	for i, v := range slice {
		val := mapFn(v)
		if !val.IsValid() {
			return reflect.Value{}
		}
		res.Index(i).Set(val)
	}
	return res
}
