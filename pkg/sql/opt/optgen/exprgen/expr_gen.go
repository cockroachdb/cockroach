// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/sql/opt/ordering"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/props/physical"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/xform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
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
//  - A Root custom function is used to set the physical properties of the root.
//    Setting the physical properties (in particular the presentation) is always
//    necessary for the plan to be run via PREPARE .. AS OPT PLAN '..'.
//
// Some examples of valid inputs:
//    (Tuple [ (True) (False) ] "tuple{bool, bool}" )
//
//    (Root
//      (Scan [ (Table "abc") (Index "abc@ab") (Cols "a,b") ])
//      (Presentation "a,b")
//      (OrderingChoice "+a,+b")
//    )
//
//    (Select
//      (Scan [ (Table "abc") (Cols "a,b,c") ])
//      [ (Eq (Var "a") (Const 1)) ]
//    )
//
// For more examples, see the various testdata/ files.
//
func Build(catalog cat.Catalog, factory *norm.Factory, input string) (_ opt.Expr, err error) {
	return buildAndOptimize(catalog, nil /* optimizer */, factory, input)
}

// Optimize generates an expression from an optgen string and runs normalization
// and exploration rules. The string must represent a relational expression.
func Optimize(catalog cat.Catalog, o *xform.Optimizer, input string) (_ opt.Expr, err error) {
	return buildAndOptimize(catalog, o, o.Factory(), input)
}

func buildAndOptimize(
	catalog cat.Catalog, optimizer *xform.Optimizer, factory *norm.Factory, input string,
) (_ opt.Expr, err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(exprGenErr); ok {
				err = e.error
			} else {
				err = r.(error)
			}
		}
	}()

	eg := exprGen{
		customFuncs: customFuncs{
			f:   factory,
			mem: factory.Memo(),
			cat: catalog,
		},
		coster: xform.MakeDefaultCoster(factory.Memo()),
	}

	// To create a valid optgen "file", we create a rule with a bogus match.
	contents := "[FakeRule] (True) => " + input
	parsed := eg.parse(contents)
	result := eg.eval(parsed.Rules[0].Replace)

	var expr opt.Expr
	required := physical.MinRequired
	if root, ok := result.(*rootSentinel); ok {
		expr = root.expr
		required = root.required
	} else {
		expr = result.(opt.Expr)
	}

	// A non-nil optimizer indicates that exploration should be performed.
	if optimizer != nil {
		if rel, ok := expr.(memo.RelExpr); ok {
			optimizer.Memo().SetRoot(rel, &physical.Required{})
			return optimizer.Optimize()
		}
		return nil, errors.AssertionFailedf("expropt requires a relational expression")
	}

	eg.populateBestProps(expr, required)
	if rel, ok := expr.(memo.RelExpr); ok {
		eg.mem.SetRoot(rel, required)
	}
	return expr, nil
}

type exprGen struct {
	customFuncs

	coster xform.Coster
}

type exprGenErr struct {
	error
}

func errorf(format string, a ...interface{}) error {
	return exprGenErr{fmt.Errorf(format, a...)}
}

func wrapf(err error, format string, a ...interface{}) error {
	return exprGenErr{errors.WrapWithDepthf(1, err, format, a...)}
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
		panic(errorf("unsupported expression %s: %v", expr.Op(), expr))
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
		converted := eg.castToDesiredType(arg, desiredType)
		if converted == nil {
			panic(errorf("%s: using %T as type %s", name, arg, desiredType))
		}
		argVals[i] = reflect.ValueOf(converted)
	}
	return method.Call(argVals)[0].Interface()
}

// castToDesiredType tries to convert the given argument to a value of the given
// type. Returns nil if conversion was not possible.
func (eg *exprGen) castToDesiredType(arg interface{}, desiredType reflect.Type) interface{} {
	actualType := reflect.TypeOf(arg)
	if actualType.AssignableTo(desiredType) {
		return arg
	}

	if slice, ok := arg.([]interface{}); ok {
		// Special case for converting slice of ColumnIDs to a ColSet.
		if desiredType == reflect.TypeOf(opt.ColSet{}) {
			var set opt.ColSet
			for i := range slice {
				col, ok := slice[i].(opt.ColumnID)
				if !ok {
					return nil
				}
				set.Add(col)
			}
			return set
		}

		if desiredType.Kind() != reflect.Slice {
			return nil
		}

		// See if we can convert all elements to the desired slice element type.
		converted := convertSlice(slice, desiredType, func(v interface{}) interface{} {
			if val := reflect.ValueOf(v); val.Type().AssignableTo(desiredType.Elem()) {
				return val.Convert(desiredType.Elem()).Interface()
			}
			return nil
		})
		if converted != nil {
			return converted
		}

		// Special case for converting slice of values implementing ScalarExpr to a
		// FiltersExpr.
		if desiredType == reflect.TypeOf(memo.FiltersExpr{}) {
			converted := convertSlice(slice, desiredType, func(v interface{}) interface{} {
				expr, ok := v.(opt.ScalarExpr)
				if !ok {
					return nil
				}
				return eg.f.ConstructFiltersItem(expr)
			})
			if converted != nil {
				return converted
			}
		}
	}

	if i, ok := arg.(*tree.DInt); ok {
		if desiredType == reflect.TypeOf(memo.ScanLimit(0)) {
			return memo.MakeScanLimit(int64(*i), false)
		}
	}

	if str, ok := arg.(string); ok {
		// String to type.
		if desiredType == reflect.TypeOf((*types.T)(nil)) {
			typ, err := ParseType(str)
			if err != nil {
				panic(exprGenErr{err})
			}
			return typ
		}

		// String to Ordering.
		if desiredType == reflect.TypeOf(opt.Ordering{}) {
			return eg.Ordering(str)
		}

		// String to OrderingChoice.
		if desiredType == reflect.TypeOf(props.OrderingChoice{}) {
			return eg.OrderingChoice(str)
		}

		// String to ColList.
		if desiredType == reflect.TypeOf(opt.ColList{}) {
			return eg.ColList(str)
		}

		// String to ColSet.
		if desiredType == reflect.TypeOf(opt.ColSet{}) {
			return eg.ColSet(str)
		}

		// String to ExplainOptions.
		if desiredType == reflect.TypeOf(tree.ExplainOptions{}) {
			return eg.ExplainOptions(str)
		}

		// String to bool.
		if desiredType == reflect.TypeOf(true) {
			switch str {
			case "true":
				return true
			case "false":
				return false
			default:
				panic(errorf("invalid boolean value \"%s\" (expected \"true\" or \"false\")", str))
			}
		}

		// String to Datum.
		if desiredType == reflect.TypeOf((*tree.Datum)(nil)).Elem() {
			return tree.NewDString(str)
		}
	}
	return nil
}

// convertSlice tries to create a slice of the given type; each element is the
// result of calling mapFn on the input slice element.
//
// If mapFn returns nil for any element, convertSlice also returns nil.
func convertSlice(
	slice []interface{}, toType reflect.Type, mapFn func(v interface{}) interface{},
) interface{} {
	res := reflect.MakeSlice(toType, len(slice), len(slice))

	for i, v := range slice {
		val := mapFn(v)
		if val == nil {
			return nil
		}
		res.Index(i).Set(reflect.ValueOf(val))
	}
	return res.Interface()
}

// populateBestProps sets the physical properties and costs of the expressions
// in the tree. Returns the cost of the expression tree.
func (eg *exprGen) populateBestProps(expr opt.Expr, required *physical.Required) memo.Cost {
	rel, _ := expr.(memo.RelExpr)
	if rel != nil {
		if !xform.CanProvidePhysicalProps(eg.f.EvalContext(), rel, required) {
			panic(errorf("operator %s cannot provide required props %s", rel.Op(), required))
		}
	}

	var cost memo.Cost
	for i, n := 0, expr.ChildCount(); i < n; i++ {
		var childProps *physical.Required
		if rel != nil {
			childProps = xform.BuildChildPhysicalProps(eg.mem, rel, i, required)
		} else {
			childProps = xform.BuildChildPhysicalPropsScalar(eg.mem, expr, i)
		}
		cost += eg.populateBestProps(expr.Child(i), childProps)
	}

	if rel != nil {
		provided := &physical.Provided{}
		// BuildProvided relies on ProvidedPhysical() being set in the children, so
		// it must run after the recursive calls on the children.
		provided.Ordering = ordering.BuildProvided(rel, &required.Ordering)

		cost += eg.coster.ComputeCost(rel, required)
		eg.mem.SetBestProps(rel, required, provided, cost)
	}
	return cost
}
