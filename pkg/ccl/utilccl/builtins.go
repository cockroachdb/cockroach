// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package utilccl

import (
	"fmt"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinsregistry"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

// RegisterableBuiltin must be a function whose argument types
// are in BuiltinArgTypes and whose return type is in BuiltinReturnTypes.
type RegisterableBuiltin interface{}

// TODO: anyArg gets used at runtime, so for performance this should be
// not be a reflect type, but that's tricky without using generics
// or a potentially cumbersome union struct.
type anyArg = reflect.Value

func makeAnyArg(x interface{}) anyArg {
	return reflect.ValueOf(x)
}

func union(v anyArg) interface{} {
	return v.Interface()
}

func makePGError(v anyArg) error {
	if v.IsNil() {
		return nil
	}
	err := v.Interface().(error)
	return pgerror.Wrap(err, pgerror.GetPGCode(err), "internal implementation error")
}

// NativeToDatum defines a conversion from a golang primitive or CCL
// type into a tree.Datum.
type NativeToDatum struct {
	Type       *types.T
	Conversion func(anyArg) tree.Datum
}

// DatumToNative defines a conversion from a tree.Datum to a golang
// primitive or CCL type.
type DatumToNative struct {
	Type       *types.T
	Conversion func(tree.Datum) anyArg
}

// BuiltinReturnTypes maps types returned by a go function to the equivalent
// tree.Datum for a SQL function to return.
var BuiltinReturnTypes = map[reflect.Type]NativeToDatum{
	reflect.TypeOf("string"): {
		Type:       types.String,
		Conversion: func(x anyArg) tree.Datum { return tree.NewDString(union(x).(string)) },
	},
	reflect.TypeOf([]byte{}): {
		Type:       types.Bytes,
		Conversion: func(x anyArg) tree.Datum { return tree.NewDBytes(tree.DBytes(union(x).([]byte))) },
	},
}

// BuiltinArgTypes maps argument types taken by a go function to an equivalent
// tree.Datum for a SQL function to take.
var BuiltinArgTypes = map[reflect.Type]DatumToNative{
	reflect.TypeOf("string"): {
		Type:       types.String,
		Conversion: func(d tree.Datum) anyArg { return makeAnyArg(string(tree.MustBeDString(d))) },
	},
}

// RegisterArgType allows a package-specific type to be used as input to a SQL function.
func RegisterArgType(
	zeroValue interface{}, datumType *types.T, converter func(tree.Datum) interface{},
) {
	BuiltinArgTypes[reflect.TypeOf(zeroValue)] = DatumToNative{
		Type:       datumType,
		Conversion: func(d tree.Datum) anyArg { return makeAnyArg(converter(d)) },
	}
}

// RegisterCCLBuiltin converts an arbitrary go function to an equivalent SQL function
// using reflection. Names should be namespaced (usually starting with `crdb_internal.ccl_`).
// impl must be a function that returns a single type with a key in BuiltinReturnTypes,
// plus optionally an error. This is a convenience method meant for generating SQL functions
// that are tightly coupled to other behavior. It's generally preferable to create SQL builtins
// via calling builtinsregistry.Register directly.
func RegisterCCLBuiltin(name string, description string, impl RegisterableBuiltin) {
	props := tree.FunctionProperties{
		Class:    tree.NormalClass,
		Category: `CCL-only internal function`,
	}
	overload := tree.Overload{
		Volatility: volatility.Volatile,
		Info:       description + " This function was generated automatically from internal database code and is not intended for production use.",
	}
	sig := reflect.TypeOf(impl)
	if sig.Kind() != reflect.Func {
		panic(fmt.Sprintf("%s implementation must be function, was %v", name, impl))
	}
	overload.ReturnType = makeReturnTyper(sig)
	overload.Types = makeArgList(sig)

	inputWrapper := makeInputWrapper(sig)
	logic := reflect.ValueOf(impl).Call
	if sig.NumOut() == 1 {
		outputWrapper := makeOutputWrapper(sig)
		overload.Fn = func(_ *eval.Context, args tree.Datums) (tree.Datum, error) {
			return outputWrapper(logic(inputWrapper(args))[0])
		}
	} else {
		outputWrapper := makeOutputWrapperWithError(sig)
		overload.Fn = func(_ *eval.Context, args tree.Datums) (tree.Datum, error) {
			return outputWrapper(logic(inputWrapper(args)))
		}
	}

	builtinsregistry.Register(name, &props, []tree.Overload{overload})
}

func makeInputWrapper(sig reflect.Type) func(tree.Datums) []anyArg {
	conversions := make([]func(d tree.Datum) anyArg, sig.NumIn())
	for i := range conversions {
		if i == sig.NumIn()-1 && sig.IsVariadic() {
			conversions[i] = BuiltinArgTypes[sig.In(i).Elem()].Conversion
		} else {
			conversions[i] = BuiltinArgTypes[sig.In(i)].Conversion
		}
	}
	conversionForVariadicArgs := conversions[sig.NumIn()-1]
	return func(datums tree.Datums) []anyArg {
		convertedArgs := make([]anyArg, len(datums))
		for i, d := range datums {
			if i >= sig.NumIn()-1 {
				convertedArgs[i] = conversionForVariadicArgs(d)
			} else {
				convertedArgs[i] = conversions[i](d)
			}
		}
		return convertedArgs
	}
}

func makeOutputWrapper(sig reflect.Type) func(anyArg) (tree.Datum, error) {
	conversion := BuiltinReturnTypes[sig.Out(0)].Conversion
	return func(v anyArg) (tree.Datum, error) {
		return conversion(v), nil
	}
}

func makeOutputWrapperWithError(sig reflect.Type) func([]anyArg) (tree.Datum, error) {
	conversion := BuiltinReturnTypes[sig.Out(0)].Conversion
	return func(args []anyArg) (tree.Datum, error) {
		return conversion(args[0]), makePGError(args[1])
	}
}

func makeReturnTyper(sig reflect.Type) tree.ReturnTyper {
	if sig.NumOut() > 2 || sig.NumOut() == 0 {
		panic("builtin function implementation must return exactly one value, optionally plus an error")
	}
	ntd, ok := BuiltinReturnTypes[sig.Out(0)]
	if !ok {
		panic(fmt.Sprintf("return type %v not in BuiltinReturnTypes", sig.Out(0)))
	}
	return tree.FixedReturnType(ntd.Type)
}

func makeArgList(sig reflect.Type) (tl tree.TypeList) {
	var addFixedType func(idx int, t *types.T)
	var numFixed int
	if sig.IsVariadic() {
		numFixed = sig.NumIn() - 1
		fixed := make([]*types.T, numFixed)
		tl = tree.VariadicType{
			FixedTypes: fixed,
			VarType:    getArgType(sig.In(sig.NumIn() - 1).Elem()),
		}
		addFixedType = func(idx int, t *types.T) {
			fixed[idx] = t
		}
	} else {
		numFixed = sig.NumIn()
		args := make(tree.ArgTypes, numFixed)
		tl = args
		addFixedType = func(idx int, t *types.T) {
			args[idx].Typ = t
		}
	}
	for idx := 0; idx < numFixed; idx++ {
		addFixedType(idx, getArgType(sig.In(idx)))
	}
	return tl
}

func getArgType(t reflect.Type) *types.T {
	dtn, ok := BuiltinArgTypes[t]
	if !ok {
		panic(fmt.Sprintf("arg type %v not in BuiltInArgTypes", t))
	}
	return dtn.Type
}
