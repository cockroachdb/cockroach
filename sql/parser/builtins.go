// Copyright 2015 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package parser

import (
	"bytes"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
)

type typeList []reflect.Type

type builtin struct {
	types typeList
	fn    func(DTuple) (Datum, error)
}

func (b builtin) match(types typeList) bool {
	if b.types == nil {
		return true
	}
	if len(types) != len(b.types) {
		return false
	}
	for i := range types {
		if types[i] != b.types[i] {
			return false
		}
	}
	return true
}

// The map from function name to function data. Keep the list of functions
// sorted please.
//
// TODO(pmattis): Need to flesh out the supported functions. For example, these
// are the math functions postgres supports:
//
//   - abs
//   - acos
//   - asin
//   - atan
//   - atan2
//   - ceil/ceiling
//   - cos
//   - degrees
//   - div
//   - exp
//   - floor
//   - ln
//   - log
//   - mod
//   - pi
//   - pow/power
//   - radians
//   - round
//   - sign
//   - sin
//   - sqrt
//   - tan
//   - trunc
//
// Need to figure out if there is a standard set of functions or if we can
// customize this to our liking. Would be good to support type conversion
// functions.
var builtins = map[string][]builtin{
	// TODO(XisiHuang): support encoding, i.e., length(str, encoding).
	"length": {stringBuiltin(func(s string) (Datum, error) {
		return DInt(len(s)), nil
	})},

	"lower": {stringBuiltin(func(s string) (Datum, error) {
		return DString(strings.ToLower(s)), nil
	})},

	"upper": {stringBuiltin(func(s string) (Datum, error) {
		return DString(strings.ToUpper(s)), nil
	})},

	"substr":    substringImpls,
	"substring": substringImpls,

	// concat concatenates the text representations of all the arguments.
	// NULL arguments are ignored.
	"concat": {
		builtin{
			fn: func(args DTuple) (Datum, error) {
				var buffer bytes.Buffer
				for _, d := range args {
					ds, err := datumToRawString(d)
					if err != nil {
						return DNull, err
					}
					buffer.WriteString(ds)
				}
				return DString(buffer.String()), nil
			},
		},
	},

	"random": {
		builtin{
			types: typeList{},
			fn: func(args DTuple) (Datum, error) {
				return DFloat(rand.Float64()), nil
			},
		},
	},
}

var substringImpls = []builtin{
	{
		types: typeList{stringType, intType},
		fn: func(args DTuple) (Datum, error) {
			str := args[0].(DString)
			// SQL strings are 1-indexed.
			start := int(args[1].(DInt)) - 1

			if start < 0 {
				start = 0
			} else if start > len(str) {
				start = len(str)
			}

			return str[start:], nil
		},
	},
	{
		types: typeList{stringType, intType, intType},
		fn: func(args DTuple) (Datum, error) {
			str := args[0].(DString)
			// SQL strings are 1-indexed.
			start := int(args[1].(DInt)) - 1
			length := int(args[2].(DInt))

			if length < 0 {
				return DNull, fmt.Errorf("negative substring length %d not allowed", length)
			}

			end := start + length
			if end < 0 {
				end = 0
			} else if end > len(str) {
				end = len(str)
			}

			if start < 0 {
				start = 0
			} else if start > len(str) {
				start = len(str)
			}

			return str[start:end], nil
		},
	},
}

func stringBuiltin(f func(string) (Datum, error)) builtin {
	return builtin{
		types: typeList{stringType},
		fn: func(args DTuple) (Datum, error) {
			return f(string(args[0].(DString)))
		},
	}
}

func datumToRawString(datum Datum) (string, error) {
	switch d := datum.(type) {
	case DString:
		return string(d), nil

	case DInt:
		return strconv.FormatInt(int64(d), 10), nil

	case DFloat:
		return strconv.FormatFloat(float64(d), 'f', -1, 64), nil

	case DBool:
		if bool(d) {
			return "t", nil
		}
		return "f", nil

	case dNull:
		return "", nil

	default:
		return "", fmt.Errorf("argument type unsupported: %s", datum.Type())
	}
}
