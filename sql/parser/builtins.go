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
	"strconv"
	"strings"
)

type builtin struct {
	nArgs int
	fn    func(args DTuple) (Datum, error)
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
var builtins = map[string]builtin{
	// TODO(XisiHuang): support encoding, i.e., length(str, encoding).
	"length": stringBuiltin(func(s string) (Datum, error) {
		return DInt(len(s)), nil
	}),

	"lower": stringBuiltin(func(s string) (Datum, error) {
		return DString(strings.ToLower(s)), nil
	}),

	"upper": stringBuiltin(func(s string) (Datum, error) {
		return DString(strings.ToUpper(s)), nil
	}),

	// TODO(XisiHuang): support the substring(str FROM x FOR y) syntax.
	"substring": {
		nArgs: -1,
		fn: func(args DTuple) (Datum, error) {
			argsNum := len(args)
			if argsNum != 2 && argsNum != 3 {
				return DNull, fmt.Errorf("incorrect number of arguments: %d vs %s",
					argsNum, "2 or 3")
			}

			dstr, ok := args[0].(DString)
			if !ok {
				return DNull, argTypeError(args[0], dstr.Type())
			}
			str := string(dstr)

			start, ok := args[1].(DInt)
			if !ok {
				return DNull, argTypeError(args[1], start.Type())
			}
			s := int(start)

			var e int
			if argsNum == 2 {
				e = len(str) + 1
			} else {
				count, ok := args[2].(DInt)
				if !ok {
					return DNull, argTypeError(args[2], count.Type())
				}
				c := int(count)
				if c < 0 {
					return DNull, fmt.Errorf("negative substring length not allowed: %d", c)
				}
				e = s + c
			}

			if e <= 1 || s > len(str) {
				return DString(""), nil
			}

			if s < 1 {
				s = 1
			}
			if e > len(str)+1 {
				e = len(str) + 1
			}
			return DString(str[s-1 : e-1]), nil
		},
	},

	// concat concatenate the text representations of all the arguments.
	// NULL arguments are ignored.
	"concat": {
		nArgs: -1,
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
}

func argTypeError(arg Datum, expected string) error {
	return fmt.Errorf("argument type mismatch: %s expected, but found %s",
		expected, arg.Type())
}

func stringBuiltin(f func(string) (Datum, error)) builtin {
	return builtin{
		nArgs: 1,
		fn: func(args DTuple) (Datum, error) {
			s, ok := args[0].(DString)
			if !ok {
				return DNull, argTypeError(args[0], "string")
			}
			return f(string(s))
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
