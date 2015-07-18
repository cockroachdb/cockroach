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
	"fmt"
	"strings"
)

type builtin struct {
	nArgs int
	fn    func(args dtuple) (Datum, error)
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
	"length": stringBuiltin(func(s string) (Datum, error) {
		return dint(len(s)), nil
	}),

	"lower": stringBuiltin(func(s string) (Datum, error) {
		return dstring(strings.ToLower(s)), nil
	}),

	"upper": stringBuiltin(func(s string) (Datum, error) {
		return dstring(strings.ToUpper(s)), nil
	}),
}

func argTypeError(arg Datum, expected string) error {
	return fmt.Errorf("argument type mismatch: %s expected, but found %s",
		expected, arg.Type())
}

func stringBuiltin(f func(string) (Datum, error)) builtin {
	return builtin{
		nArgs: 1,
		fn: func(args dtuple) (Datum, error) {
			s, ok := args[0].(dstring)
			if !ok {
				return null, argTypeError(args[0], "string")
			}
			return f(string(s))
		},
	}
}
