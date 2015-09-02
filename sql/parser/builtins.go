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
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
)

type typeList []reflect.Type

type builtin struct {
	types      typeList
	fn         func(DTuple) (Datum, error)
	returnType Datum
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
	"length": {stringBuiltin1(func(s string) (Datum, error) {
		return DInt(len(s)), nil
	}, DummyInt)},

	"lower": {stringBuiltin1(func(s string) (Datum, error) {
		return DString(strings.ToLower(s)), nil
	}, DummyString)},

	"upper": {stringBuiltin1(func(s string) (Datum, error) {
		return DString(strings.ToUpper(s)), nil
	}, DummyString)},

	"substr":    substringImpls,
	"substring": substringImpls,

	// concat concatenates the text representations of all the arguments.
	// NULL arguments are ignored.
	"concat": {
		builtin{
			returnType: DummyString,
			fn: func(args DTuple) (Datum, error) {
				var buffer bytes.Buffer
				for _, d := range args {
					if d == DNull {
						continue
					}
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

	"concat_ws": {
		builtin{
			returnType: DummyString,
			fn: func(args DTuple) (Datum, error) {
				dstr, ok := args[0].(DString)
				if !ok {
					return DNull, fmt.Errorf("unknown signature for concat_ws: concat_ws(%s, ...)", args[0].Type())
				}
				sep := string(dstr)
				var ss []string
				for _, d := range args[1:] {
					if d == DNull {
						continue
					}
					ds, err := datumToRawString(d)
					if err != nil {
						return DNull, err
					}
					ss = append(ss, ds)
				}
				return DString(strings.Join(ss, sep)), nil
			},
		},
	},

	"split_part": {
		builtin{
			types:      typeList{stringType, stringType, intType},
			returnType: DummyString,
			fn: func(args DTuple) (Datum, error) {
				text := string(args[0].(DString))
				sep := string(args[1].(DString))
				field := int(args[2].(DInt))

				if field <= 0 {
					return DNull, fmt.Errorf("field position must be greater than zero")
				}

				splits := strings.Split(text, sep)
				if field > len(splits) {
					return DString(""), nil
				}
				return DString(splits[field-1]), nil
			},
		},
	},

	"repeat": {
		builtin{
			types:      typeList{stringType, intType},
			returnType: DummyString,
			fn: func(args DTuple) (Datum, error) {
				s := string(args[0].(DString))
				count := int(args[1].(DInt))
				if count < 0 {
					count = 0
				}
				return DString(strings.Repeat(s, count)), nil
			},
		},
	},

	"ascii": {stringBuiltin1(func(s string) (Datum, error) {
		for _, ch := range s {
			return DInt(ch), nil
		}
		return nil, fmt.Errorf("the input string should not be empty")
	}, DummyInt)},

	"md5": {stringBuiltin1(func(s string) (Datum, error) {
		return DString(fmt.Sprintf("%x", md5.Sum([]byte(s)))), nil
	}, DummyString)},

	"sha1": {stringBuiltin1(func(s string) (Datum, error) {
		return DString(fmt.Sprintf("%x", sha1.Sum([]byte(s)))), nil
	}, DummyString)},

	"sha256": {stringBuiltin1(func(s string) (Datum, error) {
		return DString(fmt.Sprintf("%x", sha256.Sum256([]byte(s)))), nil
	}, DummyString)},

	"to_hex": {
		builtin{
			types:      typeList{intType},
			returnType: DummyString,
			fn: func(args DTuple) (Datum, error) {
				return DString(fmt.Sprintf("%x", int64(args[0].(DInt)))), nil
			},
		},
	},

	// TODO(XisiHuang): support the position(substring in string) syntax.
	"strpos": {stringBuiltin2(func(s, substring string) (Datum, error) {
		return DInt(strings.Index(s, substring) + 1), nil
	}, DummyInt)},

	// TODO(XisiHuang): support the trim([leading|trailing|both] [characters]
	// from string) syntax.
	"btrim": {
		stringBuiltin2(func(s, chars string) (Datum, error) {
			return DString(strings.Trim(s, chars)), nil
		}, DummyString),
		stringBuiltin1(func(s string) (Datum, error) {
			return DString(strings.Trim(s, " ")), nil
		}, DummyString),
	},

	"ltrim": {
		stringBuiltin2(func(s, chars string) (Datum, error) {
			return DString(strings.TrimLeft(s, chars)), nil
		}, DummyString),
		stringBuiltin1(func(s string) (Datum, error) {
			return DString(strings.TrimLeft(s, " ")), nil
		}, DummyString),
	},

	"rtrim": {
		stringBuiltin2(func(s, chars string) (Datum, error) {
			return DString(strings.TrimRight(s, chars)), nil
		}, DummyString),
		stringBuiltin1(func(s string) (Datum, error) {
			return DString(strings.Trim(s, " ")), nil
		}, DummyString),
	},

	"reverse": {stringBuiltin1(func(s string) (Datum, error) {
		runes := []rune(s)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return DString(string(runes)), nil
	}, DummyString)},

	"replace": {stringBuiltin3(func(s, from, to string) (Datum, error) {
		return DString(strings.Replace(s, from, to, -1)), nil
	}, DummyString)},

	"initcap": {stringBuiltin1(func(s string) (Datum, error) {
		return DString(strings.Title(strings.ToLower(s))), nil
	}, DummyString)},

	"left": {
		builtin{
			types:      typeList{stringType, intType},
			returnType: DummyString,
			fn: func(args DTuple) (Datum, error) {
				str := string(args[0].(DString))
				n := int(args[1].(DInt))

				if n < -len(str) {
					n = 0
				} else if n < 0 {
					n = len(str) + n
				} else if n > len(str) {
					n = len(str)
				}
				return DString(str[:n]), nil
			},
		},
	},

	"right": {
		builtin{
			types:      typeList{stringType, intType},
			returnType: DummyString,
			fn: func(args DTuple) (Datum, error) {
				str := string(args[0].(DString))
				n := int(args[1].(DInt))

				if n < -len(str) {
					n = 0
				} else if n < 0 {
					n = len(str) + n
				} else if n > len(str) {
					n = len(str)
				}
				return DString(str[len(str)-n:]), nil
			},
		},
	},

	"random": {
		builtin{
			types:      typeList{},
			returnType: DummyFloat,
			fn: func(args DTuple) (Datum, error) {
				return DFloat(rand.Float64()), nil
			},
		},
	},

	// Aggregate functions.

	"avg": {
		builtin{
			types:      typeList{intType},
			returnType: DummyFloat,
			fn: func(args DTuple) (Datum, error) {
				if args[0] == DNull {
					return args[0], nil
				}
				// AVG returns a float when given an int argument.
				return DFloat(args[0].(DInt)), nil
			},
		},
		builtin{
			types:      typeList{floatType},
			returnType: DummyFloat,
			fn: func(args DTuple) (Datum, error) {
				return args[0], nil
			},
		},
	},

	"count": countImpls(),

	"max": aggregateImpls(boolType, intType, floatType, stringType),
	"min": aggregateImpls(boolType, intType, floatType, stringType),
	"sum": aggregateImpls(intType, floatType),
}

// The aggregate functions all just return their first argument. We don't
// perform any type checking here either. The bulk of the aggregate function
// implementation is performed at a higher level in sql.groupNode.
func aggregateImpls(types ...reflect.Type) []builtin {
	var r []builtin
	for _, t := range types {
		r = append(r, builtin{
			types: typeList{t},
			fn: func(args DTuple) (Datum, error) {
				return args[0], nil
			},
		})
	}
	return r
}

func countImpls() []builtin {
	var r []builtin
	types := typeList{boolType, intType, floatType, stringType, tupleType}
	for _, t := range types {
		r = append(r, builtin{
			types:      typeList{t},
			returnType: DummyInt,
			fn: func(args DTuple) (Datum, error) {
				if _, ok := args[0].(DInt); ok {
					return args[0], nil
				}
				// COUNT always returns an int.
				return DummyInt, nil
			},
		})
	}
	return r
}

var substringImpls = []builtin{
	{
		types:      typeList{stringType, intType},
		returnType: DummyString,
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
		types:      typeList{stringType, intType, intType},
		returnType: DummyFloat,
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

func stringBuiltin1(f func(string) (Datum, error), returnType Datum) builtin {
	return builtin{
		types:      typeList{stringType},
		returnType: returnType,
		fn: func(args DTuple) (Datum, error) {
			return f(string(args[0].(DString)))
		},
	}
}

func stringBuiltin2(f func(string, string) (Datum, error), returnType Datum) builtin {
	return builtin{
		types:      typeList{stringType, stringType},
		returnType: returnType,
		fn: func(args DTuple) (Datum, error) {
			return f(string(args[0].(DString)), string(args[1].(DString)))
		},
	}
}

func stringBuiltin3(f func(string, string, string) (Datum, error), returnType Datum) builtin {
	return builtin{
		types:      typeList{stringType, stringType, stringType},
		returnType: returnType,
		fn: func(args DTuple) (Datum, error) {
			return f(string(args[0].(DString)), string(args[1].(DString)), string(args[2].(DString)))
		},
	}
}

func datumToRawString(datum Datum) (string, error) {
	if dString, ok := datum.(DString); ok {
		return string(dString), nil
	}

	return "", fmt.Errorf("argument type unsupported: %s", datum.Type())
}
