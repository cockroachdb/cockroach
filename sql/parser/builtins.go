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
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

var errEmptyInputString = errors.New("the input string must not be empty")
var errAbsOfMinInt64 = errors.New("abs of min integer value (-9223372036854775808) not defined")
var errRoundNumberDigits = errors.New("number of digits must be between 0 and 50")

type typeList []reflect.Type

type builtin struct {
	// Set to typeList{} for nullary functions and to nil for varidic
	// functions.
	types      typeList
	returnType Datum
	// Set to true when a function returns a different value when called with
	// the same parameters. e.g.: random(), now().
	impure bool
	fn     func(DTuple) (Datum, error)
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
var builtins = map[string][]builtin{
	// TODO(XisiHuang): support encoding, i.e., length(str, encoding).
	"length": {
		stringBuiltin1(func(s string) (Datum, error) {
			return DInt(utf8.RuneCountInString(s)), nil
		}, DummyInt),
		bytesBuiltin1(func(s string) (Datum, error) {
			return DInt(len(s)), nil
		}, DummyInt),
	},

	// TODO(pmattis): What string functions should also support bytesType?

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
					return DNull, fmt.Errorf("field position %d must be greater than zero", field)
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
		return nil, errEmptyInputString
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
		index := strings.Index(s, substring)
		if index < 0 {
			return DInt(0), nil
		}

		return DInt(utf8.RuneCountInString(s[:index]) + 1), nil
	}, DummyInt)},

	// TODO(XisiHuang): support the trim([leading|trailing|both] [characters]
	// from string) syntax.
	"btrim": {
		stringBuiltin2(func(s, chars string) (Datum, error) {
			return DString(strings.Trim(s, chars)), nil
		}, DummyString),
		stringBuiltin1(func(s string) (Datum, error) {
			return DString(strings.TrimSpace(s)), nil
		}, DummyString),
	},

	"ltrim": {
		stringBuiltin2(func(s, chars string) (Datum, error) {
			return DString(strings.TrimLeft(s, chars)), nil
		}, DummyString),
		stringBuiltin1(func(s string) (Datum, error) {
			return DString(strings.TrimLeftFunc(s, unicode.IsSpace)), nil
		}, DummyString),
	},

	"rtrim": {
		stringBuiltin2(func(s, chars string) (Datum, error) {
			return DString(strings.TrimRight(s, chars)), nil
		}, DummyString),
		stringBuiltin1(func(s string) (Datum, error) {
			return DString(strings.TrimRightFunc(s, unicode.IsSpace)), nil
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

	"translate": {stringBuiltin3(func(s, from, to string) (Datum, error) {
		const deletionRune = utf8.MaxRune + 1
		translation := make(map[rune]rune, len(from))
		for _, fromRune := range from {
			toRune, size := utf8.DecodeRuneInString(to)
			if toRune == utf8.RuneError {
				toRune = deletionRune
			} else {
				to = to[size:]
			}
			translation[fromRune] = toRune
		}

		runes := make([]rune, 0, len(s))
		for _, c := range s {
			if t, ok := translation[c]; ok {
				if t != deletionRune {
					runes = append(runes, t)
				}
			} else {
				runes = append(runes, c)
			}
		}
		return DString(string(runes)), nil
	}, DummyString)},

	"initcap": {stringBuiltin1(func(s string) (Datum, error) {
		return DString(strings.Title(strings.ToLower(s))), nil
	}, DummyString)},

	"left": {
		builtin{
			types:      typeList{stringType, intType},
			returnType: DummyString,
			fn: func(args DTuple) (Datum, error) {
				runes := []rune(string(args[0].(DString)))
				n := int(args[1].(DInt))

				if n < -len(runes) {
					n = 0
				} else if n < 0 {
					n = len(runes) + n
				} else if n > len(runes) {
					n = len(runes)
				}
				return DString(runes[:n]), nil
			},
		},
	},

	"right": {
		builtin{
			types:      typeList{stringType, intType},
			returnType: DummyString,
			fn: func(args DTuple) (Datum, error) {
				runes := []rune(string(args[0].(DString)))
				n := int(args[1].(DInt))

				if n < -len(runes) {
					n = 0
				} else if n < 0 {
					n = len(runes) + n
				} else if n > len(runes) {
					n = len(runes)
				}
				return DString(runes[len(runes)-n:]), nil
			},
		},
	},

	"random": {
		builtin{
			types:      typeList{},
			returnType: DummyFloat,
			impure:     true,
			fn: func(args DTuple) (Datum, error) {
				return DFloat(rand.Float64()), nil
			},
		},
	},

	// Timestamp/Date functions.

	"age": {
		builtin{
			types:      typeList{timestampType},
			returnType: DummyInterval,
			impure:     true,
			fn: func(args DTuple) (Datum, error) {
				return DInterval{Duration: time.Now().Sub(args[0].(DTimestamp).Time)}, nil
			},
		},
		builtin{
			types:      typeList{timestampType, timestampType},
			returnType: DummyInterval,
			fn: func(args DTuple) (Datum, error) {
				return DInterval{Duration: args[0].(DTimestamp).Sub(args[1].(DTimestamp).Time)}, nil
			},
		},
	},

	"current_date": {
		builtin{
			types:      typeList{},
			returnType: DummyDate,
			impure:     true,
			fn: func(args DTuple) (Datum, error) {
				return DDate{Time: time.Now().Truncate(24 * time.Hour)}, nil
			},
		},
	},

	"current_timestamp": {nowImpl},
	"now":               {nowImpl},

	"extract": {
		builtin{
			types:      typeList{stringType, timestampType},
			returnType: DummyInt,
			fn: func(args DTuple) (Datum, error) {
				// extract timeSpan fromTime.
				fromTime := args[1].(DTimestamp)
				timeSpan := strings.ToLower(string(args[0].(DString)))
				switch timeSpan {
				case "year":
					return DInt(fromTime.Year()), nil

				case "quarter":
					return DInt(fromTime.Month()/4 + 1), nil

				case "month":
					return DInt(fromTime.Month()), nil

				case "week":
					_, week := fromTime.ISOWeek()
					return DInt(week), nil

				case "day":
					return DInt(fromTime.Day()), nil

				case "dayofweek", "dow":
					return DInt(fromTime.Weekday()), nil

				case "dayofyear", "doy":
					return DInt(fromTime.YearDay()), nil

				case "hour":
					return DInt(fromTime.Hour()), nil

				case "minute":
					return DInt(fromTime.Minute()), nil

				case "second":
					return DInt(fromTime.Second()), nil

				case "millisecond":
					return DInt(fromTime.Nanosecond() / int(time.Millisecond)), nil

				case "microsecond":
					return DInt(fromTime.Nanosecond() / int(time.Microsecond)), nil

				case "nanosecond":
					return DInt(fromTime.Nanosecond()), nil

				case "epoch":
					return DInt(fromTime.Unix()), nil

				default:
					return DNull, fmt.Errorf("unsupported timespan: %s", timeSpan)
				}
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

	"max": aggregateImpls(boolType, intType, floatType, stringType, bytesType),
	"min": aggregateImpls(boolType, intType, floatType, stringType, bytesType),
	"sum": aggregateImpls(intType, floatType),

	// Math functions

	"abs": {
		builtin{
			returnType: DummyFloat,
			types:      typeList{floatType},
			fn: func(args DTuple) (Datum, error) {
				return DFloat(math.Abs(float64(args[0].(DFloat)))), nil
			},
		},
		builtin{
			returnType: DummyInt,
			types:      typeList{intType},
			fn: func(args DTuple) (Datum, error) {
				x := args[0].(DInt)
				switch {
				case x == math.MinInt64:
					return DNull, errAbsOfMinInt64
				case x < 0:
					return -x, nil
				}
				return x, nil
			},
		},
	},

	"acos": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(math.Acos(x)), nil
		}),
	},

	"asin": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(math.Asin(x)), nil
		}),
	},

	"atan": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(math.Atan(x)), nil
		}),
	},

	"atan2": {
		floatBuiltin2(func(x, y float64) (Datum, error) {
			return DFloat(math.Atan2(x, y)), nil
		}),
	},

	"cbrt": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(math.Cbrt(x)), nil
		}),
	},

	"ceil":    {ceilImpl},
	"ceiling": {ceilImpl},

	"cos": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(math.Cos(x)), nil
		}),
	},

	"degrees": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(180.0 * x / math.Pi), nil
		}),
	},

	"div": {
		floatBuiltin2(func(x, y float64) (Datum, error) {
			return DFloat(x / y), nil
		}),
	},

	"exp": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(math.Exp(x)), nil
		}),
	},

	"floor": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(math.Floor(x)), nil
		}),
	},

	"ln": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(math.Log(x)), nil
		}),
	},

	"log": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(math.Log10(x)), nil
		}),
	},

	"mod": {
		floatBuiltin2(func(x, y float64) (Datum, error) {
			return DFloat(math.Mod(x, y)), nil
		}),
		builtin{
			returnType: DummyInt,
			types:      typeList{intType, intType},
			fn: func(args DTuple) (Datum, error) {
				y := args[1].(DInt)
				if y == 0 {
					return DNull, errZeroModulus
				}
				x := args[0].(DInt)
				return DInt(x % y), nil
			},
		},
	},

	"pi": {
		builtin{
			returnType: DummyFloat,
			types:      typeList{},
			fn: func(args DTuple) (Datum, error) {
				return DFloat(math.Pi), nil
			},
		},
	},

	"pow":   {powImpl},
	"power": {powImpl},

	"radians": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(x * math.Pi / 180.0), nil
		}),
	},

	"round": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return round(x, 0)
		}),
		builtin{
			returnType: DummyFloat,
			types:      typeList{floatType, intType},
			fn: func(args DTuple) (Datum, error) {
				return round(float64(args[0].(DFloat)), int64(args[1].(DInt)))
			},
		},
	},

	"sin": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(math.Sin(x)), nil
		}),
	},

	"sign": {
		floatBuiltin1(func(x float64) (Datum, error) {
			switch {
			case x < 0:
				return DFloat(-1), nil
			case x == 0:
				return DFloat(0), nil
			}
			return DFloat(1), nil
		}),
		builtin{
			returnType: DummyInt,
			types:      typeList{intType},
			fn: func(args DTuple) (Datum, error) {
				x := args[0].(DInt)
				switch {
				case x < 0:
					return DInt(-1), nil
				case x == 0:
					return DInt(0), nil
				}
				return DInt(1), nil
			},
		},
	},

	"sqrt": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(math.Sqrt(x)), nil
		}),
	},

	"tan": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(math.Tan(x)), nil
		}),
	},

	"trunc": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(math.Trunc(x)), nil
		}),
	},
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
	types := typeList{boolType, intType, floatType, stringType, bytesType, tupleType}
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
		returnType: DummyString,
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

var ceilImpl = floatBuiltin1(func(x float64) (Datum, error) {
	return DFloat(math.Ceil(x)), nil
})

var nowImpl = builtin{
	types:      typeList{},
	returnType: DummyTimestamp,
	impure:     true,
	fn: func(args DTuple) (Datum, error) {
		return DTimestamp{Time: time.Now()}, nil
	},
}

var powImpl = floatBuiltin2(func(x, y float64) (Datum, error) {
	return DFloat(math.Pow(x, y)), nil
})

func floatBuiltin1(f func(float64) (Datum, error)) builtin {
	return builtin{
		types:      typeList{floatType},
		returnType: DummyFloat,
		fn: func(args DTuple) (Datum, error) {
			return f(float64(args[0].(DFloat)))
		},
	}
}

func floatBuiltin2(f func(float64, float64) (Datum, error)) builtin {
	return builtin{
		types:      typeList{floatType, floatType},
		returnType: DummyFloat,
		fn: func(args DTuple) (Datum, error) {
			return f(float64(args[0].(DFloat)),
				float64(args[1].(DFloat)))
		},
	}
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

func bytesBuiltin1(f func(string) (Datum, error), returnType Datum) builtin {
	return builtin{
		types:      typeList{bytesType},
		returnType: returnType,
		fn: func(args DTuple) (Datum, error) {
			return f(string(args[0].(DBytes)))
		},
	}
}

func datumToRawString(datum Datum) (string, error) {
	if dString, ok := datum.(DString); ok {
		return string(dString), nil
	}

	return "", fmt.Errorf("argument type unsupported: %s", datum.Type())
}

func round(x float64, n int64) (Datum, error) {
	if n < 0 || n > 50 {
		return DNull, errRoundNumberDigits
	}
	const b = 64
	y, err := strconv.ParseFloat(strconv.FormatFloat(x, 'f', int(n), b), b)
	return DFloat(y), err
}
