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
// permissions and limitations under the License.
//
// Author: Peter Mattis (peter@cockroachlabs.com)

package parser

import (
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"regexp"
	"regexp/syntax"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/uuid"
	"github.com/cockroachdb/decimal"
)

var errEmptyInputString = errors.New("the input string must not be empty")
var errAbsOfMinInt64 = errors.New("abs of min integer value (-9223372036854775808) not defined")
var errRoundNumberDigits = errors.New("number of digits must be greater than 0")

type argTypes []reflect.Type

type typeList interface {
	match(types argTypes) bool
}

func (a argTypes) match(types argTypes) bool {
	if len(types) != len(a) {
		return false
	}
	for i := range types {
		if types[i] != a[i] {
			return false
		}
	}
	return true
}

type anyType struct{}

func (anyType) match(types argTypes) bool {
	return true
}

// variadicType is an implementation of typeList which matches when
// each argument is either NULL or of the type typ.
type variadicType struct {
	typ reflect.Type
}

func (v variadicType) match(types argTypes) bool {
	for _, typ := range types {
		if !(typ == nullType || typ == v.typ) {
			return false
		}
	}
	return true
}

type builtin struct {
	types      typeList
	returnType func(MapArgs, DTuple) (Datum, error)
	// Set to true when a function potentially returns a different value
	// when called in the same statement with the same parameters.
	// e.g.: random(), clock_timestamp(). Some functions like now()
	// return the same value in the same statement, but different values
	// in separate statements, and should not be marked as impure.
	impure bool
	fn     func(EvalContext, DTuple) (Datum, error)
}

// The map from function name to function data. Keep the list of functions
// sorted please.
var builtins = map[string][]builtin{
	// TODO(XisiHuang): support encoding, i.e., length(str, encoding).
	"length": {
		stringBuiltin1(func(s string) (Datum, error) {
			return DInt(utf8.RuneCountInString(s)), nil
		}, typeInt),
		bytesBuiltin1(func(s string) (Datum, error) {
			return DInt(len(s)), nil
		}, typeInt),
	},

	// TODO(pmattis): What string functions should also support bytesType?

	"lower": {stringBuiltin1(func(s string) (Datum, error) {
		return DString(strings.ToLower(s)), nil
	}, typeString)},

	"upper": {stringBuiltin1(func(s string) (Datum, error) {
		return DString(strings.ToUpper(s)), nil
	}, typeString)},

	"substr":    substringImpls,
	"substring": substringImpls,

	// concat concatenates the text representations of all the arguments.
	// NULL arguments are ignored.
	"concat": {
		builtin{
			types:      variadicType{stringType},
			returnType: typeString,
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
				var buffer bytes.Buffer
				for _, d := range args {
					if d == DNull {
						continue
					}
					buffer.WriteString(string(d.(DString)))
				}
				return DString(buffer.String()), nil
			},
		},
	},

	"concat_ws": {
		builtin{
			types:      variadicType{stringType},
			returnType: typeString,
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
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
					ss = append(ss, string(d.(DString)))
				}
				return DString(strings.Join(ss, sep)), nil
			},
		},
	},

	"split_part": {
		builtin{
			types:      argTypes{stringType, stringType, intType},
			returnType: typeString,
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
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
			types:      argTypes{stringType, intType},
			returnType: typeString,
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
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
	}, typeInt)},

	"md5": {stringBuiltin1(func(s string) (Datum, error) {
		return DString(fmt.Sprintf("%x", md5.Sum([]byte(s)))), nil
	}, typeString)},

	"sha1": {stringBuiltin1(func(s string) (Datum, error) {
		return DString(fmt.Sprintf("%x", sha1.Sum([]byte(s)))), nil
	}, typeString)},

	"sha256": {stringBuiltin1(func(s string) (Datum, error) {
		return DString(fmt.Sprintf("%x", sha256.Sum256([]byte(s)))), nil
	}, typeString)},

	"to_hex": {
		builtin{
			types:      argTypes{intType},
			returnType: typeString,
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
				return DString(fmt.Sprintf("%x", int64(args[0].(DInt)))), nil
			},
		},
	},

	// The SQL parser coerces POSITION to STRPOS.
	"strpos": {stringBuiltin2(func(s, substring string) (Datum, error) {
		index := strings.Index(s, substring)
		if index < 0 {
			return DInt(0), nil
		}

		return DInt(utf8.RuneCountInString(s[:index]) + 1), nil
	}, typeInt)},

	"overlay": {
		builtin{
			types:      argTypes{stringType, stringType, intType},
			returnType: typeString,
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
				s := string(args[0].(DString))
				to := string(args[1].(DString))
				pos := int(args[2].(DInt))
				size := utf8.RuneCountInString(to)
				return overlay(s, to, pos, size)
			},
		},
		builtin{
			types:      argTypes{stringType, stringType, intType, intType},
			returnType: typeString,
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
				s := string(args[0].(DString))
				to := string(args[1].(DString))
				pos := int(args[2].(DInt))
				size := int(args[3].(DInt))
				return overlay(s, to, pos, size)
			},
		},
	},

	// The SQL parser coerces TRIM(...) and TRIM(BOTH ...) to BTRIM(...).
	"btrim": {
		stringBuiltin2(func(s, chars string) (Datum, error) {
			return DString(strings.Trim(s, chars)), nil
		}, typeString),
		stringBuiltin1(func(s string) (Datum, error) {
			return DString(strings.TrimSpace(s)), nil
		}, typeString),
	},

	// The SQL parser coerces TRIM(LEADING ...) to LTRIM(...).
	"ltrim": {
		stringBuiltin2(func(s, chars string) (Datum, error) {
			return DString(strings.TrimLeft(s, chars)), nil
		}, typeString),
		stringBuiltin1(func(s string) (Datum, error) {
			return DString(strings.TrimLeftFunc(s, unicode.IsSpace)), nil
		}, typeString),
	},

	// The SQL parser coerces TRIM(TRAILING ...) to RTRIM(...).
	"rtrim": {
		stringBuiltin2(func(s, chars string) (Datum, error) {
			return DString(strings.TrimRight(s, chars)), nil
		}, typeString),
		stringBuiltin1(func(s string) (Datum, error) {
			return DString(strings.TrimRightFunc(s, unicode.IsSpace)), nil
		}, typeString),
	},

	"reverse": {stringBuiltin1(func(s string) (Datum, error) {
		runes := []rune(s)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return DString(string(runes)), nil
	}, typeString)},

	"replace": {stringBuiltin3(func(s, from, to string) (Datum, error) {
		return DString(strings.Replace(s, from, to, -1)), nil
	}, typeString)},

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
	}, typeString)},

	"regexp_extract": {
		builtin{
			types:      argTypes{stringType, stringType},
			returnType: typeString,
			fn: func(ctx EvalContext, args DTuple) (Datum, error) {
				s := string(args[0].(DString))
				pattern := string(args[1].(DString))
				return regexpExtract(ctx, s, pattern, `\`)
			},
		},
	},

	"regexp_replace": {
		builtin{
			types:      argTypes{stringType, stringType, stringType},
			returnType: typeString,
			fn: func(ctx EvalContext, args DTuple) (Datum, error) {
				s := string(args[0].(DString))
				pattern := string(args[1].(DString))
				to := string(args[2].(DString))
				return regexpReplace(ctx, s, pattern, to, "")
			},
		},
		builtin{
			types:      argTypes{stringType, stringType, stringType, stringType},
			returnType: typeString,
			fn: func(ctx EvalContext, args DTuple) (Datum, error) {
				s := string(args[0].(DString))
				pattern := string(args[1].(DString))
				to := string(args[2].(DString))
				sqlFlags := string(args[3].(DString))
				return regexpReplace(ctx, s, pattern, to, sqlFlags)
			},
		},
	},

	"initcap": {stringBuiltin1(func(s string) (Datum, error) {
		return DString(strings.Title(strings.ToLower(s))), nil
	}, typeString)},

	"left": {
		builtin{
			types:      argTypes{bytesType, intType},
			returnType: typeBytes,
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
				bytes := []byte(args[0].(DBytes))
				n := int(args[1].(DInt))

				if n < -len(bytes) {
					n = 0
				} else if n < 0 {
					n = len(bytes) + n
				} else if n > len(bytes) {
					n = len(bytes)
				}
				return DBytes(bytes[:n]), nil
			},
		},
		builtin{
			types:      argTypes{stringType, intType},
			returnType: typeString,
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
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
			types:      argTypes{bytesType, intType},
			returnType: typeBytes,
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
				bytes := []byte(args[0].(DBytes))
				n := int(args[1].(DInt))

				if n < -len(bytes) {
					n = 0
				} else if n < 0 {
					n = len(bytes) + n
				} else if n > len(bytes) {
					n = len(bytes)
				}
				return DBytes(bytes[len(bytes)-n:]), nil
			},
		},
		builtin{
			types:      argTypes{stringType, intType},
			returnType: typeString,
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
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
			types:      argTypes{},
			returnType: typeFloat,
			impure:     true,
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
				return DFloat(rand.Float64()), nil
			},
		},
	},

	"experimental_unique_bytes": {
		builtin{
			types:      argTypes{},
			returnType: typeBytes,
			impure:     true,
			fn: func(ctx EvalContext, args DTuple) (Datum, error) {
				return generateUniqueBytes(ctx.NodeID), nil
			},
		},
	},

	"experimental_unique_int": {
		builtin{
			types:      argTypes{},
			returnType: typeInt,
			impure:     true,
			fn: func(ctx EvalContext, args DTuple) (Datum, error) {
				return generateUniqueInt(ctx.NodeID), nil
			},
		},
	},

	"experimental_uuid_v4": {
		builtin{
			types:      argTypes{},
			returnType: typeBytes,
			impure:     true,
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
				return DBytes(uuid.NewUUID4()), nil
			},
		},
	},

	"unique_rowid": {
		builtin{
			types:      argTypes{},
			returnType: typeInt,
			impure:     true,
			fn: func(ctx EvalContext, args DTuple) (Datum, error) {
				return uniqueRowID(ctx.NodeID), nil
			},
		},
	},

	"greatest": {
		builtin{
			types:      anyType{},
			returnType: typeTuple,
			fn: func(ctx EvalContext, args DTuple) (Datum, error) {
				return pickFromTuple(ctx, true /* greatest */, args)
			},
		},
	},

	"least": {
		builtin{
			types:      anyType{},
			returnType: typeTuple,
			fn: func(ctx EvalContext, args DTuple) (Datum, error) {
				return pickFromTuple(ctx, false /* !greatest */, args)
			},
		},
	},

	// Timestamp/Date functions.

	"age": {
		builtin{
			types:      argTypes{timestampType},
			returnType: typeInterval,
			fn: func(e EvalContext, args DTuple) (Datum, error) {
				return DInterval{Duration: e.StmtTimestamp.Sub(args[0].(DTimestamp).Time)}, nil
			},
		},
		builtin{
			types:      argTypes{timestampType, timestampType},
			returnType: typeInterval,
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
				return DInterval{Duration: args[0].(DTimestamp).Sub(args[1].(DTimestamp).Time)}, nil
			},
		},
	},

	"current_date": {
		builtin{
			types:      argTypes{},
			returnType: typeDate,
			fn: func(e EvalContext, args DTuple) (Datum, error) {
				return e.makeDDate(e.StmtTimestamp.Time)
			},
		},
	},

	"statement_timestamp": {nowImpl},
	"current_timestamp":   {nowImpl},
	"now":                 {nowImpl},

	"clock_timestamp": {
		builtin{
			types:      argTypes{},
			returnType: typeTimestamp,
			impure:     true,
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
				return DTimestamp{Time: time.Now()}, nil
			},
		},
	},

	"transaction_timestamp": {
		builtin{
			types:      argTypes{},
			returnType: typeTimestamp,
			fn: func(e EvalContext, args DTuple) (Datum, error) {
				return e.TxnTimestamp, nil
			},
		},
	},

	"extract": {
		builtin{
			types:      argTypes{stringType, timestampType},
			returnType: typeInt,
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
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
			types:      argTypes{intType},
			returnType: typeFloat,
		},
		builtin{
			types:      argTypes{floatType},
			returnType: typeFloat,
		},
		builtin{
			types:      argTypes{decimalType},
			returnType: typeDecimal,
		},
	},

	"count": countImpls(),

	"max": aggregateImpls(boolType, intType, floatType, decimalType, stringType, bytesType, dateType, timestampType, intervalType),
	"min": aggregateImpls(boolType, intType, floatType, decimalType, stringType, bytesType, dateType, timestampType, intervalType),
	"sum": aggregateImpls(intType, floatType, decimalType),

	"variance": {
		builtin{
			types:      argTypes{intType},
			returnType: typeFloat,
		},
		builtin{
			types:      argTypes{floatType},
			returnType: typeFloat,
		},
	},

	"stddev": {
		builtin{
			types:      argTypes{intType},
			returnType: typeFloat,
		},
		builtin{
			types:      argTypes{floatType},
			returnType: typeFloat,
		},
	},

	// Math functions

	"abs": {
		builtin{
			returnType: typeFloat,
			types:      argTypes{floatType},
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
				return DFloat(math.Abs(float64(args[0].(DFloat)))), nil
			},
		},
		builtin{
			returnType: typeDecimal,
			types:      argTypes{decimalType},
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
				return DDecimal{Decimal: args[0].(DDecimal).Abs()}, nil
			},
		},
		builtin{
			returnType: typeInt,
			types:      argTypes{intType},
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
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

	// TODO(nvanbenschoten) Add native support for decimal.
	"cbrt": floatOrDecimalBuiltin1(func(x float64) (Datum, error) {
		return DFloat(math.Cbrt(x)), nil
	}),

	"ceil":    ceilImpl,
	"ceiling": ceilImpl,

	"cos": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(math.Cos(x)), nil
		}),
	},

	"cot": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(1 / math.Tan(x)), nil
		}),
	},

	"degrees": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(180.0 * x / math.Pi), nil
		}),
	},

	"div": {
		floatBuiltin2(func(x, y float64) (Datum, error) {
			return DFloat(math.Trunc(x / y)), nil
		}),
		decimalBuiltin2(func(x, y decimal.Decimal) (Datum, error) {
			if y.Equals(decimal.Zero) {
				return nil, errDivByZero
			}
			return DDecimal{Decimal: x.Div(y).Truncate(0)}, nil
		}),
	},

	// TODO(nvanbenschoten) Add native support for decimal.
	"exp": floatOrDecimalBuiltin1(func(x float64) (Datum, error) {
		return DFloat(math.Exp(x)), nil
	}),

	"floor": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(math.Floor(x)), nil
		}),
		decimalBuiltin1(func(x decimal.Decimal) (Datum, error) {
			return DDecimal{Decimal: x.Floor()}, nil
		}),
	},

	// TODO(nvanbenschoten) Add native support for decimal.
	"ln": floatOrDecimalBuiltin1(func(x float64) (Datum, error) {
		return DFloat(math.Log(x)), nil
	}),

	// TODO(nvanbenschoten) Add native support for decimal.
	"log": floatOrDecimalBuiltin1(func(x float64) (Datum, error) {
		return DFloat(math.Log10(x)), nil
	}),

	"mod": {
		floatBuiltin2(func(x, y float64) (Datum, error) {
			return DFloat(math.Mod(x, y)), nil
		}),
		decimalBuiltin2(func(x, y decimal.Decimal) (Datum, error) {
			if y.Equals(decimal.Zero) {
				return nil, errZeroModulus
			}
			return DDecimal{Decimal: x.Mod(y)}, nil
		}),
		builtin{
			returnType: typeInt,
			types:      argTypes{intType, intType},
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
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
			returnType: typeFloat,
			types:      argTypes{},
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
				return DFloat(math.Pi), nil
			},
		},
	},

	"pow":   powImpls,
	"power": powImpls,

	"radians": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(x * math.Pi / 180.0), nil
		}),
	},

	"round": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return round(x, 0)
		}),
		decimalBuiltin1(func(x decimal.Decimal) (Datum, error) {
			return DDecimal{Decimal: x.Round(0)}, nil
		}),
		builtin{
			returnType: typeFloat,
			types:      argTypes{floatType, intType},
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
				return round(float64(args[0].(DFloat)), int64(args[1].(DInt)))
			},
		},
		builtin{
			returnType: typeFloat,
			types:      argTypes{decimalType, intType},
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
				return DDecimal{Decimal: args[0].(DDecimal).Round(int32(args[1].(DInt)))}, nil
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
		decimalBuiltin1(func(x decimal.Decimal) (Datum, error) {
			return DFloat(x.Cmp(decimal.Zero)), nil
		}),
		builtin{
			returnType: typeInt,
			types:      argTypes{intType},
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
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

	// TODO(nvanbenschoten) Add native support for decimal.
	"sqrt": floatOrDecimalBuiltin1(func(x float64) (Datum, error) {
		return DFloat(math.Sqrt(x)), nil
	}),

	"tan": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(math.Tan(x)), nil
		}),
	},

	"trunc": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(math.Trunc(x)), nil
		}),
		decimalBuiltin1(func(x decimal.Decimal) (Datum, error) {
			return DDecimal{Decimal: x.Truncate(0)}, nil
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
			types: argTypes{t},
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
				return args[0], nil
			},
		})
	}
	return r
}

func countImpls() []builtin {
	var r []builtin
	types := argTypes{boolType, intType, floatType, stringType, bytesType, dateType, timestampType, intervalType, tupleType}
	for _, t := range types {
		r = append(r, builtin{
			types:      argTypes{t},
			returnType: typeInt,
		})
	}
	return r
}

var substringImpls = []builtin{
	{
		types:      argTypes{stringType, intType},
		returnType: typeString,
		fn: func(_ EvalContext, args DTuple) (Datum, error) {
			runes := []rune(string(args[0].(DString)))
			// SQL strings are 1-indexed.
			start := int(args[1].(DInt)) - 1

			if start < 0 {
				start = 0
			} else if start > len(runes) {
				start = len(runes)
			}

			return DString(runes[start:]), nil
		},
	},
	{
		types:      argTypes{stringType, intType, intType},
		returnType: typeString,
		fn: func(_ EvalContext, args DTuple) (Datum, error) {
			runes := []rune(string(args[0].(DString)))
			// SQL strings are 1-indexed.
			start := int(args[1].(DInt)) - 1
			length := int(args[2].(DInt))

			if length < 0 {
				return DNull, fmt.Errorf("negative substring length %d not allowed", length)
			}

			end := start + length
			if end < 0 {
				end = 0
			} else if end > len(runes) {
				end = len(runes)
			}

			if start < 0 {
				start = 0
			} else if start > len(runes) {
				start = len(runes)
			}

			return DString(runes[start:end]), nil
		},
	},
	{
		types:      argTypes{stringType, stringType},
		returnType: typeString,
		fn: func(ctx EvalContext, args DTuple) (Datum, error) {
			s := string(args[0].(DString))
			pattern := string(args[1].(DString))
			return regexpExtract(ctx, s, pattern, `\`)
		},
	},
	{
		types:      argTypes{stringType, stringType, stringType},
		returnType: typeString,
		fn: func(ctx EvalContext, args DTuple) (Datum, error) {
			s := string(args[0].(DString))
			pattern := string(args[1].(DString))
			escape := string(args[2].(DString))
			return regexpExtract(ctx, s, pattern, escape)
		},
	},
}

var ceilImpl = []builtin{
	floatBuiltin1(func(x float64) (Datum, error) {
		return DFloat(math.Ceil(x)), nil
	}),
	decimalBuiltin1(func(x decimal.Decimal) (Datum, error) {
		return DDecimal{Decimal: x.Ceil()}, nil
	}),
}

var nowImpl = builtin{
	types:      argTypes{},
	returnType: typeTimestamp,
	fn: func(e EvalContext, args DTuple) (Datum, error) {
		return e.StmtTimestamp, nil
	},
}

// TODO(nvanbenschoten) Add native support for decimal.
var powImpls = floatOrDecimalBuiltin2(func(x, y float64) (Datum, error) {
	return DFloat(math.Pow(x, y)), nil
})

func floatBuiltin1(f func(float64) (Datum, error)) builtin {
	return builtin{
		types:      argTypes{floatType},
		returnType: typeFloat,
		fn: func(_ EvalContext, args DTuple) (Datum, error) {
			return f(float64(args[0].(DFloat)))
		},
	}
}

func floatBuiltin2(f func(float64, float64) (Datum, error)) builtin {
	return builtin{
		types:      argTypes{floatType, floatType},
		returnType: typeFloat,
		fn: func(_ EvalContext, args DTuple) (Datum, error) {
			return f(float64(args[0].(DFloat)),
				float64(args[1].(DFloat)))
		},
	}
}

func decimalBuiltin1(f func(decimal.Decimal) (Datum, error)) builtin {
	return builtin{
		types:      argTypes{decimalType},
		returnType: typeDecimal,
		fn: func(_ EvalContext, args DTuple) (Datum, error) {
			return f(args[0].(DDecimal).Decimal)
		},
	}
}

func decimalBuiltin2(f func(decimal.Decimal, decimal.Decimal) (Datum, error)) builtin {
	return builtin{
		types:      argTypes{decimalType, decimalType},
		returnType: typeDecimal,
		fn: func(_ EvalContext, args DTuple) (Datum, error) {
			return f(args[0].(DDecimal).Decimal,
				args[1].(DDecimal).Decimal)
		},
	}
}

func floatOrDecimalBuiltin1(f func(float64) (Datum, error)) []builtin {
	return []builtin{
		{
			types:      argTypes{floatType},
			returnType: typeFloat,
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
				return f(float64(args[0].(DFloat)))
			},
		}, {
			types:      argTypes{decimalType},
			returnType: typeDecimal,
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
				v, _ := args[0].(DDecimal).Float64()
				r, err := f(v)
				if err != nil {
					return r, err
				}
				rf := float64(r.(DFloat))
				if math.IsNaN(rf) || math.IsInf(rf, 0) {
					// TODO(nvanbenschoten) NaN semmantics should be introduced
					// into the decimal library to support it here.
					return nil, fmt.Errorf("decimal does not support NaN")
				}
				return DDecimal{Decimal: decimal.NewFromFloat(rf)}, nil
			},
		},
	}
}

func floatOrDecimalBuiltin2(f func(float64, float64) (Datum, error)) []builtin {
	return []builtin{
		{
			types:      argTypes{floatType, floatType},
			returnType: typeFloat,
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
				return f(float64(args[0].(DFloat)),
					float64(args[1].(DFloat)))
			},
		}, {
			types:      argTypes{decimalType, decimalType},
			returnType: typeDecimal,
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
				v1, _ := args[0].(DDecimal).Float64()
				v2, _ := args[1].(DDecimal).Float64()
				r, err := f(v1, v2)
				if err != nil {
					return r, err
				}
				rf := float64(r.(DFloat))
				if math.IsNaN(rf) || math.IsInf(rf, 0) {
					// TODO(nvanbenschoten) NaN semmantics should be introduced
					// into the decimal library to support it here.
					return nil, fmt.Errorf("decimal does not support NaN")
				}
				return DDecimal{Decimal: decimal.NewFromFloat(rf)}, nil
			},
		},
	}
}

func stringBuiltin1(f func(string) (Datum, error), returnType func(MapArgs, DTuple) (Datum, error)) builtin {
	return builtin{
		types:      argTypes{stringType},
		returnType: returnType,
		fn: func(_ EvalContext, args DTuple) (Datum, error) {
			return f(string(args[0].(DString)))
		},
	}
}

func stringBuiltin2(f func(string, string) (Datum, error), returnType func(MapArgs, DTuple) (Datum, error)) builtin {
	return builtin{
		types:      argTypes{stringType, stringType},
		returnType: returnType,
		fn: func(_ EvalContext, args DTuple) (Datum, error) {
			return f(string(args[0].(DString)), string(args[1].(DString)))
		},
	}
}

func stringBuiltin3(f func(string, string, string) (Datum, error), returnType func(MapArgs, DTuple) (Datum, error)) builtin {
	return builtin{
		types:      argTypes{stringType, stringType, stringType},
		returnType: returnType,
		fn: func(_ EvalContext, args DTuple) (Datum, error) {
			return f(string(args[0].(DString)), string(args[1].(DString)), string(args[2].(DString)))
		},
	}
}

func stringBuiltin4(f func(string, string, string, string) (Datum, error), returnType func(MapArgs, DTuple) (Datum, error)) builtin {
	return builtin{
		types:      argTypes{stringType, stringType, stringType, stringType},
		returnType: returnType,
		fn: func(_ EvalContext, args DTuple) (Datum, error) {
			return f(string(args[0].(DString)), string(args[1].(DString)), string(args[2].(DString)), string(args[3].(DString)))
		},
	}
}

func bytesBuiltin1(f func(string) (Datum, error), returnType func(MapArgs, DTuple) (Datum, error)) builtin {
	return builtin{
		types:      argTypes{bytesType},
		returnType: returnType,
		fn: func(_ EvalContext, args DTuple) (Datum, error) {
			return f(string(args[0].(DBytes)))
		},
	}
}

type regexpEscapeKey struct {
	sqlPattern string
	sqlEscape  string
}

func (k regexpEscapeKey) pattern() (string, error) {
	pattern := k.sqlPattern
	if k.sqlEscape != `\` {
		pattern = strings.Replace(pattern, `\`, `\\`, -1)
		pattern = strings.Replace(pattern, k.sqlEscape, `\`, -1)
	}
	return pattern, nil
}

func regexpExtract(ctx EvalContext, s, pattern, escape string) (Datum, error) {
	patternRe, err := ctx.ReCache.GetRegexp(regexpEscapeKey{pattern, escape})
	if err != nil {
		return nil, err
	}

	match := patternRe.FindStringSubmatch(s)
	if match == nil {
		return DNull, nil
	}

	if len(match) > 1 {
		return DString(match[1]), nil
	}
	return DString(match[0]), nil
}

type regexpFlagKey struct {
	sqlPattern string
	sqlFlags   string
}

func (k regexpFlagKey) pattern() (string, error) {
	return regexpEvalFlags(k.sqlPattern, k.sqlFlags)
}

var replaceSubRe = regexp.MustCompile(`\\[&1-9]`)

func regexpReplace(ctx EvalContext, s, pattern, to, sqlFlags string) (Datum, error) {
	patternRe, err := ctx.ReCache.GetRegexp(regexpFlagKey{pattern, sqlFlags})
	if err != nil {
		return nil, err
	}

	matchCount := 1
	if strings.ContainsRune(sqlFlags, 'g') {
		matchCount = -1
	}

	replaceIndex := 0
	var newString bytes.Buffer

	// regexp.ReplaceAllStringFunc cannot be used here because it does not provide
	// access to regexp submatches for expansion in the replacement string.
	// regexp.ReplaceAllString cannot be used here because it does not allow
	// replacement of a specific number of matches, and does not expose the full
	// match for expansion in the replacement string.
	//
	// regexp.FindAllStringSubmatchIndex must therefore be used, which returns a 2D
	// int array. The outer array is iterated over with this for-range loop, and corresponds
	// to each match of the pattern in the string s. Inside each outer array is an int
	// array with index pairs. The first pair in a given match n ([n][0] & [n][1]) represents
	// the start and end index in s of the matched pattern. Subsequent pairs ([n][2] & [n][3],
	// and so on) represent the start and end index in s of matched subexpressions within the
	// pattern.
	for _, matchIndex := range patternRe.FindAllStringSubmatchIndex(s, matchCount) {
		start := matchIndex[0]
		end := matchIndex[1]

		// Add sections of s either before the first match or between matches.
		preMatch := s[replaceIndex:start]
		newString.WriteString(preMatch)

		// Add the replacement string for the current match.
		match := s[start:end]
		matchTo := replaceSubRe.ReplaceAllStringFunc(to, func(repl string) string {
			subRef := repl[len(repl)-1]
			if subRef == '&' {
				return match
			}

			sub, err := strconv.Atoi(string(subRef))
			if err != nil {
				panic(fmt.Sprintf("Invalid integer submatch reference seen: %v", err))
			}
			if 2*sub >= len(matchIndex) {
				// regexpReplace expects references to "out-of-bounds" capture groups
				// to be ignored, so replace with an empty string.
				return ""
			}

			subStart := matchIndex[2*sub]
			subEnd := matchIndex[2*sub+1]
			return s[subStart:subEnd]
		})
		newString.WriteString(matchTo)

		replaceIndex = end
	}

	// Add the section of s past the final match.
	newString.WriteString(s[replaceIndex:])

	return DString(newString.String()), nil
}

var flagToByte = map[syntax.Flags]byte{
	syntax.FoldCase: 'i',
	syntax.DotNL:    's',
}

var flagToNotByte = map[syntax.Flags]byte{
	syntax.OneLine: 'm',
}

// regexpEvalFlags evaluates the provided Postgres regexp flags in
// accordance with their definitions provided at
// http://www.postgresql.org/docs/9.0/static/functions-matching.html#POSIX-EMBEDDED-OPTIONS-TABLE.
// It then returns an adjusted regexp pattern.
func regexpEvalFlags(pattern, sqlFlags string) (string, error) {
	flags := syntax.DotNL

	for _, sqlFlag := range sqlFlags {
		switch sqlFlag {
		case 'g':
			// Handled in `regexpReplace`.
		case 'i':
			flags |= syntax.FoldCase
		case 'c':
			flags &^= syntax.FoldCase
		case 's':
			flags |= syntax.DotNL
		case 'm', 'n':
			flags &^= syntax.DotNL
			flags |= syntax.OneLine
		case 'p':
			flags |= syntax.DotNL
			flags |= syntax.OneLine
		case 'w':
			flags |= syntax.DotNL
			flags &^= syntax.OneLine
		default:
			return "", fmt.Errorf("invalid regexp flag: %q", sqlFlag)
		}
	}

	var goFlags bytes.Buffer
	for flag, b := range flagToByte {
		if flags&flag != 0 {
			goFlags.WriteByte(b)
		}
	}
	for flag, b := range flagToNotByte {
		if flags&flag == 0 {
			goFlags.WriteByte(b)
		}
	}
	// Bytes() instead of String() to save an allocation.
	bs := goFlags.Bytes()
	if len(bs) == 0 {
		return pattern, nil
	}
	return fmt.Sprintf("(?%s:%s)", bs, pattern), nil
}

func overlay(s, to string, pos, size int) (Datum, error) {
	if pos < 1 {
		return nil, fmt.Errorf("non-positive substring length not allowed: %d", pos)
	}
	pos--

	runes := []rune(s)
	if pos > len(runes) {
		pos = len(runes)
	}
	after := pos + size
	if after < 0 {
		after = 0
	} else if after > len(runes) {
		after = len(runes)
	}
	return DString(string(runes[:pos]) + to + string(runes[after:])), nil
}

func round(x float64, n int64) (Datum, error) {
	switch {
	case n < 0:
		return DNull, errRoundNumberDigits
	case n > 323:
		// When rounding to more than 323 digits after the decimal
		// point, the original number is returned, because rounding has
		// no effect at scales smaller than 1e-323.
		//
		// 323 is the sum of
		//
		// 15, the maximum number of significant digits in a decimal
		// string that can be converted to the IEEE 754 double precision
		// representation and back to a string that matches the
		// original; and
		//
		// 308, the largest exponent. The significant digits can be
		// right shifted by 308 positions at most, by setting the
		// exponent to -308.
		return DFloat(x), nil
	}
	const b = 64
	y, err := strconv.ParseFloat(strconv.FormatFloat(x, 'f', int(n), b), b)
	if err != nil {
		panic(fmt.Sprintf("parsing a float that was just formatted failed: %s", err))
	}
	return DFloat(y), nil
}

// typeTuple returns the Datum type that all arguments share, or an error
// if they do not share types.
func typeTuple(params MapArgs, args DTuple) (Datum, error) {
	datum := DNull
	hasValArgs := false
	for _, arg := range args {
		if arg == DNull {
			continue
		}
		if _, ok := arg.(DValArg); ok {
			hasValArgs = true
			continue
		}
		// Find the first non-null argument.
		if datum == DNull {
			datum = arg
			continue
		}
		if arg != datum {
			return nil, fmt.Errorf("incompatible argument types %s, %s", datum.Type(), arg.Type())
		}
	}
	if hasValArgs {
		for _, arg := range args {
			_, err := params.SetInferredType(arg, datum)
			if err != nil {
				return nil, err
			}
		}
	}
	return datum, nil
}

// Pick the greatest (or least value) from a tuple.
func pickFromTuple(ctx EvalContext, greatest bool, args DTuple) (Datum, error) {
	g := args[0]
	// Pick a greater (or smaller) value.
	for _, d := range args[1:] {
		var eval Datum
		var err error
		if greatest {
			eval, err = evalComparison(ctx, LT, g, d)
		} else {
			eval, err = evalComparison(ctx, LT, d, g)
		}
		if err != nil {
			return nil, err
		}
		if eval == DBool(true) ||
			(eval == DNull && g == DNull) {
			g = d
		}
	}
	return g, nil
}

var uniqueBytesState struct {
	sync.Mutex
	nanos uint64
}

func generateUniqueBytes(nodeID roachpb.NodeID) DBytes {
	// Unique bytes are composed of the current time in nanoseconds and the
	// node-id. If the nanosecond value is the same on two consecutive calls to
	// time.Now() the nanoseconds value is incremented. The node-id is varint
	// encoded. Since node-ids are allocated consecutively starting at 1, the
	// node-id field will consume 1 or 2 bytes for any reasonably sized cluster.
	//
	// TODO(pmattis): Do we have to worry about persisting the milliseconds value
	// periodically to avoid the clock ever going backwards (e.g. due to NTP
	// adjustment)?
	nanos := uint64(time.Now().UnixNano())
	uniqueBytesState.Lock()
	if nanos <= uniqueBytesState.nanos {
		nanos = uniqueBytesState.nanos + 1
	}
	uniqueBytesState.nanos = nanos
	uniqueBytesState.Unlock()

	b := make([]byte, 0, 8+binary.MaxVarintLen32)
	b = encoding.EncodeUint64Ascending(b, nanos)
	// We use binary.PutUvarint instead of encoding.EncodeUvarint because the
	// former uses less space for values < 128 which is a common occurrence for
	// node IDs.
	n := binary.PutUvarint(b[len(b):len(b)+binary.MaxVarintLen32], uint64(nodeID))
	return DBytes(b[:len(b)+n])
}

var uniqueIntState struct {
	sync.Mutex
	timestamp uint64
}

var uniqueIDEpoch = time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano()

func generateUniqueInt(nodeID roachpb.NodeID) DInt {
	// Unique ints are composed of the current time at a 10-microsecond
	// granularity and the node-id. The node-id is stored in the lower 15 bits of
	// the returned value and the timestamp is stored in the upper 48 bits. The
	// top-bit is left empty so that negative values are not returned. The 48-bit
	// timestamp field provides for 89 years of timestamps. We use a custom epoch
	// (Jan 1, 2015) in order to utilize the entire timestamp range.
	//
	// Note that generateUniqueInt() imposes a limit on node IDs while
	// generateUniqueBytes() does not.
	//
	// TODO(pmattis): Do we have to worry about persisting the milliseconds value
	// periodically to avoid the clock ever going backwards (e.g. due to NTP
	// adjustment)?
	const precision = uint64(10 * time.Microsecond)
	const nodeIDBits = 15

	id := uint64(time.Now().UnixNano()-uniqueIDEpoch) / precision

	uniqueIntState.Lock()
	if id <= uniqueIntState.timestamp {
		id = uniqueIntState.timestamp + 1
	}
	uniqueIntState.timestamp = id
	uniqueIntState.Unlock()

	id = (id << nodeIDBits) | uint64(nodeID)
	return DInt(id)
}

// uniqueRowID returns a unique integer. It has same properties as
// generateUniqueInt, except with its bits reversed so that the high bits
// change instead of the lows. This makes multiple calls to this function
// in a short amount of time generate IDs that will end up in differing ranges.
func uniqueRowID(nodeID roachpb.NodeID) DInt {
	i := generateUniqueInt(nodeID)
	return DInt(reverseBits(int64(i)))
}

func reverseBits(v int64) int64 {
	return int64(bitReverseTable[byte(v>>56)]) |
		int64(bitReverseTable[byte(v>>48)])<<8 |
		int64(bitReverseTable[byte(v>>40)])<<16 |
		int64(bitReverseTable[byte(v>>32)])<<24 |
		int64(bitReverseTable[byte(v>>24)])<<32 |
		int64(bitReverseTable[byte(v>>16)])<<40 |
		int64(bitReverseTable[byte(v>>8)])<<48 |
		int64(bitReverseTable[byte(v)])<<56
}

var bitReverseTable = [...]byte{
	0x00, 0x80, 0x40, 0xC0, 0x20, 0xA0, 0x60, 0xE0, 0x10, 0x90, 0x50, 0xD0, 0x30, 0xB0, 0x70, 0xF0,
	0x08, 0x88, 0x48, 0xC8, 0x28, 0xA8, 0x68, 0xE8, 0x18, 0x98, 0x58, 0xD8, 0x38, 0xB8, 0x78, 0xF8,
	0x04, 0x84, 0x44, 0xC4, 0x24, 0xA4, 0x64, 0xE4, 0x14, 0x94, 0x54, 0xD4, 0x34, 0xB4, 0x74, 0xF4,
	0x0C, 0x8C, 0x4C, 0xCC, 0x2C, 0xAC, 0x6C, 0xEC, 0x1C, 0x9C, 0x5C, 0xDC, 0x3C, 0xBC, 0x7C, 0xFC,
	0x02, 0x82, 0x42, 0xC2, 0x22, 0xA2, 0x62, 0xE2, 0x12, 0x92, 0x52, 0xD2, 0x32, 0xB2, 0x72, 0xF2,
	0x0A, 0x8A, 0x4A, 0xCA, 0x2A, 0xAA, 0x6A, 0xEA, 0x1A, 0x9A, 0x5A, 0xDA, 0x3A, 0xBA, 0x7A, 0xFA,
	0x06, 0x86, 0x46, 0xC6, 0x26, 0xA6, 0x66, 0xE6, 0x16, 0x96, 0x56, 0xD6, 0x36, 0xB6, 0x76, 0xF6,
	0x0E, 0x8E, 0x4E, 0xCE, 0x2E, 0xAE, 0x6E, 0xEE, 0x1E, 0x9E, 0x5E, 0xDE, 0x3E, 0xBE, 0x7E, 0xFE,
	0x01, 0x81, 0x41, 0xC1, 0x21, 0xA1, 0x61, 0xE1, 0x11, 0x91, 0x51, 0xD1, 0x31, 0xB1, 0x71, 0xF1,
	0x09, 0x89, 0x49, 0xC9, 0x29, 0xA9, 0x69, 0xE9, 0x19, 0x99, 0x59, 0xD9, 0x39, 0xB9, 0x79, 0xF9,
	0x05, 0x85, 0x45, 0xC5, 0x25, 0xA5, 0x65, 0xE5, 0x15, 0x95, 0x55, 0xD5, 0x35, 0xB5, 0x75, 0xF5,
	0x0D, 0x8D, 0x4D, 0xCD, 0x2D, 0xAD, 0x6D, 0xED, 0x1D, 0x9D, 0x5D, 0xDD, 0x3D, 0xBD, 0x7D, 0xFD,
	0x03, 0x83, 0x43, 0xC3, 0x23, 0xA3, 0x63, 0xE3, 0x13, 0x93, 0x53, 0xD3, 0x33, 0xB3, 0x73, 0xF3,
	0x0B, 0x8B, 0x4B, 0xCB, 0x2B, 0xAB, 0x6B, 0xEB, 0x1B, 0x9B, 0x5B, 0xDB, 0x3B, 0xBB, 0x7B, 0xFB,
	0x07, 0x87, 0x47, 0xC7, 0x27, 0xA7, 0x67, 0xE7, 0x17, 0x97, 0x57, 0xD7, 0x37, 0xB7, 0x77, 0xF7,
	0x0F, 0x8F, 0x4F, 0xCF, 0x2F, 0xAF, 0x6F, 0xEF, 0x1F, 0x9F, 0x5F, 0xDF, 0x3F, 0xBF, 0x7F, 0xFF,
}
