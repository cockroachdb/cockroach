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

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/decimal"
	"github.com/cockroachdb/cockroach/util/duration"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/timeutil"
	"github.com/cockroachdb/cockroach/util/uuid"
)

var (
	errEmptyInputString  = errors.New("the input string must not be empty")
	errAbsOfMinInt64     = errors.New("abs of min integer value (-9223372036854775808) not defined")
	errRoundNumberDigits = errors.New("number of digits must be greater than 0")
	errSqrtOfNegNumber   = errors.New("cannot take square root of a negative number")
	errLogOfNegNumber    = errors.New("cannot take logarithm of a negative number")
	errLogOfZero         = errors.New("cannot take logarithm of zero")
)

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

	"octet_length": {
		stringBuiltin1(func(s string) (Datum, error) {
			return DInt(len(s)), nil
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

	"unique_rowid": {
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
				return DBytes(uuid.NewV4().GetBytes()), nil
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
				nanos := e.GetTxnTimestamp().GoTime().Sub(args[0].(DTimestamp).Time).Nanoseconds()
				return DInterval{Duration: duration.Duration{Nanos: nanos}}, nil
			},
		},
		builtin{
			types:      argTypes{timestampType, timestampType},
			returnType: typeInterval,
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
				nanos := args[0].(DTimestamp).Sub(args[1].(DTimestamp).Time).Nanoseconds()
				return DInterval{Duration: duration.Duration{Nanos: nanos}}, nil
			},
		},
	},

	"current_date": {
		builtin{
			types:      argTypes{},
			returnType: typeDate,
			fn: func(e EvalContext, args DTuple) (Datum, error) {
				return e.makeDDate(e.GetTxnTimestamp().GoTime())
			},
		},
	},

	"now":                   {txnTSImpl},
	"current_timestamp":     {txnTSImpl},
	"transaction_timestamp": {txnTSImpl},
	"transaction_timestamp_unique": {
		builtin{
			types:      argTypes{},
			returnType: typeInt,
			impure:     true,
			fn: func(e EvalContext, args DTuple) (Datum, error) {
				return DInt(e.GetTxnTimestamp().Logical), nil
			},
		},
	},

	"statement_timestamp": {stmtTSImpl},

	"clock_timestamp": {
		builtin{
			types:      argTypes{},
			returnType: typeTimestamp,
			impure:     true,
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
				return DTimestamp{Time: timeutil.Now()}, nil
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
				case "year", "years":
					return DInt(fromTime.Year()), nil

				case "quarter":
					return DInt(fromTime.Month()/4 + 1), nil

				case "month", "months":
					return DInt(fromTime.Month()), nil

				case "week", "weeks":
					_, week := fromTime.ISOWeek()
					return DInt(week), nil

				case "day", "days":
					return DInt(fromTime.Day()), nil

				case "dayofweek", "dow":
					return DInt(fromTime.Weekday()), nil

				case "dayofyear", "doy":
					return DInt(fromTime.YearDay()), nil

				case "hour", "hours":
					return DInt(fromTime.Hour()), nil

				case "minute", "minutes":
					return DInt(fromTime.Minute()), nil

				case "second", "seconds":
					return DInt(fromTime.Second()), nil

				case "millisecond", "milliseconds":
					// This a PG extension not supported in MySQL.
					return DInt(fromTime.Nanosecond() / int(time.Millisecond)), nil

				case "microsecond", "microseconds":
					return DInt(fromTime.Nanosecond() / int(time.Microsecond)), nil

				case "nanosecond", "nanoseconds":
					// This is a CockroachDB extension.
					return DInt(fromTime.Nanosecond()), nil

				case "epoch_nanosecond", "epoch_nanoseconds":
					// This is a CockroachDB extension.
					return DInt(fromTime.UnixNano()), nil

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
			returnType: typeDecimal,
		},
		builtin{
			types:      argTypes{decimalType},
			returnType: typeDecimal,
		},
		builtin{
			types:      argTypes{floatType},
			returnType: typeFloat,
		},
	},

	"stddev": {
		builtin{
			types:      argTypes{intType},
			returnType: typeDecimal,
		},
		builtin{
			types:      argTypes{decimalType},
			returnType: typeDecimal,
		},
		builtin{
			types:      argTypes{floatType},
			returnType: typeFloat,
		},
	},

	// Math functions

	"abs": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(math.Abs(x)), nil
		}),
		decimalBuiltin1(func(x *inf.Dec) (Datum, error) {
			dd := &DDecimal{}
			dd.Abs(x)
			return dd, nil
		}),
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

	"cbrt": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(math.Cbrt(x)), nil
		}),
		decimalBuiltin1(func(x *inf.Dec) (Datum, error) {
			dd := &DDecimal{}
			decimal.Cbrt(&dd.Dec, x, decimal.Precision)
			return dd, nil
		}),
	},

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
		decimalBuiltin2(func(x, y *inf.Dec) (Datum, error) {
			if y.Sign() == 0 {
				return nil, errDivByZero
			}
			dd := &DDecimal{}
			dd.QuoRound(x, y, 0, inf.RoundDown)
			return dd, nil
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
		decimalBuiltin1(func(x *inf.Dec) (Datum, error) {
			dd := &DDecimal{}
			dd.Round(x, 0, inf.RoundFloor)
			return dd, nil
		}),
	},

	"ln": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(math.Log(x)), nil
		}),
		decimalLogFn(decimal.Log),
	},

	"log": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return DFloat(math.Log10(x)), nil
		}),
		decimalLogFn(decimal.Log10),
	},

	"mod": {
		floatBuiltin2(func(x, y float64) (Datum, error) {
			return DFloat(math.Mod(x, y)), nil
		}),
		decimalBuiltin2(func(x, y *inf.Dec) (Datum, error) {
			if y.Sign() == 0 {
				return nil, errZeroModulus
			}
			dd := &DDecimal{}
			decimal.Mod(&dd.Dec, x, y)
			return dd, nil
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
		decimalBuiltin1(func(x *inf.Dec) (Datum, error) {
			dd := &DDecimal{}
			dd.Round(x, 0, inf.RoundHalfUp)
			return dd, nil
		}),
		builtin{
			returnType: typeFloat,
			types:      argTypes{floatType, intType},
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
				return round(float64(args[0].(DFloat)), int64(args[1].(DInt)))
			},
		},
		builtin{
			returnType: typeDecimal,
			types:      argTypes{decimalType, intType},
			fn: func(_ EvalContext, args DTuple) (Datum, error) {
				dec := args[0].(*DDecimal)
				dd := &DDecimal{}
				dd.Round(&dec.Dec, inf.Scale(args[1].(DInt)), inf.RoundHalfUp)
				return dd, nil
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
		decimalBuiltin1(func(x *inf.Dec) (Datum, error) {
			return DFloat(x.Sign()), nil
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

	"sqrt": {
		floatBuiltin1(func(x float64) (Datum, error) {
			if x < 0 {
				return nil, errSqrtOfNegNumber
			}
			return DFloat(math.Sqrt(x)), nil
		}),
		decimalBuiltin1(func(x *inf.Dec) (Datum, error) {
			if x.Sign() < 0 {
				return nil, errSqrtOfNegNumber
			}
			dd := &DDecimal{}
			decimal.Sqrt(&dd.Dec, x, decimal.Precision)
			return dd, nil
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
		decimalBuiltin1(func(x *inf.Dec) (Datum, error) {
			dd := &DDecimal{}
			dd.Round(x, 0, inf.RoundDown)
			return dd, nil
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
			impure:     true, // COUNT(1) is not a const. #5170.
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
	decimalBuiltin1(func(x *inf.Dec) (Datum, error) {
		dd := &DDecimal{}
		dd.Round(x, 0, inf.RoundCeil)
		return dd, nil
	}),
}

var stmtTSImpl = builtin{
	types:      argTypes{},
	returnType: typeTimestamp,
	fn: func(e EvalContext, args DTuple) (Datum, error) {
		return e.GetStmtTimestamp(), nil
	},
}

var txnTSImpl = builtin{
	types:      argTypes{},
	returnType: typeTimestamp,
	impure:     true,
	fn: func(e EvalContext, args DTuple) (Datum, error) {
		return DTimestamp{Time: e.GetTxnTimestamp().GoTime()}, nil
	},
}

// TODO(nvanbenschoten) Add native support for decimal.
var powImpls = floatOrDecimalBuiltin2(func(x, y float64) (Datum, error) {
	return DFloat(math.Pow(x, y)), nil
})

func decimalLogFn(logFn func(*inf.Dec, *inf.Dec, inf.Scale) *inf.Dec) builtin {
	return decimalBuiltin1(func(x *inf.Dec) (Datum, error) {
		switch x.Sign() {
		case -1:
			return nil, errLogOfNegNumber
		case 0:
			return nil, errLogOfZero
		}
		dd := &DDecimal{}
		logFn(&dd.Dec, x, decimal.Precision)
		return dd, nil
	})
}

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

func decimalBuiltin1(f func(*inf.Dec) (Datum, error)) builtin {
	return builtin{
		types:      argTypes{decimalType},
		returnType: typeDecimal,
		fn: func(_ EvalContext, args DTuple) (Datum, error) {
			dec := args[0].(*DDecimal)
			return f(&dec.Dec)
		},
	}
}

func decimalBuiltin2(f func(*inf.Dec, *inf.Dec) (Datum, error)) builtin {
	return builtin{
		types:      argTypes{decimalType, decimalType},
		returnType: typeDecimal,
		fn: func(_ EvalContext, args DTuple) (Datum, error) {
			dec1 := args[0].(*DDecimal)
			dec2 := args[1].(*DDecimal)
			return f(&dec1.Dec, &dec2.Dec)
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
				dec := args[0].(*DDecimal)
				v, err := decimal.Float64FromDec(&dec.Dec)
				if err != nil {
					return nil, err
				}
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
				dd := &DDecimal{}
				decimal.SetFromFloat(&dd.Dec, rf)
				return dd, nil
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
				dec1 := args[0].(*DDecimal)
				v1, err := decimal.Float64FromDec(&dec1.Dec)
				if err != nil {
					return nil, err
				}
				dec2 := args[1].(*DDecimal)
				v2, err := decimal.Float64FromDec(&dec2.Dec)
				if err != nil {
					return nil, err
				}
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
				dd := &DDecimal{}
				decimal.SetFromFloat(&dd.Dec, rf)
				return dd, nil
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
	// timeutil.Now() the nanoseconds value is incremented. The node-id is varint
	// encoded. Since node-ids are allocated consecutively starting at 1, the
	// node-id field will consume 1 or 2 bytes for any reasonably sized cluster.
	//
	// TODO(pmattis): Do we have to worry about persisting the milliseconds value
	// periodically to avoid the clock ever going backwards (e.g. due to NTP
	// adjustment)?
	nanos := uint64(timeutil.Now().UnixNano())
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

var uniqueIntEpoch = time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano()

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

	nowNanos := timeutil.Now().UnixNano()
	// Paranoia: nowNanos should never be less than uniqueIntEpoch.
	if nowNanos < uniqueIntEpoch {
		nowNanos = uniqueIntEpoch
	}
	id := uint64(nowNanos-uniqueIntEpoch) / precision

	uniqueIntState.Lock()
	if id <= uniqueIntState.timestamp {
		id = uniqueIntState.timestamp + 1
	}
	uniqueIntState.timestamp = id
	uniqueIntState.Unlock()

	// We xor in the nodeID so that nodeIDs larger than 32K will flip bits in the
	// timestamp portion of the final value instead of always setting them.
	id = (id << nodeIDBits) ^ uint64(nodeID)
	return DInt(id)
}
