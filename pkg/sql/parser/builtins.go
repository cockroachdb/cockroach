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
	"regexp"
	"regexp/syntax"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"gopkg.in/inf.v0"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/decimal"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

var (
	errEmptyInputString = errors.New("the input string must not be empty")
	errAbsOfMinInt64    = errors.New("abs of min integer value (-9223372036854775808) not defined")
	errRoundTooLow      = errors.New("rounding would extend value by more than 2000 decimal digits")
	errArgTooBig        = errors.New("argument value is too large")
	errSqrtOfNegNumber  = errors.New("cannot take square root of a negative number")
	errLogOfNegNumber   = errors.New("cannot take logarithm of a negative number")
	errLogOfZero        = errors.New("cannot take logarithm of zero")
)

// FunctionClass specifies the class of the builtin function.
type FunctionClass int

const (
	// NormalClass is a standard builtin function.
	NormalClass FunctionClass = iota
	// AggregateClass is a builtin aggregate function.
	AggregateClass
	// WindowClass is a builtin window function.
	WindowClass
)

// Avoid vet warning about unused enum value.
var _ = NormalClass

const (
	categoryIDGeneration = "ID Generation"
	categorySystemInfo   = "System Info"
	categoryDateAndTime  = "Date and Time"
	categoryString       = "String and Byte"
	categoryMath         = "Math and Numeric"
	categoryComparison   = "Comparison"
)

// Builtin is a built-in function.
type Builtin struct {
	Types      typeList
	ReturnType Type

	// When multiple overloads are eligible based on types even after all of of
	// the heuristics to pick one have been used, if one of the overloads is a
	// Builtin with the `preferredOverload` flag set to true it can be selected
	// rather than returning a no-such-method error.
	// This should generally be avoided -- avoiding introducing ambiguous
	// overloads in the first place is a much better solution -- and only done
	// after consultation with @knz @nvanbenschoten.
	preferredOverload bool

	// Set to true when a function potentially returns a different value
	// when called in the same statement with the same parameters.
	// e.g.: random(), clock_timestamp(). Some functions like now()
	// return the same value in the same statement, but different values
	// in separate statements, and should not be marked as impure.
	impure         bool
	class          FunctionClass
	category, Info string

	AggregateFunc func() AggregateFunc
	WindowFunc    func() WindowFunc
	fn            func(*EvalContext, DTuple) (Datum, error)
}

func (b Builtin) params() typeList {
	return b.Types
}

func (b Builtin) returnType() Type {
	return b.ReturnType
}

func (b Builtin) preferred() bool {
	return b.preferredOverload
}

func categorizeType(t Type) string {
	switch t {
	case TypeDate, TypeInterval, TypeTimestamp, TypeTimestampTZ:
		return categoryDateAndTime
	case TypeInt, TypeDecimal, TypeFloat:
		return categoryMath
	case TypeString, TypeBytes:
		return categoryString
	default:
		return strings.ToUpper(t.String())
	}
}

// Category is used to categorize a function (for documentation purposes).
func (b Builtin) Category() string {
	if b.category != "" {
		return b.category
	}
	if types, ok := b.Types.(ArgTypes); ok && len(types) == 1 {
		return categorizeType(types[0])
	}
	if b.ReturnType != nil {
		return categorizeType(b.ReturnType)
	}
	return ""
}

// Signature returns a human-readable signature
func (b Builtin) Signature() string {
	if b.ReturnType == nil {
		return "<T>... -> <T>" // Special-case for LEAST and GREATEST.
	}
	return fmt.Sprintf("(%s) -> %s", b.Types.String(), b.ReturnType)
}

// Builtins contains the built-in functions indexed by name.
var Builtins = map[string][]Builtin{
	// Keep the list of functions sorted.

	// TODO(XisiHuang): support encoding, i.e., length(str, encoding).
	"length": {
		stringBuiltin1(func(s string) (Datum, error) {
			return NewDInt(DInt(utf8.RuneCountInString(s))), nil
		}, TypeInt),
		bytesBuiltin1(func(s string) (Datum, error) {
			return NewDInt(DInt(len(s))), nil
		}, TypeInt),
	},

	"octet_length": {
		stringBuiltin1(func(s string) (Datum, error) {
			return NewDInt(DInt(len(s))), nil
		}, TypeInt),
		bytesBuiltin1(func(s string) (Datum, error) {
			return NewDInt(DInt(len(s))), nil
		}, TypeInt),
	},

	// TODO(pmattis): What string functions should also support TypeBytes?

	"lower": {stringBuiltin1(func(s string) (Datum, error) {
		return NewDString(strings.ToLower(s)), nil
	}, TypeString)},

	"upper": {stringBuiltin1(func(s string) (Datum, error) {
		return NewDString(strings.ToUpper(s)), nil
	}, TypeString)},

	"substr":    substringImpls,
	"substring": substringImpls,

	// concat concatenates the text representations of all the arguments.
	// NULL arguments are ignored.
	"concat": {
		Builtin{
			Types:      VariadicType{TypeString},
			ReturnType: TypeString,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				var buffer bytes.Buffer
				for _, d := range args {
					if d == DNull {
						continue
					}
					buffer.WriteString(string(*d.(*DString)))
				}
				return NewDString(buffer.String()), nil
			},
		},
	},

	"concat_ws": {
		Builtin{
			Types:      VariadicType{TypeString},
			ReturnType: TypeString,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				dstr, ok := args[0].(*DString)
				if !ok {
					return DNull, fmt.Errorf("unknown signature for concat_ws: concat_ws(%s, ...)", args[0])
				}
				sep := string(*dstr)
				var ss []string
				for _, d := range args[1:] {
					if d == DNull {
						continue
					}
					ss = append(ss, string(*d.(*DString)))
				}
				return NewDString(strings.Join(ss, sep)), nil
			},
		},
	},

	"split_part": {
		Builtin{
			Types:      ArgTypes{TypeString, TypeString, TypeInt},
			ReturnType: TypeString,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				text := string(*args[0].(*DString))
				sep := string(*args[1].(*DString))
				field := int(*args[2].(*DInt))

				if field <= 0 {
					return DNull, fmt.Errorf("field position %d must be greater than zero", field)
				}

				splits := strings.Split(text, sep)
				if field > len(splits) {
					return NewDString(""), nil
				}
				return NewDString(splits[field-1]), nil
			},
		},
	},

	"repeat": {
		Builtin{
			Types:      ArgTypes{TypeString, TypeInt},
			ReturnType: TypeString,
			fn: func(_ *EvalContext, args DTuple) (_ Datum, err error) {
				s := string(*args[0].(*DString))
				count := int(*args[1].(*DInt))
				if count < 0 {
					count = 0
				}
				// Repeat can overflow if len(s) * count is very large. The computation
				// for the limit about what make can allocate is not trivial, so it's most
				// accurate to detect it with a recover.
				defer func() {
					if r := recover(); r != nil {
						err = fmt.Errorf("%s", r)
					}
				}()
				return NewDString(strings.Repeat(s, count)), nil
			},
		},
	},

	"ascii": {stringBuiltin1(func(s string) (Datum, error) {
		for _, ch := range s {
			return NewDInt(DInt(ch)), nil
		}
		return nil, errEmptyInputString
	}, TypeInt)},

	"md5": {stringBuiltin1(func(s string) (Datum, error) {
		return NewDString(fmt.Sprintf("%x", md5.Sum([]byte(s)))), nil
	}, TypeString)},

	"sha1": {stringBuiltin1(func(s string) (Datum, error) {
		return NewDString(fmt.Sprintf("%x", sha1.Sum([]byte(s)))), nil
	}, TypeString)},

	"sha256": {stringBuiltin1(func(s string) (Datum, error) {
		return NewDString(fmt.Sprintf("%x", sha256.Sum256([]byte(s)))), nil
	}, TypeString)},

	"to_hex": {
		Builtin{
			Types:      ArgTypes{TypeInt},
			ReturnType: TypeString,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				return NewDString(fmt.Sprintf("%x", int64(*args[0].(*DInt)))), nil
			},
		},
	},

	// The SQL parser coerces POSITION to STRPOS.
	"strpos": {stringBuiltin2(func(s, substring string) (Datum, error) {
		index := strings.Index(s, substring)
		if index < 0 {
			return NewDInt(0), nil
		}

		return NewDInt(DInt(utf8.RuneCountInString(s[:index]) + 1)), nil
	}, TypeInt)},

	"overlay": {
		Builtin{
			Types:      ArgTypes{TypeString, TypeString, TypeInt},
			ReturnType: TypeString,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				s := string(*args[0].(*DString))
				to := string(*args[1].(*DString))
				pos := int(*args[2].(*DInt))
				size := utf8.RuneCountInString(to)
				return overlay(s, to, pos, size)
			},
		},
		Builtin{
			Types:      ArgTypes{TypeString, TypeString, TypeInt, TypeInt},
			ReturnType: TypeString,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				s := string(*args[0].(*DString))
				to := string(*args[1].(*DString))
				pos := int(*args[2].(*DInt))
				size := int(*args[3].(*DInt))
				return overlay(s, to, pos, size)
			},
		},
	},

	// The SQL parser coerces TRIM(...) and TRIM(BOTH ...) to BTRIM(...).
	"btrim": {
		stringBuiltin2(func(s, chars string) (Datum, error) {
			return NewDString(strings.Trim(s, chars)), nil
		}, TypeString),
		stringBuiltin1(func(s string) (Datum, error) {
			return NewDString(strings.TrimSpace(s)), nil
		}, TypeString),
	},

	// The SQL parser coerces TRIM(LEADING ...) to LTRIM(...).
	"ltrim": {
		stringBuiltin2(func(s, chars string) (Datum, error) {
			return NewDString(strings.TrimLeft(s, chars)), nil
		}, TypeString),
		stringBuiltin1(func(s string) (Datum, error) {
			return NewDString(strings.TrimLeftFunc(s, unicode.IsSpace)), nil
		}, TypeString),
	},

	// The SQL parser coerces TRIM(TRAILING ...) to RTRIM(...).
	"rtrim": {
		stringBuiltin2(func(s, chars string) (Datum, error) {
			return NewDString(strings.TrimRight(s, chars)), nil
		}, TypeString),
		stringBuiltin1(func(s string) (Datum, error) {
			return NewDString(strings.TrimRightFunc(s, unicode.IsSpace)), nil
		}, TypeString),
	},

	"reverse": {stringBuiltin1(func(s string) (Datum, error) {
		runes := []rune(s)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return NewDString(string(runes)), nil
	}, TypeString)},

	"replace": {stringBuiltin3(
		"input", "from", "to",
		func(input, from, to string) (Datum, error) {
			return NewDString(strings.Replace(input, from, to, -1)), nil
		},
		TypeString,
		"Replace all occurrences of 'from' with 'to' in 'input'",
	)},

	"translate": {stringBuiltin3(
		"input", "from", "to",
		func(s, from, to string) (Datum, error) {
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
			return NewDString(string(runes)), nil
		}, TypeString, "")},

	"regexp_extract": {
		Builtin{
			Types:      ArgTypes{TypeString, TypeString},
			ReturnType: TypeString,
			fn: func(ctx *EvalContext, args DTuple) (Datum, error) {
				s := string(*args[0].(*DString))
				pattern := string(*args[1].(*DString))
				return regexpExtract(ctx, s, pattern, `\`)
			},
		},
	},

	"regexp_replace": {
		Builtin{
			Types:      ArgTypes{TypeString, TypeString, TypeString},
			ReturnType: TypeString,
			fn: func(ctx *EvalContext, args DTuple) (Datum, error) {
				s := string(*args[0].(*DString))
				pattern := string(*args[1].(*DString))
				to := string(*args[2].(*DString))
				return regexpReplace(ctx, s, pattern, to, "")
			},
		},
		Builtin{
			Types:      ArgTypes{TypeString, TypeString, TypeString, TypeString},
			ReturnType: TypeString,
			fn: func(ctx *EvalContext, args DTuple) (Datum, error) {
				s := string(*args[0].(*DString))
				pattern := string(*args[1].(*DString))
				to := string(*args[2].(*DString))
				sqlFlags := string(*args[3].(*DString))
				return regexpReplace(ctx, s, pattern, to, sqlFlags)
			},
		},
	},

	"initcap": {stringBuiltin1(func(s string) (Datum, error) {
		return NewDString(strings.Title(strings.ToLower(s))), nil
	}, TypeString)},

	"left": {
		Builtin{
			Types:      ArgTypes{TypeBytes, TypeInt},
			ReturnType: TypeBytes,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				bytes := []byte(*args[0].(*DBytes))
				n := int(*args[1].(*DInt))

				if n < -len(bytes) {
					n = 0
				} else if n < 0 {
					n = len(bytes) + n
				} else if n > len(bytes) {
					n = len(bytes)
				}
				return NewDBytes(DBytes(bytes[:n])), nil
			},
		},
		Builtin{
			Types:      ArgTypes{TypeString, TypeInt},
			ReturnType: TypeString,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				runes := []rune(string(*args[0].(*DString)))
				n := int(*args[1].(*DInt))

				if n < -len(runes) {
					n = 0
				} else if n < 0 {
					n = len(runes) + n
				} else if n > len(runes) {
					n = len(runes)
				}
				return NewDString(string(runes[:n])), nil
			},
		},
	},

	"right": {
		Builtin{
			Types:      ArgTypes{TypeBytes, TypeInt},
			ReturnType: TypeBytes,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				bytes := []byte(*args[0].(*DBytes))
				n := int(*args[1].(*DInt))

				if n < -len(bytes) {
					n = 0
				} else if n < 0 {
					n = len(bytes) + n
				} else if n > len(bytes) {
					n = len(bytes)
				}
				return NewDBytes(DBytes(bytes[len(bytes)-n:])), nil
			},
		},
		Builtin{
			Types:      ArgTypes{TypeString, TypeInt},
			ReturnType: TypeString,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				runes := []rune(string(*args[0].(*DString)))
				n := int(*args[1].(*DInt))

				if n < -len(runes) {
					n = 0
				} else if n < 0 {
					n = len(runes) + n
				} else if n > len(runes) {
					n = len(runes)
				}
				return NewDString(string(runes[len(runes)-n:])), nil
			},
		},
	},

	"random": {
		Builtin{
			Types:      ArgTypes{},
			ReturnType: TypeFloat,
			impure:     true,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				return NewDFloat(DFloat(rand.Float64())), nil
			},
		},
	},

	"experimental_unique_bytes": {
		Builtin{
			Types:      ArgTypes{},
			ReturnType: TypeBytes,
			category:   categoryIDGeneration,
			impure:     true,
			fn: func(ctx *EvalContext, args DTuple) (Datum, error) {
				return NewDBytes(generateUniqueBytes(ctx.NodeID)), nil
			},
		},
	},

	"unique_rowid": {
		Builtin{
			Types:      ArgTypes{},
			ReturnType: TypeInt,
			category:   categoryIDGeneration,
			impure:     true,
			fn: func(ctx *EvalContext, args DTuple) (Datum, error) {
				return NewDInt(generateUniqueInt(ctx.NodeID)), nil
			},
		},
	},

	"experimental_uuid_v4": {uuidV4Impl},
	"uuid_v4":              {uuidV4Impl},

	"greatest": {
		Builtin{
			Types:      AnyType{},
			ReturnType: nil, // No explicit return type because AnyType parameters.
			category:   categoryComparison,
			fn: func(ctx *EvalContext, args DTuple) (Datum, error) {
				return pickFromTuple(ctx, true /* greatest */, args)
			},
		},
	},

	"least": {
		Builtin{
			Types:      AnyType{},
			ReturnType: nil, // No explicit return type because AnyType parameters.
			category:   categoryComparison,
			fn: func(ctx *EvalContext, args DTuple) (Datum, error) {
				return pickFromTuple(ctx, false /* !greatest */, args)
			},
		},
	},

	// Timestamp/Date functions.

	"experimental_strftime": {
		Builtin{
			Types:      ArgTypes{TypeTimestamp, TypeString},
			ReturnType: TypeString,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				fromTime := args[0].(*DTimestamp).Time
				format := string(*args[1].(*DString))
				t, err := timeutil.Strftime(fromTime, format)
				if err != nil {
					return nil, err
				}
				return NewDString(t), nil
			},
		},
		Builtin{
			Types:      ArgTypes{TypeDate, TypeString},
			ReturnType: TypeString,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				fromTime := time.Unix(int64(*args[0].(*DDate))*secondsInDay, 0).UTC()
				format := string(*args[1].(*DString))
				t, err := timeutil.Strftime(fromTime, format)
				if err != nil {
					return nil, err
				}
				return NewDString(t), nil
			},
		},
		Builtin{
			Types:      ArgTypes{TypeTimestampTZ, TypeString},
			ReturnType: TypeString,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				fromTime := args[0].(*DTimestampTZ).Time
				format := string(*args[1].(*DString))
				t, err := timeutil.Strftime(fromTime, format)
				if err != nil {
					return nil, err
				}
				return NewDString(t), nil
			},
		},
	},

	"experimental_strptime": {
		Builtin{
			Types:      ArgTypes{TypeString, TypeString},
			ReturnType: TypeTimestampTZ,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				format := string(*args[0].(*DString))
				toParse := string(*args[1].(*DString))
				t, err := timeutil.Strptime(toParse, format)
				if err != nil {
					return nil, err
				}
				return MakeDTimestampTZ(t.UTC(), time.Microsecond), nil
			},
		},
	},

	"age": {
		Builtin{
			Types:      ArgTypes{TypeTimestampTZ},
			ReturnType: TypeInterval,
			fn: func(ctx *EvalContext, args DTuple) (Datum, error) {
				return timestampMinusBinOp.fn(ctx, ctx.GetTxnTimestamp(time.Microsecond), args[0])
			},
		},
		Builtin{
			Types:      ArgTypes{TypeTimestampTZ, TypeTimestampTZ},
			ReturnType: TypeInterval,
			fn: func(ctx *EvalContext, args DTuple) (Datum, error) {
				return timestampMinusBinOp.fn(ctx, args[0], args[1])
			},
		},
	},

	"current_date": {
		Builtin{
			Types:      ArgTypes{},
			ReturnType: TypeDate,
			fn: func(ctx *EvalContext, args DTuple) (Datum, error) {
				t := ctx.GetTxnTimestamp(time.Microsecond).Time
				return NewDDateFromTime(t, ctx.GetLocation()), nil
			},
		},
	},

	"now":                   txnTSImpl,
	"current_timestamp":     txnTSImpl,
	"transaction_timestamp": txnTSImpl,

	"statement_timestamp": {
		Builtin{
			Types:             ArgTypes{},
			ReturnType:        TypeTimestampTZ,
			preferredOverload: true,
			impure:            true,
			fn: func(ctx *EvalContext, args DTuple) (Datum, error) {
				return MakeDTimestampTZ(ctx.GetStmtTimestamp(), time.Microsecond), nil
			},
		},
		Builtin{
			Types:      ArgTypes{},
			ReturnType: TypeTimestamp,
			impure:     true,
			fn: func(ctx *EvalContext, args DTuple) (Datum, error) {
				return MakeDTimestamp(ctx.GetStmtTimestamp(), time.Microsecond), nil
			},
		},
	},

	"cluster_logical_timestamp": {
		Builtin{
			Types:      ArgTypes{},
			ReturnType: TypeDecimal,
			category:   categorySystemInfo,
			impure:     true,
			fn: func(ctx *EvalContext, args DTuple) (Datum, error) {
				return ctx.GetClusterTimestamp(), nil
			},
		},
	},

	"clock_timestamp": {
		Builtin{
			Types:             ArgTypes{},
			ReturnType:        TypeTimestampTZ,
			preferredOverload: true,
			impure:            true,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				return MakeDTimestampTZ(timeutil.Now(), time.Microsecond), nil
			},
		},
		Builtin{
			Types:      ArgTypes{},
			ReturnType: TypeTimestamp,
			impure:     true,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				return MakeDTimestamp(timeutil.Now(), time.Microsecond), nil
			},
		},
	},

	"extract": {
		Builtin{
			Types:      ArgTypes{TypeString, TypeTimestamp},
			ReturnType: TypeInt,
			category:   categoryDateAndTime,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				// extract timeSpan fromTime.
				fromTime := *args[1].(*DTimestamp)
				timeSpan := strings.ToLower(string(*args[0].(*DString)))
				switch timeSpan {
				case "year", "years":
					return NewDInt(DInt(fromTime.Year())), nil

				case "quarter":
					return NewDInt(DInt(fromTime.Month()/4 + 1)), nil

				case "month", "months":
					return NewDInt(DInt(fromTime.Month())), nil

				case "week", "weeks":
					_, week := fromTime.ISOWeek()
					return NewDInt(DInt(week)), nil

				case "day", "days":
					return NewDInt(DInt(fromTime.Day())), nil

				case "dayofweek", "dow":
					return NewDInt(DInt(fromTime.Weekday())), nil

				case "dayofyear", "doy":
					return NewDInt(DInt(fromTime.YearDay())), nil

				case "hour", "hours":
					return NewDInt(DInt(fromTime.Hour())), nil

				case "minute", "minutes":
					return NewDInt(DInt(fromTime.Minute())), nil

				case "second", "seconds":
					return NewDInt(DInt(fromTime.Second())), nil

				case "millisecond", "milliseconds":
					// This a PG extension not supported in MySQL.
					return NewDInt(DInt(fromTime.Nanosecond() / int(time.Millisecond))), nil

				case "microsecond", "microseconds":
					return NewDInt(DInt(fromTime.Nanosecond() / int(time.Microsecond))), nil

				case "epoch":
					return NewDInt(DInt(fromTime.Unix())), nil

				default:
					return DNull, fmt.Errorf("unsupported timespan: %s", timeSpan)
				}
			},
		},
	},
	"extract_duration": {
		Builtin{
			Types:      ArgTypes{TypeString, TypeInterval},
			ReturnType: TypeInt,
			category:   categoryDateAndTime,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				// extract timeSpan fromTime.
				fromInterval := *args[1].(*DInterval)
				timeSpan := strings.ToLower(string(*args[0].(*DString)))
				switch timeSpan {
				case "hour", "hours":
					return NewDInt(DInt(fromInterval.Nanos / int64(time.Hour))), nil

				case "minute", "minutes":
					return NewDInt(DInt(fromInterval.Nanos / int64(time.Minute))), nil

				case "second", "seconds":
					return NewDInt(DInt(fromInterval.Nanos / int64(time.Second))), nil

				case "millisecond", "milliseconds":
					// This a PG extension not supported in MySQL.
					return NewDInt(DInt(fromInterval.Nanos / int64(time.Millisecond))), nil

				case "microsecond", "microseconds":
					return NewDInt(DInt(fromInterval.Nanos / int64(time.Microsecond))), nil

				default:
					return DNull, fmt.Errorf("unsupported timespan: %s", timeSpan)
				}
			},
		},
	},

	// Math functions

	"abs": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(math.Abs(x))), nil
		}),
		decimalBuiltin1(func(x *inf.Dec) (Datum, error) {
			dd := &DDecimal{}
			dd.Abs(x)
			return dd, nil
		}),
		Builtin{
			Types:      ArgTypes{TypeInt},
			ReturnType: TypeInt,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				x := *args[0].(*DInt)
				switch {
				case x == math.MinInt64:
					return DNull, errAbsOfMinInt64
				case x < 0:
					return NewDInt(-x), nil
				}
				return args[0], nil
			},
		},
	},

	"acos": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(math.Acos(x))), nil
		}),
	},

	"asin": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(math.Asin(x))), nil
		}),
	},

	"atan": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(math.Atan(x))), nil
		}),
	},

	"atan2": {
		floatBuiltin2(func(x, y float64) (Datum, error) {
			return NewDFloat(DFloat(math.Atan2(x, y))), nil
		}),
	},

	"cbrt": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(math.Cbrt(x))), nil
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
			return NewDFloat(DFloat(math.Cos(x))), nil
		}),
	},

	"cot": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(1 / math.Tan(x))), nil
		}),
	},

	// For now, schemas are the same as databases. So, current_schemas returns the
	// current database (if one has been set by the user) and, if the passed in
	// parameter is true, pg_catalog.
	"current_schemas": {
		Builtin{
			Types:      ArgTypes{TypeBool},
			ReturnType: TypeArray,
			fn: func(ctx *EvalContext, args DTuple) (Datum, error) {
				var schemas DArray
				if len(ctx.Database) != 0 {
					schemas = append(schemas, NewDString(ctx.Database))
				}
				if args[0] == DNull {
					return DNull, nil
				}
				showImplicitSchemas := args[0].(*DBool)
				if showImplicitSchemas == DBoolTrue {
					// TODO(cuongdo): This is hack, because we don't yet have a schema
					// search path.
					schemas = append(schemas, NewDString("pg_catalog"))
				}
				return &schemas, nil
			},
		},
	},

	"degrees": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(180.0 * x / math.Pi)), nil
		}),
	},

	"div": {
		floatBuiltin2(func(x, y float64) (Datum, error) {
			return NewDFloat(DFloat(math.Trunc(x / y))), nil
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

	"exp": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(math.Exp(x))), nil
		}),
		decimalBuiltin1(func(x *inf.Dec) (Datum, error) {
			return expDecimal(x)
		}),
	},

	"floor": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(math.Floor(x))), nil
		}),
		decimalBuiltin1(func(x *inf.Dec) (Datum, error) {
			dd := &DDecimal{}
			dd.Round(x, 0, inf.RoundFloor)
			return dd, nil
		}),
	},

	"ln": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(math.Log(x))), nil
		}),
		decimalLogFn(decimal.Log),
	},

	"log": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(math.Log10(x))), nil
		}),
		decimalLogFn(decimal.Log10),
	},

	"mod": {
		floatBuiltin2(func(x, y float64) (Datum, error) {
			return NewDFloat(DFloat(math.Mod(x, y))), nil
		}),
		decimalBuiltin2(func(x, y *inf.Dec) (Datum, error) {
			if y.Sign() == 0 {
				return nil, errZeroModulus
			}
			dd := &DDecimal{}
			decimal.Mod(&dd.Dec, x, y)
			return dd, nil
		}),
		Builtin{
			Types:      ArgTypes{TypeInt, TypeInt},
			ReturnType: TypeInt,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				y := *args[1].(*DInt)
				if y == 0 {
					return DNull, errZeroModulus
				}
				x := *args[0].(*DInt)
				return NewDInt(x % y), nil
			},
		},
	},

	"pi": {
		Builtin{
			Types:      ArgTypes{},
			ReturnType: TypeFloat,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				return NewDFloat(math.Pi), nil
			},
		},
	},

	"pow":   powImpls,
	"power": powImpls,

	"radians": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(x * math.Pi / 180.0)), nil
		}),
	},

	"round": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return round(x, 0)
		}),
		decimalBuiltin1(func(x *inf.Dec) (Datum, error) {
			return roundDecimal(x, 0)
		}),
		Builtin{
			Types:      ArgTypes{TypeFloat, TypeInt},
			ReturnType: TypeFloat,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				return round(float64(*args[0].(*DFloat)), int64(*args[1].(*DInt)))
			},
		},
		Builtin{
			Types:      ArgTypes{TypeDecimal, TypeInt},
			ReturnType: TypeDecimal,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				scale := int64(*args[1].(*DInt))
				return roundDecimal(&args[0].(*DDecimal).Dec, scale)
			},
		},
	},

	"sin": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(math.Sin(x))), nil
		}),
	},

	"sign": {
		floatBuiltin1(func(x float64) (Datum, error) {
			switch {
			case x < 0:
				return NewDFloat(-1), nil
			case x == 0:
				return NewDFloat(0), nil
			}
			return NewDFloat(1), nil
		}),
		decimalBuiltin1(func(x *inf.Dec) (Datum, error) {
			return NewDFloat(DFloat(x.Sign())), nil
		}),
		Builtin{
			Types:      ArgTypes{TypeInt},
			ReturnType: TypeInt,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				x := *args[0].(*DInt)
				switch {
				case x < 0:
					return NewDInt(-1), nil
				case x == 0:
					return NewDInt(0), nil
				}
				return NewDInt(1), nil
			},
		},
	},

	"sqrt": {
		floatBuiltin1(func(x float64) (Datum, error) {
			if x < 0 {
				return nil, errSqrtOfNegNumber
			}
			return NewDFloat(DFloat(math.Sqrt(x))), nil
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
			return NewDFloat(DFloat(math.Tan(x))), nil
		}),
	},

	"trunc": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(math.Trunc(x))), nil
		}),
		decimalBuiltin1(func(x *inf.Dec) (Datum, error) {
			dd := &DDecimal{}
			dd.Round(x, 0, inf.RoundDown)
			return dd, nil
		}),
	},
	"version": {
		Builtin{
			Types:      ArgTypes{},
			ReturnType: TypeString,
			category:   categorySystemInfo,
			fn: func(_ *EvalContext, args DTuple) (Datum, error) {
				return NewDString(build.GetInfo().Short()), nil
			},
		},
	},
}

func init() {
	for k, v := range Builtins {
		Builtins[strings.ToUpper(k)] = v
	}
}

var substringImpls = []Builtin{
	{
		Types:      ArgTypes{TypeString, TypeInt},
		ReturnType: TypeString,
		fn: func(_ *EvalContext, args DTuple) (Datum, error) {
			runes := []rune(string(*args[0].(*DString)))
			// SQL strings are 1-indexed.
			start := int(*args[1].(*DInt)) - 1

			if start < 0 {
				start = 0
			} else if start > len(runes) {
				start = len(runes)
			}

			return NewDString(string(runes[start:])), nil
		},
	},
	{
		Types:      ArgTypes{TypeString, TypeInt, TypeInt},
		ReturnType: TypeString,
		fn: func(_ *EvalContext, args DTuple) (Datum, error) {
			runes := []rune(string(*args[0].(*DString)))
			// SQL strings are 1-indexed.
			start := int(*args[1].(*DInt)) - 1
			length := int(*args[2].(*DInt))

			if length < 0 {
				return DNull, fmt.Errorf("negative substring length %d not allowed", length)
			}

			end := start + length
			// Check for integer overflow.
			if end < start {
				end = len(runes)
			} else if end < 0 {
				end = 0
			} else if end > len(runes) {
				end = len(runes)
			}

			if start < 0 {
				start = 0
			} else if start > len(runes) {
				start = len(runes)
			}

			return NewDString(string(runes[start:end])), nil
		},
	},
	{
		Types:      ArgTypes{TypeString, TypeString},
		ReturnType: TypeString,
		fn: func(ctx *EvalContext, args DTuple) (Datum, error) {
			s := string(*args[0].(*DString))
			pattern := string(*args[1].(*DString))
			return regexpExtract(ctx, s, pattern, `\`)
		},
	},
	{
		Types:      ArgTypes{TypeString, TypeString, TypeString},
		ReturnType: TypeString,
		fn: func(ctx *EvalContext, args DTuple) (Datum, error) {
			s := string(*args[0].(*DString))
			pattern := string(*args[1].(*DString))
			escape := string(*args[2].(*DString))
			return regexpExtract(ctx, s, pattern, escape)
		},
	},
}

var uuidV4Impl = Builtin{
	Types:      ArgTypes{},
	ReturnType: TypeBytes,
	category:   categoryIDGeneration,
	impure:     true,
	fn: func(_ *EvalContext, args DTuple) (Datum, error) {
		return NewDBytes(DBytes(uuid.NewV4().GetBytes())), nil
	},
}

var ceilImpl = []Builtin{
	floatBuiltin1(func(x float64) (Datum, error) {
		return NewDFloat(DFloat(math.Ceil(x))), nil
	}),
	decimalBuiltin1(func(x *inf.Dec) (Datum, error) {
		dd := &DDecimal{}
		dd.Round(x, 0, inf.RoundCeil)
		return dd, nil
	}),
}

var txnTSImpl = []Builtin{
	{
		Types:             ArgTypes{},
		ReturnType:        TypeTimestampTZ,
		preferredOverload: true,
		impure:            true,
		fn: func(ctx *EvalContext, args DTuple) (Datum, error) {
			return ctx.GetTxnTimestamp(time.Microsecond), nil
		},
	},
	{
		Types:      ArgTypes{},
		ReturnType: TypeTimestamp,
		impure:     true,
		fn: func(ctx *EvalContext, args DTuple) (Datum, error) {
			return ctx.GetTxnTimestampNoZone(time.Microsecond), nil
		},
	},
}

var powImpls = []Builtin{
	floatBuiltin2(func(x, y float64) (Datum, error) {
		return NewDFloat(DFloat(math.Pow(x, y))), nil
	}),
	decimalBuiltin2(func(x, y *inf.Dec) (Datum, error) {
		dd := &DDecimal{}
		_, err := decimal.Pow(&dd.Dec, x, y, decimal.Precision)
		return dd, err
	}),
}

func decimalLogFn(logFn func(*inf.Dec, *inf.Dec, inf.Scale) *inf.Dec) Builtin {
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

func floatBuiltin1(f func(float64) (Datum, error)) Builtin {
	return Builtin{
		Types:      ArgTypes{TypeFloat},
		ReturnType: TypeFloat,
		fn: func(_ *EvalContext, args DTuple) (Datum, error) {
			return f(float64(*args[0].(*DFloat)))
		},
	}
}

func floatBuiltin2(f func(float64, float64) (Datum, error)) Builtin {
	return Builtin{
		Types:      ArgTypes{TypeFloat, TypeFloat},
		ReturnType: TypeFloat,
		fn: func(_ *EvalContext, args DTuple) (Datum, error) {
			return f(float64(*args[0].(*DFloat)),
				float64(*args[1].(*DFloat)))
		},
	}
}

func decimalBuiltin1(f func(*inf.Dec) (Datum, error)) Builtin {
	return Builtin{
		Types:      ArgTypes{TypeDecimal},
		ReturnType: TypeDecimal,
		fn: func(_ *EvalContext, args DTuple) (Datum, error) {
			dec := &args[0].(*DDecimal).Dec
			return f(dec)
		},
	}
}

func decimalBuiltin2(f func(*inf.Dec, *inf.Dec) (Datum, error)) Builtin {
	return Builtin{
		Types:      ArgTypes{TypeDecimal, TypeDecimal},
		ReturnType: TypeDecimal,
		fn: func(_ *EvalContext, args DTuple) (Datum, error) {
			dec1 := &args[0].(*DDecimal).Dec
			dec2 := &args[1].(*DDecimal).Dec
			return f(dec1, dec2)
		},
	}
}

func stringBuiltin1(f func(string) (Datum, error), returnType Type) Builtin {
	return Builtin{
		Types:      ArgTypes{TypeString},
		ReturnType: returnType,
		fn: func(_ *EvalContext, args DTuple) (Datum, error) {
			return f(string(*args[0].(*DString)))
		},
	}
}

func stringBuiltin2(f func(string, string) (Datum, error), returnType Type) Builtin {
	return Builtin{
		Types:      ArgTypes{TypeString, TypeString},
		ReturnType: returnType,
		category:   categorizeType(TypeString),
		fn: func(_ *EvalContext, args DTuple) (Datum, error) {
			return f(string(*args[0].(*DString)), string(*args[1].(*DString)))
		},
	}
}

func stringBuiltin3(
	a, b, c string, f func(string, string, string) (Datum, error), returnType Type, info string,
) Builtin {
	return Builtin{
		Types:      NamedArgTypes{{a, TypeString}, {b, TypeString}, {c, TypeString}},
		ReturnType: returnType,
		fn: func(_ *EvalContext, args DTuple) (Datum, error) {
			return f(string(*args[0].(*DString)), string(*args[1].(*DString)), string(*args[2].(*DString)))
		},
		Info: info,
	}
}

func bytesBuiltin1(f func(string) (Datum, error), returnType Type) Builtin {
	return Builtin{
		Types:      ArgTypes{TypeBytes},
		ReturnType: returnType,
		fn: func(_ *EvalContext, args DTuple) (Datum, error) {
			return f(string(*args[0].(*DBytes)))
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

func regexpExtract(ctx *EvalContext, s, pattern, escape string) (Datum, error) {
	patternRe, err := ctx.ReCache.GetRegexp(regexpEscapeKey{pattern, escape})
	if err != nil {
		return nil, err
	}

	match := patternRe.FindStringSubmatch(s)
	if match == nil {
		return DNull, nil
	}

	if len(match) > 1 {
		return NewDString(match[1]), nil
	}
	return NewDString(match[0]), nil
}

type regexpFlagKey struct {
	sqlPattern string
	sqlFlags   string
}

func (k regexpFlagKey) pattern() (string, error) {
	return regexpEvalFlags(k.sqlPattern, k.sqlFlags)
}

var replaceSubRe = regexp.MustCompile(`\\[&1-9]`)

func regexpReplace(ctx *EvalContext, s, pattern, to, sqlFlags string) (Datum, error) {
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

	return NewDString(newString.String()), nil
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
	return NewDString(string(runes[:pos]) + to + string(runes[after:])), nil
}

func round(x float64, n int64) (Datum, error) {
	pow := math.Pow(10, float64(n))

	if pow == 0 {
		// Rounding to so many digits on the left that we're underflowing.
		// Avoid a NaN below.
		return NewDFloat(DFloat(0)), nil
	}
	if math.Abs(x*pow) > 1e17 {
		// Rounding touches decimals below float precision; the operation
		// is a no-op.
		return NewDFloat(DFloat(x)), nil
	}

	v, frac := math.Modf(x * pow)
	// The following computation implements unbiased rounding, also
	// called bankers' rounding. It ensures that values that fall
	// exactly between two integers get equal chance to be rounded up or
	// down.
	if x > 0.0 {
		if frac > 0.5 || (frac == 0.5 && uint64(v)%2 != 0) {
			v += 1.0
		}
	} else {
		if frac < -0.5 || (frac == -0.5 && uint64(v)%2 != 0) {
			v -= 1.0
		}
	}

	return NewDFloat(DFloat(v / pow)), nil
}

const (
	scaleRatio = math.Ln2 / math.Ln10
)

func roundDecimal(x *inf.Dec, n int64) (Datum, error) {
	curScale := int64(x.Scale())

	if n > curScale+2000 {
		// If we let the decimal value grow too many decimals, the server
		// could explode (#8633).
		return nil, errRoundTooLow
	}

	dd := &DDecimal{}

	// We use WordLen(Bits())*8 instead of UnscaledBig().BitLen() here
	// as this is faster and we do not need an exact value for the
	// optimization below.
	upperCurDigits := encoding.WordLen(x.UnscaledBig().Bits()) * 8
	upperDigitsLeft := float64(curScale) - float64(upperCurDigits)*scaleRatio
	if n < int64(upperDigitsLeft)-1 {
		// This is an optimization. When the rounding scale is definitely
		// larger than the number, the result is 0, so we avoid
		// spending a lot of time in the division for nothing.
		return dd, nil
	}
	dd.Round(x, inf.Scale(n), inf.RoundHalfEven)
	return dd, nil
}

func expDecimal(x *inf.Dec) (Datum, error) {
	// The computation of Exp is separated in the decimal module by
	// computing the exponents on the left and right of the decimal
	// separator. The computation on the right is bounded by
	// decimal.Precision already; however if the value is too large on
	// the left the decimal value can grow too large in memory and slow
	// down / crash the entire server. So we prevent this from happening
	// and limit the argument to be ~1000 or less.
	curDigits := x.UnscaledBig().BitLen()
	binDigitsLeft := curDigits - int(float64(x.Scale())/scaleRatio)
	if binDigitsLeft > 10 /* 1024 */ {
		return nil, errArgTooBig
	}
	dd := &DDecimal{}
	decimal.Exp(&dd.Dec, x, decimal.Precision)
	return dd, nil
}

// Pick the greatest (or least value) from a tuple.
func pickFromTuple(ctx *EvalContext, greatest bool, args DTuple) (Datum, error) {
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
		if eval == DBoolTrue ||
			(eval == DNull && g == DNull) {
			g = d
		}
	}
	return g, nil
}

var uniqueBytesState struct {
	syncutil.Mutex
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
	syncutil.Mutex
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
