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
	"errors"
	"fmt"
	"math"
	"math/rand"
	"net"
	"regexp"
	"regexp/syntax"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/knz/strtime"

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

var (
	errEmptyInputString = errors.New("the input string must not be empty")
	errAbsOfMinInt64    = errors.New("abs of min integer value (-9223372036854775808) not defined")
	errSqrtOfNegNumber  = errors.New("cannot take square root of a negative number")
	errLogOfNegNumber   = errors.New("cannot take logarithm of a negative number")
	errLogOfZero        = errors.New("cannot take logarithm of zero")
	errInsufficientArgs = errors.New("unknown signature: concat_ws()")
	errZeroIP           = errors.New("zero length IP")
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
	// GeneratorClass is a builtin generator function.
	GeneratorClass
)

// Avoid vet warning about unused enum value.
var _ = NormalClass

const (
	categoryComparison    = "Comparison"
	categoryCompatibility = "Compatibility"
	categoryDateAndTime   = "Date and Time"
	categoryIDGeneration  = "ID Generation"
	categoryMath          = "Math and Numeric"
	categoryString        = "String and Byte"
	categorySystemInfo    = "System Info"
)

// Builtin is a built-in function.
type Builtin struct {
	Types      typeList
	ReturnType returnTyper

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
	impure bool

	// Set to true when a function depends on members of the EvalContext that are
	// not marshalled by DistSQL (e.g. planner). Currently used for DistSQL to
	// determine if expressions can be evaluated on a different node without
	// sending over the EvalContext.
	//
	// TODO(andrei): Get rid of the planner from the EvalContext and then we can
	// get rid of this blacklist.
	distsqlBlacklist bool

	// Set to true when a function may change at every row whether or
	// not it is applied to an expression that contains row-dependent
	// variables. Used e.g. by `random` and aggregate functions.
	needsRepeatedEvaluation bool

	// Set to true when the built-in can only be used by security.RootUser.
	privileged bool

	class    FunctionClass
	category string

	// Info is a description of the function, which is surfaced on the CockroachDB
	// docs site on the "Functions and Operators" page. Descriptions typically use
	// third-person with the function as an implicit subject (e.g. "Calculates
	// infinity"), but should focus more on ease of understanding so other structures
	// might be more appropriate.
	Info string

	AggregateFunc func([]Type, *EvalContext) AggregateFunc
	WindowFunc    func([]Type, *EvalContext) WindowFunc
	fn            func(*EvalContext, Datums) (Datum, error)
}

func (b Builtin) params() typeList {
	return b.Types
}

func (b Builtin) returnType() returnTyper {
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
	// If an explicit category is specified, use it.
	if b.category != "" {
		return b.category
	}
	// If single argument attempt to categorize by the type of the argument.
	switch typ := b.Types.(type) {
	case ArgTypes:
		if len(typ) == 1 {
			return categorizeType(typ[0].Typ)
		}
	}
	// Fall back to categorizing by return type.
	if retType := b.FixedReturnType(); retType != nil {
		return categorizeType(retType)
	}
	return ""
}

// Class returns the FunctionClass of this builtin.
func (b Builtin) Class() FunctionClass {
	return b.class
}

// Impure returns false if this builtin is a pure function of its inputs.
func (b Builtin) Impure() bool {
	return b.impure
}

// DistSQLBlacklist returns true if the builtin is not supported by DistSQL.
// See distsqlBlacklist.
func (b Builtin) DistSQLBlacklist() bool {
	return b.distsqlBlacklist
}

// FixedReturnType returns a fixed type that the function returns, returning Any
// if the return type is based on the function's arguments.
func (b Builtin) FixedReturnType() Type {
	if b.ReturnType == nil {
		return nil
	}
	return returnTypeToFixedType(b.ReturnType)
}

// Signature returns a human-readable signature.
func (b Builtin) Signature() string {
	return fmt.Sprintf("(%s) -> %s", b.Types.String(), b.FixedReturnType())
}

func init() {
	initAggregateBuiltins()
	initWindowBuiltins()
	initGeneratorBuiltins()
	initPGBuiltins()

	names := make([]string, 0, len(Builtins))
	funDefs = make(map[string]*FunctionDefinition)
	for name, def := range Builtins {
		funDefs[name] = newFunctionDefinition(name, def)
		names = append(names, name)
	}

	// We alias the builtins to uppercase to hasten the lookup in the
	// common case.
	for _, name := range names {
		uname := strings.ToUpper(name)
		def := Builtins[name]
		Builtins[uname] = def
		funDefs[uname] = funDefs[name]
	}
}

var digitNames = []string{"zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"}

// Builtins contains the built-in functions indexed by name.
var Builtins = map[string][]Builtin{
	// Keep the list of functions sorted.

	// TODO(XisiHuang): support encoding, i.e., length(str, encoding).
	"length": {
		stringBuiltin1(func(s string) (Datum, error) {
			return NewDInt(DInt(utf8.RuneCountInString(s))), nil
		}, TypeInt, "Calculates the number of characters in `val`."),
		bytesBuiltin1(func(s string) (Datum, error) {
			return NewDInt(DInt(len(s))), nil
		}, TypeInt, "Calculates the number of bytes in `val`."),
	},

	"octet_length": {
		stringBuiltin1(func(s string) (Datum, error) {
			return NewDInt(DInt(len(s))), nil
		}, TypeInt, "Calculates the number of bytes used to represent `val`."),
		bytesBuiltin1(func(s string) (Datum, error) {
			return NewDInt(DInt(len(s))), nil
		}, TypeInt, "Calculates the number of bytes in `val`."),
	},

	// TODO(pmattis): What string functions should also support TypeBytes?

	"lower": {stringBuiltin1(func(s string) (Datum, error) {
		return NewDString(strings.ToLower(s)), nil
	}, TypeString, "Converts all characters in `val`to their lower-case equivalents.")},

	"upper": {stringBuiltin1(func(s string) (Datum, error) {
		return NewDString(strings.ToUpper(s)), nil
	}, TypeString, "Converts all characters in `val`to their to their upper-case equivalents.")},

	"substr":    substringImpls,
	"substring": substringImpls,

	// concat concatenates the text representations of all the arguments.
	// NULL arguments are ignored.
	"concat": {
		Builtin{
			Types:      VariadicType{TypeString},
			ReturnType: fixedReturnType(TypeString),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				var buffer bytes.Buffer
				for _, d := range args {
					if d == DNull {
						continue
					}
					buffer.WriteString(string(MustBeDString(d)))
				}
				return NewDString(buffer.String()), nil
			},
			Info: "Concatenates a comma-separated list of strings.",
		},
	},

	"concat_ws": {
		Builtin{
			Types:      VariadicType{TypeString},
			ReturnType: fixedReturnType(TypeString),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				if len(args) == 0 {
					return nil, errInsufficientArgs
				}
				if args[0] == DNull {
					return DNull, nil
				}
				sep := string(MustBeDString(args[0]))
				var buf bytes.Buffer
				prefix := ""
				for _, d := range args[1:] {
					if d == DNull {
						continue
					}
					// Note: we can't use the range index here because that
					// would break when the 2nd argument is NULL.
					buf.WriteString(prefix)
					prefix = sep
					buf.WriteString(string(MustBeDString(d)))
				}
				return NewDString(buf.String()), nil
			},
			Info: "Uses the first argument as a separator between the concatenation of the " +
				"subsequent arguments. <br/><br/>For example `concat_ws('!','wow','great')` " +
				"returns `wow!great`.",
		},
	},

	"to_uuid": {
		Builtin{
			Types:      ArgTypes{{"val", TypeString}},
			ReturnType: fixedReturnType(TypeBytes),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				s := string(MustBeDString(args[0]))
				uv, err := uuid.FromString(s)
				if err != nil {
					return nil, err
				}
				return NewDBytes(DBytes(uv.GetBytes())), nil
			},
			Info: "Converts the character string representation of a UUID to its byte string " +
				"representation.",
		},
	},

	"from_uuid": {
		Builtin{
			Types:      ArgTypes{{"val", TypeBytes}},
			ReturnType: fixedReturnType(TypeString),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				b := []byte(*args[0].(*DBytes))
				uv, err := uuid.FromBytes(b)
				if err != nil {
					return nil, err
				}
				return NewDString(uv.String()), nil
			},
			Info: "Converts the byte string representation of a UUID to its character string " +
				"representation.",
		},
	},

	"from_ip": {
		Builtin{
			Types:      ArgTypes{{"val", TypeBytes}},
			ReturnType: fixedReturnType(TypeString),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				ipstr := args[0].(*DBytes)
				nboip := net.IP(*ipstr)
				sv := nboip.String()
				// if nboip has a length of 0, sv will be "<nil>"
				if sv == "<nil>" {
					return nil, errZeroIP
				}
				return NewDString(sv), nil
			},
			Info: "Converts the byte string representation of an IP to its character string " +
				"representation.",
		},
	},

	"to_ip": {
		Builtin{
			Types:      ArgTypes{{"val", TypeString}},
			ReturnType: fixedReturnType(TypeBytes),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				ipdstr := MustBeDString(args[0])
				ip := net.ParseIP(string(ipdstr))
				// If ipdstr could not be parsed to a valid IP,
				// ip will be nil.
				if ip == nil {
					return nil, fmt.Errorf("invalid IP format: %s", ipdstr.String())
				}
				return NewDBytes(DBytes(ip)), nil
			},
			Info: "Converts the character string representation of an IP to its byte string " +
				"representation.",
		},
	},

	"split_part": {
		Builtin{
			Types: ArgTypes{
				{"input", TypeString},
				{"delimiter", TypeString},
				{"return_index_pos", TypeInt},
			},
			ReturnType: fixedReturnType(TypeString),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				text := string(MustBeDString(args[0]))
				sep := string(MustBeDString(args[1]))
				field := int(MustBeDInt(args[2]))

				if field <= 0 {
					return nil, fmt.Errorf("field position %d must be greater than zero", field)
				}

				splits := strings.Split(text, sep)
				if field > len(splits) {
					return NewDString(""), nil
				}
				return NewDString(splits[field-1]), nil
			},
			Info: "Splits `input` on `delimiter` and return the value in the `return_index_pos`  " +
				"position (starting at 1). <br/><br/>For example, `split_part('123.456.789.0','.',3)`" +
				"returns `789`.",
		},
	},

	"repeat": {
		Builtin{
			Types:            ArgTypes{{"input", TypeString}, {"repeat_counter", TypeInt}},
			distsqlBlacklist: true,
			ReturnType:       fixedReturnType(TypeString),
			fn: func(_ *EvalContext, args Datums) (_ Datum, err error) {
				s := string(MustBeDString(args[0]))
				count := int(MustBeDInt(args[1]))
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
			Info: "Concatenates `input` `repeat_counter` number of times.<br/><br/>For example, " +
				"`repeat('dog', 2)` returns `dogdog`.",
		},
	},

	"ascii": {stringBuiltin1(func(s string) (Datum, error) {
		for _, ch := range s {
			return NewDInt(DInt(ch)), nil
		}
		return nil, errEmptyInputString
	}, TypeInt, "Calculates the ASCII value for the first character in `val`.")},

	"md5": {stringBuiltin1(func(s string) (Datum, error) {
		return NewDString(fmt.Sprintf("%x", md5.Sum([]byte(s)))), nil
	}, TypeString, "Calculates the MD5 hash value of `val`.")},

	"sha1": {stringBuiltin1(func(s string) (Datum, error) {
		return NewDString(fmt.Sprintf("%x", sha1.Sum([]byte(s)))), nil
	}, TypeString, "Calculates the SHA1 hash value of `val`.")},

	"sha256": {stringBuiltin1(func(s string) (Datum, error) {
		return NewDString(fmt.Sprintf("%x", sha256.Sum256([]byte(s)))), nil
	}, TypeString, "Calculates the SHA256 hash value of `val`.")},

	"to_hex": {
		Builtin{
			Types:      ArgTypes{{"val", TypeInt}},
			ReturnType: fixedReturnType(TypeString),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				return NewDString(fmt.Sprintf("%x", int64(MustBeDInt(args[0])))), nil
			},
			Info: "Converts `val` to its hexadecimal representation.",
		},
	},

	// The SQL parser coerces POSITION to STRPOS.
	"strpos": {stringBuiltin2("input", "find", func(s, substring string) (Datum, error) {
		index := strings.Index(s, substring)
		if index < 0 {
			return DZero, nil
		}

		return NewDInt(DInt(utf8.RuneCountInString(s[:index]) + 1)), nil
	}, TypeInt, "Calculates the position where the string `find` begins in `input`. <br/><br/>For"+
		" example, `strpos('doggie', 'gie')` returns `4`.")},

	"overlay": {
		Builtin{
			Types: ArgTypes{
				{"input", TypeString},
				{"overlay_val", TypeString},
				{"start_pos", TypeInt},
			},
			ReturnType: fixedReturnType(TypeString),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				s := string(MustBeDString(args[0]))
				to := string(MustBeDString(args[1]))
				pos := int(MustBeDInt(args[2]))
				size := utf8.RuneCountInString(to)
				return overlay(s, to, pos, size)
			},
			Info: "Replaces characters in `input` with `overlay_val` starting at `start_pos` " +
				"(begins at 1). <br/><br/>For example, `overlay('doggie', 'CAT', 2)` returns " +
				"`dCATie`.",
		},
		Builtin{
			Types: ArgTypes{
				{"input", TypeString},
				{"overlay_val", TypeString},
				{"start_pos", TypeInt},
				{"end_pos", TypeInt},
			},
			ReturnType: fixedReturnType(TypeString),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				s := string(MustBeDString(args[0]))
				to := string(MustBeDString(args[1]))
				pos := int(MustBeDInt(args[2]))
				size := int(MustBeDInt(args[3]))
				return overlay(s, to, pos, size)
			},
			Info: "Deletes the characters in `input` between `start_pos` and `end_pos` (count " +
				"starts at 1), and then insert `overlay_val` at `start_pos`.",
		},
	},

	// The SQL parser coerces TRIM(...) and TRIM(BOTH ...) to BTRIM(...).
	"btrim": {
		stringBuiltin2("input", "trim_chars", func(s, chars string) (Datum, error) {
			return NewDString(strings.Trim(s, chars)), nil
		}, TypeString, "Removes any characters included in `trim_chars` from the beginning or end"+
			" of `input` (applies recursively). <br/><br/>For example, `btrim('doggie', 'eod')` "+
			"returns `ggi`."),
		stringBuiltin1(func(s string) (Datum, error) {
			return NewDString(strings.TrimSpace(s)), nil
		}, TypeString, "Removes all spaces from the beginning and end of `val`."),
	},

	// The SQL parser coerces TRIM(LEADING ...) to LTRIM(...).
	"ltrim": {
		stringBuiltin2("input", "trim_chars", func(s, chars string) (Datum, error) {
			return NewDString(strings.TrimLeft(s, chars)), nil
		}, TypeString, "Removes any characters included in `trim_chars` from the beginning "+
			"(left-hand side) of `input` (applies recursively). <br/><br/>For example, "+
			"`ltrim('doggie', 'od')` returns `ggie`."),
		stringBuiltin1(func(s string) (Datum, error) {
			return NewDString(strings.TrimLeftFunc(s, unicode.IsSpace)), nil
		}, TypeString, "Removes all spaces from the beginning (left-hand side) of `val`."),
	},

	// The SQL parser coerces TRIM(TRAILING ...) to RTRIM(...).
	"rtrim": {
		stringBuiltin2("input", "trim_chars", func(s, chars string) (Datum, error) {
			return NewDString(strings.TrimRight(s, chars)), nil
		}, TypeString, "Removes any characters included in `trim_chars` from the end (right-hand "+
			"side) of `input` (applies recursively). <br/><br/>For example, `rtrim('doggie', 'ei')` "+
			"returns `dogg`."),
		stringBuiltin1(func(s string) (Datum, error) {
			return NewDString(strings.TrimRightFunc(s, unicode.IsSpace)), nil
		}, TypeString, "Removes all spaces from the end (right-hand side) of `val`."),
	},

	"reverse": {stringBuiltin1(func(s string) (Datum, error) {
		runes := []rune(s)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return NewDString(string(runes)), nil
	}, TypeString, "Reverses the order of the string's characters.")},

	"replace": {stringBuiltin3(
		"input", "find", "replace",
		func(input, from, to string) (Datum, error) {
			return NewDString(strings.Replace(input, from, to, -1)), nil
		},
		TypeString,
		"Replaces all occurrences of `find` with `replace` in `input`",
	)},

	"translate": {stringBuiltin3(
		"input", "find", "replace",
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
		}, TypeString, "In `input`, replaces the first character from `find` with the first "+
			"character in `replace`; repeat for each character in `find`. <br/><br/>For example, "+
			"`translate('doggie', 'dog', '123');` returns `1233ie`.")},

	"regexp_extract": {
		Builtin{
			Types:      ArgTypes{{"input", TypeString}, {"regex", TypeString}},
			ReturnType: fixedReturnType(TypeString),
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				s := string(MustBeDString(args[0]))
				pattern := string(MustBeDString(args[1]))
				return regexpExtract(ctx, s, pattern, `\`)
			},
			Info: "Returns the first match for the Regular Expression `regex` in `input`.",
		},
	},

	"regexp_replace": {
		Builtin{
			Types: ArgTypes{
				{"input", TypeString},
				{"regex", TypeString},
				{"replace", TypeString},
			},
			ReturnType: fixedReturnType(TypeString),
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				s := string(MustBeDString(args[0]))
				pattern := string(MustBeDString(args[1]))
				to := string(MustBeDString(args[2]))
				return regexpReplace(ctx, s, pattern, to, "")
			},
			Info: "Replaces matches for the Regular Expression `regex` in `input` with the " +
				"Regular Expression `replace`.",
		},
		Builtin{
			Types: ArgTypes{
				{"input", TypeString},
				{"regex", TypeString},
				{"replace", TypeString},
				{"flags", TypeString},
			},
			ReturnType: fixedReturnType(TypeString),
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				s := string(MustBeDString(args[0]))
				pattern := string(MustBeDString(args[1]))
				to := string(MustBeDString(args[2]))
				sqlFlags := string(MustBeDString(args[3]))
				return regexpReplace(ctx, s, pattern, to, sqlFlags)
			},
			Info: "Replaces matches for the Regular Expression `regex` in `input` with the Regular " +
				"Expression `replace` using `flags`.<br/><br/>CockroachDB supports the following " +
				"flags:<br/><br/>&#8226; **c**: Case-sensitive matching<br/><br/>&#8226; **g**: " +
				"Global matching (match each substring instead of only the first).<br/><br/>&#8226; " +
				"**i**: Case-insensitive matching<br/><br/>&#8226; **m** or **n**: Newline-sensitive " +
				"`.` and negated brackets (`[^...]`) do not match newline characters (preventing " +
				"matching: matches from crossing newlines unless explicitly defined to); `^` and " +
				"`$` match the space before and after newline characters respectively (so characters " +
				"between newline characters are treated as if they're on a separate line).<br/>" +
				"<br/>&#8226; **p**: Partial newline-sensitive matching: `.` and negated brackets " +
				"(`[^...]`) do not match newline characters (preventing matches from crossing " +
				"newlines unless explicitly defined to), but `^` and `$` still only match the " +
				"beginning and end of `val`.<br/><br/>&#8226; **s**: Newline-insensitive " +
				"matching *(default)*.<br/><br/>&#8226; **w**: Inverse partial newline-sensitive " +
				"matching:`.` and negated brackets (`[^...]`) *do* match newline characters, but  `^` " +
				"and `$` match the space before and after newline characters respectively (so " +
				"characters between newline characters are treated as if they're on a separate line).",
		},
	},

	"initcap": {stringBuiltin1(func(s string) (Datum, error) {
		return NewDString(strings.Title(strings.ToLower(s))), nil
	}, TypeString, "Capitalizes the first letter of `val`.")},

	"left": {
		Builtin{
			Types:      ArgTypes{{"input", TypeBytes}, {"return_set", TypeInt}},
			ReturnType: fixedReturnType(TypeBytes),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				bytes := []byte(*args[0].(*DBytes))
				n := int(MustBeDInt(args[1]))

				if n < -len(bytes) {
					n = 0
				} else if n < 0 {
					n = len(bytes) + n
				} else if n > len(bytes) {
					n = len(bytes)
				}
				return NewDBytes(DBytes(bytes[:n])), nil
			},
			Info: "Returns the first `return_set` bytes from `input`.",
		},
		Builtin{
			Types:      ArgTypes{{"input", TypeString}, {"return_set", TypeInt}},
			ReturnType: fixedReturnType(TypeString),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				runes := []rune(string(MustBeDString(args[0])))
				n := int(MustBeDInt(args[1]))

				if n < -len(runes) {
					n = 0
				} else if n < 0 {
					n = len(runes) + n
				} else if n > len(runes) {
					n = len(runes)
				}
				return NewDString(string(runes[:n])), nil
			},
			Info: "Returns the first `return_set` characters from `input`.",
		},
	},

	"right": {
		Builtin{
			Types:      ArgTypes{{"input", TypeBytes}, {"return_set", TypeInt}},
			ReturnType: fixedReturnType(TypeBytes),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				bytes := []byte(*args[0].(*DBytes))
				n := int(MustBeDInt(args[1]))

				if n < -len(bytes) {
					n = 0
				} else if n < 0 {
					n = len(bytes) + n
				} else if n > len(bytes) {
					n = len(bytes)
				}
				return NewDBytes(DBytes(bytes[len(bytes)-n:])), nil
			},
			Info: "Returns the last `return_set` bytes from `input`.",
		},
		Builtin{
			Types:      ArgTypes{{"input", TypeString}, {"return_set", TypeInt}},
			ReturnType: fixedReturnType(TypeString),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				runes := []rune(string(MustBeDString(args[0])))
				n := int(MustBeDInt(args[1]))

				if n < -len(runes) {
					n = 0
				} else if n < 0 {
					n = len(runes) + n
				} else if n > len(runes) {
					n = len(runes)
				}
				return NewDString(string(runes[len(runes)-n:])), nil
			},
			Info: "Returns the last `return_set` characters from `input`.",
		},
	},

	"random": {
		Builtin{
			Types:                   ArgTypes{},
			ReturnType:              fixedReturnType(TypeFloat),
			impure:                  true,
			needsRepeatedEvaluation: true,
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				return NewDFloat(DFloat(rand.Float64())), nil
			},
			Info: "Returns a random float between 0 and 1.",
		},
	},

	"unique_rowid": {
		Builtin{
			Types:      ArgTypes{},
			ReturnType: fixedReturnType(TypeInt),
			category:   categoryIDGeneration,
			impure:     true,
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				return NewDInt(GenerateUniqueInt(ctx.NodeID)), nil
			},
			Info: "Returns a unique ID used by CockroachDB to generate unique row IDs if a " +
				"Primary Key isn't defined for the table. The value is a combination of the " +
				" insert timestamp and the ID of the node executing the statement, which " +
				" guarantees this combination is globally unique.",
		},
	},

	"experimental_uuid_v4": {uuidV4Impl},
	"uuid_v4":              {uuidV4Impl},

	"greatest": {
		Builtin{
			Types:      HomogeneousType{},
			ReturnType: identityReturnType(0),
			category:   categoryComparison,
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				return pickFromTuple(ctx, true /* greatest */, args)
			},
			Info: "Returns the element with the greatest value.",
		},
	},

	"least": {
		Builtin{
			Types:      HomogeneousType{},
			ReturnType: identityReturnType(0),
			category:   categoryComparison,
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				return pickFromTuple(ctx, false /* !greatest */, args)
			},
			Info: "Returns the element with the lowest value.",
		},
	},

	// Timestamp/Date functions.

	"experimental_strftime": {
		Builtin{
			Types:      ArgTypes{{"input", TypeTimestamp}, {"extract_format", TypeString}},
			ReturnType: fixedReturnType(TypeString),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				fromTime := args[0].(*DTimestamp).Time
				format := string(MustBeDString(args[1]))
				t, err := strtime.Strftime(fromTime, format)
				if err != nil {
					return nil, err
				}
				return NewDString(t), nil
			},
			Info: "From `input`, extracts and formats the time as identified in `extract_format` " +
				"using standard `strftime` notation (though not all formatting is supported).",
		},
		Builtin{
			Types:      ArgTypes{{"input", TypeDate}, {"extract_format", TypeString}},
			ReturnType: fixedReturnType(TypeString),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				fromTime := time.Unix(int64(*args[0].(*DDate))*secondsInDay, 0).UTC()
				format := string(MustBeDString(args[1]))
				t, err := strtime.Strftime(fromTime, format)
				if err != nil {
					return nil, err
				}
				return NewDString(t), nil
			},
			Info: "From `input`, extracts and formats the time as identified in `extract_format` " +
				"using standard `strftime` notation (though not all formatting is supported).",
		},
		Builtin{
			Types:      ArgTypes{{"input", TypeTimestampTZ}, {"extract_format", TypeString}},
			ReturnType: fixedReturnType(TypeString),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				fromTime := args[0].(*DTimestampTZ).Time
				format := string(MustBeDString(args[1]))
				t, err := strtime.Strftime(fromTime, format)
				if err != nil {
					return nil, err
				}
				return NewDString(t), nil
			},
			Info: "From `input`, extracts and formats the time as identified in `extract_format` " +
				"using standard `strftime` notation (though not all formatting is supported).",
		},
	},

	"experimental_strptime": {
		Builtin{
			Types:      ArgTypes{{"input", TypeString}, {"format", TypeString}},
			ReturnType: fixedReturnType(TypeTimestampTZ),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				toParse := string(MustBeDString(args[0]))
				format := string(MustBeDString(args[1]))
				t, err := strtime.Strptime(toParse, format)
				if err != nil {
					return nil, err
				}
				return MakeDTimestampTZ(t.UTC(), time.Microsecond), nil
			},
			Info: "Returns `input` as a timestamptz using `format` (which uses standard " +
				"`strptime` formatting).",
		},
	},

	"age": {
		Builtin{
			Types:      ArgTypes{{"val", TypeTimestampTZ}},
			ReturnType: fixedReturnType(TypeInterval),
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				return timestampMinusBinOp.fn(ctx, ctx.GetTxnTimestamp(time.Microsecond), args[0])
			},
			Info: "Calculates the interval between `val` and the current time.",
		},
		Builtin{
			Types:      ArgTypes{{"begin", TypeTimestampTZ}, {"end", TypeTimestampTZ}},
			ReturnType: fixedReturnType(TypeInterval),
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				return timestampMinusBinOp.fn(ctx, args[0], args[1])
			},
			Info: "Calculates the interval between `begin` and `end`.",
		},
	},

	"current_date": {
		Builtin{
			Types:      ArgTypes{},
			ReturnType: fixedReturnType(TypeDate),
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				t := ctx.GetTxnTimestamp(time.Microsecond).Time
				return NewDDateFromTime(t, ctx.GetLocation()), nil
			},
			Info: "Returns the current date.",
		},
	},

	"now":                   txnTSImpl,
	"current_timestamp":     txnTSImpl,
	"transaction_timestamp": txnTSImpl,

	"statement_timestamp": {
		Builtin{
			Types:             ArgTypes{},
			ReturnType:        fixedReturnType(TypeTimestampTZ),
			preferredOverload: true,
			impure:            true,
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				return MakeDTimestampTZ(ctx.GetStmtTimestamp(), time.Microsecond), nil
			},
			Info: "Returns the current statement's timestamp.",
		},
		Builtin{
			Types:      ArgTypes{},
			ReturnType: fixedReturnType(TypeTimestamp),
			impure:     true,
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				return MakeDTimestamp(ctx.GetStmtTimestamp(), time.Microsecond), nil
			},
			Info: "Returns the current statement's timestamp.",
		},
	},

	"cluster_logical_timestamp": {
		Builtin{
			Types:      ArgTypes{},
			ReturnType: fixedReturnType(TypeDecimal),
			category:   categorySystemInfo,
			impure:     true,
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				return ctx.GetClusterTimestamp(), nil
			},
			Info: "This function is used only by CockroachDB's developers for testing purposes.",
		},
	},

	"clock_timestamp": {
		Builtin{
			Types:             ArgTypes{},
			ReturnType:        fixedReturnType(TypeTimestampTZ),
			preferredOverload: true,
			impure:            true,
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				return MakeDTimestampTZ(timeutil.Now(), time.Microsecond), nil
			},
			Info: "Returns the current wallclock time.",
		},
		Builtin{
			Types:      ArgTypes{},
			ReturnType: fixedReturnType(TypeTimestamp),
			impure:     true,
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				return MakeDTimestamp(timeutil.Now(), time.Microsecond), nil
			},
			Info: "Returns the current wallclock time.",
		},
	},

	"extract": {
		Builtin{
			Types:      ArgTypes{{"element", TypeString}, {"input", TypeTimestamp}},
			ReturnType: fixedReturnType(TypeInt),
			category:   categoryDateAndTime,
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				// extract timeSpan fromTime.
				fromTS := args[1].(*DTimestamp)
				timeSpan := strings.ToLower(string(MustBeDString(args[0])))
				return extractStringFromTimestamp(ctx, fromTS.Time, timeSpan)
			},
			Info: "Extracts `element` from `input`. Compatible `elements` are: <br/>&#8226; " +
				"year<br/>&#8226; quarter<br/>&#8226; month<br/>&#8226; week<br/>&#8226; " +
				"dayofweek<br/>&#8226; dayofyear<br/>&#8226; hour<br/>&#8226; minute<br/>&#8226; " +
				"second<br/>&#8226; millisecond<br/>&#8226; microsecond<br/>&#8226; epoch",
		},
		Builtin{
			Types:      ArgTypes{{"element", TypeString}, {"input", TypeDate}},
			ReturnType: fixedReturnType(TypeInt),
			category:   categoryDateAndTime,
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				timeSpan := strings.ToLower(string(MustBeDString(args[0])))
				date := args[1].(*DDate)
				fromTSTZ := MakeDTimestampTZFromDate(ctx.GetLocation(), date)
				return extractStringFromTimestamp(ctx, fromTSTZ.Time, timeSpan)
			},
			Info: "Extracts `element` from `input`. Compatible `elements` are: <br/>&#8226; " +
				"year<br/>&#8226; quarter<br/>&#8226; month<br/>&#8226; week<br/>&#8226; " +
				"dayofweek<br/>&#8226; dayofyear<br/>&#8226; hour<br/>&#8226; minute<br/>&#8226; " +
				"second<br/>&#8226; millisecond<br/>&#8226; microsecond<br/>&#8226; epoch",
		},
	},

	"extract_duration": {
		Builtin{
			Types:      ArgTypes{{"element", TypeString}, {"input", TypeInterval}},
			ReturnType: fixedReturnType(TypeInt),
			category:   categoryDateAndTime,
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				// extract timeSpan fromTime.
				fromInterval := *args[1].(*DInterval)
				timeSpan := strings.ToLower(string(MustBeDString(args[0])))
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
					return nil, fmt.Errorf("unsupported timespan: %s", timeSpan)
				}
			},
			Info: "Extracts `element` from `input`. Compatible `elements` are: <br/>&#8226; hour" +
				"<br/>&#8226; minute<br/>&#8226; second<br/>&#8226; millisecond<br/>&#8226; " +
				"microsecond",
		},
	},

	// Math functions
	"abs": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(math.Abs(x))), nil
		}, "Calculates the absolute value of `val`."),
		decimalBuiltin1(func(x *apd.Decimal) (Datum, error) {
			dd := &DDecimal{}
			dd.Abs(x)
			return dd, nil
		}, "Calculates the absolute value of `val`."),
		Builtin{
			Types:      ArgTypes{{"val", TypeInt}},
			ReturnType: fixedReturnType(TypeInt),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				x := MustBeDInt(args[0])
				switch {
				case x == math.MinInt64:
					return nil, errAbsOfMinInt64
				case x < 0:
					return NewDInt(-x), nil
				}
				return args[0], nil
			},
			Info: "Calculates the absolute value of `val`.",
		},
	},

	"acos": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(math.Acos(x))), nil
		}, "Calculates the inverse cosine of `val`."),
	},

	"asin": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(math.Asin(x))), nil
		}, "Calculates the inverse sine of `val`."),
	},

	"atan": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(math.Atan(x))), nil
		}, "Calculates the inverse tangent of `val`."),
	},

	"atan2": {
		floatBuiltin2("x", "y", func(x, y float64) (Datum, error) {
			return NewDFloat(DFloat(math.Atan2(x, y))), nil
		}, "Calculates the inverse tangent of `x`/`y`."),
	},

	"cbrt": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(math.Cbrt(x))), nil
		}, "Calculates the cube root (&#8731;) of `val`."),
		decimalBuiltin1(func(x *apd.Decimal) (Datum, error) {
			dd := &DDecimal{}
			_, err := DecimalCtx.Cbrt(&dd.Decimal, x)
			return dd, err
		}, "Calculates the cube root (&#8731;) of `val`."),
	},

	"ceil":    ceilImpl,
	"ceiling": ceilImpl,

	"cos": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(math.Cos(x))), nil
		}, "Calculates the cosine of `val`."),
	},

	"cot": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(1 / math.Tan(x))), nil
		}, "Calculates the cotangent of `val`."),
	},

	"degrees": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(180.0 * x / math.Pi)), nil
		}, "Converts `val` as a radian value to a degree value."),
	},

	"div": {
		floatBuiltin2("x", "y", func(x, y float64) (Datum, error) {
			return NewDFloat(DFloat(math.Trunc(x / y))), nil
		}, "Calculates the integer quotient of `x`/`y`."),
		decimalBuiltin2("x", "y", func(x, y *apd.Decimal) (Datum, error) {
			if y.Sign() == 0 {
				return nil, errDivByZero
			}
			dd := &DDecimal{}
			_, err := HighPrecisionCtx.QuoInteger(&dd.Decimal, x, y)
			return dd, err
		}, "Calculates the integer quotient of `x`/`y`."),
		{
			Types:      ArgTypes{{"x", TypeInt}, {"y", TypeInt}},
			ReturnType: fixedReturnType(TypeInt),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				y := MustBeDInt(args[1])
				if y == 0 {
					return nil, errDivByZero
				}
				x := MustBeDInt(args[0])
				return NewDInt(x / y), nil
			},
			Info: "Calculates the integer quotient of `x`/`y`.",
		},
	},

	"exp": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(math.Exp(x))), nil
		}, "Calculates *e* ^ `val`."),
		decimalBuiltin1(func(x *apd.Decimal) (Datum, error) {
			dd := &DDecimal{}
			_, err := DecimalCtx.Exp(&dd.Decimal, x)
			return dd, err
		}, "Calculates *e* ^ `val`."),
	},

	"floor": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(math.Floor(x))), nil
		}, "Calculates the largest integer not greater than `val`."),
		decimalBuiltin1(func(x *apd.Decimal) (Datum, error) {
			dd := &DDecimal{}
			_, err := ExactCtx.Floor(&dd.Decimal, x)
			return dd, err
		}, "Calculates the largest integer not greater than `val`."),
	},

	"isnan": {
		Builtin{
			// Can't use floatBuiltin1 here because this one returns
			// a boolean.
			Types:      ArgTypes{{"val", TypeFloat}},
			ReturnType: fixedReturnType(TypeBool),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				return MakeDBool(DBool(isNaN(args[0]))), nil
			},
			Info: "Returns true if `val` is NaN, false otherwise.",
		},
		Builtin{
			Types:      ArgTypes{{"val", TypeDecimal}},
			ReturnType: fixedReturnType(TypeBool),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				return MakeDBool(DBool(isNaN(args[0]))), nil
			},
			Info: "Returns true if `val` is NaN, false otherwise.",
		},
	},

	"ln": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(math.Log(x))), nil
		}, "Calculates the natural log of `val`."),
		decimalLogFn(DecimalCtx.Ln, "Calculates the natural log of `val`."),
	},

	"log": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(math.Log10(x))), nil
		}, "Calculates the base 10 log of `val`."),
		decimalLogFn(DecimalCtx.Log10, "Calculates the base 10 log of `val`."),
	},

	"mod": {
		floatBuiltin2("x", "y", func(x, y float64) (Datum, error) {
			return NewDFloat(DFloat(math.Mod(x, y))), nil
		}, "Calculates `x`%`y`."),
		decimalBuiltin2("x", "y", func(x, y *apd.Decimal) (Datum, error) {
			if y.Sign() == 0 {
				return nil, errZeroModulus
			}
			dd := &DDecimal{}
			_, err := HighPrecisionCtx.Rem(&dd.Decimal, x, y)
			return dd, err
		}, "Calculates `x`%`y`."),
		Builtin{
			Types:      ArgTypes{{"x", TypeInt}, {"y", TypeInt}},
			ReturnType: fixedReturnType(TypeInt),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				y := MustBeDInt(args[1])
				if y == 0 {
					return nil, errZeroModulus
				}
				x := MustBeDInt(args[0])
				return NewDInt(x % y), nil
			},
			Info: "Calculates `x`%`y`.",
		},
	},

	"pi": {
		Builtin{
			Types:      ArgTypes{},
			ReturnType: fixedReturnType(TypeFloat),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				return NewDFloat(math.Pi), nil
			},
			Info: "Returns the value for pi (3.141592653589793).",
		},
	},

	"pow":   powImpls,
	"power": powImpls,

	"radians": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(x * math.Pi / 180.0)), nil
		}, "Converts `val` as a degree value to a radians value."),
	},

	"round": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(round(x))), nil
		}, "Rounds `val` to the nearest integer using half to even (banker's) rounding."),
		decimalBuiltin1(func(x *apd.Decimal) (Datum, error) {
			return roundDecimal(x, 0)
		}, "Rounds `val` to the nearest integer, half away from zero: "+
			"ROUND(+/-2.4) = +/-2, ROUND(+/-2.5) = +/-3."),
		Builtin{
			Types:      ArgTypes{{"input", TypeFloat}, {"decimal_accuracy", TypeInt}},
			ReturnType: fixedReturnType(TypeFloat),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				var x apd.Decimal
				if _, err := x.SetFloat64(float64(*args[0].(*DFloat))); err != nil {
					return nil, err
				}

				// TODO(mjibson): make sure this fits in an int32.
				scale := int32(MustBeDInt(args[1]))

				var d apd.Decimal
				if _, err := RoundCtx.Quantize(&d, &x, -scale); err != nil {
					return nil, err
				}

				f, err := d.Float64()
				if err != nil {
					return nil, err
				}

				return NewDFloat(DFloat(f)), nil
			},
			Info: "Keeps `decimal_accuracy` number of figures to the right of the zero position " +
				" in `input` using half to even (banker's) rounding.",
		},
		Builtin{
			Types:      ArgTypes{{"input", TypeDecimal}, {"decimal_accuracy", TypeInt}},
			ReturnType: fixedReturnType(TypeDecimal),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				// TODO(mjibson): make sure this fits in an int32.
				scale := int32(MustBeDInt(args[1]))
				return roundDecimal(&args[0].(*DDecimal).Decimal, scale)
			},
			Info: "Keeps `decimal_accuracy` number of figures to the right of the zero position " +
				" in `input using half away from zero rounding. If `decimal_accuracy` " +
				"is not in the range -2^31...(2^31-1), the results are undefined.",
		},
	},

	"sin": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(math.Sin(x))), nil
		}, "Calculates the sine of `val`."),
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
		}, "Determines the sign of `val`: **1** for positive; **0** for 0 values; **-1** for "+
			"negative."),
		decimalBuiltin1(func(x *apd.Decimal) (Datum, error) {
			d := &DDecimal{}
			d.Decimal.SetCoefficient(int64(x.Sign()))
			return d, nil
		}, "Determines the sign of `val`: **1** for positive; **0** for 0 values; **-1** for "+
			"negative."),
		Builtin{
			Types:      ArgTypes{{"val", TypeInt}},
			ReturnType: fixedReturnType(TypeInt),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				x := MustBeDInt(args[0])
				switch {
				case x < 0:
					return NewDInt(-1), nil
				case x == 0:
					return DZero, nil
				}
				return NewDInt(1), nil
			},
			Info: "Determines the sign of `val`: **1** for positive; **0** for 0 values; **-1** " +
				"for negative.",
		},
	},

	"sqrt": {
		floatBuiltin1(func(x float64) (Datum, error) {
			// TODO(mjibson): see #13642
			if x < 0 {
				return nil, errSqrtOfNegNumber
			}
			return NewDFloat(DFloat(math.Sqrt(x))), nil
		}, "Calculates the square root of `val`."),
		decimalBuiltin1(func(x *apd.Decimal) (Datum, error) {
			if x.Sign() < 0 {
				return nil, errSqrtOfNegNumber
			}
			dd := &DDecimal{}
			_, err := DecimalCtx.Sqrt(&dd.Decimal, x)
			return dd, err
		}, "Calculates the square root of `val`."),
	},

	"tan": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(math.Tan(x))), nil
		}, "Calculates the tangent of `val`."),
	},

	"trunc": {
		floatBuiltin1(func(x float64) (Datum, error) {
			return NewDFloat(DFloat(math.Trunc(x))), nil
		}, "Truncates the decimal values of `val`."),
		decimalBuiltin1(func(x *apd.Decimal) (Datum, error) {
			// TODO(mjibson): see cockroachdb/apd#24
			dd := &DDecimal{}
			frac := new(apd.Decimal)
			x.Modf(&dd.Decimal, frac)
			return dd, nil
		}, "Truncates the decimal values of `val`."),
	},

	"to_english": {
		Builtin{
			Types:      ArgTypes{{"val", TypeInt}},
			ReturnType: fixedReturnType(TypeString),
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				val := int(*args[0].(*DInt))
				var buf bytes.Buffer
				if val < 0 {
					buf.WriteString("minus-")
					val = -val
				}
				var digits []string
				digits = append(digits, digitNames[val%10])
				for val > 9 {
					val /= 10
					digits = append(digits, digitNames[val%10])
				}
				for i := len(digits) - 1; i >= 0; i-- {
					if i < len(digits)-1 {
						buf.WriteByte('-')
					}
					buf.WriteString(digits[i])
				}
				return NewDString(buf.String()), nil
			},
			category: categoryString,
			Info:     "This function enunciates the value of its argument using English cardinals.",
		},
	},

	// Array functions.

	"array_length": {
		Builtin{
			Types:      ArgTypes{{"input", TypeAnyArray}, {"array_dimension", TypeInt}},
			ReturnType: fixedReturnType(TypeInt),
			category:   categorySystemInfo,
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				arr := MustBeDArray(args[0])
				dimen := int64(MustBeDInt(args[1]))
				return arrayLength(arr, dimen), nil
			},
			Info: "Calculates the length of `input` on the provided `array_dimension`. However, " +
				"because CockroachDB doesn't yet support multi-dimensional arrays, the only supported" +
				" `array_dimension` is **1**.",
		},
	},

	"array_lower": {
		Builtin{
			Types:      ArgTypes{{"input", TypeAnyArray}, {"array_dimension", TypeInt}},
			ReturnType: fixedReturnType(TypeInt),
			category:   categorySystemInfo,
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				arr := MustBeDArray(args[0])
				dimen := int64(MustBeDInt(args[1]))
				return arrayLower(arr, dimen), nil
			},
			Info: "Calculates the minimum value of `input` on the provided `array_dimension`. " +
				"However, because CockroachDB doesn't yet support multi-dimensional arrays, the only " +
				"supported `array_dimension` is **1**.",
		},
	},

	"array_upper": {
		Builtin{
			Types:      ArgTypes{{"input", TypeAnyArray}, {"array_dimension", TypeInt}},
			ReturnType: fixedReturnType(TypeInt),
			category:   categorySystemInfo,
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				arr := MustBeDArray(args[0])
				dimen := int64(MustBeDInt(args[1]))
				return arrayLength(arr, dimen), nil
			},
			Info: "Calculates the maximum value of `input` on the provided `array_dimension`. " +
				"However, because CockroachDB doesn't yet support multi-dimensional arrays, the only " +
				"supported `array_dimension` is **1**.",
		},
	},

	// Metadata functions.

	"version": {
		Builtin{
			Types:      ArgTypes{},
			ReturnType: fixedReturnType(TypeString),
			category:   categorySystemInfo,
			fn: func(_ *EvalContext, args Datums) (Datum, error) {
				return NewDString(build.GetInfo().Short()), nil
			},
			Info: "Returns the node's version of CockroachDB.",
		},
	},

	"current_database": {
		Builtin{
			Types:      ArgTypes{},
			ReturnType: fixedReturnType(TypeString),
			category:   categorySystemInfo,
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				if len(ctx.Database) == 0 {
					return DNull, nil
				}
				return NewDString(ctx.Database), nil
			},
			Info: "Returns the current database.",
		},
	},

	"current_schema": {
		Builtin{
			Types:      ArgTypes{},
			ReturnType: fixedReturnType(TypeString),
			category:   categorySystemInfo,
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				if len(ctx.Database) == 0 {
					return DNull, nil
				}
				return NewDString(ctx.Database), nil
			},
			Info: "Returns the current schema. This function is provided for " +
				"compatibility with PostgreSQL. For a new CockroachDB application, " +
				"consider using current_database() instead.",
		},
	},

	// For now, schemas are the same as databases. So, current_schemas
	// returns the current database (if one has been set by the user)
	// and the session's database search path.
	"current_schemas": {
		Builtin{
			Types:      ArgTypes{{"include_pg_catalog", TypeBool}},
			ReturnType: fixedReturnType(TypeStringArray),
			category:   categorySystemInfo,
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				includePgCatalog := *(args[0].(*DBool))
				schemas := NewDArray(TypeString)
				if len(ctx.Database) != 0 {
					if err := schemas.Append(NewDString(ctx.Database)); err != nil {
						return nil, err
					}
				}
				for _, p := range ctx.SearchPath {
					if !includePgCatalog && p == "pg_catalog" {
						continue
					}
					if err := schemas.Append(NewDString(p)); err != nil {
						return nil, err
					}
				}
				return schemas, nil
			},
			Info: "Returns the current search path for unqualified names.",
		},
	},

	"crdb_internal.force_internal_error": {
		Builtin{
			Types:      ArgTypes{{"msg", TypeString}},
			ReturnType: fixedReturnType(TypeInt),
			impure:     true,
			privileged: true,
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				msg := string(*args[0].(*DString))
				return nil, pgerror.NewError(pgerror.CodeInternalError, msg)
			},
			category: categorySystemInfo,
			Info:     "This function is used only by CockroachDB's developers for testing purposes.",
		},
	},

	"crdb_internal.force_panic": {
		Builtin{
			Types:      ArgTypes{{"msg", TypeString}},
			ReturnType: fixedReturnType(TypeInt),
			impure:     true,
			privileged: true,
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				msg := string(*args[0].(*DString))
				panic(msg)
			},
			category: categorySystemInfo,
			Info:     "This function is used only by CockroachDB's developers for testing purposes.",
		},
	},

	"crdb_internal.force_log_fatal": {
		Builtin{
			Types:      ArgTypes{{"msg", TypeString}},
			ReturnType: fixedReturnType(TypeInt),
			impure:     true,
			privileged: true,
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				msg := string(*args[0].(*DString))
				log.Fatal(ctx.Ctx(), msg)
				return nil, nil
			},
			category: categorySystemInfo,
			Info:     "This function is used only by CockroachDB's developers for testing purposes.",
		},
	},

	"crdb_internal.force_retry": {
		Builtin{
			Types:      ArgTypes{{"val", TypeInterval}},
			ReturnType: fixedReturnType(TypeInt),
			impure:     true,
			privileged: true,
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				minDuration := args[0].(*DInterval).Duration
				elapsed := duration.Duration{
					Nanos: int64(ctx.stmtTimestamp.Sub(ctx.txnTimestamp)),
				}
				if elapsed.Compare(minDuration) < 0 {
					return nil, roachpb.NewHandledRetryableTxnError(
						"forced by crdb_internal.force_retry()",
						nil, /* txnID */
						roachpb.Transaction{})
				}
				return DZero, nil
			},
			category: categorySystemInfo,
			Info:     "This function is used only by CockroachDB's developers for testing purposes.",
		},
		Builtin{
			Types: ArgTypes{
				{"val", TypeInterval},
				{"txnID", TypeString}},
			ReturnType: fixedReturnType(TypeInt),
			impure:     true,
			fn: func(ctx *EvalContext, args Datums) (Datum, error) {
				minDuration := args[0].(*DInterval).Duration
				txnID := args[1].(*DString)
				elapsed := duration.Duration{
					Nanos: int64(ctx.stmtTimestamp.Sub(ctx.txnTimestamp)),
				}
				if elapsed.Compare(minDuration) < 0 {
					uuid, err := uuid.FromString(string(*txnID))
					if err != nil {
						return nil, err
					}
					return nil, roachpb.NewHandledRetryableTxnError(
						"forced by crdb_internal.force_retry()", &uuid, roachpb.Transaction{})
				}
				return DZero, nil
			},
			category: categorySystemInfo,
			Info:     "This function is used only by CockroachDB's developers for testing purposes.",
		},
	},
}

var substringImpls = []Builtin{
	{
		Types: ArgTypes{
			{"input", TypeString},
			{"substr_pos", TypeInt},
		},
		ReturnType: fixedReturnType(TypeString),
		fn: func(_ *EvalContext, args Datums) (Datum, error) {
			runes := []rune(string(MustBeDString(args[0])))
			// SQL strings are 1-indexed.
			start := int(MustBeDInt(args[1])) - 1

			if start < 0 {
				start = 0
			} else if start > len(runes) {
				start = len(runes)
			}

			return NewDString(string(runes[start:])), nil
		},
		Info: "Returns a substring of `input` starting at `substr_pos` (count starts at 1).",
	},
	{
		Types: ArgTypes{
			{"input", TypeString},
			{"start_pos", TypeInt},
			{"end_pos", TypeInt},
		},
		ReturnType: fixedReturnType(TypeString),
		fn: func(_ *EvalContext, args Datums) (Datum, error) {
			runes := []rune(string(MustBeDString(args[0])))
			// SQL strings are 1-indexed.
			start := int(MustBeDInt(args[1])) - 1
			length := int(MustBeDInt(args[2]))

			if length < 0 {
				return nil, fmt.Errorf("negative substring length %d not allowed", length)
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
		Info: "Returns a substring of `input` between `start_pos` and `end_pos` (count starts at 1).",
	},
	{
		Types: ArgTypes{
			{"input", TypeString},
			{"regex", TypeString},
		},
		ReturnType: fixedReturnType(TypeString),
		fn: func(ctx *EvalContext, args Datums) (Datum, error) {
			s := string(MustBeDString(args[0]))
			pattern := string(MustBeDString(args[1]))
			return regexpExtract(ctx, s, pattern, `\`)
		},
		Info: "Returns a substring of `input` that matches the regular expression `regex`.",
	},
	{
		Types: ArgTypes{
			{"input", TypeString},
			{"regex", TypeString},
			{"escape_char", TypeString},
		},
		ReturnType: fixedReturnType(TypeString),
		fn: func(ctx *EvalContext, args Datums) (Datum, error) {
			s := string(MustBeDString(args[0]))
			pattern := string(MustBeDString(args[1]))
			escape := string(MustBeDString(args[2]))
			return regexpExtract(ctx, s, pattern, escape)
		},
		Info: "Returns a substring of `input` that matches the regular expression `regex` using " +
			"`escape_char` as your escape character instead of `\\`.",
	},
}

var uuidV4Impl = Builtin{
	Types:      ArgTypes{},
	ReturnType: fixedReturnType(TypeBytes),
	category:   categoryIDGeneration,
	impure:     true,
	fn: func(_ *EvalContext, args Datums) (Datum, error) {
		return NewDBytes(DBytes(uuid.MakeV4().GetBytes())), nil
	},
	Info: "Returns a UUID.",
}

var ceilImpl = []Builtin{
	floatBuiltin1(func(x float64) (Datum, error) {
		return NewDFloat(DFloat(math.Ceil(x))), nil
	}, "Calculates the smallest integer greater than `val`."),
	decimalBuiltin1(func(x *apd.Decimal) (Datum, error) {
		dd := &DDecimal{}
		_, err := ExactCtx.Ceil(&dd.Decimal, x)
		return dd, err
	}, "Calculates the smallest integer greater than `val`."),
}

var txnTSImpl = []Builtin{
	{
		Types:             ArgTypes{},
		ReturnType:        fixedReturnType(TypeTimestampTZ),
		preferredOverload: true,
		impure:            true,
		fn: func(ctx *EvalContext, args Datums) (Datum, error) {
			return ctx.GetTxnTimestamp(time.Microsecond), nil
		},
		Info: "Returns the current transaction's timestamp.",
	},
	{
		Types:      ArgTypes{},
		ReturnType: fixedReturnType(TypeTimestamp),
		impure:     true,
		fn: func(ctx *EvalContext, args Datums) (Datum, error) {
			return ctx.GetTxnTimestampNoZone(time.Microsecond), nil
		},
		Info: "Returns the current transaction's timestamp.",
	},
}

var powImpls = []Builtin{
	floatBuiltin2("x", "y", func(x, y float64) (Datum, error) {
		return NewDFloat(DFloat(math.Pow(x, y))), nil
	}, "Calculates `x`^`y`."),
	decimalBuiltin2("x", "y", func(x, y *apd.Decimal) (Datum, error) {
		dd := &DDecimal{}
		_, err := DecimalCtx.Pow(&dd.Decimal, x, y)
		return dd, err
	}, "Calculates `x`^`y`."),
	{
		Types: ArgTypes{
			{"x", TypeInt},
			{"y", TypeInt},
		},
		ReturnType: fixedReturnType(TypeInt),
		fn: func(_ *EvalContext, args Datums) (Datum, error) {
			return intPow(MustBeDInt(args[0]), MustBeDInt(args[1]))
		},
		Info: "Calculates `x`^`y`.",
	},
}

func decimalLogFn(
	logFn func(*apd.Decimal, *apd.Decimal) (apd.Condition, error), info string,
) Builtin {
	return decimalBuiltin1(func(x *apd.Decimal) (Datum, error) {
		// TODO(mjibson): see #13642
		switch x.Sign() {
		case -1:
			return nil, errLogOfNegNumber
		case 0:
			return nil, errLogOfZero
		}
		dd := &DDecimal{}
		_, err := logFn(&dd.Decimal, x)
		return dd, err
	}, info)
}

func floatBuiltin1(f func(float64) (Datum, error), info string) Builtin {
	return Builtin{
		Types:      ArgTypes{{"val", TypeFloat}},
		ReturnType: fixedReturnType(TypeFloat),
		fn: func(_ *EvalContext, args Datums) (Datum, error) {
			return f(float64(*args[0].(*DFloat)))
		},
		Info: info,
	}
}

func floatBuiltin2(a, b string, f func(float64, float64) (Datum, error), info string) Builtin {
	return Builtin{
		Types:      ArgTypes{{a, TypeFloat}, {b, TypeFloat}},
		ReturnType: fixedReturnType(TypeFloat),
		fn: func(_ *EvalContext, args Datums) (Datum, error) {
			return f(float64(*args[0].(*DFloat)),
				float64(*args[1].(*DFloat)))
		},
		Info: info,
	}
}

func decimalBuiltin1(f func(*apd.Decimal) (Datum, error), info string) Builtin {
	return Builtin{
		Types:      ArgTypes{{"val", TypeDecimal}},
		ReturnType: fixedReturnType(TypeDecimal),
		fn: func(_ *EvalContext, args Datums) (Datum, error) {
			dec := &args[0].(*DDecimal).Decimal
			return f(dec)
		},
		Info: info,
	}
}

func decimalBuiltin2(
	a, b string, f func(*apd.Decimal, *apd.Decimal) (Datum, error), info string,
) Builtin {
	return Builtin{
		Types:      ArgTypes{{a, TypeDecimal}, {b, TypeDecimal}},
		ReturnType: fixedReturnType(TypeDecimal),
		fn: func(_ *EvalContext, args Datums) (Datum, error) {
			dec1 := &args[0].(*DDecimal).Decimal
			dec2 := &args[1].(*DDecimal).Decimal
			return f(dec1, dec2)
		},
		Info: info,
	}
}

func stringBuiltin1(f func(string) (Datum, error), returnType Type, info string) Builtin {
	return Builtin{
		Types:      ArgTypes{{"val", TypeString}},
		ReturnType: fixedReturnType(returnType),
		fn: func(_ *EvalContext, args Datums) (Datum, error) {
			return f(string(MustBeDString(args[0])))
		},
		Info: info,
	}
}

func stringBuiltin2(
	a, b string, f func(string, string) (Datum, error), returnType Type, info string,
) Builtin {
	return Builtin{
		Types:      ArgTypes{{a, TypeString}, {b, TypeString}},
		ReturnType: fixedReturnType(returnType),
		category:   categorizeType(TypeString),
		fn: func(_ *EvalContext, args Datums) (Datum, error) {
			return f(string(MustBeDString(args[0])), string(MustBeDString(args[1])))
		},
		Info: info,
	}
}

func stringBuiltin3(
	a, b, c string, f func(string, string, string) (Datum, error), returnType Type, info string,
) Builtin {
	return Builtin{
		Types:      ArgTypes{{a, TypeString}, {b, TypeString}, {c, TypeString}},
		ReturnType: fixedReturnType(returnType),
		fn: func(_ *EvalContext, args Datums) (Datum, error) {
			return f(string(MustBeDString(args[0])), string(MustBeDString(args[1])), string(MustBeDString(args[2])))
		},
		Info: info,
	}
}

func bytesBuiltin1(f func(string) (Datum, error), returnType Type, info string) Builtin {
	return Builtin{
		Types:      ArgTypes{{"val", TypeBytes}},
		ReturnType: fixedReturnType(returnType),
		fn: func(_ *EvalContext, args Datums) (Datum, error) {
			return f(string(*args[0].(*DBytes)))
		},
		Info: info,
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
				panic(fmt.Sprintf("invalid integer submatch reference seen: %v", err))
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
	flags := syntax.DotNL | syntax.OneLine

	for _, sqlFlag := range sqlFlags {
		switch sqlFlag {
		case 'g':
			// Handled in `regexpReplace`.
		case 'i':
			flags |= syntax.FoldCase
		case 'c':
			flags &^= syntax.FoldCase
		case 's':
			flags &^= syntax.DotNL
		case 'm', 'n':
			flags &^= syntax.DotNL
			flags &^= syntax.OneLine
		case 'p':
			flags &^= syntax.DotNL
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

// Transcribed from Postgres' src/port/rint.c, with c-style comments preserved
// for ease of mapping.
//
// https://github.com/postgres/postgres/blob/REL9_6_3/src/port/rint.c
func round(x float64) float64 {
	/* Per POSIX, NaNs must be returned unchanged. */
	if math.IsNaN(x) {
		return x
	}

	/* Both positive and negative zero should be returned unchanged. */
	if x == 0.0 {
		return x
	}

	roundFn := math.Ceil
	if math.Signbit(x) {
		roundFn = math.Floor
	}

	/*
	 * Subtracting 0.5 from a number very close to -0.5 can round to
	 * exactly -1.0, producing incorrect results, so we take the opposite
	 * approach: add 0.5 to the negative number, so that it goes closer to
	 * zero (or at most to +0.5, which is dealt with next), avoiding the
	 * precision issue.
	 */
	xOrig := x
	x -= math.Copysign(0.5, x)

	/*
	 * Be careful to return minus zero when input+0.5 >= 0, as that's what
	 * rint() should return with negative input.
	 */
	if x == 0 || math.Signbit(x) != math.Signbit(xOrig) {
		return math.Copysign(0.0, xOrig)
	}

	/*
	 * For very big numbers the input may have no decimals.  That case is
	 * detected by testing x+0.5 == x+1.0; if that happens, the input is
	 * returned unchanged.  This also covers the case of minus infinity.
	 */
	if x == xOrig-math.Copysign(1.0, x) {
		return xOrig
	}

	/* Otherwise produce a rounded estimate. */
	r := roundFn(x)

	/*
	 * If the rounding did not produce exactly input+0.5 then we're done.
	 */
	if r != x {
		return r
	}

	/*
	 * The original fractional part was exactly 0.5 (since
	 * floor(input+0.5) == input+0.5).  We need to round to nearest even.
	 * Dividing input+0.5 by 2, taking the floor and multiplying by 2
	 * yields the closest even number.  This part assumes that division by
	 * 2 is exact, which should be OK because underflow is impossible
	 * here: x is an integer.
	 */
	return roundFn(x*0.5) * 2.0
}

func roundDecimal(x *apd.Decimal, n int32) (Datum, error) {
	dd := &DDecimal{}
	_, err := HighPrecisionCtx.Quantize(&dd.Decimal, x, -n)
	return dd, err
}

// Pick the greatest (or least value) from a tuple.
func pickFromTuple(ctx *EvalContext, greatest bool, args Datums) (Datum, error) {
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

func intPow(x, y DInt) (*DInt, error) {
	xd := apd.New(int64(x), 0)
	yd := apd.New(int64(y), 0)
	_, err := DecimalCtx.Pow(xd, xd, yd)
	if err != nil {
		return nil, err
	}
	i, err := xd.Int64()
	if err != nil {
		return nil, errIntOutOfRange
	}
	return NewDInt(DInt(i)), nil
}

var uniqueIntState struct {
	syncutil.Mutex
	timestamp uint64
}

var uniqueIntEpoch = time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano()

// GenerateUniqueInt creates a unique int composed of the current time at a
// 10-microsecond granularity and the node-id. The node-id is stored in the
// lower 15 bits of the returned value and the timestamp is stored in the upper
// 48 bits. The top-bit is left empty so that negative values are not returned.
// The 48-bit timestamp field provides for 89 years of timestamps. We use a
// custom epoch (Jan 1, 2015) in order to utilize the entire timestamp range.
//
// Note that GenerateUniqueInt() imposes a limit on node IDs while
// generateUniqueBytes() does not.
//
// TODO(pmattis): Do we have to worry about persisting the milliseconds value
// periodically to avoid the clock ever going backwards (e.g. due to NTP
// adjustment)?
func GenerateUniqueInt(nodeID roachpb.NodeID) DInt {
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

func arrayLength(arr *DArray, dim int64) Datum {
	if arr.Len() == 0 || dim < 1 {
		return DNull
	}
	if dim == 1 {
		return NewDInt(DInt(arr.Len()))
	}
	a, ok := AsDArray(arr.Array[0])
	if !ok {
		return DNull
	}
	return arrayLength(a, dim-1)
}

var intOne = NewDInt(DInt(1))

func arrayLower(arr *DArray, dim int64) Datum {
	if arr.Len() == 0 || dim < 1 {
		return DNull
	}
	if dim == 1 {
		return intOne
	}
	a, ok := AsDArray(arr.Array[0])
	if !ok {
		return DNull
	}
	return arrayLower(a, dim-1)
}

func extractStringFromTimestamp(
	_ *EvalContext, fromTime time.Time, timeSpan string,
) (Datum, error) {
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
		return nil, fmt.Errorf("unsupported timespan: %s", timeSpan)
	}
}
