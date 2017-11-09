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

package builtins

import (
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"hash"
	"hash/crc32"
	"hash/fnv"
	"math"
	"math/rand"
	"net"
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
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/pkg/errors"
)

var (
	errEmptyInputString = pgerror.NewError(pgerror.CodeInvalidParameterValueError, "the input string must not be empty")
	errAbsOfMinInt64    = pgerror.NewError(pgerror.CodeNumericValueOutOfRangeError, "abs of min integer value (-9223372036854775808) not defined")
	errSqrtOfNegNumber  = pgerror.NewError(pgerror.CodeInvalidArgumentForPowerFunctionError, "cannot take square root of a negative number")
	errLogOfNegNumber   = pgerror.NewError(pgerror.CodeInvalidArgumentForLogarithmError, "cannot take logarithm of a negative number")
	errLogOfZero        = pgerror.NewError(pgerror.CodeInvalidArgumentForLogarithmError, "cannot take logarithm of zero")
	errZeroIP           = pgerror.NewError(pgerror.CodeInvalidParameterValueError, "zero length IP")
)

const errInsufficientArgsFmtString = "unknown signature: %s()"

const (
	categoryComparison    = "Comparison"
	categoryCompatibility = "Compatibility"
	categoryDateAndTime   = "Date and Time"
	categoryIDGeneration  = "ID Generation"
	categoryMath          = "Math and Numeric"
	categoryString        = "String and Byte"
	categoryArray         = "Array"
	categorySystemInfo    = "System Info"
)

func categorizeType(t types.T) string {
	switch t {
	case types.Date, types.Interval, types.Timestamp, types.TimestampTZ:
		return categoryDateAndTime
	case types.Int, types.Decimal, types.Float:
		return categoryMath
	case types.String, types.Bytes:
		return categoryString
	default:
		return strings.ToUpper(t.String())
	}
}

var digitNames = []string{"zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"}

// Builtins contains the built-in functions indexed by name.
var Builtins = map[string][]parser.Builtin{
	// TODO(XisiHuang): support encoding, i.e., length(str, encoding).
	"length": {
		stringBuiltin1(func(_ *parser.EvalContext, s string) (parser.Datum, error) {
			return parser.NewDInt(parser.DInt(utf8.RuneCountInString(s))), nil
		}, types.Int, "Calculates the number of characters in `val`."),
		bytesBuiltin1(func(_ *parser.EvalContext, s string) (parser.Datum, error) {
			return parser.NewDInt(parser.DInt(len(s))), nil
		}, types.Int, "Calculates the number of bytes in `val`."),
	},

	"octet_length": {
		stringBuiltin1(func(_ *parser.EvalContext, s string) (parser.Datum, error) {
			return parser.NewDInt(parser.DInt(len(s))), nil
		}, types.Int, "Calculates the number of bytes used to represent `val`."),
		bytesBuiltin1(func(_ *parser.EvalContext, s string) (parser.Datum, error) {
			return parser.NewDInt(parser.DInt(len(s))), nil
		}, types.Int, "Calculates the number of bytes in `val`."),
	},

	// TODO(pmattis): What string functions should also support types.Bytes?

	"lower": {stringBuiltin1(func(evalCtx *parser.EvalContext, s string) (parser.Datum, error) {
		if err := evalCtx.ActiveMemAcc.Grow(evalCtx.Ctx(), int64(len(s))); err != nil {
			return nil, err
		}
		return parser.NewDString(strings.ToLower(s)), nil
	}, types.String, "Converts all characters in `val` to their lower-case equivalents.")},

	"upper": {stringBuiltin1(func(evalCtx *parser.EvalContext, s string) (parser.Datum, error) {
		if err := evalCtx.ActiveMemAcc.Grow(evalCtx.Ctx(), int64(len(s))); err != nil {
			return nil, err
		}
		return parser.NewDString(strings.ToUpper(s)), nil
	}, types.String, "Converts all characters in `val` to their to their upper-case equivalents.")},

	"substr":    substringImpls,
	"substring": substringImpls,

	// concat concatenates the text representations of all the arguments.
	// NULL arguments are ignored.
	"concat": {
		parser.Builtin{
			Types:        parser.VariadicType{Typ: types.String},
			ReturnType:   parser.FixedReturnType(types.String),
			NullableArgs: true,
			Fn: func(evalCtx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				var buffer bytes.Buffer
				for _, d := range args {
					if d == parser.DNull {
						continue
					}
					nextLength := len(string(parser.MustBeDString(d)))
					if err := evalCtx.ActiveMemAcc.Grow(evalCtx.Ctx(), int64(nextLength)); err != nil {
						return nil, err
					}
					buffer.WriteString(string(parser.MustBeDString(d)))
				}
				return parser.NewDString(buffer.String()), nil
			},
			Info: "Concatenates a comma-separated list of strings.",
		},
	},

	"concat_ws": {
		parser.Builtin{
			Types:        parser.VariadicType{Typ: types.String},
			ReturnType:   parser.FixedReturnType(types.String),
			NullableArgs: true,
			Fn: func(evalCtx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				if len(args) == 0 {
					return nil, pgerror.NewErrorf(pgerror.CodeUndefinedFunctionError, errInsufficientArgsFmtString, "concat_ws")
				}
				if args[0] == parser.DNull {
					return parser.DNull, nil
				}
				sep := string(parser.MustBeDString(args[0]))
				var buf bytes.Buffer
				prefix := ""
				for _, d := range args[1:] {
					if d == parser.DNull {
						continue
					}
					nextLength := len(prefix) + len(string(parser.MustBeDString(d)))
					if err := evalCtx.ActiveMemAcc.Grow(
						evalCtx.Ctx(), int64(nextLength)); err != nil {
						return nil, err
					}
					// Note: we can't use the range index here because that
					// would break when the 2nd argument is NULL.
					buf.WriteString(prefix)
					prefix = sep
					buf.WriteString(string(parser.MustBeDString(d)))
				}
				return parser.NewDString(buf.String()), nil
			},
			Info: "Uses the first argument as a separator between the concatenation of the " +
				"subsequent arguments. \n\nFor example `concat_ws('!','wow','great')` " +
				"returns `wow!great`.",
		},
	},

	"gen_random_uuid": {
		parser.Builtin{
			Types:      parser.ArgTypes{},
			ReturnType: parser.FixedReturnType(types.UUID),
			Category:   categoryIDGeneration,
			Impure:     true,
			Fn: func(_ *parser.EvalContext, _ parser.Datums) (parser.Datum, error) {
				uv := uuid.MakeV4()
				return parser.NewDUuid(parser.DUuid{UUID: uv}), nil
			},
			Info: "Generates a random UUID and returns it as a value of UUID type.",
		},
	},

	"to_uuid": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"val", types.String}},
			ReturnType: parser.FixedReturnType(types.Bytes),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				s := string(parser.MustBeDString(args[0]))
				uv, err := uuid.FromString(s)
				if err != nil {
					return nil, err
				}
				return parser.NewDBytes(parser.DBytes(uv.GetBytes())), nil
			},
			Info: "Converts the character string representation of a UUID to its byte string " +
				"representation.",
		},
	},

	"from_uuid": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"val", types.Bytes}},
			ReturnType: parser.FixedReturnType(types.String),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				b := []byte(*args[0].(*parser.DBytes))
				uv, err := uuid.FromBytes(b)
				if err != nil {
					return nil, err
				}
				return parser.NewDString(uv.String()), nil
			},
			Info: "Converts the byte string representation of a UUID to its character string " +
				"representation.",
		},
	},

	// The following functions are all part of the NET address functions. They can
	// be found in the postgres reference at https://www.postgresql.org/docs/9.6/static/functions-net.html#CIDR-INET-FUNCTIONS-TABLE
	// This includes:
	// - abbrev
	// - broadcast
	// - family
	// - host
	// - hostmask
	// - masklen
	// - netmask
	// - set_masklen
	// - text(inet)
	// - inet_same_family

	"abbrev": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"val", types.INet}},
			ReturnType: parser.FixedReturnType(types.String),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				dIPAddr := parser.MustBeDIPAddr(args[0])
				return parser.NewDString(dIPAddr.IPAddr.String()), nil
			},
			Info: "Converts the combined IP address and prefix length to an abbreviated display format as text." +
				"For INET types, this will omit the prefix length if it's not the default (32 or IPv4, 128 for IPv6)" +
				"\n\nFor example, `abbrev('192.168.1.2/24')` returns `'192.168.1.2/24'`",
		},
	},

	"broadcast": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"val", types.INet}},
			ReturnType: parser.FixedReturnType(types.INet),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				dIPAddr := parser.MustBeDIPAddr(args[0])
				broadcastIPAddr := dIPAddr.IPAddr.Broadcast()
				return &parser.DIPAddr{IPAddr: broadcastIPAddr}, nil
			},
			Info: "Gets the broadcast address for the network address represented by the value." +
				"\n\nFor example, `broadcast('192.168.1.2/24')` returns `'192.168.1.255/24'`",
		},
	},

	"family": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"val", types.INet}},
			ReturnType: parser.FixedReturnType(types.Int),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				dIPAddr := parser.MustBeDIPAddr(args[0])
				if dIPAddr.Family == ipaddr.IPv4family {
					return parser.NewDInt(parser.DInt(4)), nil
				}
				return parser.NewDInt(parser.DInt(6)), nil
			},
			Info: "Extracts the IP family of the value; 4 for IPv4, 6 for IPv6." +
				"\n\nFor example, `family('::1')` returns `6`",
		},
	},

	"host": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"val", types.INet}},
			ReturnType: parser.FixedReturnType(types.String),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				dIPAddr := parser.MustBeDIPAddr(args[0])
				s := dIPAddr.IPAddr.String()
				if i := strings.IndexByte(s, '/'); i != -1 {
					return parser.NewDString(s[:i]), nil
				}
				return parser.NewDString(s), nil
			},
			Info: "Extracts the address part of the combined address/prefixlen value as text." +
				"\n\nFor example, `host('192.168.1.2/16')` returns `'192.168.1.2'`",
		},
	},

	"hostmask": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"val", types.INet}},
			ReturnType: parser.FixedReturnType(types.INet),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				dIPAddr := parser.MustBeDIPAddr(args[0])
				ipAddr := dIPAddr.IPAddr.Hostmask()
				return &parser.DIPAddr{IPAddr: ipAddr}, nil
			},
			Info: "Creates an IP host mask corresponding to the prefix length in the value." +
				"\n\nFor example, `hostmask('192.168.1.2/16')` returns `'0.0.255.255'`",
		},
	},

	"masklen": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"val", types.INet}},
			ReturnType: parser.FixedReturnType(types.Int),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				dIPAddr := parser.MustBeDIPAddr(args[0])
				return parser.NewDInt(parser.DInt(dIPAddr.Mask)), nil
			},
			Info: "Retrieves the prefix length stored in the value." +
				"\n\nFor example, `masklen('192.168.1.2/16')` returns `16`",
		},
	},

	"netmask": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"val", types.INet}},
			ReturnType: parser.FixedReturnType(types.INet),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				dIPAddr := parser.MustBeDIPAddr(args[0])
				ipAddr := dIPAddr.IPAddr.Netmask()
				return &parser.DIPAddr{IPAddr: ipAddr}, nil
			},
			Info: "Creates an IP network mask corresponding to the prefix length in the value." +
				"\n\nFor example, `netmask('192.168.1.2/16')` returns `'255.255.0.0'`",
		},
	},

	"set_masklen": {
		parser.Builtin{
			Types: parser.ArgTypes{
				{"val", types.INet},
				{"prefixlen", types.Int},
			},
			ReturnType: parser.FixedReturnType(types.INet),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				dIPAddr := parser.MustBeDIPAddr(args[0])
				mask := int(parser.MustBeDInt(args[1]))

				if !(dIPAddr.Family == ipaddr.IPv4family && mask >= 0 && mask <= 32) && !(dIPAddr.Family == ipaddr.IPv6family && mask >= 0 && mask <= 128) {
					return nil, pgerror.NewErrorf(
						pgerror.CodeInvalidParameterValueError, "invalid mask length: %d", mask)
				}
				return &parser.DIPAddr{IPAddr: ipaddr.IPAddr{Family: dIPAddr.Family, Addr: dIPAddr.Addr, Mask: byte(mask)}}, nil
			},
			Info: "Sets the prefix length of `val` to `prefixlen`.\n\n" +
				"For example, `set_masklen('192.168.1.2', 16)` returns `'192.168.1.2/16'`.",
		},
	},

	"text": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"val", types.INet}},
			ReturnType: parser.FixedReturnType(types.String),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				dIPAddr := parser.MustBeDIPAddr(args[0])
				s := dIPAddr.IPAddr.String()
				// Ensure the string has a "/mask" suffix.
				if strings.IndexByte(s, '/') == -1 {
					s += "/" + strconv.Itoa(int(dIPAddr.Mask))
				}
				return parser.NewDString(s), nil
			},
			Info: "Converts the IP address and prefix length to text.",
		},
	},

	"inet_same_family": {
		parser.Builtin{
			Types: parser.ArgTypes{
				{"val", types.INet},
				{"val", types.INet},
			},
			ReturnType: parser.FixedReturnType(types.Bool),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				first := parser.MustBeDIPAddr(args[0])
				other := parser.MustBeDIPAddr(args[1])
				return parser.MakeDBool(parser.DBool(first.Family == other.Family)), nil
			},
			Info: "Checks if two IP addresses are of the same IP family.",
		},
	},

	"from_ip": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"val", types.Bytes}},
			ReturnType: parser.FixedReturnType(types.String),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				ipstr := args[0].(*parser.DBytes)
				nboip := net.IP(*ipstr)
				sv := nboip.String()
				// if nboip has a length of 0, sv will be "<nil>"
				if sv == "<nil>" {
					return nil, errZeroIP
				}
				return parser.NewDString(sv), nil
			},
			Info: "Converts the byte string representation of an IP to its character string " +
				"representation.",
		},
	},

	"to_ip": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"val", types.String}},
			ReturnType: parser.FixedReturnType(types.Bytes),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				ipdstr := parser.MustBeDString(args[0])
				ip := net.ParseIP(string(ipdstr))
				// If ipdstr could not be parsed to a valid IP,
				// ip will be nil.
				if ip == nil {
					return nil, pgerror.NewErrorf(
						pgerror.CodeInvalidParameterValueError, "invalid IP format: %s", ipdstr.String())
				}
				return parser.NewDBytes(parser.DBytes(ip)), nil
			},
			Info: "Converts the character string representation of an IP to its byte string " +
				"representation.",
		},
	},

	"split_part": {
		parser.Builtin{
			Types: parser.ArgTypes{
				{"input", types.String},
				{"delimiter", types.String},
				{"return_index_pos", types.Int},
			},
			ReturnType: parser.FixedReturnType(types.String),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				text := string(parser.MustBeDString(args[0]))
				sep := string(parser.MustBeDString(args[1]))
				field := int(parser.MustBeDInt(args[2]))

				if field <= 0 {
					return nil, pgerror.NewErrorf(
						pgerror.CodeInvalidParameterValueError, "field position %d must be greater than zero", field)
				}

				splits := strings.Split(text, sep)
				if field > len(splits) {
					return parser.NewDString(""), nil
				}
				return parser.NewDString(splits[field-1]), nil
			},
			Info: "Splits `input` on `delimiter` and return the value in the `return_index_pos`  " +
				"position (starting at 1). \n\nFor example, `split_part('123.456.789.0','.',3)`" +
				"returns `789`.",
		},
	},

	"repeat": {
		parser.Builtin{
			Types:            parser.ArgTypes{{"input", types.String}, {"repeat_counter", types.Int}},
			DistsqlBlacklist: true,
			ReturnType:       parser.FixedReturnType(types.String),
			Fn: func(evalCtx *parser.EvalContext, args parser.Datums) (_ parser.Datum, err error) {
				s := string(parser.MustBeDString(args[0]))
				count := int(parser.MustBeDInt(args[1]))

				ln := len(s) * count
				// Use <= here instead of < to prevent a possible divide-by-zero in the next
				// if block.
				if count <= 0 {
					count = 0
				} else if ln/count != len(s) {
					// Detect overflow and trigger an error.
					return nil, pgerror.NewError(
						pgerror.CodeProgramLimitExceededError, "requested length too large",
					)
				}

				if err := evalCtx.ActiveMemAcc.Grow(evalCtx.Ctx(), int64(ln)); err != nil {
					return nil, err
				}
				return parser.NewDString(strings.Repeat(s, count)), nil
			},
			Info: "Concatenates `input` `repeat_counter` number of times.\n\nFor example, " +
				"`repeat('dog', 2)` returns `dogdog`.",
		},
	},

	// https://www.postgresql.org/docs/10/static/functions-binarystring.html
	"encode": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"data", types.Bytes}, {"format", types.String}},
			ReturnType: parser.FixedReturnType(types.String),
			Fn: func(evalCtx *parser.EvalContext, args parser.Datums) (_ parser.Datum, err error) {
				data, format := string(*args[0].(*parser.DBytes)), string(parser.MustBeDString(args[1]))
				if format != "hex" {
					return nil, pgerror.NewError(pgerror.CodeInvalidParameterValueError, "only 'hex' format is supported for ENCODE")
				}
				if !utf8.ValidString(data) {
					return nil, pgerror.NewError(pgerror.CodeCharacterNotInRepertoireError, "invalid UTF-8 sequence")
				}
				return parser.NewDString(data), nil
			},
			Info: "Encodes `data` in the text format specified by `format` (only \"hex\" is supported).",
		},
	},

	"decode": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"text", types.String}, {"format", types.String}},
			ReturnType: parser.FixedReturnType(types.String),
			Fn: func(evalCtx *parser.EvalContext, args parser.Datums) (_ parser.Datum, err error) {
				data, format := string(parser.MustBeDString(args[0])), string(parser.MustBeDString(args[1]))
				if format != "hex" {
					return nil, pgerror.NewError(pgerror.CodeInvalidParameterValueError, "only 'hex' format is supported for DECODE")
				}
				var buf bytes.Buffer
				lex.HexEncodeString(&buf, data)
				return parser.NewDString(buf.String()), nil
			},
			Info: "Decodes `data` as the format specified by `format` (only \"hex\" is supported).",
		},
	},

	"ascii": {stringBuiltin1(func(_ *parser.EvalContext, s string) (parser.Datum, error) {
		for _, ch := range s {
			return parser.NewDInt(parser.DInt(ch)), nil
		}
		return nil, errEmptyInputString
	}, types.Int, "Calculates the ASCII value for the first character in `val`.")},

	"md5": hashBuiltin(
		func() hash.Hash { return md5.New() },
		"Calculates the MD5 hash value of a set of values.",
	),

	"sha1": hashBuiltin(
		func() hash.Hash { return sha1.New() },
		"Calculates the SHA1 hash value of a set of values.",
	),

	"sha256": hashBuiltin(
		func() hash.Hash { return sha256.New() },
		"Calculates the SHA256 hash value of a set of values.",
	),

	"sha512": hashBuiltin(
		func() hash.Hash { return sha512.New() },
		"Calculates the SHA512 hash value of a set of values.",
	),

	"fnv32": hash32Builtin(
		func() hash.Hash32 { return fnv.New32() },
		"Calculates the 32-bit FNV-1 hash value of a set of values.",
	),

	"fnv32a": hash32Builtin(
		func() hash.Hash32 { return fnv.New32a() },
		"Calculates the 32-bit FNV-1a hash value of a set of values.",
	),

	"fnv64": hash64Builtin(
		func() hash.Hash64 { return fnv.New64() },
		"Calculates the 64-bit FNV-1 hash value of a set of values.",
	),

	"fnv64a": hash64Builtin(
		func() hash.Hash64 { return fnv.New64a() },
		"Calculates the 64-bit FNV-1a hash value of a set of values.",
	),

	"crc32ieee": hash32Builtin(
		func() hash.Hash32 { return crc32.New(crc32.IEEETable) },
		"Calculates the CRC-32 hash using the IEEE polynomial.",
	),

	"crc32c": hash32Builtin(
		func() hash.Hash32 { return crc32.New(crc32.MakeTable(crc32.Castagnoli)) },
		"Calculates the CRC-32 hash using the Castagnoli polynomial.",
	),

	"to_hex": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"val", types.Int}},
			ReturnType: parser.FixedReturnType(types.String),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				return parser.NewDString(fmt.Sprintf("%x", int64(parser.MustBeDInt(args[0])))), nil
			},
			Info: "Converts `val` to its hexadecimal representation.",
		},
		parser.Builtin{
			Types:      parser.ArgTypes{{"val", types.Bytes}},
			ReturnType: parser.FixedReturnType(types.String),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				bytes := *(args[0].(*parser.DBytes))
				return parser.NewDString(fmt.Sprintf("%x", []byte(bytes))), nil
			},
			Info: "Converts `val` to its hexadecimal representation.",
		},
	},

	// The SQL parser coerces POSITION to STRPOS.
	"strpos": {stringBuiltin2("input", "find", func(_ *parser.EvalContext, s, substring string) (parser.Datum, error) {
		index := strings.Index(s, substring)
		if index < 0 {
			return parser.DZero, nil
		}

		return parser.NewDInt(parser.DInt(utf8.RuneCountInString(s[:index]) + 1)), nil
	}, types.Int, "Calculates the position where the string `find` begins in `input`. \n\nFor"+
		" example, `strpos('doggie', 'gie')` returns `4`.")},

	"overlay": {
		parser.Builtin{
			Types: parser.ArgTypes{
				{"input", types.String},
				{"overlay_val", types.String},
				{"start_pos", types.Int},
			},
			ReturnType: parser.FixedReturnType(types.String),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				s := string(parser.MustBeDString(args[0]))
				to := string(parser.MustBeDString(args[1]))
				pos := int(parser.MustBeDInt(args[2]))
				size := utf8.RuneCountInString(to)
				return overlay(s, to, pos, size)
			},
			Info: "Replaces characters in `input` with `overlay_val` starting at `start_pos` " +
				"(begins at 1). \n\nFor example, `overlay('doggie', 'CAT', 2)` returns " +
				"`dCATie`.",
		},
		parser.Builtin{
			Types: parser.ArgTypes{
				{"input", types.String},
				{"overlay_val", types.String},
				{"start_pos", types.Int},
				{"end_pos", types.Int},
			},
			ReturnType: parser.FixedReturnType(types.String),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				s := string(parser.MustBeDString(args[0]))
				to := string(parser.MustBeDString(args[1]))
				pos := int(parser.MustBeDInt(args[2]))
				size := int(parser.MustBeDInt(args[3]))
				return overlay(s, to, pos, size)
			},
			Info: "Deletes the characters in `input` between `start_pos` and `end_pos` (count " +
				"starts at 1), and then insert `overlay_val` at `start_pos`.",
		},
	},

	// The SQL parser coerces TRIM(...) and TRIM(BOTH ...) to BTRIM(...).
	"btrim": {
		stringBuiltin2("input", "trim_chars", func(_ *parser.EvalContext, s, chars string) (parser.Datum, error) {
			return parser.NewDString(strings.Trim(s, chars)), nil
		}, types.String, "Removes any characters included in `trim_chars` from the beginning or end"+
			" of `input` (applies recursively). \n\nFor example, `btrim('doggie', 'eod')` "+
			"returns `ggi`."),
		stringBuiltin1(func(_ *parser.EvalContext, s string) (parser.Datum, error) {
			return parser.NewDString(strings.TrimSpace(s)), nil
		}, types.String, "Removes all spaces from the beginning and end of `val`."),
	},

	// The SQL parser coerces TRIM(LEADING ...) to LTRIM(...).
	"ltrim": {
		stringBuiltin2("input", "trim_chars", func(_ *parser.EvalContext, s, chars string) (parser.Datum, error) {
			return parser.NewDString(strings.TrimLeft(s, chars)), nil
		}, types.String, "Removes any characters included in `trim_chars` from the beginning "+
			"(left-hand side) of `input` (applies recursively). \n\nFor example, "+
			"`ltrim('doggie', 'od')` returns `ggie`."),
		stringBuiltin1(func(_ *parser.EvalContext, s string) (parser.Datum, error) {
			return parser.NewDString(strings.TrimLeftFunc(s, unicode.IsSpace)), nil
		}, types.String, "Removes all spaces from the beginning (left-hand side) of `val`."),
	},

	// The SQL parser coerces TRIM(TRAILING ...) to RTRIM(...).
	"rtrim": {
		stringBuiltin2("input", "trim_chars", func(_ *parser.EvalContext, s, chars string) (parser.Datum, error) {
			return parser.NewDString(strings.TrimRight(s, chars)), nil
		}, types.String, "Removes any characters included in `trim_chars` from the end (right-hand "+
			"side) of `input` (applies recursively). \n\nFor example, `rtrim('doggie', 'ei')` "+
			"returns `dogg`."),
		stringBuiltin1(func(_ *parser.EvalContext, s string) (parser.Datum, error) {
			return parser.NewDString(strings.TrimRightFunc(s, unicode.IsSpace)), nil
		}, types.String, "Removes all spaces from the end (right-hand side) of `val`."),
	},

	"reverse": {stringBuiltin1(func(evalCtx *parser.EvalContext, s string) (parser.Datum, error) {
		if err := evalCtx.ActiveMemAcc.Grow(evalCtx.Ctx(), int64(len(s))); err != nil {
			return nil, err
		}
		runes := []rune(s)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return parser.NewDString(string(runes)), nil
	}, types.String, "Reverses the order of the string's characters.")},

	"replace": {stringBuiltin3(
		"input", "find", "replace",
		func(evalCtx *parser.EvalContext, input, from, to string) (parser.Datum, error) {
			result := strings.Replace(input, from, to, -1)
			if err := evalCtx.ActiveMemAcc.Grow(evalCtx.Ctx(), int64(len(result))); err != nil {
				return nil, err
			}
			return parser.NewDString(result), nil
		},
		types.String,
		"Replaces all occurrences of `find` with `replace` in `input`",
	)},

	"translate": {stringBuiltin3(
		"input", "find", "replace",
		func(evalCtx *parser.EvalContext, s, from, to string) (parser.Datum, error) {
			if err := evalCtx.ActiveMemAcc.Grow(evalCtx.Ctx(), int64(len(s))); err != nil {
				return nil, err
			}
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
			return parser.NewDString(string(runes)), nil
		}, types.String, "In `input`, replaces the first character from `find` with the first "+
			"character in `replace`; repeat for each character in `find`. \n\nFor example, "+
			"`translate('doggie', 'dog', '123');` returns `1233ie`.")},

	"regexp_extract": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"input", types.String}, {"regex", types.String}},
			ReturnType: parser.FixedReturnType(types.String),
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				s := string(parser.MustBeDString(args[0]))
				pattern := string(parser.MustBeDString(args[1]))
				return regexpExtract(ctx, s, pattern, `\`)
			},
			Info: "Returns the first match for the Regular Expression `regex` in `input`.",
		},
	},

	"regexp_replace": {
		parser.Builtin{
			Types: parser.ArgTypes{
				{"input", types.String},
				{"regex", types.String},
				{"replace", types.String},
			},
			ReturnType: parser.FixedReturnType(types.String),
			Fn: func(evalCtx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				s := string(parser.MustBeDString(args[0]))
				pattern := string(parser.MustBeDString(args[1]))
				to := string(parser.MustBeDString(args[2]))
				result, err := regexpReplace(evalCtx, s, pattern, to, "")
				if err != nil {
					return nil, err
				}
				if err := evalCtx.ActiveMemAcc.Grow(evalCtx.Ctx(), int64(len(string(parser.MustBeDString(result))))); err != nil {
					return nil, err
				}
				return result, nil
			},
			Info: "Replaces matches for the Regular Expression `regex` in `input` with the " +
				"Regular Expression `replace`.",
		},
		parser.Builtin{
			Types: parser.ArgTypes{
				{"input", types.String},
				{"regex", types.String},
				{"replace", types.String},
				{"flags", types.String},
			},
			ReturnType: parser.FixedReturnType(types.String),
			Fn: func(evalCtx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				s := string(parser.MustBeDString(args[0]))
				pattern := string(parser.MustBeDString(args[1]))
				to := string(parser.MustBeDString(args[2]))
				sqlFlags := string(parser.MustBeDString(args[3]))
				result, err := regexpReplace(evalCtx, s, pattern, to, sqlFlags)
				if err != nil {
					return nil, err
				}
				if err := evalCtx.ActiveMemAcc.Grow(evalCtx.Ctx(), int64(len(string(parser.MustBeDString(result))))); err != nil {
					return nil, err
				}
				return result, nil
			},
			Info: "Replaces matches for the regular expression `regex` in `input` with the regular " +
				"expression `replace` using `flags`." + `

CockroachDB supports the following flags:

| Flag           | Description                                                       |
|----------------|-------------------------------------------------------------------|
| **c**          | Case-sensitive matching                                           |
| **i**          | Global matching (match each substring instead of only the first). |
| **m** or **n** | Newline-sensitive (see below)                                     |
| **p**          | Partial newline-sensitive matching (see below)                    |
| **s**          | Newline-insensitive (default)                                     |
| **w**          | Inverse partial newline-sensitive matching (see below)            |

| Mode | ` + "`.` and `[^...]` match newlines | `^` and `$` match line boundaries" + `|
|------|----------------------------------|--------------------------------------|
| s    | yes                              | no                                   |
| w    | yes                              | yes                                  |
| p    | no                               | no                                   |
| m/n  | no                               | yes                                  |`,
		},
	},

	"initcap": {stringBuiltin1(func(evalCtx *parser.EvalContext, s string) (parser.Datum, error) {
		if err := evalCtx.ActiveMemAcc.Grow(evalCtx.Ctx(), int64(len(s))); err != nil {
			return nil, err
		}
		return parser.NewDString(strings.Title(strings.ToLower(s))), nil
	}, types.String, "Capitalizes the first letter of `val`.")},

	"left": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"input", types.Bytes}, {"return_set", types.Int}},
			ReturnType: parser.FixedReturnType(types.Bytes),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				bytes := []byte(*args[0].(*parser.DBytes))
				n := int(parser.MustBeDInt(args[1]))

				if n < -len(bytes) {
					n = 0
				} else if n < 0 {
					n = len(bytes) + n
				} else if n > len(bytes) {
					n = len(bytes)
				}
				return parser.NewDBytes(parser.DBytes(bytes[:n])), nil
			},
			Info: "Returns the first `return_set` bytes from `input`.",
		},
		parser.Builtin{
			Types:      parser.ArgTypes{{"input", types.String}, {"return_set", types.Int}},
			ReturnType: parser.FixedReturnType(types.String),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				runes := []rune(string(parser.MustBeDString(args[0])))
				n := int(parser.MustBeDInt(args[1]))

				if n < -len(runes) {
					n = 0
				} else if n < 0 {
					n = len(runes) + n
				} else if n > len(runes) {
					n = len(runes)
				}
				return parser.NewDString(string(runes[:n])), nil
			},
			Info: "Returns the first `return_set` characters from `input`.",
		},
	},

	"right": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"input", types.Bytes}, {"return_set", types.Int}},
			ReturnType: parser.FixedReturnType(types.Bytes),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				bytes := []byte(*args[0].(*parser.DBytes))
				n := int(parser.MustBeDInt(args[1]))

				if n < -len(bytes) {
					n = 0
				} else if n < 0 {
					n = len(bytes) + n
				} else if n > len(bytes) {
					n = len(bytes)
				}
				return parser.NewDBytes(parser.DBytes(bytes[len(bytes)-n:])), nil
			},
			Info: "Returns the last `return_set` bytes from `input`.",
		},
		parser.Builtin{
			Types:      parser.ArgTypes{{"input", types.String}, {"return_set", types.Int}},
			ReturnType: parser.FixedReturnType(types.String),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				runes := []rune(string(parser.MustBeDString(args[0])))
				n := int(parser.MustBeDInt(args[1]))

				if n < -len(runes) {
					n = 0
				} else if n < 0 {
					n = len(runes) + n
				} else if n > len(runes) {
					n = len(runes)
				}
				return parser.NewDString(string(runes[len(runes)-n:])), nil
			},
			Info: "Returns the last `return_set` characters from `input`.",
		},
	},

	"random": {
		parser.Builtin{
			Types:                   parser.ArgTypes{},
			ReturnType:              parser.FixedReturnType(types.Float),
			Impure:                  true,
			NeedsRepeatedEvaluation: true,
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				return parser.NewDFloat(parser.DFloat(rand.Float64())), nil
			},
			Info: "Returns a random float between 0 and 1.",
		},
	},

	"unique_rowid": {
		parser.Builtin{
			Types:      parser.ArgTypes{},
			ReturnType: parser.FixedReturnType(types.Int),
			Category:   categoryIDGeneration,
			Impure:     true,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				return parser.NewDInt(GenerateUniqueInt(ctx.NodeID)), nil
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
		parser.Builtin{
			Types:        parser.HomogeneousType{},
			ReturnType:   parser.IdentityReturnType(0),
			NullableArgs: true,
			Category:     categoryComparison,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				return parser.PickFromTuple(ctx, true /* greatest */, args)
			},
			Info: "Returns the element with the greatest value.",
		},
	},
	"least": {
		parser.Builtin{
			Types:        parser.HomogeneousType{},
			ReturnType:   parser.IdentityReturnType(0),
			NullableArgs: true,
			Category:     categoryComparison,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				return parser.PickFromTuple(ctx, false /* greatest */, args)
			},
			Info: "Returns the element with the lowest value.",
		},
	},

	// Timestamp/Date functions.

	"experimental_strftime": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"input", types.Timestamp}, {"extract_format", types.String}},
			ReturnType: parser.FixedReturnType(types.String),
			Category:   categoryDateAndTime,
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				fromTime := args[0].(*parser.DTimestamp).Time
				format := string(parser.MustBeDString(args[1]))
				t, err := strtime.Strftime(fromTime, format)
				if err != nil {
					return nil, err
				}
				return parser.NewDString(t), nil
			},
			Info: "From `input`, extracts and formats the time as identified in `extract_format` " +
				"using standard `strftime` notation (though not all formatting is supported).",
		},
		parser.Builtin{
			Types:      parser.ArgTypes{{"input", types.Date}, {"extract_format", types.String}},
			ReturnType: parser.FixedReturnType(types.String),
			Category:   categoryDateAndTime,
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				fromTime := timeutil.Unix(int64(*args[0].(*parser.DDate))*parser.SecondsInDay, 0)
				format := string(parser.MustBeDString(args[1]))
				t, err := strtime.Strftime(fromTime, format)
				if err != nil {
					return nil, err
				}
				return parser.NewDString(t), nil
			},
			Info: "From `input`, extracts and formats the time as identified in `extract_format` " +
				"using standard `strftime` notation (though not all formatting is supported).",
		},
		parser.Builtin{
			Types:      parser.ArgTypes{{"input", types.TimestampTZ}, {"extract_format", types.String}},
			ReturnType: parser.FixedReturnType(types.String),
			Category:   categoryDateAndTime,
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				fromTime := args[0].(*parser.DTimestampTZ).Time
				format := string(parser.MustBeDString(args[1]))
				t, err := strtime.Strftime(fromTime, format)
				if err != nil {
					return nil, err
				}
				return parser.NewDString(t), nil
			},
			Info: "From `input`, extracts and formats the time as identified in `extract_format` " +
				"using standard `strftime` notation (though not all formatting is supported).",
		},
	},

	"experimental_strptime": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"input", types.String}, {"format", types.String}},
			ReturnType: parser.FixedReturnType(types.TimestampTZ),
			Category:   categoryDateAndTime,
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				toParse := string(parser.MustBeDString(args[0]))
				format := string(parser.MustBeDString(args[1]))
				t, err := strtime.Strptime(toParse, format)
				if err != nil {
					return nil, err
				}
				return parser.MakeDTimestampTZ(t.UTC(), time.Microsecond), nil
			},
			Info: "Returns `input` as a timestamptz using `format` (which uses standard " +
				"`strptime` formatting).",
		},
	},

	"age": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"val", types.TimestampTZ}},
			ReturnType: parser.FixedReturnType(types.Interval),
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				return parser.TimestampDifference(ctx, ctx.GetTxnTimestamp(time.Microsecond), args[0])
			},
			Info: "Calculates the interval between `val` and the current time.",
		},
		parser.Builtin{
			Types:      parser.ArgTypes{{"begin", types.TimestampTZ}, {"end", types.TimestampTZ}},
			ReturnType: parser.FixedReturnType(types.Interval),
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				return parser.TimestampDifference(ctx, args[0], args[1])
			},
			Info: "Calculates the interval between `begin` and `end`.",
		},
	},

	"current_date": {
		parser.Builtin{
			Types:      parser.ArgTypes{},
			ReturnType: parser.FixedReturnType(types.Date),
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				t := ctx.GetTxnTimestamp(time.Microsecond).Time
				return parser.NewDDateFromTime(t, ctx.GetLocation()), nil
			},
			Info: "Returns the current date.",
		},
	},

	"now":                   txnTSImpl,
	"current_timestamp":     txnTSImpl,
	"transaction_timestamp": txnTSImpl,

	"statement_timestamp": {
		parser.Builtin{
			Types:             parser.ArgTypes{},
			ReturnType:        parser.FixedReturnType(types.TimestampTZ),
			PreferredOverload: true,
			Impure:            true,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				return parser.MakeDTimestampTZ(ctx.GetStmtTimestamp(), time.Microsecond), nil
			},
			Info: "Returns the current statement's timestamp.",
		},
		parser.Builtin{
			Types:      parser.ArgTypes{},
			ReturnType: parser.FixedReturnType(types.Timestamp),
			Impure:     true,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				return parser.MakeDTimestamp(ctx.GetStmtTimestamp(), time.Microsecond), nil
			},
			Info: "Returns the current statement's timestamp.",
		},
	},

	"cluster_logical_timestamp": {
		parser.Builtin{
			Types:      parser.ArgTypes{},
			ReturnType: parser.FixedReturnType(types.Decimal),
			Category:   categorySystemInfo,
			Impure:     true,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				return ctx.GetClusterTimestamp(), nil
			},
			Info: "This function is used only by CockroachDB's developers for testing purposes.",
		},
	},

	"clock_timestamp": {
		parser.Builtin{
			Types:             parser.ArgTypes{},
			ReturnType:        parser.FixedReturnType(types.TimestampTZ),
			PreferredOverload: true,
			Impure:            true,
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				return parser.MakeDTimestampTZ(timeutil.Now(), time.Microsecond), nil
			},
			Info: "Returns the current wallclock time.",
		},
		parser.Builtin{
			Types:      parser.ArgTypes{},
			ReturnType: parser.FixedReturnType(types.Timestamp),
			Impure:     true,
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				return parser.MakeDTimestamp(timeutil.Now(), time.Microsecond), nil
			},
			Info: "Returns the current wallclock time.",
		},
	},

	"extract": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"element", types.String}, {"input", types.Timestamp}},
			ReturnType: parser.FixedReturnType(types.Int),
			Category:   categoryDateAndTime,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				// extract timeSpan fromTime.
				fromTS := args[1].(*parser.DTimestamp)
				timeSpan := strings.ToLower(string(parser.MustBeDString(args[0])))
				return extractStringFromTimestamp(ctx, fromTS.Time, timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: year, quarter, month, week, dayofweek, dayofyear,\n" +
				"hour, minute, second, millisecond, microsecond, epoch",
		},
		parser.Builtin{
			Types:      parser.ArgTypes{{"element", types.String}, {"input", types.Date}},
			ReturnType: parser.FixedReturnType(types.Int),
			Category:   categoryDateAndTime,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				timeSpan := strings.ToLower(string(parser.MustBeDString(args[0])))
				date := args[1].(*parser.DDate)
				fromTSTZ := parser.MakeDTimestampTZFromDate(ctx.GetLocation(), date)
				return extractStringFromTimestamp(ctx, fromTSTZ.Time, timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: year, quarter, month, week, dayofweek, dayofyear,\n" +
				"hour, minute, second, millisecond, microsecond, epoch",
		},
		parser.Builtin{
			Types:      parser.ArgTypes{{"element", types.String}, {"input", types.TimestampTZ}},
			ReturnType: parser.FixedReturnType(types.Int),
			Category:   categoryDateAndTime,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				fromTSTZ := args[1].(*parser.DTimestampTZ)
				timeSpan := strings.ToLower(string(parser.MustBeDString(args[0])))
				return extractStringFromTimestamp(ctx, fromTSTZ.Time, timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: year, quarter, month, week, dayofweek, dayofyear,\n" +
				"hour, minute, second, millisecond, microsecond, epoch",
		},
	},

	"extract_duration": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"element", types.String}, {"input", types.Interval}},
			ReturnType: parser.FixedReturnType(types.Int),
			Category:   categoryDateAndTime,
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				// extract timeSpan fromTime.
				fromInterval := *args[1].(*parser.DInterval)
				timeSpan := strings.ToLower(string(parser.MustBeDString(args[0])))
				switch timeSpan {
				case "hour", "hours":
					return parser.NewDInt(parser.DInt(fromInterval.Nanos / int64(time.Hour))), nil

				case "minute", "minutes":
					return parser.NewDInt(parser.DInt(fromInterval.Nanos / int64(time.Minute))), nil

				case "second", "seconds":
					return parser.NewDInt(parser.DInt(fromInterval.Nanos / int64(time.Second))), nil

				case "millisecond", "milliseconds":
					// This a PG extension not supported in MySQL.
					return parser.NewDInt(parser.DInt(fromInterval.Nanos / int64(time.Millisecond))), nil

				case "microsecond", "microseconds":
					return parser.NewDInt(parser.DInt(fromInterval.Nanos / int64(time.Microsecond))), nil

				default:
					return nil, pgerror.NewErrorf(
						pgerror.CodeInvalidParameterValueError, "unsupported timespan: %s", timeSpan)
				}
			},
			Info: "Extracts `element` from `input`.\n" +
				"Compatible elements: hour, minute, second, millisecond, microsecond.",
		},
	},

	// https://www.postgresql.org/docs/10/static/functions-datetime.html#functions-datetime-trunc
	"date_trunc": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"element", types.String}, {"input", types.Timestamp}},
			ReturnType: parser.FixedReturnType(types.Timestamp),
			Category:   categoryDateAndTime,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				// extract timeSpan fromTime.
				fromTS := args[1].(*parser.DTimestamp)
				timeSpan := strings.ToLower(string(parser.MustBeDString(args[0])))
				return truncateTimestamp(ctx, fromTS.Time, timeSpan)
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero (or one, for day and month)\n\n" +
				"Compatible elements: year, quarter, month, week, hour, minute, second,\n" +
				"millisecond, microsecond.",
		},
		parser.Builtin{
			Types:      parser.ArgTypes{{"element", types.String}, {"input", types.Date}},
			ReturnType: parser.FixedReturnType(types.Date),
			Category:   categoryDateAndTime,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				timeSpan := strings.ToLower(string(parser.MustBeDString(args[0])))
				date := args[1].(*parser.DDate)
				fromTSTZ := parser.MakeDTimestampTZFromDate(ctx.GetLocation(), date)
				return truncateTimestamp(ctx, fromTSTZ.Time, timeSpan)
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero (or one, for day and month)\n\n" +
				"Compatible elements: year, quarter, month, week, hour, minute, second,\n" +
				"millisecond, microsecond.",
		},
		parser.Builtin{
			Types:      parser.ArgTypes{{"element", types.String}, {"input", types.TimestampTZ}},
			ReturnType: parser.FixedReturnType(types.TimestampTZ),
			Category:   categoryDateAndTime,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				fromTSTZ := args[1].(*parser.DTimestampTZ)
				timeSpan := strings.ToLower(string(parser.MustBeDString(args[0])))
				return truncateTimestamp(ctx, fromTSTZ.Time, timeSpan)
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero (or one, for day and month)\n\n" +
				"Compatible elements: year, quarter, month, week, hour, minute, second,\n" +
				"millisecond, microsecond.",
		},
	},

	// Math functions
	"abs": {
		floatBuiltin1(func(x float64) (parser.Datum, error) {
			return parser.NewDFloat(parser.DFloat(math.Abs(x))), nil
		}, "Calculates the absolute value of `val`."),
		decimalBuiltin1(func(x *apd.Decimal) (parser.Datum, error) {
			dd := &parser.DDecimal{}
			dd.Abs(x)
			return dd, nil
		}, "Calculates the absolute value of `val`."),
		parser.Builtin{
			Types:      parser.ArgTypes{{"val", types.Int}},
			ReturnType: parser.FixedReturnType(types.Int),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				x := parser.MustBeDInt(args[0])
				switch {
				case x == math.MinInt64:
					return nil, errAbsOfMinInt64
				case x < 0:
					return parser.NewDInt(-x), nil
				}
				return args[0], nil
			},
			Info: "Calculates the absolute value of `val`.",
		},
	},

	"acos": {
		floatBuiltin1(func(x float64) (parser.Datum, error) {
			return parser.NewDFloat(parser.DFloat(math.Acos(x))), nil
		}, "Calculates the inverse cosine of `val`."),
	},

	"asin": {
		floatBuiltin1(func(x float64) (parser.Datum, error) {
			return parser.NewDFloat(parser.DFloat(math.Asin(x))), nil
		}, "Calculates the inverse sine of `val`."),
	},

	"atan": {
		floatBuiltin1(func(x float64) (parser.Datum, error) {
			return parser.NewDFloat(parser.DFloat(math.Atan(x))), nil
		}, "Calculates the inverse tangent of `val`."),
	},

	"atan2": {
		floatBuiltin2("x", "y", func(x, y float64) (parser.Datum, error) {
			return parser.NewDFloat(parser.DFloat(math.Atan2(x, y))), nil
		}, "Calculates the inverse tangent of `x`/`y`."),
	},

	"cbrt": {
		floatBuiltin1(func(x float64) (parser.Datum, error) {
			return parser.NewDFloat(parser.DFloat(math.Cbrt(x))), nil
		}, "Calculates the cube root (∛) of `val`."),
		decimalBuiltin1(func(x *apd.Decimal) (parser.Datum, error) {
			dd := &parser.DDecimal{}
			_, err := parser.DecimalCtx.Cbrt(&dd.Decimal, x)
			return dd, err
		}, "Calculates the cube root (∛) of `val`."),
	},

	"ceil":    ceilImpl,
	"ceiling": ceilImpl,

	"cos": {
		floatBuiltin1(func(x float64) (parser.Datum, error) {
			return parser.NewDFloat(parser.DFloat(math.Cos(x))), nil
		}, "Calculates the cosine of `val`."),
	},

	"cot": {
		floatBuiltin1(func(x float64) (parser.Datum, error) {
			return parser.NewDFloat(parser.DFloat(1 / math.Tan(x))), nil
		}, "Calculates the cotangent of `val`."),
	},

	"degrees": {
		floatBuiltin1(func(x float64) (parser.Datum, error) {
			return parser.NewDFloat(parser.DFloat(180.0 * x / math.Pi)), nil
		}, "Converts `val` as a radian value to a degree value."),
	},

	"div": {
		floatBuiltin2("x", "y", func(x, y float64) (parser.Datum, error) {
			return parser.NewDFloat(parser.DFloat(math.Trunc(x / y))), nil
		}, "Calculates the integer quotient of `x`/`y`."),
		decimalBuiltin2("x", "y", func(x, y *apd.Decimal) (parser.Datum, error) {
			if y.Sign() == 0 {
				return nil, parser.ErrDivByZero
			}
			dd := &parser.DDecimal{}
			_, err := parser.HighPrecisionCtx.QuoInteger(&dd.Decimal, x, y)
			return dd, err
		}, "Calculates the integer quotient of `x`/`y`."),
		{
			Types:      parser.ArgTypes{{"x", types.Int}, {"y", types.Int}},
			ReturnType: parser.FixedReturnType(types.Int),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				y := parser.MustBeDInt(args[1])
				if y == 0 {
					return nil, parser.ErrDivByZero
				}
				x := parser.MustBeDInt(args[0])
				return parser.NewDInt(x / y), nil
			},
			Info: "Calculates the integer quotient of `x`/`y`.",
		},
	},

	"exp": {
		floatBuiltin1(func(x float64) (parser.Datum, error) {
			return parser.NewDFloat(parser.DFloat(math.Exp(x))), nil
		}, "Calculates *e* ^ `val`."),
		decimalBuiltin1(func(x *apd.Decimal) (parser.Datum, error) {
			dd := &parser.DDecimal{}
			_, err := parser.DecimalCtx.Exp(&dd.Decimal, x)
			return dd, err
		}, "Calculates *e* ^ `val`."),
	},

	"floor": {
		floatBuiltin1(func(x float64) (parser.Datum, error) {
			return parser.NewDFloat(parser.DFloat(math.Floor(x))), nil
		}, "Calculates the largest integer not greater than `val`."),
		decimalBuiltin1(func(x *apd.Decimal) (parser.Datum, error) {
			dd := &parser.DDecimal{}
			_, err := parser.ExactCtx.Floor(&dd.Decimal, x)
			return dd, err
		}, "Calculates the largest integer not greater than `val`."),
	},

	"isnan": {
		parser.Builtin{
			// Can't use floatBuiltin1 here because this one returns
			// a boolean.
			Types:      parser.ArgTypes{{"val", types.Float}},
			ReturnType: parser.FixedReturnType(types.Bool),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				return parser.MakeDBool(parser.DBool(math.IsNaN(float64(*args[0].(*parser.DFloat))))), nil
			},
			Info: "Returns true if `val` is NaN, false otherwise.",
		},
		parser.Builtin{
			Types:      parser.ArgTypes{{"val", types.Decimal}},
			ReturnType: parser.FixedReturnType(types.Bool),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				isNaN := args[0].(*parser.DDecimal).Decimal.Form == apd.NaN
				return parser.MakeDBool(parser.DBool(isNaN)), nil
			},
			Info: "Returns true if `val` is NaN, false otherwise.",
		},
	},

	"json_remove_path": {
	// TODO(justin): added here so #- can be desugared into it, still needs to be
	// implemented.
	},

	"ln": {
		floatBuiltin1(func(x float64) (parser.Datum, error) {
			return parser.NewDFloat(parser.DFloat(math.Log(x))), nil
		}, "Calculates the natural log of `val`."),
		decimalLogFn(parser.DecimalCtx.Ln, "Calculates the natural log of `val`."),
	},

	"log": {
		floatBuiltin1(func(x float64) (parser.Datum, error) {
			return parser.NewDFloat(parser.DFloat(math.Log10(x))), nil
		}, "Calculates the base 10 log of `val`."),
		decimalLogFn(parser.DecimalCtx.Log10, "Calculates the base 10 log of `val`."),
	},

	"mod": {
		floatBuiltin2("x", "y", func(x, y float64) (parser.Datum, error) {
			return parser.NewDFloat(parser.DFloat(math.Mod(x, y))), nil
		}, "Calculates `x`%`y`."),
		decimalBuiltin2("x", "y", func(x, y *apd.Decimal) (parser.Datum, error) {
			if y.Sign() == 0 {
				return nil, parser.ErrZeroModulus
			}
			dd := &parser.DDecimal{}
			_, err := parser.HighPrecisionCtx.Rem(&dd.Decimal, x, y)
			return dd, err
		}, "Calculates `x`%`y`."),
		parser.Builtin{
			Types:      parser.ArgTypes{{"x", types.Int}, {"y", types.Int}},
			ReturnType: parser.FixedReturnType(types.Int),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				y := parser.MustBeDInt(args[1])
				if y == 0 {
					return nil, parser.ErrZeroModulus
				}
				x := parser.MustBeDInt(args[0])
				return parser.NewDInt(x % y), nil
			},
			Info: "Calculates `x`%`y`.",
		},
	},

	"pi": {
		parser.Builtin{
			Types:      parser.ArgTypes{},
			ReturnType: parser.FixedReturnType(types.Float),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				return parser.NewDFloat(math.Pi), nil
			},
			Info: "Returns the value for pi (3.141592653589793).",
		},
	},

	"pow":   powImpls,
	"power": powImpls,

	"radians": {
		floatBuiltin1(func(x float64) (parser.Datum, error) {
			return parser.NewDFloat(parser.DFloat(x * math.Pi / 180.0)), nil
		}, "Converts `val` as a degree value to a radians value."),
	},

	"round": {
		floatBuiltin1(func(x float64) (parser.Datum, error) {
			return parser.NewDFloat(parser.DFloat(round(x))), nil
		}, "Rounds `val` to the nearest integer using half to even (banker's) rounding."),
		decimalBuiltin1(func(x *apd.Decimal) (parser.Datum, error) {
			return roundDecimal(x, 0)
		}, "Rounds `val` to the nearest integer, half away from zero: "+
			"ROUND(+/-2.4) = +/-2, ROUND(+/-2.5) = +/-3."),
		parser.Builtin{
			Types:      parser.ArgTypes{{"input", types.Float}, {"decimal_accuracy", types.Int}},
			ReturnType: parser.FixedReturnType(types.Float),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				f := float64(*args[0].(*parser.DFloat))
				if math.IsInf(f, 0) || math.IsNaN(f) {
					return args[0], nil
				}
				var x apd.Decimal
				if _, err := x.SetFloat64(f); err != nil {
					return nil, err
				}

				// TODO(mjibson): make sure this fits in an int32.
				scale := int32(parser.MustBeDInt(args[1]))

				var d apd.Decimal
				if _, err := parser.RoundCtx.Quantize(&d, &x, -scale); err != nil {
					return nil, err
				}

				f, err := d.Float64()
				if err != nil {
					return nil, err
				}

				return parser.NewDFloat(parser.DFloat(f)), nil
			},
			Info: "Keeps `decimal_accuracy` number of figures to the right of the zero position " +
				" in `input` using half to even (banker's) rounding.",
		},
		parser.Builtin{
			Types:      parser.ArgTypes{{"input", types.Decimal}, {"decimal_accuracy", types.Int}},
			ReturnType: parser.FixedReturnType(types.Decimal),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				// TODO(mjibson): make sure this fits in an int32.
				scale := int32(parser.MustBeDInt(args[1]))
				return roundDecimal(&args[0].(*parser.DDecimal).Decimal, scale)
			},
			Info: "Keeps `decimal_accuracy` number of figures to the right of the zero position " +
				" in `input using half away from zero rounding. If `decimal_accuracy` " +
				"is not in the range -2^31...(2^31-1), the results are undefined.",
		},
	},

	"sin": {
		floatBuiltin1(func(x float64) (parser.Datum, error) {
			return parser.NewDFloat(parser.DFloat(math.Sin(x))), nil
		}, "Calculates the sine of `val`."),
	},

	"sign": {
		floatBuiltin1(func(x float64) (parser.Datum, error) {
			switch {
			case x < 0:
				return parser.NewDFloat(-1), nil
			case x == 0:
				return parser.NewDFloat(0), nil
			}
			return parser.NewDFloat(1), nil
		}, "Determines the sign of `val`: **1** for positive; **0** for 0 values; **-1** for "+
			"negative."),
		decimalBuiltin1(func(x *apd.Decimal) (parser.Datum, error) {
			d := &parser.DDecimal{}
			d.Decimal.SetCoefficient(int64(x.Sign()))
			return d, nil
		}, "Determines the sign of `val`: **1** for positive; **0** for 0 values; **-1** for "+
			"negative."),
		parser.Builtin{
			Types:      parser.ArgTypes{{"val", types.Int}},
			ReturnType: parser.FixedReturnType(types.Int),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				x := parser.MustBeDInt(args[0])
				switch {
				case x < 0:
					return parser.NewDInt(-1), nil
				case x == 0:
					return parser.DZero, nil
				}
				return parser.NewDInt(1), nil
			},
			Info: "Determines the sign of `val`: **1** for positive; **0** for 0 values; **-1** " +
				"for negative.",
		},
	},

	"sqrt": {
		floatBuiltin1(func(x float64) (parser.Datum, error) {
			// TODO(mjibson): see #13642
			if x < 0 {
				return nil, errSqrtOfNegNumber
			}
			return parser.NewDFloat(parser.DFloat(math.Sqrt(x))), nil
		}, "Calculates the square root of `val`."),
		decimalBuiltin1(func(x *apd.Decimal) (parser.Datum, error) {
			if x.Sign() < 0 {
				return nil, errSqrtOfNegNumber
			}
			dd := &parser.DDecimal{}
			_, err := parser.DecimalCtx.Sqrt(&dd.Decimal, x)
			return dd, err
		}, "Calculates the square root of `val`."),
	},

	"tan": {
		floatBuiltin1(func(x float64) (parser.Datum, error) {
			return parser.NewDFloat(parser.DFloat(math.Tan(x))), nil
		}, "Calculates the tangent of `val`."),
	},

	"trunc": {
		floatBuiltin1(func(x float64) (parser.Datum, error) {
			return parser.NewDFloat(parser.DFloat(math.Trunc(x))), nil
		}, "Truncates the decimal values of `val`."),
		decimalBuiltin1(func(x *apd.Decimal) (parser.Datum, error) {
			dd := &parser.DDecimal{}
			x.Modf(&dd.Decimal, nil)
			return dd, nil
		}, "Truncates the decimal values of `val`."),
	},

	"to_english": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"val", types.Int}},
			ReturnType: parser.FixedReturnType(types.String),
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				val := int(*args[0].(*parser.DInt))
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
				return parser.NewDString(buf.String()), nil
			},
			Category: categoryString,
			Info:     "This function enunciates the value of its argument using English cardinals.",
		},
	},

	// Array functions.

	"array_length": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"input", types.AnyArray}, {"array_dimension", types.Int}},
			ReturnType: parser.FixedReturnType(types.Int),
			Category:   categoryArray,
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				arr := parser.MustBeDArray(args[0])
				dimen := int64(parser.MustBeDInt(args[1]))
				return arrayLength(arr, dimen), nil
			},
			Info: "Calculates the length of `input` on the provided `array_dimension`. However, " +
				"because CockroachDB doesn't yet support multi-dimensional arrays, the only supported" +
				" `array_dimension` is **1**.",
		},
	},

	"array_lower": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"input", types.AnyArray}, {"array_dimension", types.Int}},
			ReturnType: parser.FixedReturnType(types.Int),
			Category:   categoryArray,
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				arr := parser.MustBeDArray(args[0])
				dimen := int64(parser.MustBeDInt(args[1]))
				return arrayLower(arr, dimen), nil
			},
			Info: "Calculates the minimum value of `input` on the provided `array_dimension`. " +
				"However, because CockroachDB doesn't yet support multi-dimensional arrays, the only " +
				"supported `array_dimension` is **1**.",
		},
	},

	"array_upper": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"input", types.AnyArray}, {"array_dimension", types.Int}},
			ReturnType: parser.FixedReturnType(types.Int),
			Category:   categoryArray,
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				arr := parser.MustBeDArray(args[0])
				dimen := int64(parser.MustBeDInt(args[1]))
				return arrayLength(arr, dimen), nil
			},
			Info: "Calculates the maximum value of `input` on the provided `array_dimension`. " +
				"However, because CockroachDB doesn't yet support multi-dimensional arrays, the only " +
				"supported `array_dimension` is **1**.",
		},
	},

	"array_append": arrayBuiltin(func(typ types.T) parser.Builtin {
		return parser.Builtin{
			Types:        parser.ArgTypes{{"array", types.TArray{Typ: typ}}, {"elem", typ}},
			ReturnType:   parser.FixedReturnType(types.TArray{Typ: typ}),
			Category:     categoryArray,
			NullableArgs: true,
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				return parser.AppendToMaybeNullArray(typ, args[0], args[1])
			},
			Info: "Appends `elem` to `array`, returning the result.",
		}
	}),

	"array_prepend": arrayBuiltin(func(typ types.T) parser.Builtin {
		return parser.Builtin{
			Types:        parser.ArgTypes{{"elem", typ}, {"array", types.TArray{Typ: typ}}},
			ReturnType:   parser.FixedReturnType(types.TArray{Typ: typ}),
			Category:     categoryArray,
			NullableArgs: true,
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				return parser.PrependToMaybeNullArray(typ, args[0], args[1])
			},
			Info: "Prepends `elem` to `array`, returning the result.",
		}
	}),

	"array_cat": arrayBuiltin(func(typ types.T) parser.Builtin {
		return parser.Builtin{
			Types:        parser.ArgTypes{{"left", types.TArray{Typ: typ}}, {"right", types.TArray{Typ: typ}}},
			ReturnType:   parser.FixedReturnType(types.TArray{Typ: typ}),
			Category:     categoryArray,
			NullableArgs: true,
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				return parser.ConcatArrays(typ, args[0], args[1])
			},
			Info: "Appends two arrays.",
		}
	}),

	"array_remove": arrayBuiltin(func(typ types.T) parser.Builtin {
		return parser.Builtin{
			Types:        parser.ArgTypes{{"array", types.TArray{Typ: typ}}, {"elem", typ}},
			ReturnType:   parser.FixedReturnType(types.TArray{Typ: typ}),
			Category:     categoryArray,
			NullableArgs: true,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				if args[0] == parser.DNull {
					return parser.DNull, nil
				}
				result := parser.NewDArray(typ)
				for _, e := range parser.MustBeDArray(args[0]).Array {
					if e.Compare(ctx, args[1]) != 0 {
						if err := result.Append(e); err != nil {
							return nil, err
						}
					}
				}
				return result, nil
			},
			Info: "Remove from `array` all elements equal to `elem`.",
		}
	}),

	"array_replace": arrayBuiltin(func(typ types.T) parser.Builtin {
		return parser.Builtin{
			Types:        parser.ArgTypes{{"array", types.TArray{Typ: typ}}, {"toreplace", typ}, {"replacewith", typ}},
			ReturnType:   parser.FixedReturnType(types.TArray{Typ: typ}),
			Category:     categoryArray,
			NullableArgs: true,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				if args[0] == parser.DNull {
					return parser.DNull, nil
				}
				result := parser.NewDArray(typ)
				for _, e := range parser.MustBeDArray(args[0]).Array {
					if e.Compare(ctx, args[1]) == 0 {
						if err := result.Append(args[2]); err != nil {
							return nil, err
						}
					} else {
						if err := result.Append(e); err != nil {
							return nil, err
						}
					}
				}
				return result, nil
			},
			Info: "Replace all occurrences of `toreplace` in `array` with `replacewith`.",
		}
	}),

	"array_position": arrayBuiltin(func(typ types.T) parser.Builtin {
		return parser.Builtin{
			Types:        parser.ArgTypes{{"array", types.TArray{Typ: typ}}, {"elem", typ}},
			ReturnType:   parser.FixedReturnType(types.Int),
			Category:     categoryArray,
			NullableArgs: true,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				if args[0] == parser.DNull {
					return parser.DNull, nil
				}
				for i, e := range parser.MustBeDArray(args[0]).Array {
					if e.Compare(ctx, args[1]) == 0 {
						return parser.NewDInt(parser.DInt(i + 1)), nil
					}
				}
				return parser.DNull, nil
			},
			Info: "Return the index of the first occurrence of `elem` in `array`.",
		}
	}),

	"array_positions": arrayBuiltin(func(typ types.T) parser.Builtin {
		return parser.Builtin{
			Types:        parser.ArgTypes{{"array", types.TArray{Typ: typ}}, {"elem", typ}},
			ReturnType:   parser.FixedReturnType(types.TArray{Typ: typ}),
			Category:     categoryArray,
			NullableArgs: true,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				if args[0] == parser.DNull {
					return parser.DNull, nil
				}
				result := parser.NewDArray(types.Int)
				for i, e := range parser.MustBeDArray(args[0]).Array {
					if e.Compare(ctx, args[1]) == 0 {
						if err := result.Append(parser.NewDInt(parser.DInt(i + 1))); err != nil {
							return nil, err
						}
					}
				}
				return result, nil
			},
			Info: "Returns and array of indexes of all occurrences of `elem` in `array`.",
		}
	}),

	// Metadata functions.

	"version": {
		parser.Builtin{
			Types:      parser.ArgTypes{},
			ReturnType: parser.FixedReturnType(types.String),
			Category:   categorySystemInfo,
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				return parser.NewDString(build.GetInfo().Short()), nil
			},
			Info: "Returns the node's version of CockroachDB.",
		},
	},

	"current_database": {
		parser.Builtin{
			Types:      parser.ArgTypes{},
			ReturnType: parser.FixedReturnType(types.String),
			Category:   categorySystemInfo,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				if len(ctx.Database) == 0 {
					return parser.DNull, nil
				}
				return parser.NewDString(ctx.Database), nil
			},
			Info: "Returns the current database.",
		},
	},

	"current_schema": {
		parser.Builtin{
			Types:      parser.ArgTypes{},
			ReturnType: parser.FixedReturnType(types.String),
			Category:   categorySystemInfo,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				if len(ctx.Database) == 0 {
					return parser.DNull, nil
				}
				return parser.NewDString(ctx.Database), nil
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
		parser.Builtin{
			Types:      parser.ArgTypes{{"include_pg_catalog", types.Bool}},
			ReturnType: parser.FixedReturnType(types.TArray{Typ: types.String}),
			Category:   categorySystemInfo,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				includePgCatalog := *(args[0].(*parser.DBool))
				schemas := parser.NewDArray(types.String)
				if len(ctx.Database) != 0 {
					if err := schemas.Append(parser.NewDString(ctx.Database)); err != nil {
						return nil, err
					}
				}
				var iter func() (string, bool)
				if includePgCatalog {
					iter = ctx.SearchPath.Iter()
				} else {
					iter = ctx.SearchPath.IterWithoutImplicitPGCatalog()
				}
				for p, ok := iter(); ok; p, ok = iter() {
					if p == ctx.Database {
						continue
					}
					if err := schemas.Append(parser.NewDString(p)); err != nil {
						return nil, err
					}
				}
				return schemas, nil
			},
			Info: "Returns the current search path for unqualified names.",
		},
	},

	"current_user": {
		parser.Builtin{
			Types:      parser.ArgTypes{},
			ReturnType: parser.FixedReturnType(types.String),
			Category:   categorySystemInfo,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				if len(ctx.User) == 0 {
					return parser.DNull, nil
				}
				return parser.NewDString(ctx.User), nil
			},
			Info: "Returns the current user. This function is provided for " +
				"compatibility with PostgreSQL.",
		},
	},

	"crdb_internal.cluster_id": {
		parser.Builtin{
			Types:      parser.ArgTypes{},
			ReturnType: parser.FixedReturnType(types.UUID),
			Category:   categorySystemInfo,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				return parser.NewDUuid(parser.DUuid{UUID: ctx.ClusterID}), nil
			},
			Info: "Returns the cluster ID.",
		},
	},

	"crdb_internal.force_error": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"errorCode", types.String}, {"msg", types.String}},
			ReturnType: parser.FixedReturnType(types.Int),
			Impure:     true,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				errCode := string(*args[0].(*parser.DString))
				msg := string(*args[1].(*parser.DString))
				return nil, pgerror.NewError(errCode, msg)
			},
			Category: categorySystemInfo,
			Info:     "This function is used only by CockroachDB's developers for testing purposes.",
		},
	},

	"crdb_internal.force_panic": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"msg", types.String}},
			ReturnType: parser.FixedReturnType(types.Int),
			Impure:     true,
			Privileged: true,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				msg := string(*args[0].(*parser.DString))
				panic(msg)
			},
			Category: categorySystemInfo,
			Info:     "This function is used only by CockroachDB's developers for testing purposes.",
		},
	},

	"crdb_internal.force_log_fatal": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"msg", types.String}},
			ReturnType: parser.FixedReturnType(types.Int),
			Impure:     true,
			Privileged: true,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				msg := string(*args[0].(*parser.DString))
				log.Fatal(ctx.Ctx(), msg)
				return nil, nil
			},
			Category: categorySystemInfo,
			Info:     "This function is used only by CockroachDB's developers for testing purposes.",
		},
	},

	// If force_retry is called during the specified interval from the beginning
	// of the transaction it returns a retryable error. If not, 0 is returned
	// instead of an error.
	// The second version allows one to create an error intended for a transaction
	// different than the current statement's transaction.
	"crdb_internal.force_retry": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"val", types.Interval}},
			ReturnType: parser.FixedReturnType(types.Int),
			Impure:     true,
			Privileged: true,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				minDuration := args[0].(*parser.DInterval).Duration
				elapsed := duration.Duration{
					Nanos: int64(ctx.StmtTimestamp.Sub(ctx.TxnTimestamp)),
				}
				if elapsed.Compare(minDuration) < 0 {
					return nil, ctx.Txn.GenerateForcedRetryableError("forced by crdb_internal.force_retry()")
				}
				return parser.DZero, nil
			},
			Category: categorySystemInfo,
			Info:     "This function is used only by CockroachDB's developers for testing purposes.",
		},
		parser.Builtin{
			Types: parser.ArgTypes{
				{"val", types.Interval},
				{"txnID", types.String}},
			ReturnType: parser.FixedReturnType(types.Int),
			Impure:     true,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				minDuration := args[0].(*parser.DInterval).Duration
				txnID := args[1].(*parser.DString)
				elapsed := duration.Duration{
					Nanos: int64(ctx.StmtTimestamp.Sub(ctx.TxnTimestamp)),
				}
				if elapsed.Compare(minDuration) < 0 {
					uuid, err := uuid.FromString(string(*txnID))
					if err != nil {
						return nil, err
					}
					err = ctx.Txn.GenerateForcedRetryableError("forced by crdb_internal.force_retry()")
					err.(*roachpb.HandledRetryableTxnError).TxnID = uuid
					return nil, err
				}
				return parser.DZero, nil
			},
			Category: categorySystemInfo,
			Info:     "This function is used only by CockroachDB's developers for testing purposes.",
		},
	},

	// Identity function which is marked as impure to avoid constant folding.
	"crdb_internal.no_constant_folding": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"input", types.Any}},
			ReturnType: parser.IdentityReturnType(0),
			Impure:     true,
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				return args[0], nil
			},
			Category: categorySystemInfo,
			Info:     "This function is used only by CockroachDB's developers for testing purposes.",
		},
	},

	"crdb_internal.set_vmodule": {
		parser.Builtin{
			Types:      parser.ArgTypes{{"vmodule_string", types.String}},
			ReturnType: parser.FixedReturnType(types.Int),
			Impure:     true,
			Privileged: true,
			Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				return parser.DZero, log.SetVModule(string(*args[0].(*parser.DString)))
			},
			Category: categorySystemInfo,
			Info: "This function is used for internal debugging purposes. " +
				"Incorrect use can severely impact performance.",
		},
	},
}

var substringImpls = []parser.Builtin{
	{
		Types: parser.ArgTypes{
			{"input", types.String},
			{"substr_pos", types.Int},
		},
		ReturnType: parser.FixedReturnType(types.String),
		Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
			runes := []rune(string(parser.MustBeDString(args[0])))
			// SQL strings are 1-indexed.
			start := int(parser.MustBeDInt(args[1])) - 1

			if start < 0 {
				start = 0
			} else if start > len(runes) {
				start = len(runes)
			}

			return parser.NewDString(string(runes[start:])), nil
		},
		Info: "Returns a substring of `input` starting at `substr_pos` (count starts at 1).",
	},
	{
		Types: parser.ArgTypes{
			{"input", types.String},
			{"start_pos", types.Int},
			{"end_pos", types.Int},
		},
		ReturnType: parser.FixedReturnType(types.String),
		Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
			runes := []rune(string(parser.MustBeDString(args[0])))
			// SQL strings are 1-indexed.
			start := int(parser.MustBeDInt(args[1])) - 1
			length := int(parser.MustBeDInt(args[2]))

			if length < 0 {
				return nil, pgerror.NewErrorf(
					pgerror.CodeInvalidParameterValueError, "negative substring length %d not allowed", length)
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

			return parser.NewDString(string(runes[start:end])), nil
		},
		Info: "Returns a substring of `input` between `start_pos` and `end_pos` (count starts at 1).",
	},
	{
		Types: parser.ArgTypes{
			{"input", types.String},
			{"regex", types.String},
		},
		ReturnType: parser.FixedReturnType(types.String),
		Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
			s := string(parser.MustBeDString(args[0]))
			pattern := string(parser.MustBeDString(args[1]))
			return regexpExtract(ctx, s, pattern, `\`)
		},
		Info: "Returns a substring of `input` that matches the regular expression `regex`.",
	},
	{
		Types: parser.ArgTypes{
			{"input", types.String},
			{"regex", types.String},
			{"escape_char", types.String},
		},
		ReturnType: parser.FixedReturnType(types.String),
		Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
			s := string(parser.MustBeDString(args[0]))
			pattern := string(parser.MustBeDString(args[1]))
			escape := string(parser.MustBeDString(args[2]))
			return regexpExtract(ctx, s, pattern, escape)
		},
		Info: "Returns a substring of `input` that matches the regular expression `regex` using " +
			"`escape_char` as your escape character instead of `\\`.",
	},
}

var uuidV4Impl = parser.Builtin{
	Types:      parser.ArgTypes{},
	ReturnType: parser.FixedReturnType(types.Bytes),
	Category:   categoryIDGeneration,
	Impure:     true,
	Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
		return parser.NewDBytes(parser.DBytes(uuid.MakeV4().GetBytes())), nil
	},
	Info: "Returns a UUID.",
}

var ceilImpl = []parser.Builtin{
	floatBuiltin1(func(x float64) (parser.Datum, error) {
		return parser.NewDFloat(parser.DFloat(math.Ceil(x))), nil
	}, "Calculates the smallest integer greater than `val`."),
	decimalBuiltin1(func(x *apd.Decimal) (parser.Datum, error) {
		dd := &parser.DDecimal{}
		_, err := parser.ExactCtx.Ceil(&dd.Decimal, x)
		return dd, err
	}, "Calculates the smallest integer greater than `val`."),
}

var txnTSImpl = []parser.Builtin{
	{
		Types:             parser.ArgTypes{},
		ReturnType:        parser.FixedReturnType(types.TimestampTZ),
		PreferredOverload: true,
		Impure:            true,
		Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
			return ctx.GetTxnTimestamp(time.Microsecond), nil
		},
		Info: "Returns the current transaction's timestamp.",
	},
	{
		Types:      parser.ArgTypes{},
		ReturnType: parser.FixedReturnType(types.Timestamp),
		Impure:     true,
		Fn: func(ctx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
			return ctx.GetTxnTimestampNoZone(time.Microsecond), nil
		},
		Info: "Returns the current transaction's timestamp.",
	},
}

var powImpls = []parser.Builtin{
	floatBuiltin2("x", "y", func(x, y float64) (parser.Datum, error) {
		return parser.NewDFloat(parser.DFloat(math.Pow(x, y))), nil
	}, "Calculates `x`^`y`."),
	decimalBuiltin2("x", "y", func(x, y *apd.Decimal) (parser.Datum, error) {
		dd := &parser.DDecimal{}
		_, err := parser.DecimalCtx.Pow(&dd.Decimal, x, y)
		return dd, err
	}, "Calculates `x`^`y`."),
	{
		Types: parser.ArgTypes{
			{"x", types.Int},
			{"y", types.Int},
		},
		ReturnType: parser.FixedReturnType(types.Int),
		Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
			return parser.IntPow(parser.MustBeDInt(args[0]), parser.MustBeDInt(args[1]))
		},
		Info: "Calculates `x`^`y`.",
	},
}

func arrayBuiltin(impl func(types.T) parser.Builtin) []parser.Builtin {
	result := make([]parser.Builtin, 0, len(types.AnyNonArray))
	for _, typ := range types.AnyNonArray {
		if types.IsValidArrayElementType(typ) {
			result = append(result, impl(typ))
		}
	}
	return result
}

func decimalLogFn(
	logFn func(*apd.Decimal, *apd.Decimal) (apd.Condition, error), info string,
) parser.Builtin {
	return decimalBuiltin1(func(x *apd.Decimal) (parser.Datum, error) {
		// TODO(mjibson): see #13642
		switch x.Sign() {
		case -1:
			return nil, errLogOfNegNumber
		case 0:
			return nil, errLogOfZero
		}
		dd := &parser.DDecimal{}
		_, err := logFn(&dd.Decimal, x)
		return dd, err
	}, info)
}

func floatBuiltin1(f func(float64) (parser.Datum, error), info string) parser.Builtin {
	return parser.Builtin{
		Types:      parser.ArgTypes{{"val", types.Float}},
		ReturnType: parser.FixedReturnType(types.Float),
		Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
			return f(float64(*args[0].(*parser.DFloat)))
		},
		Info: info,
	}
}

func floatBuiltin2(
	a, b string, f func(float64, float64) (parser.Datum, error), info string,
) parser.Builtin {
	return parser.Builtin{
		Types:      parser.ArgTypes{{a, types.Float}, {b, types.Float}},
		ReturnType: parser.FixedReturnType(types.Float),
		Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
			return f(float64(*args[0].(*parser.DFloat)),
				float64(*args[1].(*parser.DFloat)))
		},
		Info: info,
	}
}

func decimalBuiltin1(f func(*apd.Decimal) (parser.Datum, error), info string) parser.Builtin {
	return parser.Builtin{
		Types:      parser.ArgTypes{{"val", types.Decimal}},
		ReturnType: parser.FixedReturnType(types.Decimal),
		Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
			dec := &args[0].(*parser.DDecimal).Decimal
			return f(dec)
		},
		Info: info,
	}
}

func decimalBuiltin2(
	a, b string, f func(*apd.Decimal, *apd.Decimal) (parser.Datum, error), info string,
) parser.Builtin {
	return parser.Builtin{
		Types:      parser.ArgTypes{{a, types.Decimal}, {b, types.Decimal}},
		ReturnType: parser.FixedReturnType(types.Decimal),
		Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
			dec1 := &args[0].(*parser.DDecimal).Decimal
			dec2 := &args[1].(*parser.DDecimal).Decimal
			return f(dec1, dec2)
		},
		Info: info,
	}
}

func stringBuiltin1(
	f func(*parser.EvalContext, string) (parser.Datum, error), returnType types.T, info string,
) parser.Builtin {
	return parser.Builtin{
		Types:      parser.ArgTypes{{"val", types.String}},
		ReturnType: parser.FixedReturnType(returnType),
		Fn: func(evalCtx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
			return f(evalCtx, string(parser.MustBeDString(args[0])))
		},
		Info: info,
	}
}

func stringBuiltin2(
	a, b string,
	f func(*parser.EvalContext, string, string) (parser.Datum, error),
	returnType types.T,
	info string,
) parser.Builtin {
	return parser.Builtin{
		Types:      parser.ArgTypes{{a, types.String}, {b, types.String}},
		ReturnType: parser.FixedReturnType(returnType),
		Category:   categorizeType(types.String),
		Fn: func(evalCtx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
			return f(evalCtx, string(parser.MustBeDString(args[0])), string(parser.MustBeDString(args[1])))
		},
		Info: info,
	}
}

func stringBuiltin3(
	a, b, c string,
	f func(*parser.EvalContext, string, string, string) (parser.Datum, error),
	returnType types.T,
	info string,
) parser.Builtin {
	return parser.Builtin{
		Types:      parser.ArgTypes{{a, types.String}, {b, types.String}, {c, types.String}},
		ReturnType: parser.FixedReturnType(returnType),
		Fn: func(evalCtx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
			return f(evalCtx, string(parser.MustBeDString(args[0])), string(parser.MustBeDString(args[1])), string(parser.MustBeDString(args[2])))
		},
		Info: info,
	}
}

func bytesBuiltin1(
	f func(*parser.EvalContext, string) (parser.Datum, error), returnType types.T, info string,
) parser.Builtin {
	return parser.Builtin{
		Types:      parser.ArgTypes{{"val", types.Bytes}},
		ReturnType: parser.FixedReturnType(returnType),
		Fn: func(evalCtx *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
			return f(evalCtx, string(*args[0].(*parser.DBytes)))
		},
		Info: info,
	}
}

func feedHash(h hash.Hash, args parser.Datums) {
	for _, datum := range args {
		if datum == parser.DNull {
			continue
		}
		var buf string
		if d, ok := datum.(*parser.DBytes); ok {
			buf = string(*d)
		} else {
			buf = string(parser.MustBeDString(datum))
		}
		if _, err := h.Write([]byte(buf)); err != nil {
			panic(errors.Wrap(err, `"It never returns an error." -- https://golang.org/pkg/hash`))
		}
	}
}

func hashBuiltin(newHash func() hash.Hash, info string) []parser.Builtin {
	return []parser.Builtin{
		{
			Types:        parser.VariadicType{Typ: types.String},
			ReturnType:   parser.FixedReturnType(types.String),
			NullableArgs: true,
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				h := newHash()
				feedHash(h, args)
				return parser.NewDString(fmt.Sprintf("%x", h.Sum(nil))), nil
			},
			Info: info,
		},
		{
			Types:        parser.VariadicType{Typ: types.Bytes},
			ReturnType:   parser.FixedReturnType(types.String),
			NullableArgs: true,
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				h := newHash()
				feedHash(h, args)
				return parser.NewDString(fmt.Sprintf("%x", h.Sum(nil))), nil
			},
			Info: info,
		},
	}
}

func hash32Builtin(newHash func() hash.Hash32, info string) []parser.Builtin {
	return []parser.Builtin{
		{
			Types:        parser.VariadicType{Typ: types.String},
			ReturnType:   parser.FixedReturnType(types.Int),
			NullableArgs: true,
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				h := newHash()
				feedHash(h, args)
				return parser.NewDInt(parser.DInt(h.Sum32())), nil
			},
			Info: info,
		},
		{
			Types:        parser.VariadicType{Typ: types.Bytes},
			ReturnType:   parser.FixedReturnType(types.Int),
			NullableArgs: true,
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				h := newHash()
				feedHash(h, args)
				return parser.NewDInt(parser.DInt(h.Sum32())), nil
			},
			Info: info,
		},
	}
}
func hash64Builtin(newHash func() hash.Hash64, info string) []parser.Builtin {
	return []parser.Builtin{
		{
			Types:        parser.VariadicType{Typ: types.String},
			ReturnType:   parser.FixedReturnType(types.Int),
			NullableArgs: true,
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				h := newHash()
				feedHash(h, args)
				return parser.NewDInt(parser.DInt(h.Sum64())), nil
			},
			Info: info,
		},
		{
			Types:        parser.VariadicType{Typ: types.Bytes},
			ReturnType:   parser.FixedReturnType(types.Int),
			NullableArgs: true,
			Fn: func(_ *parser.EvalContext, args parser.Datums) (parser.Datum, error) {
				h := newHash()
				feedHash(h, args)
				return parser.NewDInt(parser.DInt(h.Sum64())), nil
			},
			Info: info,
		},
	}
}

type regexpEscapeKey struct {
	sqlPattern string
	sqlEscape  string
}

// Pattern implements the RegexpCacheKey interface.
func (k regexpEscapeKey) Pattern() (string, error) {
	pattern := k.sqlPattern
	if k.sqlEscape != `\` {
		pattern = strings.Replace(pattern, `\`, `\\`, -1)
		pattern = strings.Replace(pattern, k.sqlEscape, `\`, -1)
	}
	return pattern, nil
}

func regexpExtract(ctx *parser.EvalContext, s, pattern, escape string) (parser.Datum, error) {
	patternRe, err := ctx.ReCache.GetRegexp(regexpEscapeKey{pattern, escape})
	if err != nil {
		return nil, err
	}

	match := patternRe.FindStringSubmatch(s)
	if match == nil {
		return parser.DNull, nil
	}

	if len(match) > 1 {
		return parser.NewDString(match[1]), nil
	}
	return parser.NewDString(match[0]), nil
}

type regexpFlagKey struct {
	sqlPattern string
	sqlFlags   string
}

// Pattern implements the RegexpCacheKey interface.
func (k regexpFlagKey) Pattern() (string, error) {
	return regexpEvalFlags(k.sqlPattern, k.sqlFlags)
}

func regexpReplace(ctx *parser.EvalContext, s, pattern, to, sqlFlags string) (parser.Datum, error) {
	patternRe, err := ctx.ReCache.GetRegexp(regexpFlagKey{pattern, sqlFlags})
	if err != nil {
		return nil, err
	}

	matchCount := 1
	if strings.ContainsRune(sqlFlags, 'g') {
		matchCount = -1
	}

	finalIndex := 0
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
		// matchStart and matchEnd are the boundaries of the current regexp match
		// in the searched text.
		matchStart := matchIndex[0]
		matchEnd := matchIndex[1]

		// Add sections of s either before the first match or between matches.
		preMatch := s[finalIndex:matchStart]
		newString.WriteString(preMatch)

		// We write out `to` into `newString` in chunks, flushing out the next chunk
		// when we hit a `\\` or a backreference.
		// chunkStart is the start of the next chunk we will flush out.
		chunkStart := 0
		// i is the current position in the replacement text that we are scanning
		// through.
		i := 0
		for i < len(to) {
			if to[i] == '\\' && i+1 < len(to) {
				i++
				if to[i] == '\\' {
					// `\\` is special in regexpReplace to insert a literal backslash.
					newString.WriteString(to[chunkStart:i])
					chunkStart = i + 1
				} else if ('0' <= to[i] && to[i] <= '9') || to[i] == '&' {
					newString.WriteString(to[chunkStart : i-1])
					chunkStart = i + 1
					if to[i] == '&' {
						// & refers to the entire match.
						newString.WriteString(s[matchStart:matchEnd])
					} else {
						idx := int(to[i] - '0')
						// regexpReplace expects references to "out-of-bounds" capture groups
						// to be ignored.
						if 2*idx < len(matchIndex) {
							newString.WriteString(s[matchIndex[2*idx]:matchIndex[2*idx+1]])
						}
					}
				}
			}
			i++
		}
		newString.WriteString(to[chunkStart:])

		finalIndex = matchEnd
	}

	// Add the section of s past the final match.
	newString.WriteString(s[finalIndex:])

	return parser.NewDString(newString.String()), nil
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
			return "", pgerror.NewErrorf(
				pgerror.CodeInvalidRegularExpressionError, "invalid regexp flag: %q", sqlFlag)
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

func overlay(s, to string, pos, size int) (parser.Datum, error) {
	if pos < 1 {
		return nil, pgerror.NewErrorf(
			pgerror.CodeInvalidParameterValueError, "non-positive substring length not allowed: %d", pos)
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
	return parser.NewDString(string(runes[:pos]) + to + string(runes[after:])), nil
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

func roundDecimal(x *apd.Decimal, n int32) (parser.Datum, error) {
	dd := &parser.DDecimal{}
	_, err := parser.HighPrecisionCtx.Quantize(&dd.Decimal, x, -n)
	return dd, err
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
func GenerateUniqueInt(nodeID roachpb.NodeID) parser.DInt {
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
	return parser.DInt(id)
}

func arrayLength(arr *parser.DArray, dim int64) parser.Datum {
	if arr.Len() == 0 || dim < 1 {
		return parser.DNull
	}
	if dim == 1 {
		return parser.NewDInt(parser.DInt(arr.Len()))
	}
	a, ok := parser.AsDArray(arr.Array[0])
	if !ok {
		return parser.DNull
	}
	return arrayLength(a, dim-1)
}

var intOne = parser.NewDInt(parser.DInt(1))

func arrayLower(arr *parser.DArray, dim int64) parser.Datum {
	if arr.Len() == 0 || dim < 1 {
		return parser.DNull
	}
	if dim == 1 {
		return intOne
	}
	a, ok := parser.AsDArray(arr.Array[0])
	if !ok {
		return parser.DNull
	}
	return arrayLower(a, dim-1)
}

func extractStringFromTimestamp(
	_ *parser.EvalContext, fromTime time.Time, timeSpan string,
) (parser.Datum, error) {
	switch timeSpan {
	case "year", "years":
		return parser.NewDInt(parser.DInt(fromTime.Year())), nil

	case "quarter":
		return parser.NewDInt(parser.DInt((fromTime.Month()-1)/3 + 1)), nil

	case "month", "months":
		return parser.NewDInt(parser.DInt(fromTime.Month())), nil

	case "week", "weeks":
		_, week := fromTime.ISOWeek()
		return parser.NewDInt(parser.DInt(week)), nil

	case "day", "days":
		return parser.NewDInt(parser.DInt(fromTime.Day())), nil

	case "dayofweek", "dow":
		return parser.NewDInt(parser.DInt(fromTime.Weekday())), nil

	case "dayofyear", "doy":
		return parser.NewDInt(parser.DInt(fromTime.YearDay())), nil

	case "hour", "hours":
		return parser.NewDInt(parser.DInt(fromTime.Hour())), nil

	case "minute", "minutes":
		return parser.NewDInt(parser.DInt(fromTime.Minute())), nil

	case "second", "seconds":
		return parser.NewDInt(parser.DInt(fromTime.Second())), nil

	case "millisecond", "milliseconds":
		// This a PG extension not supported in MySQL.
		return parser.NewDInt(parser.DInt(fromTime.Nanosecond() / int(time.Millisecond))), nil

	case "microsecond", "microseconds":
		return parser.NewDInt(parser.DInt(fromTime.Nanosecond() / int(time.Microsecond))), nil

	case "epoch":
		return parser.NewDInt(parser.DInt(fromTime.Unix())), nil

	default:
		return nil, pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError, "unsupported timespan: %s", timeSpan)
	}
}

func truncateTimestamp(
	_ *parser.EvalContext, fromTime time.Time, timeSpan string,
) (parser.Datum, error) {
	year := fromTime.Year()
	month := fromTime.Month()
	day := fromTime.Day()
	hour := fromTime.Hour()
	min := fromTime.Minute()
	sec := fromTime.Second()
	nsec := fromTime.Nanosecond()
	loc := fromTime.Location()

	monthTrunc := time.January
	dayTrunc := 1
	hourTrunc := 0
	minTrunc := 0
	secTrunc := 0
	nsecTrunc := 0

	switch timeSpan {
	case "year", "years":
		month, day, hour, min, sec, nsec = monthTrunc, dayTrunc, hourTrunc, minTrunc, secTrunc, nsecTrunc

	case "quarter":
		firstMonthInQuarter := ((month-1)/3)*3 + 1
		month, day, hour, min, sec, nsec = firstMonthInQuarter, dayTrunc, hourTrunc, minTrunc, secTrunc, nsecTrunc

	case "month", "months":
		day, hour, min, sec, nsec = dayTrunc, hourTrunc, minTrunc, secTrunc, nsecTrunc

	case "week", "weeks":
		// Subtract (day of week * nanoseconds per day) to get date as of previous Sunday.
		previousSunday := fromTime.Add(time.Duration(-1 * int64(fromTime.Weekday()) * int64(time.Hour) * 24))
		year, month, day = previousSunday.Year(), previousSunday.Month(), previousSunday.Day()
		hour, min, sec, nsec = hourTrunc, minTrunc, secTrunc, nsecTrunc

	case "day", "days":
		hour, min, sec, nsec = hourTrunc, minTrunc, secTrunc, nsecTrunc

	case "hour", "hours":
		min, sec, nsec = minTrunc, secTrunc, nsecTrunc

	case "minute", "minutes":
		sec, nsec = secTrunc, nsecTrunc

	case "second", "seconds":
		nsec = nsecTrunc

	case "millisecond", "milliseconds":
		// This a PG extension not supported in MySQL.
		milliseconds := (nsec / int(time.Millisecond)) * int(time.Millisecond)
		nsec = milliseconds

	case "microsecond", "microseconds":
		microseconds := (nsec / int(time.Microsecond)) * int(time.Microsecond)
		nsec = microseconds

	default:
		return nil, pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError, "unsupported timespan: %s", timeSpan)
	}

	toTime := time.Date(year, month, day, hour, min, sec, nsec, loc)
	return parser.MakeDTimestampTZ(toTime, time.Microsecond), nil
}
