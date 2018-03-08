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
	"encoding/hex"
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
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
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
	categorySequences     = "Sequence"
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
var Builtins = map[string][]tree.Builtin{
	// TODO(XisiHuang): support encoding, i.e., length(str, encoding).
	"length": {
		stringBuiltin1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDInt(tree.DInt(utf8.RuneCountInString(s))), nil
		}, types.Int, "Calculates the number of characters in `val`."),
		bytesBuiltin1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDInt(tree.DInt(len(s))), nil
		}, types.Int, "Calculates the number of bytes in `val`."),
	},

	"octet_length": {
		stringBuiltin1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDInt(tree.DInt(len(s))), nil
		}, types.Int, "Calculates the number of bytes used to represent `val`."),
		bytesBuiltin1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDInt(tree.DInt(len(s))), nil
		}, types.Int, "Calculates the number of bytes in `val`."),
	},

	// TODO(pmattis): What string functions should also support types.Bytes?

	"lower": {stringBuiltin1(func(evalCtx *tree.EvalContext, s string) (tree.Datum, error) {
		if err := evalCtx.ActiveMemAcc.Grow(evalCtx.Ctx(), int64(len(s))); err != nil {
			return nil, err
		}
		return tree.NewDString(strings.ToLower(s)), nil
	}, types.String, "Converts all characters in `val` to their lower-case equivalents.")},

	"upper": {stringBuiltin1(func(evalCtx *tree.EvalContext, s string) (tree.Datum, error) {
		if err := evalCtx.ActiveMemAcc.Grow(evalCtx.Ctx(), int64(len(s))); err != nil {
			return nil, err
		}
		return tree.NewDString(strings.ToUpper(s)), nil
	}, types.String, "Converts all characters in `val` to their to their upper-case equivalents.")},

	"substr":    substringImpls,
	"substring": substringImpls,

	// concat concatenates the text representations of all the arguments.
	// NULL arguments are ignored.
	"concat": {
		tree.Builtin{
			Types:        tree.VariadicType{VarType: types.String},
			ReturnType:   tree.FixedReturnType(types.String),
			NullableArgs: true,
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				var buffer bytes.Buffer
				for _, d := range args {
					if d == tree.DNull {
						continue
					}
					nextLength := len(string(tree.MustBeDString(d)))
					if err := evalCtx.ActiveMemAcc.Grow(evalCtx.Ctx(), int64(nextLength)); err != nil {
						return nil, err
					}
					buffer.WriteString(string(tree.MustBeDString(d)))
				}
				return tree.NewDString(buffer.String()), nil
			},
			Info: "Concatenates a comma-separated list of strings.",
		},
	},

	"concat_ws": {
		tree.Builtin{
			Types:        tree.VariadicType{VarType: types.String},
			ReturnType:   tree.FixedReturnType(types.String),
			NullableArgs: true,
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if len(args) == 0 {
					return nil, pgerror.NewErrorf(pgerror.CodeUndefinedFunctionError, errInsufficientArgsFmtString, "concat_ws")
				}
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				sep := string(tree.MustBeDString(args[0]))
				var buf bytes.Buffer
				prefix := ""
				for _, d := range args[1:] {
					if d == tree.DNull {
						continue
					}
					nextLength := len(prefix) + len(string(tree.MustBeDString(d)))
					if err := evalCtx.ActiveMemAcc.Grow(
						evalCtx.Ctx(), int64(nextLength)); err != nil {
						return nil, err
					}
					// Note: we can't use the range index here because that
					// would break when the 2nd argument is NULL.
					buf.WriteString(prefix)
					prefix = sep
					buf.WriteString(string(tree.MustBeDString(d)))
				}
				return tree.NewDString(buf.String()), nil
			},
			Info: "Uses the first argument as a separator between the concatenation of the " +
				"subsequent arguments. \n\nFor example `concat_ws('!','wow','great')` " +
				"returns `wow!great`.",
		},
	},

	"gen_random_uuid": {
		tree.Builtin{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.UUID),
			Category:   categoryIDGeneration,
			Impure:     true,
			Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				uv := uuid.MakeV4()
				return tree.NewDUuid(tree.DUuid{UUID: uv}), nil
			},
			Info: "Generates a random UUID and returns it as a value of UUID type.",
		},
	},

	"to_uuid": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"val", types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				uv, err := uuid.FromString(s)
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(uv.GetBytes())), nil
			},
			Info: "Converts the character string representation of a UUID to its byte string " +
				"representation.",
		},
	},

	"from_uuid": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"val", types.Bytes}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				b := []byte(*args[0].(*tree.DBytes))
				uv, err := uuid.FromBytes(b)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(uv.String()), nil
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
		tree.Builtin{
			Types:      tree.ArgTypes{{"val", types.INet}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				return tree.NewDString(dIPAddr.IPAddr.String()), nil
			},
			Info: "Converts the combined IP address and prefix length to an abbreviated display format as text." +
				"For INET types, this will omit the prefix length if it's not the default (32 or IPv4, 128 for IPv6)" +
				"\n\nFor example, `abbrev('192.168.1.2/24')` returns `'192.168.1.2/24'`",
		},
	},

	"broadcast": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"val", types.INet}},
			ReturnType: tree.FixedReturnType(types.INet),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				broadcastIPAddr := dIPAddr.IPAddr.Broadcast()
				return &tree.DIPAddr{IPAddr: broadcastIPAddr}, nil
			},
			Info: "Gets the broadcast address for the network address represented by the value." +
				"\n\nFor example, `broadcast('192.168.1.2/24')` returns `'192.168.1.255/24'`",
		},
	},

	"family": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"val", types.INet}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				if dIPAddr.Family == ipaddr.IPv4family {
					return tree.NewDInt(tree.DInt(4)), nil
				}
				return tree.NewDInt(tree.DInt(6)), nil
			},
			Info: "Extracts the IP family of the value; 4 for IPv4, 6 for IPv6." +
				"\n\nFor example, `family('::1')` returns `6`",
		},
	},

	"host": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"val", types.INet}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				s := dIPAddr.IPAddr.String()
				if i := strings.IndexByte(s, '/'); i != -1 {
					return tree.NewDString(s[:i]), nil
				}
				return tree.NewDString(s), nil
			},
			Info: "Extracts the address part of the combined address/prefixlen value as text." +
				"\n\nFor example, `host('192.168.1.2/16')` returns `'192.168.1.2'`",
		},
	},

	"hostmask": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"val", types.INet}},
			ReturnType: tree.FixedReturnType(types.INet),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				ipAddr := dIPAddr.IPAddr.Hostmask()
				return &tree.DIPAddr{IPAddr: ipAddr}, nil
			},
			Info: "Creates an IP host mask corresponding to the prefix length in the value." +
				"\n\nFor example, `hostmask('192.168.1.2/16')` returns `'0.0.255.255'`",
		},
	},

	"masklen": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"val", types.INet}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				return tree.NewDInt(tree.DInt(dIPAddr.Mask)), nil
			},
			Info: "Retrieves the prefix length stored in the value." +
				"\n\nFor example, `masklen('192.168.1.2/16')` returns `16`",
		},
	},

	"netmask": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"val", types.INet}},
			ReturnType: tree.FixedReturnType(types.INet),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				ipAddr := dIPAddr.IPAddr.Netmask()
				return &tree.DIPAddr{IPAddr: ipAddr}, nil
			},
			Info: "Creates an IP network mask corresponding to the prefix length in the value." +
				"\n\nFor example, `netmask('192.168.1.2/16')` returns `'255.255.0.0'`",
		},
	},

	"set_masklen": {
		tree.Builtin{
			Types: tree.ArgTypes{
				{"val", types.INet},
				{"prefixlen", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.INet),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				mask := int(tree.MustBeDInt(args[1]))

				if !(dIPAddr.Family == ipaddr.IPv4family && mask >= 0 && mask <= 32) && !(dIPAddr.Family == ipaddr.IPv6family && mask >= 0 && mask <= 128) {
					return nil, pgerror.NewErrorf(
						pgerror.CodeInvalidParameterValueError, "invalid mask length: %d", mask)
				}
				return &tree.DIPAddr{IPAddr: ipaddr.IPAddr{Family: dIPAddr.Family, Addr: dIPAddr.Addr, Mask: byte(mask)}}, nil
			},
			Info: "Sets the prefix length of `val` to `prefixlen`.\n\n" +
				"For example, `set_masklen('192.168.1.2', 16)` returns `'192.168.1.2/16'`.",
		},
	},

	"text": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"val", types.INet}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				s := dIPAddr.IPAddr.String()
				// Ensure the string has a "/mask" suffix.
				if strings.IndexByte(s, '/') == -1 {
					s += "/" + strconv.Itoa(int(dIPAddr.Mask))
				}
				return tree.NewDString(s), nil
			},
			Info: "Converts the IP address and prefix length to text.",
		},
	},

	"inet_same_family": {
		tree.Builtin{
			Types: tree.ArgTypes{
				{"val", types.INet},
				{"val", types.INet},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				first := tree.MustBeDIPAddr(args[0])
				other := tree.MustBeDIPAddr(args[1])
				return tree.MakeDBool(tree.DBool(first.Family == other.Family)), nil
			},
			Info: "Checks if two IP addresses are of the same IP family.",
		},
	},

	"inet_contained_by_or_equals": {
		tree.Builtin{
			Types: tree.ArgTypes{
				{"val", types.INet},
				{"container", types.INet},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ipAddr := tree.MustBeDIPAddr(args[0]).IPAddr
				other := tree.MustBeDIPAddr(args[1]).IPAddr
				return tree.MakeDBool(tree.DBool(ipAddr.ContainedByOrEquals(&other))), nil
			},
			Info: "Test for subnet inclusion or equality, using only the network parts of the addresses. " +
				"The host part of the addresses is ignored.",
		},
	},

	"inet_contains_or_contained_by": {
		tree.Builtin{
			Types: tree.ArgTypes{
				{"val", types.INet},
				{"val", types.INet},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ipAddr := tree.MustBeDIPAddr(args[0]).IPAddr
				other := tree.MustBeDIPAddr(args[1]).IPAddr
				return tree.MakeDBool(tree.DBool(ipAddr.ContainsOrContainedBy(&other))), nil
			},
			Info: "Test for subnet inclusion, using only the network parts of the addresses. " +
				"The host part of the addresses is ignored.",
		},
	},

	"inet_contains_or_equals": {
		tree.Builtin{
			Types: tree.ArgTypes{
				{"container", types.INet},
				{"val", types.INet},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ipAddr := tree.MustBeDIPAddr(args[0]).IPAddr
				other := tree.MustBeDIPAddr(args[1]).IPAddr
				return tree.MakeDBool(tree.DBool(ipAddr.ContainsOrEquals(&other))), nil
			},
			Info: "Test for subnet inclusion or equality, using only the network parts of the addresses. " +
				"The host part of the addresses is ignored.",
		},
	},

	"from_ip": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"val", types.Bytes}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ipstr := args[0].(*tree.DBytes)
				nboip := net.IP(*ipstr)
				sv := nboip.String()
				// if nboip has a length of 0, sv will be "<nil>"
				if sv == "<nil>" {
					return nil, errZeroIP
				}
				return tree.NewDString(sv), nil
			},
			Info: "Converts the byte string representation of an IP to its character string " +
				"representation.",
		},
	},

	"to_ip": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"val", types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ipdstr := tree.MustBeDString(args[0])
				ip := net.ParseIP(string(ipdstr))
				// If ipdstr could not be parsed to a valid IP,
				// ip will be nil.
				if ip == nil {
					return nil, pgerror.NewErrorf(
						pgerror.CodeInvalidParameterValueError, "invalid IP format: %s", ipdstr.String())
				}
				return tree.NewDBytes(tree.DBytes(ip)), nil
			},
			Info: "Converts the character string representation of an IP to its byte string " +
				"representation.",
		},
	},

	"split_part": {
		tree.Builtin{
			Types: tree.ArgTypes{
				{"input", types.String},
				{"delimiter", types.String},
				{"return_index_pos", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				text := string(tree.MustBeDString(args[0]))
				sep := string(tree.MustBeDString(args[1]))
				field := int(tree.MustBeDInt(args[2]))

				if field <= 0 {
					return nil, pgerror.NewErrorf(
						pgerror.CodeInvalidParameterValueError, "field position %d must be greater than zero", field)
				}

				splits := strings.Split(text, sep)
				if field > len(splits) {
					return tree.NewDString(""), nil
				}
				return tree.NewDString(splits[field-1]), nil
			},
			Info: "Splits `input` on `delimiter` and return the value in the `return_index_pos`  " +
				"position (starting at 1). \n\nFor example, `split_part('123.456.789.0','.',3)`" +
				"returns `789`.",
		},
	},

	"repeat": {
		tree.Builtin{
			Types:            tree.ArgTypes{{"input", types.String}, {"repeat_counter", types.Int}},
			DistsqlBlacklist: true,
			ReturnType:       tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (_ tree.Datum, err error) {
				s := string(tree.MustBeDString(args[0]))
				count := int(tree.MustBeDInt(args[1]))

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
				return tree.NewDString(strings.Repeat(s, count)), nil
			},
			Info: "Concatenates `input` `repeat_counter` number of times.\n\nFor example, " +
				"`repeat('dog', 2)` returns `dogdog`.",
		},
	},

	// https://www.postgresql.org/docs/10/static/functions-binarystring.html
	"encode": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"data", types.Bytes}, {"format", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (_ tree.Datum, err error) {
				data, format := *args[0].(*tree.DBytes), string(tree.MustBeDString(args[1]))
				switch format {
				case "hex":
					var buf bytes.Buffer
					lex.HexEncodeString(&buf, string(data))
					return tree.NewDString(buf.String()), nil
				case "escape":
					return tree.NewDString(encodeEscape([]byte(data))), nil
				default:
					return nil, pgerror.NewError(pgerror.CodeInvalidParameterValueError, "only 'hex' and 'escape' formats are supported for ENCODE")
				}
			},
			Info: "Encodes `data` in the text format specified by `format` (only \"hex\" and \"escape\" are supported).",
		},
	},

	"decode": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"text", types.String}, {"format", types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (_ tree.Datum, err error) {
				data, format := string(tree.MustBeDString(args[0])), string(tree.MustBeDString(args[1]))
				switch format {
				case "hex":
					decoded, err := hex.DecodeString(data)
					if err != nil {
						return nil, err
					}
					return tree.NewDBytes(tree.DBytes(decoded)), nil
				case "escape":
					decoded, err := decodeEscape(data)
					if err != nil {
						return nil, err
					}
					return tree.NewDBytes(tree.DBytes(decoded)), nil
				default:
					return nil, pgerror.NewError(pgerror.CodeInvalidParameterValueError, "only 'hex' and 'escape' formats are supported for DECODE")
				}
			},
			Info: "Decodes `data` as the format specified by `format` (only \"hex\" and \"escape\" are supported).",
		},
	},

	"ascii": {stringBuiltin1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
		for _, ch := range s {
			return tree.NewDInt(tree.DInt(ch)), nil
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
		tree.Builtin{
			Types:      tree.ArgTypes{{"val", types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(fmt.Sprintf("%x", int64(tree.MustBeDInt(args[0])))), nil
			},
			Info: "Converts `val` to its hexadecimal representation.",
		},
		tree.Builtin{
			Types:      tree.ArgTypes{{"val", types.Bytes}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				bytes := *(args[0].(*tree.DBytes))
				return tree.NewDString(fmt.Sprintf("%x", []byte(bytes))), nil
			},
			Info: "Converts `val` to its hexadecimal representation.",
		},
	},

	// The SQL parser coerces POSITION to STRPOS.
	"strpos": {stringBuiltin2("input", "find", func(_ *tree.EvalContext, s, substring string) (tree.Datum, error) {
		index := strings.Index(s, substring)
		if index < 0 {
			return tree.DZero, nil
		}

		return tree.NewDInt(tree.DInt(utf8.RuneCountInString(s[:index]) + 1)), nil
	}, types.Int, "Calculates the position where the string `find` begins in `input`. \n\nFor"+
		" example, `strpos('doggie', 'gie')` returns `4`.")},

	"overlay": {
		tree.Builtin{
			Types: tree.ArgTypes{
				{"input", types.String},
				{"overlay_val", types.String},
				{"start_pos", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				to := string(tree.MustBeDString(args[1]))
				pos := int(tree.MustBeDInt(args[2]))
				size := utf8.RuneCountInString(to)
				return overlay(s, to, pos, size)
			},
			Info: "Replaces characters in `input` with `overlay_val` starting at `start_pos` " +
				"(begins at 1). \n\nFor example, `overlay('doggie', 'CAT', 2)` returns " +
				"`dCATie`.",
		},
		tree.Builtin{
			Types: tree.ArgTypes{
				{"input", types.String},
				{"overlay_val", types.String},
				{"start_pos", types.Int},
				{"end_pos", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				to := string(tree.MustBeDString(args[1]))
				pos := int(tree.MustBeDInt(args[2]))
				size := int(tree.MustBeDInt(args[3]))
				return overlay(s, to, pos, size)
			},
			Info: "Deletes the characters in `input` between `start_pos` and `end_pos` (count " +
				"starts at 1), and then insert `overlay_val` at `start_pos`.",
		},
	},

	// The SQL parser coerces TRIM(...) and TRIM(BOTH ...) to BTRIM(...).
	"btrim": {
		stringBuiltin2("input", "trim_chars", func(_ *tree.EvalContext, s, chars string) (tree.Datum, error) {
			return tree.NewDString(strings.Trim(s, chars)), nil
		}, types.String, "Removes any characters included in `trim_chars` from the beginning or end"+
			" of `input` (applies recursively). \n\nFor example, `btrim('doggie', 'eod')` "+
			"returns `ggi`."),
		stringBuiltin1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDString(strings.TrimSpace(s)), nil
		}, types.String, "Removes all spaces from the beginning and end of `val`."),
	},

	// The SQL parser coerces TRIM(LEADING ...) to LTRIM(...).
	"ltrim": {
		stringBuiltin2("input", "trim_chars", func(_ *tree.EvalContext, s, chars string) (tree.Datum, error) {
			return tree.NewDString(strings.TrimLeft(s, chars)), nil
		}, types.String, "Removes any characters included in `trim_chars` from the beginning "+
			"(left-hand side) of `input` (applies recursively). \n\nFor example, "+
			"`ltrim('doggie', 'od')` returns `ggie`."),
		stringBuiltin1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDString(strings.TrimLeftFunc(s, unicode.IsSpace)), nil
		}, types.String, "Removes all spaces from the beginning (left-hand side) of `val`."),
	},

	// The SQL parser coerces TRIM(TRAILING ...) to RTRIM(...).
	"rtrim": {
		stringBuiltin2("input", "trim_chars", func(_ *tree.EvalContext, s, chars string) (tree.Datum, error) {
			return tree.NewDString(strings.TrimRight(s, chars)), nil
		}, types.String, "Removes any characters included in `trim_chars` from the end (right-hand "+
			"side) of `input` (applies recursively). \n\nFor example, `rtrim('doggie', 'ei')` "+
			"returns `dogg`."),
		stringBuiltin1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDString(strings.TrimRightFunc(s, unicode.IsSpace)), nil
		}, types.String, "Removes all spaces from the end (right-hand side) of `val`."),
	},

	"reverse": {stringBuiltin1(func(evalCtx *tree.EvalContext, s string) (tree.Datum, error) {
		if err := evalCtx.ActiveMemAcc.Grow(evalCtx.Ctx(), int64(len(s))); err != nil {
			return nil, err
		}
		runes := []rune(s)
		for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
			runes[i], runes[j] = runes[j], runes[i]
		}
		return tree.NewDString(string(runes)), nil
	}, types.String, "Reverses the order of the string's characters.")},

	"replace": {stringBuiltin3(
		"input", "find", "replace",
		func(evalCtx *tree.EvalContext, input, from, to string) (tree.Datum, error) {
			result := strings.Replace(input, from, to, -1)
			if err := evalCtx.ActiveMemAcc.Grow(evalCtx.Ctx(), int64(len(result))); err != nil {
				return nil, err
			}
			return tree.NewDString(result), nil
		},
		types.String,
		"Replaces all occurrences of `find` with `replace` in `input`",
	)},

	"translate": {stringBuiltin3(
		"input", "find", "replace",
		func(evalCtx *tree.EvalContext, s, from, to string) (tree.Datum, error) {
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
			return tree.NewDString(string(runes)), nil
		}, types.String, "In `input`, replaces the first character from `find` with the first "+
			"character in `replace`; repeat for each character in `find`. \n\nFor example, "+
			"`translate('doggie', 'dog', '123');` returns `1233ie`.")},

	"regexp_extract": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"input", types.String}, {"regex", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				pattern := string(tree.MustBeDString(args[1]))
				return regexpExtract(ctx, s, pattern, `\`)
			},
			Info: "Returns the first match for the Regular Expression `regex` in `input`.",
		},
	},

	"regexp_replace": {
		tree.Builtin{
			Types: tree.ArgTypes{
				{"input", types.String},
				{"regex", types.String},
				{"replace", types.String},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				pattern := string(tree.MustBeDString(args[1]))
				to := string(tree.MustBeDString(args[2]))
				result, err := regexpReplace(evalCtx, s, pattern, to, "")
				if err != nil {
					return nil, err
				}
				if err := evalCtx.ActiveMemAcc.Grow(evalCtx.Ctx(), int64(len(string(tree.MustBeDString(result))))); err != nil {
					return nil, err
				}
				return result, nil
			},
			Info: "Replaces matches for the Regular Expression `regex` in `input` with the " +
				"Regular Expression `replace`.",
		},
		tree.Builtin{
			Types: tree.ArgTypes{
				{"input", types.String},
				{"regex", types.String},
				{"replace", types.String},
				{"flags", types.String},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				pattern := string(tree.MustBeDString(args[1]))
				to := string(tree.MustBeDString(args[2]))
				sqlFlags := string(tree.MustBeDString(args[3]))
				result, err := regexpReplace(evalCtx, s, pattern, to, sqlFlags)
				if err != nil {
					return nil, err
				}
				if err := evalCtx.ActiveMemAcc.Grow(evalCtx.Ctx(), int64(len(string(tree.MustBeDString(result))))); err != nil {
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

	"initcap": {stringBuiltin1(func(evalCtx *tree.EvalContext, s string) (tree.Datum, error) {
		if err := evalCtx.ActiveMemAcc.Grow(evalCtx.Ctx(), int64(len(s))); err != nil {
			return nil, err
		}
		return tree.NewDString(strings.Title(strings.ToLower(s))), nil
	}, types.String, "Capitalizes the first letter of `val`.")},

	"left": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"input", types.Bytes}, {"return_set", types.Int}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				bytes := []byte(*args[0].(*tree.DBytes))
				n := int(tree.MustBeDInt(args[1]))

				if n < -len(bytes) {
					n = 0
				} else if n < 0 {
					n = len(bytes) + n
				} else if n > len(bytes) {
					n = len(bytes)
				}
				return tree.NewDBytes(tree.DBytes(bytes[:n])), nil
			},
			Info: "Returns the first `return_set` bytes from `input`.",
		},
		tree.Builtin{
			Types:      tree.ArgTypes{{"input", types.String}, {"return_set", types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				runes := []rune(string(tree.MustBeDString(args[0])))
				n := int(tree.MustBeDInt(args[1]))

				if n < -len(runes) {
					n = 0
				} else if n < 0 {
					n = len(runes) + n
				} else if n > len(runes) {
					n = len(runes)
				}
				return tree.NewDString(string(runes[:n])), nil
			},
			Info: "Returns the first `return_set` characters from `input`.",
		},
	},

	"right": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"input", types.Bytes}, {"return_set", types.Int}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				bytes := []byte(*args[0].(*tree.DBytes))
				n := int(tree.MustBeDInt(args[1]))

				if n < -len(bytes) {
					n = 0
				} else if n < 0 {
					n = len(bytes) + n
				} else if n > len(bytes) {
					n = len(bytes)
				}
				return tree.NewDBytes(tree.DBytes(bytes[len(bytes)-n:])), nil
			},
			Info: "Returns the last `return_set` bytes from `input`.",
		},
		tree.Builtin{
			Types:      tree.ArgTypes{{"input", types.String}, {"return_set", types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				runes := []rune(string(tree.MustBeDString(args[0])))
				n := int(tree.MustBeDInt(args[1]))

				if n < -len(runes) {
					n = 0
				} else if n < 0 {
					n = len(runes) + n
				} else if n > len(runes) {
					n = len(runes)
				}
				return tree.NewDString(string(runes[len(runes)-n:])), nil
			},
			Info: "Returns the last `return_set` characters from `input`.",
		},
	},

	"random": {
		tree.Builtin{
			Types:                   tree.ArgTypes{},
			ReturnType:              tree.FixedReturnType(types.Float),
			Impure:                  true,
			NeedsRepeatedEvaluation: true,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDFloat(tree.DFloat(rand.Float64())), nil
			},
			Info: "Returns a random float between 0 and 1.",
		},
	},

	"unique_rowid": {
		tree.Builtin{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Category:   categoryIDGeneration,
			Impure:     true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDInt(GenerateUniqueInt(ctx.NodeID)), nil
			},
			Info: "Returns a unique ID used by CockroachDB to generate unique row IDs if a " +
				"Primary Key isn't defined for the table. The value is a combination of the " +
				" insert timestamp and the ID of the node executing the statement, which " +
				" guarantees this combination is globally unique.",
		},
	},

	// Sequence functions.

	"nextval": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"sequence_name", types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Category:   categorySequences,
			Impure:     true,
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				name := tree.MustBeDString(args[0])
				qualifiedName, err := evalCtx.Sequence.ParseQualifiedTableName(evalCtx.Ctx(), string(name))
				if err != nil {
					return nil, err
				}
				res, err := evalCtx.Sequence.IncrementSequence(evalCtx.Ctx(), qualifiedName)
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(res)), nil
			},
			Info: "Advances the given sequence and returns its new value.",
		},
	},

	"currval": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"sequence_name", types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Category:   categorySequences,
			Impure:     true,
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				name := tree.MustBeDString(args[0])
				qualifiedName, err := evalCtx.Sequence.ParseQualifiedTableName(evalCtx.Ctx(), string(name))
				if err != nil {
					return nil, err
				}
				res, err := evalCtx.Sequence.GetLatestValueInSessionForSequence(evalCtx.Ctx(), qualifiedName)
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(res)), nil
			},
			Info: "Returns the latest value obtained with nextval for this sequence in this session.",
		},
	},

	"lastval": {
		tree.Builtin{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Category:   categorySequences,
			Impure:     true,
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				val, err := evalCtx.SessionData.SequenceState.GetLastValue()
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(val)), nil
			},
			Info: "Return value most recently obtained with nextval in this session.",
		},
	},

	// Note: behavior is slightly different than Postgres for performance reasons.
	// See https://github.com/cockroachdb/cockroach/issues/21564
	"setval": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"sequence_name", types.String}, {"value", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Category:   categorySequences,
			Impure:     true,
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				name := tree.MustBeDString(args[0])
				qualifiedName, err := evalCtx.Sequence.ParseQualifiedTableName(evalCtx.Ctx(), string(name))
				if err != nil {
					return nil, err
				}

				newVal := tree.MustBeDInt(args[1])
				if err := evalCtx.Sequence.SetSequenceValue(
					evalCtx.Ctx(), qualifiedName, int64(newVal), true); err != nil {
					return nil, err
				}
				return args[1], nil
			},
			Info: "Set the given sequence's current value. The next call to nextval will return " +
				"`value + Increment`",
		},
		tree.Builtin{
			Types: tree.ArgTypes{
				{"sequence_name", types.String}, {"value", types.Int}, {"is_called", types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Category:   categorySequences,
			Impure:     true,
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				name := tree.MustBeDString(args[0])
				qualifiedName, err := evalCtx.Sequence.ParseQualifiedTableName(evalCtx.Ctx(), string(name))
				if err != nil {
					return nil, err
				}

				isCalled := bool(tree.MustBeDBool(args[2]))

				newVal := tree.MustBeDInt(args[1])
				if err := evalCtx.Sequence.SetSequenceValue(
					evalCtx.Ctx(), qualifiedName, int64(newVal), isCalled); err != nil {
					return nil, err
				}
				return args[1], nil
			},
			Info: "Set the given sequence's current value. If is_called is false, the next call to " +
				"nextval will return `value`; otherwise `value + Increment`.",
		},
	},

	"experimental_uuid_v4": {uuidV4Impl},
	"uuid_v4":              {uuidV4Impl},

	"greatest": {
		tree.Builtin{
			Types:        tree.HomogeneousType{},
			ReturnType:   tree.IdentityReturnType(0),
			NullableArgs: true,
			Category:     categoryComparison,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.PickFromTuple(ctx, true /* greatest */, args)
			},
			Info: "Returns the element with the greatest value.",
		},
	},
	"least": {
		tree.Builtin{
			Types:        tree.HomogeneousType{},
			ReturnType:   tree.IdentityReturnType(0),
			NullableArgs: true,
			Category:     categoryComparison,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.PickFromTuple(ctx, false /* greatest */, args)
			},
			Info: "Returns the element with the lowest value.",
		},
	},

	// Timestamp/Date functions.

	"experimental_strftime": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"input", types.Timestamp}, {"extract_format", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Category:   categoryDateAndTime,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fromTime := args[0].(*tree.DTimestamp).Time
				format := string(tree.MustBeDString(args[1]))
				t, err := strtime.Strftime(fromTime, format)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(t), nil
			},
			Info: "From `input`, extracts and formats the time as identified in `extract_format` " +
				"using standard `strftime` notation (though not all formatting is supported).",
		},
		tree.Builtin{
			Types:      tree.ArgTypes{{"input", types.Date}, {"extract_format", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Category:   categoryDateAndTime,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fromTime := timeutil.Unix(int64(*args[0].(*tree.DDate))*tree.SecondsInDay, 0)
				format := string(tree.MustBeDString(args[1]))
				t, err := strtime.Strftime(fromTime, format)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(t), nil
			},
			Info: "From `input`, extracts and formats the time as identified in `extract_format` " +
				"using standard `strftime` notation (though not all formatting is supported).",
		},
		tree.Builtin{
			Types:      tree.ArgTypes{{"input", types.TimestampTZ}, {"extract_format", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Category:   categoryDateAndTime,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fromTime := args[0].(*tree.DTimestampTZ).Time
				format := string(tree.MustBeDString(args[1]))
				t, err := strtime.Strftime(fromTime, format)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(t), nil
			},
			Info: "From `input`, extracts and formats the time as identified in `extract_format` " +
				"using standard `strftime` notation (though not all formatting is supported).",
		},
	},

	"experimental_strptime": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"input", types.String}, {"format", types.String}},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Category:   categoryDateAndTime,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				toParse := string(tree.MustBeDString(args[0]))
				format := string(tree.MustBeDString(args[1]))
				t, err := strtime.Strptime(toParse, format)
				if err != nil {
					return nil, err
				}
				return tree.MakeDTimestampTZ(t.UTC(), time.Microsecond), nil
			},
			Info: "Returns `input` as a timestamptz using `format` (which uses standard " +
				"`strptime` formatting).",
		},
	},

	"age": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"val", types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.Interval),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.TimestampDifference(ctx, ctx.GetTxnTimestamp(time.Microsecond), args[0])
			},
			Info: "Calculates the interval between `val` and the current time.",
		},
		tree.Builtin{
			Types:      tree.ArgTypes{{"begin", types.TimestampTZ}, {"end", types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.Interval),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.TimestampDifference(ctx, args[0], args[1])
			},
			Info: "Calculates the interval between `begin` and `end`.",
		},
	},

	"current_date": {
		tree.Builtin{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				t := ctx.GetTxnTimestamp(time.Microsecond).Time
				return tree.NewDDateFromTime(t, ctx.GetLocation()), nil
			},
			Info: "Returns the current date.",
		},
	},

	"now":                   txnTSImpl,
	"current_timestamp":     txnTSImpl,
	"transaction_timestamp": txnTSImpl,

	"statement_timestamp": {
		tree.Builtin{
			Types:             tree.ArgTypes{},
			ReturnType:        tree.FixedReturnType(types.TimestampTZ),
			PreferredOverload: true,
			Impure:            true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDTimestampTZ(ctx.GetStmtTimestamp(), time.Microsecond), nil
			},
			Info: "Returns the current statement's timestamp.",
		},
		tree.Builtin{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Impure:     true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDTimestamp(ctx.GetStmtTimestamp(), time.Microsecond), nil
			},
			Info: "Returns the current statement's timestamp.",
		},
	},

	"cluster_logical_timestamp": {
		tree.Builtin{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Decimal),
			Category:   categorySystemInfo,
			Impure:     true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return ctx.GetClusterTimestamp(), nil
			},
			Info: "This function is used only by CockroachDB's developers for testing purposes.",
		},
	},

	"clock_timestamp": {
		tree.Builtin{
			Types:             tree.ArgTypes{},
			ReturnType:        tree.FixedReturnType(types.TimestampTZ),
			PreferredOverload: true,
			Impure:            true,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDTimestampTZ(timeutil.Now(), time.Microsecond), nil
			},
			Info: "Returns the current wallclock time.",
		},
		tree.Builtin{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Impure:     true,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDTimestamp(timeutil.Now(), time.Microsecond), nil
			},
			Info: "Returns the current wallclock time.",
		},
	},

	"extract": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"element", types.String}, {"input", types.Timestamp}},
			ReturnType: tree.FixedReturnType(types.Int),
			Category:   categoryDateAndTime,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// extract timeSpan fromTime.
				fromTS := args[1].(*tree.DTimestamp)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				return extractStringFromTimestamp(ctx, fromTS.Time, timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: year, quarter, month, week, dayofweek, dayofyear,\n" +
				"hour, minute, second, millisecond, microsecond, epoch",
		},
		tree.Builtin{
			Types:      tree.ArgTypes{{"element", types.String}, {"input", types.Date}},
			ReturnType: tree.FixedReturnType(types.Int),
			Category:   categoryDateAndTime,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				date := args[1].(*tree.DDate)
				fromTSTZ := tree.MakeDTimestampTZFromDate(ctx.GetLocation(), date)
				return extractStringFromTimestamp(ctx, fromTSTZ.Time, timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: year, quarter, month, week, dayofweek, dayofyear,\n" +
				"hour, minute, second, millisecond, microsecond, epoch",
		},
		tree.Builtin{
			Types:      tree.ArgTypes{{"element", types.String}, {"input", types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.Int),
			Category:   categoryDateAndTime,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fromTSTZ := args[1].(*tree.DTimestampTZ)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				return extractStringFromTimestamp(ctx, fromTSTZ.Time, timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: year, quarter, month, week, dayofweek, dayofyear,\n" +
				"hour, minute, second, millisecond, microsecond, epoch",
		},
		tree.Builtin{
			Types:      tree.ArgTypes{{"element", types.String}, {"input", types.Time}},
			ReturnType: tree.FixedReturnType(types.Int),
			Category:   categoryDateAndTime,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fromTime := args[1].(*tree.DTime)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				return extractStringFromTime(fromTime, timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: hour, minute, second, millisecond, microsecond, epoch",
		},
	},

	"extract_duration": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"element", types.String}, {"input", types.Interval}},
			ReturnType: tree.FixedReturnType(types.Int),
			Category:   categoryDateAndTime,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// extract timeSpan fromTime.
				fromInterval := *args[1].(*tree.DInterval)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				switch timeSpan {
				case "hour", "hours":
					return tree.NewDInt(tree.DInt(fromInterval.Nanos / int64(time.Hour))), nil

				case "minute", "minutes":
					return tree.NewDInt(tree.DInt(fromInterval.Nanos / int64(time.Minute))), nil

				case "second", "seconds":
					return tree.NewDInt(tree.DInt(fromInterval.Nanos / int64(time.Second))), nil

				case "millisecond", "milliseconds":
					// This a PG extension not supported in MySQL.
					return tree.NewDInt(tree.DInt(fromInterval.Nanos / int64(time.Millisecond))), nil

				case "microsecond", "microseconds":
					return tree.NewDInt(tree.DInt(fromInterval.Nanos / int64(time.Microsecond))), nil

				default:
					return nil, pgerror.NewErrorf(
						pgerror.CodeInvalidParameterValueError, "unsupported timespan: %s", timeSpan)
				}
			},
			Info: "Extracts `element` from `input`.\n" +
				"Compatible elements: hour, minute, second, millisecond, microsecond.",
		},
	},

	// https://www.postgresql.org/docs/10/static/functions-datetime.html#FUNCTIONS-DATETIME-TRUNC
	//
	// PostgreSQL documents date_trunc for timestamp, timestamptz and
	// interval. It will also handle date and time inputs by casting them,
	// so we support those for compatibility. This gives us the following
	// function signatures:
	//
	//  date_trunc(string, time)        -> interval
	//  date_trunc(string, date)        -> timestamptz
	//  date_trunc(string, timestamp)   -> timestamp
	//  date_trunc(string, timestamptz) -> timestamptz
	//
	// See the following snippet from running the functions in PostgreSQL:
	//
	// 		postgres=# select pg_typeof(date_trunc('month', '2017-04-11 00:00:00'::timestamp));
	// 							pg_typeof
	// 		-----------------------------
	// 		timestamp without time zone
	//
	// 		postgres=# select pg_typeof(date_trunc('month', '2017-04-11 00:00:00'::date));
	// 						pg_typeof
	// 		--------------------------
	// 		timestamp with time zone
	//
	// 		postgres=# select pg_typeof(date_trunc('month', '2017-04-11 00:00:00'::time));
	// 		pg_typeof
	// 		-----------
	// 		interval
	//
	// This implicit casting behavior is mentioned in the PostgreSQL documentation:
	// https://www.postgresql.org/docs/10/static/functions-datetime.html
	// > source is a value expression of type timestamp or interval. (Values
	// > of type date and time are cast automatically to timestamp or interval,
	// > respectively.)
	//
	"date_trunc": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"element", types.String}, {"input", types.Timestamp}},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Category:   categoryDateAndTime,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				fromTS := args[1].(*tree.DTimestamp)
				tsTZ, err := truncateTimestamp(ctx, fromTS.Time, timeSpan)
				if err != nil {
					return nil, err
				}
				return tree.MakeDTimestamp(tsTZ.Time.In(ctx.GetLocation()), time.Microsecond), nil
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero (or one, for day and month)\n\n" +
				"Compatible elements: year, quarter, month, week, hour, minute, second,\n" +
				"millisecond, microsecond.",
		},
		tree.Builtin{
			Types:      tree.ArgTypes{{"element", types.String}, {"input", types.Date}},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Category:   categoryDateAndTime,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				date := args[1].(*tree.DDate)
				fromTSTZ := tree.MakeDTimestampTZFromDate(ctx.GetLocation(), date)
				return truncateTimestamp(ctx, fromTSTZ.Time, timeSpan)
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero (or one, for day and month)\n\n" +
				"Compatible elements: year, quarter, month, week, hour, minute, second,\n" +
				"millisecond, microsecond.",
		},
		tree.Builtin{
			Types:      tree.ArgTypes{{"element", types.String}, {"input", types.Time}},
			ReturnType: tree.FixedReturnType(types.Interval),
			Category:   categoryDateAndTime,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				fromTime := args[1].(*tree.DTime)
				time, err := truncateTime(fromTime, timeSpan)
				if err != nil {
					return nil, err
				}
				return &tree.DInterval{Duration: duration.Duration{Nanos: int64(*time) * 1000}}, nil
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero.\n\n" +
				"Compatible elements: hour, minute, second, millisecond, microsecond.",
		},
		tree.Builtin{
			Types:      tree.ArgTypes{{"element", types.String}, {"input", types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Category:   categoryDateAndTime,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fromTSTZ := args[1].(*tree.DTimestampTZ)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
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
		floatBuiltin1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Abs(x))), nil
		}, "Calculates the absolute value of `val`."),
		decimalBuiltin1(func(x *apd.Decimal) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			dd.Abs(x)
			return dd, nil
		}, "Calculates the absolute value of `val`."),
		tree.Builtin{
			Types:      tree.ArgTypes{{"val", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				x := tree.MustBeDInt(args[0])
				switch {
				case x == math.MinInt64:
					return nil, errAbsOfMinInt64
				case x < 0:
					return tree.NewDInt(-x), nil
				}
				return args[0], nil
			},
			Info: "Calculates the absolute value of `val`.",
		},
	},

	"acos": {
		floatBuiltin1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Acos(x))), nil
		}, "Calculates the inverse cosine of `val`."),
	},

	"asin": {
		floatBuiltin1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Asin(x))), nil
		}, "Calculates the inverse sine of `val`."),
	},

	"atan": {
		floatBuiltin1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Atan(x))), nil
		}, "Calculates the inverse tangent of `val`."),
	},

	"atan2": {
		floatBuiltin2("x", "y", func(x, y float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Atan2(x, y))), nil
		}, "Calculates the inverse tangent of `x`/`y`."),
	},

	"cbrt": {
		floatBuiltin1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Cbrt(x))), nil
		}, "Calculates the cube root (∛) of `val`."),
		decimalBuiltin1(func(x *apd.Decimal) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			_, err := tree.DecimalCtx.Cbrt(&dd.Decimal, x)
			return dd, err
		}, "Calculates the cube root (∛) of `val`."),
	},

	"ceil":    ceilImpl,
	"ceiling": ceilImpl,

	"cos": {
		floatBuiltin1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Cos(x))), nil
		}, "Calculates the cosine of `val`."),
	},

	"cot": {
		floatBuiltin1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(1 / math.Tan(x))), nil
		}, "Calculates the cotangent of `val`."),
	},

	"degrees": {
		floatBuiltin1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(180.0 * x / math.Pi)), nil
		}, "Converts `val` as a radian value to a degree value."),
	},

	"div": {
		floatBuiltin2("x", "y", func(x, y float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Trunc(x / y))), nil
		}, "Calculates the integer quotient of `x`/`y`."),
		decimalBuiltin2("x", "y", func(x, y *apd.Decimal) (tree.Datum, error) {
			if y.Sign() == 0 {
				return nil, tree.ErrDivByZero
			}
			dd := &tree.DDecimal{}
			_, err := tree.HighPrecisionCtx.QuoInteger(&dd.Decimal, x, y)
			return dd, err
		}, "Calculates the integer quotient of `x`/`y`."),
		{
			Types:      tree.ArgTypes{{"x", types.Int}, {"y", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				y := tree.MustBeDInt(args[1])
				if y == 0 {
					return nil, tree.ErrDivByZero
				}
				x := tree.MustBeDInt(args[0])
				return tree.NewDInt(x / y), nil
			},
			Info: "Calculates the integer quotient of `x`/`y`.",
		},
	},

	"exp": {
		floatBuiltin1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Exp(x))), nil
		}, "Calculates *e* ^ `val`."),
		decimalBuiltin1(func(x *apd.Decimal) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			_, err := tree.DecimalCtx.Exp(&dd.Decimal, x)
			return dd, err
		}, "Calculates *e* ^ `val`."),
	},

	"floor": {
		floatBuiltin1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Floor(x))), nil
		}, "Calculates the largest integer not greater than `val`."),
		decimalBuiltin1(func(x *apd.Decimal) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			_, err := tree.ExactCtx.Floor(&dd.Decimal, x)
			return dd, err
		}, "Calculates the largest integer not greater than `val`."),
	},

	"isnan": {
		tree.Builtin{
			// Can't use floatBuiltin1 here because this one returns
			// a boolean.
			Types:      tree.ArgTypes{{"val", types.Float}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDBool(tree.DBool(math.IsNaN(float64(*args[0].(*tree.DFloat))))), nil
			},
			Info: "Returns true if `val` is NaN, false otherwise.",
		},
		tree.Builtin{
			Types:      tree.ArgTypes{{"val", types.Decimal}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				isNaN := args[0].(*tree.DDecimal).Decimal.Form == apd.NaN
				return tree.MakeDBool(tree.DBool(isNaN)), nil
			},
			Info: "Returns true if `val` is NaN, false otherwise.",
		},
	},

	"json_remove_path": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"val", types.JSON}, {"path", types.TArray{Typ: types.String}}},
			ReturnType: tree.FixedReturnType(types.JSON),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				path := darrayToStringSlice(*tree.MustBeDArray(args[1]))
				s, _, err := tree.MustBeDJSON(args[0]).JSON.RemovePath(path)
				if err != nil {
					return nil, err
				}
				return &tree.DJSON{JSON: s}, nil
			},
			Info: "Remove the specified path from the JSON object.",
		},
	},

	"json_extract_path": {jsonExtractPathImpl},

	"jsonb_extract_path": {jsonExtractPathImpl},

	"json_set": {jsonSetImpl, jsonSetWithCreateMissingImpl},

	"jsonb_set": {jsonSetImpl, jsonSetWithCreateMissingImpl},

	"jsonb_pretty": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"val", types.JSON}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s, err := json.Pretty(tree.MustBeDJSON(args[0]).JSON)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(s), nil
			},
			Info: "Returns the given JSON value as a STRING indented and with newlines.",
		},
	},

	"json_typeof": {jsonTypeOfImpl},

	"jsonb_typeof": {jsonTypeOfImpl},

	"to_json": {toJSONImpl},

	"to_jsonb": {toJSONImpl},

	"json_build_array": {jsonBuildArrayImpl},

	"jsonb_build_array": {jsonBuildArrayImpl},

	"json_build_object": {jsonBuildObjectImpl},

	"jsonb_build_object": {jsonBuildObjectImpl},

	"json_object": jsonObjectImpls,

	"jsonb_object": jsonObjectImpls,

	"json_strip_nulls": {jsonStripNullsImpl},

	"jsonb_strip_nulls": {jsonStripNullsImpl},

	"json_array_length": {jsonArrayLengthImpl},

	"jsonb_array_length": {jsonArrayLengthImpl},

	"ln": {
		floatBuiltin1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Log(x))), nil
		}, "Calculates the natural log of `val`."),
		decimalLogFn(tree.DecimalCtx.Ln, "Calculates the natural log of `val`."),
	},

	"log": {
		floatBuiltin1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Log10(x))), nil
		}, "Calculates the base 10 log of `val`."),
		decimalLogFn(tree.DecimalCtx.Log10, "Calculates the base 10 log of `val`."),
	},

	"mod": {
		floatBuiltin2("x", "y", func(x, y float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Mod(x, y))), nil
		}, "Calculates `x`%`y`."),
		decimalBuiltin2("x", "y", func(x, y *apd.Decimal) (tree.Datum, error) {
			if y.Sign() == 0 {
				return nil, tree.ErrZeroModulus
			}
			dd := &tree.DDecimal{}
			_, err := tree.HighPrecisionCtx.Rem(&dd.Decimal, x, y)
			return dd, err
		}, "Calculates `x`%`y`."),
		tree.Builtin{
			Types:      tree.ArgTypes{{"x", types.Int}, {"y", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				y := tree.MustBeDInt(args[1])
				if y == 0 {
					return nil, tree.ErrZeroModulus
				}
				x := tree.MustBeDInt(args[0])
				return tree.NewDInt(x % y), nil
			},
			Info: "Calculates `x`%`y`.",
		},
	},

	"pi": {
		tree.Builtin{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDFloat(math.Pi), nil
			},
			Info: "Returns the value for pi (3.141592653589793).",
		},
	},

	"pow":   powImpls,
	"power": powImpls,

	"radians": {
		floatBuiltin1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(x * math.Pi / 180.0)), nil
		}, "Converts `val` as a degree value to a radians value."),
	},

	"round": {
		floatBuiltin1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.RoundToEven(x))), nil
		}, "Rounds `val` to the nearest integer using half to even (banker's) rounding."),
		decimalBuiltin1(func(x *apd.Decimal) (tree.Datum, error) {
			return roundDecimal(x, 0)
		}, "Rounds `val` to the nearest integer, half away from zero: "+
			"ROUND(+/-2.4) = +/-2, ROUND(+/-2.5) = +/-3."),
		tree.Builtin{
			Types:      tree.ArgTypes{{"input", types.Float}, {"decimal_accuracy", types.Int}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				f := float64(*args[0].(*tree.DFloat))
				if math.IsInf(f, 0) || math.IsNaN(f) {
					return args[0], nil
				}
				var x apd.Decimal
				if _, err := x.SetFloat64(f); err != nil {
					return nil, err
				}

				// TODO(mjibson): make sure this fits in an int32.
				scale := int32(tree.MustBeDInt(args[1]))

				var d apd.Decimal
				if _, err := tree.RoundCtx.Quantize(&d, &x, -scale); err != nil {
					return nil, err
				}

				f, err := d.Float64()
				if err != nil {
					return nil, err
				}

				return tree.NewDFloat(tree.DFloat(f)), nil
			},
			Info: "Keeps `decimal_accuracy` number of figures to the right of the zero position " +
				" in `input` using half to even (banker's) rounding.",
		},
		tree.Builtin{
			Types:      tree.ArgTypes{{"input", types.Decimal}, {"decimal_accuracy", types.Int}},
			ReturnType: tree.FixedReturnType(types.Decimal),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// TODO(mjibson): make sure this fits in an int32.
				scale := int32(tree.MustBeDInt(args[1]))
				return roundDecimal(&args[0].(*tree.DDecimal).Decimal, scale)
			},
			Info: "Keeps `decimal_accuracy` number of figures to the right of the zero position " +
				" in `input using half away from zero rounding. If `decimal_accuracy` " +
				"is not in the range -2^31...(2^31-1), the results are undefined.",
		},
	},

	"sin": {
		floatBuiltin1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Sin(x))), nil
		}, "Calculates the sine of `val`."),
	},

	"sign": {
		floatBuiltin1(func(x float64) (tree.Datum, error) {
			switch {
			case x < 0:
				return tree.NewDFloat(-1), nil
			case x == 0:
				return tree.NewDFloat(0), nil
			}
			return tree.NewDFloat(1), nil
		}, "Determines the sign of `val`: **1** for positive; **0** for 0 values; **-1** for "+
			"negative."),
		decimalBuiltin1(func(x *apd.Decimal) (tree.Datum, error) {
			d := &tree.DDecimal{}
			d.Decimal.SetCoefficient(int64(x.Sign()))
			return d, nil
		}, "Determines the sign of `val`: **1** for positive; **0** for 0 values; **-1** for "+
			"negative."),
		tree.Builtin{
			Types:      tree.ArgTypes{{"val", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				x := tree.MustBeDInt(args[0])
				switch {
				case x < 0:
					return tree.NewDInt(-1), nil
				case x == 0:
					return tree.DZero, nil
				}
				return tree.NewDInt(1), nil
			},
			Info: "Determines the sign of `val`: **1** for positive; **0** for 0 values; **-1** " +
				"for negative.",
		},
	},

	"sqrt": {
		floatBuiltin1(func(x float64) (tree.Datum, error) {
			// TODO(mjibson): see #13642
			if x < 0 {
				return nil, errSqrtOfNegNumber
			}
			return tree.NewDFloat(tree.DFloat(math.Sqrt(x))), nil
		}, "Calculates the square root of `val`."),
		decimalBuiltin1(func(x *apd.Decimal) (tree.Datum, error) {
			if x.Sign() < 0 {
				return nil, errSqrtOfNegNumber
			}
			dd := &tree.DDecimal{}
			_, err := tree.DecimalCtx.Sqrt(&dd.Decimal, x)
			return dd, err
		}, "Calculates the square root of `val`."),
	},

	"tan": {
		floatBuiltin1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Tan(x))), nil
		}, "Calculates the tangent of `val`."),
	},

	"trunc": {
		floatBuiltin1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Trunc(x))), nil
		}, "Truncates the decimal values of `val`."),
		decimalBuiltin1(func(x *apd.Decimal) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			x.Modf(&dd.Decimal, nil)
			return dd, nil
		}, "Truncates the decimal values of `val`."),
	},

	"to_english": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"val", types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				val := int(*args[0].(*tree.DInt))
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
				return tree.NewDString(buf.String()), nil
			},
			Category: categoryString,
			Info:     "This function enunciates the value of its argument using English cardinals.",
		},
	},

	// Array functions.

	"string_to_array": {
		tree.Builtin{
			Types:        tree.ArgTypes{{"str", types.String}, {"delimiter", types.String}},
			ReturnType:   tree.FixedReturnType(types.TArray{Typ: types.String}),
			Category:     categoryArray,
			NullableArgs: true,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				str := string(tree.MustBeDString(args[0]))
				delimOrNil := stringOrNil(args[1])
				return stringToArray(str, delimOrNil, nil)
			},
			Info: "Split a string into components on a delimiter.",
		},
		tree.Builtin{
			Types:        tree.ArgTypes{{"str", types.String}, {"delimiter", types.String}, {"null", types.String}},
			ReturnType:   tree.FixedReturnType(types.TArray{Typ: types.String}),
			Category:     categoryArray,
			NullableArgs: true,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				str := string(tree.MustBeDString(args[0]))
				delimOrNil := stringOrNil(args[1])
				nullStr := stringOrNil(args[2])
				return stringToArray(str, delimOrNil, nullStr)
			},
			Info: "Split a string into components on a delimiter with a specified string to consider NULL.",
		},
	},

	"array_length": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"input", types.AnyArray}, {"array_dimension", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Category:   categoryArray,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				arr := tree.MustBeDArray(args[0])
				dimen := int64(tree.MustBeDInt(args[1]))
				return arrayLength(arr, dimen), nil
			},
			Info: "Calculates the length of `input` on the provided `array_dimension`. However, " +
				"because CockroachDB doesn't yet support multi-dimensional arrays, the only supported" +
				" `array_dimension` is **1**.",
		},
	},

	"array_lower": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"input", types.AnyArray}, {"array_dimension", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Category:   categoryArray,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				arr := tree.MustBeDArray(args[0])
				dimen := int64(tree.MustBeDInt(args[1]))
				return arrayLower(arr, dimen), nil
			},
			Info: "Calculates the minimum value of `input` on the provided `array_dimension`. " +
				"However, because CockroachDB doesn't yet support multi-dimensional arrays, the only " +
				"supported `array_dimension` is **1**.",
		},
	},

	"array_upper": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"input", types.AnyArray}, {"array_dimension", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Category:   categoryArray,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				arr := tree.MustBeDArray(args[0])
				dimen := int64(tree.MustBeDInt(args[1]))
				return arrayLength(arr, dimen), nil
			},
			Info: "Calculates the maximum value of `input` on the provided `array_dimension`. " +
				"However, because CockroachDB doesn't yet support multi-dimensional arrays, the only " +
				"supported `array_dimension` is **1**.",
		},
	},

	"array_append": arrayBuiltin(func(typ types.T) tree.Builtin {
		return tree.Builtin{
			Types:        tree.ArgTypes{{"array", types.TArray{Typ: typ}}, {"elem", typ}},
			ReturnType:   tree.FixedReturnType(types.TArray{Typ: typ}),
			Category:     categoryArray,
			NullableArgs: true,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.AppendToMaybeNullArray(typ, args[0], args[1])
			},
			Info: "Appends `elem` to `array`, returning the result.",
		}
	}),

	"array_prepend": arrayBuiltin(func(typ types.T) tree.Builtin {
		return tree.Builtin{
			Types:        tree.ArgTypes{{"elem", typ}, {"array", types.TArray{Typ: typ}}},
			ReturnType:   tree.FixedReturnType(types.TArray{Typ: typ}),
			Category:     categoryArray,
			NullableArgs: true,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.PrependToMaybeNullArray(typ, args[0], args[1])
			},
			Info: "Prepends `elem` to `array`, returning the result.",
		}
	}),

	"array_cat": arrayBuiltin(func(typ types.T) tree.Builtin {
		return tree.Builtin{
			Types:        tree.ArgTypes{{"left", types.TArray{Typ: typ}}, {"right", types.TArray{Typ: typ}}},
			ReturnType:   tree.FixedReturnType(types.TArray{Typ: typ}),
			Category:     categoryArray,
			NullableArgs: true,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.ConcatArrays(typ, args[0], args[1])
			},
			Info: "Appends two arrays.",
		}
	}),

	"array_remove": arrayBuiltin(func(typ types.T) tree.Builtin {
		return tree.Builtin{
			Types:        tree.ArgTypes{{"array", types.TArray{Typ: typ}}, {"elem", typ}},
			ReturnType:   tree.FixedReturnType(types.TArray{Typ: typ}),
			Category:     categoryArray,
			NullableArgs: true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				result := tree.NewDArray(typ)
				for _, e := range tree.MustBeDArray(args[0]).Array {
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

	"array_replace": arrayBuiltin(func(typ types.T) tree.Builtin {
		return tree.Builtin{
			Types:        tree.ArgTypes{{"array", types.TArray{Typ: typ}}, {"toreplace", typ}, {"replacewith", typ}},
			ReturnType:   tree.FixedReturnType(types.TArray{Typ: typ}),
			Category:     categoryArray,
			NullableArgs: true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				result := tree.NewDArray(typ)
				for _, e := range tree.MustBeDArray(args[0]).Array {
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

	"array_position": arrayBuiltin(func(typ types.T) tree.Builtin {
		return tree.Builtin{
			Types:        tree.ArgTypes{{"array", types.TArray{Typ: typ}}, {"elem", typ}},
			ReturnType:   tree.FixedReturnType(types.Int),
			Category:     categoryArray,
			NullableArgs: true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				for i, e := range tree.MustBeDArray(args[0]).Array {
					if e.Compare(ctx, args[1]) == 0 {
						return tree.NewDInt(tree.DInt(i + 1)), nil
					}
				}
				return tree.DNull, nil
			},
			Info: "Return the index of the first occurrence of `elem` in `array`.",
		}
	}),

	"array_positions": arrayBuiltin(func(typ types.T) tree.Builtin {
		return tree.Builtin{
			Types:        tree.ArgTypes{{"array", types.TArray{Typ: typ}}, {"elem", typ}},
			ReturnType:   tree.FixedReturnType(types.TArray{Typ: types.Int}),
			Category:     categoryArray,
			NullableArgs: true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				result := tree.NewDArray(types.Int)
				for i, e := range tree.MustBeDArray(args[0]).Array {
					if e.Compare(ctx, args[1]) == 0 {
						if err := result.Append(tree.NewDInt(tree.DInt(i + 1))); err != nil {
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
		tree.Builtin{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Category:   categorySystemInfo,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(build.GetInfo().Short()), nil
			},
			Info: "Returns the node's version of CockroachDB.",
		},
	},

	"current_database": {
		tree.Builtin{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Category:   categorySystemInfo,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if len(ctx.SessionData.Database) == 0 {
					return tree.DNull, nil
				}
				return tree.NewDString(ctx.SessionData.Database), nil
			},
			Info: "Returns the current database.",
		},
	},

	"current_schema": {
		tree.Builtin{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Category:   categorySystemInfo,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				hasFirst, first := ctx.SessionData.SearchPath.FirstSpecified()
				if !hasFirst {
					return tree.DNull, nil
				}
				return tree.NewDString(first), nil
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
		tree.Builtin{
			Types:      tree.ArgTypes{{"include_pg_catalog", types.Bool}},
			ReturnType: tree.FixedReturnType(types.TArray{Typ: types.String}),
			Category:   categorySystemInfo,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				includePgCatalog := *(args[0].(*tree.DBool))
				schemas := tree.NewDArray(types.String)
				var iter func() (string, bool)
				if includePgCatalog {
					iter = ctx.SessionData.SearchPath.Iter()
				} else {
					iter = ctx.SessionData.SearchPath.IterWithoutImplicitPGCatalog()
				}
				for p, ok := iter(); ok; p, ok = iter() {
					if err := schemas.Append(tree.NewDString(p)); err != nil {
						return nil, err
					}
				}
				return schemas, nil
			},
			Info: "Returns the current search path for unqualified names.",
		},
	},

	"current_user": {
		tree.Builtin{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Category:   categorySystemInfo,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if len(ctx.SessionData.User) == 0 {
					return tree.DNull, nil
				}
				return tree.NewDString(ctx.SessionData.User), nil
			},
			Info: "Returns the current user. This function is provided for " +
				"compatibility with PostgreSQL.",
		},
	},

	"crdb_internal.node_executable_version": {
		tree.Builtin{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Category:   categorySystemInfo,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				v := "unknown"
				// TODO(tschottdorf): we should always have a Settings, but there
				// are many random places that create an ad-hoc EvalContext that
				// they only partially populate.
				if st := ctx.Settings; st != nil {
					v = st.Version.ServerVersion.String()
				}
				return tree.NewDString(v), nil
			},
			Info: "Returns the version of CockroachDB this node is running.",
		},
	},

	"crdb_internal.cluster_id": {
		tree.Builtin{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.UUID),
			Category:   categorySystemInfo,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDUuid(tree.DUuid{UUID: ctx.ClusterID}), nil
			},
			Info: "Returns the cluster ID.",
		},
	},

	"crdb_internal.force_error": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"errorCode", types.String}, {"msg", types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Impure:     true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				errCode := string(*args[0].(*tree.DString))
				msg := string(*args[1].(*tree.DString))
				if errCode == "" {
					return nil, errors.New(msg)
				}
				return nil, pgerror.NewError(errCode, msg)
			},
			Category: categorySystemInfo,
			Info:     "This function is used only by CockroachDB's developers for testing purposes.",
		},
	},

	"crdb_internal.force_panic": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"msg", types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Impure:     true,
			Privileged: true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				msg := string(*args[0].(*tree.DString))
				panic(msg)
			},
			Category: categorySystemInfo,
			Info:     "This function is used only by CockroachDB's developers for testing purposes.",
		},
	},

	"crdb_internal.force_log_fatal": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"msg", types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Impure:     true,
			Privileged: true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				msg := string(*args[0].(*tree.DString))
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
		tree.Builtin{
			Types:      tree.ArgTypes{{"val", types.Interval}},
			ReturnType: tree.FixedReturnType(types.Int),
			Impure:     true,
			Privileged: true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				minDuration := args[0].(*tree.DInterval).Duration
				elapsed := duration.Duration{
					Nanos: int64(ctx.StmtTimestamp.Sub(ctx.TxnTimestamp)),
				}
				if elapsed.Compare(minDuration) < 0 {
					return nil, ctx.Txn.GenerateForcedRetryableError("forced by crdb_internal.force_retry()")
				}
				return tree.DZero, nil
			},
			Category: categorySystemInfo,
			Info:     "This function is used only by CockroachDB's developers for testing purposes.",
		},
	},

	// Identity function which is marked as impure to avoid constant folding.
	"crdb_internal.no_constant_folding": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"input", types.Any}},
			ReturnType: tree.IdentityReturnType(0),
			Impure:     true,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return args[0], nil
			},
			Category: categorySystemInfo,
			Info:     "This function is used only by CockroachDB's developers for testing purposes.",
		},
	},

	"crdb_internal.set_vmodule": {
		tree.Builtin{
			Types:      tree.ArgTypes{{"vmodule_string", types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Impure:     true,
			Privileged: true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.DZero, log.SetVModule(string(*args[0].(*tree.DString)))
			},
			Category: categorySystemInfo,
			Info: "This function is used for internal debugging purposes. " +
				"Incorrect use can severely impact performance.",
		},
	},
}

var substringImpls = []tree.Builtin{
	{
		Types: tree.ArgTypes{
			{"input", types.String},
			{"substr_pos", types.Int},
		},
		ReturnType: tree.FixedReturnType(types.String),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			runes := []rune(string(tree.MustBeDString(args[0])))
			// SQL strings are 1-indexed.
			start := int(tree.MustBeDInt(args[1])) - 1

			if start < 0 {
				start = 0
			} else if start > len(runes) {
				start = len(runes)
			}

			return tree.NewDString(string(runes[start:])), nil
		},
		Info: "Returns a substring of `input` starting at `substr_pos` (count starts at 1).",
	},
	{
		Types: tree.ArgTypes{
			{"input", types.String},
			{"start_pos", types.Int},
			{"end_pos", types.Int},
		},
		ReturnType: tree.FixedReturnType(types.String),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			runes := []rune(string(tree.MustBeDString(args[0])))
			// SQL strings are 1-indexed.
			start := int(tree.MustBeDInt(args[1])) - 1
			length := int(tree.MustBeDInt(args[2]))

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

			return tree.NewDString(string(runes[start:end])), nil
		},
		Info: "Returns a substring of `input` between `start_pos` and `end_pos` (count starts at 1).",
	},
	{
		Types: tree.ArgTypes{
			{"input", types.String},
			{"regex", types.String},
		},
		ReturnType: tree.FixedReturnType(types.String),
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			s := string(tree.MustBeDString(args[0]))
			pattern := string(tree.MustBeDString(args[1]))
			return regexpExtract(ctx, s, pattern, `\`)
		},
		Info: "Returns a substring of `input` that matches the regular expression `regex`.",
	},
	{
		Types: tree.ArgTypes{
			{"input", types.String},
			{"regex", types.String},
			{"escape_char", types.String},
		},
		ReturnType: tree.FixedReturnType(types.String),
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			s := string(tree.MustBeDString(args[0]))
			pattern := string(tree.MustBeDString(args[1]))
			escape := string(tree.MustBeDString(args[2]))
			return regexpExtract(ctx, s, pattern, escape)
		},
		Info: "Returns a substring of `input` that matches the regular expression `regex` using " +
			"`escape_char` as your escape character instead of `\\`.",
	},
}

var uuidV4Impl = tree.Builtin{
	Types:      tree.ArgTypes{},
	ReturnType: tree.FixedReturnType(types.Bytes),
	Category:   categoryIDGeneration,
	Impure:     true,
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		return tree.NewDBytes(tree.DBytes(uuid.MakeV4().GetBytes())), nil
	},
	Info: "Returns a UUID.",
}

var ceilImpl = []tree.Builtin{
	floatBuiltin1(func(x float64) (tree.Datum, error) {
		return tree.NewDFloat(tree.DFloat(math.Ceil(x))), nil
	}, "Calculates the smallest integer greater than `val`."),
	decimalBuiltin1(func(x *apd.Decimal) (tree.Datum, error) {
		dd := &tree.DDecimal{}
		_, err := tree.ExactCtx.Ceil(&dd.Decimal, x)
		return dd, err
	}, "Calculates the smallest integer greater than `val`."),
}

var txnTSImpl = []tree.Builtin{
	{
		Types:             tree.ArgTypes{},
		ReturnType:        tree.FixedReturnType(types.TimestampTZ),
		PreferredOverload: true,
		Impure:            true,
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return ctx.GetTxnTimestamp(time.Microsecond), nil
		},
		Info: "Returns the current transaction's timestamp.",
	},
	{
		Types:      tree.ArgTypes{},
		ReturnType: tree.FixedReturnType(types.Timestamp),
		Impure:     true,
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return ctx.GetTxnTimestampNoZone(time.Microsecond), nil
		},
		Info: "Returns the current transaction's timestamp.",
	},
}

var powImpls = []tree.Builtin{
	floatBuiltin2("x", "y", func(x, y float64) (tree.Datum, error) {
		return tree.NewDFloat(tree.DFloat(math.Pow(x, y))), nil
	}, "Calculates `x`^`y`."),
	decimalBuiltin2("x", "y", func(x, y *apd.Decimal) (tree.Datum, error) {
		dd := &tree.DDecimal{}
		_, err := tree.DecimalCtx.Pow(&dd.Decimal, x, y)
		return dd, err
	}, "Calculates `x`^`y`."),
	{
		Types: tree.ArgTypes{
			{"x", types.Int},
			{"y", types.Int},
		},
		ReturnType: tree.FixedReturnType(types.Int),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return tree.IntPow(tree.MustBeDInt(args[0]), tree.MustBeDInt(args[1]))
		},
		Info: "Calculates `x`^`y`.",
	},
}

var (
	jsonNullDString    = tree.NewDString("null")
	jsonStringDString  = tree.NewDString("string")
	jsonNumberDString  = tree.NewDString("number")
	jsonBooleanDString = tree.NewDString("boolean")
	jsonArrayDString   = tree.NewDString("array")
	jsonObjectDString  = tree.NewDString("object")
)

var (
	errJSONObjectNotEvenNumberOfElements = pgerror.NewError(pgerror.CodeInvalidParameterValueError,
		"array must have even number of elements")
	errJSONObjectNullValueForKey = pgerror.NewError(pgerror.CodeInvalidParameterValueError,
		"null value not allowed for object key")
	errJSONObjectMismatchedArrayDim = pgerror.NewError(pgerror.CodeInvalidParameterValueError,
		"mismatched array dimensions")
)

var jsonExtractPathImpl = tree.Builtin{
	Types:      tree.VariadicType{FixedTypes: []types.T{types.JSON}, VarType: types.String},
	ReturnType: tree.FixedReturnType(types.JSON),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		j := tree.MustBeDJSON(args[0])
		path := make([]string, len(args)-1)
		for i, v := range args {
			if i == 0 {
				continue
			}
			path[i-1] = string(tree.MustBeDString(v))
		}
		result, err := json.FetchPath(j.JSON, path)
		if err != nil {
			return nil, err
		}
		if result == nil {
			return tree.DNull, nil
		}
		return &tree.DJSON{JSON: result}, nil
	},
	Info: "Returns the JSON value pointed to by the variadic arguments.",
}

func darrayToStringSlice(d tree.DArray) []string {
	result := make([]string, len(d.Array))
	for i, s := range d.Array {
		result[i] = string(tree.MustBeDString(s))
	}
	return result
}

var jsonSetImpl = tree.Builtin{
	Types: tree.ArgTypes{
		{"val", types.JSON},
		{"path", types.TArray{Typ: types.String}},
		{"to", types.JSON},
	},
	ReturnType: tree.FixedReturnType(types.JSON),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		path := darrayToStringSlice(*tree.MustBeDArray(args[1]))
		j, err := json.DeepSet(tree.MustBeDJSON(args[0]).JSON, path, tree.MustBeDJSON(args[2]).JSON, true)
		if err != nil {
			return nil, err
		}
		return &tree.DJSON{JSON: j}, nil
	},
	Info: "Returns the JSON value pointed to by the variadic arguments.",
}

var jsonSetWithCreateMissingImpl = tree.Builtin{
	Types: tree.ArgTypes{
		{"val", types.JSON},
		{"path", types.TArray{Typ: types.String}},
		{"to", types.JSON},
		{"create_missing", types.Bool},
	},
	ReturnType: tree.FixedReturnType(types.JSON),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		path := darrayToStringSlice(*tree.MustBeDArray(args[1]))
		j, err := json.DeepSet(tree.MustBeDJSON(args[0]).JSON, path, tree.MustBeDJSON(args[2]).JSON, bool(*(args[3].(*tree.DBool))))
		if err != nil {
			return nil, err
		}
		return &tree.DJSON{JSON: j}, nil
	},
	Info: "Returns the JSON value pointed to by the variadic arguments. " +
		"If `create_missing` is false, new keys will not be inserted to objects " +
		"and values will not be prepended or appended to arrays.",
}

var jsonTypeOfImpl = tree.Builtin{
	Types:      tree.ArgTypes{{"val", types.JSON}},
	ReturnType: tree.FixedReturnType(types.String),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		t := tree.MustBeDJSON(args[0]).JSON.Type()
		switch t {
		case json.NullJSONType:
			return jsonNullDString, nil
		case json.StringJSONType:
			return jsonStringDString, nil
		case json.NumberJSONType:
			return jsonNumberDString, nil
		case json.FalseJSONType, json.TrueJSONType:
			return jsonBooleanDString, nil
		case json.ArrayJSONType:
			return jsonArrayDString, nil
		case json.ObjectJSONType:
			return jsonObjectDString, nil
		}
		return nil, pgerror.NewErrorf(pgerror.CodeInternalError, "unexpected JSON type %d", t)
	},
	Info: "Returns the type of the outermost JSON value as a text string.",
}

var jsonBuildObjectImpl = tree.Builtin{
	Types:        tree.VariadicType{VarType: types.Any},
	ReturnType:   tree.FixedReturnType(types.JSON),
	NullableArgs: true,
	Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		if len(args)%2 != 0 {
			return nil, pgerror.NewError(pgerror.CodeInvalidParameterValueError,
				"argument list must have even number of elements")
		}

		builder := json.NewObjectBuilder(len(args) / 2)
		for i := 0; i < len(args); i += 2 {
			if args[i] == tree.DNull {
				return nil, pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError,
					"argument %d cannot be null", i+1)
			}

			key, err := asJSONBuildObjectKey(args[i])
			if err != nil {
				return nil, err
			}

			val, err := asJSON(args[i+1])
			if err != nil {
				return nil, err
			}

			builder.Add(key, val)
		}

		return tree.NewDJSON(builder.Build()), nil
	},
	Info: "Builds a JSON object out of a variadic argument list.",
}

var toJSONImpl = tree.Builtin{
	Types:      tree.ArgTypes{{"val", types.Any}},
	ReturnType: tree.FixedReturnType(types.JSON),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		j, err := asJSON(args[0])
		if err != nil {
			return nil, err
		}
		return tree.NewDJSON(j), nil
	},
	Info: "Returns the value as JSON or JSONB.",
}

var jsonBuildArrayImpl = tree.Builtin{
	Types:        tree.VariadicType{VarType: types.Any},
	ReturnType:   tree.FixedReturnType(types.JSON),
	NullableArgs: true,
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		builder := json.NewArrayBuilder(len(args))
		for _, arg := range args {
			j, err := asJSON(arg)
			if err != nil {
				return nil, err
			}
			builder.Add(j)
		}
		return tree.NewDJSON(builder.Build()), nil
	},
	Info: "Builds a possibly-heterogeneously-typed JSON or JSONB array out of a variadic argument list.",
}

var jsonObjectImpls = []tree.Builtin{
	{
		Types:      tree.ArgTypes{{"texts", types.TArray{Typ: types.String}}},
		ReturnType: tree.FixedReturnType(types.JSON),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			arr := tree.MustBeDArray(args[0])
			if arr.Len()%2 != 0 {
				return nil, errJSONObjectNotEvenNumberOfElements
			}
			builder := json.NewObjectBuilder(arr.Len() / 2)
			for i := 0; i < arr.Len(); i += 2 {
				if arr.Array[i] == tree.DNull {
					return nil, errJSONObjectNullValueForKey
				}
				key, err := asJSONObjectKey(arr.Array[i])
				if err != nil {
					return nil, err
				}
				val, err := asJSON(arr.Array[i+1])
				if err != nil {
					return nil, err
				}
				builder.Add(key, val)
			}
			return tree.NewDJSON(builder.Build()), nil
		},
		Info: "Builds a JSON or JSONB object out of a text array. The array must have " +
			"exactly one dimension with an even number of members, in which case " +
			"they are taken as alternating key/value pairs.",
	},
	{
		Types: tree.ArgTypes{{"keys", types.TArray{Typ: types.String}},
			{"values", types.TArray{Typ: types.String}}},
		ReturnType: tree.FixedReturnType(types.JSON),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			keys := tree.MustBeDArray(args[0])
			values := tree.MustBeDArray(args[1])
			if keys.Len() != values.Len() {
				return nil, errJSONObjectMismatchedArrayDim
			}
			builder := json.NewObjectBuilder(keys.Len())
			for i := 0; i < keys.Len(); i++ {
				if keys.Array[i] == tree.DNull {
					return nil, errJSONObjectNullValueForKey
				}
				key, err := asJSONObjectKey(keys.Array[i])
				if err != nil {
					return nil, err
				}
				val, err := asJSON(values.Array[i])
				if err != nil {
					return nil, err
				}
				builder.Add(key, val)
			}
			return tree.NewDJSON(builder.Build()), nil
		},
		Info: "This form of json_object takes keys and values pairwise from two " +
			"separate arrays. In all other respects it is identical to the " +
			"one-argument form.",
	},
}

var jsonStripNullsImpl = tree.Builtin{
	Types:      tree.ArgTypes{{"from_json", types.JSON}},
	ReturnType: tree.FixedReturnType(types.JSON),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		j, _, err := tree.MustBeDJSON(args[0]).StripNulls()
		return tree.NewDJSON(j), err
	},
	Info: "Returns from_json with all object fields that have null values omitted. Other null values are untouched.",
}

var jsonArrayLengthImpl = tree.Builtin{
	Types:      tree.ArgTypes{{"json", types.JSON}},
	ReturnType: tree.FixedReturnType(types.Int),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		j := tree.MustBeDJSON(args[0])
		switch j.Type() {
		case json.ArrayJSONType:
			return tree.NewDInt(tree.DInt(j.Len())), nil
		case json.ObjectJSONType:
			return nil, pgerror.NewError(pgerror.CodeInvalidParameterValueError,
				"cannot get array length of a non-array")
		default:
			return nil, pgerror.NewError(pgerror.CodeInvalidParameterValueError,
				"cannot get array length of a scalar")
		}
	},
	Info: "Returns the number of elements in the outermost JSON or JSONB array.",
}

func arrayBuiltin(impl func(types.T) tree.Builtin) []tree.Builtin {
	result := make([]tree.Builtin, 0, len(types.AnyNonArray))
	for _, typ := range types.AnyNonArray {
		if types.IsValidArrayElementType(typ) {
			result = append(result, impl(typ))
		}
	}
	return result
}

func decimalLogFn(
	logFn func(*apd.Decimal, *apd.Decimal) (apd.Condition, error), info string,
) tree.Builtin {
	return decimalBuiltin1(func(x *apd.Decimal) (tree.Datum, error) {
		// TODO(mjibson): see #13642
		switch x.Sign() {
		case -1:
			return nil, errLogOfNegNumber
		case 0:
			return nil, errLogOfZero
		}
		dd := &tree.DDecimal{}
		_, err := logFn(&dd.Decimal, x)
		return dd, err
	}, info)
}

func floatBuiltin1(f func(float64) (tree.Datum, error), info string) tree.Builtin {
	return tree.Builtin{
		Types:      tree.ArgTypes{{"val", types.Float}},
		ReturnType: tree.FixedReturnType(types.Float),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(float64(*args[0].(*tree.DFloat)))
		},
		Info: info,
	}
}

func floatBuiltin2(
	a, b string, f func(float64, float64) (tree.Datum, error), info string,
) tree.Builtin {
	return tree.Builtin{
		Types:      tree.ArgTypes{{a, types.Float}, {b, types.Float}},
		ReturnType: tree.FixedReturnType(types.Float),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(float64(*args[0].(*tree.DFloat)),
				float64(*args[1].(*tree.DFloat)))
		},
		Info: info,
	}
}

func decimalBuiltin1(f func(*apd.Decimal) (tree.Datum, error), info string) tree.Builtin {
	return tree.Builtin{
		Types:      tree.ArgTypes{{"val", types.Decimal}},
		ReturnType: tree.FixedReturnType(types.Decimal),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			dec := &args[0].(*tree.DDecimal).Decimal
			return f(dec)
		},
		Info: info,
	}
}

func decimalBuiltin2(
	a, b string, f func(*apd.Decimal, *apd.Decimal) (tree.Datum, error), info string,
) tree.Builtin {
	return tree.Builtin{
		Types:      tree.ArgTypes{{a, types.Decimal}, {b, types.Decimal}},
		ReturnType: tree.FixedReturnType(types.Decimal),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			dec1 := &args[0].(*tree.DDecimal).Decimal
			dec2 := &args[1].(*tree.DDecimal).Decimal
			return f(dec1, dec2)
		},
		Info: info,
	}
}

func stringBuiltin1(
	f func(*tree.EvalContext, string) (tree.Datum, error), returnType types.T, info string,
) tree.Builtin {
	return tree.Builtin{
		Types:      tree.ArgTypes{{"val", types.String}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(evalCtx, string(tree.MustBeDString(args[0])))
		},
		Info: info,
	}
}

func stringBuiltin2(
	a, b string,
	f func(*tree.EvalContext, string, string) (tree.Datum, error),
	returnType types.T,
	info string,
) tree.Builtin {
	return tree.Builtin{
		Types:      tree.ArgTypes{{a, types.String}, {b, types.String}},
		ReturnType: tree.FixedReturnType(returnType),
		Category:   categorizeType(types.String),
		Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(evalCtx, string(tree.MustBeDString(args[0])), string(tree.MustBeDString(args[1])))
		},
		Info: info,
	}
}

func stringBuiltin3(
	a, b, c string,
	f func(*tree.EvalContext, string, string, string) (tree.Datum, error),
	returnType types.T,
	info string,
) tree.Builtin {
	return tree.Builtin{
		Types:      tree.ArgTypes{{a, types.String}, {b, types.String}, {c, types.String}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(evalCtx, string(tree.MustBeDString(args[0])), string(tree.MustBeDString(args[1])), string(tree.MustBeDString(args[2])))
		},
		Info: info,
	}
}

func bytesBuiltin1(
	f func(*tree.EvalContext, string) (tree.Datum, error), returnType types.T, info string,
) tree.Builtin {
	return tree.Builtin{
		Types:      tree.ArgTypes{{"val", types.Bytes}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(evalCtx, string(*args[0].(*tree.DBytes)))
		},
		Info: info,
	}
}

func feedHash(h hash.Hash, args tree.Datums) {
	for _, datum := range args {
		if datum == tree.DNull {
			continue
		}
		var buf string
		if d, ok := datum.(*tree.DBytes); ok {
			buf = string(*d)
		} else {
			buf = string(tree.MustBeDString(datum))
		}
		if _, err := h.Write([]byte(buf)); err != nil {
			panic(errors.Wrap(err, `"It never returns an error." -- https://golang.org/pkg/hash`))
		}
	}
}

func hashBuiltin(newHash func() hash.Hash, info string) []tree.Builtin {
	return []tree.Builtin{
		{
			Types:        tree.VariadicType{VarType: types.String},
			ReturnType:   tree.FixedReturnType(types.String),
			NullableArgs: true,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				h := newHash()
				feedHash(h, args)
				return tree.NewDString(fmt.Sprintf("%x", h.Sum(nil))), nil
			},
			Info: info,
		},
		{
			Types:        tree.VariadicType{VarType: types.Bytes},
			ReturnType:   tree.FixedReturnType(types.String),
			NullableArgs: true,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				h := newHash()
				feedHash(h, args)
				return tree.NewDString(fmt.Sprintf("%x", h.Sum(nil))), nil
			},
			Info: info,
		},
	}
}

func hash32Builtin(newHash func() hash.Hash32, info string) []tree.Builtin {
	return []tree.Builtin{
		{
			Types:        tree.VariadicType{VarType: types.String},
			ReturnType:   tree.FixedReturnType(types.Int),
			NullableArgs: true,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				h := newHash()
				feedHash(h, args)
				return tree.NewDInt(tree.DInt(h.Sum32())), nil
			},
			Info: info,
		},
		{
			Types:        tree.VariadicType{VarType: types.Bytes},
			ReturnType:   tree.FixedReturnType(types.Int),
			NullableArgs: true,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				h := newHash()
				feedHash(h, args)
				return tree.NewDInt(tree.DInt(h.Sum32())), nil
			},
			Info: info,
		},
	}
}
func hash64Builtin(newHash func() hash.Hash64, info string) []tree.Builtin {
	return []tree.Builtin{
		{
			Types:        tree.VariadicType{VarType: types.String},
			ReturnType:   tree.FixedReturnType(types.Int),
			NullableArgs: true,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				h := newHash()
				feedHash(h, args)
				return tree.NewDInt(tree.DInt(h.Sum64())), nil
			},
			Info: info,
		},
		{
			Types:        tree.VariadicType{VarType: types.Bytes},
			ReturnType:   tree.FixedReturnType(types.Int),
			NullableArgs: true,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				h := newHash()
				feedHash(h, args)
				return tree.NewDInt(tree.DInt(h.Sum64())), nil
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

func regexpExtract(ctx *tree.EvalContext, s, pattern, escape string) (tree.Datum, error) {
	patternRe, err := ctx.ReCache.GetRegexp(regexpEscapeKey{pattern, escape})
	if err != nil {
		return nil, err
	}

	match := patternRe.FindStringSubmatch(s)
	if match == nil {
		return tree.DNull, nil
	}

	if len(match) > 1 {
		return tree.NewDString(match[1]), nil
	}
	return tree.NewDString(match[0]), nil
}

type regexpFlagKey struct {
	sqlPattern string
	sqlFlags   string
}

// Pattern implements the RegexpCacheKey interface.
func (k regexpFlagKey) Pattern() (string, error) {
	return regexpEvalFlags(k.sqlPattern, k.sqlFlags)
}

func regexpReplace(ctx *tree.EvalContext, s, pattern, to, sqlFlags string) (tree.Datum, error) {
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

	return tree.NewDString(newString.String()), nil
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

func overlay(s, to string, pos, size int) (tree.Datum, error) {
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
	return tree.NewDString(string(runes[:pos]) + to + string(runes[after:])), nil
}

func roundDecimal(x *apd.Decimal, n int32) (tree.Datum, error) {
	dd := &tree.DDecimal{}
	_, err := tree.HighPrecisionCtx.Quantize(&dd.Decimal, x, -n)
	return dd, err
}

var uniqueIntState struct {
	syncutil.Mutex
	timestamp uint64
}

var uniqueIntEpoch = time.Date(2015, time.January, 1, 0, 0, 0, 0, time.UTC).UnixNano()

// NodeIDBits is the number of bits stored in the lower portion of
// GenerateUniqueInt.
const NodeIDBits = 15

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
func GenerateUniqueInt(nodeID roachpb.NodeID) tree.DInt {
	const precision = uint64(10 * time.Microsecond)

	nowNanos := timeutil.Now().UnixNano()
	// Paranoia: nowNanos should never be less than uniqueIntEpoch.
	if nowNanos < uniqueIntEpoch {
		nowNanos = uniqueIntEpoch
	}
	timestamp := uint64(nowNanos-uniqueIntEpoch) / precision

	uniqueIntState.Lock()
	if timestamp <= uniqueIntState.timestamp {
		timestamp = uniqueIntState.timestamp + 1
	}
	uniqueIntState.timestamp = timestamp
	uniqueIntState.Unlock()

	return GenerateUniqueID(int32(nodeID), timestamp)
}

// GenerateUniqueID encapsulates the logic to generate a unique number from
// a nodeID and timestamp.
func GenerateUniqueID(nodeID int32, timestamp uint64) tree.DInt {
	// We xor in the nodeID so that nodeIDs larger than 32K will flip bits in the
	// timestamp portion of the final value instead of always setting them.
	id := (timestamp << NodeIDBits) ^ uint64(nodeID)
	return tree.DInt(id)
}

func arrayLength(arr *tree.DArray, dim int64) tree.Datum {
	if arr.Len() == 0 || dim < 1 {
		return tree.DNull
	}
	if dim == 1 {
		return tree.NewDInt(tree.DInt(arr.Len()))
	}
	a, ok := tree.AsDArray(arr.Array[0])
	if !ok {
		return tree.DNull
	}
	return arrayLength(a, dim-1)
}

var intOne = tree.NewDInt(tree.DInt(1))

func arrayLower(arr *tree.DArray, dim int64) tree.Datum {
	if arr.Len() == 0 || dim < 1 {
		return tree.DNull
	}
	if dim == 1 {
		return intOne
	}
	a, ok := tree.AsDArray(arr.Array[0])
	if !ok {
		return tree.DNull
	}
	return arrayLower(a, dim-1)
}

const microsPerMilli = 1000

func extractStringFromTime(fromTime *tree.DTime, timeSpan string) (tree.Datum, error) {
	t := timeofday.TimeOfDay(*fromTime)
	switch timeSpan {
	case "hour", "hours":
		return tree.NewDInt(tree.DInt(t.Hour())), nil
	case "minute", "minutes":
		return tree.NewDInt(tree.DInt(t.Minute())), nil
	case "second", "seconds":
		return tree.NewDInt(tree.DInt(t.Second())), nil
	case "millisecond", "milliseconds":
		return tree.NewDInt(tree.DInt(t.Microsecond() / microsPerMilli)), nil
	case "microsecond", "microseconds":
		return tree.NewDInt(tree.DInt(t.Microsecond())), nil
	case "epoch":
		seconds := time.Duration(t) * time.Microsecond / time.Second
		return tree.NewDInt(tree.DInt(int64(seconds))), nil
	default:
		return nil, pgerror.NewErrorf(
			pgerror.CodeInvalidParameterValueError, "unsupported timespan: %s", timeSpan)
	}
}

func extractStringFromTimestamp(
	_ *tree.EvalContext, fromTime time.Time, timeSpan string,
) (tree.Datum, error) {
	switch timeSpan {
	case "year", "years":
		return tree.NewDInt(tree.DInt(fromTime.Year())), nil

	case "quarter":
		return tree.NewDInt(tree.DInt((fromTime.Month()-1)/3 + 1)), nil

	case "month", "months":
		return tree.NewDInt(tree.DInt(fromTime.Month())), nil

	case "week", "weeks":
		_, week := fromTime.ISOWeek()
		return tree.NewDInt(tree.DInt(week)), nil

	case "day", "days":
		return tree.NewDInt(tree.DInt(fromTime.Day())), nil

	case "dayofweek", "dow":
		return tree.NewDInt(tree.DInt(fromTime.Weekday())), nil

	case "dayofyear", "doy":
		return tree.NewDInt(tree.DInt(fromTime.YearDay())), nil

	case "hour", "hours":
		return tree.NewDInt(tree.DInt(fromTime.Hour())), nil

	case "minute", "minutes":
		return tree.NewDInt(tree.DInt(fromTime.Minute())), nil

	case "second", "seconds":
		return tree.NewDInt(tree.DInt(fromTime.Second())), nil

	case "millisecond", "milliseconds":
		// This a PG extension not supported in MySQL.
		return tree.NewDInt(tree.DInt(fromTime.Nanosecond() / int(time.Millisecond))), nil

	case "microsecond", "microseconds":
		return tree.NewDInt(tree.DInt(fromTime.Nanosecond() / int(time.Microsecond))), nil

	case "epoch":
		return tree.NewDInt(tree.DInt(fromTime.Unix())), nil

	default:
		return nil, pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError, "unsupported timespan: %s", timeSpan)
	}
}

func truncateTime(fromTime *tree.DTime, timeSpan string) (*tree.DTime, error) {
	t := timeofday.TimeOfDay(*fromTime)
	hour := t.Hour()
	min := t.Minute()
	sec := t.Second()
	micro := t.Microsecond()

	minTrunc := 0
	secTrunc := 0
	microTrunc := 0

	switch timeSpan {
	case "hour", "hours":
		min, sec, micro = minTrunc, secTrunc, microTrunc
	case "minute", "minutes":
		sec, micro = secTrunc, microTrunc
	case "second", "seconds":
		micro = microTrunc
	case "millisecond", "milliseconds":
		// This a PG extension not supported in MySQL.
		micro = (micro / microsPerMilli) * microsPerMilli
	case "microsecond", "microseconds":
	default:
		return nil, pgerror.NewErrorf(pgerror.CodeInvalidParameterValueError, "unsupported timespan: %s", timeSpan)
	}

	return tree.MakeDTime(timeofday.New(hour, min, sec, micro)), nil
}

func stringOrNil(d tree.Datum) *string {
	if d == tree.DNull {
		return nil
	}
	s := string(tree.MustBeDString(d))
	return &s
}

// stringToArray implements the string_to_array builtin - str is split on delim to form an array of strings.
// If nullStr is set, any elements equal to it will be NULL.
func stringToArray(str string, delimPtr *string, nullStr *string) (tree.Datum, error) {
	var split []string

	if delimPtr != nil {
		delim := *delimPtr
		if str == "" {
			split = nil
		} else if delim == "" {
			split = []string{str}
		} else {
			split = strings.Split(str, delim)
		}
	} else {
		// When given a NULL delimiter, string_to_array splits into each character.
		split = make([]string, len(str))
		for i, c := range str {
			split[i] = string(c)
		}
	}

	result := tree.NewDArray(types.String)
	for _, s := range split {
		var next tree.Datum = tree.NewDString(s)
		if nullStr != nil && s == *nullStr {
			next = tree.DNull
		}
		if err := result.Append(next); err != nil {
			return nil, err
		}
	}
	return result, nil
}

// encodeEscape implements the encode(..., 'escape') Postgres builtin. It's
// described "escape converts zero bytes and high-bit-set bytes to octal
// sequences (\nnn) and doubles backslashes."
func encodeEscape(input []byte) string {
	var result bytes.Buffer
	start := 0
	for i := range input {
		if input[i] == 0 || input[i]&128 != 0 {
			result.Write(input[start:i])
			start = i + 1
			result.WriteString(fmt.Sprintf(`\%03o`, input[i]))
		} else if input[i] == '\\' {
			result.Write(input[start:i])
			start = i + 1
			result.WriteString(`\\`)
		}
	}
	result.Write(input[start:])
	return result.String()
}

var errInvalidSyntaxForDecode = pgerror.NewError(pgerror.CodeInvalidParameterValueError, "invalid syntax for decode(..., 'escape')")

func isOctalDigit(c byte) bool {
	return '0' <= c && c <= '7'
}

func decodeOctalTriplet(input string) byte {
	return (input[0]-'0')*64 + (input[1]-'0')*8 + (input[2] - '0')
}

// decodeEscape implements the decode(..., 'escape') Postgres builtin. The
// escape format is described as "escape converts zero bytes and high-bit-set
// bytes to octal sequences (\nnn) and doubles backslashes."
func decodeEscape(input string) ([]byte, error) {
	result := make([]byte, 0, len(input))
	for i := 0; i < len(input); i++ {
		if input[i] == '\\' {
			if i+1 < len(input) && input[i+1] == '\\' {
				result = append(result, '\\')
				i++
			} else if i+3 < len(input) &&
				isOctalDigit(input[i+1]) &&
				isOctalDigit(input[i+2]) &&
				isOctalDigit(input[i+3]) {
				result = append(result, decodeOctalTriplet(input[i+1:i+4]))
				i += 3
			} else {
				return nil, errInvalidSyntaxForDecode
			}
		} else {
			result = append(result, input[i])
		}
	}
	return result, nil
}

func truncateTimestamp(
	_ *tree.EvalContext, fromTime time.Time, timeSpan string,
) (*tree.DTimestampTZ, error) {
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
	return tree.MakeDTimestampTZ(toTime, time.Microsecond), nil
}

func asJSON(d tree.Datum) (json.JSON, error) {
	switch t := d.(type) {
	case *tree.DBool:
		return json.FromBool(bool(*t)), nil
	case *tree.DInt:
		return json.FromInt(int(*t)), nil
	case *tree.DFloat:
		return json.FromFloat64(float64(*t))
	case *tree.DDecimal:
		return json.FromDecimal(t.Decimal), nil
	case *tree.DString:
		return json.FromString(string(*t)), nil
	case *tree.DCollatedString:
		return json.FromString(t.Contents), nil
	case *tree.DJSON:
		return t.JSON, nil
	case *tree.DArray:
		builder := json.NewArrayBuilder(t.Len())
		for _, e := range t.Array {
			j, err := asJSON(e)
			if err != nil {
				return nil, err
			}
			builder.Add(j)
		}
		return builder.Build(), nil
	case *tree.DTuple:
		builder := json.NewObjectBuilder(len(t.D))
		for i, e := range t.D {
			j, err := asJSON(e)
			if err != nil {
				return nil, err
			}
			builder.Add(fmt.Sprintf("f%d", i+1), j)
		}
		return builder.Build(), nil
	case *tree.DTimestamp, *tree.DTimestampTZ, *tree.DDate, *tree.DUuid, *tree.DOid, *tree.DInterval, *tree.DBytes, *tree.DIPAddr, *tree.DTime:
		return json.FromString(tree.AsStringWithFlags(t, tree.FmtBareStrings)), nil
	default:
		if d == tree.DNull {
			return json.NullJSONValue, nil
		}

		return nil, pgerror.NewErrorf(pgerror.CodeInternalError, "unexpected type %T for asJSON", d)
	}
}

// Converts a scalar Datum to its string representation
func asJSONBuildObjectKey(d tree.Datum) (string, error) {
	switch t := d.(type) {
	case *tree.DJSON, *tree.DArray, *tree.DTuple:
		return "", pgerror.NewError(pgerror.CodeInvalidParameterValueError,
			"key value must be scalar, not array, tuple, or json")
	case *tree.DString:
		return string(*t), nil
	case *tree.DCollatedString:
		return t.Contents, nil
	case *tree.DBool, *tree.DInt, *tree.DFloat, *tree.DDecimal, *tree.DTimestamp, *tree.DTimestampTZ, *tree.DDate, *tree.DUuid, *tree.DInterval, *tree.DBytes, *tree.DIPAddr, *tree.DOid, *tree.DTime:
		return tree.AsStringWithFlags(d, tree.FmtBareStrings), nil
	default:
		return "", pgerror.NewErrorf(pgerror.CodeInternalError, "unexpected type %T for key value", d)
	}
}

func asJSONObjectKey(d tree.Datum) (string, error) {
	switch t := d.(type) {
	case *tree.DString:
		return string(*t), nil
	default:
		return "", pgerror.NewErrorf(pgerror.CodeInternalError, "unexpected type %T for asJSONObjectKey", d)
	}
}
