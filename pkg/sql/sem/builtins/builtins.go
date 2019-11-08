// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package builtins

import (
	"bytes"
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"crypto/sha512"
	gojson "encoding/json"
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

	"github.com/cockroachdb/apd"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/knz/strtime"
)

var (
	errEmptyInputString = pgerror.New(pgcode.InvalidParameterValue, "the input string must not be empty")
	errAbsOfMinInt64    = pgerror.New(pgcode.NumericValueOutOfRange, "abs of min integer value (-9223372036854775808) not defined")
	errSqrtOfNegNumber  = pgerror.New(pgcode.InvalidArgumentForPowerFunction, "cannot take square root of a negative number")
	errLogOfNegNumber   = pgerror.New(pgcode.InvalidArgumentForLogarithm, "cannot take logarithm of a negative number")
	errLogOfZero        = pgerror.New(pgcode.InvalidArgumentForLogarithm, "cannot take logarithm of zero")
	errZeroIP           = pgerror.New(pgcode.InvalidParameterValue, "zero length IP")
	errChrValueTooSmall = pgerror.New(pgcode.InvalidParameterValue, "input value must be >= 0")
	errChrValueTooLarge = pgerror.Newf(pgcode.InvalidParameterValue,
		"input value must be <= %d (maximum Unicode code point)", utf8.MaxRune)
	errStringTooLarge = pgerror.Newf(pgcode.ProgramLimitExceeded,
		fmt.Sprintf("requested length too large, exceeds %s", humanizeutil.IBytes(maxAllocatedStringSize)))
)

const maxAllocatedStringSize = 128 * 1024 * 1024

const errInsufficientArgsFmtString = "unknown signature: %s()"

const (
	categoryComparison    = "Comparison"
	categoryCompatibility = "Compatibility"
	categoryDateAndTime   = "Date and time"
	categoryIDGeneration  = "ID generation"
	categorySequences     = "Sequence"
	categoryMath          = "Math and numeric"
	categoryString        = "String and byte"
	categoryArray         = "Array"
	categorySystemInfo    = "System info"
	categoryGenerator     = "Set-returning"
	categoryJSON          = "JSONB"
)

func categorizeType(t *types.T) string {
	switch t.Family() {
	case types.DateFamily, types.IntervalFamily, types.TimestampFamily, types.TimestampTZFamily:
		return categoryDateAndTime
	case types.IntFamily, types.DecimalFamily, types.FloatFamily:
		return categoryMath
	case types.StringFamily, types.BytesFamily:
		return categoryString
	default:
		return strings.ToUpper(t.String())
	}
}

var digitNames = []string{"zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"}

// builtinDefinition represents a built-in function before it becomes
// a tree.FunctionDefinition.
type builtinDefinition struct {
	props     tree.FunctionProperties
	overloads []tree.Overload
}

// GetBuiltinProperties provides low-level access to a built-in function's properties.
// For a better, semantic-rich interface consider using tree.FunctionDefinition
// instead, and resolve function names via ResolvableFunctionReference.Resolve().
func GetBuiltinProperties(name string) (*tree.FunctionProperties, []tree.Overload) {
	def, ok := builtins[name]
	if !ok {
		return nil, nil
	}
	return &def.props, def.overloads
}

// defProps is used below to define built-in functions with default properties.
func defProps() tree.FunctionProperties { return tree.FunctionProperties{} }

// arrayProps is used below for array functions.
func arrayProps() tree.FunctionProperties { return tree.FunctionProperties{Category: categoryArray} }

// arrayPropsNullableArgs is used below for array functions that accept NULLs as arguments.
func arrayPropsNullableArgs() tree.FunctionProperties {
	p := arrayProps()
	p.NullableArgs = true
	return p
}

func makeBuiltin(props tree.FunctionProperties, overloads ...tree.Overload) builtinDefinition {
	return builtinDefinition{
		props:     props,
		overloads: overloads,
	}
}

func newDecodeError(enc string) error {
	return pgerror.Newf(pgcode.CharacterNotInRepertoire,
		"invalid byte sequence for encoding %q", enc)
}

func newEncodeError(c rune, enc string) error {
	return pgerror.Newf(pgcode.UntranslatableCharacter,
		"character %q has no representation in encoding %q", c, enc)
}

// builtins contains the built-in functions indexed by name.
//
// For use in other packages, see AllBuiltinNames and GetBuiltinProperties().
var builtins = map[string]builtinDefinition{
	// TODO(XisiHuang): support encoding, i.e., length(str, encoding).
	"length":           lengthImpls,
	"char_length":      lengthImpls,
	"character_length": lengthImpls,

	"bit_length": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		stringOverload1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDInt(tree.DInt(len(s) * 8)), nil
		}, types.Int, "Calculates the number of bits used to represent `val`."),
		bytesOverload1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDInt(tree.DInt(len(s) * 8)), nil
		}, types.Int, "Calculates the number of bits in `val`."),
	),

	"octet_length": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		stringOverload1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDInt(tree.DInt(len(s))), nil
		}, types.Int, "Calculates the number of bytes used to represent `val`."),
		bytesOverload1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDInt(tree.DInt(len(s))), nil
		}, types.Int, "Calculates the number of bytes in `val`."),
	),

	// TODO(pmattis): What string functions should also support types.Bytes?

	"lower": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		stringOverload1(func(evalCtx *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDString(strings.ToLower(s)), nil
		}, types.String, "Converts all characters in `val` to their lower-case equivalents.")),

	"upper": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		stringOverload1(func(evalCtx *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDString(strings.ToUpper(s)), nil
		}, types.String, "Converts all characters in `val` to their to their upper-case equivalents.")),

	"substr":    substringImpls,
	"substring": substringImpls,

	// concat concatenates the text representations of all the arguments.
	// NULL arguments are ignored.
	"concat": makeBuiltin(tree.FunctionProperties{NullableArgs: true},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.String},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				var buffer bytes.Buffer
				length := 0
				for _, d := range args {
					if d == tree.DNull {
						continue
					}
					length += len(string(tree.MustBeDString(d)))
					if length > maxAllocatedStringSize {
						return nil, errStringTooLarge
					}
					buffer.WriteString(string(tree.MustBeDString(d)))
				}
				return tree.NewDString(buffer.String()), nil
			},
			Info: "Concatenates a comma-separated list of strings.",
		},
	),

	"concat_ws": makeBuiltin(tree.FunctionProperties{NullableArgs: true},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.String},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if len(args) == 0 {
					return nil, pgerror.Newf(pgcode.UndefinedFunction, errInsufficientArgsFmtString, "concat_ws")
				}
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				sep := string(tree.MustBeDString(args[0]))
				var buf bytes.Buffer
				prefix := ""
				length := 0
				for _, d := range args[1:] {
					if d == tree.DNull {
						continue
					}
					length += len(prefix) + len(string(tree.MustBeDString(d)))
					if length > maxAllocatedStringSize {
						return nil, errStringTooLarge
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
	),

	// https://www.postgresql.org/docs/10/static/functions-string.html#FUNCTIONS-STRING-OTHER
	"convert_from": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{"str", types.Bytes}, {"enc", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				str := []byte(tree.MustBeDBytes(args[0]))
				enc := CleanEncodingName(string(tree.MustBeDString(args[1])))
				switch enc {
				// All the following are aliases to each other in PostgreSQL.
				case "utf8", "unicode", "cp65001":
					if !utf8.Valid(str) {
						return nil, newDecodeError("UTF8")
					}
					return tree.NewDString(string(str)), nil

					// All the following are aliases to each other in PostgreSQL.
				case "latin1", "iso88591", "cp28591":
					var buf strings.Builder
					for _, c := range str {
						buf.WriteRune(rune(c))
					}
					return tree.NewDString(buf.String()), nil
				}
				return nil, pgerror.Newf(pgcode.InvalidParameterValue,
					"invalid source encoding name %q", enc)
			},
			Info: "Decode the bytes in `str` into a string using encoding `enc`. " +
				"Supports encodings 'UTF8' and 'LATIN1'.",
		}),

	// https://www.postgresql.org/docs/10/static/functions-string.html#FUNCTIONS-STRING-OTHER
	"convert_to": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{"str", types.String}, {"enc", types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				str := string(tree.MustBeDString(args[0]))
				enc := CleanEncodingName(string(tree.MustBeDString(args[1])))
				switch enc {
				// All the following are aliases to each other in PostgreSQL.
				case "utf8", "unicode", "cp65001":
					return tree.NewDBytes(tree.DBytes([]byte(str))), nil

					// All the following are aliases to each other in PostgreSQL.
				case "latin1", "iso88591", "cp28591":
					res := make([]byte, 0, len(str))
					for _, c := range str {
						if c > 255 {
							return nil, newEncodeError(c, "LATIN1")
						}
						res = append(res, byte(c))
					}
					return tree.NewDBytes(tree.DBytes(res)), nil
				}
				return nil, pgerror.Newf(pgcode.InvalidParameterValue,
					"invalid destination encoding name %q", enc)
			},
			Info: "Encode the string `str` as a byte array using encoding `enc`. " +
				"Supports encodings 'UTF8' and 'LATIN1'.",
		}),

	"gen_random_uuid": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryIDGeneration,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Uuid),
			Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				uv := uuid.MakeV4()
				return tree.NewDUuid(tree.DUuid{UUID: uv}), nil
			},
			Info: "Generates a random UUID and returns it as a value of UUID type.",
		},
	),

	"to_uuid": makeBuiltin(defProps(),
		tree.Overload{
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
	),

	"from_uuid": makeBuiltin(defProps(),
		tree.Overload{
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
	),

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

	"abbrev": makeBuiltin(defProps(),
		tree.Overload{
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
	),

	"broadcast": makeBuiltin(defProps(),
		tree.Overload{
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
	),

	"family": makeBuiltin(defProps(),
		tree.Overload{
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
	),

	"host": makeBuiltin(defProps(),
		tree.Overload{
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
	),

	"hostmask": makeBuiltin(defProps(),
		tree.Overload{
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
	),

	"masklen": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.INet}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				return tree.NewDInt(tree.DInt(dIPAddr.Mask)), nil
			},
			Info: "Retrieves the prefix length stored in the value." +
				"\n\nFor example, `masklen('192.168.1.2/16')` returns `16`",
		},
	),

	"netmask": makeBuiltin(defProps(),
		tree.Overload{
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
	),

	"set_masklen": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"val", types.INet},
				{"prefixlen", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.INet),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				mask := int(tree.MustBeDInt(args[1]))

				if !(dIPAddr.Family == ipaddr.IPv4family && mask >= 0 && mask <= 32) && !(dIPAddr.Family == ipaddr.IPv6family && mask >= 0 && mask <= 128) {
					return nil, pgerror.Newf(
						pgcode.InvalidParameterValue, "invalid mask length: %d", mask)
				}
				return &tree.DIPAddr{IPAddr: ipaddr.IPAddr{Family: dIPAddr.Family, Addr: dIPAddr.Addr, Mask: byte(mask)}}, nil
			},
			Info: "Sets the prefix length of `val` to `prefixlen`.\n\n" +
				"For example, `set_masklen('192.168.1.2', 16)` returns `'192.168.1.2/16'`.",
		},
	),

	"text": makeBuiltin(defProps(),
		tree.Overload{
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
	),

	"inet_same_family": makeBuiltin(defProps(),
		tree.Overload{
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
	),

	"inet_contained_by_or_equals": makeBuiltin(defProps(),
		tree.Overload{
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
	),

	"inet_contains_or_equals": makeBuiltin(defProps(),
		tree.Overload{
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
	),

	"from_ip": makeBuiltin(defProps(),
		tree.Overload{
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
	),

	"to_ip": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ipdstr := tree.MustBeDString(args[0])
				ip := net.ParseIP(string(ipdstr))
				// If ipdstr could not be parsed to a valid IP,
				// ip will be nil.
				if ip == nil {
					return nil, pgerror.Newf(
						pgcode.InvalidParameterValue, "invalid IP format: %s", ipdstr.String())
				}
				return tree.NewDBytes(tree.DBytes(ip)), nil
			},
			Info: "Converts the character string representation of an IP to its byte string " +
				"representation.",
		},
	),

	"split_part": makeBuiltin(defProps(),
		tree.Overload{
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
					return nil, pgerror.Newf(
						pgcode.InvalidParameterValue, "field position %d must be greater than zero", field)
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
	),

	"repeat": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"input", types.String}, {"repeat_counter", types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
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
					return nil, errStringTooLarge
				} else if ln > maxAllocatedStringSize {
					return nil, errStringTooLarge
				}

				return tree.NewDString(strings.Repeat(s, count)), nil
			},
			Info: "Concatenates `input` `repeat_counter` number of times.\n\nFor example, " +
				"`repeat('dog', 2)` returns `dogdog`.",
		},
	),

	// https://www.postgresql.org/docs/10/static/functions-binarystring.html
	"encode": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"data", types.Bytes}, {"format", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (_ tree.Datum, err error) {
				data, format := *args[0].(*tree.DBytes), string(tree.MustBeDString(args[1]))
				be, ok := sessiondata.BytesEncodeFormatFromString(format)
				if !ok {
					return nil, pgerror.New(pgcode.InvalidParameterValue,
						"only 'hex', 'escape', and 'base64' formats are supported for encode()")
				}
				return tree.NewDString(lex.EncodeByteArrayToRawBytes(
					string(data), be, true /* skipHexPrefix */)), nil
			},
			Info: "Encodes `data` using `format` (`hex` / `escape` / `base64`).",
		},
	),

	"decode": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"text", types.String}, {"format", types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (_ tree.Datum, err error) {
				data, format := string(tree.MustBeDString(args[0])), string(tree.MustBeDString(args[1]))
				be, ok := sessiondata.BytesEncodeFormatFromString(format)
				if !ok {
					return nil, pgerror.New(pgcode.InvalidParameterValue,
						"only 'hex', 'escape', and 'base64' formats are supported for decode()")
				}
				res, err := lex.DecodeRawBytesToByteArray(data, be)
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(res)), nil
			},
			Info: "Decodes `data` using `format` (`hex` / `escape` / `base64`).",
		},
	),

	"ascii": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		stringOverload1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			for _, ch := range s {
				return tree.NewDInt(tree.DInt(ch)), nil
			}
			return nil, errEmptyInputString
		}, types.Int, "Returns the character code of the first character in `val`. Despite the name, the function supports Unicode too.")),

	"chr": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				x := tree.MustBeDInt(args[0])
				var answer string
				switch {
				case x < 0:
					return nil, errChrValueTooSmall
				case x > utf8.MaxRune:
					return nil, errChrValueTooLarge
				default:
					answer = string(rune(x))
				}
				return tree.NewDString(answer), nil
			},
			Info: "Returns the character with the code given in `val`. Inverse function of `ascii()`.",
		},
	),

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

	"to_hex": makeBuiltin(
		tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(fmt.Sprintf("%x", int64(tree.MustBeDInt(args[0])))), nil
			},
			Info: "Converts `val` to its hexadecimal representation.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.Bytes}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				bytes := *(args[0].(*tree.DBytes))
				return tree.NewDString(fmt.Sprintf("%x", []byte(bytes))), nil
			},
			Info: "Converts `val` to its hexadecimal representation.",
		},
	),

	"to_english": makeBuiltin(
		tree.FunctionProperties{Category: categoryString},
		tree.Overload{
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
			Info: "This function enunciates the value of its argument using English cardinals.",
		},
	),

	// The SQL parser coerces POSITION to STRPOS.
	"strpos": makeBuiltin(
		tree.FunctionProperties{Category: categoryString},
		stringOverload2("input", "find", func(_ *tree.EvalContext, s, substring string) (tree.Datum, error) {
			index := strings.Index(s, substring)
			if index < 0 {
				return tree.DZero, nil
			}

			return tree.NewDInt(tree.DInt(utf8.RuneCountInString(s[:index]) + 1)), nil
		}, types.Int, "Calculates the position where the string `find` begins in `input`. \n\nFor"+
			" example, `strpos('doggie', 'gie')` returns `4`.")),

	"overlay": makeBuiltin(defProps(),
		tree.Overload{
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
		tree.Overload{
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
	),

	"lpad": makeBuiltin(
		tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{"string", types.String}, {"length", types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				length := int(tree.MustBeDInt(args[1]))
				ret, err := lpad(s, length, " ")
				if err != nil {
					return nil, err
				}
				return tree.NewDString(ret), nil
			},
			Info: "Pads `string` to `length` by adding ' ' to the left of `string`." +
				"If `string` is longer than `length` it is truncated.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"string", types.String}, {"length", types.Int}, {"fill", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				length := int(tree.MustBeDInt(args[1]))
				fill := string(tree.MustBeDString(args[2]))
				ret, err := lpad(s, length, fill)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(ret), nil
			},
			Info: "Pads `string` by adding `fill` to the left of `string` to make it `length`. " +
				"If `string` is longer than `length` it is truncated.",
		},
	),

	"rpad": makeBuiltin(
		tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{"string", types.String}, {"length", types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				length := int(tree.MustBeDInt(args[1]))
				ret, err := rpad(s, length, " ")
				if err != nil {
					return nil, err
				}
				return tree.NewDString(ret), nil
			},
			Info: "Pads `string` to `length` by adding ' ' to the right of string. " +
				"If `string` is longer than `length` it is truncated.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"string", types.String}, {"length", types.Int}, {"fill", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				length := int(tree.MustBeDInt(args[1]))
				fill := string(tree.MustBeDString(args[2]))
				ret, err := rpad(s, length, fill)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(ret), nil
			},
			Info: "Pads `string` to `length` by adding `fill` to the right of `string`. " +
				"If `string` is longer than `length` it is truncated.",
		},
	),

	// The SQL parser coerces TRIM(...) and TRIM(BOTH ...) to BTRIM(...).
	"btrim": makeBuiltin(defProps(),
		stringOverload2("input", "trim_chars", func(_ *tree.EvalContext, s, chars string) (tree.Datum, error) {
			return tree.NewDString(strings.Trim(s, chars)), nil
		}, types.String, "Removes any characters included in `trim_chars` from the beginning or end"+
			" of `input` (applies recursively). \n\nFor example, `btrim('doggie', 'eod')` "+
			"returns `ggi`."),
		stringOverload1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDString(strings.TrimSpace(s)), nil
		}, types.String, "Removes all spaces from the beginning and end of `val`."),
	),

	// The SQL parser coerces TRIM(LEADING ...) to LTRIM(...).
	"ltrim": makeBuiltin(defProps(),
		stringOverload2("input", "trim_chars", func(_ *tree.EvalContext, s, chars string) (tree.Datum, error) {
			return tree.NewDString(strings.TrimLeft(s, chars)), nil
		}, types.String, "Removes any characters included in `trim_chars` from the beginning "+
			"(left-hand side) of `input` (applies recursively). \n\nFor example, "+
			"`ltrim('doggie', 'od')` returns `ggie`."),
		stringOverload1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDString(strings.TrimLeftFunc(s, unicode.IsSpace)), nil
		}, types.String, "Removes all spaces from the beginning (left-hand side) of `val`."),
	),

	// The SQL parser coerces TRIM(TRAILING ...) to RTRIM(...).
	"rtrim": makeBuiltin(defProps(),
		stringOverload2("input", "trim_chars", func(_ *tree.EvalContext, s, chars string) (tree.Datum, error) {
			return tree.NewDString(strings.TrimRight(s, chars)), nil
		}, types.String, "Removes any characters included in `trim_chars` from the end (right-hand "+
			"side) of `input` (applies recursively). \n\nFor example, `rtrim('doggie', 'ei')` "+
			"returns `dogg`."),
		stringOverload1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDString(strings.TrimRightFunc(s, unicode.IsSpace)), nil
		}, types.String, "Removes all spaces from the end (right-hand side) of `val`."),
	),

	"reverse": makeBuiltin(defProps(),
		stringOverload1(func(evalCtx *tree.EvalContext, s string) (tree.Datum, error) {
			if len(s) > maxAllocatedStringSize {
				return nil, errStringTooLarge
			}
			runes := []rune(s)
			for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
				runes[i], runes[j] = runes[j], runes[i]
			}
			return tree.NewDString(string(runes)), nil
		}, types.String, "Reverses the order of the string's characters.")),

	"replace": makeBuiltin(defProps(),
		stringOverload3("input", "find", "replace",
			func(evalCtx *tree.EvalContext, input, from, to string) (tree.Datum, error) {
				// Reserve memory for the largest possible result.
				var maxResultLen int64
				if len(from) == 0 {
					// Replacing the empty string causes len(input)+1 insertions.
					maxResultLen = int64(len(input) + (len(input)+1)*len(to))
				} else if len(from) < len(to) {
					// Largest result is if input is [from] repeated over and over.
					maxResultLen = int64(len(input) / len(from) * len(to))
				} else {
					// Largest result is if there are no replacements.
					maxResultLen = int64(len(input))
				}
				if maxResultLen > maxAllocatedStringSize {
					return nil, errStringTooLarge
				}
				result := strings.Replace(input, from, to, -1)
				return tree.NewDString(result), nil
			},
			types.String,
			"Replaces all occurrences of `find` with `replace` in `input`",
		)),

	"translate": makeBuiltin(defProps(),
		stringOverload3("input", "find", "replace",
			func(evalCtx *tree.EvalContext, s, from, to string) (tree.Datum, error) {
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
				"`translate('doggie', 'dog', '123');` returns `1233ie`.")),

	"regexp_extract": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"input", types.String}, {"regex", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				pattern := string(tree.MustBeDString(args[1]))
				return regexpExtract(ctx, s, pattern, `\`)
			},
			Info: "Returns the first match for the Regular Expression `regex` in `input`.",
		},
	),

	"regexp_replace": makeBuiltin(defProps(),
		tree.Overload{
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
				return result, nil
			},
			Info: "Replaces matches for the Regular Expression `regex` in `input` with the " +
				"Regular Expression `replace`.",
		},
		tree.Overload{
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
				return result, nil
			},
			Info: "Replaces matches for the regular expression `regex` in `input` with the regular " +
				"expression `replace` using `flags`." + `

CockroachDB supports the following flags:

| Flag           | Description                                                       |
|----------------|-------------------------------------------------------------------|
| **c**          | Case-sensitive matching                                           |
| **g**          | Global matching (match each substring instead of only the first)  |
| **i**          | Case-insensitive matching                                         |
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
	),

	"like_escape": makeBuiltin(defProps(),
		stringOverload3(
			"unescaped", "pattern", "escape",
			func(evalCtx *tree.EvalContext, unescaped, pattern, escape string) (tree.Datum, error) {
				return tree.MatchLikeEscape(evalCtx, unescaped, pattern, escape, false)
			},
			types.Bool,
			"Matches `unescaped` with `pattern` using 'escape' as an escape token.",
		)),

	"not_like_escape": makeBuiltin(defProps(),
		stringOverload3(
			"unescaped", "pattern", "escape",
			func(evalCtx *tree.EvalContext, unescaped, pattern, escape string) (tree.Datum, error) {
				dmatch, err := tree.MatchLikeEscape(evalCtx, unescaped, pattern, escape, false)
				if err != nil {
					return dmatch, err
				}
				bmatch, err := tree.GetBool(dmatch)
				return tree.MakeDBool(!bmatch), err
			},
			types.Bool,
			"Checks whether `unescaped` not matches with `pattern` using 'escape' as an escape token.",
		)),

	"ilike_escape": makeBuiltin(defProps(),
		stringOverload3(
			"unescaped", "pattern", "escape",
			func(evalCtx *tree.EvalContext, unescaped, pattern, escape string) (tree.Datum, error) {
				return tree.MatchLikeEscape(evalCtx, unescaped, pattern, escape, true)
			},
			types.Bool,
			"Matches case insensetively `unescaped` with `pattern` using 'escape' as an escape token.",
		)),

	"not_ilike_escape": makeBuiltin(defProps(),
		stringOverload3(
			"unescaped", "pattern", "escape",
			func(evalCtx *tree.EvalContext, unescaped, pattern, escape string) (tree.Datum, error) {
				dmatch, err := tree.MatchLikeEscape(evalCtx, unescaped, pattern, escape, true)
				if err != nil {
					return dmatch, err
				}
				bmatch, err := tree.GetBool(dmatch)
				return tree.MakeDBool(!bmatch), err
			},
			types.Bool,
			"Checks whether `unescaped` not matches case insensetively with `pattern` using 'escape' as an escape token.",
		)),

	"similar_to_escape": makeBuiltin(defProps(),
		stringOverload3(
			"unescaped", "pattern", "escape",
			func(evalCtx *tree.EvalContext, unescaped, pattern, escape string) (tree.Datum, error) {
				return tree.SimilarToEscape(evalCtx, unescaped, pattern, escape)
			},
			types.Bool,
			"Matches `unescaped` with `pattern` using 'escape' as an escape token.",
		)),

	"not_similar_to_escape": makeBuiltin(defProps(),
		stringOverload3(
			"unescaped", "pattern", "escape",
			func(evalCtx *tree.EvalContext, unescaped, pattern, escape string) (tree.Datum, error) {
				dmatch, err := tree.SimilarToEscape(evalCtx, unescaped, pattern, escape)
				if err != nil {
					return dmatch, err
				}
				bmatch, err := tree.GetBool(dmatch)
				return tree.MakeDBool(!bmatch), err
			},
			types.Bool,
			"Checks whether `unescaped` not matches with `pattern` using 'escape' as an escape token.",
		)),

	"initcap": makeBuiltin(defProps(),
		stringOverload1(func(evalCtx *tree.EvalContext, s string) (tree.Datum, error) {
			return tree.NewDString(strings.Title(strings.ToLower(s))), nil
		}, types.String, "Capitalizes the first letter of `val`.")),

	"quote_ident": makeBuiltin(defProps(),
		stringOverload1(func(evalCtx *tree.EvalContext, s string) (tree.Datum, error) {
			var buf bytes.Buffer
			lex.EncodeRestrictedSQLIdent(&buf, s, lex.EncNoFlags)
			return tree.NewDString(buf.String()), nil
		}, types.String, "Return `val` suitably quoted to serve as identifier in a SQL statement.")),

	"quote_literal": makeBuiltin(defProps(),
		tree.Overload{
			Types:             tree.ArgTypes{{"val", types.String}},
			ReturnType:        tree.FixedReturnType(types.String),
			PreferredOverload: true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := tree.MustBeDString(args[0])
				return tree.NewDString(lex.EscapeSQLString(string(s))), nil
			},
			Info: "Return `val` suitably quoted to serve as string literal in a SQL statement.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.Any}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// PostgreSQL specifies that this variant first casts to the SQL string type,
				// and only then quotes. We can't use (Datum).String() directly.
				d := tree.UnwrapDatum(ctx, args[0])
				strD, err := tree.PerformCast(ctx, d, types.String)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(strD.String()), nil
			},
			Info: "Coerce `val` to a string and then quote it as a literal.",
		},
	),

	// quote_nullable is the same as quote_literal but accepts NULL arguments.
	"quote_nullable": makeBuiltin(
		tree.FunctionProperties{
			Category:     categoryString,
			NullableArgs: true,
		},
		tree.Overload{
			Types:             tree.ArgTypes{{"val", types.String}},
			ReturnType:        tree.FixedReturnType(types.String),
			PreferredOverload: true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.NewDString("NULL"), nil
				}
				s := tree.MustBeDString(args[0])
				return tree.NewDString(lex.EscapeSQLString(string(s))), nil
			},
			Info: "Coerce `val` to a string and then quote it as a literal. If `val` is NULL, returns 'NULL'.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.Any}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.NewDString("NULL"), nil
				}
				// PostgreSQL specifies that this variant first casts to the SQL string type,
				// and only then quotes. We can't use (Datum).String() directly.
				d := tree.UnwrapDatum(ctx, args[0])
				strD, err := tree.PerformCast(ctx, d, types.String)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(strD.String()), nil
			},
			Info: "Coerce `val` to a string and then quote it as a literal. If `val` is NULL, returns 'NULL'.",
		},
	),

	"left": makeBuiltin(defProps(),
		tree.Overload{
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
		tree.Overload{
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
	),

	"right": makeBuiltin(defProps(),
		tree.Overload{
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
		tree.Overload{
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
	),

	"random": makeBuiltin(
		tree.FunctionProperties{
			Impure:                  true,
			NeedsRepeatedEvaluation: true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDFloat(tree.DFloat(rand.Float64())), nil
			},
			Info: "Returns a random float between 0 and 1.",
		},
	),

	"unique_rowid": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryIDGeneration,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDInt(GenerateUniqueInt(ctx.NodeID)), nil
			},
			Info: "Returns a unique ID used by CockroachDB to generate unique row IDs if a " +
				"Primary Key isn't defined for the table. The value is a combination of the " +
				"insert timestamp and the ID of the node executing the statement, which " +
				"guarantees this combination is globally unique. However, there can be " +
				"gaps and the order is not completely guaranteed.",
		},
	),

	// Sequence functions.

	"nextval": makeBuiltin(
		tree.FunctionProperties{
			Category:         categorySequences,
			DistsqlBlacklist: true,
			Impure:           true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"sequence_name", types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
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
	),

	"currval": makeBuiltin(
		tree.FunctionProperties{
			Category:         categorySequences,
			DistsqlBlacklist: true,
			Impure:           true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"sequence_name", types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
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
	),

	"lastval": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySequences,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				val, err := evalCtx.SessionData.SequenceState.GetLastValue()
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(val)), nil
			},
			Info: "Return value most recently obtained with nextval in this session.",
		},
	),

	// Note: behavior is slightly different than Postgres for performance reasons.
	// See https://github.com/cockroachdb/cockroach/issues/21564
	"setval": makeBuiltin(
		tree.FunctionProperties{
			Category:         categorySequences,
			DistsqlBlacklist: true,
			Impure:           true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"sequence_name", types.String}, {"value", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
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
		tree.Overload{
			Types: tree.ArgTypes{
				{"sequence_name", types.String}, {"value", types.Int}, {"is_called", types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Int),
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
	),

	"experimental_uuid_v4": uuidV4Impl,
	"uuid_v4":              uuidV4Impl,

	"greatest": makeBuiltin(
		tree.FunctionProperties{
			Category:     categoryComparison,
			NullableArgs: true,
		},
		tree.Overload{
			Types:      tree.HomogeneousType{},
			ReturnType: tree.FirstNonNullReturnType(),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.PickFromTuple(ctx, true /* greatest */, args)
			},
			Info: "Returns the element with the greatest value.",
		},
	),

	"least": makeBuiltin(
		tree.FunctionProperties{
			Category:     categoryComparison,
			NullableArgs: true,
		},
		tree.Overload{
			Types:      tree.HomogeneousType{},
			ReturnType: tree.FirstNonNullReturnType(),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.PickFromTuple(ctx, false /* greatest */, args)
			},
			Info: "Returns the element with the lowest value.",
		},
	),

	// Timestamp/Date functions.

	"experimental_strftime": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryDateAndTime,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"input", types.Timestamp}, {"extract_format", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
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
		tree.Overload{
			Types:      tree.ArgTypes{{"input", types.Date}, {"extract_format", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fromTime, err := args[0].(*tree.DDate).ToTime()
				if err != nil {
					return nil, err
				}
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
		tree.Overload{
			Types:      tree.ArgTypes{{"input", types.TimestampTZ}, {"extract_format", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
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
	),

	"experimental_strptime": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryDateAndTime,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"input", types.String}, {"format", types.String}},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
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
	),

	// https://www.postgresql.org/docs/10/static/functions-datetime.html
	"age": makeBuiltin(
		tree.FunctionProperties{Impure: true},
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.Interval),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.TimestampDifference(ctx, ctx.GetTxnTimestamp(time.Microsecond), args[0])
			},
			Info: "Calculates the interval between `val` and the current time.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"end", types.TimestampTZ}, {"begin", types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.Interval),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.TimestampDifference(ctx, args[0], args[1])
			},
			Info: "Calculates the interval between `begin` and `end`.",
		},
	),

	"current_date": makeBuiltin(
		tree.FunctionProperties{Impure: true},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn:         currentDate,
			Info:       "Returns the date of the current transaction." + txnTSContextDoc,
		},
	),

	"now":                   txnTSImpl,
	"current_timestamp":     txnTSImpl,
	"transaction_timestamp": txnTSImpl,

	"statement_timestamp": makeBuiltin(
		tree.FunctionProperties{Impure: true},
		tree.Overload{
			Types:             tree.ArgTypes{},
			ReturnType:        tree.FixedReturnType(types.TimestampTZ),
			PreferredOverload: true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDTimestampTZ(ctx.GetStmtTimestamp(), time.Microsecond), nil
			},
			Info: "Returns the start time of the current statement.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDTimestamp(ctx.GetStmtTimestamp(), time.Microsecond), nil
			},
			Info: "Returns the start time of the current statement.",
		},
	),

	tree.FollowerReadTimestampFunctionName: makeBuiltin(
		tree.FunctionProperties{Impure: true},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ts, err := recentTimestamp(ctx)
				if err != nil {
					return nil, err
				}
				return tree.MakeDTimestampTZ(ts, time.Microsecond), nil
			},
			Info: `Returns a timestamp which is very likely to be safe to perform
against a follower replica.

This function is intended to be used with an AS OF SYSTEM TIME clause to perform
historical reads against a time which is recent but sufficiently old for reads
to be performed against the closest replica as opposed to the currently
leaseholder for a given range.

Note that this function requires an enterprise license on a CCL distribution to
return without an error.`,
		},
	),

	"cluster_logical_timestamp": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Decimal),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return ctx.GetClusterTimestamp(), nil
			},
			Info: `Returns the logical time of the current transaction.

This function is reserved for testing purposes by CockroachDB
developers and its definition may change without prior notice.

Note that uses of this function disable server-side optimizations and
may increase either contention or retry errors, or both.`,
		},
	),

	"clock_timestamp": makeBuiltin(
		tree.FunctionProperties{Impure: true},
		tree.Overload{
			Types:             tree.ArgTypes{},
			ReturnType:        tree.FixedReturnType(types.TimestampTZ),
			PreferredOverload: true,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDTimestampTZ(timeutil.Now(), time.Microsecond), nil
			},
			Info: "Returns the current system time on one of the cluster nodes.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDTimestamp(timeutil.Now(), time.Microsecond), nil
			},
			Info: "Returns the current system time on one of the cluster nodes.",
		},
	),

	"extract": makeBuiltin(
		tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{"element", types.String}, {"input", types.Timestamp}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// extract timeSpan fromTime.
				fromTS := args[1].(*tree.DTimestamp)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				return extractStringFromTimestamp(ctx, fromTS.Time, timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: millennium, century, decade, year, isoyear,\n" +
				"quarter, month, week, dayofweek, isodow, dayofyear, julian,\n" +
				"hour, minute, second, millisecond, microsecond, epoch",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"element", types.String}, {"input", types.Date}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				date := args[1].(*tree.DDate)
				fromTSTZ, err := tree.MakeDTimestampTZFromDate(time.UTC, date)
				if err != nil {
					return nil, err
				}
				return extractStringFromTimestamp(ctx, fromTSTZ.Time, timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: millennium, century, decade, year, isoyear,\n" +
				"quarter, month, week, dayofweek, isodow, dayofyear, julian,\n" +
				"hour, minute, second, millisecond, microsecond, epoch",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"element", types.String}, {"input", types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fromTSTZ := args[1].(*tree.DTimestampTZ)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				return extractStringFromTimestamp(ctx, fromTSTZ.Time, timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: millennium, century, decade, year, isoyear,\n" +
				"quarter, month, week, dayofweek, isodow, dayofyear, julian,\n" +
				"hour, minute, second, millisecond, microsecond, epoch",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"element", types.String}, {"input", types.Time}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fromTime := args[1].(*tree.DTime)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				return extractStringFromTime(fromTime, timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: hour, minute, second, millisecond, microsecond, epoch",
		},
	),

	"extract_duration": makeBuiltin(
		tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{"element", types.String}, {"input", types.Interval}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// extract timeSpan fromTime.
				fromInterval := *args[1].(*tree.DInterval)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				switch timeSpan {
				case "hour", "hours":
					return tree.NewDInt(tree.DInt(fromInterval.Nanos() / int64(time.Hour))), nil

				case "minute", "minutes":
					return tree.NewDInt(tree.DInt(fromInterval.Nanos() / int64(time.Minute))), nil

				case "second", "seconds":
					return tree.NewDInt(tree.DInt(fromInterval.Nanos() / int64(time.Second))), nil

				case "millisecond", "milliseconds":
					// This a PG extension not supported in MySQL.
					return tree.NewDInt(tree.DInt(fromInterval.Nanos() / int64(time.Millisecond))), nil

				case "microsecond", "microseconds":
					return tree.NewDInt(tree.DInt(fromInterval.Nanos() / int64(time.Microsecond))), nil

				default:
					return nil, pgerror.Newf(
						pgcode.InvalidParameterValue, "unsupported timespan: %s", timeSpan)
				}
			},
			Info: "Extracts `element` from `input`.\n" +
				"Compatible elements: hour, minute, second, millisecond, microsecond.",
		},
	),

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
	"date_trunc": makeBuiltin(
		tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{"element", types.String}, {"input", types.Timestamp}},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				fromTS := args[1].(*tree.DTimestamp)
				tsTZ, err := truncateTimestamp(ctx, fromTS.Time, timeSpan)
				if err != nil {
					return nil, err
				}
				return tree.MakeDTimestamp(tsTZ.Time, time.Microsecond), nil
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero (or one, for day and month)\n\n" +
				"Compatible elements: millennium, century, decade, year, quarter, month,\n" +
				"week, day, hour, minute, second, millisecond, microsecond.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"element", types.String}, {"input", types.Date}},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				date := args[1].(*tree.DDate)
				// Localize the timestamp into the given location.
				fromTSTZ, err := tree.MakeDTimestampTZFromDate(ctx.GetLocation(), date)
				if err != nil {
					return nil, err
				}
				// From the given time in the context location, we also have to subtract
				// the offset.
				// This is because we expect to truncate in the given timezone,
				// but the date argument assumed no timezone, meaning converting it into
				// the location's timestamp with the date library converts it into the
				// wrong time locally.
				_, offset := fromTSTZ.Time.Zone()
				fromTSTZTime := fromTSTZ.Time.Add(time.Duration(-offset) * time.Second)
				return truncateTimestamp(ctx, fromTSTZTime, timeSpan)
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero (or one, for day and month)\n\n" +
				"Compatible elements: millennium, century, decade, year, quarter, month,\n" +
				"week, day, hour, minute, second, millisecond, microsecond.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"element", types.String}, {"input", types.Time}},
			ReturnType: tree.FixedReturnType(types.Interval),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				fromTime := args[1].(*tree.DTime)
				time, err := truncateTime(fromTime, timeSpan)
				if err != nil {
					return nil, err
				}
				return &tree.DInterval{Duration: duration.MakeDuration(int64(*time)*1000, 0, 0)}, nil
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero.\n\n" +
				"Compatible elements: hour, minute, second, millisecond, microsecond.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"element", types.String}, {"input", types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fromTSTZ := args[1].(*tree.DTimestampTZ)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				return truncateTimestamp(ctx, fromTSTZ.Time.In(ctx.GetLocation()), timeSpan)
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero (or one, for day and month)\n\n" +
				"Compatible elements: millennium, century, decade, year, quarter, month,\n" +
				"week, day, hour, minute, second, millisecond, microsecond.",
		},
	),

	// Math functions
	"abs": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Abs(x))), nil
		}, "Calculates the absolute value of `val`."),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			dd.Abs(x)
			return dd, nil
		}, "Calculates the absolute value of `val`."),
		tree.Overload{
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
	),

	"acos": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Acos(x))), nil
		}, "Calculates the inverse cosine of `val`."),
	),

	"asin": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Asin(x))), nil
		}, "Calculates the inverse sine of `val`."),
	),

	"atan": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Atan(x))), nil
		}, "Calculates the inverse tangent of `val`."),
	),

	"atan2": makeBuiltin(defProps(),
		floatOverload2("x", "y", func(x, y float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Atan2(x, y))), nil
		}, "Calculates the inverse tangent of `x`/`y`."),
	),

	"cbrt": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Cbrt(x))), nil
		}, "Calculates the cube root (∛) of `val`."),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			_, err := tree.DecimalCtx.Cbrt(&dd.Decimal, x)
			return dd, err
		}, "Calculates the cube root (∛) of `val`."),
	),

	"ceil":    ceilImpl,
	"ceiling": ceilImpl,

	"cos": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Cos(x))), nil
		}, "Calculates the cosine of `val`."),
	),

	"cot": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(1 / math.Tan(x))), nil
		}, "Calculates the cotangent of `val`."),
	),

	"degrees": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(180.0 * x / math.Pi)), nil
		}, "Converts `val` as a radian value to a degree value."),
	),

	"div": makeBuiltin(defProps(),
		floatOverload2("x", "y", func(x, y float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Trunc(x / y))), nil
		}, "Calculates the integer quotient of `x`/`y`."),
		decimalOverload2("x", "y", func(x, y *apd.Decimal) (tree.Datum, error) {
			if y.Sign() == 0 {
				return nil, tree.ErrDivByZero
			}
			dd := &tree.DDecimal{}
			_, err := tree.HighPrecisionCtx.QuoInteger(&dd.Decimal, x, y)
			return dd, err
		}, "Calculates the integer quotient of `x`/`y`."),
		tree.Overload{
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
	),

	"exp": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Exp(x))), nil
		}, "Calculates *e* ^ `val`."),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			_, err := tree.DecimalCtx.Exp(&dd.Decimal, x)
			return dd, err
		}, "Calculates *e* ^ `val`."),
	),

	"floor": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Floor(x))), nil
		}, "Calculates the largest integer not greater than `val`."),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			_, err := tree.ExactCtx.Floor(&dd.Decimal, x)
			return dd, err
		}, "Calculates the largest integer not greater than `val`."),
	),

	"isnan": makeBuiltin(defProps(),
		tree.Overload{
			// Can't use floatBuiltin1 here because this one returns
			// a boolean.
			Types:      tree.ArgTypes{{"val", types.Float}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDBool(tree.DBool(math.IsNaN(float64(*args[0].(*tree.DFloat))))), nil
			},
			Info: "Returns true if `val` is NaN, false otherwise.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.Decimal}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				isNaN := args[0].(*tree.DDecimal).Decimal.Form == apd.NaN
				return tree.MakeDBool(tree.DBool(isNaN)), nil
			},
			Info: "Returns true if `val` is NaN, false otherwise.",
		},
	),

	"ln": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Log(x))), nil
		}, "Calculates the natural log of `val`."),
		decimalLogFn(tree.DecimalCtx.Ln, "Calculates the natural log of `val`."),
	),

	"log": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Log10(x))), nil
		}, "Calculates the base 10 log of `val`."),
		decimalLogFn(tree.DecimalCtx.Log10, "Calculates the base 10 log of `val`."),
	),

	"mod": makeBuiltin(defProps(),
		floatOverload2("x", "y", func(x, y float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Mod(x, y))), nil
		}, "Calculates `x`%`y`."),
		decimalOverload2("x", "y", func(x, y *apd.Decimal) (tree.Datum, error) {
			if y.Sign() == 0 {
				return nil, tree.ErrZeroModulus
			}
			dd := &tree.DDecimal{}
			_, err := tree.HighPrecisionCtx.Rem(&dd.Decimal, x, y)
			return dd, err
		}, "Calculates `x`%`y`."),
		tree.Overload{
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
	),

	"pi": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDFloat(math.Pi), nil
			},
			Info: "Returns the value for pi (3.141592653589793).",
		},
	),

	"pow":   powImpls,
	"power": powImpls,

	"radians": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(x * math.Pi / 180.0)), nil
		}, "Converts `val` as a degree value to a radians value."),
	),

	"round": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.RoundToEven(x))), nil
		}, "Rounds `val` to the nearest integer using half to even (banker's) rounding."),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			return roundDecimal(x, 0)
		}, "Rounds `val` to the nearest integer, half away from zero: "+
			"round(+/-2.4) = +/-2, round(+/-2.5) = +/-3."),
		tree.Overload{
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
		tree.Overload{
			Types:      tree.ArgTypes{{"input", types.Decimal}, {"decimal_accuracy", types.Int}},
			ReturnType: tree.FixedReturnType(types.Decimal),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// TODO(mjibson): make sure this fits in an int32.
				scale := int32(tree.MustBeDInt(args[1]))
				return roundDecimal(&args[0].(*tree.DDecimal).Decimal, scale)
			},
			Info: "Keeps `decimal_accuracy` number of figures to the right of the zero position " +
				"in `input` using half away from zero rounding. If `decimal_accuracy` " +
				"is not in the range -2^31...(2^31-1), the results are undefined.",
		},
	),

	"row_to_json": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"row", types.AnyTuple}},
			ReturnType: tree.FixedReturnType(types.Jsonb),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				tuple := args[0].(*tree.DTuple)
				builder := json.NewObjectBuilder(len(tuple.D))
				typ := tuple.ResolvedType()
				labels := typ.TupleLabels()
				for i, d := range tuple.D {
					var label string
					if labels != nil {
						label = labels[i]
					}
					if label == "" {
						label = fmt.Sprintf("f%d", i+1)
					}
					val, err := tree.AsJSON(d)
					if err != nil {
						return nil, err
					}
					builder.Add(label, val)
				}
				return tree.NewDJSON(builder.Build()), nil
			},
			Info: "Returns the row as a JSON object.",
		},
	),

	"sin": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Sin(x))), nil
		}, "Calculates the sine of `val`."),
	),

	"sign": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			switch {
			case x < 0:
				return tree.NewDFloat(-1), nil
			case x == 0:
				return tree.NewDFloat(0), nil
			}
			return tree.NewDFloat(1), nil
		}, "Determines the sign of `val`: **1** for positive; **0** for 0 values; **-1** for "+
			"negative."),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			d := &tree.DDecimal{}
			d.Decimal.SetFinite(int64(x.Sign()), 0)
			return d, nil
		}, "Determines the sign of `val`: **1** for positive; **0** for 0 values; **-1** for "+
			"negative."),
		tree.Overload{
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
	),

	"sqrt": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			// TODO(mjibson): see #13642
			if x < 0 {
				return nil, errSqrtOfNegNumber
			}
			return tree.NewDFloat(tree.DFloat(math.Sqrt(x))), nil
		}, "Calculates the square root of `val`."),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			if x.Sign() < 0 {
				return nil, errSqrtOfNegNumber
			}
			dd := &tree.DDecimal{}
			_, err := tree.DecimalCtx.Sqrt(&dd.Decimal, x)
			return dd, err
		}, "Calculates the square root of `val`."),
	),

	"tan": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Tan(x))), nil
		}, "Calculates the tangent of `val`."),
	),

	// https://www.postgresql.org/docs/9.6/functions-datetime.html
	"timezone": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"timestamp", types.Timestamp},
				{"timezone", types.String},
			},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ts := tree.MustBeDTimestamp(args[0])
				tzStr := string(tree.MustBeDString(args[1]))
				loc, err := timeutil.TimeZoneStringToLocation(tzStr)
				if err != nil {
					return nil, err
				}
				_, before := ts.Time.Zone()
				_, after := ts.Time.In(loc).Zone()
				return tree.MakeDTimestampTZ(ts.Time.Add(time.Duration(before-after)*time.Second), time.Microsecond), nil
			},
			Info: "Treat given time stamp without time zone as located in the specified time zone",
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"timestamptz", types.TimestampTZ},
				{"timezone", types.String},
			},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ts := tree.MustBeDTimestampTZ(args[0])
				tzStr := string(tree.MustBeDString(args[1]))
				loc, err := timeutil.TimeZoneStringToLocation(tzStr)
				if err != nil {
					return nil, err
				}
				return ts.EvalAtTimeZone(ctx, loc), nil
			},
			Info: "Convert given time stamp with time zone to the new time zone, with no time zone designation",
		},
	),

	"trunc": makeBuiltin(defProps(),
		floatOverload1(func(x float64) (tree.Datum, error) {
			return tree.NewDFloat(tree.DFloat(math.Trunc(x))), nil
		}, "Truncates the decimal values of `val`."),
		decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
			dd := &tree.DDecimal{}
			x.Modf(&dd.Decimal, nil)
			return dd, nil
		}, "Truncates the decimal values of `val`."),
	),

	"width_bucket": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ArgTypes{{"operand", types.Decimal}, {"b1", types.Decimal},
				{"b2", types.Decimal}, {"count", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				operand, _ := args[0].(*tree.DDecimal).Float64()
				b1, _ := args[1].(*tree.DDecimal).Float64()
				b2, _ := args[2].(*tree.DDecimal).Float64()
				count := int(tree.MustBeDInt(args[3]))
				return tree.NewDInt(tree.DInt(widthBucket(operand, b1, b2, count))), nil
			},
			Info: "return the bucket number to which operand would be assigned in a histogram having count " +
				"equal-width buckets spanning the range b1 to b2.",
		},
		tree.Overload{
			Types: tree.ArgTypes{{"operand", types.Int}, {"b1", types.Int},
				{"b2", types.Int}, {"count", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				operand := float64(tree.MustBeDInt(args[0]))
				b1 := float64(tree.MustBeDInt(args[1]))
				b2 := float64(tree.MustBeDInt(args[2]))
				count := int(tree.MustBeDInt(args[3]))
				return tree.NewDInt(tree.DInt(widthBucket(operand, b1, b2, count))), nil
			},
			Info: "return the bucket number to which operand would be assigned in a histogram having count " +
				"equal-width buckets spanning the range b1 to b2.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"operand", types.Any}, {"thresholds", types.AnyArray}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				operand := args[0]
				thresholds := tree.MustBeDArray(args[1])

				if !operand.ResolvedType().Equivalent(thresholds.ParamTyp) {
					return tree.NewDInt(0), errors.New("Operand and thresholds must be of the same type")
				}

				for i, v := range thresholds.Array {
					if operand.Compare(ctx, v) < 0 {
						return tree.NewDInt(tree.DInt(i)), nil
					}
				}

				return tree.NewDInt(tree.DInt(thresholds.Len())), nil
			},
			Info: "return the bucket number to which operand would be assigned given an array listing the " +
				"lower bounds of the buckets; returns 0 for an input less than the first lower bound; the " +
				"thresholds array must be sorted, smallest first, or unexpected results will be obtained",
		},
	),

	// Array functions.

	"string_to_array": makeBuiltin(arrayPropsNullableArgs(),
		tree.Overload{
			Types:      tree.ArgTypes{{"str", types.String}, {"delimiter", types.String}},
			ReturnType: tree.FixedReturnType(types.StringArray),
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
		tree.Overload{
			Types:      tree.ArgTypes{{"str", types.String}, {"delimiter", types.String}, {"null", types.String}},
			ReturnType: tree.FixedReturnType(types.StringArray),
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
	),

	"array_to_string": makeBuiltin(arrayPropsNullableArgs(),
		tree.Overload{
			Types:      tree.ArgTypes{{"input", types.AnyArray}, {"delim", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull || args[1] == tree.DNull {
					return tree.DNull, nil
				}
				arr := tree.MustBeDArray(args[0])
				delim := string(tree.MustBeDString(args[1]))
				return arrayToString(arr, delim, nil)
			},
			Info: "Join an array into a string with a delimiter.",
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"input", types.AnyArray}, {"delimiter", types.String}, {"null", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull || args[1] == tree.DNull {
					return tree.DNull, nil
				}
				arr := tree.MustBeDArray(args[0])
				delim := string(tree.MustBeDString(args[1]))
				nullStr := stringOrNil(args[2])
				return arrayToString(arr, delim, nullStr)
			},
			Info: "Join an array into a string with a delimiter, replacing NULLs with a null string.",
		},
	),

	"array_length": makeBuiltin(arrayProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"input", types.AnyArray}, {"array_dimension", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				arr := tree.MustBeDArray(args[0])
				dimen := int64(tree.MustBeDInt(args[1]))
				return arrayLength(arr, dimen), nil
			},
			Info: "Calculates the length of `input` on the provided `array_dimension`. However, " +
				"because CockroachDB doesn't yet support multi-dimensional arrays, the only supported" +
				" `array_dimension` is **1**.",
		},
	),

	"array_lower": makeBuiltin(arrayProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"input", types.AnyArray}, {"array_dimension", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				arr := tree.MustBeDArray(args[0])
				dimen := int64(tree.MustBeDInt(args[1]))
				return arrayLower(arr, dimen), nil
			},
			Info: "Calculates the minimum value of `input` on the provided `array_dimension`. " +
				"However, because CockroachDB doesn't yet support multi-dimensional arrays, the only " +
				"supported `array_dimension` is **1**.",
		},
	),

	"array_upper": makeBuiltin(arrayProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"input", types.AnyArray}, {"array_dimension", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				arr := tree.MustBeDArray(args[0])
				dimen := int64(tree.MustBeDInt(args[1]))
				return arrayLength(arr, dimen), nil
			},
			Info: "Calculates the maximum value of `input` on the provided `array_dimension`. " +
				"However, because CockroachDB doesn't yet support multi-dimensional arrays, the only " +
				"supported `array_dimension` is **1**.",
		},
	),

	"array_append": setProps(arrayPropsNullableArgs(), arrayBuiltin(func(typ *types.T) tree.Overload {
		return tree.Overload{
			Types:      tree.ArgTypes{{"array", types.MakeArray(typ)}, {"elem", typ}},
			ReturnType: tree.FixedReturnType(types.MakeArray(typ)),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.AppendToMaybeNullArray(typ, args[0], args[1])
			},
			Info: "Appends `elem` to `array`, returning the result.",
		}
	})),

	"array_prepend": setProps(arrayPropsNullableArgs(), arrayBuiltin(func(typ *types.T) tree.Overload {
		return tree.Overload{
			Types:      tree.ArgTypes{{"elem", typ}, {"array", types.MakeArray(typ)}},
			ReturnType: tree.FixedReturnType(types.MakeArray(typ)),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.PrependToMaybeNullArray(typ, args[0], args[1])
			},
			Info: "Prepends `elem` to `array`, returning the result.",
		}
	})),

	"array_cat": setProps(arrayPropsNullableArgs(), arrayBuiltin(func(typ *types.T) tree.Overload {
		return tree.Overload{
			Types:      tree.ArgTypes{{"left", types.MakeArray(typ)}, {"right", types.MakeArray(typ)}},
			ReturnType: tree.FixedReturnType(types.MakeArray(typ)),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.ConcatArrays(typ, args[0], args[1])
			},
			Info: "Appends two arrays.",
		}
	})),

	"array_remove": setProps(arrayPropsNullableArgs(), arrayBuiltin(func(typ *types.T) tree.Overload {
		return tree.Overload{
			Types:      tree.ArgTypes{{"array", types.MakeArray(typ)}, {"elem", typ}},
			ReturnType: tree.FixedReturnType(types.MakeArray(typ)),
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
	})),

	"array_replace": setProps(arrayPropsNullableArgs(), arrayBuiltin(func(typ *types.T) tree.Overload {
		return tree.Overload{
			Types:      tree.ArgTypes{{"array", types.MakeArray(typ)}, {"toreplace", typ}, {"replacewith", typ}},
			ReturnType: tree.FixedReturnType(types.MakeArray(typ)),
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
	})),

	"array_position": setProps(arrayPropsNullableArgs(), arrayBuiltin(func(typ *types.T) tree.Overload {
		return tree.Overload{
			Types:      tree.ArgTypes{{"array", types.MakeArray(typ)}, {"elem", typ}},
			ReturnType: tree.FixedReturnType(types.Int),
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
	})),

	"array_positions": setProps(arrayPropsNullableArgs(), arrayBuiltin(func(typ *types.T) tree.Overload {
		return tree.Overload{
			Types:      tree.ArgTypes{{"array", types.MakeArray(typ)}, {"elem", typ}},
			ReturnType: tree.FixedReturnType(types.IntArray),
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
	})),

	// JSON functions.

	"json_to_recordset":        makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 33285, Category: categoryJSON}),
	"jsonb_to_recordset":       makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 33285, Category: categoryJSON}),
	"json_populate_recordset":  makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 33285, Category: categoryJSON}),
	"jsonb_populate_recordset": makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 33285, Category: categoryJSON}),

	"json_remove_path": makeBuiltin(jsonProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.Jsonb}, {"path", types.StringArray}},
			ReturnType: tree.FixedReturnType(types.Jsonb),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ary := *tree.MustBeDArray(args[1])
				if err := checkHasNulls(ary); err != nil {
					return nil, err
				}
				path, _ := darrayToStringSlice(ary)
				s, _, err := tree.MustBeDJSON(args[0]).JSON.RemovePath(path)
				if err != nil {
					return nil, err
				}
				return &tree.DJSON{JSON: s}, nil
			},
			Info: "Remove the specified path from the JSON object.",
		},
	),

	"json_extract_path": makeBuiltin(jsonProps(), jsonExtractPathImpl),

	"jsonb_extract_path": makeBuiltin(jsonProps(), jsonExtractPathImpl),

	"json_set": makeBuiltin(jsonProps(), jsonSetImpl, jsonSetWithCreateMissingImpl),

	"jsonb_set": makeBuiltin(jsonProps(), jsonSetImpl, jsonSetWithCreateMissingImpl),

	"jsonb_insert": makeBuiltin(jsonProps(), jsonInsertImpl, jsonInsertWithInsertAfterImpl),

	"jsonb_pretty": makeBuiltin(jsonProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.Jsonb}},
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
	),

	"json_typeof": makeBuiltin(jsonProps(), jsonTypeOfImpl),

	"jsonb_typeof": makeBuiltin(jsonProps(), jsonTypeOfImpl),

	"array_to_json": arrayToJSONImpls,

	"to_json": makeBuiltin(jsonProps(), toJSONImpl),

	"to_jsonb": makeBuiltin(jsonProps(), toJSONImpl),

	"json_build_array": makeBuiltin(jsonPropsNullableArgs(), jsonBuildArrayImpl),

	"jsonb_build_array": makeBuiltin(jsonPropsNullableArgs(), jsonBuildArrayImpl),

	"json_build_object": makeBuiltin(jsonPropsNullableArgs(), jsonBuildObjectImpl),

	"jsonb_build_object": makeBuiltin(jsonPropsNullableArgs(), jsonBuildObjectImpl),

	"json_object": jsonObjectImpls,

	"jsonb_object": jsonObjectImpls,

	"json_strip_nulls": makeBuiltin(jsonProps(), jsonStripNullsImpl),

	"jsonb_strip_nulls": makeBuiltin(jsonProps(), jsonStripNullsImpl),

	"json_array_length": makeBuiltin(jsonProps(), jsonArrayLengthImpl),

	"jsonb_array_length": makeBuiltin(jsonProps(), jsonArrayLengthImpl),

	// Metadata functions.

	// https://www.postgresql.org/docs/10/static/functions-info.html
	"version": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(build.GetInfo().Short()), nil
			},
			Info: "Returns the node's version of CockroachDB.",
		},
	),

	// https://www.postgresql.org/docs/10/static/functions-info.html
	"current_database": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if len(ctx.SessionData.Database) == 0 {
					return tree.DNull, nil
				}
				return tree.NewDString(ctx.SessionData.Database), nil
			},
			Info: "Returns the current database.",
		},
	),

	// https://www.postgresql.org/docs/10/static/functions-info.html
	//
	// Note that in addition to what the pg doc says ("current_schema =
	// first item in search path"), the pg server actually skips over
	// non-existent schemas in the search path to determine
	// current_schema. This is not documented but can be verified by a
	// SQL client against a pg server.
	"current_schema": makeBuiltin(
		tree.FunctionProperties{
			Category:         categorySystemInfo,
			DistsqlBlacklist: true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ctx := evalCtx.Ctx()
				curDb := evalCtx.SessionData.Database
				iter := evalCtx.SessionData.SearchPath.IterWithoutImplicitPGCatalog()
				for scName, ok := iter.Next(); ok; scName, ok = iter.Next() {
					if found, _, err := evalCtx.Planner.LookupSchema(ctx, curDb, scName); found || err != nil {
						if err != nil {
							return nil, err
						}
						return tree.NewDString(scName), nil
					}
				}
				return tree.DNull, nil
			},
			Info: "Returns the current schema.",
		},
	),

	// https://www.postgresql.org/docs/10/static/functions-info.html
	//
	// Note that in addition to what the pg doc says ("current_schemas =
	// items in search path with or without pg_catalog depending on
	// argument"), the pg server actually skips over non-existent
	// schemas in the search path to compute current_schemas. This is
	// not documented but can be verified by a SQL client against a pg
	// server.
	"current_schemas": makeBuiltin(
		tree.FunctionProperties{
			Category:         categorySystemInfo,
			DistsqlBlacklist: true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"include_pg_catalog", types.Bool}},
			ReturnType: tree.FixedReturnType(types.StringArray),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ctx := evalCtx.Ctx()
				curDb := evalCtx.SessionData.Database
				includePgCatalog := *(args[0].(*tree.DBool))
				schemas := tree.NewDArray(types.String)
				var iter sessiondata.SearchPathIter
				if includePgCatalog {
					iter = evalCtx.SessionData.SearchPath.Iter()
				} else {
					iter = evalCtx.SessionData.SearchPath.IterWithoutImplicitPGCatalog()
				}
				for scName, ok := iter.Next(); ok; scName, ok = iter.Next() {
					if found, _, err := evalCtx.Planner.LookupSchema(ctx, curDb, scName); found || err != nil {
						if err != nil {
							return nil, err
						}
						if err := schemas.Append(tree.NewDString(scName)); err != nil {
							return nil, err
						}
					}
				}
				return schemas, nil
			},
			Info: "Returns the valid schemas in the search path.",
		},
	),

	// https://www.postgresql.org/docs/10/static/functions-info.html
	"current_user": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if len(ctx.SessionData.User) == 0 {
					return tree.DNull, nil
				}
				return tree.NewDString(ctx.SessionData.User), nil
			},
			Info: "Returns the current user. This function is provided for " +
				"compatibility with PostgreSQL.",
		},
	),

	// https://www.postgresql.org/docs/10/functions-info.html#FUNCTIONS-INFO-CATALOG-TABLE
	"pg_collation_for": makeBuiltin(
		tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{"str", types.Any}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				var collation string
				switch t := args[0].(type) {
				case *tree.DString:
					collation = "default"
				case *tree.DCollatedString:
					collation = t.Locale
				default:
					return tree.DNull, pgerror.Newf(pgcode.DatatypeMismatch,
						"collations are not supported by type: %s", t.ResolvedType())
				}
				return tree.NewDString(fmt.Sprintf(`"%s"`, collation)), nil
			},
			Info: "Returns the collation of the argument",
		},
	),

	"crdb_internal.locality_value": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types:      tree.ArgTypes{{"key", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				key := string(*(args[0].(*tree.DString)))
				for i := range ctx.Locality.Tiers {
					tier := &ctx.Locality.Tiers[i]
					if tier.Key == key {
						return tree.NewDString(tier.Value), nil
					}
				}
				return tree.DNull, nil
			},
			Info: "Returns the value of the specified locality key.",
		},
	),

	"crdb_internal.node_executable_version": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				v := cluster.Version.BinaryVersion(ctx.Settings).String()
				return tree.NewDString(v), nil
			},
			Info: "Returns the version of CockroachDB this node is running.",
		},
	),

	"crdb_internal.cluster_id": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Uuid),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDUuid(tree.DUuid{UUID: ctx.ClusterID}), nil
			},
			Info: "Returns the cluster ID.",
		},
	),

	"crdb_internal.cluster_name": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(ctx.ClusterName), nil
			},
			Info: "Returns the cluster name.",
		},
	),

	"crdb_internal.encode_key": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types: tree.ArgTypes{
				{"table_id", types.Int},
				{"index_id", types.Int},
				{"row_tuple", types.Any},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				tableID := int(tree.MustBeDInt(args[0]))
				indexID := int(tree.MustBeDInt(args[1]))
				rowDatums, ok := tree.AsDTuple(args[2])
				if !ok {
					return nil, errors.AssertionFailedf("expected tuple argument for row_tuple, found %s", args[2])
				}

				tableDesc, err := sqlbase.GetTableDescFromID(ctx.Context, ctx.Txn, sqlbase.ID(tableID))
				if err != nil {
					return nil, err
				}

				if len(rowDatums.D) != len(tableDesc.Columns) {
					return nil, pgerror.Newf(pgcode.Syntax, "number of values provided must equal number of columns in table")
				}
				// Check that all the input datums have types that line up with the columns.
				var datums tree.Datums
				for i, d := range rowDatums.D {
					// We try to perform a cast here rather than a typecheck because the individual datums
					// already have a fixed type, and not information is known at typechecking time for those
					// datums to line up with the column types of the input table. Instead we try to cast the
					// parsed datums into the types of the table's columns.
					var newDatum tree.Datum
					if d.ResolvedType() == types.Unknown {
						if !tableDesc.Columns[i].Nullable {
							return nil, pgerror.Newf(pgcode.NotNullViolation, "NULL provided as a value for a non-nullable column")
						}
						newDatum = tree.DNull
					} else {
						expectedTyp := tableDesc.Columns[i].DatumType()
						newDatum, err = tree.PerformCast(ctx, d, expectedTyp)
						if err != nil {
							return nil, errors.WithHint(err, "try to explicitly cast each value to the corresponding column type")
						}
					}
					datums = append(datums, newDatum)
				}

				indexDesc := tableDesc.AllNonDropIndexes()[indexID-1]

				// Create a column id to row index map. In this case, each column ID just maps to the i'th ordinal.
				colMap := make(map[sqlbase.ColumnID]int)
				for i := range tableDesc.Columns {
					colMap[tableDesc.Columns[i].ID] = i
				}

				if indexDesc.ID == tableDesc.PrimaryIndex.ID {
					keyPrefix := tableDesc.IndexSpan(indexDesc.ID).Key
					res, _, err := sqlbase.EncodeIndexKey(tableDesc, indexDesc, colMap, datums, keyPrefix)
					if err != nil {
						return nil, err
					}
					return tree.NewDBytes(tree.DBytes(res)), err
				}
				// We have a secondary index.
				res, err := sqlbase.EncodeSecondaryIndex(tableDesc, indexDesc, colMap, datums)
				if err != nil {
					return nil, err
				}
				// If EncodeSecondaryIndex returns more than one element then we have an inverted index,
				// which this command does not support right now.
				if len(res) > 1 {
					return nil, unimplemented.NewWithIssue(41232, "inverted indexes not supported right now")
				}
				return tree.NewDBytes(tree.DBytes(res[0].Key)), err
			},
			Info: "Generate the key for a row on a particular table and index.",
		},
	),

	"crdb_internal.force_error": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"errorCode", types.String}, {"msg", types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				errCode := string(*args[0].(*tree.DString))
				msg := string(*args[1].(*tree.DString))
				if errCode == "" {
					return nil, errors.New(msg)
				}
				return nil, pgerror.New(errCode, msg)
			},
			Info: "This function is used only by CockroachDB's developers for testing purposes.",
		},
	),

	"crdb_internal.force_assertion_error": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"msg", types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				msg := string(*args[0].(*tree.DString))
				return nil, errors.AssertionFailedf("%s", msg)
			},
			Info: "This function is used only by CockroachDB's developers for testing purposes.",
		},
	),

	"crdb_internal.force_panic": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"msg", types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if err := checkPrivilegedUser(ctx); err != nil {
					return nil, err
				}
				msg := string(*args[0].(*tree.DString))
				panic(msg)
			},
			Info: "This function is used only by CockroachDB's developers for testing purposes.",
		},
	),

	"crdb_internal.force_log_fatal": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"msg", types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if err := checkPrivilegedUser(ctx); err != nil {
					return nil, err
				}
				msg := string(*args[0].(*tree.DString))
				log.Fatal(ctx.Ctx(), msg)
				return nil, nil
			},
			Info: "This function is used only by CockroachDB's developers for testing purposes.",
		},
	),

	// If force_retry is called during the specified interval from the beginning
	// of the transaction it returns a retryable error. If not, 0 is returned
	// instead of an error.
	// The second version allows one to create an error intended for a transaction
	// different than the current statement's transaction.
	"crdb_internal.force_retry": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.Interval}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				minDuration := args[0].(*tree.DInterval).Duration
				elapsed := duration.MakeDuration(int64(ctx.StmtTimestamp.Sub(ctx.TxnTimestamp)), 0, 0)
				if elapsed.Compare(minDuration) < 0 {
					return nil, ctx.Txn.GenerateForcedRetryableError(
						ctx.Ctx(), "forced by crdb_internal.force_retry()")
				}
				return tree.DZero, nil
			},
			Info: "This function is used only by CockroachDB's developers for testing purposes.",
		},
	),

	// Fetches the corresponding lease_holder for the request key.
	"crdb_internal.lease_holder": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"key", types.Bytes}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				key := []byte(tree.MustBeDBytes(args[0]))
				b := &client.Batch{}
				b.AddRawRequest(&roachpb.LeaseInfoRequest{
					RequestHeader: roachpb.RequestHeader{
						Key: key,
					},
				})
				if err := ctx.Txn.Run(ctx.Context, b); err != nil {
					return nil, pgerror.Newf(pgcode.InvalidParameterValue, "message: %s", err)
				}
				resp := b.RawResponse().Responses[0].GetInner().(*roachpb.LeaseInfoResponse)

				return tree.NewDInt(tree.DInt(resp.Lease.Replica.StoreID)), nil
			},
			Info: "This function is used to fetch the leaseholder corresponding to a request key",
		},
	),

	// Identity function which is marked as impure to avoid constant folding.
	"crdb_internal.no_constant_folding": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"input", types.Any}},
			ReturnType: tree.IdentityReturnType(0),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return args[0], nil
			},
			Info: "This function is used only by CockroachDB's developers for testing purposes.",
		},
	),

	// Return a pretty key for a given raw key, skipping the specified number of
	// fields.
	"crdb_internal.pretty_key": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"raw_key", types.Bytes},
				{"skip_fields", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(sqlbase.PrettyKey(
					nil, /* valDirs */
					roachpb.Key(tree.MustBeDBytes(args[0])),
					int(tree.MustBeDInt(args[1])))), nil
			},
			Info: "This function is used only by CockroachDB's developers for testing purposes.",
		},
	),

	// Return statistics about a range.
	"crdb_internal.range_stats": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"key", types.Bytes},
			},
			ReturnType: tree.FixedReturnType(types.Jsonb),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				key := []byte(tree.MustBeDBytes(args[0]))
				b := &client.Batch{}
				b.AddRawRequest(&roachpb.RangeStatsRequest{
					RequestHeader: roachpb.RequestHeader{
						Key: key,
					},
				})
				if err := ctx.Txn.Run(ctx.Context, b); err != nil {
					return nil, pgerror.Newf(pgcode.InvalidParameterValue, "message: %s", err)
				}
				resp := b.RawResponse().Responses[0].GetInner().(*roachpb.RangeStatsResponse).MVCCStats
				jsonStr, err := gojson.Marshal(&resp)
				if err != nil {
					return nil, err
				}
				jsonDatum, err := tree.ParseDJSON(string(jsonStr))
				if err != nil {
					return nil, err
				}
				return jsonDatum, nil
			},
			Info: "This function is used to retrieve range statistics information as a JSON object.",
		},
	),

	"crdb_internal.set_vmodule": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
			Impure:   true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"vmodule_string", types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if err := checkPrivilegedUser(ctx); err != nil {
					return nil, err
				}
				return tree.DZero, log.SetVModule(string(*args[0].(*tree.DString)))
			},
			Info: "Set the equivalent of the `--vmodule` flag on the gateway node processing this request; " +
				"it affords control over the logging verbosity of different files. " +
				"Example syntax: `crdb_internal.set_vmodule('recordio=2,file=1,gfs*=3')`. " +
				"Reset with: `crdb_internal.set_vmodule('')`. " +
				"Raising the verbosity can severely affect performance.",
		},
	),

	// Returns the number of distinct inverted index entries that would be generated for a JSON value.
	"crdb_internal.json_num_index_entries": makeBuiltin(
		tree.FunctionProperties{
			Category:     categorySystemInfo,
			NullableArgs: true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.Jsonb}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				arg := args[0]
				if arg == tree.DNull {
					return tree.NewDInt(tree.DInt(1)), nil
				}
				n, err := json.NumInvertedIndexEntries(tree.MustBeDJSON(arg).JSON)
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(n)), nil
			},
			Info: "This function is used only by CockroachDB's developers for testing purposes.",
		},
	),

	"crdb_internal.round_decimal_values": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"val", types.Decimal},
				{"scale", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Decimal),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				value := args[0].(*tree.DDecimal)
				scale := int32(tree.MustBeDInt(args[1]))
				return roundDDecimal(value, scale)
			},
			Info: "This function is used internally to round decimal values during mutations.",
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"val", types.DecimalArray},
				{"scale", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.DecimalArray),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				value := args[0].(*tree.DArray)
				scale := int32(tree.MustBeDInt(args[1]))

				// Lazily allocate a new array only if/when one of its elements
				// is rounded.
				var newArr tree.Datums
				for i, elem := range value.Array {
					// Skip NULL values.
					if elem == tree.DNull {
						continue
					}

					rounded, err := roundDDecimal(elem.(*tree.DDecimal), scale)
					if err != nil {
						return nil, err
					}
					if rounded != elem {
						if newArr == nil {
							newArr = make(tree.Datums, len(value.Array))
							copy(newArr, value.Array)
						}
						newArr[i] = rounded
					}
				}
				if newArr != nil {
					ret := &tree.DArray{}
					*ret = *value
					ret.Array = newArr
					return ret, nil
				}
				return value, nil
			},
			Info: "This function is used internally to round decimal array values during mutations.",
		},
	),
}

var lengthImpls = makeBuiltin(tree.FunctionProperties{Category: categoryString},
	stringOverload1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
		return tree.NewDInt(tree.DInt(utf8.RuneCountInString(s))), nil
	}, types.Int, "Calculates the number of characters in `val`."),
	bytesOverload1(func(_ *tree.EvalContext, s string) (tree.Datum, error) {
		return tree.NewDInt(tree.DInt(len(s))), nil
	}, types.Int, "Calculates the number of bytes in `val`."),
)

var substringImpls = makeBuiltin(tree.FunctionProperties{Category: categoryString},
	tree.Overload{
		Types: tree.ArgTypes{
			{"input", types.String},
			{"start_pos", types.Int},
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
		Info: "Returns a substring of `input` starting at `start_pos` (count starts at 1).",
	},
	tree.Overload{
		Types: tree.ArgTypes{
			{"input", types.String},
			{"start_pos", types.Int},
			{"length", types.Int},
		},
		SpecializedVecBuiltin: tree.SubstringStringIntInt,
		ReturnType:            tree.FixedReturnType(types.String),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			runes := []rune(string(tree.MustBeDString(args[0])))
			// SQL strings are 1-indexed.
			start := int(tree.MustBeDInt(args[1])) - 1
			length := int(tree.MustBeDInt(args[2]))

			if length < 0 {
				return nil, pgerror.Newf(
					pgcode.InvalidParameterValue, "negative substring length %d not allowed", length)
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
		Info: "Returns a substring of `input` starting at `start_pos` (count starts at 1) and " +
			"including up to `length` characters.",
	},
	tree.Overload{
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
	tree.Overload{
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
)

var uuidV4Impl = makeBuiltin(
	tree.FunctionProperties{
		Category: categoryIDGeneration,
		Impure:   true,
	},
	tree.Overload{
		Types:      tree.ArgTypes{},
		ReturnType: tree.FixedReturnType(types.Bytes),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return tree.NewDBytes(tree.DBytes(uuid.MakeV4().GetBytes())), nil
		},
		Info: "Returns a UUID.",
	},
)

var ceilImpl = makeBuiltin(defProps(),
	floatOverload1(func(x float64) (tree.Datum, error) {
		return tree.NewDFloat(tree.DFloat(math.Ceil(x))), nil
	}, "Calculates the smallest integer greater than `val`."),
	decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
		dd := &tree.DDecimal{}
		_, err := tree.ExactCtx.Ceil(&dd.Decimal, x)
		if dd.IsZero() {
			dd.Negative = false
		}
		return dd, err
	}, "Calculates the smallest integer greater than `val`."),
)

var txnTSContextDoc = `

The value is based on a timestamp picked when the transaction starts
and which stays constant throughout the transaction. This timestamp
has no relationship with the commit order of concurrent transactions.`

var txnTSDoc = `Returns the time of the current transaction.` + txnTSContextDoc

var txnTSImpl = makeBuiltin(
	tree.FunctionProperties{
		Category: categoryDateAndTime,
		Impure:   true,
	},
	tree.Overload{
		Types:             tree.ArgTypes{},
		ReturnType:        tree.FixedReturnType(types.TimestampTZ),
		PreferredOverload: true,
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return ctx.GetTxnTimestamp(time.Microsecond), nil
		},
		Info: txnTSDoc,
	},
	tree.Overload{
		Types:      tree.ArgTypes{},
		ReturnType: tree.FixedReturnType(types.Timestamp),
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return ctx.GetTxnTimestampNoZone(time.Microsecond), nil
		},
		Info: txnTSDoc,
	},
	tree.Overload{
		Types:      tree.ArgTypes{},
		ReturnType: tree.FixedReturnType(types.Date),
		Fn:         currentDate,
		Info:       txnTSDoc,
	},
)

func currentDate(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
	t := ctx.GetTxnTimestamp(time.Microsecond).Time
	t = t.In(ctx.GetLocation())
	return tree.NewDDateFromTime(t)
}

var powImpls = makeBuiltin(defProps(),
	floatOverload2("x", "y", func(x, y float64) (tree.Datum, error) {
		return tree.NewDFloat(tree.DFloat(math.Pow(x, y))), nil
	}, "Calculates `x`^`y`."),
	decimalOverload2("x", "y", func(x, y *apd.Decimal) (tree.Datum, error) {
		dd := &tree.DDecimal{}
		_, err := tree.DecimalCtx.Pow(&dd.Decimal, x, y)
		return dd, err
	}, "Calculates `x`^`y`."),
	tree.Overload{
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
)

var (
	jsonNullDString    = tree.NewDString("null")
	jsonStringDString  = tree.NewDString("string")
	jsonNumberDString  = tree.NewDString("number")
	jsonBooleanDString = tree.NewDString("boolean")
	jsonArrayDString   = tree.NewDString("array")
	jsonObjectDString  = tree.NewDString("object")
)

var (
	errJSONObjectNotEvenNumberOfElements = pgerror.New(pgcode.InvalidParameterValue,
		"array must have even number of elements")
	errJSONObjectNullValueForKey = pgerror.New(pgcode.InvalidParameterValue,
		"null value not allowed for object key")
	errJSONObjectMismatchedArrayDim = pgerror.New(pgcode.InvalidParameterValue,
		"mismatched array dimensions")
)

var jsonExtractPathImpl = tree.Overload{
	Types:      tree.VariadicType{FixedTypes: []*types.T{types.Jsonb}, VarType: types.String},
	ReturnType: tree.FixedReturnType(types.Jsonb),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		j := tree.MustBeDJSON(args[0])
		path := make([]string, len(args)-1)
		for i, v := range args {
			if i == 0 {
				continue
			}
			if v == tree.DNull {
				return tree.DNull, nil
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

// darrayToStringSlice converts an array of string datums to a Go array of
// strings. If any of the elements are NULL, then ok will be returned as false.
func darrayToStringSlice(d tree.DArray) (result []string, ok bool) {
	result = make([]string, len(d.Array))
	for i, s := range d.Array {
		if s == tree.DNull {
			return nil, false
		}
		result[i] = string(tree.MustBeDString(s))
	}
	return result, true
}

// checkHasNulls returns an appropriate error if the array contains a NULL.
func checkHasNulls(ary tree.DArray) error {
	if ary.HasNulls {
		for i := range ary.Array {
			if ary.Array[i] == tree.DNull {
				return pgerror.Newf(pgcode.NullValueNotAllowed, "path element at position %d is null", i+1)
			}
		}
	}
	return nil
}

var jsonSetImpl = tree.Overload{
	Types: tree.ArgTypes{
		{"val", types.Jsonb},
		{"path", types.StringArray},
		{"to", types.Jsonb},
	},
	ReturnType: tree.FixedReturnType(types.Jsonb),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		return jsonDatumSet(args[0], args[1], args[2], tree.DBoolTrue)
	},
	Info: "Returns the JSON value pointed to by the variadic arguments.",
}

var jsonSetWithCreateMissingImpl = tree.Overload{
	Types: tree.ArgTypes{
		{"val", types.Jsonb},
		{"path", types.StringArray},
		{"to", types.Jsonb},
		{"create_missing", types.Bool},
	},
	ReturnType: tree.FixedReturnType(types.Jsonb),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		return jsonDatumSet(args[0], args[1], args[2], args[3])
	},
	Info: "Returns the JSON value pointed to by the variadic arguments. " +
		"If `create_missing` is false, new keys will not be inserted to objects " +
		"and values will not be prepended or appended to arrays.",
}

func jsonDatumSet(
	targetD tree.Datum, pathD tree.Datum, toD tree.Datum, createMissingD tree.Datum,
) (tree.Datum, error) {
	ary := *tree.MustBeDArray(pathD)
	// jsonb_set only errors if there is a null at the first position, but not
	// at any other positions.
	if err := checkHasNulls(ary); err != nil {
		return nil, err
	}
	path, ok := darrayToStringSlice(ary)
	if !ok {
		return targetD, nil
	}
	j, err := json.DeepSet(tree.MustBeDJSON(targetD).JSON, path, tree.MustBeDJSON(toD).JSON, bool(tree.MustBeDBool(createMissingD)))
	if err != nil {
		return nil, err
	}
	return &tree.DJSON{JSON: j}, nil
}

var jsonInsertImpl = tree.Overload{
	Types: tree.ArgTypes{
		{"target", types.Jsonb},
		{"path", types.StringArray},
		{"new_val", types.Jsonb},
	},
	ReturnType: tree.FixedReturnType(types.Jsonb),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		return insertToJSONDatum(args[0], args[1], args[2], tree.DBoolFalse)
	},
	Info: "Returns the JSON value pointed to by the variadic arguments. `new_val` will be inserted before path target.",
}

var jsonInsertWithInsertAfterImpl = tree.Overload{
	Types: tree.ArgTypes{
		{"target", types.Jsonb},
		{"path", types.StringArray},
		{"new_val", types.Jsonb},
		{"insert_after", types.Bool},
	},
	ReturnType: tree.FixedReturnType(types.Jsonb),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		return insertToJSONDatum(args[0], args[1], args[2], args[3])
	},
	Info: "Returns the JSON value pointed to by the variadic arguments. " +
		"If `insert_after` is true (default is false), `new_val` will be inserted after path target.",
}

func insertToJSONDatum(
	targetD tree.Datum, pathD tree.Datum, newValD tree.Datum, insertAfterD tree.Datum,
) (tree.Datum, error) {
	ary := *tree.MustBeDArray(pathD)

	// jsonb_insert only errors if there is a null at the first position, but not
	// at any other positions.
	if err := checkHasNulls(ary); err != nil {
		return nil, err
	}
	path, ok := darrayToStringSlice(ary)
	if !ok {
		return targetD, nil
	}
	j, err := json.DeepInsert(tree.MustBeDJSON(targetD).JSON, path, tree.MustBeDJSON(newValD).JSON, bool(tree.MustBeDBool(insertAfterD)))
	if err != nil {
		return nil, err
	}
	return &tree.DJSON{JSON: j}, nil
}

var jsonTypeOfImpl = tree.Overload{
	Types:      tree.ArgTypes{{"val", types.Jsonb}},
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
		return nil, errors.AssertionFailedf("unexpected JSON type %d", t)
	},
	Info: "Returns the type of the outermost JSON value as a text string.",
}

func jsonProps() tree.FunctionProperties {
	return tree.FunctionProperties{
		Category: categoryJSON,
	}
}

func jsonPropsNullableArgs() tree.FunctionProperties {
	d := jsonProps()
	d.NullableArgs = true
	return d
}

var jsonBuildObjectImpl = tree.Overload{
	Types:      tree.VariadicType{VarType: types.Any},
	ReturnType: tree.FixedReturnType(types.Jsonb),
	Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		if len(args)%2 != 0 {
			return nil, pgerror.New(pgcode.InvalidParameterValue,
				"argument list must have even number of elements")
		}

		builder := json.NewObjectBuilder(len(args) / 2)
		for i := 0; i < len(args); i += 2 {
			if args[i] == tree.DNull {
				return nil, pgerror.Newf(pgcode.InvalidParameterValue,
					"argument %d cannot be null", i+1)
			}

			key, err := asJSONBuildObjectKey(args[i])
			if err != nil {
				return nil, err
			}

			val, err := tree.AsJSON(args[i+1])
			if err != nil {
				return nil, err
			}

			builder.Add(key, val)
		}

		return tree.NewDJSON(builder.Build()), nil
	},
	Info: "Builds a JSON object out of a variadic argument list.",
}

var toJSONImpl = tree.Overload{
	Types:      tree.ArgTypes{{"val", types.Any}},
	ReturnType: tree.FixedReturnType(types.Jsonb),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		return toJSONObject(args[0])
	},
	Info: "Returns the value as JSON or JSONB.",
}

var prettyPrintNotSupportedError = pgerror.Newf(pgcode.FeatureNotSupported, "pretty printing is not supported")

var arrayToJSONImpls = makeBuiltin(jsonProps(),
	tree.Overload{
		Types:      tree.ArgTypes{{"array", types.AnyArray}},
		ReturnType: tree.FixedReturnType(types.Jsonb),
		Fn:         toJSONImpl.Fn,
		Info:       "Returns the array as JSON or JSONB.",
	},
	tree.Overload{
		Types:      tree.ArgTypes{{"array", types.AnyArray}, {"pretty_bool", types.Bool}},
		ReturnType: tree.FixedReturnType(types.Jsonb),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			prettyPrint := bool(tree.MustBeDBool(args[1]))
			if prettyPrint {
				return nil, prettyPrintNotSupportedError
			}
			return toJSONObject(args[0])
		},
		Info: "Returns the array as JSON or JSONB.",
	},
)

var jsonBuildArrayImpl = tree.Overload{
	Types:      tree.VariadicType{VarType: types.Any},
	ReturnType: tree.FixedReturnType(types.Jsonb),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		builder := json.NewArrayBuilder(len(args))
		for _, arg := range args {
			j, err := tree.AsJSON(arg)
			if err != nil {
				return nil, err
			}
			builder.Add(j)
		}
		return tree.NewDJSON(builder.Build()), nil
	},
	Info: "Builds a possibly-heterogeneously-typed JSON or JSONB array out of a variadic argument list.",
}

var jsonObjectImpls = makeBuiltin(jsonProps(),
	tree.Overload{
		Types:      tree.ArgTypes{{"texts", types.StringArray}},
		ReturnType: tree.FixedReturnType(types.Jsonb),
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
				val, err := tree.AsJSON(arr.Array[i+1])
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
	tree.Overload{
		Types: tree.ArgTypes{{"keys", types.StringArray},
			{"values", types.StringArray}},
		ReturnType: tree.FixedReturnType(types.Jsonb),
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
				val, err := tree.AsJSON(values.Array[i])
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
)

var jsonStripNullsImpl = tree.Overload{
	Types:      tree.ArgTypes{{"from_json", types.Jsonb}},
	ReturnType: tree.FixedReturnType(types.Jsonb),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		j, _, err := tree.MustBeDJSON(args[0]).StripNulls()
		return tree.NewDJSON(j), err
	},
	Info: "Returns from_json with all object fields that have null values omitted. Other null values are untouched.",
}

var jsonArrayLengthImpl = tree.Overload{
	Types:      tree.ArgTypes{{"json", types.Jsonb}},
	ReturnType: tree.FixedReturnType(types.Int),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		j := tree.MustBeDJSON(args[0])
		switch j.Type() {
		case json.ArrayJSONType:
			return tree.NewDInt(tree.DInt(j.Len())), nil
		case json.ObjectJSONType:
			return nil, pgerror.New(pgcode.InvalidParameterValue,
				"cannot get array length of a non-array")
		default:
			return nil, pgerror.New(pgcode.InvalidParameterValue,
				"cannot get array length of a scalar")
		}
	},
	Info: "Returns the number of elements in the outermost JSON or JSONB array.",
}

func arrayBuiltin(impl func(*types.T) tree.Overload) builtinDefinition {
	overloads := make([]tree.Overload, 0, len(types.Scalar))
	for _, typ := range types.Scalar {
		if ok, _ := types.IsValidArrayElementType(typ); ok {
			overloads = append(overloads, impl(typ))
		}
	}
	return builtinDefinition{
		props:     tree.FunctionProperties{Category: categoryArray},
		overloads: overloads,
	}
}

func setProps(props tree.FunctionProperties, d builtinDefinition) builtinDefinition {
	d.props = props
	return d
}

func decimalLogFn(
	logFn func(*apd.Decimal, *apd.Decimal) (apd.Condition, error), info string,
) tree.Overload {
	return decimalOverload1(func(x *apd.Decimal) (tree.Datum, error) {
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

func floatOverload1(f func(float64) (tree.Datum, error), info string) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{"val", types.Float}},
		ReturnType: tree.FixedReturnType(types.Float),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(float64(*args[0].(*tree.DFloat)))
		},
		Info: info,
	}
}

func floatOverload2(
	a, b string, f func(float64, float64) (tree.Datum, error), info string,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{a, types.Float}, {b, types.Float}},
		ReturnType: tree.FixedReturnType(types.Float),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(float64(*args[0].(*tree.DFloat)),
				float64(*args[1].(*tree.DFloat)))
		},
		Info: info,
	}
}

func decimalOverload1(f func(*apd.Decimal) (tree.Datum, error), info string) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{"val", types.Decimal}},
		ReturnType: tree.FixedReturnType(types.Decimal),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			dec := &args[0].(*tree.DDecimal).Decimal
			return f(dec)
		},
		Info: info,
	}
}

func decimalOverload2(
	a, b string, f func(*apd.Decimal, *apd.Decimal) (tree.Datum, error), info string,
) tree.Overload {
	return tree.Overload{
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

func stringOverload1(
	f func(*tree.EvalContext, string) (tree.Datum, error), returnType *types.T, info string,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{"val", types.String}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(evalCtx, string(tree.MustBeDString(args[0])))
		},
		Info: info,
	}
}

func stringOverload2(
	a, b string,
	f func(*tree.EvalContext, string, string) (tree.Datum, error),
	returnType *types.T,
	info string,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{a, types.String}, {b, types.String}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(evalCtx, string(tree.MustBeDString(args[0])), string(tree.MustBeDString(args[1])))
		},
		Info: info,
	}
}

func stringOverload3(
	a, b, c string,
	f func(*tree.EvalContext, string, string, string) (tree.Datum, error),
	returnType *types.T,
	info string,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{a, types.String}, {b, types.String}, {c, types.String}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(evalCtx, string(tree.MustBeDString(args[0])), string(tree.MustBeDString(args[1])), string(tree.MustBeDString(args[2])))
		},
		Info: info,
	}
}

func bytesOverload1(
	f func(*tree.EvalContext, string) (tree.Datum, error), returnType *types.T, info string,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{"val", types.Bytes}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(evalCtx, string(*args[0].(*tree.DBytes)))
		},
		Info: info,
	}
}

// feedHash returns true if it encounters any non-Null datum.
func feedHash(h hash.Hash, args tree.Datums) (bool, error) {
	var nonNullSeen bool
	for _, datum := range args {
		if datum == tree.DNull {
			continue
		} else {
			nonNullSeen = true
		}
		var buf string
		if d, ok := datum.(*tree.DBytes); ok {
			buf = string(*d)
		} else {
			buf = string(tree.MustBeDString(datum))
		}
		_, err := h.Write([]byte(buf))
		if err != nil {
			return false, errors.NewAssertionErrorWithWrappedErrf(err,
				`"It never returns an error." -- https://golang.org/pkg/hash: %T`, h)
		}
	}
	return nonNullSeen, nil
}

func hashBuiltin(newHash func() hash.Hash, info string) builtinDefinition {
	return makeBuiltin(tree.FunctionProperties{NullableArgs: true},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.String},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				h := newHash()
				if ok, err := feedHash(h, args); !ok || err != nil {
					return tree.DNull, err
				}
				return tree.NewDString(fmt.Sprintf("%x", h.Sum(nil))), nil
			},
			Info: info,
		},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.Bytes},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				h := newHash()
				if ok, err := feedHash(h, args); !ok || err != nil {
					return tree.DNull, err
				}
				return tree.NewDString(fmt.Sprintf("%x", h.Sum(nil))), nil
			},
			Info: info,
		},
	)
}

func hash32Builtin(newHash func() hash.Hash32, info string) builtinDefinition {
	return makeBuiltin(tree.FunctionProperties{NullableArgs: true},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.String},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				h := newHash()
				if ok, err := feedHash(h, args); !ok || err != nil {
					return tree.DNull, err
				}
				return tree.NewDInt(tree.DInt(h.Sum32())), nil
			},
			Info: info,
		},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.Bytes},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				h := newHash()
				if ok, err := feedHash(h, args); !ok || err != nil {
					return tree.DNull, err
				}
				return tree.NewDInt(tree.DInt(h.Sum32())), nil
			},
			Info: info,
		},
	)
}

func hash64Builtin(newHash func() hash.Hash64, info string) builtinDefinition {
	return makeBuiltin(tree.FunctionProperties{NullableArgs: true},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.String},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				h := newHash()
				if ok, err := feedHash(h, args); !ok || err != nil {
					return tree.DNull, err
				}
				return tree.NewDInt(tree.DInt(h.Sum64())), nil
			},
			Info: info,
		},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.Bytes},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				h := newHash()
				if ok, err := feedHash(h, args); !ok || err != nil {
					return tree.DNull, err
				}
				return tree.NewDInt(tree.DInt(h.Sum64())), nil
			},
			Info: info,
		},
	)
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
			return "", pgerror.Newf(
				pgcode.InvalidRegularExpression, "invalid regexp flag: %q", sqlFlag)
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
		return nil, pgerror.Newf(
			pgcode.InvalidParameterValue, "non-positive substring length not allowed: %d", pos)
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

// roundDDecimal avoids creation of a new DDecimal in common case where no
// rounding is necessary.
func roundDDecimal(d *tree.DDecimal, scale int32) (tree.Datum, error) {
	// Fast path: check if number of digits after decimal point is already low
	// enough.
	if -d.Exponent <= scale {
		return d, nil
	}
	return roundDecimal(&d.Decimal, scale)
}

func roundDecimal(x *apd.Decimal, scale int32) (tree.Datum, error) {
	dd := &tree.DDecimal{}
	_, err := tree.HighPrecisionCtx.Quantize(&dd.Decimal, x, -scale)
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
		return tree.NewDInt(tree.DInt((t.Second() * 1000) + (t.Microsecond() / microsPerMilli))), nil
	case "microsecond", "microseconds":
		return tree.NewDInt(tree.DInt((t.Second() * 1000 * 1000) + t.Microsecond())), nil
	case "epoch":
		seconds := time.Duration(t) * time.Microsecond / time.Second
		return tree.NewDInt(tree.DInt(int64(seconds))), nil
	default:
		return nil, pgerror.Newf(
			pgcode.InvalidParameterValue, "unsupported timespan: %s", timeSpan)
	}
}

// dateToJulianDay is based on the date2j function in PostgreSQL 10.5.
func dateToJulianDay(year int, month int, day int) int {
	if month > 2 {
		month++
		year += 4800
	} else {
		month += 13
		year += 4799
	}

	century := year / 100
	jd := year*365 - 32167
	jd += year/4 - century + century/4
	jd += 7834*month/256 + day

	return jd
}

func extractStringFromTimestamp(
	_ *tree.EvalContext, fromTime time.Time, timeSpan string,
) (tree.Datum, error) {
	switch timeSpan {
	case "millennia", "millennium", "millenniums":
		year := fromTime.Year()
		if year > 0 {
			return tree.NewDInt(tree.DInt((year + 999) / 1000)), nil
		}
		return tree.NewDInt(tree.DInt(-((999 - (year - 1)) / 1000))), nil

	case "centuries", "century":
		year := fromTime.Year()
		if year > 0 {
			return tree.NewDInt(tree.DInt((year + 99) / 100)), nil
		}
		return tree.NewDInt(tree.DInt(-((99 - (year - 1)) / 100))), nil

	case "decade", "decades":
		year := fromTime.Year()
		if year >= 0 {
			return tree.NewDInt(tree.DInt(year / 10)), nil
		}
		return tree.NewDInt(tree.DInt(-((8 - (year - 1)) / 10))), nil

	case "year", "years":
		return tree.NewDInt(tree.DInt(fromTime.Year())), nil

	case "isoyear":
		year, _ := fromTime.ISOWeek()
		return tree.NewDInt(tree.DInt(year)), nil

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

	case "isodow":
		day := fromTime.Weekday()
		if day == 0 {
			return tree.NewDInt(tree.DInt(7)), nil
		}
		return tree.NewDInt(tree.DInt(day)), nil

	case "dayofyear", "doy":
		return tree.NewDInt(tree.DInt(fromTime.YearDay())), nil

	case "julian":
		julianDay := dateToJulianDay(fromTime.Year(), int(fromTime.Month()), fromTime.Day())
		return tree.NewDInt(tree.DInt(julianDay)), nil

	case "hour", "hours":
		return tree.NewDInt(tree.DInt(fromTime.Hour())), nil

	case "minute", "minutes":
		return tree.NewDInt(tree.DInt(fromTime.Minute())), nil

	case "second", "seconds":
		return tree.NewDInt(tree.DInt(fromTime.Second())), nil

	case "millisecond", "milliseconds":
		// This a PG extension not supported in MySQL.
		return tree.NewDInt(tree.DInt((fromTime.Second() * 1000) + (fromTime.Nanosecond() / int(time.Millisecond)))), nil

	case "microsecond", "microseconds":
		return tree.NewDInt(tree.DInt((fromTime.Second() * 1000 * 1000) + (fromTime.Nanosecond() / int(time.Microsecond)))), nil

	case "epoch":
		return tree.NewDInt(tree.DInt(fromTime.Unix())), nil

	default:
		return nil, pgerror.Newf(pgcode.InvalidParameterValue, "unsupported timespan: %s", timeSpan)
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
		return nil, pgerror.Newf(pgcode.InvalidParameterValue, "unsupported timespan: %s", timeSpan)
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

// arrayToString implements the array_to_string builtin - arr is joined using
// delim. If nullStr is non-nil, NULL values in the array will be replaced by
// it.
func arrayToString(arr *tree.DArray, delim string, nullStr *string) (tree.Datum, error) {
	f := tree.NewFmtCtx(tree.FmtArrayToString)

	for i := range arr.Array {
		if arr.Array[i] == tree.DNull {
			if nullStr == nil {
				continue
			}
			f.WriteString(*nullStr)
		} else {
			f.FormatNode(arr.Array[i])
		}
		if i < len(arr.Array)-1 {
			f.WriteString(delim)
		}
	}
	return tree.NewDString(f.CloseAndGetString()), nil
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

var errInvalidSyntaxForDecode = pgerror.New(pgcode.InvalidParameterValue, "invalid syntax for decode(..., 'escape')")

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
	case "millennia", "millennium", "millenniums":
		if year > 0 {
			year = ((year+999)/1000)*1000 - 999
		} else {
			year = -((999-(year-1))/1000)*1000 + 1
		}
		month, day, hour, min, sec, nsec = monthTrunc, dayTrunc, hourTrunc, minTrunc, secTrunc, nsecTrunc

	case "centuries", "century":
		if year > 0 {
			year = ((year+99)/100)*100 - 99
		} else {
			year = -((99-(year-1))/100)*100 + 1
		}
		month, day, hour, min, sec, nsec = monthTrunc, dayTrunc, hourTrunc, minTrunc, secTrunc, nsecTrunc

	case "decade", "decades":
		if year >= 0 {
			year = (year / 10) * 10
		} else {
			year = -((8 - (year - 1)) / 10) * 10
		}
		month, day, hour, min, sec, nsec = monthTrunc, dayTrunc, hourTrunc, minTrunc, secTrunc, nsecTrunc

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
		return nil, pgerror.Newf(pgcode.InvalidParameterValue, "unsupported timespan: %s", timeSpan)
	}

	toTime := time.Date(year, month, day, hour, min, sec, nsec, loc)
	return tree.MakeDTimestampTZ(toTime, time.Microsecond), nil
}

// Converts a scalar Datum to its string representation
func asJSONBuildObjectKey(d tree.Datum) (string, error) {
	switch t := d.(type) {
	case *tree.DJSON, *tree.DArray, *tree.DTuple:
		return "", pgerror.New(pgcode.InvalidParameterValue,
			"key value must be scalar, not array, tuple, or json")
	case *tree.DString:
		return string(*t), nil
	case *tree.DCollatedString:
		return t.Contents, nil
	case *tree.DBool, *tree.DInt, *tree.DFloat, *tree.DDecimal, *tree.DTimestamp, *tree.DTimestampTZ,
		*tree.DDate, *tree.DUuid, *tree.DInterval, *tree.DBytes, *tree.DIPAddr, *tree.DOid,
		*tree.DTime, *tree.DBitArray:
		return tree.AsStringWithFlags(d, tree.FmtBareStrings), nil
	default:
		return "", errors.AssertionFailedf("unexpected type %T for key value", d)
	}
}

func asJSONObjectKey(d tree.Datum) (string, error) {
	switch t := d.(type) {
	case *tree.DString:
		return string(*t), nil
	default:
		return "", errors.AssertionFailedf("unexpected type %T for asJSONObjectKey", d)
	}
}

func toJSONObject(d tree.Datum) (tree.Datum, error) {
	j, err := tree.AsJSON(d)
	if err != nil {
		return nil, err
	}
	return tree.NewDJSON(j), nil
}

// padMaybeTruncate truncates the input string to length if the string is
// longer or equal in size to length. If truncated, the first return value
// will be true, and the last return value will be the truncated string.
// The second return value is set to the length of the input string in runes.
func padMaybeTruncate(s string, length int, fill string) (ok bool, slen int, ret string) {
	if length < 0 {
		// lpad and rpad both return an empty string if the input length is
		// negative.
		length = 0
	}
	slen = utf8.RuneCountInString(s)
	if length == slen {
		return true, slen, s
	}

	// If string is longer then length truncate it to the requested number
	// of characters.
	if length < slen {
		return true, slen, string([]rune(s)[:length])
	}

	// If the input fill is the empty string, return the original string.
	if len(fill) == 0 {
		return true, slen, s
	}

	return false, slen, s
}

func lpad(s string, length int, fill string) (string, error) {
	if length > maxAllocatedStringSize {
		return "", errStringTooLarge
	}
	ok, slen, ret := padMaybeTruncate(s, length, fill)
	if ok {
		return ret, nil
	}
	var buf strings.Builder
	fillRunes := []rune(fill)
	for i := 0; i < length-slen; i++ {
		buf.WriteRune(fillRunes[i%len(fillRunes)])
	}
	buf.WriteString(s)

	return buf.String(), nil
}

func rpad(s string, length int, fill string) (string, error) {
	if length > maxAllocatedStringSize {
		return "", errStringTooLarge
	}
	ok, slen, ret := padMaybeTruncate(s, length, fill)
	if ok {
		return ret, nil
	}
	var buf strings.Builder
	buf.WriteString(s)
	fillRunes := []rune(fill)
	for i := 0; i < length-slen; i++ {
		buf.WriteRune(fillRunes[i%len(fillRunes)])
	}

	return buf.String(), nil
}

// widthBucket returns the bucket number to which operand would be assigned in a histogram having count
// equal-width buckets spanning the range b1 to b2
func widthBucket(operand float64, b1 float64, b2 float64, count int) int {
	bucket := 0
	if (b1 < b2 && operand > b2) || (b1 > b2 && operand < b2) {
		return count + 1
	}

	if (b1 < b2 && operand < b1) || (b1 > b2 && operand > b1) {
		return 0
	}

	width := (b2 - b1) / float64(count)
	difference := operand - b1
	bucket = int(math.Floor(difference/width) + 1)

	return bucket
}

// CleanEncodingName sanitizes the string meant to represent a
// recognized encoding. This ignores any non-alphanumeric character.
//
// See function clean_encoding_name() in postgres' sources
// in backend/utils/mb/encnames.c.
func CleanEncodingName(s string) string {
	b := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			b = append(b, c-'A'+'a')
		} else if (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') {
			b = append(b, c)
		}
	}
	return string(b)
}

var errInsufficientPriv = pgerror.New(
	pgcode.InsufficientPrivilege, "insufficient privilege",
)

func checkPrivilegedUser(ctx *tree.EvalContext) error {
	if ctx.SessionData.User != security.RootUser {
		return errInsufficientPriv
	}
	return nil
}

// EvalFollowerReadOffset is a function used often with AS OF SYSTEM TIME queries
// to determine the appropriate offset from now which is likely to be safe for
// follower reads. It is injected by followerreadsccl. An error may be returned
// if an enterprise license is not installed.
var EvalFollowerReadOffset func(clusterID uuid.UUID, _ *cluster.Settings) (time.Duration, error)

func recentTimestamp(ctx *tree.EvalContext) (time.Time, error) {
	if EvalFollowerReadOffset == nil {
		return time.Time{}, pgerror.New(pgcode.FeatureNotSupported,
			tree.FollowerReadTimestampFunctionName+
				" is only available in ccl distribution")
	}
	offset, err := EvalFollowerReadOffset(ctx.ClusterID, ctx.Settings)
	if err != nil {
		return time.Time{}, err
	}
	return ctx.StmtTimestamp.Add(offset), nil
}
