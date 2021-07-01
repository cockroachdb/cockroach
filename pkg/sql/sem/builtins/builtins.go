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
	cryptorand "crypto/rand"
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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/sql/roleoption"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/streaming"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/fuzzystrmatch"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timetz"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/ulid"
	"github.com/cockroachdb/cockroach/pkg/util/unaccent"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/knz/strtime"
)

var (
	errEmptyInputString = pgerror.New(pgcode.InvalidParameterValue, "the input string must not be empty")
	errZeroIP           = pgerror.New(pgcode.InvalidParameterValue, "zero length IP")
	errChrValueTooSmall = pgerror.New(pgcode.InvalidParameterValue, "input value must be >= 0")
	errChrValueTooLarge = pgerror.Newf(pgcode.InvalidParameterValue,
		"input value must be <= %d (maximum Unicode code point)", utf8.MaxRune)
	errStringTooLarge = pgerror.Newf(pgcode.ProgramLimitExceeded,
		"requested length too large, exceeds %s", humanizeutil.IBytes(maxAllocatedStringSize))
	errInvalidNull = pgerror.New(pgcode.InvalidParameterValue, "input cannot be NULL")
	// SequenceNameArg represents the name of sequence (string) arguments in
	// builtin functions.
	SequenceNameArg = "sequence_name"
)

const defaultFollowerReadDuration = -4800 * time.Millisecond

const maxAllocatedStringSize = 128 * 1024 * 1024

const errInsufficientArgsFmtString = "unknown signature: %s()"

const (
	categoryArray               = "Array"
	categoryComparison          = "Comparison"
	categoryCompatibility       = "Compatibility"
	categoryDateAndTime         = "Date and time"
	categoryEnum                = "Enum"
	categoryFullTextSearch      = "Full Text Search"
	categoryGenerator           = "Set-returning"
	categoryTrigram             = "Trigrams"
	categoryFuzzyStringMatching = "Fuzzy String Matching"
	categoryIDGeneration        = "ID generation"
	categoryJSON                = "JSONB"
	categoryMultiRegion         = "Multi-region"
	categoryMultiTenancy        = "Multi-tenancy"
	categorySequences           = "Sequence"
	categorySpatial             = "Spatial"
	categoryString              = "String and byte"
	categorySystemInfo          = "System info"
	categorySystemRepair        = "System repair"
	categoryStreamIngestion     = "Stream Ingestion"
)

func categorizeType(t *types.T) string {
	switch t.Family() {
	case types.DateFamily, types.IntervalFamily, types.TimestampFamily, types.TimestampTZFamily:
		return categoryDateAndTime
	case types.StringFamily, types.BytesFamily:
		return categoryString
	default:
		return strings.ToUpper(t.String())
	}
}

const (
	// GatewayRegionBuiltinName is the name for the builtin that returns the gateway
	// region of the current node.
	GatewayRegionBuiltinName = "gateway_region"
	// DefaultToDatabasePrimaryRegionBuiltinName is the name for the builtin that
	// takes in a region and returns it if it is a valid region on the database.
	// Otherwise, it returns the primary region.
	DefaultToDatabasePrimaryRegionBuiltinName = "default_to_database_primary_region"
)

var digitNames = [...]string{"zero", "one", "two", "three", "four", "five", "six", "seven", "eight", "nine"}

const regexpFlagInfo = `

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
| m/n  | no                               | yes                                  |`

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
	"length":           lengthImpls(true /* includeBitOverload */),
	"char_length":      lengthImpls(false /* includeBitOverload */),
	"character_length": lengthImpls(false /* includeBitOverload */),

	"bit_length": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		stringOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				return tree.NewDInt(tree.DInt(len(s) * 8)), nil
			},
			types.Int,
			"Calculates the number of bits used to represent `val`.",
			tree.VolatilityImmutable,
		),
		bytesOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				return tree.NewDInt(tree.DInt(len(s) * 8)), nil
			},
			types.Int,
			"Calculates the number of bits used to represent `val`.",
			tree.VolatilityImmutable,
		),
		bitsOverload1(
			func(_ *tree.EvalContext, s *tree.DBitArray) (tree.Datum, error) {
				return tree.NewDInt(tree.DInt(s.BitArray.BitLen())), nil
			},
			types.Int,
			"Calculates the number of bits used to represent `val`.",
			tree.VolatilityImmutable,
		),
	),

	"octet_length": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		stringOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				return tree.NewDInt(tree.DInt(len(s))), nil
			},
			types.Int,
			"Calculates the number of bytes used to represent `val`.",
			tree.VolatilityImmutable,
		),
		bytesOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				return tree.NewDInt(tree.DInt(len(s))), nil
			},
			types.Int,
			"Calculates the number of bytes used to represent `val`.",
			tree.VolatilityImmutable,
		),
		bitsOverload1(
			func(_ *tree.EvalContext, s *tree.DBitArray) (tree.Datum, error) {
				return tree.NewDInt(tree.DInt((s.BitArray.BitLen() + 7) / 8)), nil
			},
			types.Int,
			"Calculates the number of bits used to represent `val`.",
			tree.VolatilityImmutable,
		),
	),

	// TODO(pmattis): What string functions should also support types.Bytes?

	"lower": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		stringOverload1(
			func(evalCtx *tree.EvalContext, s string) (tree.Datum, error) {
				return tree.NewDString(strings.ToLower(s)), nil
			},
			types.String,
			"Converts all characters in `val` to their lower-case equivalents.",
			tree.VolatilityImmutable,
		),
	),

	"unaccent": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		stringOverload1(
			func(evalCtx *tree.EvalContext, s string) (tree.Datum, error) {
				var b strings.Builder
				for _, ch := range s {
					v, ok := unaccent.Dictionary[ch]
					if ok {
						b.WriteString(v)
					} else {
						b.WriteRune(ch)
					}
				}
				return tree.NewDString(b.String()), nil
			},
			types.String,
			"Removes accents (diacritic signs) from the text provided in `val`.",
			tree.VolatilityImmutable,
		),
	),

	"upper": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		stringOverload1(
			func(evalCtx *tree.EvalContext, s string) (tree.Datum, error) {
				return tree.NewDString(strings.ToUpper(s)), nil
			},
			types.String,
			"Converts all characters in `val` to their to their upper-case equivalents.",
			tree.VolatilityImmutable,
		),
	),

	"substr":    substringImpls,
	"substring": substringImpls,

	// concat concatenates the text representations of all the arguments.
	// NULL arguments are ignored.
	"concat": makeBuiltin(
		tree.FunctionProperties{
			NullableArgs: true,
		},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.String},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
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
			Info:       "Concatenates a comma-separated list of strings.",
			Volatility: tree.VolatilityImmutable,
			// In Postgres concat can take any arguments, converting them to
			// their text representation. Since the text representation can
			// depend on the context (e.g. timezone), the function is Stable. In
			// our case, we only take String inputs so our version is ImmutableCopy.
			IgnoreVolatilityCheck: true,
		},
	),

	"concat_ws": makeBuiltin(
		tree.FunctionProperties{
			NullableArgs: true,
		},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.String},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
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
			Volatility: tree.VolatilityImmutable,
			// In Postgres concat_ws can take any arguments, converting them to
			// their text representation. Since the text representation can
			// depend on the context (e.g. timezone), the function is Stable. In
			// our case, we only take String inputs so our version is ImmutableCopy.
			IgnoreVolatilityCheck: true,
		},
	),

	// https://www.postgresql.org/docs/10/static/functions-string.html#FUNCTIONS-STRING-OTHER
	"convert_from": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{"str", types.Bytes}, {"enc", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
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
			Volatility:            tree.VolatilityImmutable,
			IgnoreVolatilityCheck: true,
		}),

	// https://www.postgresql.org/docs/10/static/functions-string.html#FUNCTIONS-STRING-OTHER
	"convert_to": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{"str", types.String}, {"enc", types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
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
			Volatility:            tree.VolatilityImmutable,
			IgnoreVolatilityCheck: true,
		}),

	// https://www.postgresql.org/docs/9.0/functions-binarystring.html#FUNCTIONS-BINARYSTRING-OTHER
	"get_bit": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{"bit_string", types.VarBit}, {"index", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				bitString := tree.MustBeDBitArray(args[0])
				index := int(tree.MustBeDInt(args[1]))
				bit, err := bitString.GetBitAtIndex(index)
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(bit)), nil
			},
			Info:       "Extracts a bit at given index in the bit array.",
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"byte_string", types.Bytes}, {"index", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				byteString := []byte(*args[0].(*tree.DBytes))
				index := int(tree.MustBeDInt(args[1]))
				// Check whether index asked is inside ByteArray.
				if index < 0 || index >= 8*len(byteString) {
					return nil, pgerror.Newf(pgcode.ArraySubscript,
						"bit index %d out of valid range (0..%d)", index, 8*len(byteString)-1)
				}
				// To extract a bit at the given index, we have to determine the
				// position within byte array, i.e. index/8 after that checked
				// the bit at residual index.
				if byteString[index/8]&(byte(1)<<(byte(index)%8)) != 0 {
					return tree.NewDInt(tree.DInt(1)), nil
				}
				return tree.NewDInt(tree.DInt(0)), nil
			},
			Info:       "Extracts a bit at the given index in the byte array.",
			Volatility: tree.VolatilityImmutable,
		}),

	// https://www.postgresql.org/docs/9.0/functions-binarystring.html#FUNCTIONS-BINARYSTRING-OTHER
	"get_byte": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{"byte_string", types.Bytes}, {"index", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				byteString := []byte(*args[0].(*tree.DBytes))
				index := int(tree.MustBeDInt(args[1]))
				// Check whether index asked is inside ByteArray.
				if index < 0 || index >= len(byteString) {
					return nil, pgerror.Newf(pgcode.ArraySubscript,
						"byte index %d out of valid range (0..%d)", index, len(byteString)-1)
				}
				return tree.NewDInt(tree.DInt(byteString[index])), nil
			},
			Info:       "Extracts a byte at the given index in the byte array.",
			Volatility: tree.VolatilityImmutable,
		}),

	// https://www.postgresql.org/docs/9.0/functions-binarystring.html#FUNCTIONS-BINARYSTRING-OTHER
	"set_bit": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types: tree.ArgTypes{
				{"bit_string", types.VarBit},
				{"index", types.Int},
				{"to_set", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.VarBit),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				bitString := tree.MustBeDBitArray(args[0])
				index := int(tree.MustBeDInt(args[1]))
				toSet := int(tree.MustBeDInt(args[2]))

				// Value of bit can only be set to 1 or 0.
				if toSet != 0 && toSet != 1 {
					return nil, pgerror.Newf(pgcode.InvalidParameterValue,
						"new bit must be 0 or 1.")
				}
				updatedBitString, err := bitString.SetBitAtIndex(index, toSet)
				if err != nil {
					return nil, err
				}
				return &tree.DBitArray{BitArray: updatedBitString}, nil
			},
			Info:       "Updates a bit at given index in the bit array.",
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"byte_string", types.Bytes},
				{"index", types.Int},
				{"to_set", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				byteString := []byte(*args[0].(*tree.DBytes))
				index := int(tree.MustBeDInt(args[1]))
				toSet := int(tree.MustBeDInt(args[2]))
				// Value of bit can only be set to 1 or 0.
				if toSet != 0 && toSet != 1 {
					return nil, pgerror.Newf(pgcode.InvalidParameterValue,
						"new bit must be 0 or 1")
				}
				// Check whether index asked is inside ByteArray.
				if index < 0 || index >= 8*len(byteString) {
					return nil, pgerror.Newf(pgcode.ArraySubscript,
						"bit index %d out of valid range (0..%d)", index, 8*len(byteString)-1)
				}
				// To update a bit at the given index, we have to determine the
				// position within byte array, i.e. index/8 after that checked
				// the bit at residual index.
				// Forcefully making bit at the index to 0.
				byteString[index/8] &= ^(byte(1) << (byte(index) % 8))
				// Updating value at the index to toSet.
				byteString[index/8] |= byte(toSet) << (byte(index) % 8)
				return tree.NewDBytes(tree.DBytes(byteString)), nil
			},
			Info:       "Updates a bit at the given index in the byte array.",
			Volatility: tree.VolatilityImmutable,
		}),

	// https://www.postgresql.org/docs/9.0/functions-binarystring.html#FUNCTIONS-BINARYSTRING-OTHER
	"set_byte": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types: tree.ArgTypes{
				{"byte_string", types.Bytes},
				{"index", types.Int},
				{"to_set", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				byteString := []byte(*args[0].(*tree.DBytes))
				index := int(tree.MustBeDInt(args[1]))
				toSet := int(tree.MustBeDInt(args[2]))
				// Check whether index asked is inside ByteArray.
				if index < 0 || index >= len(byteString) {
					return nil, pgerror.Newf(pgcode.ArraySubscript,
						"byte index %d out of valid range (0..%d)", index, len(byteString)-1)
				}
				byteString[index] = byte(toSet)
				return tree.NewDBytes(tree.DBytes(byteString)), nil
			},
			Info:       "Updates a byte at the given index in the byte array.",
			Volatility: tree.VolatilityImmutable,
		}),

	"gen_random_uuid":  generateRandomUUIDImpl,
	"uuid_generate_v4": generateRandomUUIDImpl,

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
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
		},
	),

	"gen_random_ulid": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryIDGeneration,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Uuid),
			Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				entropy := ulid.Monotonic(cryptorand.Reader, 0)
				uv := ulid.MustNew(ulid.Now(), entropy)
				return tree.NewDUuid(tree.DUuid{UUID: uuid.UUID(uv)}), nil
			},
			Info:       "Generates a random ULID and returns it as a value of UUID type.",
			Volatility: tree.VolatilityVolatile,
		},
	),

	"uuid_to_ulid": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.Uuid}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				b := (*args[0].(*tree.DUuid)).GetBytes()
				var ul ulid.ULID
				if err := ul.UnmarshalBinary(b); err != nil {
					return nil, err
				}
				return tree.NewDString(ul.String()), nil
			},
			Info:       "Converts a UUID-encoded ULID to its string representation.",
			Volatility: tree.VolatilityImmutable,
		},
	),

	"ulid_to_uuid": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.String}},
			ReturnType: tree.FixedReturnType(types.Uuid),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := tree.MustBeDString(args[0])
				u, err := ulid.Parse(string(s))
				if err != nil {
					return nil, err
				}
				b, err := u.MarshalBinary()
				if err != nil {
					return nil, err
				}
				uv, err := uuid.FromBytes(b)
				if err != nil {
					return nil, err
				}
				return tree.NewDUuid(tree.DUuid{UUID: uv}), nil
			},
			Info:       "Converts a ULID string to its UUID-encoded representation.",
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
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
			Info:       "Converts the IP address and prefix length to text.",
			Volatility: tree.VolatilityImmutable,
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
			Info:       "Checks if two IP addresses are of the same IP family.",
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
		},
	),

	// https://www.postgresql.org/docs/10/static/functions-binarystring.html
	"encode": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"data", types.Bytes}, {"format", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (_ tree.Datum, err error) {
				data, format := *args[0].(*tree.DBytes), string(tree.MustBeDString(args[1]))
				be, ok := sessiondatapb.BytesEncodeFormatFromString(format)
				if !ok {
					return nil, pgerror.New(pgcode.InvalidParameterValue,
						"only 'hex', 'escape', and 'base64' formats are supported for encode()")
				}
				return tree.NewDString(lex.EncodeByteArrayToRawBytes(
					string(data), be, true /* skipHexPrefix */)), nil
			},
			Info:       "Encodes `data` using `format` (`hex` / `escape` / `base64`).",
			Volatility: tree.VolatilityImmutable,
		},
	),

	"decode": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"text", types.String}, {"format", types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (_ tree.Datum, err error) {
				data, format := string(tree.MustBeDString(args[0])), string(tree.MustBeDString(args[1]))
				be, ok := sessiondatapb.BytesEncodeFormatFromString(format)
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
			Info:       "Decodes `data` using `format` (`hex` / `escape` / `base64`).",
			Volatility: tree.VolatilityImmutable,
		},
	),

	"ascii": makeBuiltin(tree.FunctionProperties{Category: categoryString},
		stringOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				for _, ch := range s {
					return tree.NewDInt(tree.DInt(ch)), nil
				}
				return nil, errEmptyInputString
			},
			types.Int,
			"Returns the character code of the first character in `val`. Despite the name, the function supports Unicode too.",
			tree.VolatilityImmutable,
		)),

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
			Info:       "Returns the character with the code given in `val`. Inverse function of `ascii()`.",
			Volatility: tree.VolatilityImmutable,
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

	"sha224": hashBuiltin(
		func() hash.Hash { return sha256.New224() },
		"Calculates the SHA224 hash value of a set of values.",
	),

	"sha256": hashBuiltin(
		func() hash.Hash { return sha256.New() },
		"Calculates the SHA256 hash value of a set of values.",
	),

	"sha384": hashBuiltin(
		func() hash.Hash { return sha512.New384() },
		"Calculates the SHA384 hash value of a set of values.",
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
				val := tree.MustBeDInt(args[0])
				// This should technically match the precision of the types entered
				// into the function, e.g. `-1 :: int4` should use uint32 for correctness.
				// However, we don't encode that information for function resolution.
				// As such, always assume bigint / uint64.
				return tree.NewDString(fmt.Sprintf("%x", uint64(val))), nil
			},
			Info:       "Converts `val` to its hexadecimal representation.",
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.Bytes}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(fmt.Sprintf("%x", tree.MustBeDBytes(args[0]))), nil
			},
			Info:       "Converts `val` to its hexadecimal representation.",
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(fmt.Sprintf("%x", tree.MustBeDString(args[0]))), nil
			},
			Info:       "Converts `val` to its hexadecimal representation.",
			Volatility: tree.VolatilityImmutable,
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
				var digits []string
				if val < 0 {
					buf.WriteString("minus-")
					if val == math.MinInt64 {
						// Converting MinInt64 to positive overflows the value.
						// Take the first digit pre-emptively.
						digits = append(digits, digitNames[8])
						val /= 10
					}
					val = -val
				}
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
			Info:       "This function enunciates the value of its argument using English cardinals.",
			Volatility: tree.VolatilityImmutable,
		},
	),

	// The SQL parser coerces POSITION to STRPOS.
	"strpos": makeBuiltin(
		tree.FunctionProperties{Category: categoryString},
		stringOverload2(
			"input",
			"find",
			func(_ *tree.EvalContext, s, substring string) (tree.Datum, error) {
				index := strings.Index(s, substring)
				if index < 0 {
					return tree.DZero, nil
				}

				return tree.NewDInt(tree.DInt(utf8.RuneCountInString(s[:index]) + 1)), nil
			},
			types.Int,
			"Calculates the position where the string `find` begins in `input`. \n\nFor"+
				" example, `strpos('doggie', 'gie')` returns `4`.",
			tree.VolatilityImmutable,
		),
		bitsOverload2("input", "find",
			func(_ *tree.EvalContext, bitString, bitSubstring *tree.DBitArray) (tree.Datum, error) {
				index := strings.Index(bitString.BitArray.String(), bitSubstring.BitArray.String())
				if index < 0 {
					return tree.DZero, nil
				}
				return tree.NewDInt(tree.DInt(index + 1)), nil
			},
			types.Int,
			"Calculates the position where the bit subarray `find` begins in `input`.",
			tree.VolatilityImmutable,
		),
		bytesOverload2(
			"input",
			"find",
			func(_ *tree.EvalContext, byteString, byteSubstring string) (tree.Datum, error) {
				index := strings.Index(byteString, byteSubstring)
				if index < 0 {
					return tree.DZero, nil
				}
				return tree.NewDInt(tree.DInt(index + 1)), nil
			},
			types.Int,
			"Calculates the position where the byte subarray `find` begins in `input`.",
			tree.VolatilityImmutable,
		)),

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
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
		},
	),

	// The SQL parser coerces TRIM(...) and TRIM(BOTH ...) to BTRIM(...).
	"btrim": makeBuiltin(defProps(),
		stringOverload2(
			"input",
			"trim_chars",
			func(_ *tree.EvalContext, s, chars string) (tree.Datum, error) {
				return tree.NewDString(strings.Trim(s, chars)), nil
			},
			types.String,
			"Removes any characters included in `trim_chars` from the beginning or end"+
				" of `input` (applies recursively). \n\nFor example, `btrim('doggie', 'eod')` "+
				"returns `ggi`.",
			tree.VolatilityImmutable,
		),
		stringOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				return tree.NewDString(strings.TrimSpace(s)), nil
			},
			types.String,
			"Removes all spaces from the beginning and end of `val`.",
			tree.VolatilityImmutable,
		),
	),

	// The SQL parser coerces TRIM(LEADING ...) to LTRIM(...).
	"ltrim": makeBuiltin(defProps(),
		stringOverload2(
			"input",
			"trim_chars",
			func(_ *tree.EvalContext, s, chars string) (tree.Datum, error) {
				return tree.NewDString(strings.TrimLeft(s, chars)), nil
			},
			types.String,
			"Removes any characters included in `trim_chars` from the beginning "+
				"(left-hand side) of `input` (applies recursively). \n\nFor example, "+
				"`ltrim('doggie', 'od')` returns `ggie`.",
			tree.VolatilityImmutable,
		),
		stringOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				return tree.NewDString(strings.TrimLeftFunc(s, unicode.IsSpace)), nil
			},
			types.String,
			"Removes all spaces from the beginning (left-hand side) of `val`.",
			tree.VolatilityImmutable,
		),
	),

	// The SQL parser coerces TRIM(TRAILING ...) to RTRIM(...).
	"rtrim": makeBuiltin(defProps(),
		stringOverload2(
			"input",
			"trim_chars",
			func(_ *tree.EvalContext, s, chars string) (tree.Datum, error) {
				return tree.NewDString(strings.TrimRight(s, chars)), nil
			},
			types.String,
			"Removes any characters included in `trim_chars` from the end (right-hand "+
				"side) of `input` (applies recursively). \n\nFor example, `rtrim('doggie', 'ei')` "+
				"returns `dogg`.",
			tree.VolatilityImmutable,
		),
		stringOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				return tree.NewDString(strings.TrimRightFunc(s, unicode.IsSpace)), nil
			},
			types.String,
			"Removes all spaces from the end (right-hand side) of `val`.",
			tree.VolatilityImmutable,
		),
	),

	"reverse": makeBuiltin(defProps(),
		stringOverload1(
			func(evalCtx *tree.EvalContext, s string) (tree.Datum, error) {
				if len(s) > maxAllocatedStringSize {
					return nil, errStringTooLarge
				}
				runes := []rune(s)
				for i, j := 0, len(runes)-1; i < j; i, j = i+1, j-1 {
					runes[i], runes[j] = runes[j], runes[i]
				}
				return tree.NewDString(string(runes)), nil
			},
			types.String,
			"Reverses the order of the string's characters.",
			tree.VolatilityImmutable,
		),
	),

	"replace": makeBuiltin(defProps(),
		stringOverload3(
			"input",
			"find",
			"replace",
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
			tree.VolatilityImmutable,
		),
	),

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
			},
			types.String,
			"In `input`, replaces the first character from `find` with the first "+
				"character in `replace`; repeat for each character in `find`. \n\nFor example, "+
				"`translate('doggie', 'dog', '123');` returns `1233ie`.",
			tree.VolatilityImmutable,
		),
	),

	"regexp_extract": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"input", types.String}, {"regex", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				pattern := string(tree.MustBeDString(args[1]))
				return regexpExtract(ctx, s, pattern, `\`)
			},
			Info:       "Returns the first match for the Regular Expression `regex` in `input`.",
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
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
				"expression `replace` using `flags`." + regexpFlagInfo,
			Volatility: tree.VolatilityImmutable,
		},
	),

	"regexp_split_to_array": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"string", types.String},
				{"pattern", types.String},
			},
			ReturnType: tree.FixedReturnType(types.MakeArray(types.String)),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return regexpSplitToArray(ctx, args, false /* hasFlags */)
			},
			Info:       "Split string using a POSIX regular expression as the delimiter.",
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"string", types.String},
				{"pattern", types.String},
				{"flags", types.String},
			},
			ReturnType: tree.FixedReturnType(types.MakeArray(types.String)),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return regexpSplitToArray(ctx, args, true /* hasFlags */)
			},
			Info:       "Split string using a POSIX regular expression as the delimiter with flags." + regexpFlagInfo,
			Volatility: tree.VolatilityImmutable,
		},
	),

	"like_escape": makeBuiltin(defProps(),
		stringOverload3(
			"unescaped", "pattern", "escape",
			func(evalCtx *tree.EvalContext, unescaped, pattern, escape string) (tree.Datum, error) {
				return tree.MatchLikeEscape(evalCtx, unescaped, pattern, escape, false)
			},
			types.Bool,
			"Matches `unescaped` with `pattern` using `escape` as an escape token.",
			tree.VolatilityImmutable,
		),
	),

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
			"Checks whether `unescaped` not matches with `pattern` using `escape` as an escape token.",
			tree.VolatilityImmutable,
		)),

	"ilike_escape": makeBuiltin(defProps(),
		stringOverload3(
			"unescaped", "pattern", "escape",
			func(evalCtx *tree.EvalContext, unescaped, pattern, escape string) (tree.Datum, error) {
				return tree.MatchLikeEscape(evalCtx, unescaped, pattern, escape, true)
			},
			types.Bool,
			"Matches case insensetively `unescaped` with `pattern` using `escape` as an escape token.",
			tree.VolatilityImmutable,
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
			"Checks whether `unescaped` not matches case insensetively with `pattern` using `escape` as an escape token.",
			tree.VolatilityImmutable,
		)),

	"similar_escape": makeBuiltin(
		tree.FunctionProperties{
			NullableArgs: true,
		},
		similarOverloads...),

	"similar_to_escape": makeBuiltin(
		defProps(),
		append(
			similarOverloads,
			stringOverload3(
				"unescaped", "pattern", "escape",
				func(evalCtx *tree.EvalContext, unescaped, pattern, escape string) (tree.Datum, error) {
					return tree.SimilarToEscape(evalCtx, unescaped, pattern, escape)
				},
				types.Bool,
				"Matches `unescaped` with `pattern` using `escape` as an escape token.",
				tree.VolatilityImmutable,
			),
		)...,
	),

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
			"Checks whether `unescaped` not matches with `pattern` using `escape` as an escape token.",
			tree.VolatilityImmutable,
		)),

	"initcap": makeBuiltin(defProps(),
		stringOverload1(
			func(evalCtx *tree.EvalContext, s string) (tree.Datum, error) {
				return tree.NewDString(strings.Title(strings.ToLower(s))), nil
			},
			types.String,
			"Capitalizes the first letter of `val`.",
			tree.VolatilityImmutable,
		)),

	"quote_ident": makeBuiltin(defProps(),
		stringOverload1(
			func(evalCtx *tree.EvalContext, s string) (tree.Datum, error) {
				var buf bytes.Buffer
				lexbase.EncodeRestrictedSQLIdent(&buf, s, lexbase.EncNoFlags)
				return tree.NewDString(buf.String()), nil
			},
			types.String,
			"Return `val` suitably quoted to serve as identifier in a SQL statement.",
			tree.VolatilityImmutable,
		)),

	"quote_literal": makeBuiltin(defProps(),
		tree.Overload{
			Types:             tree.ArgTypes{{"val", types.String}},
			ReturnType:        tree.FixedReturnType(types.String),
			PreferredOverload: true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := tree.MustBeDString(args[0])
				return tree.NewDString(lex.EscapeSQLString(string(s))), nil
			},
			Info:       "Return `val` suitably quoted to serve as string literal in a SQL statement.",
			Volatility: tree.VolatilityImmutable,
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
			Info:       "Coerce `val` to a string and then quote it as a literal.",
			Volatility: tree.VolatilityStable,
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
			Info:       "Coerce `val` to a string and then quote it as a literal. If `val` is NULL, returns 'NULL'.",
			Volatility: tree.VolatilityImmutable,
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
			Info:       "Coerce `val` to a string and then quote it as a literal. If `val` is NULL, returns 'NULL'.",
			Volatility: tree.VolatilityStable,
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
			Info:       "Returns the first `return_set` bytes from `input`.",
			Volatility: tree.VolatilityImmutable,
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
			Info:       "Returns the first `return_set` characters from `input`.",
			Volatility: tree.VolatilityImmutable,
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
			Info:       "Returns the last `return_set` bytes from `input`.",
			Volatility: tree.VolatilityImmutable,
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
			Info:       "Returns the last `return_set` characters from `input`.",
			Volatility: tree.VolatilityImmutable,
		},
	),

	"random": makeBuiltin(
		tree.FunctionProperties{},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDFloat(tree.DFloat(rand.Float64())), nil
			},
			Info: "Returns a random floating-point number between 0 (inclusive) and 1 (exclusive). " +
				"Note that the value contains at most 53 bits of randomness.",
			Volatility: tree.VolatilityVolatile,
		},
	),

	"unique_rowid": makeBuiltin(
		tree.FunctionProperties{
			Category: categoryIDGeneration,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.NewDInt(GenerateUniqueInt(ctx.NodeID.SQLInstanceID())), nil
			},
			Info: "Returns a unique ID used by CockroachDB to generate unique row IDs if a " +
				"Primary Key isn't defined for the table. The value is a combination of the " +
				"insert timestamp and the ID of the node executing the statement, which " +
				"guarantees this combination is globally unique. However, there can be " +
				"gaps and the order is not completely guaranteed.",
			Volatility: tree.VolatilityVolatile,
		},
	),

	// Sequence functions.

	"nextval": makeBuiltin(
		tree.FunctionProperties{
			Category:             categorySequences,
			DistsqlBlocklist:     true,
			HasSequenceArguments: true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{SequenceNameArg, types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				name := tree.MustBeDString(args[0])
				qualifiedName, err := parser.ParseQualifiedTableName(string(name))
				if err != nil {
					return nil, err
				}
				res, err := evalCtx.Sequence.IncrementSequence(evalCtx.Ctx(), qualifiedName)
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(res)), nil
			},
			Info:       "Advances the given sequence and returns its new value.",
			Volatility: tree.VolatilityVolatile,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{SequenceNameArg, types.RegClass}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				oid := tree.MustBeDOid(args[0])
				res, err := evalCtx.Sequence.IncrementSequenceByID(evalCtx.Ctx(), int64(oid.DInt))
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(res)), nil
			},
			Info:       "Advances the given sequence and returns its new value.",
			Volatility: tree.VolatilityVolatile,
		},
	),

	"currval": makeBuiltin(
		tree.FunctionProperties{
			Category:             categorySequences,
			DistsqlBlocklist:     true,
			HasSequenceArguments: true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{SequenceNameArg, types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				name := tree.MustBeDString(args[0])
				qualifiedName, err := parser.ParseQualifiedTableName(string(name))
				if err != nil {
					return nil, err
				}
				res, err := evalCtx.Sequence.GetLatestValueInSessionForSequence(evalCtx.Ctx(), qualifiedName)
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(res)), nil
			},
			Info:       "Returns the latest value obtained with nextval for this sequence in this session.",
			Volatility: tree.VolatilityVolatile,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{SequenceNameArg, types.RegClass}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				oid := tree.MustBeDOid(args[0])
				res, err := evalCtx.Sequence.GetLatestValueInSessionForSequenceByID(evalCtx.Ctx(), int64(oid.DInt))
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(res)), nil
			},
			Info:       "Returns the latest value obtained with nextval for this sequence in this session.",
			Volatility: tree.VolatilityVolatile,
		},
	),

	"pg_get_serial_sequence": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySequences,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"table_name", types.String}, {"column_name", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				tableName := tree.MustBeDString(args[0])
				columnName := tree.MustBeDString(args[1])
				qualifiedName, err := parser.ParseQualifiedTableName(string(tableName))
				if err != nil {
					return nil, err
				}
				res, err := evalCtx.Sequence.GetSerialSequenceNameFromColumn(evalCtx.Ctx(), qualifiedName, tree.Name(columnName))
				if err != nil {
					return nil, err
				}
				if res == nil {
					return tree.DNull, nil
				}
				res.ExplicitCatalog = false
				return tree.NewDString(fmt.Sprintf(`%s.%s`, res.Schema(), res.Object())), nil
			},
			Info:       "Returns the name of the sequence used by the given column_name in the table table_name.",
			Volatility: tree.VolatilityStable,
		},
	),

	"lastval": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySequences,
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
			Info:       "Return value most recently obtained with nextval in this session.",
			Volatility: tree.VolatilityVolatile,
		},
	),

	// Note: behavior is slightly different than Postgres for performance reasons.
	// See https://github.com/cockroachdb/cockroach/issues/21564
	"setval": makeBuiltin(
		tree.FunctionProperties{
			Category:             categorySequences,
			DistsqlBlocklist:     true,
			HasSequenceArguments: true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{SequenceNameArg, types.String}, {"value", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				name := tree.MustBeDString(args[0])
				qualifiedName, err := parser.ParseQualifiedTableName(string(name))
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
			Volatility: tree.VolatilityVolatile,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{SequenceNameArg, types.RegClass}, {"value", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				oid := tree.MustBeDOid(args[0])
				newVal := tree.MustBeDInt(args[1])
				if err := evalCtx.Sequence.SetSequenceValueByID(
					evalCtx.Ctx(), int64(oid.DInt), int64(newVal), true); err != nil {
					return nil, err
				}
				return args[1], nil
			},
			Info: "Set the given sequence's current value. The next call to nextval will return " +
				"`value + Increment`",
			Volatility: tree.VolatilityVolatile,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{SequenceNameArg, types.String}, {"value", types.Int}, {"is_called", types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				name := tree.MustBeDString(args[0])
				qualifiedName, err := parser.ParseQualifiedTableName(string(name))
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
			Volatility: tree.VolatilityVolatile,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{SequenceNameArg, types.RegClass}, {"value", types.Int}, {"is_called", types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				oid := tree.MustBeDOid(args[0])
				isCalled := bool(tree.MustBeDBool(args[2]))

				newVal := tree.MustBeDInt(args[1])
				if err := evalCtx.Sequence.SetSequenceValueByID(
					evalCtx.Ctx(), int64(oid.DInt), int64(newVal), isCalled); err != nil {
					return nil, err
				}
				return args[1], nil
			},
			Info: "Set the given sequence's current value. If is_called is false, the next call to " +
				"nextval will return `value`; otherwise `value + Increment`.",
			Volatility: tree.VolatilityVolatile,
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
			Info:       "Returns the element with the greatest value.",
			Volatility: tree.VolatilityImmutable,
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
			Info:       "Returns the element with the lowest value.",
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
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
				return tree.MakeDTimestampTZ(t.UTC(), time.Microsecond)
			},
			Info: "Returns `input` as a timestamptz using `format` (which uses standard " +
				"`strptime` formatting).",
			Volatility: tree.VolatilityImmutable,
		},
	),

	// https://www.postgresql.org/docs/10/static/functions-datetime.html
	"age": makeBuiltin(
		tree.FunctionProperties{},
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.Interval),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return &tree.DInterval{
					Duration: duration.Age(
						ctx.GetTxnTimestamp(time.Microsecond).Time,
						args[0].(*tree.DTimestampTZ).Time,
					),
				}, nil
			},
			Info: "Calculates the interval between `val` and the current time, normalized into years, months and days." + `

Note this may not be an accurate time span since years and months are normalized
from days, and years and months are out of context. To avoid normalizing days into
months and years, use ` + "`now() - timestamptz`" + `.`,
			Volatility: tree.VolatilityStable,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"end", types.TimestampTZ}, {"begin", types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.Interval),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return &tree.DInterval{
					Duration: duration.Age(
						args[0].(*tree.DTimestampTZ).Time,
						args[1].(*tree.DTimestampTZ).Time,
					),
				}, nil
			},
			Info: "Calculates the interval between `begin` and `end`, normalized into years, months and days." + `

Note this may not be an accurate time span since years and months are normalized
from days, and years and months are out of context. To avoid normalizing days into
months and years, use the timestamptz subtraction operator.`,
			Volatility: tree.VolatilityImmutable,
		},
	),

	"current_date": makeBuiltin(
		tree.FunctionProperties{},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn:         currentDate,
			Info:       "Returns the date of the current transaction." + txnTSContextDoc,
			Volatility: tree.VolatilityStable,
		},
	),

	"now":                   txnTSImplBuiltin(true),
	"current_time":          txnTimeWithPrecisionBuiltin(true),
	"current_timestamp":     txnTSWithPrecisionImplBuiltin(true),
	"transaction_timestamp": txnTSImplBuiltin(true),

	"localtimestamp": txnTSWithPrecisionImplBuiltin(false),
	"localtime":      txnTimeWithPrecisionBuiltin(false),

	"statement_timestamp": makeBuiltin(
		tree.FunctionProperties{},
		tree.Overload{
			Types:             tree.ArgTypes{},
			ReturnType:        tree.FixedReturnType(types.TimestampTZ),
			PreferredOverload: true,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDTimestampTZ(ctx.GetStmtTimestamp(), time.Microsecond)
			},
			Info:       "Returns the start time of the current statement.",
			Volatility: tree.VolatilityStable,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDTimestamp(ctx.GetStmtTimestamp(), time.Microsecond)
			},
			Info:       "Returns the start time of the current statement.",
			Volatility: tree.VolatilityStable,
		},
	),

	tree.FollowerReadTimestampFunctionName: makeBuiltin(
		tree.FunctionProperties{},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn:         followerReadTimestamp,
			Info: fmt.Sprintf(`Returns a timestamp which is very likely to be safe to perform
against a follower replica.

This function is intended to be used with an AS OF SYSTEM TIME clause to perform
historical reads against a time which is recent but sufficiently old for reads
to be performed against the closest replica as opposed to the currently
leaseholder for a given range.

Note that this function requires an enterprise license on a CCL distribution to
return a result that is less likely the closest replica. It is otherwise
hardcoded as %s from the statement time, which may not result in reading from the
nearest replica.`, defaultFollowerReadDuration),
			Volatility: tree.VolatilityVolatile,
		},
	),

	tree.FollowerReadTimestampExperimentalFunctionName: makeBuiltin(
		tree.FunctionProperties{},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn:         followerReadTimestamp,
			Info:       fmt.Sprintf("Same as %s. This name is deprecated.", tree.FollowerReadTimestampFunctionName),
			Volatility: tree.VolatilityVolatile,
		},
	),

	"cluster_logical_timestamp": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
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
			Volatility: tree.VolatilityVolatile,
		},
	),

	"clock_timestamp": makeBuiltin(
		tree.FunctionProperties{},
		tree.Overload{
			Types:             tree.ArgTypes{},
			ReturnType:        tree.FixedReturnType(types.TimestampTZ),
			PreferredOverload: true,
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDTimestampTZ(timeutil.Now(), time.Microsecond)
			},
			Info:       "Returns the current system time on one of the cluster nodes.",
			Volatility: tree.VolatilityVolatile,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDTimestamp(timeutil.Now(), time.Microsecond)
			},
			Info:       "Returns the current system time on one of the cluster nodes.",
			Volatility: tree.VolatilityVolatile,
		},
	),

	"timeofday": makeBuiltin(
		tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ctxTime := ctx.GetRelativeParseTime()
				// From postgres@a166d408eb0b35023c169e765f4664c3b114b52e src/backend/utils/adt/timestamp.c#L1637,
				// we should support "%a %b %d %H:%M:%S.%%06d %Y %Z".
				return tree.NewDString(ctxTime.Format("Mon Jan 2 15:04:05.000000 2006 -0700")), nil
			},
			Info:       "Returns the current system time on one of the cluster nodes as a string.",
			Volatility: tree.VolatilityStable,
		},
	),

	"extract": makeBuiltin(
		tree.FunctionProperties{Category: categoryDateAndTime},
		tree.Overload{
			Types:      tree.ArgTypes{{"element", types.String}, {"input", types.Timestamp}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// extract timeSpan fromTime.
				fromTS := args[1].(*tree.DTimestamp)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				return extractTimeSpanFromTimestamp(ctx, fromTS.Time, timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: millennium, century, decade, year, isoyear,\n" +
				"quarter, month, week, dayofweek, isodow, dayofyear, julian,\n" +
				"hour, minute, second, millisecond, microsecond, epoch",
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"element", types.String}, {"input", types.Interval}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fromInterval := args[1].(*tree.DInterval)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				return extractTimeSpanFromInterval(fromInterval, timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: millennium, century, decade, year,\n" +
				"month, day, hour, minute, second, millisecond, microsecond, epoch",
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"element", types.String}, {"input", types.Date}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				date := args[1].(*tree.DDate)
				fromTime, err := date.ToTime()
				if err != nil {
					return nil, err
				}
				return extractTimeSpanFromTimestamp(ctx, fromTime, timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: millennium, century, decade, year, isoyear,\n" +
				"quarter, month, week, dayofweek, isodow, dayofyear, julian,\n" +
				"hour, minute, second, millisecond, microsecond, epoch",
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"element", types.String}, {"input", types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fromTSTZ := args[1].(*tree.DTimestampTZ)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				return extractTimeSpanFromTimestampTZ(ctx, fromTSTZ.Time.In(ctx.GetLocation()), timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: millennium, century, decade, year, isoyear,\n" +
				"quarter, month, week, dayofweek, isodow, dayofyear, julian,\n" +
				"hour, minute, second, millisecond, microsecond, epoch,\n" +
				"timezone, timezone_hour, timezone_minute",
			Volatility: tree.VolatilityStable,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"element", types.String}, {"input", types.Time}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fromTime := args[1].(*tree.DTime)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				return extractTimeSpanFromTime(fromTime, timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: hour, minute, second, millisecond, microsecond, epoch",
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"element", types.String}, {"input", types.TimeTZ}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fromTime := args[1].(*tree.DTimeTZ)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				return extractTimeSpanFromTimeTZ(fromTime, timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: hour, minute, second, millisecond, microsecond, epoch,\n" +
				"timezone, timezone_hour, timezone_minute",
			Volatility: tree.VolatilityImmutable,
		},
	),

	// TODO(knz,otan): Remove in 20.2.
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
				"Compatible elements: hour, minute, second, millisecond, microsecond.\n" +
				"This is deprecated in favor of `extract` which supports duration.",
			Volatility: tree.VolatilityImmutable,
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
				tsTZ, err := truncateTimestamp(fromTS.Time, timeSpan)
				if err != nil {
					return nil, err
				}
				return tree.MakeDTimestamp(tsTZ.Time, time.Microsecond)
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero (or one, for day and month)\n\n" +
				"Compatible elements: millennium, century, decade, year, quarter, month,\n" +
				"week, day, hour, minute, second, millisecond, microsecond.",
			Volatility: tree.VolatilityImmutable,
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
				return truncateTimestamp(fromTSTZ.Time, timeSpan)
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero (or one, for day and month)\n\n" +
				"Compatible elements: millennium, century, decade, year, quarter, month,\n" +
				"week, day, hour, minute, second, millisecond, microsecond.",
			Volatility: tree.VolatilityStable,
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
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"element", types.String}, {"input", types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				fromTSTZ := args[1].(*tree.DTimestampTZ)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				return truncateTimestamp(fromTSTZ.Time.In(ctx.GetLocation()), timeSpan)
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero (or one, for day and month)\n\n" +
				"Compatible elements: millennium, century, decade, year, quarter, month,\n" +
				"week, day, hour, minute, second, millisecond, microsecond.",
			Volatility: tree.VolatilityStable,
		},
	),

	"row_to_json": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"row", types.AnyTuple}},
			ReturnType: tree.FixedReturnType(types.Jsonb),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
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
					val, err := tree.AsJSON(
						d,
						ctx.SessionData.DataConversionConfig,
						ctx.GetLocation(),
					)
					if err != nil {
						return nil, err
					}
					builder.Add(label, val)
				}
				return tree.NewDJSON(builder.Build()), nil
			},
			Info:       "Returns the row as a JSON object.",
			Volatility: tree.VolatilityStable,
		},
	),

	// https://www.postgresql.org/docs/9.6/functions-datetime.html
	"timezone": makeBuiltin(defProps(),
		// NOTE(otan): this should be deleted and replaced with the correct
		// function overload promoting the string to timestamptz.
		tree.Overload{
			Types: tree.ArgTypes{
				{"timezone", types.String},
				{"timestamptz_string", types.String},
			},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				tzArg := string(tree.MustBeDString(args[0]))
				tsArg := string(tree.MustBeDString(args[1]))
				ts, _, err := tree.ParseDTimestampTZ(ctx, tsArg, time.Microsecond)
				if err != nil {
					return nil, err
				}
				loc, err := timeutil.TimeZoneStringToLocation(tzArg, timeutil.TimeZoneStringToLocationPOSIXStandard)
				if err != nil {
					return nil, err
				}
				return ts.EvalAtTimeZone(ctx, loc)
			},
			Info:       "Convert given time stamp with time zone to the new time zone, with no time zone designation.",
			Volatility: tree.VolatilityStable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"timezone", types.String},
				{"timestamp", types.Timestamp},
			},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				tzStr := string(tree.MustBeDString(args[0]))
				ts := tree.MustBeDTimestamp(args[1])
				loc, err := timeutil.TimeZoneStringToLocation(
					tzStr,
					timeutil.TimeZoneStringToLocationPOSIXStandard,
				)
				if err != nil {
					return nil, err
				}
				_, beforeOffsetSecs := ts.Time.Zone()
				_, afterOffsetSecs := ts.Time.In(loc).Zone()
				durationDelta := time.Duration(beforeOffsetSecs-afterOffsetSecs) * time.Second
				return tree.MakeDTimestampTZ(ts.Time.Add(durationDelta), time.Microsecond)
			},
			Info:       "Treat given time stamp without time zone as located in the specified time zone.",
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"timezone", types.String},
				{"timestamptz", types.TimestampTZ},
			},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				tzStr := string(tree.MustBeDString(args[0]))
				ts := tree.MustBeDTimestampTZ(args[1])
				loc, err := timeutil.TimeZoneStringToLocation(
					tzStr,
					timeutil.TimeZoneStringToLocationPOSIXStandard,
				)
				if err != nil {
					return nil, err
				}
				return ts.EvalAtTimeZone(ctx, loc)
			},
			Info:       "Convert given time stamp with time zone to the new time zone, with no time zone designation.",
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"timezone", types.String},
				{"time", types.Time},
			},
			ReturnType: tree.FixedReturnType(types.TimeTZ),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				tzStr := string(tree.MustBeDString(args[0]))
				tArg := args[1].(*tree.DTime)
				loc, err := timeutil.TimeZoneStringToLocation(
					tzStr,
					timeutil.TimeZoneStringToLocationPOSIXStandard,
				)
				if err != nil {
					return nil, err
				}
				tTime := timeofday.TimeOfDay(*tArg).ToTime()
				_, beforeOffsetSecs := tTime.In(ctx.GetLocation()).Zone()
				durationDelta := time.Duration(-beforeOffsetSecs) * time.Second
				return tree.NewDTimeTZ(timetz.MakeTimeTZFromTime(tTime.In(loc).Add(durationDelta))), nil
			},
			Info:       "Treat given time without time zone as located in the specified time zone.",
			Volatility: tree.VolatilityStable,
			// This overload's volatility does not match Postgres. See
			// https://github.com/cockroachdb/cockroach/pull/51037 for details.
			IgnoreVolatilityCheck: true,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"timezone", types.String},
				{"timetz", types.TimeTZ},
			},
			ReturnType: tree.FixedReturnType(types.TimeTZ),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// This one should disappear with implicit casts.
				tzStr := string(tree.MustBeDString(args[0]))
				tArg := args[1].(*tree.DTimeTZ)
				loc, err := timeutil.TimeZoneStringToLocation(
					tzStr,
					timeutil.TimeZoneStringToLocationPOSIXStandard,
				)
				if err != nil {
					return nil, err
				}
				tTime := tArg.TimeTZ.ToTime()
				return tree.NewDTimeTZ(timetz.MakeTimeTZFromTime(tTime.In(loc))), nil
			},
			Info:       "Convert given time with time zone to the new time zone.",
			Volatility: tree.VolatilityStable,
			// This overload's volatility does not match Postgres. See
			// https://github.com/cockroachdb/cockroach/pull/51037 for details.
			IgnoreVolatilityCheck: true,
		},
	),

	// parse_timestamp converts strings to timestamps. It is useful in expressions
	// where casts (which are not immutable) cannot be used, like computed column
	// expressions or partial index predicates. Only absolute timestamps that do
	// not depend on the current context are supported (relative timestamps like
	// 'now' are not supported).
	"parse_timestamp": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"string", types.String}},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				arg := string(tree.MustBeDString(args[0]))
				ts, dependsOnContext, err := tree.ParseDTimestamp(ctx, arg, time.Microsecond)
				if err != nil {
					return nil, err
				}
				if dependsOnContext {
					return nil, pgerror.Newf(pgcode.InvalidParameterValue, "relative timestamps are not supported")
				}
				return ts, nil
			},
			Info:       "Convert a string containing an absolute timestamp to the corresponding timestamp.",
			Volatility: tree.VolatilityImmutable,
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
			Info:       "Split a string into components on a delimiter.",
			Volatility: tree.VolatilityImmutable,
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
			Info:       "Split a string into components on a delimiter with a specified string to consider NULL.",
			Volatility: tree.VolatilityImmutable,
		},
	),

	"array_to_string": makeBuiltin(arrayPropsNullableArgs(),
		tree.Overload{
			Types:      tree.ArgTypes{{"input", types.AnyArray}, {"delim", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull || args[1] == tree.DNull {
					return tree.DNull, nil
				}
				arr := tree.MustBeDArray(args[0])
				delim := string(tree.MustBeDString(args[1]))
				return arrayToString(evalCtx, arr, delim, nil)
			},
			Info:       "Join an array into a string with a delimiter.",
			Volatility: tree.VolatilityStable,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"input", types.AnyArray}, {"delimiter", types.String}, {"null", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull || args[1] == tree.DNull {
					return tree.DNull, nil
				}
				arr := tree.MustBeDArray(args[0])
				delim := string(tree.MustBeDString(args[1]))
				nullStr := stringOrNil(args[2])
				return arrayToString(evalCtx, arr, delim, nullStr)
			},
			Info:       "Join an array into a string with a delimiter, replacing NULLs with a null string.",
			Volatility: tree.VolatilityStable,
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
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
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
			Volatility: tree.VolatilityImmutable,
		},
	),

	"array_append": setProps(arrayPropsNullableArgs(), arrayBuiltin(func(typ *types.T) tree.Overload {
		return tree.Overload{
			Types:      tree.ArgTypes{{"array", types.MakeArray(typ)}, {"elem", typ}},
			ReturnType: tree.FixedReturnType(types.MakeArray(typ)),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.AppendToMaybeNullArray(typ, args[0], args[1])
			},
			Info:       "Appends `elem` to `array`, returning the result.",
			Volatility: tree.VolatilityImmutable,
		}
	})),

	"array_prepend": setProps(arrayPropsNullableArgs(), arrayBuiltin(func(typ *types.T) tree.Overload {
		return tree.Overload{
			Types:      tree.ArgTypes{{"elem", typ}, {"array", types.MakeArray(typ)}},
			ReturnType: tree.FixedReturnType(types.MakeArray(typ)),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.PrependToMaybeNullArray(typ, args[0], args[1])
			},
			Info:       "Prepends `elem` to `array`, returning the result.",
			Volatility: tree.VolatilityImmutable,
		}
	})),

	"array_cat": setProps(arrayPropsNullableArgs(), arrayBuiltin(func(typ *types.T) tree.Overload {
		return tree.Overload{
			Types:      tree.ArgTypes{{"left", types.MakeArray(typ)}, {"right", types.MakeArray(typ)}},
			ReturnType: tree.FixedReturnType(types.MakeArray(typ)),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.ConcatArrays(typ, args[0], args[1])
			},
			Info:       "Appends two arrays.",
			Volatility: tree.VolatilityImmutable,
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
			Info:       "Remove from `array` all elements equal to `elem`.",
			Volatility: tree.VolatilityImmutable,
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
			Info:       "Replace all occurrences of `toreplace` in `array` with `replacewith`.",
			Volatility: tree.VolatilityImmutable,
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
			Info:       "Return the index of the first occurrence of `elem` in `array`.",
			Volatility: tree.VolatilityImmutable,
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
			Info:       "Returns and array of indexes of all occurrences of `elem` in `array`.",
			Volatility: tree.VolatilityImmutable,
		}
	})),

	// Full text search functions.
	"ts_match_qv":                    makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"ts_match_vq":                    makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"tsvector_cmp":                   makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"tsvector_concat":                makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"ts_debug":                       makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"ts_headline":                    makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"ts_lexize":                      makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"websearch_to_tsquery":           makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"array_to_tsvector":              makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"get_current_ts_config":          makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"numnode":                        makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"plainto_tsquery":                makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"phraseto_tsquery":               makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"querytree":                      makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"setweight":                      makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"strip":                          makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"to_tsquery":                     makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"to_tsvector":                    makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"json_to_tsvector":               makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"jsonb_to_tsvector":              makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"ts_delete":                      makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"ts_filter":                      makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"ts_rank":                        makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"ts_rank_cd":                     makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"ts_rewrite":                     makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"tsquery_phrase":                 makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"tsvector_to_array":              makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"tsvector_update_trigger":        makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),
	"tsvector_update_trigger_column": makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: categoryFullTextSearch}),

	// Fuzzy String Matching
	"soundex": makeBuiltin(
		tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{"source", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				t := fuzzystrmatch.Soundex(s)
				return tree.NewDString(t), nil
			},
			Info:       "Convert a string to its Soundex code.",
			Volatility: tree.VolatilityImmutable,
		},
	),
	// The function is confusingly named, `similarity` would have been a better name,
	// but this name matches the name in PostgreSQL.
	// See https://www.postgresql.org/docs/current/fuzzystrmatch.html"
	"difference": makeBuiltin(
		tree.FunctionProperties{Category: categoryString},
		tree.Overload{
			Types:      tree.ArgTypes{{"source", types.String}, {"target", types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s, t := string(tree.MustBeDString(args[0])), string(tree.MustBeDString(args[1]))
				diff := fuzzystrmatch.Difference(s, t)
				return tree.NewDString(strconv.Itoa(diff)), nil
			},
			Info:       "Convert two strings to their Soundex codes and then reports the number of matching code positions.",
			Volatility: tree.VolatilityImmutable,
		},
	),
	"levenshtein": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ArgTypes{{"source", types.String}, {"target", types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s, t := string(tree.MustBeDString(args[0])), string(tree.MustBeDString(args[1]))
				const maxLen = 255
				if len(s) > maxLen || len(t) > maxLen {
					return nil, pgerror.Newf(pgcode.InvalidParameterValue,
						"levenshtein argument exceeds maximum length of %d characters", maxLen)
				}
				ld := fuzzystrmatch.LevenshteinDistance(s, t)
				return tree.NewDInt(tree.DInt(ld)), nil
			},
			Info:       "Calculates the Levenshtein distance between two strings. Maximum input length is 255 characters.",
			Volatility: tree.VolatilityImmutable,
		},
		tree.Overload{
			Types: tree.ArgTypes{{"source", types.String}, {"target", types.String},
				{"ins_cost", types.Int}, {"del_cost", types.Int}, {"sub_cost", types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				s, t := string(tree.MustBeDString(args[0])), string(tree.MustBeDString(args[1]))
				ins, del, sub := int(tree.MustBeDInt(args[2])), int(tree.MustBeDInt(args[3])), int(tree.MustBeDInt(args[4]))
				const maxLen = 255
				if len(s) > maxLen || len(t) > maxLen {
					return nil, pgerror.Newf(pgcode.InvalidParameterValue,
						"levenshtein argument exceeds maximum length of %d characters", maxLen)
				}
				ld := fuzzystrmatch.LevenshteinDistanceWithCost(s, t, ins, del, sub)
				return tree.NewDInt(tree.DInt(ld)), nil
			},
			Info: "Calculates the Levenshtein distance between two strings. The cost parameters specify how much to " +
				"charge for each edit operation. Maximum input length is 255 characters.",
			Volatility: tree.VolatilityImmutable,
		}),
	"levenshtein_less_equal": makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 56820, Category: categoryFuzzyStringMatching}),
	"metaphone":              makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 56820, Category: categoryFuzzyStringMatching}),
	"dmetaphone_alt":         makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 56820, Category: categoryFuzzyStringMatching}),

	// Trigram functions.
	"similarity":             makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 41285, Category: categoryTrigram}),
	"show_trgm":              makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 41285, Category: categoryTrigram}),
	"word_similarity":        makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 41285, Category: categoryTrigram}),
	"strict_word_similarity": makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 41285, Category: categoryTrigram}),
	"show_limit":             makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 41285, Category: categoryTrigram}),
	"set_limit":              makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 41285, Category: categoryTrigram}),

	// JSON functions.
	// The behavior of both the JSON and JSONB data types in CockroachDB is
	// similar to the behavior of the JSONB data type in Postgres.

	"json_to_recordset":        makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 33285, Category: categoryJSON}),
	"jsonb_to_recordset":       makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 33285, Category: categoryJSON}),
	"json_populate_recordset":  makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 33285, Category: categoryJSON}),
	"jsonb_populate_recordset": makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 33285, Category: categoryJSON}),

	"jsonb_path_exists":      makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 22513, Category: categoryJSON}),
	"jsonb_path_exists_opr":  makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 22513, Category: categoryJSON}),
	"jsonb_path_match":       makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 22513, Category: categoryJSON}),
	"jsonb_path_match_opr":   makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 22513, Category: categoryJSON}),
	"jsonb_path_query":       makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 22513, Category: categoryJSON}),
	"jsonb_path_query_array": makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 22513, Category: categoryJSON}),
	"jsonb_path_query_first": makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 22513, Category: categoryJSON}),

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
			Info:       "Remove the specified path from the JSON object.",
			Volatility: tree.VolatilityImmutable,
		},
	),

	"json_extract_path": makeBuiltin(jsonProps(), jsonExtractPathImpl),

	"jsonb_extract_path": makeBuiltin(jsonProps(), jsonExtractPathImpl),

	"json_extract_path_text": makeBuiltin(jsonProps(), jsonExtractPathTextImpl),

	"jsonb_extract_path_text": makeBuiltin(jsonProps(), jsonExtractPathTextImpl),

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
			Info:       "Returns the given JSON value as a STRING indented and with newlines.",
			Volatility: tree.VolatilityImmutable,
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

	"jsonb_exists_any": makeBuiltin(
		jsonProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"json", types.Jsonb},
				{"array", types.StringArray},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(e *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.JSONExistsAny(e, tree.MustBeDJSON(args[0]), tree.MustBeDArray(args[1]))
			},
			Info:       "Returns whether any of the strings in the text array exist as top-level keys or array elements",
			Volatility: tree.VolatilityImmutable,
		},
	),

	"crdb_internal.pb_to_json": makeBuiltin(
		jsonProps(),
		func() []tree.Overload {
			pbToJSON := func(typ string, data []byte, emitDefaults bool) (tree.Datum, error) {
				msg, err := protoreflect.DecodeMessage(typ, data)
				if err != nil {
					return nil, err
				}
				j, err := protoreflect.MessageToJSON(msg, emitDefaults)
				if err != nil {
					return nil, err
				}
				return tree.NewDJSON(j), nil
			}
			returnType := tree.FixedReturnType(types.Jsonb)
			const info = "Converts protocol message to its JSONB representation."
			volatility := tree.VolatilityImmutable
			return []tree.Overload{
				{
					Info:       info,
					Volatility: volatility,
					Types: tree.ArgTypes{
						{"pbname", types.String},
						{"data", types.Bytes},
					},
					ReturnType: returnType,
					Fn: func(context *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
						const emitDefaults = true
						return pbToJSON(
							string(tree.MustBeDString(args[0])),
							[]byte(tree.MustBeDBytes(args[1])),
							emitDefaults,
						)
					},
				},
				{
					Info:       info,
					Volatility: volatility,
					Types: tree.ArgTypes{
						{"pbname", types.String},
						{"data", types.Bytes},
						{"emit_defaults", types.Bool},
					},
					ReturnType: returnType,
					Fn: func(context *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
						return pbToJSON(
							string(tree.MustBeDString(args[0])),
							[]byte(tree.MustBeDBytes(args[1])),
							bool(tree.MustBeDBool(args[2])),
						)
					},
				},
			}
		}()...),

	"crdb_internal.json_to_pb": makeBuiltin(
		jsonProps(),
		tree.Overload{
			Types: tree.ArgTypes{
				{"pbname", types.String},
				{"json", types.Jsonb},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				msg, err := protoreflect.NewMessage(string(tree.MustBeDString(args[0])))
				if err != nil {
					return nil, err
				}
				data, err := protoreflect.JSONBMarshalToMessage(tree.MustBeDJSON(args[1]).JSON, msg)
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(data)), nil
			},
			Info:       "Convert JSONB data to protocol message bytes",
			Volatility: tree.VolatilityImmutable,
		}),

	// Enum functions.
	"enum_first": makeBuiltin(
		tree.FunctionProperties{NullableArgs: true, Category: categoryEnum},
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.AnyEnum}},
			ReturnType: tree.IdentityReturnType(0),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return nil, pgerror.Newf(pgcode.NullValueNotAllowed, "argument cannot be NULL")
				}
				arg := args[0].(*tree.DEnum)
				min, ok := arg.MinWriteable()
				if !ok {
					return nil, errors.Newf("enum %s contains no values", arg.ResolvedType().Name())
				}
				return min, nil
			},
			Info:       "Returns the first value of the input enum type.",
			Volatility: tree.VolatilityStable,
		},
	),

	"enum_last": makeBuiltin(
		tree.FunctionProperties{NullableArgs: true, Category: categoryEnum},
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.AnyEnum}},
			ReturnType: tree.IdentityReturnType(0),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return nil, pgerror.Newf(pgcode.NullValueNotAllowed, "argument cannot be NULL")
				}
				arg := args[0].(*tree.DEnum)
				max, ok := arg.MaxWriteable()
				if !ok {
					return nil, errors.Newf("enum %s contains no values", arg.ResolvedType().Name())
				}
				return max, nil
			},
			Info:       "Returns the last value of the input enum type.",
			Volatility: tree.VolatilityStable,
		},
	),

	"enum_range": makeBuiltin(
		tree.FunctionProperties{NullableArgs: true, Category: categoryEnum},
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.AnyEnum}},
			ReturnType: tree.ArrayOfFirstNonNullReturnType(),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return nil, pgerror.Newf(pgcode.NullValueNotAllowed, "argument cannot be NULL")
				}
				arg := args[0].(*tree.DEnum)
				typ := arg.EnumTyp
				arr := tree.NewDArray(typ)
				for i := range typ.TypeMeta.EnumData.LogicalRepresentations {
					// Read-only members should be excluded.
					if typ.TypeMeta.EnumData.IsMemberReadOnly[i] {
						continue
					}
					enum := &tree.DEnum{
						EnumTyp:     typ,
						PhysicalRep: typ.TypeMeta.EnumData.PhysicalRepresentations[i],
						LogicalRep:  typ.TypeMeta.EnumData.LogicalRepresentations[i],
					}
					if err := arr.Append(enum); err != nil {
						return nil, err
					}
				}
				return arr, nil
			},
			Info:       "Returns all values of the input enum in an ordered array.",
			Volatility: tree.VolatilityStable,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"lower", types.AnyEnum}, {"upper", types.AnyEnum}},
			ReturnType: tree.ArrayOfFirstNonNullReturnType(),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull && args[1] == tree.DNull {
					return nil, pgerror.Newf(pgcode.NullValueNotAllowed, "both arguments cannot be NULL")
				}
				var bottom, top int
				var typ *types.T
				switch {
				case args[0] == tree.DNull:
					right := args[1].(*tree.DEnum)
					typ = right.ResolvedType()
					idx, err := typ.EnumGetIdxOfPhysical(right.PhysicalRep)
					if err != nil {
						return nil, err
					}
					bottom, top = 0, idx
				case args[1] == tree.DNull:
					left := args[0].(*tree.DEnum)
					typ = left.ResolvedType()
					idx, err := typ.EnumGetIdxOfPhysical(left.PhysicalRep)
					if err != nil {
						return nil, err
					}
					bottom, top = idx, len(typ.TypeMeta.EnumData.PhysicalRepresentations)-1
				default:
					left, right := args[0].(*tree.DEnum), args[1].(*tree.DEnum)
					if !left.ResolvedType().Equivalent(right.ResolvedType()) {
						return nil, pgerror.Newf(
							pgcode.DatatypeMismatch,
							"mismatched types %s and %s",
							left.ResolvedType(),
							right.ResolvedType(),
						)
					}
					typ = left.ResolvedType()
					var err error
					bottom, err = typ.EnumGetIdxOfPhysical(left.PhysicalRep)
					if err != nil {
						return nil, err
					}
					top, err = typ.EnumGetIdxOfPhysical(right.PhysicalRep)
					if err != nil {
						return nil, err
					}
				}
				arr := tree.NewDArray(typ)
				for i := bottom; i <= top; i++ {
					// Read-only members should be excluded.
					if typ.TypeMeta.EnumData.IsMemberReadOnly[i] {
						continue
					}
					enum := &tree.DEnum{
						EnumTyp:     typ,
						PhysicalRep: typ.TypeMeta.EnumData.PhysicalRepresentations[i],
						LogicalRep:  typ.TypeMeta.EnumData.LogicalRepresentations[i],
					}
					if err := arr.Append(enum); err != nil {
						return nil, err
					}
				}
				return arr, nil
			},
			Info:       "Returns all values of the input enum in an ordered array between the two arguments (inclusive).",
			Volatility: tree.VolatilityStable,
		},
	),

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
			Info:       "Returns the node's version of CockroachDB.",
			Volatility: tree.VolatilityVolatile,
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
			Info:       "Returns the current database.",
			Volatility: tree.VolatilityStable,
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
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ctx := evalCtx.Ctx()
				curDb := evalCtx.SessionData.Database
				iter := evalCtx.SessionData.SearchPath.IterWithoutImplicitPGSchemas()
				for scName, ok := iter.Next(); ok; scName, ok = iter.Next() {
					if found, err := evalCtx.Planner.SchemaExists(ctx, curDb, scName); found || err != nil {
						if err != nil {
							return nil, err
						}
						return tree.NewDString(scName), nil
					}
				}
				return tree.DNull, nil
			},
			Info:       "Returns the current schema.",
			Volatility: tree.VolatilityStable,
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
	// The argument supplied applies to all implicit pg schemas, which includes
	// pg_catalog and pg_temp (if one exists).
	"current_schemas": makeBuiltin(
		tree.FunctionProperties{
			Category:         categorySystemInfo,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"include_pg_catalog", types.Bool}},
			ReturnType: tree.FixedReturnType(types.StringArray),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				ctx := evalCtx.Ctx()
				curDb := evalCtx.SessionData.Database
				includeImplicitPgSchemas := *(args[0].(*tree.DBool))
				schemas := tree.NewDArray(types.String)
				var iter sessiondata.SearchPathIter
				if includeImplicitPgSchemas {
					iter = evalCtx.SessionData.SearchPath.Iter()
				} else {
					iter = evalCtx.SessionData.SearchPath.IterWithoutImplicitPGSchemas()
				}
				for scName, ok := iter.Next(); ok; scName, ok = iter.Next() {
					if found, err := evalCtx.Planner.SchemaExists(ctx, curDb, scName); found || err != nil {
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
			Info:       "Returns the valid schemas in the search path.",
			Volatility: tree.VolatilityStable,
		},
	),

	// https://www.postgresql.org/docs/10/static/functions-info.html
	"current_user": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if ctx.SessionData.User().Undefined() {
					return tree.DNull, nil
				}
				return tree.NewDString(ctx.SessionData.User().Normalized()), nil
			},
			Info: "Returns the current user. This function is provided for " +
				"compatibility with PostgreSQL.",
			Volatility: tree.VolatilityStable,
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
			Info:       "Returns the collation of the argument",
			Volatility: tree.VolatilityStable,
		},
	),

	// Get the current trace ID.
	"crdb_internal.trace_id": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// The user must be an admin to use this builtin.
				isAdmin, err := ctx.SessionAccessor.HasAdminRole(ctx.Context)
				if err != nil {
					return nil, err
				}
				if !isAdmin {
					if err := checkPrivilegedUser(ctx); err != nil {
						return nil, err
					}
				}

				sp := tracing.SpanFromContext(ctx.Context)
				if sp == nil {
					return tree.DNull, nil
				}

				traceID := sp.TraceID()
				if traceID == 0 {
					return tree.DNull, nil
				}
				return tree.NewDInt(tree.DInt(traceID)), nil
			},
			Info: "Returns the current trace ID or an error if no trace is open.",
			// NB: possibly this is or could be made stable, but it's not worth it.
			Volatility: tree.VolatilityVolatile,
		},
	),

	// Toggles all spans of the requested trace to verbose or non-verbose.
	"crdb_internal.set_trace_verbose": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types: tree.ArgTypes{
				{"trace_id", types.Int},
				{"verbosity", types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// The user must be an admin to use this builtin.
				isAdmin, err := ctx.SessionAccessor.HasAdminRole(ctx.Context)
				if err != nil {
					return nil, err
				}
				if !isAdmin {
					if err := checkPrivilegedUser(ctx); err != nil {
						return nil, err
					}
				}

				traceID := uint64(*(args[0].(*tree.DInt)))
				verbosity := bool(*(args[1].(*tree.DBool)))

				var rootSpan *tracing.Span
				if err := ctx.Settings.Tracer.VisitSpans(func(span *tracing.Span) error {
					if span.TraceID() == traceID && rootSpan == nil {
						rootSpan = span
					}

					return nil
				}); err != nil {
					return nil, err
				}
				if rootSpan == nil { // not found
					return tree.DBoolFalse, nil
				}

				rootSpan.SetVerboseRecursively(verbosity)
				return tree.DBoolTrue, nil
			},
			Info:       "Returns true if root span was found and verbosity was set, false otherwise.",
			Volatility: tree.VolatilityVolatile,
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
			Info:       "Returns the value of the specified locality key.",
			Volatility: tree.VolatilityStable,
		},
	),

	"crdb_internal.node_executable_version": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				v := ctx.Settings.Version.BinaryVersion().String()
				return tree.NewDString(v), nil
			},
			Info:       "Returns the version of CockroachDB this node is running.",
			Volatility: tree.VolatilityVolatile,
		},
	),

	"crdb_internal.approximate_timestamp": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types:      tree.ArgTypes{{"timestamp", types.Decimal}},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return tree.DecimalToInexactDTimestamp(args[0].(*tree.DDecimal))
			},
			Info:       "Converts the crdb_internal_mvcc_timestamp column into an approximate timestamp.",
			Volatility: tree.VolatilityImmutable,
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
			Info:       "Returns the cluster ID.",
			Volatility: tree.VolatilityStable,
		},
	),

	"crdb_internal.node_id": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				dNodeID := tree.DNull
				if nodeID, ok := ctx.NodeID.OptionalNodeID(); ok {
					dNodeID = tree.NewDInt(tree.DInt(nodeID))
				}
				return dNodeID, nil
			},
			Info:       "Returns the node ID.",
			Volatility: tree.VolatilityStable,
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
			Info:       "Returns the cluster name.",
			Volatility: tree.VolatilityVolatile,
		},
	),

	"crdb_internal.create_tenant": makeBuiltin(
		tree.FunctionProperties{
			Category:     categoryMultiTenancy,
			NullableArgs: true,
			Undocumented: true,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"id", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if err := requireNonNull(args[0]); err != nil {
					return nil, err
				}
				sTenID := int64(tree.MustBeDInt(args[0]))
				if sTenID <= 0 {
					return nil, pgerror.New(pgcode.InvalidParameterValue, "tenant ID must be positive")
				}
				if err := ctx.Tenant.CreateTenant(ctx.Context, uint64(sTenID)); err != nil {
					return nil, err
				}
				return args[0], nil
			},
			Info:       "Creates a new tenant with the provided ID. Must be run by the System tenant.",
			Volatility: tree.VolatilityVolatile,
		},
	),

	"crdb_internal.create_join_token": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				token, err := ctx.JoinTokenCreator.CreateJoinToken(ctx.Context)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(token), nil
			},
			Info:       "Creates a join token for use when adding a new node to a secure cluster.",
			Volatility: tree.VolatilityVolatile,
		},
	),

	"crdb_internal.destroy_tenant": makeBuiltin(
		tree.FunctionProperties{
			Category:     categoryMultiTenancy,
			Undocumented: true,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"id", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				sTenID := int64(tree.MustBeDInt(args[0]))
				if sTenID <= 0 {
					return nil, pgerror.New(pgcode.InvalidParameterValue, "tenant ID must be positive")
				}
				if err := ctx.Tenant.DestroyTenant(ctx.Context, uint64(sTenID)); err != nil {
					return nil, err
				}
				return args[0], nil
			},
			// TODO(spaskob): this built-in currently does not actually delete the
			// data but just marks it as DROP. This is for done for safety in case we
			// would like to restore the tenant later. If data in needs to be removed
			// use gc_tenant built-in.
			// We should just add a new built-in called `drop_tenant` instead and use
			// this one to really destroy the tenant.
			Info:       "Destroys a tenant with the provided ID. Must be run by the System tenant.",
			Volatility: tree.VolatilityVolatile,
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
					return nil, pgerror.Newf(
						pgcode.DatatypeMismatch,
						"expected tuple argument for row_tuple, found %s",
						args[2],
					)
				}

				// Get the referenced table and index.
				tableDescIntf, err := ctx.Planner.GetImmutableTableInterfaceByID(
					ctx.Context,
					tableID,
				)
				if err != nil {
					return nil, err
				}
				tableDesc := tableDescIntf.(catalog.TableDescriptor)
				index, err := tableDesc.FindIndexWithID(descpb.IndexID(indexID))
				if err != nil {
					return nil, err
				}
				// Collect the index columns. If the index is a non-unique secondary
				// index, it might have some extra key columns.
				indexColIDs := make([]descpb.ColumnID, index.NumKeyColumns(), index.NumKeyColumns()+index.NumKeySuffixColumns())
				for i := 0; i < index.NumKeyColumns(); i++ {
					indexColIDs[i] = index.GetKeyColumnID(i)
				}
				if index.GetID() != tableDesc.GetPrimaryIndexID() && !index.IsUnique() {
					for i := 0; i < index.NumKeySuffixColumns(); i++ {
						indexColIDs = append(indexColIDs, index.GetKeySuffixColumnID(i))
					}
				}

				// Ensure that the input tuple length equals the number of index cols.
				if len(rowDatums.D) != len(indexColIDs) {
					err := errors.Newf(
						"number of values must equal number of columns in index %q",
						index.GetName(),
					)
					// If the index has some extra key columns, then output an error
					// message with some extra information to explain the subtlety.
					if index.GetID() != tableDesc.GetPrimaryIndexID() && !index.IsUnique() && index.NumKeySuffixColumns() > 0 {
						var extraColNames []string
						for i := 0; i < index.NumKeySuffixColumns(); i++ {
							id := index.GetKeySuffixColumnID(i)
							col, colErr := tableDesc.FindColumnWithID(id)
							if colErr != nil {
								return nil, errors.CombineErrors(err, colErr)
							}
							extraColNames = append(extraColNames, col.GetName())
						}
						var allColNames []string
						for _, id := range indexColIDs {
							col, colErr := tableDesc.FindColumnWithID(id)
							if colErr != nil {
								return nil, errors.CombineErrors(err, colErr)
							}
							allColNames = append(allColNames, col.GetName())
						}
						return nil, errors.WithHintf(
							err,
							"columns %v are implicitly part of index %q's key, include columns %v in this order",
							extraColNames,
							index.GetName(),
							allColNames,
						)
					}
					return nil, err
				}

				// Check that the input datums are typed as the index columns types.
				var datums tree.Datums
				for i, d := range rowDatums.D {
					// We perform a cast here rather than a type check because datums
					// already have a fixed type, and not enough information is known at
					// typechecking time to ensure that the datums are typed with the
					// types of the index columns. So, try to cast the input datums to
					// the types of the index columns here.
					var newDatum tree.Datum
					col, err := tableDesc.FindColumnWithID(indexColIDs[i])
					if err != nil {
						return nil, err
					}
					if d.ResolvedType() == types.Unknown {
						if !col.IsNullable() {
							return nil, pgerror.Newf(pgcode.NotNullViolation, "NULL provided as a value for a non-nullable column")
						}
						newDatum = tree.DNull
					} else {
						expectedTyp := col.GetType()
						newDatum, err = tree.PerformCast(ctx, d, expectedTyp)
						if err != nil {
							return nil, errors.WithHint(err, "try to explicitly cast each value to the corresponding column type")
						}
					}
					datums = append(datums, newDatum)
				}

				// Create a column id to row index map. In this case, each column ID
				// just maps to the i'th ordinal.
				var colMap catalog.TableColMap
				for i, id := range indexColIDs {
					colMap.Set(id, i)
				}
				// Finally, encode the index key using the provided datums.
				keyPrefix := rowenc.MakeIndexKeyPrefix(ctx.Codec, tableDesc, index.GetID())
				res, _, err := rowenc.EncodePartialIndexKey(tableDesc, index, len(datums), colMap, datums, keyPrefix)
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(res)), err
			},
			Info:       "Generate the key for a row on a particular table and index.",
			Volatility: tree.VolatilityStable,
		},
	),

	"crdb_internal.force_error": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"errorCode", types.String}, {"msg", types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				errCode := string(*args[0].(*tree.DString))
				msg := string(*args[1].(*tree.DString))
				// We construct the errors below via %s as the
				// message may contain PII.
				if errCode == "" {
					return nil, errors.Newf("%s", msg)
				}
				return nil, pgerror.Newf(pgcode.MakeCode(errCode), "%s", msg)
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: tree.VolatilityVolatile,
		},
	),

	"crdb_internal.notice": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"msg", types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				msg := string(*args[0].(*tree.DString))
				return crdbInternalSendNotice(ctx, "NOTICE", msg)
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: tree.VolatilityVolatile,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"severity", types.String}, {"msg", types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				severityString := string(*args[0].(*tree.DString))
				msg := string(*args[1].(*tree.DString))
				if _, ok := pgnotice.ParseDisplaySeverity(severityString); !ok {
					return nil, pgerror.Newf(pgcode.InvalidParameterValue, "severity %s is invalid", severityString)
				}
				return crdbInternalSendNotice(ctx, severityString, msg)
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: tree.VolatilityVolatile,
		},
	),

	"crdb_internal.force_assertion_error": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"msg", types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				msg := string(*args[0].(*tree.DString))
				return nil, errors.AssertionFailedf("%s", msg)
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: tree.VolatilityVolatile,
		},
	),

	"crdb_internal.force_panic": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"msg", types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if err := checkPrivilegedUser(ctx); err != nil {
					return nil, err
				}
				msg := string(*args[0].(*tree.DString))
				// Use a special method to panic in order to go around the
				// vectorized panic-catcher (which would catch the panic from
				// Golang's 'panic' and would convert it into an internal
				// error).
				colexecerror.NonCatchablePanic(msg)
				// This code is unreachable.
				panic(msg)
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: tree.VolatilityVolatile,
		},
	),

	"crdb_internal.force_log_fatal": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"msg", types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if err := checkPrivilegedUser(ctx); err != nil {
					return nil, err
				}
				msg := string(*args[0].(*tree.DString))
				log.Fatalf(ctx.Ctx(), "force_log_fatal(): %s", msg)
				return nil, nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: tree.VolatilityVolatile,
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
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: tree.VolatilityVolatile,
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
				b := &kv.Batch{}
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
			Info:       "This function is used to fetch the leaseholder corresponding to a request key",
			Volatility: tree.VolatilityVolatile,
		},
	),

	// Identity function which is marked as impure to avoid constant folding.
	"crdb_internal.no_constant_folding": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"input", types.Any}},
			ReturnType: tree.IdentityReturnType(0),
			Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return args[0], nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: tree.VolatilityVolatile,
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
				return tree.NewDString(catalogkeys.PrettyKey(
					nil, /* valDirs */
					roachpb.Key(tree.MustBeDBytes(args[0])),
					int(tree.MustBeDInt(args[1])))), nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: tree.VolatilityImmutable,
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
				b := &kv.Batch{}
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
			Info:       "This function is used to retrieve range statistics information as a JSON object.",
			Volatility: tree.VolatilityVolatile,
		},
	),

	// Returns a namespace_id based on parentID and a given name.
	// Allows a non-admin to query the system.namespace table, but performs
	// the relevant permission checks to ensure secure access.
	// Returns NULL if none is found.
	// Errors if there is no permission for the current user to view the descriptor.
	"crdb_internal.get_namespace_id": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types:      tree.ArgTypes{{"parent_id", types.Int}, {"name", types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				parentID := tree.MustBeDInt(args[0])
				name := tree.MustBeDString(args[1])
				id, found, err := ctx.PrivilegedAccessor.LookupNamespaceID(
					ctx.Context,
					int64(parentID),
					string(name),
				)
				if err != nil {
					return nil, err
				}
				if !found {
					return tree.DNull, nil
				}
				return tree.NewDInt(id), nil
			},
			Volatility: tree.VolatilityStable,
		},
	),

	// Returns the descriptor of a database based on its name.
	// Allows a non-admin to query the system.namespace table, but performs
	// the relevant permission checks to ensure secure access.
	// Returns NULL if none is found.
	// Errors if there is no permission for the current user to view the descriptor.
	"crdb_internal.get_database_id": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types:      tree.ArgTypes{{"name", types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				name := tree.MustBeDString(args[0])
				id, found, err := ctx.PrivilegedAccessor.LookupNamespaceID(
					ctx.Context,
					int64(0),
					string(name),
				)
				if err != nil {
					return nil, err
				}
				if !found {
					return tree.DNull, nil
				}
				return tree.NewDInt(id), nil
			},
			Volatility: tree.VolatilityStable,
		},
	),

	// Returns the zone config based on a given namespace id.
	// Returns NULL if a zone configuration is not found.
	// Errors if there is no permission for the current user to view the zone config.
	"crdb_internal.get_zone_config": makeBuiltin(
		tree.FunctionProperties{Category: categorySystemInfo},
		tree.Overload{
			Types:      tree.ArgTypes{{"namespace_id", types.Int}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				id := tree.MustBeDInt(args[0])
				bytes, found, err := ctx.PrivilegedAccessor.LookupZoneConfigByNamespaceID(
					ctx.Context,
					int64(id),
				)
				if err != nil {
					return nil, err
				}
				if !found {
					return tree.DNull, nil
				}
				return tree.NewDBytes(bytes), nil
			},
			Volatility: tree.VolatilityStable,
		},
	),

	"crdb_internal.set_vmodule": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
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
			Volatility: tree.VolatilityVolatile,
		},
	),

	"crdb_internal.get_vmodule": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				if err := checkPrivilegedUser(ctx); err != nil {
					return nil, err
				}
				return tree.NewDString(log.GetVModule()), nil
			},
			Info:       "Returns the vmodule configuration on the gateway node processing this request.",
			Volatility: tree.VolatilityVolatile,
		},
	),

	// Returns the number of distinct inverted index entries that would be
	// generated for a value.
	"crdb_internal.num_geo_inverted_index_entries": makeBuiltin(
		tree.FunctionProperties{
			Category:     categorySystemInfo,
			NullableArgs: true,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"table_id", types.Int},
				{"index_id", types.Int},
				{"val", types.Geography},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull || args[1] == tree.DNull || args[2] == tree.DNull {
					return tree.DZero, nil
				}
				tableID := int(tree.MustBeDInt(args[0]))
				indexID := int(tree.MustBeDInt(args[1]))
				g := tree.MustBeDGeography(args[2])
				// TODO(ajwerner): This should be able to use the normal lookup mechanisms.
				tableDesc, err := catalogkv.MustGetTableDescByID(ctx.Context, ctx.Txn, ctx.Codec, descpb.ID(tableID))
				if err != nil {
					return nil, err
				}
				index, err := tableDesc.FindIndexWithID(descpb.IndexID(indexID))
				if err != nil {
					return nil, err
				}
				if index.GetGeoConfig().S2Geography == nil {
					return nil, errors.Errorf("index_id %d is not a geography inverted index", indexID)
				}
				keys, err := rowenc.EncodeGeoInvertedIndexTableKeys(g, nil, index.GetGeoConfig())
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(len(keys))), nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: tree.VolatilityStable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"table_id", types.Int},
				{"index_id", types.Int},
				{"val", types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull || args[1] == tree.DNull || args[2] == tree.DNull {
					return tree.DZero, nil
				}
				tableID := int(tree.MustBeDInt(args[0]))
				indexID := int(tree.MustBeDInt(args[1]))
				g := tree.MustBeDGeometry(args[2])
				tableDesc, err := catalogkv.MustGetTableDescByID(ctx.Context, ctx.Txn, ctx.Codec, descpb.ID(tableID))
				if err != nil {
					return nil, err
				}
				index, err := tableDesc.FindIndexWithID(descpb.IndexID(indexID))
				if err != nil {
					return nil, err
				}
				if index.GetGeoConfig().S2Geometry == nil {
					return nil, errors.Errorf("index_id %d is not a geometry inverted index", indexID)
				}
				keys, err := rowenc.EncodeGeoInvertedIndexTableKeys(g, nil, index.GetGeoConfig())
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(len(keys))), nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: tree.VolatilityStable,
		}),
	// Returns the number of distinct inverted index entries that would be
	// generated for a value.
	"crdb_internal.num_inverted_index_entries": makeBuiltin(
		tree.FunctionProperties{
			Category:     categorySystemInfo,
			NullableArgs: true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.Jsonb}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return jsonNumInvertedIndexEntries(ctx, args[0])
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: tree.VolatilityStable,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"val", types.AnyArray}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return arrayNumInvertedIndexEntries(ctx, args[0], tree.DNull)
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: tree.VolatilityStable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"val", types.Jsonb},
				{"version", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				// The version argument is currently ignored for JSON inverted indexes,
				// since all prior versions of JSON inverted indexes include the same
				// entries. (The version argument was introduced for array indexes,
				// since prior versions of array indexes did not include keys for empty
				// arrays.)
				return jsonNumInvertedIndexEntries(ctx, args[0])
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: tree.VolatilityStable,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"val", types.AnyArray},
				{"version", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return arrayNumInvertedIndexEntries(ctx, args[0], args[1])
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: tree.VolatilityStable,
		}),

	// Returns true iff the current user has admin role.
	// Note: it would be a privacy leak to extend this to check arbitrary usernames.
	"crdb_internal.is_admin": makeBuiltin(
		tree.FunctionProperties{
			Category:         categorySystemInfo,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(evalCtx *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
				if evalCtx.SessionAccessor == nil {
					return nil, errors.AssertionFailedf("session accessor not set")
				}
				ctx := evalCtx.Ctx()
				isAdmin, err := evalCtx.SessionAccessor.HasAdminRole(ctx)
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(isAdmin)), nil
			},
			Info:       "Retrieves the current user's admin status.",
			Volatility: tree.VolatilityStable,
		},
	),

	// Returns true iff the current user has the specified role option.
	// Note: it would be a privacy leak to extend this to check arbitrary usernames.
	"crdb_internal.has_role_option": makeBuiltin(
		tree.FunctionProperties{
			Category:         categorySystemInfo,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"option", types.String},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if evalCtx.SessionAccessor == nil {
					return nil, errors.AssertionFailedf("session accessor not set")
				}
				optionStr := string(tree.MustBeDString(args[0]))
				option, ok := roleoption.ByName[optionStr]
				if !ok {
					return nil, errors.Newf("unrecognized role option %s", optionStr)
				}
				ctx := evalCtx.Ctx()
				ok, err := evalCtx.SessionAccessor.HasRoleOption(ctx, option)
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(ok)), nil
			},
			Info:       "Returns whether the current user has the specified role option",
			Volatility: tree.VolatilityStable,
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
			Info:       "This function is used internally to round decimal values during mutations.",
			Volatility: tree.VolatilityImmutable,
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
			Info:       "This function is used internally to round decimal array values during mutations.",
			Volatility: tree.VolatilityStable,
		},
	),
	"crdb_internal.completed_migrations": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.StringArray),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				prefix := ctx.Codec.MigrationKeyPrefix()
				keyvals, err := ctx.Txn.Scan(ctx.Context, prefix, prefix.PrefixEnd(), 0 /* maxRows */)
				if err != nil {
					return nil, errors.Wrapf(err, "failed to get list of completed migrations")
				}
				ret := &tree.DArray{ParamTyp: types.String, Array: make(tree.Datums, 0, len(keyvals))}
				for _, keyval := range keyvals {
					key := keyval.Key
					if len(key) > len(keys.MigrationPrefix) {
						key = key[len(keys.MigrationPrefix):]
					}
					ret.Array = append(ret.Array, tree.NewDString(string(key)))
				}
				return ret, nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: tree.VolatilityVolatile,
		},
	),
	"crdb_internal.unsafe_upsert_descriptor": makeBuiltin(
		tree.FunctionProperties{
			Category:         categorySystemRepair,
			DistsqlBlocklist: true,
			Undocumented:     true,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"id", types.Int},
				{"desc", types.Bytes},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if err := ctx.Planner.UnsafeUpsertDescriptor(ctx.Context,
					int64(*args[0].(*tree.DInt)),
					[]byte(*args[1].(*tree.DBytes)),
					false /* force */); err != nil {
					return nil, err
				}
				return tree.DBoolTrue, nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: tree.VolatilityVolatile,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"id", types.Int},
				{"desc", types.Bytes},
				{"force", types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if err := ctx.Planner.UnsafeUpsertDescriptor(ctx.Context,
					int64(*args[0].(*tree.DInt)),
					[]byte(*args[1].(*tree.DBytes)),
					bool(*args[2].(*tree.DBool))); err != nil {
					return nil, err
				}
				return tree.DBoolTrue, nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: tree.VolatilityVolatile,
		},
	),
	"crdb_internal.unsafe_delete_descriptor": makeBuiltin(
		tree.FunctionProperties{
			Category:         categorySystemRepair,
			DistsqlBlocklist: true,
			Undocumented:     true,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"id", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if err := ctx.Planner.UnsafeDeleteDescriptor(ctx.Context,
					int64(*args[0].(*tree.DInt)),
					false, /* force */
				); err != nil {
					return nil, err
				}
				return tree.DBoolTrue, nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: tree.VolatilityVolatile,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"id", types.Int},
				{"force", types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if err := ctx.Planner.UnsafeDeleteDescriptor(ctx.Context,
					int64(*args[0].(*tree.DInt)),
					bool(*args[1].(*tree.DBool)),
				); err != nil {
					return nil, err
				}
				return tree.DBoolTrue, nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: tree.VolatilityVolatile,
		},
	),
	"crdb_internal.unsafe_upsert_namespace_entry": makeBuiltin(
		tree.FunctionProperties{
			Category:         categorySystemRepair,
			DistsqlBlocklist: true,
			Undocumented:     true,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"parent_id", types.Int},
				{"parent_schema_id", types.Int},
				{"name", types.String},
				{"desc_id", types.Int},
				{"force", types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if err := ctx.Planner.UnsafeUpsertNamespaceEntry(
					ctx.Context,
					int64(*args[0].(*tree.DInt)),     // parentID
					int64(*args[1].(*tree.DInt)),     // parentSchemaID
					string(*args[2].(*tree.DString)), // name
					int64(*args[3].(*tree.DInt)),     // descID
					bool(*args[4].(*tree.DBool)),     // force
				); err != nil {
					return nil, err
				}
				return tree.DBoolTrue, nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: tree.VolatilityVolatile,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"parent_id", types.Int},
				{"parent_schema_id", types.Int},
				{"name", types.String},
				{"desc_id", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if err := ctx.Planner.UnsafeUpsertNamespaceEntry(
					ctx.Context,
					int64(*args[0].(*tree.DInt)),     // parentID
					int64(*args[1].(*tree.DInt)),     // parentSchemaID
					string(*args[2].(*tree.DString)), // name
					int64(*args[3].(*tree.DInt)),     // descID
					false,                            // force
				); err != nil {
					return nil, err
				}
				return tree.DBoolTrue, nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: tree.VolatilityVolatile,
		},
	),
	"crdb_internal.unsafe_delete_namespace_entry": makeBuiltin(
		tree.FunctionProperties{
			Category:         categorySystemRepair,
			DistsqlBlocklist: true,
			Undocumented:     true,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"parent_id", types.Int},
				{"parent_schema_id", types.Int},
				{"name", types.String},
				{"desc_id", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if err := ctx.Planner.UnsafeDeleteNamespaceEntry(
					ctx.Context,
					int64(*args[0].(*tree.DInt)),     // parentID
					int64(*args[1].(*tree.DInt)),     // parentSchemaID
					string(*args[2].(*tree.DString)), // name
					int64(*args[3].(*tree.DInt)),     // id
					false,                            // force
				); err != nil {
					return nil, err
				}
				return tree.DBoolTrue, nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: tree.VolatilityVolatile,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"parent_id", types.Int},
				{"parent_schema_id", types.Int},
				{"name", types.String},
				{"desc_id", types.Int},
				{"force", types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if err := ctx.Planner.UnsafeDeleteNamespaceEntry(
					ctx.Context,
					int64(*args[0].(*tree.DInt)),     // parentID
					int64(*args[1].(*tree.DInt)),     // parentSchemaID
					string(*args[2].(*tree.DString)), // name
					int64(*args[3].(*tree.DInt)),     // id
					bool(*args[4].(*tree.DBool)),     // force
				); err != nil {
					return nil, err
				}
				return tree.DBoolTrue, nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: tree.VolatilityVolatile,
		},
	),

	// Returns true iff the given sqlliveness session is not expired.
	"crdb_internal.sql_liveness_is_alive": makeBuiltin(
		tree.FunctionProperties{Category: categoryMultiTenancy},
		tree.Overload{
			Types:      tree.ArgTypes{{"session_id", types.Bytes}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				sid := sqlliveness.SessionID(*(args[0].(*tree.DBytes)))
				live, err := evalCtx.SQLLivenessReader.IsAlive(evalCtx.Context, sid)
				if err != nil {
					return tree.MakeDBool(true), err
				}
				return tree.MakeDBool(tree.DBool(live)), nil
			},
			Info:       "Checks is given sqlliveness session id is not expired",
			Volatility: tree.VolatilityStable,
		},
	),

	"crdb_internal.gc_tenant": makeBuiltin(
		tree.FunctionProperties{
			Category:     categoryMultiTenancy,
			Undocumented: true,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"id", types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				sTenID := int64(tree.MustBeDInt(args[0]))
				if sTenID <= 0 {
					return nil, pgerror.New(pgcode.InvalidParameterValue, "tenant ID must be positive")
				}
				if err := ctx.Tenant.GCTenant(ctx.Context, uint64(sTenID)); err != nil {
					return nil, err
				}
				return args[0], nil
			},
			Info:       "Garbage collects a tenant with the provided ID. Must be run by the System tenant.",
			Volatility: tree.VolatilityVolatile,
		},
	),

	"crdb_internal.compact_engine_span": makeBuiltin(
		tree.FunctionProperties{
			Category:         categorySystemRepair,
			DistsqlBlocklist: true,
			Undocumented:     true,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"node_id", types.Int},
				{"store_id", types.Int},
				{"start_key", types.Bytes},
				{"end_key", types.Bytes},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				nodeID := int32(tree.MustBeDInt(args[0]))
				storeID := int32(tree.MustBeDInt(args[1]))
				startKey := []byte(tree.MustBeDBytes(args[2]))
				endKey := []byte(tree.MustBeDBytes(args[3]))
				if err := ctx.CompactEngineSpan(
					ctx.Context, nodeID, storeID, startKey, endKey); err != nil {
					return nil, err
				}
				return tree.DBoolTrue, nil
			},
			Info: "This function is used only by CockroachDB's developers for restoring engine health. " +
				"It is used to compact a span of the engine at the given node and store. The start and " +
				"end keys are bytes. To compact a particular rangeID, one can do: " +
				"SELECT crdb_internal.compact_engine_span(<node>, <store>, start_key, end_key) " +
				"FROM crdb_internal.ranges_no_leases WHERE range_id=<value>. If one has hex or escape " +
				"formatted bytea, one can use decode(<key string>, 'hex'|'escape') as the parameter. " +
				"The compaction is run synchronously, so this function may take a long time to return. " +
				"One can use the logs at the node to confirm that a compaction has started.",
			Volatility: tree.VolatilityVolatile,
		},
	),

	"crdb_internal.increment_feature_counter": makeBuiltin(
		tree.FunctionProperties{
			Category:     categorySystemInfo,
			Undocumented: true,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"feature", types.String}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				feature := string(*args[0].(*tree.DString))
				telemetry.Inc(sqltelemetry.HashedFeatureCounter(feature))
				return tree.DBoolTrue, nil
			},
			Info: "This function can be used to report the usage of an arbitrary feature. The " +
				"feature name is hashed for privacy purposes.",
			Volatility: tree.VolatilityVolatile,
		},
	),

	"crdb_internal.complete_stream_ingestion_job": makeBuiltin(
		tree.FunctionProperties{
			Category:         categoryStreamIngestion,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types: tree.ArgTypes{
				{"job_id", types.Int},
				{"cutover_ts", types.TimestampTZ},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				jobID := int(*args[0].(*tree.DInt))
				cutoverTime := args[1].(*tree.DTimestampTZ).Time
				cutoverTimestamp := hlc.Timestamp{WallTime: cutoverTime.UnixNano()}
				if streaming.CompleteIngestionHook == nil {
					return nil, errors.New("completing a stream ingestion job requires a CCL binary")
				}
				err := streaming.CompleteIngestionHook(evalCtx, evalCtx.Txn, jobID, cutoverTimestamp)
				return tree.NewDInt(tree.DInt(jobID)), err
			},
			Info: "This function can be used to signal a running stream ingestion job to complete. " +
				"The job will eventually stop ingesting, revert to the specified timestamp and leave the " +
				"cluster in a consistent state. The specified timestamp can only be specified up to the" +
				" microsecond. " +
				"This function does not wait for the job to reach a terminal state, " +
				"but instead returns the job id as soon as it has signaled the job to complete. " +
				"This builtin can be used in conjunction with SHOW JOBS WHEN COMPLETE to ensure that the" +
				" job has left the cluster in a consistent state.",
			Volatility: tree.VolatilityVolatile,
		},
	),

	"num_nulls": makeBuiltin(
		tree.FunctionProperties{
			Category:     categoryComparison,
			NullableArgs: true,
		},
		tree.Overload{
			Types: tree.VariadicType{
				VarType: types.Any,
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				var numNulls int
				for _, arg := range args {
					if arg == tree.DNull {
						numNulls++
					}
				}
				return tree.NewDInt(tree.DInt(numNulls)), nil
			},
			Info:       "Returns the number of null arguments.",
			Volatility: tree.VolatilityImmutable,
		},
	),
	"num_nonnulls": makeBuiltin(
		tree.FunctionProperties{
			Category:     categoryComparison,
			NullableArgs: true,
		},
		tree.Overload{
			Types: tree.VariadicType{
				VarType: types.Any,
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				var numNonNulls int
				for _, arg := range args {
					if arg != tree.DNull {
						numNonNulls++
					}
				}
				return tree.NewDInt(tree.DInt(numNonNulls)), nil
			},
			Info:       "Returns the number of nonnull arguments.",
			Volatility: tree.VolatilityImmutable,
		},
	),

	GatewayRegionBuiltinName: makeBuiltin(
		tree.FunctionProperties{Category: categoryMultiRegion},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(evalCtx *tree.EvalContext, arg tree.Datums) (tree.Datum, error) {
				region, found := evalCtx.Locality.Find("region")
				if !found {
					return nil, pgerror.Newf(
						pgcode.ConfigFile,
						"no region set on the locality flag on this node",
					)
				}
				return tree.NewDString(region), nil
			},
			Info: `Returns the region of the connection's current node as defined by
the locality flag on node startup. Returns an error if no region is set.`,
			Volatility: tree.VolatilityStable,
		},
	),
	DefaultToDatabasePrimaryRegionBuiltinName: makeBuiltin(
		tree.FunctionProperties{Category: categoryMultiRegion},
		stringOverload1(
			func(evalCtx *tree.EvalContext, s string) (tree.Datum, error) {
				regionConfig, err := evalCtx.Sequence.CurrentDatabaseRegionConfig(evalCtx.Context)
				if err != nil {
					return nil, err
				}
				if regionConfig == nil {
					return nil, pgerror.Newf(
						pgcode.InvalidDatabaseDefinition,
						"current database %s is not multi-region enabled",
						evalCtx.SessionData.Database,
					)
				}
				if regionConfig.IsValidRegionNameString(s) {
					return tree.NewDString(s), nil
				}
				primaryRegion := regionConfig.PrimaryRegionString()
				return tree.NewDString(primaryRegion), nil
			},
			types.String,
			`Returns the given region if the region has been added to the current database.
	Otherwise, this will return the primary region of the current database.
	This will error if the current database is not a multi-region database.`,
			tree.VolatilityStable,
		),
	),
	"crdb_internal.validate_multi_region_zone_configs": makeBuiltin(
		tree.FunctionProperties{Category: categoryMultiRegion},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.Sequence.ValidateAllMultiRegionZoneConfigsInCurrentDatabase(
					evalCtx.Context,
				); err != nil {
					return nil, err
				}
				return tree.MakeDBool(true), nil
			},
			Info: `Validates all multi-region zone configurations are correctly setup
			for the current database, including all tables, indexes and partitions underneath.
			Returns an error if validation fails. This builtin uses un-leased versions of the
			each descriptor, requiring extra round trips.`,
			Volatility: tree.VolatilityVolatile,
		},
	),
	"crdb_internal.filter_multiregion_fields_from_zone_config_sql": makeBuiltin(
		tree.FunctionProperties{Category: categoryMultiRegion},
		stringOverload1(
			func(evalCtx *tree.EvalContext, s string) (tree.Datum, error) {
				stmt, err := parser.ParseOne(s)
				if err != nil {
					// Return the same statement if it does not parse.
					// This can happen for invalid zone config state, in which case
					// it is better not to error as opposed to blocking SHOW CREATE TABLE.
					return tree.NewDString(s), nil //nolint:returnerrcheck
				}
				zs, ok := stmt.AST.(*tree.SetZoneConfig)
				if !ok {
					return nil, errors.Newf("invalid CONFIGURE ZONE statement (type %T): %s", stmt.AST, stmt)
				}
				newKVOptions := zs.Options[:0]
				for _, opt := range zs.Options {
					if _, ok := zonepb.MultiRegionZoneConfigFieldsSet[opt.Key]; !ok {
						newKVOptions = append(newKVOptions, opt)
					}
				}
				if len(newKVOptions) == 0 {
					return tree.DNull, nil
				}
				zs.Options = newKVOptions
				return tree.NewDString(zs.String()), nil
			},
			types.String,
			`Takes in a CONFIGURE ZONE SQL statement and returns a modified
SQL statement omitting multi-region related zone configuration fields.
If the CONFIGURE ZONE statement can be inferred by the database's or
table's zone configuration this will return NULL.`,
			tree.VolatilityStable,
		),
	),

	"crdb_internal.reset_sql_stats": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				if evalCtx.SQLStatsResetter == nil {
					return nil, errors.AssertionFailedf("sql stats resetter not set")
				}
				ctx := evalCtx.Ctx()
				if err := evalCtx.SQLStatsResetter.ResetClusterSQLStats(ctx); err != nil {
					return nil, err
				}
				return tree.MakeDBool(true), nil
			},
			Info:       `This function is used to clear the collected SQL statistics.`,
			Volatility: tree.VolatilityVolatile,
		},
	),
	// Deletes the underlying spans backing a table, only
	// if the user provides explicit acknowledgement of the
	// form "I acknowledge this will irrevocably delete all revisions
	// for table %d"
	"crdb_internal.force_delete_table_data": makeBuiltin(
		tree.FunctionProperties{
			Category: categorySystemRepair,
		},
		tree.Overload{
			Types:      tree.ArgTypes{{"id", types.Int}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				id := int64(*args[0].(*tree.DInt))

				err := ctx.Planner.ForceDeleteTableData(ctx.Context, id)
				if err != nil {
					return tree.DBoolFalse, err
				}
				return tree.DBoolTrue, err
			},
			Info:       "This function can be used to clear the data belonging to a table, when the table cannot be dropped.",
			Volatility: tree.VolatilityVolatile,
		},
	),
}

var lengthImpls = func(incBitOverload bool) builtinDefinition {
	b := makeBuiltin(tree.FunctionProperties{Category: categoryString},
		stringOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				return tree.NewDInt(tree.DInt(utf8.RuneCountInString(s))), nil
			},
			types.Int,
			"Calculates the number of characters in `val`.",
			tree.VolatilityImmutable,
		),
		bytesOverload1(
			func(_ *tree.EvalContext, s string) (tree.Datum, error) {
				return tree.NewDInt(tree.DInt(len(s))), nil
			},
			types.Int,
			"Calculates the number of bytes in `val`.",
			tree.VolatilityImmutable,
		),
	)
	if incBitOverload {
		b.overloads = append(
			b.overloads,
			bitsOverload1(
				func(_ *tree.EvalContext, s *tree.DBitArray) (tree.Datum, error) {
					return tree.NewDInt(tree.DInt(s.BitArray.BitLen())), nil
				}, types.Int, "Calculates the number of bits in `val`.",
				tree.VolatilityImmutable,
			),
		)
	}
	return b
}

var substringImpls = makeBuiltin(tree.FunctionProperties{Category: categoryString},
	tree.Overload{
		Types: tree.ArgTypes{
			{"input", types.String},
			{"start_pos", types.Int},
		},
		ReturnType: tree.FixedReturnType(types.String),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			substring := getSubstringFromIndex(string(tree.MustBeDString(args[0])), int(tree.MustBeDInt(args[1])))
			return tree.NewDString(substring), nil
		},
		Info:       "Returns a substring of `input` starting at `start_pos` (count starts at 1).",
		Volatility: tree.VolatilityImmutable,
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
			str := string(tree.MustBeDString(args[0]))
			start := int(tree.MustBeDInt(args[1]))
			length := int(tree.MustBeDInt(args[2]))

			substring, err := getSubstringFromIndexOfLength(str, "substring", start, length)
			if err != nil {
				return nil, err
			}
			return tree.NewDString(substring), nil
		},
		Info: "Returns a substring of `input` starting at `start_pos` (count starts at 1) and " +
			"including up to `length` characters.",
		Volatility: tree.VolatilityImmutable,
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
		Info:       "Returns a substring of `input` that matches the regular expression `regex`.",
		Volatility: tree.VolatilityImmutable,
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
		Volatility: tree.VolatilityImmutable,
	},
	tree.Overload{
		Types: tree.ArgTypes{
			{"input", types.VarBit},
			{"start_pos", types.Int},
		},
		ReturnType: tree.FixedReturnType(types.VarBit),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			bitString := tree.MustBeDBitArray(args[0])
			start := int(tree.MustBeDInt(args[1]))
			substring := getSubstringFromIndex(bitString.BitArray.String(), start)
			return tree.ParseDBitArray(substring)
		},
		Info:       "Returns a bit subarray of `input` starting at `start_pos` (count starts at 1).",
		Volatility: tree.VolatilityImmutable,
	},
	tree.Overload{
		Types: tree.ArgTypes{
			{"input", types.VarBit},
			{"start_pos", types.Int},
			{"length", types.Int},
		},
		ReturnType: tree.FixedReturnType(types.VarBit),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			bitString := tree.MustBeDBitArray(args[0])
			start := int(tree.MustBeDInt(args[1]))
			length := int(tree.MustBeDInt(args[2]))

			substring, err := getSubstringFromIndexOfLength(bitString.BitArray.String(), "bit subarray", start, length)
			if err != nil {
				return nil, err
			}
			return tree.ParseDBitArray(substring)
		},
		Info: "Returns a bit subarray of `input` starting at `start_pos` (count starts at 1) and " +
			"including up to `length` characters.",
		Volatility: tree.VolatilityImmutable,
	},
	tree.Overload{
		Types: tree.ArgTypes{
			{"input", types.Bytes},
			{"start_pos", types.Int},
		},
		ReturnType: tree.FixedReturnType(types.Bytes),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			byteString := string(*args[0].(*tree.DBytes))
			start := int(tree.MustBeDInt(args[1]))
			substring := getSubstringFromIndexBytes(byteString, start)
			return tree.NewDBytes(tree.DBytes(substring)), nil
		},
		Info:       "Returns a byte subarray of `input` starting at `start_pos` (count starts at 1).",
		Volatility: tree.VolatilityImmutable,
	},
	tree.Overload{
		Types: tree.ArgTypes{
			{"input", types.Bytes},
			{"start_pos", types.Int},
			{"length", types.Int},
		},
		ReturnType: tree.FixedReturnType(types.Bytes),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			byteString := string(*args[0].(*tree.DBytes))
			start := int(tree.MustBeDInt(args[1]))
			length := int(tree.MustBeDInt(args[2]))

			substring, err := getSubstringFromIndexOfLengthBytes(byteString, "byte subarray", start, length)
			if err != nil {
				return nil, err
			}
			return tree.NewDBytes(tree.DBytes(substring)), nil
		},
		Info: "Returns a byte subarray of `input` starting at `start_pos` (count starts at 1) and " +
			"including up to `length` characters.",
		Volatility: tree.VolatilityImmutable,
	},
)

// Returns a substring of given string starting at given position.
func getSubstringFromIndex(str string, start int) string {
	runes := []rune(str)
	// SQL strings are 1-indexed.
	start--

	if start < 0 {
		start = 0
	} else if start > len(runes) {
		start = len(runes)
	}
	return string(runes[start:])
}

// Returns a substring of given string starting at given position and
// include up to a certain length.
func getSubstringFromIndexOfLength(str, errMsg string, start, length int) (string, error) {
	runes := []rune(str)
	// SQL strings are 1-indexed.
	start--

	if length < 0 {
		return "", pgerror.Newf(
			pgcode.InvalidParameterValue, "negative %s length %d not allowed", errMsg, length)
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
	return string(runes[start:end]), nil
}

// Returns a substring of given string starting at given position by
// interpreting the string as raw bytes.
func getSubstringFromIndexBytes(str string, start int) string {
	bytes := []byte(str)
	// SQL strings are 1-indexed.
	start--

	if start < 0 {
		start = 0
	} else if start > len(bytes) {
		start = len(bytes)
	}
	return string(bytes[start:])
}

// Returns a substring of given string starting at given position and include up
// to a certain length by interpreting the string as raw bytes.
func getSubstringFromIndexOfLengthBytes(str, errMsg string, start, length int) (string, error) {
	bytes := []byte(str)
	// SQL strings are 1-indexed.
	start--

	if length < 0 {
		return "", pgerror.Newf(
			pgcode.InvalidParameterValue, "negative %s length %d not allowed", errMsg, length)
	}

	end := start + length
	// Check for integer overflow.
	if end < start {
		end = len(bytes)
	} else if end < 0 {
		end = 0
	} else if end > len(bytes) {
		end = len(bytes)
	}

	if start < 0 {
		start = 0
	} else if start > len(bytes) {
		start = len(bytes)
	}
	return string(bytes[start:end]), nil
}

var generateRandomUUIDImpl = makeBuiltin(
	tree.FunctionProperties{
		Category: categoryIDGeneration,
	},
	tree.Overload{
		Types:      tree.ArgTypes{},
		ReturnType: tree.FixedReturnType(types.Uuid),
		Fn: func(_ *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
			uv := uuid.MakeV4()
			return tree.NewDUuid(tree.DUuid{UUID: uv}), nil
		},
		Info:       "Generates a random UUID and returns it as a value of UUID type.",
		Volatility: tree.VolatilityVolatile,
	},
)

var uuidV4Impl = makeBuiltin(
	tree.FunctionProperties{
		Category: categoryIDGeneration,
	},
	tree.Overload{
		Types:      tree.ArgTypes{},
		ReturnType: tree.FixedReturnType(types.Bytes),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return tree.NewDBytes(tree.DBytes(uuid.MakeV4().GetBytes())), nil
		},
		Info:       "Returns a UUID.",
		Volatility: tree.VolatilityVolatile,
	},
)

const txnTSContextDoc = `

The value is based on a timestamp picked when the transaction starts
and which stays constant throughout the transaction. This timestamp
has no relationship with the commit order of concurrent transactions.`
const txnPreferredOverloadStr = `

This function is the preferred overload and will be evaluated by default.`
const txnTSDoc = `Returns the time of the current transaction.` + txnTSContextDoc

func getTimeAdditionalDesc(preferTZOverload bool) (string, string) {
	var tzAdditionalDesc, noTZAdditionalDesc string
	if preferTZOverload {
		tzAdditionalDesc = txnPreferredOverloadStr
	} else {
		noTZAdditionalDesc = txnPreferredOverloadStr
	}
	return tzAdditionalDesc, noTZAdditionalDesc
}

func txnTSOverloads(preferTZOverload bool) []tree.Overload {
	tzAdditionalDesc, noTZAdditionalDesc := getTimeAdditionalDesc(preferTZOverload)
	return []tree.Overload{
		{
			Types:             tree.ArgTypes{},
			ReturnType:        tree.FixedReturnType(types.TimestampTZ),
			PreferredOverload: preferTZOverload,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return ctx.GetTxnTimestamp(time.Microsecond), nil
			},
			Info:       txnTSDoc + tzAdditionalDesc,
			Volatility: tree.VolatilityStable,
		},
		{
			Types:             tree.ArgTypes{},
			ReturnType:        tree.FixedReturnType(types.Timestamp),
			PreferredOverload: !preferTZOverload,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return ctx.GetTxnTimestampNoZone(time.Microsecond), nil
			},
			Info:       txnTSDoc + noTZAdditionalDesc,
			Volatility: tree.VolatilityStable,
		},
		{
			Types:      tree.ArgTypes{},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn:         currentDate,
			Info:       txnTSDoc,
			Volatility: tree.VolatilityStable,
		},
	}
}

func txnTSWithPrecisionOverloads(preferTZOverload bool) []tree.Overload {
	tzAdditionalDesc, noTZAdditionalDesc := getTimeAdditionalDesc(preferTZOverload)
	return append(
		[]tree.Overload{
			{
				Types:             tree.ArgTypes{{"precision", types.Int}},
				ReturnType:        tree.FixedReturnType(types.TimestampTZ),
				PreferredOverload: preferTZOverload,
				Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
					prec := int32(tree.MustBeDInt(args[0]))
					if prec < 0 || prec > 6 {
						return nil, pgerror.Newf(pgcode.NumericValueOutOfRange, "precision %d out of range", prec)
					}
					return ctx.GetTxnTimestamp(tree.TimeFamilyPrecisionToRoundDuration(prec)), nil
				},
				Info:       txnTSDoc + tzAdditionalDesc,
				Volatility: tree.VolatilityStable,
			},
			{
				Types:             tree.ArgTypes{{"precision", types.Int}},
				ReturnType:        tree.FixedReturnType(types.Timestamp),
				PreferredOverload: !preferTZOverload,
				Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
					prec := int32(tree.MustBeDInt(args[0]))
					if prec < 0 || prec > 6 {
						return nil, pgerror.Newf(pgcode.NumericValueOutOfRange, "precision %d out of range", prec)
					}
					return ctx.GetTxnTimestampNoZone(tree.TimeFamilyPrecisionToRoundDuration(prec)), nil
				},
				Info:       txnTSDoc + noTZAdditionalDesc,
				Volatility: tree.VolatilityStable,
			},
			{
				Types:      tree.ArgTypes{{"precision", types.Int}},
				ReturnType: tree.FixedReturnType(types.Date),
				Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
					prec := int32(tree.MustBeDInt(args[0]))
					if prec < 0 || prec > 6 {
						return nil, pgerror.Newf(pgcode.NumericValueOutOfRange, "precision %d out of range", prec)
					}
					return currentDate(ctx, args)
				},
				Info:       txnTSDoc,
				Volatility: tree.VolatilityStable,
			},
		},
		txnTSOverloads(preferTZOverload)...,
	)
}

func txnTSImplBuiltin(preferTZOverload bool) builtinDefinition {
	return makeBuiltin(
		tree.FunctionProperties{
			Category: categoryDateAndTime,
		},
		txnTSOverloads(preferTZOverload)...,
	)
}

func txnTSWithPrecisionImplBuiltin(preferTZOverload bool) builtinDefinition {
	return makeBuiltin(
		tree.FunctionProperties{
			Category: categoryDateAndTime,
		},
		txnTSWithPrecisionOverloads(preferTZOverload)...,
	)
}

func txnTimeWithPrecisionBuiltin(preferTZOverload bool) builtinDefinition {
	tzAdditionalDesc, noTZAdditionalDesc := getTimeAdditionalDesc(preferTZOverload)
	return makeBuiltin(
		tree.FunctionProperties{},
		tree.Overload{
			Types:             tree.ArgTypes{},
			ReturnType:        tree.FixedReturnType(types.TimeTZ),
			PreferredOverload: preferTZOverload,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return ctx.GetTxnTime(time.Microsecond), nil
			},
			Info:       "Returns the current transaction's time with time zone." + tzAdditionalDesc,
			Volatility: tree.VolatilityStable,
		},
		tree.Overload{
			Types:             tree.ArgTypes{},
			ReturnType:        tree.FixedReturnType(types.Time),
			PreferredOverload: !preferTZOverload,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				return ctx.GetTxnTimeNoZone(time.Microsecond), nil
			},
			Info:       "Returns the current transaction's time with no time zone." + noTZAdditionalDesc,
			Volatility: tree.VolatilityStable,
		},
		tree.Overload{
			Types:             tree.ArgTypes{{"precision", types.Int}},
			ReturnType:        tree.FixedReturnType(types.TimeTZ),
			PreferredOverload: preferTZOverload,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				prec := int32(tree.MustBeDInt(args[0]))
				if prec < 0 || prec > 6 {
					return nil, pgerror.Newf(pgcode.NumericValueOutOfRange, "precision %d out of range", prec)
				}
				return ctx.GetTxnTime(tree.TimeFamilyPrecisionToRoundDuration(prec)), nil
			},
			Info:       "Returns the current transaction's time with time zone." + tzAdditionalDesc,
			Volatility: tree.VolatilityStable,
		},
		tree.Overload{
			Types:             tree.ArgTypes{{"precision", types.Int}},
			ReturnType:        tree.FixedReturnType(types.Time),
			PreferredOverload: !preferTZOverload,
			Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
				prec := int32(tree.MustBeDInt(args[0]))
				if prec < 0 || prec > 6 {
					return nil, pgerror.Newf(pgcode.NumericValueOutOfRange, "precision %d out of range", prec)
				}
				return ctx.GetTxnTimeNoZone(tree.TimeFamilyPrecisionToRoundDuration(prec)), nil
			},
			Info:       "Returns the current transaction's time with no time zone." + noTZAdditionalDesc,
			Volatility: tree.VolatilityStable,
		},
	)
}

func currentDate(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
	t := ctx.GetTxnTimestamp(time.Microsecond).Time
	t = t.In(ctx.GetLocation())
	return tree.NewDDateFromTime(t)
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
		result, err := jsonExtractPathHelper(args)
		if err != nil {
			return nil, err
		}
		if result == nil {
			return tree.DNull, nil
		}
		return &tree.DJSON{JSON: result}, nil
	},
	Info:       "Returns the JSON value pointed to by the variadic arguments.",
	Volatility: tree.VolatilityImmutable,
}

var jsonExtractPathTextImpl = tree.Overload{
	Types:      tree.VariadicType{FixedTypes: []*types.T{types.Jsonb}, VarType: types.String},
	ReturnType: tree.FixedReturnType(types.String),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		result, err := jsonExtractPathHelper(args)
		if err != nil {
			return nil, err
		}
		if result == nil {
			return tree.DNull, nil
		}
		text, err := result.AsText()
		if err != nil {
			return nil, err
		}
		if text == nil {
			return tree.DNull, nil
		}
		return tree.NewDString(*text), nil
	},
	Info:       "Returns the JSON value as text pointed to by the variadic arguments.",
	Volatility: tree.VolatilityImmutable,
}

func jsonExtractPathHelper(args tree.Datums) (json.JSON, error) {
	j := tree.MustBeDJSON(args[0])
	path := make([]string, len(args)-1)
	for i, v := range args {
		if i == 0 {
			continue
		}
		if v == tree.DNull {
			return nil, nil
		}
		path[i-1] = string(tree.MustBeDString(v))
	}
	return json.FetchPath(j.JSON, path)
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
	Info:       "Returns the JSON value pointed to by the variadic arguments.",
	Volatility: tree.VolatilityImmutable,
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
	Volatility: tree.VolatilityImmutable,
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
	Info:       "Returns the JSON value pointed to by the variadic arguments. `new_val` will be inserted before path target.",
	Volatility: tree.VolatilityImmutable,
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
	Volatility: tree.VolatilityImmutable,
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
	Info:       "Returns the type of the outermost JSON value as a text string.",
	Volatility: tree.VolatilityImmutable,
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

			key, err := asJSONBuildObjectKey(
				args[i],
				ctx.SessionData.DataConversionConfig,
				ctx.GetLocation(),
			)
			if err != nil {
				return nil, err
			}

			val, err := tree.AsJSON(
				args[i+1],
				ctx.SessionData.DataConversionConfig,
				ctx.GetLocation(),
			)
			if err != nil {
				return nil, err
			}

			builder.Add(key, val)
		}

		return tree.NewDJSON(builder.Build()), nil
	},
	Info:       "Builds a JSON object out of a variadic argument list.",
	Volatility: tree.VolatilityStable,
}

var toJSONImpl = tree.Overload{
	Types:      tree.ArgTypes{{"val", types.Any}},
	ReturnType: tree.FixedReturnType(types.Jsonb),
	Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		return toJSONObject(ctx, args[0])
	},
	Info:       "Returns the value as JSON or JSONB.",
	Volatility: tree.VolatilityStable,
}

var prettyPrintNotSupportedError = pgerror.Newf(pgcode.FeatureNotSupported, "pretty printing is not supported")

var arrayToJSONImpls = makeBuiltin(jsonProps(),
	tree.Overload{
		Types:      tree.ArgTypes{{"array", types.AnyArray}},
		ReturnType: tree.FixedReturnType(types.Jsonb),
		Fn:         toJSONImpl.Fn,
		Info:       "Returns the array as JSON or JSONB.",
		Volatility: tree.VolatilityStable,
	},
	tree.Overload{
		Types:      tree.ArgTypes{{"array", types.AnyArray}, {"pretty_bool", types.Bool}},
		ReturnType: tree.FixedReturnType(types.Jsonb),
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			prettyPrint := bool(tree.MustBeDBool(args[1]))
			if prettyPrint {
				return nil, prettyPrintNotSupportedError
			}
			return toJSONObject(ctx, args[0])
		},
		Info:       "Returns the array as JSON or JSONB.",
		Volatility: tree.VolatilityStable,
	},
)

var jsonBuildArrayImpl = tree.Overload{
	Types:      tree.VariadicType{VarType: types.Any},
	ReturnType: tree.FixedReturnType(types.Jsonb),
	Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		builder := json.NewArrayBuilder(len(args))
		for _, arg := range args {
			j, err := tree.AsJSON(
				arg,
				ctx.SessionData.DataConversionConfig,
				ctx.GetLocation(),
			)
			if err != nil {
				return nil, err
			}
			builder.Add(j)
		}
		return tree.NewDJSON(builder.Build()), nil
	},
	Info:       "Builds a possibly-heterogeneously-typed JSON or JSONB array out of a variadic argument list.",
	Volatility: tree.VolatilityStable,
}

var jsonObjectImpls = makeBuiltin(jsonProps(),
	tree.Overload{
		Types:      tree.ArgTypes{{"texts", types.StringArray}},
		ReturnType: tree.FixedReturnType(types.Jsonb),
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
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
				val, err := tree.AsJSON(
					arr.Array[i+1],
					ctx.SessionData.DataConversionConfig,
					ctx.GetLocation(),
				)
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
		Volatility: tree.VolatilityImmutable,
	},
	tree.Overload{
		Types: tree.ArgTypes{{"keys", types.StringArray},
			{"values", types.StringArray}},
		ReturnType: tree.FixedReturnType(types.Jsonb),
		Fn: func(ctx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
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
				val, err := tree.AsJSON(
					values.Array[i],
					ctx.SessionData.DataConversionConfig,
					ctx.GetLocation(),
				)
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
		Volatility: tree.VolatilityImmutable,
	},
)

var jsonStripNullsImpl = tree.Overload{
	Types:      tree.ArgTypes{{"from_json", types.Jsonb}},
	ReturnType: tree.FixedReturnType(types.Jsonb),
	Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
		j, _, err := tree.MustBeDJSON(args[0]).StripNulls()
		return tree.NewDJSON(j), err
	},
	Info:       "Returns from_json with all object fields that have null values omitted. Other null values are untouched.",
	Volatility: tree.VolatilityImmutable,
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
	Info:       "Returns the number of elements in the outermost JSON or JSONB array.",
	Volatility: tree.VolatilityImmutable,
}

var similarOverloads = []tree.Overload{
	{
		Types:      tree.ArgTypes{{"pattern", types.String}},
		ReturnType: tree.FixedReturnType(types.String),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			if args[0] == tree.DNull {
				return tree.DNull, nil
			}
			pattern := string(tree.MustBeDString(args[0]))
			return tree.SimilarPattern(pattern, "")
		},
		Info:       "Converts a SQL regexp `pattern` to a POSIX regexp `pattern`.",
		Volatility: tree.VolatilityImmutable,
	},
	{
		Types:      tree.ArgTypes{{"pattern", types.String}, {"escape", types.String}},
		ReturnType: tree.FixedReturnType(types.String),
		Fn: func(_ *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			if args[0] == tree.DNull {
				return tree.DNull, nil
			}
			pattern := string(tree.MustBeDString(args[0]))
			if args[1] == tree.DNull {
				return tree.SimilarPattern(pattern, "")
			}
			escape := string(tree.MustBeDString(args[1]))
			return tree.SimilarPattern(pattern, escape)
		},
		Info:       "Converts a SQL regexp `pattern` to a POSIX regexp `pattern` using `escape` as an escape token.",
		Volatility: tree.VolatilityImmutable,
	},
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

func jsonOverload1(
	f func(*tree.EvalContext, json.JSON) (tree.Datum, error),
	returnType *types.T,
	info string,
	volatility tree.Volatility,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{"val", types.Jsonb}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(evalCtx, tree.MustBeDJSON(args[0]).JSON)
		},
		Info:       info,
		Volatility: volatility,
	}
}

func stringOverload1(
	f func(*tree.EvalContext, string) (tree.Datum, error),
	returnType *types.T,
	info string,
	volatility tree.Volatility,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{"val", types.String}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(evalCtx, string(tree.MustBeDString(args[0])))
		},
		Info:       info,
		Volatility: volatility,
	}
}

func stringOverload2(
	a, b string,
	f func(*tree.EvalContext, string, string) (tree.Datum, error),
	returnType *types.T,
	info string,
	volatility tree.Volatility,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{a, types.String}, {b, types.String}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(evalCtx, string(tree.MustBeDString(args[0])), string(tree.MustBeDString(args[1])))
		},
		Info:       info,
		Volatility: volatility,
	}
}

func stringOverload3(
	a, b, c string,
	f func(*tree.EvalContext, string, string, string) (tree.Datum, error),
	returnType *types.T,
	info string,
	volatility tree.Volatility,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{a, types.String}, {b, types.String}, {c, types.String}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(evalCtx, string(tree.MustBeDString(args[0])), string(tree.MustBeDString(args[1])), string(tree.MustBeDString(args[2])))
		},
		Info:       info,
		Volatility: volatility,
	}
}

func bytesOverload1(
	f func(*tree.EvalContext, string) (tree.Datum, error),
	returnType *types.T,
	info string,
	volatility tree.Volatility,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{"val", types.Bytes}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(evalCtx, string(*args[0].(*tree.DBytes)))
		},
		Info:       info,
		Volatility: volatility,
	}
}

func bytesOverload2(
	a, b string,
	f func(*tree.EvalContext, string, string) (tree.Datum, error),
	returnType *types.T,
	info string,
	volatility tree.Volatility,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{a, types.Bytes}, {b, types.Bytes}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(evalCtx, string(*args[0].(*tree.DBytes)), string(*args[1].(*tree.DBytes)))
		},
		Info:       info,
		Volatility: volatility,
	}
}

func bitsOverload1(
	f func(*tree.EvalContext, *tree.DBitArray) (tree.Datum, error),
	returnType *types.T,
	info string,
	volatility tree.Volatility,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{"val", types.VarBit}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(evalCtx, args[0].(*tree.DBitArray))
		},
		Info:       info,
		Volatility: volatility,
	}
}

func bitsOverload2(
	a, b string,
	f func(*tree.EvalContext, *tree.DBitArray, *tree.DBitArray) (tree.Datum, error),
	returnType *types.T,
	info string,
	volatility tree.Volatility,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ArgTypes{{a, types.VarBit}, {b, types.VarBit}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(evalCtx *tree.EvalContext, args tree.Datums) (tree.Datum, error) {
			return f(evalCtx, args[0].(*tree.DBitArray), args[1].(*tree.DBitArray))
		},
		Info:       info,
		Volatility: volatility,
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
			Info:       info,
			Volatility: tree.VolatilityLeakProof,
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
			Info:       info,
			Volatility: tree.VolatilityLeakProof,
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
			Info:       info,
			Volatility: tree.VolatilityLeakProof,
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
			Info:       info,
			Volatility: tree.VolatilityLeakProof,
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
			Info:       info,
			Volatility: tree.VolatilityLeakProof,
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
			Info:       info,
			Volatility: tree.VolatilityLeakProof,
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

func regexpSplit(ctx *tree.EvalContext, args tree.Datums, hasFlags bool) ([]string, error) {
	s := string(tree.MustBeDString(args[0]))
	pattern := string(tree.MustBeDString(args[1]))
	sqlFlags := ""
	if hasFlags {
		sqlFlags = string(tree.MustBeDString(args[2]))
	}
	patternRe, err := ctx.ReCache.GetRegexp(regexpFlagKey{pattern, sqlFlags})
	if err != nil {
		return nil, err
	}
	return patternRe.Split(s, -1), nil
}

func regexpSplitToArray(
	ctx *tree.EvalContext, args tree.Datums, hasFlags bool,
) (tree.Datum, error) {
	words, err := regexpSplit(ctx, args, hasFlags /* hasFlags */)
	if err != nil {
		return nil, err
	}
	result := tree.NewDArray(types.String)
	for _, word := range words {
		if err := result.Append(tree.NewDString(word)); err != nil {
			return nil, err
		}
	}
	return result, nil
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
						captureGroupNumber := int(to[i] - '0')
						// regexpReplace expects references to "out-of-bounds"
						// and empty (when the corresponding match indices
						// are negative) capture groups to be ignored.
						if matchIndexPos := 2 * captureGroupNumber; matchIndexPos < len(matchIndex) {
							startPos := matchIndex[matchIndexPos]
							endPos := matchIndex[matchIndexPos+1]
							if startPos >= 0 {
								newString.WriteString(s[startPos:endPos])
							}
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

// NodeIDBits is the number of bits stored in the lower portion of
// GenerateUniqueInt.
const NodeIDBits = 15

// GenerateUniqueInt creates a unique int composed of the current time at a
// 10-microsecond granularity and the instance-id. The instance-id is stored in the
// lower 15 bits of the returned value and the timestamp is stored in the upper
// 48 bits. The top-bit is left empty so that negative values are not returned.
// The 48-bit timestamp field provides for 89 years of timestamps. We use a
// custom epoch (Jan 1, 2015) in order to utilize the entire timestamp range.
//
// Note that GenerateUniqueInt() imposes a limit on instance IDs while
// generateUniqueBytes() does not.
//
// TODO(pmattis): Do we have to worry about persisting the milliseconds value
// periodically to avoid the clock ever going backwards (e.g. due to NTP
// adjustment)?
func GenerateUniqueInt(instanceID base.SQLInstanceID) tree.DInt {
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

	return GenerateUniqueID(int32(instanceID), timestamp)
}

// GenerateUniqueID encapsulates the logic to generate a unique number from
// a nodeID and timestamp.
func GenerateUniqueID(instanceID int32, timestamp uint64) tree.DInt {
	// We xor in the instanceID so that instanceIDs larger than 32K will flip bits
	// in the timestamp portion of the final value instead of always setting them.
	id := (timestamp << NodeIDBits) ^ uint64(instanceID)
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

func extractTimeSpanFromTime(fromTime *tree.DTime, timeSpan string) (tree.Datum, error) {
	t := timeofday.TimeOfDay(*fromTime)
	return extractTimeSpanFromTimeOfDay(t, timeSpan)
}

func extractTimezoneFromOffset(offsetSecs int32, timeSpan string) tree.Datum {
	switch timeSpan {
	case "timezone":
		return tree.NewDFloat(tree.DFloat(float64(offsetSecs)))
	case "timezone_hour", "timezone_hours":
		numHours := offsetSecs / duration.SecsPerHour
		return tree.NewDFloat(tree.DFloat(float64(numHours)))
	case "timezone_minute", "timezone_minutes":
		numMinutes := offsetSecs / duration.SecsPerMinute
		return tree.NewDFloat(tree.DFloat(float64(numMinutes % 60)))
	}
	return nil
}

func extractTimeSpanFromTimeTZ(fromTime *tree.DTimeTZ, timeSpan string) (tree.Datum, error) {
	if ret := extractTimezoneFromOffset(-fromTime.OffsetSecs, timeSpan); ret != nil {
		return ret, nil
	}
	switch timeSpan {
	case "epoch":
		// Epoch should additionally add the zone offset.
		seconds := float64(time.Duration(fromTime.TimeOfDay))*float64(time.Microsecond)/float64(time.Second) + float64(fromTime.OffsetSecs)
		return tree.NewDFloat(tree.DFloat(seconds)), nil
	}
	return extractTimeSpanFromTimeOfDay(fromTime.TimeOfDay, timeSpan)
}

func extractTimeSpanFromTimeOfDay(t timeofday.TimeOfDay, timeSpan string) (tree.Datum, error) {
	switch timeSpan {
	case "hour", "hours":
		return tree.NewDFloat(tree.DFloat(t.Hour())), nil
	case "minute", "minutes":
		return tree.NewDFloat(tree.DFloat(t.Minute())), nil
	case "second", "seconds":
		return tree.NewDFloat(tree.DFloat(float64(t.Second()) + float64(t.Microsecond())/(duration.MicrosPerMilli*duration.MillisPerSec))), nil
	case "millisecond", "milliseconds":
		return tree.NewDFloat(tree.DFloat(float64(t.Second()*duration.MillisPerSec) + float64(t.Microsecond())/duration.MicrosPerMilli)), nil
	case "microsecond", "microseconds":
		return tree.NewDFloat(tree.DFloat((t.Second() * duration.MillisPerSec * duration.MicrosPerMilli) + t.Microsecond())), nil
	case "epoch":
		seconds := float64(time.Duration(t)) * float64(time.Microsecond) / float64(time.Second)
		return tree.NewDFloat(tree.DFloat(seconds)), nil
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

func extractTimeSpanFromTimestampTZ(
	ctx *tree.EvalContext, fromTime time.Time, timeSpan string,
) (tree.Datum, error) {
	_, offsetSecs := fromTime.Zone()
	if ret := extractTimezoneFromOffset(int32(offsetSecs), timeSpan); ret != nil {
		return ret, nil
	}

	switch timeSpan {
	case "epoch":
		return extractTimeSpanFromTimestamp(ctx, fromTime, timeSpan)
	default:
		// time.Time's Year(), Month(), Day(), ISOWeek(), etc. all deal in terms
		// of UTC, rather than as the timezone.
		// Remedy this by assuming that the timezone is UTC (to prevent confusion)
		// and offsetting time when using extractTimeSpanFromTimestamp.
		pretendTime := fromTime.In(time.UTC).Add(time.Duration(offsetSecs) * time.Second)
		return extractTimeSpanFromTimestamp(ctx, pretendTime, timeSpan)
	}
}

func extractTimeSpanFromInterval(
	fromInterval *tree.DInterval, timeSpan string,
) (tree.Datum, error) {
	switch timeSpan {
	case "millennia", "millennium", "millenniums":
		return tree.NewDFloat(tree.DFloat(fromInterval.Months / (duration.MonthsPerYear * 1000))), nil

	case "centuries", "century":
		return tree.NewDFloat(tree.DFloat(fromInterval.Months / (duration.MonthsPerYear * 100))), nil

	case "decade", "decades":
		return tree.NewDFloat(tree.DFloat(fromInterval.Months / (duration.MonthsPerYear * 10))), nil

	case "year", "years":
		return tree.NewDFloat(tree.DFloat(fromInterval.Months / duration.MonthsPerYear)), nil

	case "month", "months":
		return tree.NewDFloat(tree.DFloat(fromInterval.Months % duration.MonthsPerYear)), nil

	case "day", "days":
		return tree.NewDFloat(tree.DFloat(fromInterval.Days)), nil

	case "hour", "hours":
		return tree.NewDFloat(tree.DFloat(fromInterval.Nanos() / int64(time.Hour))), nil

	case "minute", "minutes":
		// Remove the hour component.
		return tree.NewDFloat(tree.DFloat((fromInterval.Nanos() % int64(time.Second*duration.SecsPerHour)) / int64(time.Minute))), nil

	case "second", "seconds":
		return tree.NewDFloat(tree.DFloat(float64(fromInterval.Nanos()%int64(time.Minute)) / float64(time.Second))), nil

	case "millisecond", "milliseconds":
		// This a PG extension not supported in MySQL.
		return tree.NewDFloat(tree.DFloat(float64(fromInterval.Nanos()%int64(time.Minute)) / float64(time.Millisecond))), nil

	case "microsecond", "microseconds":
		return tree.NewDFloat(tree.DFloat(float64(fromInterval.Nanos()%int64(time.Minute)) / float64(time.Microsecond))), nil
	case "epoch":
		return tree.NewDFloat(tree.DFloat(fromInterval.AsFloat64())), nil
	default:
		return nil, pgerror.Newf(
			pgcode.InvalidParameterValue, "unsupported timespan: %s", timeSpan)
	}
}

func extractTimeSpanFromTimestamp(
	_ *tree.EvalContext, fromTime time.Time, timeSpan string,
) (tree.Datum, error) {
	switch timeSpan {
	case "millennia", "millennium", "millenniums":
		year := fromTime.Year()
		if year > 0 {
			return tree.NewDFloat(tree.DFloat((year + 999) / 1000)), nil
		}
		return tree.NewDFloat(tree.DFloat(-((999 - (year - 1)) / 1000))), nil

	case "centuries", "century":
		year := fromTime.Year()
		if year > 0 {
			return tree.NewDFloat(tree.DFloat((year + 99) / 100)), nil
		}
		return tree.NewDFloat(tree.DFloat(-((99 - (year - 1)) / 100))), nil

	case "decade", "decades":
		year := fromTime.Year()
		if year >= 0 {
			return tree.NewDFloat(tree.DFloat(year / 10)), nil
		}
		return tree.NewDFloat(tree.DFloat(-((8 - (year - 1)) / 10))), nil

	case "year", "years":
		return tree.NewDFloat(tree.DFloat(fromTime.Year())), nil

	case "isoyear":
		year, _ := fromTime.ISOWeek()
		return tree.NewDFloat(tree.DFloat(year)), nil

	case "quarter":
		return tree.NewDFloat(tree.DFloat((fromTime.Month()-1)/3 + 1)), nil

	case "month", "months":
		return tree.NewDFloat(tree.DFloat(fromTime.Month())), nil

	case "week", "weeks":
		_, week := fromTime.ISOWeek()
		return tree.NewDFloat(tree.DFloat(week)), nil

	case "day", "days":
		return tree.NewDFloat(tree.DFloat(fromTime.Day())), nil

	case "dayofweek", "dow":
		return tree.NewDFloat(tree.DFloat(fromTime.Weekday())), nil

	case "isodow":
		day := fromTime.Weekday()
		if day == 0 {
			return tree.NewDFloat(tree.DFloat(7)), nil
		}
		return tree.NewDFloat(tree.DFloat(day)), nil

	case "dayofyear", "doy":
		return tree.NewDFloat(tree.DFloat(fromTime.YearDay())), nil

	case "julian":
		julianDay := float64(dateToJulianDay(fromTime.Year(), int(fromTime.Month()), fromTime.Day())) +
			(float64(fromTime.Hour()*duration.SecsPerHour+fromTime.Minute()*duration.SecsPerMinute+fromTime.Second())+
				float64(fromTime.Nanosecond())/float64(time.Second))/duration.SecsPerDay
		return tree.NewDFloat(tree.DFloat(julianDay)), nil

	case "hour", "hours":
		return tree.NewDFloat(tree.DFloat(fromTime.Hour())), nil

	case "minute", "minutes":
		return tree.NewDFloat(tree.DFloat(fromTime.Minute())), nil

	case "second", "seconds":
		return tree.NewDFloat(tree.DFloat(float64(fromTime.Second()) + float64(fromTime.Nanosecond())/float64(time.Second))), nil

	case "millisecond", "milliseconds":
		// This a PG extension not supported in MySQL.
		return tree.NewDFloat(
			tree.DFloat(
				float64(fromTime.Second()*duration.MillisPerSec) + float64(fromTime.Nanosecond())/
					float64(time.Millisecond),
			),
		), nil

	case "microsecond", "microseconds":
		return tree.NewDFloat(
			tree.DFloat(
				float64(fromTime.Second()*duration.MillisPerSec*duration.MicrosPerMilli) + float64(fromTime.Nanosecond())/
					float64(time.Microsecond),
			),
		), nil

	case "epoch":
		return tree.NewDFloat(tree.DFloat(float64(fromTime.UnixNano()) / float64(time.Second))), nil

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
		micro = (micro / duration.MicrosPerMilli) * duration.MicrosPerMilli
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
func arrayToString(
	evalCtx *tree.EvalContext, arr *tree.DArray, delim string, nullStr *string,
) (tree.Datum, error) {
	f := evalCtx.FmtCtx(tree.FmtArrayToString)

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

func truncateTimestamp(fromTime time.Time, timeSpan string) (*tree.DTimestampTZ, error) {
	year := fromTime.Year()
	month := fromTime.Month()
	day := fromTime.Day()
	hour := fromTime.Hour()
	min := fromTime.Minute()
	sec := fromTime.Second()
	nsec := fromTime.Nanosecond()
	loc := fromTime.Location()

	_, origZoneOffset := fromTime.Zone()

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
		// Subtract (day of week * nanoseconds per day) to get Sunday, then add a day to get Monday.
		previousMonday := fromTime.Add(-1 * time.Hour * 24 * time.Duration(fromTime.Weekday()-1))
		if fromTime.Weekday() == time.Sunday {
			// The math above does not work for Sunday, as it roll forward to the next Monday.
			// As such, subtract six days instead.
			previousMonday = fromTime.Add(-6 * time.Hour * 24)
		}
		year, month, day = previousMonday.Year(), previousMonday.Month(), previousMonday.Day()
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
	_, newZoneOffset := toTime.Zone()
	// If we have a mismatching zone offset, check whether the truncated timestamp
	// can exist at both the new and original zone time offset.
	// e.g. in Bucharest, 2020-10-25 has 03:00+02 and 03:00+03. Using time.Date
	// automatically assumes 03:00+02.
	if origZoneOffset != newZoneOffset {
		// To remedy this, try set the time.Date to have the same fixed offset as the original timezone
		// and force it to use the same location as the incoming time.
		// If using the fixed offset in the given location gives us a timestamp that is the
		// same as the original time offset, use that timestamp instead.
		fixedOffsetLoc := timeutil.FixedOffsetTimeZoneToLocation(origZoneOffset, "date_trunc")
		fixedOffsetTime := time.Date(year, month, day, hour, min, sec, nsec, fixedOffsetLoc)
		locCorrectedOffsetTime := fixedOffsetTime.In(loc)

		if _, zoneOffset := locCorrectedOffsetTime.Zone(); origZoneOffset == zoneOffset {
			toTime = locCorrectedOffsetTime
		}
	}
	return tree.MakeDTimestampTZ(toTime, time.Microsecond)
}

// Converts a scalar Datum to its string representation
func asJSONBuildObjectKey(
	d tree.Datum, dcc sessiondatapb.DataConversionConfig, loc *time.Location,
) (string, error) {
	switch t := d.(type) {
	case *tree.DJSON, *tree.DArray, *tree.DTuple:
		return "", pgerror.New(pgcode.InvalidParameterValue,
			"key value must be scalar, not array, tuple, or json")
	case *tree.DString:
		return string(*t), nil
	case *tree.DCollatedString:
		return t.Contents, nil
	case *tree.DTimestampTZ:
		ts, err := tree.MakeDTimestampTZ(t.Time.In(loc), time.Microsecond)
		if err != nil {
			return "", err
		}
		return tree.AsStringWithFlags(
			ts,
			tree.FmtBareStrings,
			tree.FmtDataConversionConfig(dcc),
		), nil
	case *tree.DBool, *tree.DInt, *tree.DFloat, *tree.DDecimal, *tree.DTimestamp,
		*tree.DDate, *tree.DUuid, *tree.DInterval, *tree.DBytes, *tree.DIPAddr, *tree.DOid,
		*tree.DTime, *tree.DTimeTZ, *tree.DBitArray, *tree.DGeography, *tree.DGeometry, *tree.DBox2D:
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

func toJSONObject(ctx *tree.EvalContext, d tree.Datum) (tree.Datum, error) {
	j, err := tree.AsJSON(d, ctx.SessionData.DataConversionConfig, ctx.GetLocation())
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
	if !ctx.SessionData.User().IsRootUser() {
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
		telemetry.Inc(sqltelemetry.FollowerReadDisabledCCLCounter)
		ctx.ClientNoticeSender.BufferClientNotice(
			ctx.Context,
			pgnotice.Newf("follower reads disabled because you are running a non-CCL distribution"),
		)
		return ctx.StmtTimestamp.Add(defaultFollowerReadDuration), nil
	}
	offset, err := EvalFollowerReadOffset(ctx.ClusterID, ctx.Settings)
	if err != nil {
		if code := pgerror.GetPGCode(err); code == pgcode.CCLValidLicenseRequired {
			telemetry.Inc(sqltelemetry.FollowerReadDisabledNoEnterpriseLicense)
			ctx.ClientNoticeSender.BufferClientNotice(
				ctx.Context, pgnotice.Newf("follower reads disabled: %s", err.Error()),
			)
			return ctx.StmtTimestamp.Add(defaultFollowerReadDuration), nil
		}
		return time.Time{}, err
	}
	return ctx.StmtTimestamp.Add(offset), nil
}

func requireNonNull(d tree.Datum) error {
	if d == tree.DNull {
		return errInvalidNull
	}
	return nil
}

func followerReadTimestamp(ctx *tree.EvalContext, _ tree.Datums) (tree.Datum, error) {
	ts, err := recentTimestamp(ctx)
	if err != nil {
		return nil, err
	}
	return tree.MakeDTimestampTZ(ts, time.Microsecond)
}

func jsonNumInvertedIndexEntries(_ *tree.EvalContext, val tree.Datum) (tree.Datum, error) {
	if val == tree.DNull {
		return tree.DZero, nil
	}
	n, err := json.NumInvertedIndexEntries(tree.MustBeDJSON(val).JSON)
	if err != nil {
		return nil, err
	}
	return tree.NewDInt(tree.DInt(n)), nil
}

func arrayNumInvertedIndexEntries(
	ctx *tree.EvalContext, val, version tree.Datum,
) (tree.Datum, error) {
	if val == tree.DNull {
		return tree.DZero, nil
	}
	arr := tree.MustBeDArray(val)

	v := descpb.SecondaryIndexFamilyFormatVersion
	if version == tree.DNull {
		if ctx.Settings.Version.IsActive(ctx.Context, clusterversion.EmptyArraysInInvertedIndexes) {
			v = descpb.EmptyArraysInInvertedIndexesVersion
		}
	} else {
		v = descpb.IndexDescriptorVersion(tree.MustBeDInt(version))
	}

	keys, err := rowenc.EncodeInvertedIndexTableKeys(arr, nil, v)
	if err != nil {
		return nil, err
	}
	return tree.NewDInt(tree.DInt(len(keys))), nil
}
