// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package builtins

import (
	"bytes"
	"context"
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
	"math/bits"
	"net"
	"regexp/syntax"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/build"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/config/zonepb"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/password"
	"github.com/cockroachdb/cockroach/pkg/security/username"
	"github.com/cockroachdb/cockroach/pkg/server/telemetry"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/appstatspb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/colinfo"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/randgen/randgencfg"
	"github.com/cockroachdb/cockroach/pkg/sql/colexecerror"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgnotice"
	"github.com/cockroachdb/cockroach/pkg/sql/privilege"
	"github.com/cockroachdb/cockroach/pkg/sql/protoreflect"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/asof"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/builtinconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins/pgformat"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/volatility"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlstats/persistedsqlstats/sqlstatsutil"
	"github.com/cockroachdb/cockroach/pkg/sql/sqltelemetry"
	"github.com/cockroachdb/cockroach/pkg/sql/syntheticprivilege"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/envutil"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/fuzzystrmatch"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/ipaddr"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/pretty"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timetz"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/tochar"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/cockroach/pkg/util/trigram"
	"github.com/cockroachdb/cockroach/pkg/util/ulid"
	"github.com/cockroachdb/cockroach/pkg/util/unaccent"
	"github.com/cockroachdb/cockroach/pkg/util/unique"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
	"github.com/knz/strtime"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var (
	errEmptyInputString = pgerror.New(pgcode.InvalidParameterValue, "the input string must not be empty")
	errZeroIP           = pgerror.New(pgcode.InvalidParameterValue, "zero length IP")
	errChrValueTooSmall = pgerror.New(pgcode.InvalidParameterValue, "input value must be >= 0")
	errChrValueTooLarge = pgerror.Newf(pgcode.InvalidParameterValue,
		"input value must be <= %d (maximum Unicode code point)", utf8.MaxRune)
	errStringTooLarge = pgerror.Newf(pgcode.ProgramLimitExceeded,
		"requested length too large, exceeds %s", humanizeutil.IBytes(builtinconstants.MaxAllocatedStringSize))
)

func categorizeType(t *types.T) string {
	switch t.Family() {
	case types.DateFamily, types.IntervalFamily, types.TimestampFamily, types.TimestampTZFamily:
		return builtinconstants.CategoryDateAndTime
	case types.StringFamily, types.BytesFamily:
		return builtinconstants.CategoryString
	default:
		return strings.ToUpper(t.String())
	}
}

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

// enableUnsafeTestBuiltins enables unsafe builtins for testing purposes.
var enableUnsafeTestBuiltins = envutil.EnvOrDefaultBool(
	"COCKROACH_ENABLE_UNSAFE_TEST_BUILTINS", false)

// builtinDefinition represents a built-in function before it becomes
// a tree.FunctionDefinition.
type builtinDefinition struct {
	props     tree.FunctionProperties
	overloads []tree.Overload
}

// defProps is used below to define built-in functions with default properties.
func defProps() tree.FunctionProperties { return tree.FunctionProperties{} }

// arrayProps is used below for array functions.
func arrayProps() tree.FunctionProperties {
	return tree.FunctionProperties{Category: builtinconstants.CategoryArray}
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

func mustBeDIntInTenantRange(e tree.Expr) (tree.DInt, error) {
	tenID := tree.MustBeDInt(e)
	if int64(tenID) <= 0 {
		return 0, pgerror.New(pgcode.InvalidParameterValue, "tenant ID must be positive")
	}
	return tenID, nil
}

func init() {
	for k, v := range regularBuiltins {
		const enforceClass = true
		registerBuiltin(k, v, tree.NormalClass, enforceClass)
	}
}

var StartCompactionJob func(
	ctx context.Context,
	planner interface{},
	collectionURI, incrLoc []string,
	fullBackupPath string,
	encryptionOpts jobspb.BackupEncryptionOptions,
	start, end hlc.Timestamp,
) (jobspb.JobID, error)

// builtins contains the built-in functions indexed by name.
//
// For use in other packages, see AllBuiltinNames and GetBuiltinProperties().
var regularBuiltins = map[string]builtinDefinition{
	// TODO(XisiHuang): support encoding, i.e., length(str, encoding).
	"length":           lengthImpls(true /* includeBitOverload */),
	"char_length":      lengthImpls(false /* includeBitOverload */),
	"character_length": lengthImpls(false /* includeBitOverload */),

	"bit_length": makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategoryString},
		stringOverload1(
			func(_ context.Context, _ *eval.Context, s string) (tree.Datum, error) {
				return tree.NewDInt(tree.DInt(len(s) * 8)), nil
			},
			types.Int,
			"Calculates the number of bits used to represent `val`.",
			volatility.Immutable,
		),
		bytesOverload1(
			func(_ context.Context, _ *eval.Context, s string) (tree.Datum, error) {
				return tree.NewDInt(tree.DInt(len(s) * 8)), nil
			},
			types.Int,
			"Calculates the number of bits used to represent `val`.",
			volatility.Immutable,
		),
		bitsOverload1(
			func(_ context.Context, _ *eval.Context, s *tree.DBitArray) (tree.Datum, error) {
				return tree.NewDInt(tree.DInt(s.BitArray.BitLen())), nil
			},
			types.Int,
			"Calculates the number of bits used to represent `val`.",
			volatility.Immutable,
		),
	),

	"bit_count": makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategoryString},
		bytesOverload1(
			func(_ context.Context, _ *eval.Context, s string) (tree.Datum, error) {
				total := int64(0)
				for _, b := range []byte(s) {
					total += int64(bits.OnesCount8(b))
				}
				return tree.NewDInt(tree.DInt(total)), nil
			},
			types.Int,
			"Calculates the number of bits set used to represent `val`.",
			volatility.Immutable,
		),
		bitsOverload1(
			func(_ context.Context, _ *eval.Context, s *tree.DBitArray) (tree.Datum, error) {
				total := int64(0)
				parts, _ := s.BitArray.EncodingParts()
				for _, b := range parts {
					total += int64(bits.OnesCount64(b))
				}
				return tree.NewDInt(tree.DInt(total)), nil
			},
			types.Int,
			"Calculates the number of bits set used to represent `val`.",
			volatility.Immutable,
		),
	),

	"format": formatImpls,

	"octet_length": makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategoryString},
		stringOverload1(
			func(_ context.Context, _ *eval.Context, s string) (tree.Datum, error) {
				return tree.NewDInt(tree.DInt(len(s))), nil
			},
			types.Int,
			"Calculates the number of bytes used to represent `val`.",
			volatility.Immutable,
		),
		bytesOverload1(
			func(_ context.Context, _ *eval.Context, s string) (tree.Datum, error) {
				return tree.NewDInt(tree.DInt(len(s))), nil
			},
			types.Int,
			"Calculates the number of bytes used to represent `val`.",
			volatility.Immutable,
		),
		bitsOverload1(
			func(_ context.Context, _ *eval.Context, s *tree.DBitArray) (tree.Datum, error) {
				return tree.NewDInt(tree.DInt((s.BitArray.BitLen() + 7) / 8)), nil
			},
			types.Int,
			"Calculates the number of bits used to represent `val`.",
			volatility.Immutable,
		),
	),

	// TODO(pmattis): What string functions should also support types.Bytes?

	"lower": makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategoryString},
		stringOverload1(
			func(_ context.Context, _ *eval.Context, s string) (tree.Datum, error) {
				return tree.NewDString(strings.ToLower(s)), nil
			},
			types.String,
			"Converts all characters in `val` to their lower-case equivalents.",
			volatility.Immutable,
		),
	),

	"unaccent": makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategoryString},
		stringOverload1(
			func(_ context.Context, _ *eval.Context, s string) (tree.Datum, error) {
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
			volatility.Immutable,
		),
	),

	"upper": makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategoryString},
		stringOverload1(
			func(_ context.Context, _ *eval.Context, s string) (tree.Datum, error) {
				return tree.NewDString(strings.ToUpper(s)), nil
			},
			types.String,
			"Converts all characters in `val` to their to their upper-case equivalents.",
			volatility.Immutable,
		),
	),

	"prettify_statement": makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategoryString},
		stringOverload1(
			func(_ context.Context, _ *eval.Context, s string) (tree.Datum, error) {
				formattedStmt, err := prettyStatement(tree.DefaultPrettyCfg(), s)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(formattedStmt), nil
			},
			types.String,
			"Prettifies a statement using a the default pretty-printing config.",
			volatility.Immutable,
		),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "statement", Typ: types.String},
				{Name: "line_width", Typ: types.Int},
				{Name: "align_mode", Typ: types.Int},
				{Name: "case_mode", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				stmt := string(tree.MustBeDString(args[0]))
				lineWidth := int(tree.MustBeDInt(args[1]))
				alignMode := int(tree.MustBeDInt(args[2]))
				caseMode := int(tree.MustBeDInt(args[3]))
				formattedStmt, err := prettyStatementCustomConfig(stmt, lineWidth, alignMode, caseMode)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(formattedStmt), nil
			},
			Info: "Prettifies a statement using a user-configured pretty-printing config.\n" +
				"Align mode values range from 0 - 3, representing no, partial, full, and extra alignment respectively.\n" +
				"Case mode values range between 0 - 1, representing lower casing and upper casing respectively.",
			Volatility: volatility.Immutable,
		},
	),

	"substr":    makeSubStringImpls(),
	"substring": makeSubStringImpls(),

	"substring_index": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryString},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "input", Typ: types.String},
				{Name: "delim", Typ: types.String},
				{Name: "count", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				input := string(tree.MustBeDString(args[0]))
				delim := string(tree.MustBeDString(args[1]))
				count := int(tree.MustBeDInt(args[2]))

				// Handle empty input.
				if input == "" || delim == "" || count == 0 {
					return tree.NewDString(""), nil
				}

				parts := strings.Split(input, delim)
				length := len(parts)

				// If count is positive, return the first 'count' parts joined by delim
				if count > 0 {
					if count >= length {
						return tree.NewDString(input), nil // If count exceeds occurrences, return the full string
					}
					result := strings.Join(parts[:count], delim)
					return tree.NewDString(result), nil
				}

				// If count is negative, return the last 'abs(count)' parts joined by delim
				count = -count
				if count >= length {
					return tree.NewDString(input), nil // If count exceeds occurrences, return the full string
				}
				return tree.NewDString(strings.Join(parts[length-count:], delim)), nil
			},
			Info: "Returns a substring of `input` before `count` occurrences of `delim`.\n" +
				"If `count` is positive, the leftmost part is returned. If `count` is negative, the rightmost part is returned.",
			Volatility: volatility.Immutable,
		},
	),

	// concat concatenates the text representations of all the arguments.
	// NULL arguments are ignored.
	"concat": makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.AnyElement},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				ctx := tree.NewFmtCtx(tree.FmtPgwireText)
				for _, d := range args {
					if d == tree.DNull {
						continue
					}
					// This is more lenient than we want and may lead to serious
					// over-allocation for some data types (e.g. printing large arrays of
					// integers). A proper solution would add a lot of complexity
					// here, with attendant performance penalties. The right answer is
					// probably to push this functionality into the Formatter.
					if ctx.Buffer.Len()+int(d.Size()) > builtinconstants.MaxAllocatedStringSize {
						return nil, errStringTooLarge
					}
					d.Format(ctx)
				}
				return tree.NewDString(ctx.CloseAndGetString()), nil
			},
			Info:              "Concatenates a comma-separated list of strings.",
			Volatility:        volatility.Immutable,
			CalledOnNullInput: true,
			// In Postgres concat can take any arguments, converting them to
			// their text representation. Since the text representation can
			// depend on the context (e.g. timezone), the function is Stable. In
			// our case, we only take String inputs so our version is ImmutableCopy.
			IgnoreVolatilityCheck: true,
		},
	),

	"concat_ws": makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.String},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				if len(args) == 0 {
					return nil, pgerror.Newf(pgcode.UndefinedFunction, builtinconstants.ErrInsufficientArgsFmtString, "concat_ws")
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
					if length > builtinconstants.MaxAllocatedStringSize {
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
			Volatility:        volatility.Immutable,
			CalledOnNullInput: true,
			// In Postgres concat_ws can take any arguments, converting them to
			// their text representation. Since the text representation can
			// depend on the context (e.g. timezone), the function is Stable. In
			// our case, we only take String inputs so our version is ImmutableCopy.
			IgnoreVolatilityCheck: true,
		},
	),

	// https://www.postgresql.org/docs/10/static/functions-string.html#FUNCTIONS-STRING-OTHER
	"convert_from": makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategoryString},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "str", Typ: types.Bytes}, {Name: "enc", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				str := string(tree.MustBeDBytes(args[0]))
				enc := CleanEncodingName(string(tree.MustBeDString(args[1])))
				switch enc {
				// All the following are aliases to each other in PostgreSQL.
				case "utf8", "unicode", "cp65001":
					if !utf8.Valid(encoding.UnsafeConvertStringToBytes(str)) {
						return nil, newDecodeError("UTF8")
					}
					return tree.NewDString(str), nil

					// All the following are aliases to each other in PostgreSQL.
				case "latin1", "iso88591", "cp28591":
					var buf strings.Builder
					for _, c := range encoding.UnsafeConvertStringToBytes(str) {
						buf.WriteRune(rune(c))
					}
					return tree.NewDString(buf.String()), nil
				}
				return nil, pgerror.Newf(pgcode.InvalidParameterValue,
					"invalid source encoding name %q", enc)
			},
			Info: "Decode the bytes in `str` into a string using encoding `enc`. " +
				"Supports encodings 'UTF8' and 'LATIN1'.",
			Volatility:            volatility.Immutable,
			IgnoreVolatilityCheck: true,
		}),

	// https://www.postgresql.org/docs/10/static/functions-string.html#FUNCTIONS-STRING-OTHER
	"convert_to": makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategoryString},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "str", Typ: types.String}, {Name: "enc", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility:            volatility.Immutable,
			IgnoreVolatilityCheck: true,
		}),

	// https://www.postgresql.org/docs/9.0/functions-binarystring.html#FUNCTIONS-BINARYSTRING-OTHER
	"get_bit": makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategoryString},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "bit_string", Typ: types.VarBit}, {Name: "index", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				bitString := tree.MustBeDBitArray(args[0])
				index := int(tree.MustBeDInt(args[1]))
				bit, err := bitString.GetBitAtIndex(index)
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(bit)), nil
			},
			Info:       "Extracts a bit at given index in the bit array.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "byte_string", Typ: types.Bytes}, {Name: "index", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		}),

	// https://www.postgresql.org/docs/9.0/functions-binarystring.html#FUNCTIONS-BINARYSTRING-OTHER
	"get_byte": makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategoryString},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "byte_string", Typ: types.Bytes}, {Name: "index", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		}),

	// https://www.postgresql.org/docs/9.0/functions-binarystring.html#FUNCTIONS-BINARYSTRING-OTHER
	"set_bit": makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategoryString},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "bit_string", Typ: types.VarBit},
				{Name: "index", Typ: types.Int},
				{Name: "to_set", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.VarBit),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "byte_string", Typ: types.Bytes},
				{Name: "index", Typ: types.Int},
				{Name: "to_set", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		}),

	// https://www.postgresql.org/docs/9.0/functions-binarystring.html#FUNCTIONS-BINARYSTRING-OTHER
	"set_byte": makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategoryString},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "byte_string", Typ: types.Bytes},
				{Name: "index", Typ: types.Int},
				{Name: "to_set", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		}),

	"uuid_generate_v4": generateRandomUUID4Impl(),

	"uuid_nil": generateConstantUUIDImpl(
		uuid.Nil, "Returns a nil UUID constant.",
	),
	"uuid_ns_dns": generateConstantUUIDImpl(
		uuid.NamespaceDNS, "Returns a constant designating the DNS namespace for UUIDs.",
	),
	"uuid_ns_url": generateConstantUUIDImpl(
		uuid.NamespaceURL, "Returns a constant designating the URL namespace for UUIDs.",
	),
	"uuid_ns_oid": generateConstantUUIDImpl(
		uuid.NamespaceOID,
		"Returns a constant designating the ISO object identifier (OID) namespace for UUIDs. "+
			"These are unrelated to the OID type used internally in the database.",
	),
	"uuid_ns_x500": generateConstantUUIDImpl(
		uuid.NamespaceX500, "Returns a constant designating the X.500 distinguished name (DN) namespace for UUIDs.",
	),

	"uuid_generate_v1": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategoryIDGeneration,
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Uuid),
			Fn: func(_ context.Context, _ *eval.Context, _ tree.Datums) (tree.Datum, error) {
				gen := uuid.NewGenWithHWAF(uuid.RandomHardwareAddrFunc)
				uv, err := gen.NewV1()
				if err != nil {
					return nil, err
				}
				return tree.NewDUuid(tree.DUuid{UUID: uv}), nil
			},
			Info: "Generates a version 1 UUID, and returns it as a value of UUID type. " +
				"To avoid exposing the server's real MAC address, " +
				"this uses a random MAC address and a timestamp. " +
				"Essentially, this is an alias for uuid_generate_v1mc.",
			Volatility: volatility.Volatile,
		},
	),

	"uuid_generate_v1mc": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategoryIDGeneration,
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Uuid),
			Fn: func(_ context.Context, _ *eval.Context, _ tree.Datums) (tree.Datum, error) {
				gen := uuid.NewGenWithHWAF(uuid.RandomHardwareAddrFunc)
				uv, err := gen.NewV1()
				if err != nil {
					return nil, err
				}
				return tree.NewDUuid(tree.DUuid{UUID: uv}), nil
			},
			Info: "Generates a version 1 UUID, and returns it as a value of UUID type. " +
				"This uses a random MAC address and a timestamp.",
			Volatility: volatility.Volatile,
		},
	),

	"uuid_generate_v3": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategoryIDGeneration,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "namespace", Typ: types.Uuid}, {Name: "name", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Uuid),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				namespace := tree.MustBeDUuid(args[0])
				name := tree.MustBeDString(args[1])
				uv := uuid.NewV3(namespace.UUID, string(name))
				return tree.NewDUuid(tree.DUuid{UUID: uv}), nil
			},
			Info: "Generates a version 3 UUID in the given namespace using the specified input name, " +
				"with md5 as the hashing method. " +
				"The namespace should be one of the special constants produced by the uuid_ns_*() functions.",
			Volatility: volatility.Immutable,
		},
	),

	"uuid_generate_v5": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategoryIDGeneration,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "namespace", Typ: types.Uuid}, {Name: "name", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Uuid),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				namespace := tree.MustBeDUuid(args[0])
				name := tree.MustBeDString(args[1])
				uv := uuid.NewV5(namespace.UUID, string(name))
				return tree.NewDUuid(tree.DUuid{UUID: uv}), nil
			},
			Info: "Generates a version 5 UUID in the given namespace using the specified input name. " +
				"This is similar to a version 3 UUID, except it uses SHA-1 for hashing.",
			Volatility: volatility.Immutable,
		},
	),

	"to_uuid": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				uv, err := uuid.FromString(s)
				if err != nil {
					return nil, pgerror.Wrap(err, pgcode.InvalidParameterValue, "invalid UUID")
				}
				return tree.NewDBytes(tree.DBytes(uv.GetBytes())), nil
			},
			Info: "Converts the character string representation of a UUID to its byte string " +
				"representation.",
			Volatility: volatility.Immutable,
		},
	),

	"from_uuid": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.Bytes}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				b := []byte(*args[0].(*tree.DBytes))
				uv, err := uuid.FromBytes(b)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(uv.String()), nil
			},
			Info: "Converts the byte string representation of a UUID to its character string " +
				"representation.",
			Volatility: volatility.Immutable,
		},
	),

	"gen_random_ulid": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategoryIDGeneration,
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Uuid),
			Fn: func(_ context.Context, evalCtx *eval.Context, _ tree.Datums) (tree.Datum, error) {
				uv := ulid.MustNew(ulid.Now(), evalCtx.GetULIDEntropy())
				return tree.NewDUuid(tree.DUuid{UUID: uuid.UUID(uv)}), nil
			},
			Info:       "Generates a random ULID and returns it as a value of UUID type.",
			Volatility: volatility.Volatile,
		},
	),

	"uuid_to_ulid": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.Uuid}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				b := (*args[0].(*tree.DUuid)).GetBytes()
				var ul ulid.ULID
				if err := ul.UnmarshalBinary(b); err != nil {
					return nil, err
				}
				return tree.NewDString(ul.String()), nil
			},
			Info:       "Converts a UUID-encoded ULID to its string representation.",
			Volatility: volatility.Immutable,
		},
	),

	"ulid_to_uuid": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Uuid),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				s := tree.MustBeDString(args[0])
				u, err := ulid.Parse(string(s))
				if err != nil {
					return nil, pgerror.Wrap(err, pgcode.InvalidParameterValue, "invalid ULID")
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
			Volatility: volatility.Immutable,
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
			Types:      tree.ParamTypes{{Name: "val", Typ: types.INet}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				return tree.NewDString(dIPAddr.IPAddr.String()), nil
			},
			Info: "Converts the combined IP address and prefix length to an abbreviated display format as text." +
				"For INET types, this will omit the prefix length if it's not the default (32 or IPv4, 128 for IPv6)" +
				"\n\nFor example, `abbrev('192.168.1.2/24')` returns `'192.168.1.2/24'`",
			Volatility: volatility.Immutable,
		},
	),

	"broadcast": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.INet}},
			ReturnType: tree.FixedReturnType(types.INet),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				broadcastIPAddr := dIPAddr.IPAddr.Broadcast()
				return &tree.DIPAddr{IPAddr: broadcastIPAddr}, nil
			},
			Info: "Gets the broadcast address for the network address represented by the value." +
				"\n\nFor example, `broadcast('192.168.1.2/24')` returns `'192.168.1.255/24'`",
			Volatility: volatility.Immutable,
		},
	),

	"family": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.INet}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				if dIPAddr.Family == ipaddr.IPv4family {
					return tree.NewDInt(tree.DInt(4)), nil
				}
				return tree.NewDInt(tree.DInt(6)), nil
			},
			Info: "Extracts the IP family of the value; 4 for IPv4, 6 for IPv6." +
				"\n\nFor example, `family('::1')` returns `6`",
			Volatility: volatility.Immutable,
		},
	),

	"host": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.INet}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				s := dIPAddr.IPAddr.String()
				if i := strings.IndexByte(s, '/'); i != -1 {
					return tree.NewDString(s[:i]), nil
				}
				return tree.NewDString(s), nil
			},
			Info: "Extracts the address part of the combined address/prefixlen value as text." +
				"\n\nFor example, `host('192.168.1.2/16')` returns `'192.168.1.2'`",
			Volatility: volatility.Immutable,
		},
	),

	"hostmask": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.INet}},
			ReturnType: tree.FixedReturnType(types.INet),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				ipAddr := dIPAddr.IPAddr.Hostmask()
				return &tree.DIPAddr{IPAddr: ipAddr}, nil
			},
			Info: "Creates an IP host mask corresponding to the prefix length in the value." +
				"\n\nFor example, `hostmask('192.168.1.2/16')` returns `'0.0.255.255'`",
			Volatility: volatility.Immutable,
		},
	),

	"masklen": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.INet}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				return tree.NewDInt(tree.DInt(dIPAddr.Mask)), nil
			},
			Info: "Retrieves the prefix length stored in the value." +
				"\n\nFor example, `masklen('192.168.1.2/16')` returns `16`",
			Volatility: volatility.Immutable,
		},
	),

	"netmask": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.INet}},
			ReturnType: tree.FixedReturnType(types.INet),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				dIPAddr := tree.MustBeDIPAddr(args[0])
				ipAddr := dIPAddr.IPAddr.Netmask()
				return &tree.DIPAddr{IPAddr: ipAddr}, nil
			},
			Info: "Creates an IP network mask corresponding to the prefix length in the value." +
				"\n\nFor example, `netmask('192.168.1.2/16')` returns `'255.255.0.0'`",
			Volatility: volatility.Immutable,
		},
	),

	"set_masklen": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "val", Typ: types.INet},
				{Name: "prefixlen", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.INet),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
	),

	"inet_same_family": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "val", Typ: types.INet},
				{Name: "val", Typ: types.INet},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				first := tree.MustBeDIPAddr(args[0])
				other := tree.MustBeDIPAddr(args[1])
				return tree.MakeDBool(tree.DBool(first.Family == other.Family)), nil
			},
			Info:       "Checks if two IP addresses are of the same IP family.",
			Volatility: volatility.Immutable,
		},
	),

	"inet_contained_by_or_equals": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "val", Typ: types.INet},
				{Name: "container", Typ: types.INet},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				ipAddr := tree.MustBeDIPAddr(args[0]).IPAddr
				other := tree.MustBeDIPAddr(args[1]).IPAddr
				return tree.MakeDBool(tree.DBool(ipAddr.ContainedByOrEquals(&other))), nil
			},
			Info: "Test for subnet inclusion or equality, using only the network parts of the addresses. " +
				"The host part of the addresses is ignored.",
			Volatility: volatility.Immutable,
		},
	),

	"inet_contains_or_equals": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "container", Typ: types.INet},
				{Name: "val", Typ: types.INet},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				ipAddr := tree.MustBeDIPAddr(args[0]).IPAddr
				other := tree.MustBeDIPAddr(args[1]).IPAddr
				return tree.MakeDBool(tree.DBool(ipAddr.ContainsOrEquals(&other))), nil
			},
			Info: "Test for subnet inclusion or equality, using only the network parts of the addresses. " +
				"The host part of the addresses is ignored.",
			Volatility: volatility.Immutable,
		},
	),

	"from_ip": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.Bytes}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
	),

	"to_ip": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
	),

	"split_part": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "input", Typ: types.String},
				{Name: "delimiter", Typ: types.String},
				{Name: "return_index_pos", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				text := string(tree.MustBeDString(args[0]))
				sep := string(tree.MustBeDString(args[1]))
				field := int(tree.MustBeDInt(args[2]))

				if field == 0 {
					return nil, pgerror.Newf(
						pgcode.InvalidParameterValue, "field position must not be zero")
				}

				if sep == "" {
					// Return the entire text if requesting the first or last field.
					if field == 1 || field == -1 {
						return tree.NewDString(text), nil
					}
					return tree.NewDString(""), nil
				}

				splits := strings.Split(text, sep)
				if field > len(splits) || -1*field > len(splits) {
					return tree.NewDString(""), nil
				}

				// If field is negative, select from the end
				if field < 0 {
					return tree.NewDString(splits[len(splits)+field]), nil
				}
				// Otherwise, return from the beginning (1-based index)
				return tree.NewDString(splits[field-1]), nil
			},
			Info: "Splits `input` using `delimiter` and returns the field at `return_index_pos` (starting from 1). " +
				"If `return_index_pos` is negative, it returns the |`return_index_pos`|'th field from the end. " +
				"\n\nFor example, `split_part('123.456.789.0', '.', 3)` returns `789`.",
			Volatility: volatility.Immutable,
		},
	),

	"repeat": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "input", Typ: types.String}, {Name: "repeat_counter", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (_ tree.Datum, err error) {
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
				} else if ln > builtinconstants.MaxAllocatedStringSize {
					return nil, errStringTooLarge
				}

				return tree.NewDString(strings.Repeat(s, count)), nil
			},
			Info: "Concatenates `input` `repeat_counter` number of times.\n\nFor example, " +
				"`repeat('dog', 2)` returns `dogdog`.",
			Volatility: volatility.Immutable,
		},
	),

	// https://www.postgresql.org/docs/10/static/functions-binarystring.html
	"encode": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "data", Typ: types.Bytes}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (_ tree.Datum, err error) {
				data, format := *args[0].(*tree.DBytes), string(tree.MustBeDString(args[1]))
				be, ok := lex.BytesEncodeFormatFromString(format)
				if !ok {
					return nil, pgerror.New(pgcode.InvalidParameterValue,
						"only 'hex', 'escape', and 'base64' formats are supported for encode()")
				}
				return tree.NewDString(lex.EncodeByteArrayToRawBytes(
					string(data), be, true /* skipHexPrefix */)), nil
			},
			Info:       "Encodes `data` using `format` (`hex` / `escape` / `base64`).",
			Volatility: volatility.Immutable,
		},
	),

	"decode": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "text", Typ: types.String}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (_ tree.Datum, err error) {
				data, format := string(tree.MustBeDString(args[0])), string(tree.MustBeDString(args[1]))
				be, ok := lex.BytesEncodeFormatFromString(format)
				if !ok {
					return nil, pgerror.New(pgcode.InvalidParameterValue,
						"only 'hex', 'escape', and 'base64' formats are supported for decode()")
				}
				res, err := lex.DecodeRawBytesToByteArray(encoding.UnsafeConvertStringToBytes(data), be)
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(encoding.UnsafeConvertBytesToString(res))), nil
			},
			Info:       "Decodes `data` using `format` (`hex` / `escape` / `base64`).",
			Volatility: volatility.Immutable,
		},
	),

	"compress": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "data", Typ: types.Bytes}, {Name: "codec", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (_ tree.Datum, err error) {
				uncompressedData := []byte(tree.MustBeDBytes(args[0]))
				codec := string(tree.MustBeDString(args[1]))
				compressedBytes, err := compress(uncompressedData, codec)
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(compressedBytes)), nil
			},
			Info:       "Compress `data` with the specified `codec` (`gzip`, 'lz4', 'snappy', 'zstd).",
			Volatility: volatility.Immutable,
		},
	),

	"decompress": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "data", Typ: types.Bytes}, {Name: "codec", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (_ tree.Datum, err error) {
				compressedData := []byte(tree.MustBeDBytes(args[0]))
				codec := string(tree.MustBeDString(args[1]))
				decompressedBytes, err := decompress(compressedData, codec)
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(decompressedBytes)), nil
			},
			Info:       "Decompress `data` with the specified `codec` (`gzip`, 'lz4', 'snappy', 'zstd).",
			Volatility: volatility.Immutable,
		},
	),

	"ascii": makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategoryString},
		stringOverload1(
			func(_ context.Context, _ *eval.Context, s string) (tree.Datum, error) {
				for _, ch := range s {
					return tree.NewDInt(tree.DInt(ch)), nil
				}
				return nil, errEmptyInputString
			},
			types.Int,
			"Returns the character code of the first character in `val`. Despite the name, the function supports Unicode too.",
			volatility.Immutable,
		)),

	"chr": makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategoryString},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
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
		tree.FunctionProperties{Category: builtinconstants.CategoryString},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				val := tree.MustBeDInt(args[0])
				// This should technically match the precision of the types entered
				// into the function, e.g. `-1 :: int4` should use uint32 for correctness.
				// However, we don't encode that information for function resolution.
				// As such, always assume bigint / uint64.
				return tree.NewDString(fmt.Sprintf("%x", uint64(val))), nil
			},
			Info:       "Converts `val` to its hexadecimal representation.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.Bytes}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(fmt.Sprintf("%x", tree.MustBeDBytes(args[0]))), nil
			},
			Info:       "Converts `val` to its hexadecimal representation.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(fmt.Sprintf("%x", tree.MustBeDString(args[0]))), nil
			},
			Info:       "Converts `val` to its hexadecimal representation.",
			Volatility: volatility.Immutable,
		},
	),

	"to_english": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryString},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
	),

	// The SQL parser coerces POSITION to STRPOS.
	"strpos": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryString},
		stringOverload2(
			"input",
			"find",
			func(_ context.Context, _ *eval.Context, s, substring string) (tree.Datum, error) {
				index := strings.Index(s, substring)
				if index < 0 {
					return tree.DZero, nil
				}

				return tree.NewDInt(tree.DInt(utf8.RuneCountInString(s[:index]) + 1)), nil
			},
			types.Int,
			"Calculates the position where the string `find` begins in `input`. \n\nFor"+
				" example, `strpos('doggie', 'gie')` returns `4`.",
			volatility.Immutable,
		),
		bitsOverload2("input", "find",
			func(_ context.Context, _ *eval.Context, bitString, bitSubstring *tree.DBitArray) (tree.Datum, error) {
				index := strings.Index(bitString.BitArray.String(), bitSubstring.BitArray.String())
				if index < 0 {
					return tree.DZero, nil
				}
				return tree.NewDInt(tree.DInt(index + 1)), nil
			},
			types.Int,
			"Calculates the position where the bit subarray `find` begins in `input`.",
			volatility.Immutable,
		),
		bytesOverload2(
			"input",
			"find",
			func(_ context.Context, _ *eval.Context, byteString, byteSubstring string) (tree.Datum, error) {
				index := strings.Index(byteString, byteSubstring)
				if index < 0 {
					return tree.DZero, nil
				}
				return tree.NewDInt(tree.DInt(index + 1)), nil
			},
			types.Int,
			"Calculates the position where the byte subarray `find` begins in `input`.",
			volatility.Immutable,
		)),

	"overlay": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "input", Typ: types.String},
				{Name: "overlay_val", Typ: types.String},
				{Name: "start_pos", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				to := string(tree.MustBeDString(args[1]))
				pos := int(tree.MustBeDInt(args[2]))
				size := utf8.RuneCountInString(to)
				return overlay(s, to, pos, size)
			},
			Info: "Replaces characters in `input` with `overlay_val` starting at `start_pos` " +
				"(begins at 1). \n\nFor example, `overlay('doggie', 'CAT', 2)` returns " +
				"`dCATie`.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "input", Typ: types.String},
				{Name: "overlay_val", Typ: types.String},
				{Name: "start_pos", Typ: types.Int},
				{Name: "end_pos", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				to := string(tree.MustBeDString(args[1]))
				pos := int(tree.MustBeDInt(args[2]))
				size := int(tree.MustBeDInt(args[3]))
				return overlay(s, to, pos, size)
			},
			Info: "Deletes the characters in `input` between `start_pos` and `end_pos` (count " +
				"starts at 1), and then insert `overlay_val` at `start_pos`.",
			Volatility: volatility.Immutable,
		},
	),

	"lpad": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryString},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "string", Typ: types.String}, {Name: "length", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "string", Typ: types.String}, {Name: "length", Typ: types.Int}, {Name: "fill", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
	),

	"rpad": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryString},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "string", Typ: types.String}, {Name: "length", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "string", Typ: types.String}, {Name: "length", Typ: types.Int}, {Name: "fill", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
	),

	// The SQL parser coerces TRIM(...) and TRIM(BOTH ...) to BTRIM(...).
	"btrim": makeBuiltin(defProps(),
		stringOverload2(
			"input",
			"trim_chars",
			func(_ context.Context, _ *eval.Context, s, chars string) (tree.Datum, error) {
				return tree.NewDString(strings.Trim(s, chars)), nil
			},
			types.String,
			"Removes any characters included in `trim_chars` from the beginning or end"+
				" of `input` (applies recursively). \n\nFor example, `btrim('doggie', 'eod')` "+
				"returns `ggi`.",
			volatility.Immutable,
		),
		stringOverload1(
			func(_ context.Context, _ *eval.Context, s string) (tree.Datum, error) {
				return tree.NewDString(strings.TrimSpace(s)), nil
			},
			types.String,
			"Removes all spaces from the beginning and end of `val`.",
			volatility.Immutable,
		),
	),

	// The SQL parser coerces TRIM(LEADING ...) to LTRIM(...).
	"ltrim": makeBuiltin(defProps(),
		stringOverload2(
			"input",
			"trim_chars",
			func(_ context.Context, _ *eval.Context, s, chars string) (tree.Datum, error) {
				return tree.NewDString(strings.TrimLeft(s, chars)), nil
			},
			types.String,
			"Removes any characters included in `trim_chars` from the beginning "+
				"(left-hand side) of `input` (applies recursively). \n\nFor example, "+
				"`ltrim('doggie', 'od')` returns `ggie`.",
			volatility.Immutable,
		),
		stringOverload1(
			func(_ context.Context, _ *eval.Context, s string) (tree.Datum, error) {
				return tree.NewDString(strings.TrimLeftFunc(s, unicode.IsSpace)), nil
			},
			types.String,
			"Removes all spaces from the beginning (left-hand side) of `val`.",
			volatility.Immutable,
		),
	),

	// The SQL parser coerces TRIM(TRAILING ...) to RTRIM(...).
	"rtrim": makeBuiltin(defProps(),
		stringOverload2(
			"input",
			"trim_chars",
			func(_ context.Context, _ *eval.Context, s, chars string) (tree.Datum, error) {
				return tree.NewDString(strings.TrimRight(s, chars)), nil
			},
			types.String,
			"Removes any characters included in `trim_chars` from the end (right-hand "+
				"side) of `input` (applies recursively). \n\nFor example, `rtrim('doggie', 'ei')` "+
				"returns `dogg`.",
			volatility.Immutable,
		),
		stringOverload1(
			func(_ context.Context, _ *eval.Context, s string) (tree.Datum, error) {
				return tree.NewDString(strings.TrimRightFunc(s, unicode.IsSpace)), nil
			},
			types.String,
			"Removes all spaces from the end (right-hand side) of `val`.",
			volatility.Immutable,
		),
	),

	"reverse": makeBuiltin(defProps(),
		stringOverload1(
			func(_ context.Context, _ *eval.Context, s string) (tree.Datum, error) {
				if len(s) > builtinconstants.MaxAllocatedStringSize {
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
			volatility.Immutable,
		),
	),

	"replace": makeBuiltin(defProps(),
		stringOverload3(
			"input",
			"find",
			"replace",
			func(_ context.Context, _ *eval.Context, input, from, to string) (tree.Datum, error) {
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
				if maxResultLen > builtinconstants.MaxAllocatedStringSize {
					return nil, errStringTooLarge
				}
				result := strings.Replace(input, from, to, -1)
				return tree.NewDString(result), nil
			},
			types.String,
			"Replaces all occurrences of `find` with `replace` in `input`",
			volatility.Immutable,
		),
	),

	"translate": makeBuiltin(defProps(),
		stringOverload3("input", "find", "replace",
			func(_ context.Context, _ *eval.Context, s, from, to string) (tree.Datum, error) {
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
			volatility.Immutable,
		),
	),

	"regexp_extract": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "input", Typ: types.String}, {Name: "regex", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				pattern := string(tree.MustBeDString(args[1]))
				return regexpExtract(evalCtx, s, pattern, `\`)
			},
			Info:       "Returns the first match for the Regular Expression `regex` in `input`.",
			Volatility: volatility.Immutable,
		},
	),

	"regexp_replace": makeBuiltin(defProps(),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "input", Typ: types.String},
				{Name: "regex", Typ: types.String},
				{Name: "replace", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "input", Typ: types.String},
				{Name: "regex", Typ: types.String},
				{Name: "replace", Typ: types.String},
				{Name: "flags", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
	),

	"regexp_split_to_array": makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "string", Typ: types.String},
				{Name: "pattern", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.MakeArray(types.String)),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return regexpSplitToArray(evalCtx, args, false /* hasFlags */)
			},
			Info:       "Split string using a POSIX regular expression as the delimiter.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "string", Typ: types.String},
				{Name: "pattern", Typ: types.String},
				{Name: "flags", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.MakeArray(types.String)),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return regexpSplitToArray(evalCtx, args, true /* hasFlags */)
			},
			Info:       "Split string using a POSIX regular expression as the delimiter with flags." + regexpFlagInfo,
			Volatility: volatility.Immutable,
		},
	),

	"like_escape": makeBuiltin(defProps(),
		stringOverload3(
			"unescaped", "pattern", "escape",
			func(ctx context.Context, evalCtx *eval.Context, unescaped, pattern, escape string) (tree.Datum, error) {
				return eval.MatchLikeEscape(evalCtx, unescaped, pattern, escape, false)
			},
			types.Bool,
			"Matches `unescaped` with `pattern` using `escape` as an escape token.",
			volatility.Immutable,
		),
	),

	"not_like_escape": makeBuiltin(defProps(),
		stringOverload3(
			"unescaped", "pattern", "escape",
			func(ctx context.Context, evalCtx *eval.Context, unescaped, pattern, escape string) (tree.Datum, error) {
				dmatch, err := eval.MatchLikeEscape(evalCtx, unescaped, pattern, escape, false)
				if err != nil {
					return dmatch, err
				}
				bmatch, err := tree.GetBool(dmatch)
				return tree.MakeDBool(!bmatch), err
			},
			types.Bool,
			"Checks whether `unescaped` not matches with `pattern` using `escape` as an escape token.",
			volatility.Immutable,
		)),

	"ilike_escape": makeBuiltin(defProps(),
		stringOverload3(
			"unescaped", "pattern", "escape",
			func(ctx context.Context, evalCtx *eval.Context, unescaped, pattern, escape string) (tree.Datum, error) {
				return eval.MatchLikeEscape(evalCtx, unescaped, pattern, escape, true)
			},
			types.Bool,
			"Matches case insensetively `unescaped` with `pattern` using `escape` as an escape token.",
			volatility.Immutable,
		)),

	"not_ilike_escape": makeBuiltin(defProps(),
		stringOverload3(
			"unescaped", "pattern", "escape",
			func(ctx context.Context, evalCtx *eval.Context, unescaped, pattern, escape string) (tree.Datum, error) {
				dmatch, err := eval.MatchLikeEscape(evalCtx, unescaped, pattern, escape, true)
				if err != nil {
					return dmatch, err
				}
				bmatch, err := tree.GetBool(dmatch)
				return tree.MakeDBool(!bmatch), err
			},
			types.Bool,
			"Checks whether `unescaped` not matches case insensetively with `pattern` using `escape` as an escape token.",
			volatility.Immutable,
		)),

	"similar_escape": makeBuiltin(
		defProps(),
		similarOverloads(true)...),

	"similar_to_escape": makeBuiltin(
		defProps(),
		append(
			similarOverloads(false),
			stringOverload3(
				"unescaped", "pattern", "escape",
				func(ctx context.Context, evalCtx *eval.Context, unescaped, pattern, escape string) (tree.Datum, error) {
					return eval.SimilarToEscape(evalCtx, unescaped, pattern, escape)
				},
				types.Bool,
				"Matches `unescaped` with `pattern` using `escape` as an escape token.",
				volatility.Immutable,
			),
		)...,
	),

	"not_similar_to_escape": makeBuiltin(defProps(),
		stringOverload3(
			"unescaped", "pattern", "escape",
			func(ctx context.Context, evalCtx *eval.Context, unescaped, pattern, escape string) (tree.Datum, error) {
				dmatch, err := eval.SimilarToEscape(evalCtx, unescaped, pattern, escape)
				if err != nil {
					return dmatch, err
				}
				bmatch, err := tree.GetBool(dmatch)
				return tree.MakeDBool(!bmatch), err
			},
			types.Bool,
			"Checks whether `unescaped` not matches with `pattern` using `escape` as an escape token.",
			volatility.Immutable,
		)),

	"initcap": makeBuiltin(defProps(),
		stringOverload1(
			func(_ context.Context, _ *eval.Context, s string) (tree.Datum, error) {
				return tree.NewDString(cases.Title(language.English, cases.NoLower).String(strings.ToLower(s))), nil
			},
			types.String,
			"Capitalizes the first letter of `val`.",
			volatility.Immutable,
		)),

	"quote_ident": makeBuiltin(defProps(),
		stringOverload1(
			func(_ context.Context, _ *eval.Context, s string) (tree.Datum, error) {
				var buf bytes.Buffer
				lexbase.EncodeRestrictedSQLIdent(&buf, s, lexbase.EncNoFlags)
				return tree.NewDString(buf.String()), nil
			},
			types.String,
			"Return `val` suitably quoted to serve as identifier in a SQL statement.",
			volatility.Immutable,
		)),

	"quote_literal": makeBuiltin(defProps(),
		tree.Overload{
			Types:              tree.ParamTypes{{Name: "val", Typ: types.String}},
			ReturnType:         tree.FixedReturnType(types.String),
			OverloadPreference: tree.OverloadPreferencePreferred,
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				s := tree.MustBeDString(args[0])
				return tree.NewDString(lexbase.EscapeSQLString(string(s))), nil
			},
			Info:       "Return `val` suitably quoted to serve as string literal in a SQL statement.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.AnyElement}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				// PostgreSQL specifies that this variant first casts to the SQL string type,
				// and only then quotes. We can't use (Datum).String() directly.
				d := eval.UnwrapDatum(ctx, evalCtx, args[0])
				strD, err := eval.PerformCast(ctx, evalCtx, d, types.String)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(strD.String()), nil
			},
			Info:       "Coerce `val` to a string and then quote it as a literal.",
			Volatility: volatility.Stable,
		},
	),

	// quote_nullable is the same as quote_literal but accepts NULL arguments.
	"quote_nullable": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategoryString,
		},
		tree.Overload{
			Types:              tree.ParamTypes{{Name: "val", Typ: types.String}},
			ReturnType:         tree.FixedReturnType(types.String),
			OverloadPreference: tree.OverloadPreferencePreferred,
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.NewDString("NULL"), nil
				}
				s := tree.MustBeDString(args[0])
				return tree.NewDString(lexbase.EscapeSQLString(string(s))), nil
			},
			Info:              "Coerce `val` to a string and then quote it as a literal. If `val` is NULL, returns 'NULL'.",
			Volatility:        volatility.Immutable,
			CalledOnNullInput: true,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.AnyElement}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.NewDString("NULL"), nil
				}
				// PostgreSQL specifies that this variant first casts to the SQL string type,
				// and only then quotes. We can't use (Datum).String() directly.
				d := eval.UnwrapDatum(ctx, evalCtx, args[0])
				strD, err := eval.PerformCast(ctx, evalCtx, d, types.String)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(strD.String()), nil
			},
			Info:              "Coerce `val` to a string and then quote it as a literal. If `val` is NULL, returns 'NULL'.",
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
		},
	),

	"left": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "input", Typ: types.Bytes}, {Name: "return_set", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "input", Typ: types.String}, {Name: "return_set", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
	),

	"right": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "input", Typ: types.Bytes}, {Name: "return_set", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "input", Typ: types.String}, {Name: "return_set", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
	),

	"random": makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.NewDFloat(tree.DFloat(evalCtx.GetRNG().Float64())), nil
			},
			Info: "Returns a random floating-point number between 0 (inclusive) and 1 (exclusive). " +
				"Note that the value contains at most 53 bits of randomness.",
			Volatility: volatility.Volatile,
		},
	),

	"setseed": makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "seed", Typ: types.Float}},
			ReturnType: tree.FixedReturnType(types.Void),
			Fn: func(_ context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				seed := tree.MustBeDFloat(args[0])
				if seed < -1.0 || seed > 1.0 {
					return nil, pgerror.Newf(pgcode.InvalidParameterValue, "setseed parameter %f is out of allowed range [-1,1]", seed)
				}
				evalCtx.GetRNG().Seed(int64(math.Float64bits(float64(seed))))
				return tree.DVoidDatum, nil
			},
			Info: "Sets the seed for subsequent random() calls in this session (value between -1.0 and 1.0, inclusive). " +
				"There are no guarantees as to how this affects the seed of random() calls that appear in the same query as setseed().",
			Volatility: volatility.Volatile,
		},
	),

	"unique_rowid": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategoryIDGeneration,
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.NewDInt(tree.DInt(unique.GenerateUniqueInt(
					unique.ProcessUniqueID(evalCtx.NodeID.SQLInstanceID()),
				))), nil
			},
			Info: "Returns a unique ID used by CockroachDB to generate unique row IDs if a " +
				"Primary Key isn't defined for the table. The value is a combination of the " +
				"insert timestamp and the ID of the node executing the statement, which " +
				"guarantees this combination is globally unique. However, there can be " +
				"gaps and the order is not completely guaranteed.",
			Volatility: volatility.Volatile,
		},
	),

	"unordered_unique_rowid": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategoryIDGeneration,
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				instanceID := unique.ProcessUniqueID(evalCtx.NodeID.SQLInstanceID())
				v := unique.GenerateUniqueUnorderedID(instanceID)
				return tree.NewDInt(tree.DInt(v)), nil
			},
			Info: "Returns a unique ID. The value is a combination of the " +
				"insert timestamp (bit-reversed) and the ID of the node executing the statement, which " +
				"guarantees this combination is globally unique. The way it is generated is statistically " +
				"likely to not have any ordering relative to previously generated values.",
			Volatility: volatility.Volatile,
		},
	),

	// Sequence functions.

	"nextval": makeBuiltin(
		tree.FunctionProperties{
			Category:             builtinconstants.CategorySequences,
			DistsqlBlocklist:     true,
			HasSequenceArguments: true,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: builtinconstants.SequenceNameArg, Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				name := tree.MustBeDString(args[0])
				dOid, err := eval.ParseDOid(ctx, evalCtx, string(name), types.RegClass)
				if err != nil {
					return nil, err
				}
				res, err := evalCtx.Sequence.IncrementSequenceByID(ctx, int64(dOid.Oid))
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(res)), nil
			},
			Info:       "Advances the given sequence and returns its new value.",
			Volatility: volatility.Volatile,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: builtinconstants.SequenceNameArg, Typ: types.RegClass}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				oid := tree.MustBeDOid(args[0])
				res, err := evalCtx.Sequence.IncrementSequenceByID(ctx, int64(oid.Oid))
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(res)), nil
			},
			Info:       "Advances the given sequence and returns its new value.",
			Volatility: volatility.Volatile,
		},
	),

	"currval": makeBuiltin(
		tree.FunctionProperties{
			Category:             builtinconstants.CategorySequences,
			DistsqlBlocklist:     true,
			HasSequenceArguments: true,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: builtinconstants.SequenceNameArg, Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				name := tree.MustBeDString(args[0])
				dOid, err := eval.ParseDOid(ctx, evalCtx, string(name), types.RegClass)
				if err != nil {
					return nil, err
				}
				res, err := evalCtx.Sequence.GetLatestValueInSessionForSequenceByID(ctx, int64(dOid.Oid))
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(res)), nil
			},
			Info:       "Returns the latest value obtained with nextval for this sequence in this session.",
			Volatility: volatility.Volatile,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: builtinconstants.SequenceNameArg, Typ: types.RegClass}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				oid := tree.MustBeDOid(args[0])
				res, err := evalCtx.Sequence.GetLatestValueInSessionForSequenceByID(ctx, int64(oid.Oid))
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(res)), nil
			},
			Info:       "Returns the latest value obtained with nextval for this sequence in this session.",
			Volatility: volatility.Volatile,
		},
	),

	"lastval": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySequences,
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				val, err := evalCtx.SessionData().SequenceState.GetLastValue()
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(val)), nil
			},
			Info:       "Return value most recently obtained with nextval in this session.",
			Volatility: volatility.Volatile,
		},
	),

	// Note: behavior is slightly different than Postgres for performance reasons.
	// See https://github.com/cockroachdb/cockroach/issues/21564
	"setval": makeBuiltin(
		tree.FunctionProperties{
			Category:             builtinconstants.CategorySequences,
			DistsqlBlocklist:     true,
			HasSequenceArguments: true,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: builtinconstants.SequenceNameArg, Typ: types.String}, {Name: "value", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				name := tree.MustBeDString(args[0])
				dOid, err := eval.ParseDOid(ctx, evalCtx, string(name), types.RegClass)
				if err != nil {
					return nil, err
				}

				newVal := tree.MustBeDInt(args[1])
				if err := evalCtx.Sequence.SetSequenceValueByID(
					ctx, uint32(dOid.Oid), int64(newVal), true /* isCalled */); err != nil {
					return nil, err
				}
				return args[1], nil
			},
			Info: "Set the given sequence's current value. The next call to nextval will return " +
				"`value + Increment`",
			Volatility: volatility.Volatile,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: builtinconstants.SequenceNameArg, Typ: types.RegClass}, {Name: "value", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				oid := tree.MustBeDOid(args[0])
				newVal := tree.MustBeDInt(args[1])
				if err := evalCtx.Sequence.SetSequenceValueByID(
					ctx, uint32(oid.Oid), int64(newVal), true /* isCalled */); err != nil {
					return nil, err
				}
				return args[1], nil
			},
			Info: "Set the given sequence's current value. The next call to nextval will return " +
				"`value + Increment`",
			Volatility: volatility.Volatile,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: builtinconstants.SequenceNameArg, Typ: types.String}, {Name: "value", Typ: types.Int}, {Name: "is_called", Typ: types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				name := tree.MustBeDString(args[0])
				dOid, err := eval.ParseDOid(ctx, evalCtx, string(name), types.RegClass)
				if err != nil {
					return nil, err
				}
				isCalled := bool(tree.MustBeDBool(args[2]))

				newVal := tree.MustBeDInt(args[1])
				if err := evalCtx.Sequence.SetSequenceValueByID(
					ctx, uint32(dOid.Oid), int64(newVal), isCalled); err != nil {
					return nil, err
				}
				return args[1], nil
			},
			Info: "Set the given sequence's current value. If is_called is false, the next call to " +
				"nextval will return `value`; otherwise `value + Increment`.",
			Volatility: volatility.Volatile,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: builtinconstants.SequenceNameArg, Typ: types.RegClass}, {Name: "value", Typ: types.Int}, {Name: "is_called", Typ: types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				oid := tree.MustBeDOid(args[0])
				isCalled := bool(tree.MustBeDBool(args[2]))

				newVal := tree.MustBeDInt(args[1])
				if err := evalCtx.Sequence.SetSequenceValueByID(
					ctx, uint32(oid.Oid), int64(newVal), isCalled); err != nil {
					return nil, err
				}
				return args[1], nil
			},
			Info: "Set the given sequence's current value. If is_called is false, the next call to " +
				"nextval will return `value`; otherwise `value + Increment`.",
			Volatility: volatility.Volatile,
		},
	),

	"experimental_uuid_v4": uuidV4Impl(),
	"uuid_v4":              uuidV4Impl(),

	"greatest": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategoryComparison,
		},
		tree.Overload{
			Types:      tree.HomogeneousType{},
			ReturnType: tree.FirstNonNullReturnType(),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return eval.PickFromTuple(ctx, evalCtx, true /* greatest */, args)
			},
			Info:              "Returns the element with the greatest value.",
			Volatility:        volatility.Immutable,
			CalledOnNullInput: true,
		},
	),

	"least": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategoryComparison,
		},
		tree.Overload{
			Types:      tree.HomogeneousType{},
			ReturnType: tree.FirstNonNullReturnType(),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return eval.PickFromTuple(ctx, evalCtx, false /* greatest */, args)
			},
			Info:              "Returns the element with the lowest value.",
			Volatility:        volatility.Immutable,
			CalledOnNullInput: true,
		},
	),

	// Timestamp/Date functions.

	"strftime":              strftimeImpl(),
	"experimental_strftime": strftimeImpl(),

	"strptime":              strptimeImpl(),
	"experimental_strptime": strptimeImpl(),

	"to_char": makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "interval", Typ: types.Interval}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				d := tree.MustBeDInterval(args[0]).Duration
				var buf bytes.Buffer
				d.FormatWithStyle(&buf, duration.IntervalStyle_POSTGRES)
				return tree.NewDString(buf.String()), nil
			},
			Info:       "Convert an interval to a string assuming the Postgres IntervalStyle.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "timestamp", Typ: types.Timestamp}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				ts := tree.MustBeDTimestamp(args[0])
				return tree.NewDString(tree.AsStringWithFlags(&ts, tree.FmtBareStrings)), nil
			},
			Info:       "Convert an timestamp to a string assuming the ISO, MDY DateStyle.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "interval", Typ: types.Interval}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, ctx *eval.Context, args tree.Datums) (tree.Datum, error) {
				d := tree.MustBeDInterval(args[0])
				f := tree.MustBeDString(args[1])
				s, err := tochar.DurationToChar(d.Duration, ctx.ToCharFormatCache, string(f))
				return tree.NewDString(s), err
			},
			Info:       "Convert an interval to a string using the given format.",
			Volatility: volatility.Stable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "timestamp", Typ: types.Timestamp}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, ctx *eval.Context, args tree.Datums) (tree.Datum, error) {
				ts := tree.MustBeDTimestamp(args[0])
				f := tree.MustBeDString(args[1])
				s, err := tochar.TimeToChar(ts.Time, ctx.ToCharFormatCache, string(f))
				return tree.NewDString(s), err
			},
			Info:       "Convert an timestamp to a string using the given format.",
			Volatility: volatility.Stable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "timestamptz", Typ: types.TimestampTZ}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, ctx *eval.Context, args tree.Datums) (tree.Datum, error) {
				ts := tree.MustBeDTimestampTZ(args[0])
				f := tree.MustBeDString(args[1])
				s, err := tochar.TimeToChar(ts.Time, ctx.ToCharFormatCache, string(f))
				return tree.NewDString(s), err
			},
			Info:       "Convert a timestamp with time zone to a string using the given format.",
			Volatility: volatility.Stable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "date", Typ: types.Date}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				ts := tree.MustBeDDate(args[0])
				return tree.NewDString(tree.AsStringWithFlags(&ts, tree.FmtBareStrings)), nil
			},
			Info:       "Convert an date to a string assuming the ISO, MDY DateStyle.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "date", Typ: types.Date}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, ctx *eval.Context, args tree.Datums) (tree.Datum, error) {
				d := tree.MustBeDDate(args[0])
				f := tree.MustBeDString(args[1])
				t, err := d.ToTime()
				if err != nil {
					return nil, err
				}
				s, err := tochar.TimeToChar(t, ctx.ToCharFormatCache, string(f))
				return tree.NewDString(s), err
			},
			Info:       "Convert a timestamp with time zone to a string using the given format.",
			Volatility: volatility.Stable,
		},
	),

	"to_char_with_style": makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "interval", Typ: types.Interval}, {Name: "style", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				d := tree.MustBeDInterval(args[0]).Duration
				styleStr := string(tree.MustBeDString(args[1]))
				styleVal, ok := duration.IntervalStyle_value[strings.ToUpper(styleStr)]
				if !ok {
					return nil, pgerror.Newf(
						pgcode.InvalidParameterValue,
						"invalid IntervalStyle: %s",
						styleStr,
					)
				}
				var buf bytes.Buffer
				d.FormatWithStyle(&buf, duration.IntervalStyle(styleVal))
				return tree.NewDString(buf.String()), nil
			},
			Info:       "Convert an interval to a string using the given IntervalStyle.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "timestamp", Typ: types.Timestamp}, {Name: "datestyle", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				ts := tree.MustBeDTimestamp(args[0])
				dateStyleStr := string(tree.MustBeDString(args[1]))
				ds, err := pgdate.ParseDateStyle(dateStyleStr, pgdate.DefaultDateStyle())
				if err != nil {
					return nil, err
				}
				if ds.Style != pgdate.Style_ISO {
					return nil, unimplemented.NewWithIssue(41773, "only ISO style is supported")
				}
				return tree.NewDString(tree.AsStringWithFlags(&ts, tree.FmtBareStrings)), nil
			},
			Info:       "Convert an timestamp to a string assuming the string is formatted using the given DateStyle.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "date", Typ: types.Date}, {Name: "datestyle", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				ts := tree.MustBeDDate(args[0])
				dateStyleStr := string(tree.MustBeDString(args[1]))
				ds, err := pgdate.ParseDateStyle(dateStyleStr, pgdate.DefaultDateStyle())
				if err != nil {
					return nil, err
				}
				if ds.Style != pgdate.Style_ISO {
					return nil, unimplemented.NewWithIssue(41773, "only ISO style is supported")
				}
				return tree.NewDString(tree.AsStringWithFlags(&ts, tree.FmtBareStrings)), nil
			},
			Info:       "Convert an date to a string assuming the string is formatted using the given DateStyle.",
			Volatility: volatility.Immutable,
		},
	),

	"make_date": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryDateAndTime},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "year", Typ: types.Int}, {Name: "month", Typ: types.Int}, {Name: "day", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(_ context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				year := int(tree.MustBeDInt(args[0]))
				month := time.Month(int(tree.MustBeDInt(args[1])))
				day := int(tree.MustBeDInt(args[2]))
				if year == 0 {
					return nil, pgerror.New(pgcode.DatetimeFieldOverflow, "year value of 0 is not valid")
				}
				location := evalCtx.GetLocation()
				return tree.NewDDateFromTime(time.Date(year, month, day, 0, 0, 0, 0, location))
			},
			// For the ISO 8601 standard, the conversion from a negative year to BC changes the year value (ex. -2013 == 2014 BC).
			// https://en.wikipedia.org/wiki/ISO_8601#Years
			Info:       "Create date (formatted according to ISO 8601) from year, month, and day fields (negative years signify BC).",
			Volatility: volatility.Immutable,
		},
	),

	"make_timestamp": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryDateAndTime},
		makeTimestampStatementBuiltinOverload(false /* withOutputTZ */, false /* withInputTZ */),
	),

	"make_timestamptz": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryDateAndTime},
		makeTimestampStatementBuiltinOverload(true /* withOutputTZ */, true /* withInputTZ */),
		makeTimestampStatementBuiltinOverload(true /* withOutputTZ */, false /* withInputTZ */),
	),

	// https://www.postgresql.org/docs/14/functions-datetime.html#FUNCTIONS-DATETIME-TABLE
	//
	// PostgreSQL documents date_trunc for text and double precision.
	// It will also handle smallint, integer, bigint, decimal,
	// numeric, real, and numeric like text inputs by casting them,
	// so we support those for compatibility. This gives us the following
	// function signatures:
	//
	//  to_timestamp(text, text)       -> TimestampTZ
	//  to_timestamp(text)             -> TimestampTZ
	//  to_timestamp(INT)              -> TimestampTZ
	//  to_timestamp(INT2)             -> TimestampTZ
	//  to_timestamp(INT4)             -> TimestampTZ
	//  to_timestamp(INT8)             -> TimestampTZ
	//  to_timestamp(FLOAT)            -> TimestampTZ
	//  to_timestamp(REAL)             -> TimestampTZ
	//  to_timestamp(DOUBLE PRECISION) -> TimestampTZ
	//  to_timestamp(DECIMAL)          -> TimestampTZ
	//
	// See the following snippet from running the functions in PostgreSQL:
	//
	//               postgres=# select to_timestamp(32767::smallint);
	//               to_timestamp
	//               ------------------------
	//               1970-01-01 09:06:07+00
	//
	//               postgres=# select to_timestamp(1646906263::integer);
	//               to_timestamp
	//               ------------------------
	//               2022-03-10 09:57:43+00
	//
	//               postgres=# select to_timestamp(1646906263::bigint);
	//               to_timestamp
	//               ------------------------
	//               2022-03-10 09:57:43+00
	//
	//               postgres=# select to_timestamp(1646906263.123456::decimal);
	//               to_timestamp
	//               -------------------------------
	//               2022-03-10 09:57:43.123456+00
	//
	//               postgres=# select to_timestamp(1646906263.123456::numeric);
	//               to_timestamp
	//               -------------------------------
	//               2022-03-10 09:57:43.123456+00
	//
	//               postgres=# select to_timestamp(1646906263.123456::real);
	//               to_timestamp
	//               ------------------------
	//               2022-03-10 09:57:20+00
	//
	//               postgres=# select to_timestamp('1646906263.123456');
	//               to_timestamp
	//               -------------------------------
	//               2022-03-10 09:57:43.123456+00
	//
	"to_timestamp": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryDateAndTime},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "timestamp", Typ: types.Float}},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				ts, ok := tree.AsDFloat(args[0])
				if !ok {
					return nil, pgerror.New(pgcode.InvalidParameterValue, "expected float argument for to_timestamp")
				}
				fts := float64(*ts)
				if math.IsNaN(fts) {
					return nil, pgerror.New(pgcode.DatetimeFieldOverflow, "timestamp cannot be NaN")
				}
				if fts == math.Inf(1) {
					return tree.MakeDTimestampTZ(pgdate.TimeInfinity, time.Microsecond)
				}
				if fts == math.Inf(-1) {
					return tree.MakeDTimestampTZ(pgdate.TimeNegativeInfinity, time.Microsecond)
				}
				return tree.MakeDTimestampTZ(timeutil.Unix(0, int64(fts*float64(time.Second))), time.Microsecond)
			},
			Info:       "Convert Unix epoch (seconds since 1970-01-01 00:00:00+00) to timestamp with time zone.",
			Volatility: volatility.Immutable,
		},
	),

	// https://www.postgresql.org/docs/10/static/functions-datetime.html
	"age": makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.Interval),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return &tree.DInterval{
					Duration: duration.Age(
						evalCtx.GetTxnTimestamp(time.Microsecond).Time,
						args[0].(*tree.DTimestampTZ).Time,
					),
				}, nil
			},
			Info: "Calculates the interval between `val` and the current time, normalized into years, months and days." + `

Note this may not be an accurate time span since years and months are normalized
from days, and years and months are out of context. To avoid normalizing days into
months and years, use ` + "`now() - timestamptz`" + `.`,
			Volatility: volatility.Stable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "end", Typ: types.TimestampTZ}, {Name: "begin", Typ: types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.Interval),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
	),

	"current_date": makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn:         currentDate,
			Info:       "Returns the date of the current transaction." + txnTSContextDoc,
			Volatility: volatility.Stable,
		},
	),

	"now":                   txnTSImplBuiltin(true),
	"current_time":          txnTimeWithPrecisionBuiltin(true),
	"current_timestamp":     txnTSWithPrecisionImplBuiltin(true),
	"transaction_timestamp": txnTSImplBuiltin(true),

	"localtimestamp": txnTSWithPrecisionImplBuiltin(false),
	"localtime":      txnTimeWithPrecisionBuiltin(false),

	"statement_timestamp": makeBuiltin(
		defProps(),
		tree.Overload{
			Types:              tree.ParamTypes{},
			ReturnType:         tree.FixedReturnType(types.TimestampTZ),
			OverloadPreference: tree.OverloadPreferencePreferred,
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDTimestampTZ(evalCtx.GetStmtTimestamp(), time.Microsecond)
			},
			Info:       "Returns the start time of the current statement.",
			Volatility: volatility.Stable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDTimestamp(evalCtx.GetStmtTimestamp(), time.Microsecond)
			},
			Info:       "Returns the start time of the current statement.",
			Volatility: volatility.Stable,
		},
	),

	asof.FollowerReadTimestampFunctionName: makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{},
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
nearest replica.`, builtinconstants.DefaultFollowerReadDuration),
			Volatility: volatility.Volatile,
		},
	),

	asof.FollowerReadTimestampExperimentalFunctionName: makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn:         followerReadTimestamp,
			Info:       fmt.Sprintf("Same as %s. This name is deprecated.", asof.FollowerReadTimestampFunctionName),
			Volatility: volatility.Volatile,
		},
	),

	asof.WithMinTimestampFunctionName: makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "min_timestamp", Typ: types.TimestampTZ},
			},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				ts, ok := tree.AsDTimestampTZ(args[0])
				if !ok {
					return nil, pgerror.New(pgcode.InvalidParameterValue, "expected timestamptz argument for min_timestamp")
				}
				return withMinTimestamp(ctx, evalCtx, ts.Time)
			},
			Info:       withMinTimestampInfo(false /* nearestOnly */),
			Volatility: volatility.Volatile,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "min_timestamp", Typ: types.TimestampTZ},
				{Name: "nearest_only", Typ: types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				ts, ok := tree.AsDTimestampTZ(args[0])
				if !ok {
					return nil, pgerror.New(pgcode.InvalidParameterValue, "expected timestamptz argument for min_timestamp")
				}
				return withMinTimestamp(ctx, evalCtx, ts.Time)
			},
			Info:       withMinTimestampInfo(true /* nearestOnly */),
			Volatility: volatility.Volatile,
		},
	),

	asof.WithMaxStalenessFunctionName: makeBuiltin(
		defProps(),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "max_staleness", Typ: types.Interval},
			},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				interval, ok := tree.AsDInterval(args[0])
				if !ok {
					return nil, pgerror.New(pgcode.InvalidParameterValue, "expected interval argument for max_staleness")
				}
				return withMaxStaleness(ctx, evalCtx, interval.Duration)
			},
			Info:       withMaxStalenessInfo(false /* nearestOnly */),
			Volatility: volatility.Volatile,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "max_staleness", Typ: types.Interval},
				{Name: "nearest_only", Typ: types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				interval, ok := tree.AsDInterval(args[0])
				if !ok {
					return nil, pgerror.New(pgcode.InvalidParameterValue, "expected interval argument for max_staleness")
				}
				return withMaxStaleness(ctx, evalCtx, interval.Duration)
			},
			Info:       withMaxStalenessInfo(true /* nearestOnly */),
			Volatility: volatility.Volatile,
		},
	),

	"cluster_logical_timestamp": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Decimal),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return evalCtx.GetClusterTimestamp()
			},
			Info: `Returns the logical time of the current transaction as
a CockroachDB HLC in decimal form.

Note that uses of this function disable server-side optimizations and
may increase either contention or retry errors, or both.

Returns an error if run in a transaction with an isolation level weaker than SERIALIZABLE.`,
			Volatility: volatility.Volatile,
		},
	),

	"hlc_to_timestamp": makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "hlc", Typ: types.Decimal}},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				d := tree.MustBeDDecimal(args[0])
				return eval.DecimalToInexactDTimestampTZ(&d)
			},
			Info: `Returns a TimestampTZ representation of a CockroachDB HLC in decimal form.

Note that a TimestampTZ has less precision than a CockroachDB HLC. It is intended as
a convenience function to display HLCs in a print-friendly form. Use the decimal
value if you rely on the HLC for accuracy.`,
			Volatility: volatility.Immutable,
		},
	),

	"clock_timestamp": makeBuiltin(
		defProps(),
		tree.Overload{
			Types:              tree.ParamTypes{},
			ReturnType:         tree.FixedReturnType(types.TimestampTZ),
			OverloadPreference: tree.OverloadPreferencePreferred,
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDTimestampTZ(timeutil.Now(), time.Microsecond)
			},
			Info:       "Returns the current system time on one of the cluster nodes.",
			Volatility: volatility.Volatile,
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.MakeDTimestamp(timeutil.Now(), time.Microsecond)
			},
			Info:       "Returns the current system time on one of the cluster nodes.",
			Volatility: volatility.Volatile,
		},
	),

	"timeofday": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryDateAndTime},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				ctxTime := evalCtx.GetRelativeParseTime()
				// From postgres@a166d408eb0b35023c169e765f4664c3b114b52e src/backend/utils/adt/timestamp.c#L1637,
				// we should support "%a %b %d %H:%M:%S.%%06d %Y %Z".
				return tree.NewDString(ctxTime.Format("Mon Jan 2 15:04:05.000000 2006 -0700")), nil
			},
			Info:       "Returns the current system time on one of the cluster nodes as a string.",
			Volatility: volatility.Stable,
		},
	),

	"extract":   extractBuiltin(),
	"date_part": extractBuiltin(),

	// TODO(knz,otan): Remove in 20.2.
	"extract_duration": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryDateAndTime},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "element", Typ: types.String}, {Name: "input", Typ: types.Interval}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
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
		tree.FunctionProperties{Category: builtinconstants.CategoryDateAndTime},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "element", Typ: types.String}, {Name: "input", Typ: types.Timestamp}},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "element", Typ: types.String}, {Name: "input", Typ: types.Date}},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				date := args[1].(*tree.DDate)
				// Localize the timestamp into the given location.
				fromTSTZ, err := tree.MakeDTimestampTZFromDate(evalCtx.GetLocation(), date)
				if err != nil {
					return nil, err
				}
				return truncateTimestamp(fromTSTZ.Time, timeSpan)
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero (or one, for day and month)\n\n" +
				"Compatible elements: millennium, century, decade, year, quarter, month,\n" +
				"week, day, hour, minute, second, millisecond, microsecond.",
			Volatility: volatility.Stable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "element", Typ: types.String}, {Name: "input", Typ: types.Time}},
			ReturnType: tree.FixedReturnType(types.Interval),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "element", Typ: types.String}, {Name: "input", Typ: types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				fromTSTZ := args[1].(*tree.DTimestampTZ)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				return truncateTimestamp(fromTSTZ.Time.In(evalCtx.GetLocation()), timeSpan)
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero (or one, for day and month)\n\n" +
				"Compatible elements: millennium, century, decade, year, quarter, month,\n" +
				"week, day, hour, minute, second, millisecond, microsecond.",
			Volatility: volatility.Stable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "element", Typ: types.String}, {Name: "input", Typ: types.Interval}},
			ReturnType: tree.FixedReturnType(types.Interval),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				fromInterval := args[1].(*tree.DInterval)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				return truncateInterval(fromInterval, timeSpan)
			},
			Info: "Truncates `input` to precision `element`.  Sets all fields that are less\n" +
				"significant than `element` to zero (or one, for day and month)\n\n" +
				"Compatible elements: millennium, century, decade, year, quarter, month,\n" +
				"week, day, hour, minute, second, millisecond, microsecond.",
			Volatility: volatility.Stable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "element", Typ: types.String}, {Name: "input", Typ: types.TimestampTZ}, {Name: "timezone", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				fromTSTZ := tree.MustBeDTimestampTZ(args[1])
				location, err := timeutil.TimeZoneStringToLocation(string(tree.MustBeDString(args[2])), timeutil.TimeZoneStringToLocationPOSIXStandard)
				if err != nil {
					return nil, pgerror.WithCandidateCode(err, pgcode.InvalidParameterValue)
				}
				return truncateTimestamp(fromTSTZ.Time.In(location), timeSpan)
			},
			Info: "Truncates `input` to precision `element` in the specified `timezone`.  Sets all fields that are less\n" +
				"significant than `element` to zero (or one, for day and month)\n\n" +
				"Compatible elements: millennium, century, decade, year, quarter, month,\n" +
				"week, day, hour, minute, second, millisecond, microsecond.",
			Volatility: volatility.Stable,
		},
	),

	"row_to_json": makeBuiltin(jsonProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "row", Typ: types.AnyTuple}},
			ReturnType: tree.FixedReturnType(types.Jsonb),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
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
						evalCtx.SessionData().DataConversionConfig,
						evalCtx.GetLocation(),
					)
					if err != nil {
						return nil, err
					}
					builder.Add(label, val)
				}
				return tree.NewDJSON(builder.Build()), nil
			},
			Info:       "Returns the row as a JSON object.",
			Volatility: volatility.Stable,
		},
	),

	// https://www.postgresql.org/docs/9.6/functions-datetime.html
	"timezone": makeBuiltin(defProps(),
		// NOTE(otan): this should be deleted and replaced with the correct
		// function overload promoting the string to timestamptz.
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "timezone", Typ: types.String},
				{Name: "timestamptz_string", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				tzArg := string(tree.MustBeDString(args[0]))
				tsArg := string(tree.MustBeDString(args[1]))
				ts, _, err := tree.ParseDTimestampTZ(evalCtx, tsArg, time.Microsecond)
				if err != nil {
					return nil, err
				}
				loc, err := timeutil.TimeZoneStringToLocation(tzArg, timeutil.TimeZoneStringToLocationPOSIXStandard)
				if err != nil {
					return nil, err
				}
				return ts.EvalAtAndRemoveTimeZone(loc, time.Microsecond)
			},
			Info:       "Convert given time stamp with time zone to the new time zone, with no time zone designation.",
			Volatility: volatility.Stable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "timezone", Typ: types.String},
				{Name: "timestamp", Typ: types.Timestamp},
			},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				tzStr := string(tree.MustBeDString(args[0]))
				ts := tree.MustBeDTimestamp(args[1])
				loc, err := timeutil.TimeZoneStringToLocation(
					tzStr,
					timeutil.TimeZoneStringToLocationPOSIXStandard,
				)
				if err != nil {
					return nil, err
				}
				return ts.AddTimeZone(loc, time.Microsecond)
			},
			Info:       "Treat given time stamp without time zone as located in the specified time zone.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "timezone", Typ: types.String},
				{Name: "timestamptz", Typ: types.TimestampTZ},
			},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				tzStr := string(tree.MustBeDString(args[0]))
				ts := tree.MustBeDTimestampTZ(args[1])
				loc, err := timeutil.TimeZoneStringToLocation(
					tzStr,
					timeutil.TimeZoneStringToLocationPOSIXStandard,
				)
				if err != nil {
					return nil, err
				}
				return ts.EvalAtAndRemoveTimeZone(loc, time.Microsecond)
			},
			Info:       "Convert given time stamp with time zone to the new time zone, with no time zone designation.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "timezone", Typ: types.String},
				{Name: "time", Typ: types.Time},
			},
			ReturnType: tree.FixedReturnType(types.TimeTZ),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
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
				_, beforeOffsetSecs := tTime.In(evalCtx.GetLocation()).Zone()
				durationDelta := time.Duration(-beforeOffsetSecs) * time.Second
				return tree.NewDTimeTZ(timetz.MakeTimeTZFromTime(tTime.In(loc).Add(durationDelta))), nil
			},
			Info:       "Treat given time without time zone as located in the specified time zone.",
			Volatility: volatility.Stable,
			// This overload's volatility does not match Postgres. See
			// https://github.com/cockroachdb/cockroach/pull/51037 for details.
			IgnoreVolatilityCheck: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "timezone", Typ: types.String},
				{Name: "timetz", Typ: types.TimeTZ},
			},
			ReturnType: tree.FixedReturnType(types.TimeTZ),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Stable,
			// This overload's volatility does not match Postgres. See
			// https://github.com/cockroachdb/cockroach/pull/51037 for details.
			IgnoreVolatilityCheck: true,
		},
	),

	"parse_ident": makeBuiltin(
		defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "qualified_identifier", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.StringArray),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				s := tree.MustBeDString(args[0])
				idents, err := parseIdent(string(s), true /* strict */)
				if err != nil {
					return nil, pgerror.WithCandidateCode(err, pgcode.InvalidParameterValue)
				}
				arr := tree.NewDArray(types.String)
				for _, ident := range idents {
					if err := arr.Append(tree.NewDString(ident)); err != nil {
						return nil, err
					}
				}
				return arr, nil
			},
			Info: "Splits qualified_identifier into an array of identifiers, " +
				"removing any quoting of individual identifiers. " +
				"Extra characters after the last identifier are considered an error",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "qualified_identifier", Typ: types.String},
				{Name: "strict", Typ: types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.StringArray),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				s := tree.MustBeDString(args[0])
				strict := tree.MustBeDBool(args[1])
				idents, err := parseIdent(string(s), bool(strict))
				if err != nil {
					return nil, pgerror.WithCandidateCode(err, pgcode.InvalidParameterValue)
				}
				arr := tree.NewDArray(types.String)
				for _, ident := range idents {
					if err := arr.Append(tree.NewDString(ident)); err != nil {
						return nil, err
					}
				}
				return arr, nil
			},
			Info: "Splits `qualified_identifier` into an array of identifiers, " +
				"removing any quoting of individual identifiers. " +
				"If `strict` is false, then extra characters after the last identifier are ignored.",
			Volatility: volatility.Immutable,
		},
	),

	// parse_timestamp converts strings to timestamps. It is useful in expressions
	// where casts (which are not immutable) cannot be used, like computed column
	// expressions or partial index predicates. Only absolute timestamps that do
	// not depend on the current context are supported (relative timestamps like
	// 'now' are not supported).
	"parse_timestamp": makeBuiltin(
		defProps(),
		stringOverload1(
			func(ctx context.Context, evalCtx *eval.Context, s string) (tree.Datum, error) {
				ts, dependsOnContext, err := tree.ParseDTimestamp(
					tree.NewParseContext(evalCtx.GetTxnTimestamp(time.Microsecond).Time),
					s,
					time.Microsecond,
				)
				if err != nil {
					return nil, err
				}
				if dependsOnContext {
					return nil, pgerror.Newf(
						pgcode.InvalidParameterValue,
						"relative timestamps are not supported",
					)
				}
				return ts, nil
			},
			types.Timestamp,
			"Convert a string containing an absolute timestamp to the corresponding timestamp assuming dates are in MDY format.",
			volatility.Immutable,
		),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "string", Typ: types.String}, {Name: "datestyle", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				arg := string(tree.MustBeDString(args[0]))
				dateStyle := string(tree.MustBeDString(args[1]))
				parseCtx, err := parseContextFromDateStyle(evalCtx, dateStyle)
				if err != nil {
					return nil, err
				}
				ts, dependsOnContext, err := tree.ParseDTimestamp(
					parseCtx,
					arg,
					time.Microsecond,
				)
				if err != nil {
					return nil, err
				}
				if dependsOnContext {
					return nil, pgerror.Newf(
						pgcode.InvalidParameterValue,
						"relative timestamps are not supported",
					)
				}
				return ts, nil
			},
			Info:       "Convert a string containing an absolute timestamp to the corresponding timestamp assuming dates formatted using the given DateStyle.",
			Volatility: volatility.Immutable,
		},
	),

	"parse_date": makeBuiltin(
		defProps(),
		stringOverload1(
			func(ctx context.Context, evalCtx *eval.Context, s string) (tree.Datum, error) {
				ts, dependsOnContext, err := tree.ParseDDate(
					tree.NewParseContext(evalCtx.GetTxnTimestamp(time.Microsecond).Time),
					s,
				)
				if err != nil {
					return nil, err
				}
				if dependsOnContext {
					return nil, pgerror.Newf(
						pgcode.InvalidParameterValue,
						"relative dates are not supported",
					)
				}
				return ts, nil
			},
			types.Date,
			"Parses a date assuming it is in MDY format.",
			volatility.Immutable,
		),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "string", Typ: types.String}, {Name: "datestyle", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				arg := string(tree.MustBeDString(args[0]))
				dateStyle := string(tree.MustBeDString(args[1]))
				parseCtx, err := parseContextFromDateStyle(evalCtx, dateStyle)
				if err != nil {
					return nil, err
				}
				ts, dependsOnContext, err := tree.ParseDDate(parseCtx, arg)
				if err != nil {
					return nil, err
				}
				if dependsOnContext {
					return nil, pgerror.Newf(
						pgcode.InvalidParameterValue,
						"relative dates are not supported",
					)
				}
				return ts, nil
			},
			Info:       "Parses a date assuming it is in format specified by DateStyle.",
			Volatility: volatility.Immutable,
		},
	),

	"parse_time": makeBuiltin(
		defProps(),
		stringOverload1(
			func(ctx context.Context, evalCtx *eval.Context, s string) (tree.Datum, error) {
				t, dependsOnContext, err := tree.ParseDTime(
					tree.NewParseContext(evalCtx.GetTxnTimestamp(time.Microsecond).Time),
					s,
					time.Microsecond,
				)
				if err != nil {
					return nil, err
				}
				if dependsOnContext {
					return nil, pgerror.Newf(
						pgcode.InvalidParameterValue,
						"relative times are not supported",
					)
				}
				return t, nil
			},
			types.Time,
			"Parses a time assuming the date (if any) is in MDY format.",
			volatility.Immutable,
		),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "string", Typ: types.String}, {Name: "timestyle", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Time),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				arg := string(tree.MustBeDString(args[0]))
				dateStyle := string(tree.MustBeDString(args[1]))
				parseCtx, err := parseContextFromDateStyle(evalCtx, dateStyle)
				if err != nil {
					return nil, err
				}
				t, dependsOnContext, err := tree.ParseDTime(parseCtx, arg, time.Microsecond)
				if err != nil {
					return nil, err
				}
				if dependsOnContext {
					return nil, pgerror.Newf(
						pgcode.InvalidParameterValue,
						"relative times are not supported",
					)
				}
				return t, nil
			},
			Info:       "Parses a time assuming the date (if any) is in format specified by DateStyle.",
			Volatility: volatility.Immutable,
		},
	),

	"parse_interval": makeBuiltin(
		defProps(),
		stringOverload1(
			func(_ context.Context, _ *eval.Context, s string) (tree.Datum, error) {
				return tree.ParseDInterval(duration.IntervalStyle_POSTGRES, s)
			},
			types.Interval,
			"Convert a string to an interval assuming the Postgres IntervalStyle.",
			volatility.Immutable,
		),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "string", Typ: types.String}, {Name: "style", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Interval),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				styleStr := string(tree.MustBeDString(args[1]))
				styleVal, ok := duration.IntervalStyle_value[strings.ToUpper(styleStr)]
				if !ok {
					return nil, pgerror.Newf(
						pgcode.InvalidParameterValue,
						"invalid IntervalStyle: %s",
						styleStr,
					)
				}
				return tree.ParseDInterval(duration.IntervalStyle(styleVal), s)
			},
			Info:       "Convert a string to an interval using the given IntervalStyle.",
			Volatility: volatility.Immutable,
		},
	),

	"parse_timetz": makeBuiltin(
		defProps(),
		stringOverload1(
			func(ctx context.Context, evalCtx *eval.Context, s string) (tree.Datum, error) {
				t, dependsOnContext, err := tree.ParseDTimeTZ(
					tree.NewParseContext(evalCtx.GetTxnTimestamp(time.Microsecond).Time),
					s,
					time.Microsecond,
				)
				if err != nil {
					return nil, err
				}
				if dependsOnContext {
					return nil, pgerror.Newf(
						pgcode.InvalidParameterValue,
						"relative times are not supported",
					)
				}
				return t, nil
			},
			types.TimeTZ,
			"Parses a timetz assuming the date (if any) is in MDY format.",
			volatility.Immutable,
		),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "string", Typ: types.String}, {Name: "timestyle", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.TimeTZ),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				arg := string(tree.MustBeDString(args[0]))
				dateStyle := string(tree.MustBeDString(args[1]))
				parseCtx, err := parseContextFromDateStyle(evalCtx, dateStyle)
				if err != nil {
					return nil, err
				}
				t, dependsOnContext, err := tree.ParseDTimeTZ(parseCtx, arg, time.Microsecond)
				if err != nil {
					return nil, err
				}
				if dependsOnContext {
					return nil, pgerror.Newf(
						pgcode.InvalidParameterValue,
						"relative times are not supported",
					)
				}
				return t, nil
			},
			Info:       "Parses a timetz assuming the date (if any) is in format specified by DateStyle.",
			Volatility: volatility.Immutable,
		},
	),

	// Array functions.

	"string_to_array": makeBuiltin(arrayProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "str", Typ: types.String}, {Name: "delimiter", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.StringArray),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				str := string(tree.MustBeDString(args[0]))
				delimOrNil := stringOrNil(args[1])
				return stringToArray(str, delimOrNil, nil)
			},
			Info:              "Split a string into components on a delimiter.",
			Volatility:        volatility.Immutable,
			CalledOnNullInput: true,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "str", Typ: types.String}, {Name: "delimiter", Typ: types.String}, {Name: "null", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.StringArray),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				str := string(tree.MustBeDString(args[0]))
				delimOrNil := stringOrNil(args[1])
				nullStr := stringOrNil(args[2])
				return stringToArray(str, delimOrNil, nullStr)
			},
			Info:              "Split a string into components on a delimiter with a specified string to consider NULL.",
			Volatility:        volatility.Immutable,
			CalledOnNullInput: true,
		},
	),

	"jsonb_array_to_string_array": makeBuiltin(arrayProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "input", Typ: types.Jsonb}},
			ReturnType: tree.FixedReturnType(types.StringArray),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				strArray := tree.NewDArray(types.String)
				if args[0] == tree.DNull {
					return strArray, nil
				}
				jsonArray, ok := tree.MustBeDJSON(args[0]).AsArray()
				if !ok {
					return nil, pgerror.Newf(pgcode.InvalidParameterValue, "input argument must be JSON array type")
				}

				for _, elem := range jsonArray {
					str, err := elem.AsText()
					if err != nil {
						return nil, err
					}
					if str != nil {
						err = strArray.Append(tree.NewDString(*str))
						if err != nil {
							return nil, err
						}
					} else {
						err = strArray.Append(tree.DNull)
						if err != nil {
							return nil, err
						}
					}
				}
				return strArray, nil
			},
			Info:              "Convert a JSONB array into a string array.",
			Volatility:        volatility.Immutable,
			CalledOnNullInput: true,
		}),

	"array_to_string": makeBuiltin(arrayProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "input", Typ: types.AnyArray}, {Name: "delim", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull || args[1] == tree.DNull {
					return tree.DNull, nil
				}
				arr := tree.MustBeDArray(args[0])
				delim := string(tree.MustBeDString(args[1]))
				return arrayToString(evalCtx, arr, delim, nil)
			},
			Info:              "Join an array into a string with a delimiter.",
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "input", Typ: types.AnyArray}, {Name: "delimiter", Typ: types.String}, {Name: "null", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull || args[1] == tree.DNull {
					return tree.DNull, nil
				}
				arr := tree.MustBeDArray(args[0])
				delim := string(tree.MustBeDString(args[1]))
				nullStr := stringOrNil(args[2])
				return arrayToString(evalCtx, arr, delim, nullStr)
			},
			Info:              "Join an array into a string with a delimiter, replacing NULLs with a null string.",
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
		},
	),

	"array_length": makeBuiltin(arrayProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "input", Typ: types.AnyArray}, {Name: "array_dimension", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				arr := tree.MustBeDArray(args[0])
				dimen := int64(tree.MustBeDInt(args[1]))
				return arrayLength(arr, dimen), nil
			},
			Info: "Calculates the length of `input` on the provided `array_dimension`. However, " +
				"because CockroachDB doesn't yet support multi-dimensional arrays, the only supported" +
				" `array_dimension` is **1**.",
			Volatility: volatility.Immutable,
		},
	),

	"cardinality": makeBuiltin(arrayProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "input", Typ: types.AnyArray}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				arr := tree.MustBeDArray(args[0])
				return cardinality(arr), nil
			},
			Info:       "Calculates the number of elements contained in `input`",
			Volatility: volatility.Immutable,
		},
	),

	"array_lower": makeBuiltin(arrayProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "input", Typ: types.AnyArray}, {Name: "array_dimension", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				arr := tree.MustBeDArray(args[0])
				dimen := int64(tree.MustBeDInt(args[1]))
				return arrayLower(arr, dimen), nil
			},
			Info: "Calculates the minimum value of `input` on the provided `array_dimension`. " +
				"However, because CockroachDB doesn't yet support multi-dimensional arrays, the only " +
				"supported `array_dimension` is **1**.",
			Volatility: volatility.Immutable,
		},
	),

	"array_upper": makeBuiltin(arrayProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "input", Typ: types.AnyArray}, {Name: "array_dimension", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				arr := tree.MustBeDArray(args[0])
				dimen := int64(tree.MustBeDInt(args[1]))
				return arrayLength(arr, dimen), nil
			},
			Info: "Calculates the maximum value of `input` on the provided `array_dimension`. " +
				"However, because CockroachDB doesn't yet support multi-dimensional arrays, the only " +
				"supported `array_dimension` is **1**.",
			Volatility: volatility.Immutable,
		},
	),

	"array_append": setProps(arrayProps(), arrayBuiltin(func(typ *types.T) tree.Overload {
		return tree.Overload{
			Types: tree.ParamTypes{{Name: "array", Typ: types.MakeArray(typ)}, {Name: "elem", Typ: typ}},
			ReturnType: func(args []tree.TypedExpr) *types.T {
				if len(args) > 0 {
					if argTyp := args[0].ResolvedType(); argTyp.Family() != types.UnknownFamily {
						return argTyp
					}
				}
				return types.MakeArray(typ)
			},
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.AppendToMaybeNullArray(typ, args[0], args[1])
			},
			Info:              "Appends `elem` to `array`, returning the result.",
			Volatility:        volatility.Immutable,
			CalledOnNullInput: true,
		}
	}, false /* supportsArrayInput */)),

	"array_prepend": setProps(arrayProps(), arrayBuiltin(func(typ *types.T) tree.Overload {
		return tree.Overload{
			Types: tree.ParamTypes{{Name: "elem", Typ: typ}, {Name: "array", Typ: types.MakeArray(typ)}},
			ReturnType: func(args []tree.TypedExpr) *types.T {
				if len(args) > 1 {
					if argTyp := args[1].ResolvedType(); argTyp.Family() != types.UnknownFamily {
						return argTyp
					}
				}
				return types.MakeArray(typ)
			},
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.PrependToMaybeNullArray(typ, args[0], args[1])
			},
			Info:              "Prepends `elem` to `array`, returning the result.",
			Volatility:        volatility.Immutable,
			CalledOnNullInput: true,
		}
	}, false /* supportsArrayInput */)),

	"array_cat": setProps(arrayProps(), arrayBuiltin(func(typ *types.T) tree.Overload {
		return tree.Overload{
			Types: tree.ParamTypes{{Name: "left", Typ: types.MakeArray(typ)}, {Name: "right", Typ: types.MakeArray(typ)}},
			ReturnType: func(args []tree.TypedExpr) *types.T {
				if len(args) > 1 {
					if argTyp := args[1].ResolvedType(); argTyp.Family() != types.UnknownFamily {
						return argTyp
					}
				}
				if len(args) > 2 {
					if argTyp := args[2].ResolvedType(); argTyp.Family() != types.UnknownFamily {
						return argTyp
					}
				}
				return types.MakeArray(typ)
			},
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.ConcatArrays(typ, args[0], args[1])
			},
			Info:              "Appends two arrays.",
			Volatility:        volatility.Immutable,
			CalledOnNullInput: true,
		}
	}, false /* supportsArrayInput */)),

	"array_remove": setProps(arrayProps(), arrayBuiltin(func(typ *types.T) tree.Overload {
		return tree.Overload{
			Types:      tree.ParamTypes{{Name: "array", Typ: types.MakeArray(typ)}, {Name: "elem", Typ: typ}},
			ReturnType: tree.IdentityReturnType(0),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				result := tree.NewDArray(typ)
				for _, e := range tree.MustBeDArray(args[0]).Array {
					cmp, err := e.Compare(ctx, evalCtx, args[1])
					if err != nil {
						return nil, err
					}
					if cmp != 0 {
						if err := result.Append(e); err != nil {
							return nil, err
						}
					}
				}
				return result, nil
			},
			Info:              "Remove from `array` all elements equal to `elem`.",
			Volatility:        volatility.Immutable,
			CalledOnNullInput: true,
		}
	}, false /* supportsArrayInput */)),

	"array_replace": setProps(arrayProps(), arrayBuiltin(func(typ *types.T) tree.Overload {
		return tree.Overload{
			Types:      tree.ParamTypes{{Name: "array", Typ: types.MakeArray(typ)}, {Name: "toreplace", Typ: typ}, {Name: "replacewith", Typ: typ}},
			ReturnType: tree.IdentityReturnType(0),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				result := tree.NewDArray(typ)
				for _, e := range tree.MustBeDArray(args[0]).Array {
					cmp, err := e.Compare(ctx, evalCtx, args[1])
					if err != nil {
						return nil, err
					}
					if cmp == 0 {
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
			Info:              "Replace all occurrences of `toreplace` in `array` with `replacewith`.",
			Volatility:        volatility.Immutable,
			CalledOnNullInput: true,
		}
	}, false /* supportsArrayInput */)),

	"array_position": setProps(arrayProps(), arrayVariadicBuiltin(func(typ *types.T) []tree.Overload {
		return []tree.Overload{
			{
				Types:      tree.ParamTypes{{Name: "array", Typ: types.MakeArray(typ)}, {Name: "elem", Typ: typ}},
				ReturnType: tree.FixedReturnType(types.Int),
				Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
					if args[0] == tree.DNull {
						return tree.DNull, nil
					}
					for i, e := range tree.MustBeDArray(args[0]).Array {
						cmp, err := e.Compare(ctx, evalCtx, args[1])
						if err != nil {
							return nil, err
						}
						if cmp == 0 {
							return tree.NewDInt(tree.DInt(i + 1)), nil
						}
					}
					return tree.DNull, nil
				},
				Info:              "Return the index of the first occurrence of `elem` in `array`.",
				Volatility:        volatility.Immutable,
				CalledOnNullInput: true,
			},
			{
				Types:      tree.ParamTypes{{Name: "array", Typ: types.MakeArray(typ)}, {Name: "elem", Typ: typ}, {Name: "start", Typ: types.Int}},
				ReturnType: tree.FixedReturnType(types.Int),
				Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
					if args[0] == tree.DNull {
						return tree.DNull, nil
					} else if args[2] == tree.DNull {
						return nil, pgerror.Newf(pgcode.NullValueNotAllowed, "initial position must not be null")
					}
					darray := tree.MustBeDArray(args[0]).Array
					start := int(tree.MustBeDInt(args[2]))
					start = max(start, 1) // PostgreSQL behaviour - start < 1 is implicitly treated as 1
					if start > len(darray) {
						return tree.DNull, nil
					}

					darray = darray[start-1:]
					for i, e := range darray {
						cmp, err := e.Compare(ctx, evalCtx, args[1])
						if err != nil {
							return nil, err
						}
						if cmp == 0 {
							return tree.NewDInt(tree.DInt(i + start)), nil
						}
					}
					return tree.DNull, nil
				},
				Info:              "Return the index of the first occurrence of `elem` in `array`, with the search begins at `start` index.",
				Volatility:        volatility.Immutable,
				CalledOnNullInput: true,
			},
		}
	})),

	"array_positions": setProps(arrayProps(), arrayBuiltin(func(typ *types.T) tree.Overload {
		return tree.Overload{
			Types:      tree.ParamTypes{{Name: "array", Typ: types.MakeArray(typ)}, {Name: "elem", Typ: typ}},
			ReturnType: tree.FixedReturnType(types.IntArray),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				result := tree.NewDArray(types.Int)
				for i, e := range tree.MustBeDArray(args[0]).Array {
					cmp, err := e.Compare(ctx, evalCtx, args[1])
					if err != nil {
						return nil, err
					}
					if cmp == 0 {
						if err := result.Append(tree.NewDInt(tree.DInt(i + 1))); err != nil {
							return nil, err
						}
					}
				}
				return result, nil
			},
			Info:              "Returns an array of indexes of all occurrences of `elem` in `array`.",
			Volatility:        volatility.Immutable,
			CalledOnNullInput: true,
		}
	}, false /* supportsArrayInput */)),

	// Full text search functions.
	"ts_match_qv":                    makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: builtinconstants.CategoryFullTextSearch}),
	"ts_match_vq":                    makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: builtinconstants.CategoryFullTextSearch}),
	"tsvector_cmp":                   makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: builtinconstants.CategoryFullTextSearch}),
	"tsvector_concat":                makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: builtinconstants.CategoryFullTextSearch}),
	"ts_debug":                       makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: builtinconstants.CategoryFullTextSearch}),
	"ts_headline":                    makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: builtinconstants.CategoryFullTextSearch}),
	"ts_lexize":                      makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: builtinconstants.CategoryFullTextSearch}),
	"websearch_to_tsquery":           makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: builtinconstants.CategoryFullTextSearch}),
	"array_to_tsvector":              makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: builtinconstants.CategoryFullTextSearch}),
	"get_current_ts_config":          makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: builtinconstants.CategoryFullTextSearch}),
	"numnode":                        makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: builtinconstants.CategoryFullTextSearch}),
	"querytree":                      makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: builtinconstants.CategoryFullTextSearch}),
	"setweight":                      makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: builtinconstants.CategoryFullTextSearch}),
	"strip":                          makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: builtinconstants.CategoryFullTextSearch}),
	"json_to_tsvector":               makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: builtinconstants.CategoryFullTextSearch}),
	"jsonb_to_tsvector":              makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: builtinconstants.CategoryFullTextSearch}),
	"ts_delete":                      makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: builtinconstants.CategoryFullTextSearch}),
	"ts_filter":                      makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: builtinconstants.CategoryFullTextSearch}),
	"ts_rank_cd":                     makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: builtinconstants.CategoryFullTextSearch}),
	"ts_rewrite":                     makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: builtinconstants.CategoryFullTextSearch}),
	"tsquery_phrase":                 makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: builtinconstants.CategoryFullTextSearch}),
	"tsvector_to_array":              makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: builtinconstants.CategoryFullTextSearch}),
	"tsvector_update_trigger":        makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: builtinconstants.CategoryFullTextSearch}),
	"tsvector_update_trigger_column": makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 7821, Category: builtinconstants.CategoryFullTextSearch}),

	// Fuzzy String Matching
	"soundex": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryFuzzyStringMatching},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "source", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				t := fuzzystrmatch.Soundex(s)
				return tree.NewDString(t), nil
			},
			Info:       "Convert a string to its Soundex code.",
			Volatility: volatility.Immutable,
		},
	),
	// The function is confusingly named, `similarity` would have been a better name,
	// but this name matches the name in PostgreSQL.
	// See https://www.postgresql.org/docs/current/fuzzystrmatch.html"
	"difference": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryString},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "source", Typ: types.String}, {Name: "target", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				s, t := string(tree.MustBeDString(args[0])), string(tree.MustBeDString(args[1]))
				diff := fuzzystrmatch.Difference(s, t)
				return tree.NewDInt(tree.DInt(diff)), nil
			},
			Info:       "Convert two strings to their Soundex codes and report the number of matching code positions.",
			Volatility: volatility.Immutable,
		},
	),
	"levenshtein": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryFuzzyStringMatching},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "source", Typ: types.String}, {Name: "target", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{{Name: "source", Typ: types.String}, {Name: "target", Typ: types.String},
				{Name: "ins_cost", Typ: types.Int}, {Name: "del_cost", Typ: types.Int}, {Name: "sub_cost", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
	),
	"levenshtein_less_equal": makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 56820, Category: builtinconstants.CategoryFuzzyStringMatching}),
	"metaphone": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryFuzzyStringMatching},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "source", Typ: types.String}, {Name: "max_output_length", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				const maxDefaultLen = 255
				s := string(tree.MustBeDString(args[0]))
				maxOutputLen := int(tree.MustBeDInt(args[1]))
				if len(s) > maxDefaultLen {
					return nil, pgerror.Newf(pgcode.InvalidParameterValue,
						"argument exceeds maximum length of %d characters", maxDefaultLen)
				}
				if maxOutputLen > maxDefaultLen {
					return nil, pgerror.Newf(pgcode.InvalidParameterValue,
						"output exceeds maximum length of %d characters", maxDefaultLen)
				}
				if maxOutputLen <= 0 {
					return nil, pgerror.Newf(pgcode.InvalidParameterValue,
						"output length must be > 0")
				}
				m := fuzzystrmatch.Metaphone(s, maxDefaultLen)
				return tree.NewDString(m), nil
			},
			Info:       "Convert a string to its Metaphone code. Maximum input length is 255 characters",
			Volatility: volatility.Immutable,
		},
	),
	"dmetaphone":     makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 56820, Category: builtinconstants.CategoryFuzzyStringMatching}),
	"dmetaphone_alt": makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 56820, Category: builtinconstants.CategoryFuzzyStringMatching}),

	// JSON functions.
	// The behavior of both the JSON and JSONB data types in CockroachDB is
	// similar to the behavior of the JSONB data type in Postgres.

	"jsonb_path_exists":      makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 22513, Category: builtinconstants.CategoryJsonpath}),
	"jsonb_path_exists_opr":  makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 22513, Category: builtinconstants.CategoryJsonpath}),
	"jsonb_path_match":       makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 22513, Category: builtinconstants.CategoryJsonpath}),
	"jsonb_path_match_opr":   makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 22513, Category: builtinconstants.CategoryJsonpath}),
	"jsonb_path_query":       makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 22513, Category: builtinconstants.CategoryJsonpath}),
	"jsonb_path_query_array": makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 22513, Category: builtinconstants.CategoryJsonpath}),
	"jsonb_path_query_first": makeBuiltin(tree.FunctionProperties{UnsupportedWithIssue: 22513, Category: builtinconstants.CategoryJsonpath}),

	"json_remove_path": makeBuiltin(jsonProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.Jsonb}, {Name: "path", Typ: types.StringArray}},
			ReturnType: tree.FixedReturnType(types.Jsonb),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
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
			Types:      tree.ParamTypes{{Name: "val", Typ: types.Jsonb}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				s, err := json.Pretty(tree.MustBeDJSON(args[0]).JSON)
				if err != nil {
					return nil, err
				}
				return tree.NewDString(s), nil
			},
			Info:       "Returns the given JSON value as a STRING indented and with newlines.",
			Volatility: volatility.Immutable,
		},
	),

	"json_typeof": makeBuiltin(jsonProps(), jsonTypeOfImpl),

	"jsonb_typeof": makeBuiltin(jsonProps(), jsonTypeOfImpl),

	"array_to_json": arrayToJSONImpls,

	"to_json": makeBuiltin(jsonProps(), toJSONImpl),

	"to_jsonb": makeBuiltin(jsonProps(), toJSONImpl),

	"json_build_array": makeBuiltin(jsonProps(), jsonBuildArrayImpl),

	"jsonb_build_array": makeBuiltin(jsonProps(), jsonBuildArrayImpl),

	"json_build_object": makeBuiltin(jsonProps(), jsonBuildObjectImpl),

	"jsonb_build_object": makeBuiltin(jsonProps(), jsonBuildObjectImpl),

	"json_object": jsonObjectImpls(),

	"jsonb_object": jsonObjectImpls(),

	"json_strip_nulls": makeBuiltin(jsonProps(), jsonStripNullsImpl),

	"jsonb_strip_nulls": makeBuiltin(jsonProps(), jsonStripNullsImpl),

	"json_array_length": makeBuiltin(jsonProps(), jsonArrayLengthImpl),

	"jsonb_array_length": makeBuiltin(jsonProps(), jsonArrayLengthImpl),

	"jsonb_exists_any": makeBuiltin(
		jsonProps(),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "json", Typ: types.Jsonb},
				{Name: "array", Typ: types.StringArray},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.JSONExistsAny(tree.MustBeDJSON(args[0]), tree.MustBeDArray(args[1]))
			},
			Info:       "Returns whether any of the strings in the text array exist as top-level keys or array elements",
			Volatility: volatility.Immutable,
		},
	),

	"json_valid": makeBuiltin(
		jsonProps(),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "string", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return jsonValidate(evalCtx, tree.MustBeDString(args[0])), nil
			},
			Info:       "Returns whether the given string is a valid JSON or not",
			Volatility: volatility.Immutable,
		},
	),

	"oidvectortypes": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategoryCompatibility,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "vector", Typ: types.OidVector},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				var err error
				oidVector := args[0].(*tree.DArray)
				result := strings.Builder{}
				for idx, datum := range oidVector.Array {
					oidDatum := datum.(*tree.DOid)
					var typ *types.T
					if resolvedTyp, ok := types.OidToType[oidDatum.Oid]; ok {
						typ = resolvedTyp
					} else {
						typ, err = evalCtx.Planner.ResolveTypeByOID(ctx, oidDatum.Oid)
						if err != nil {
							return nil, err
						}
					}
					result.WriteString(typ.Name())
					if idx != len(oidVector.Array)-1 {
						result.WriteString(", ")
					}
				}
				return tree.NewDString(result.String()), nil
			},
			Info:       "Generates a comma seperated string of type names from an oidvector.",
			Volatility: volatility.Stable,
		}),

	"crdb_internal.pb_to_json": makeBuiltin(
		jsonProps(),
		func() []tree.Overload {
			returnType := tree.FixedReturnType(types.Jsonb)
			const info = "Converts protocol message to its JSONB representation."
			volatility := volatility.Immutable
			pbToJSON := func(typ string, data []byte, flags protoreflect.FmtFlags) (tree.Datum, error) {
				msg, err := protoreflect.DecodeMessage(typ, data)
				if err != nil {
					return nil, pgerror.Wrap(err, pgcode.InvalidParameterValue, "invalid protocol message")
				}
				j, err := protoreflect.MessageToJSON(msg, flags)
				if err != nil {
					return nil, err
				}
				return tree.NewDJSON(j), nil
			}

			return []tree.Overload{
				{
					Info:       info,
					Volatility: volatility,
					Types: tree.ParamTypes{
						{Name: "pbname", Typ: types.String},
						{Name: "data", Typ: types.Bytes},
					},
					ReturnType: returnType,
					Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
						return pbToJSON(
							string(tree.MustBeDString(args[0])),
							[]byte(tree.MustBeDBytes(args[1])),
							protoreflect.FmtFlags{EmitDefaults: false, EmitRedacted: false},
						)
					},
				},
				{
					Info:       info,
					Volatility: volatility,
					Types: tree.ParamTypes{
						{Name: "pbname", Typ: types.String},
						{Name: "data", Typ: types.Bytes},
						{Name: "emit_defaults", Typ: types.Bool},
					},
					ReturnType: returnType,
					Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
						return pbToJSON(
							string(tree.MustBeDString(args[0])),
							[]byte(tree.MustBeDBytes(args[1])),
							protoreflect.FmtFlags{
								EmitDefaults: bool(tree.MustBeDBool(args[2])),
								EmitRedacted: false,
							},
						)
					},
				},
				{
					Info:       info,
					Volatility: volatility,
					Types: tree.ParamTypes{
						{Name: "pbname", Typ: types.String},
						{Name: "data", Typ: types.Bytes},
						{Name: "emit_defaults", Typ: types.Bool},
						{Name: "emit_redacted", Typ: types.Bool},
					},
					ReturnType: returnType,
					Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
						return pbToJSON(
							string(tree.MustBeDString(args[0])),
							[]byte(tree.MustBeDBytes(args[1])),
							protoreflect.FmtFlags{
								EmitDefaults: bool(tree.MustBeDBool(args[2])),
								EmitRedacted: bool(tree.MustBeDBool(args[3])),
							},
						)
					},
				},
			}
		}()...),

	"crdb_internal.job_payload_type": makeBuiltin(
		jsonProps(),
		func() []tree.Overload {
			returnType := tree.FixedReturnType(types.String)
			getJobPayloadType := func(data []byte) (tree.Datum, error) {
				var msg jobspb.Payload
				if err := protoutil.Unmarshal(data, &msg); err != nil {
					return nil, pgerror.Wrap(err, pgcode.InvalidParameterValue, "invalid protocol message")
				}
				typ, err := msg.CheckType()
				if err != nil {
					return nil, pgerror.Wrap(err, pgcode.InvalidParameterValue, "invalid type in job payload protocol message")
				}
				return tree.NewDString(typ.String()), nil
			}

			return []tree.Overload{
				{
					Info:       "Reads the type from the jobspb.Payload protocol message.",
					Volatility: volatility.Immutable,
					Types: tree.ParamTypes{
						{Name: "data", Typ: types.Bytes},
					},
					ReturnType: returnType,
					Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
						return getJobPayloadType(
							[]byte(tree.MustBeDBytes(args[0])),
						)
					},
				},
			}
		}()...),

	"crdb_internal.json_to_pb": makeBuiltin(
		jsonProps(),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "pbname", Typ: types.String},
				{Name: "json", Typ: types.Jsonb},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				msg, err := protoreflect.NewMessage(string(tree.MustBeDString(args[0])))
				if err != nil {
					return nil, pgerror.Wrap(err, pgcode.InvalidParameterValue, "invalid proto name")
				}
				data, err := protoreflect.JSONBMarshalToMessage(tree.MustBeDJSON(args[1]).JSON, msg)
				if err != nil {
					return nil, pgerror.Wrap(err, pgcode.InvalidParameterValue, "invalid proto JSON")
				}
				return tree.NewDBytes(tree.DBytes(data)), nil
			},
			Info:       "Convert JSONB data to protocol message bytes",
			Volatility: volatility.Immutable,
		}),

	"crdb_internal.job_execution_details": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "job_id", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Jsonb),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return nil, pgerror.Newf(pgcode.NullValueNotAllowed, "argument cannot be NULL")
				}
				jobID := tree.MustBeDInt(args[0])
				json, err := evalCtx.JobsProfiler.GenerateExecutionDetailsJSON(ctx, evalCtx, jobspb.JobID(jobID))
				if err != nil {
					return nil, err
				}
				return tree.ParseDJSON(string(json))
			},
			Info: "Output a JSONB version of the specified job's execution details. The execution details are collected" +
				"and persisted during the lifetime of the job and provide more observability into the job's execution",
			Volatility: volatility.Volatile,
		}),

	"crdb_internal.read_file": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "uri", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				uri := string(tree.MustBeDString(args[0]))
				content, err := evalCtx.Planner.ExternalReadFile(ctx, uri)
				return tree.NewDBytes(tree.DBytes(content)), err
			},
			Info:       "Read the content of the file at the supplied external storage URI",
			Volatility: volatility.Volatile,
		}),

	"crdb_internal.write_file": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "data", Typ: types.Bytes},
				{Name: "uri", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				data := tree.MustBeDBytes(args[0])
				uri := string(tree.MustBeDString(args[1]))
				if err := evalCtx.Planner.ExternalWriteFile(ctx, uri, []byte(data)); err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(len(data))), nil
			},
			Info:       "Write the content passed to a file at the supplied external storage URI",
			Volatility: volatility.Volatile,
		}),

	"crdb_internal.datums_to_bytes": makeBuiltin(
		tree.FunctionProperties{
			Category:             builtinconstants.CategorySystemInfo,
			Undocumented:         true,
			CompositeInsensitive: true,
		},
		tree.Overload{
			// Note that datums_to_bytes(a) == datums_to_bytes(b) iff (a IS NOT DISTINCT FROM b)
			Info: "Converts datums into key-encoded bytes. " +
				"Supports NULLs and all data types which may be used in index keys",
			Types:      tree.VariadicType{VarType: types.AnyElement},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				var out []byte
				for i, arg := range args {
					var err error
					out, err = keyside.Encode(out, arg, encoding.Ascending)
					if err != nil {
						return nil, pgerror.Newf(
							pgcode.DatatypeMismatch,
							"illegal argument %d of type %s",
							i, arg.ResolvedType(),
						)
					}
				}
				return tree.NewDBytes(tree.DBytes(out)), nil
			},
			Volatility:        volatility.Immutable,
			CalledOnNullInput: true,
		},
	),
	"crdb_internal.merge_statement_stats": makeBuiltin(arrayProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "input", Typ: types.JSONBArray}},
			ReturnType: tree.FixedReturnType(types.Jsonb),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				arr := tree.MustBeDArray(args[0])
				var aggregatedStats appstatspb.StatementStatistics
				for _, statsDatum := range arr.Array {
					err := mergeStatementStatsHelper(&aggregatedStats, statsDatum)
					if err != nil {
						return nil, err
					}
				}

				aggregatedJSON, err := sqlstatsutil.BuildStmtStatisticsJSON(&aggregatedStats)
				if err != nil {
					return nil, err
				}

				return tree.NewDJSON(aggregatedJSON), nil
			},
			Info:       "Merge an array of appstatspb.StatementStatistics into a single JSONB object",
			Volatility: volatility.Immutable,
		},
	),
	"crdb_internal.merge_transaction_stats": makeBuiltin(arrayProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "input", Typ: types.JSONBArray}},
			ReturnType: tree.FixedReturnType(types.Jsonb),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				arr := tree.MustBeDArray(args[0])
				var aggregatedStats appstatspb.TransactionStatistics
				for _, statsDatum := range arr.Array {
					if statsDatum == tree.DNull {
						continue
					}
					err := mergeTransactionStatsHelper(&aggregatedStats, statsDatum)
					if err != nil {
						return nil, err
					}
				}

				aggregatedJSON, err := sqlstatsutil.BuildTxnStatisticsJSON(
					&appstatspb.CollectedTransactionStatistics{
						Stats: aggregatedStats,
					})
				if err != nil {
					return nil, err
				}

				return tree.NewDJSON(aggregatedJSON), nil
			},
			Info:       "Merge an array of appstatspb.TransactionStatistics into a single JSONB object",
			Volatility: volatility.Immutable,
		},
	),
	"crdb_internal.merge_stats_metadata": makeBuiltin(arrayProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "input", Typ: types.JSONBArray}},
			ReturnType: tree.FixedReturnType(types.Jsonb),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				arr := tree.MustBeDArray(args[0])
				metadata := &appstatspb.AggregatedStatementMetadata{}

				for _, metadataDatum := range arr.Array {
					if metadataDatum == tree.DNull {
						continue
					}

					err := mergeStatsMetadataHelper(metadata, metadataDatum)
					if err != nil {
						return nil, err
					}
				}
				aggregatedJSON, err := sqlstatsutil.BuildStmtDetailsMetadataJSON(metadata)
				if err != nil {
					return nil, err
				}

				return tree.NewDJSON(aggregatedJSON), nil
			},
			Info:       "Merge an array of StmtStatsMetadata into a single JSONB object",
			Volatility: volatility.Immutable,
		},
	),
	"crdb_internal.merge_aggregated_stmt_metadata": makeBuiltin(arrayProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "input", Typ: types.JSONBArray}},
			ReturnType: tree.FixedReturnType(types.Jsonb),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				arr := tree.MustBeDArray(args[0])
				metadata := &appstatspb.AggregatedStatementMetadata{}

				other := appstatspb.AggregatedStatementMetadata{}
				for _, metadataDatum := range arr.Array {
					if err := mergeAggregatedMetadataHelper(metadata, &other, metadataDatum); err != nil {
						continue
					}
				}

				aggregatedJSON, err := sqlstatsutil.BuildStmtDetailsMetadataJSON(metadata)
				if err != nil {
					return nil, err
				}

				return tree.NewDJSON(aggregatedJSON), nil
			},
			Info:       "Merge an array of AggregatedStatementMetadata into a single JSONB object",
			Volatility: volatility.Immutable,
		},
	),

	// Enum functions.
	"enum_first": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryEnum},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.AnyEnum}},
			ReturnType: tree.IdentityReturnType(0),
			FnWithExprs: makeEnumTypeFunc(func(enumType *types.T) (tree.Datum, error) {
				enum := tree.DEnum{EnumTyp: enumType}
				min, ok := enum.MinWriteable()
				if !ok {
					return nil, errors.Newf("enum %s contains no values", enumType.Name())
				}
				return min, nil
			}),
			Info:              "Returns the first value of the input enum type.",
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
		},
	),

	"enum_last": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryEnum},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.AnyEnum}},
			ReturnType: tree.IdentityReturnType(0),
			FnWithExprs: makeEnumTypeFunc(func(enumType *types.T) (tree.Datum, error) {
				enum := tree.DEnum{EnumTyp: enumType}
				max, ok := enum.MaxWriteable()
				if !ok {
					return nil, errors.Newf("enum %s contains no values", enumType.Name())
				}
				return max, nil
			}),
			Info:              "Returns the last value of the input enum type.",
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
		},
	),

	"enum_range": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryEnum},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.AnyEnum}},
			ReturnType: tree.ArrayOfFirstNonNullReturnType(),
			FnWithExprs: makeEnumTypeFunc(func(enumType *types.T) (tree.Datum, error) {
				arr := tree.NewDArray(enumType)
				for i := range enumType.TypeMeta.EnumData.LogicalRepresentations {
					// Read-only members should be excluded.
					if enumType.TypeMeta.EnumData.IsMemberReadOnly[i] {
						continue
					}
					enum := &tree.DEnum{
						EnumTyp:     enumType,
						PhysicalRep: enumType.TypeMeta.EnumData.PhysicalRepresentations[i],
						LogicalRep:  enumType.TypeMeta.EnumData.LogicalRepresentations[i],
					}
					if err := arr.Append(enum); err != nil {
						return nil, err
					}
				}
				return arr, nil
			}),
			Info:              "Returns all values of the input enum in an ordered array.",
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "lower", Typ: types.AnyEnum}, {Name: "upper", Typ: types.AnyEnum}},
			ReturnType: tree.ArrayOfFirstNonNullReturnType(),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Info:              "Returns all values of the input enum in an ordered array between the two arguments (inclusive).",
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
		},
	),

	// Metadata functions.

	// https://www.postgresql.org/docs/10/static/functions-info.html
	"version": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(build.GetInfo().Short().StripMarkers()), nil
			},
			Info:       "Returns the node's version of CockroachDB.",
			Volatility: volatility.Volatile,
		},
	),

	// https://www.postgresql.org/docs/10/static/functions-info.html
	"current_database": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if len(evalCtx.SessionData().Database) == 0 {
					return tree.DNull, nil
				}
				return tree.NewDString(evalCtx.SessionData().Database), nil
			},
			Info:       "Returns the current database.",
			Volatility: volatility.Stable,
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
			Category:         builtinconstants.CategorySystemInfo,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				curDb := evalCtx.SessionData().Database
				iter := evalCtx.SessionData().SearchPath.IterWithoutImplicitPGSchemas()
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
			Volatility: volatility.Stable,
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
			Category:         builtinconstants.CategorySystemInfo,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "include_pg_catalog", Typ: types.Bool}},
			ReturnType: tree.FixedReturnType(types.StringArray),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				curDb := evalCtx.SessionData().Database
				includeImplicitPgSchemas := *(args[0].(*tree.DBool))
				schemas := tree.NewDArray(types.String)
				var iter sessiondata.SearchPathIter
				if includeImplicitPgSchemas {
					iter = evalCtx.SessionData().SearchPath.Iter()
				} else {
					iter = evalCtx.SessionData().SearchPath.IterWithoutImplicitPGSchemas()
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
			Volatility: volatility.Stable,
		},
	),

	// https://www.postgresql.org/docs/10/static/functions-info.html
	"current_user": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if evalCtx.SessionData().User().Undefined() {
					return tree.DNull, nil
				}
				return tree.NewDString(evalCtx.SessionData().User().Normalized()), nil
			},
			Info: "Returns the current user. This function is provided for " +
				"compatibility with PostgreSQL.",
			Volatility: volatility.Stable,
		},
	),

	"session_user": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				u := evalCtx.SessionData().SessionUser()
				if u.Undefined() {
					return tree.DNull, nil
				}
				return tree.NewDString(u.Normalized()), nil
			},
			Info: "Returns the session user. This function is provided for " +
				"compatibility with PostgreSQL.",
			Volatility: volatility.Stable,
		},
	),

	// Get the current trace ID.
	"crdb_internal.trace_id": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				// The user must have VIEWCLUSTERMETADATA to use this builtin.
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}

				sp := tracing.SpanFromContext(ctx)
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
			Volatility: volatility.Volatile,
		},
	),

	// Toggles all spans of the requested trace to verbose or non-verbose.
	"crdb_internal.set_trace_verbose": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "trace_id", Typ: types.Int},
				{Name: "verbosity", Typ: types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				// The user must have REPAIRCLUSTER to use this builtin.
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}

				traceID := tracingpb.TraceID(*(args[0].(*tree.DInt)))
				verbosity := bool(*(args[1].(*tree.DBool)))

				var rootSpan tracing.RegistrySpan
				if evalCtx.Tracer == nil {
					return nil, errors.AssertionFailedf("Tracer not configured")
				}
				if err := evalCtx.Tracer.VisitSpans(func(span tracing.RegistrySpan) error {
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

				var recType tracingpb.RecordingType
				if verbosity {
					recType = tracingpb.RecordingVerbose
				} else {
					recType = tracingpb.RecordingOff
				}
				rootSpan.SetRecordingType(recType)
				return tree.DBoolTrue, nil
			},
			Info:       "Returns true if root span was found and verbosity was set, false otherwise.",
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.locality_value": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "key", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				s, ok := tree.AsDString(args[0])
				if !ok {
					return nil, errors.Newf("expected string value, got %T", args[0])
				}
				key := string(s)
				for i := range evalCtx.Locality.Tiers {
					tier := &evalCtx.Locality.Tiers[i]
					if tier.Key == key {
						return tree.NewDString(tier.Value), nil
					}
				}
				return tree.DNull, nil
			},
			Info:       "Returns the value of the specified locality key.",
			Volatility: volatility.Stable,
		},
	),

	"crdb_internal.cluster_setting_encoded_default": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "setting", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				s, ok := tree.AsDString(args[0])
				if !ok {
					return nil, errors.AssertionFailedf("expected string value, got %T", args[0])
				}
				name := settings.SettingName(strings.ToLower(string(s)))
				setting, ok, _ := settings.LookupForLocalAccess(name, evalCtx.Codec.ForSystemTenant())
				if !ok {
					return nil, errors.Newf("unknown cluster setting '%s'", name)
				}

				return tree.NewDString(setting.EncodedDefault()), nil
			},
			Info:       "Returns the encoded default value of the given cluster setting.",
			Volatility: volatility.Immutable,
		},
	),

	"crdb_internal.decode_cluster_setting": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "setting", Typ: types.String},
				{Name: "value", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				s, ok := tree.AsDString(args[0])
				if !ok {
					return nil, errors.AssertionFailedf("expected string value, got %T", args[0])
				}
				encoded, ok := tree.AsDString(args[1])
				if !ok {
					return nil, errors.AssertionFailedf("expected string value, got %T", args[1])
				}
				name := settings.SettingName(strings.ToLower(string(s)))
				setting, ok, _ := settings.LookupForLocalAccess(name, evalCtx.Codec.ForSystemTenant())
				if !ok {
					return nil, errors.Newf("unknown cluster setting '%s'", name)
				}
				repr, err := setting.DecodeToString(string(encoded))
				if err != nil {
					return nil, errors.Wrapf(err, "%v", name)
				}
				return tree.NewDString(repr), nil
			},
			Info:       "Decodes the given encoded value for a cluster setting.",
			Volatility: volatility.Immutable,
		},
	),

	"crdb_internal.node_executable_version": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				v := evalCtx.Settings.Version.LatestVersion().String()
				return tree.NewDString(v), nil
			},
			Info:       "Returns the version of CockroachDB this node is running.",
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.active_version": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Jsonb),
			Fn: func(ctx context.Context, evalCtx *eval.Context, _ tree.Datums) (tree.Datum, error) {
				activeVersion := evalCtx.Settings.Version.ActiveVersionOrEmpty(ctx)
				jsonStr, err := gojson.Marshal(&activeVersion.Version)
				if err != nil {
					return nil, err
				}
				jsonDatum, err := tree.ParseDJSON(string(jsonStr))
				if err != nil {
					return nil, err
				}
				return jsonDatum, nil
			},
			Info:       "Returns the current active cluster version.",
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.is_at_least_version": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "version", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				s, ok := tree.AsDString(args[0])
				if !ok {
					return nil, errors.Newf("expected string value, got %T", args[0])
				}
				arg, err := roachpb.ParseVersion(string(s))
				if err != nil {
					return nil, err
				}
				activeVersion := evalCtx.Settings.Version.ActiveVersionOrEmpty(ctx)
				if activeVersion == (clusterversion.ClusterVersion{}) {
					return nil, errors.AssertionFailedf("invalid uninitialized version")
				}
				if arg.LessEq(activeVersion.Version) {
					return tree.DBoolTrue, nil
				}
				return tree.DBoolFalse, nil
			},
			Info:       "Returns true if the cluster version is not older than the argument.",
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.release_series": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "version", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				s, ok := tree.AsDString(args[0])
				if !ok {
					return nil, errors.Newf("expected string value, got %T", args[0])
				}
				version, err := roachpb.ParseVersion(string(s))
				if err != nil {
					return nil, err
				}
				if version.Less(clusterversion.MinSupported.Version()) || clusterversion.Latest.Version().Less(version) {
					return nil, errors.Newf(
						"version %s not supported; this binary only understands versions %s through %s",
						args[0], clusterversion.MinSupported, clusterversion.Latest,
					)
				}
				for k := clusterversion.Latest; ; k-- {
					if k.Version().LessEq(version) {
						return tree.NewDString(k.ReleaseSeries().String()), nil
					}
				}
			},
			Info:       "Converts a cluster version to the final cluster version in that release series.",
			Volatility: volatility.Stable,
		},
	),

	"crdb_internal.approximate_timestamp": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "timestamp", Typ: types.Decimal}},
			ReturnType: tree.FixedReturnType(types.Timestamp),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return eval.DecimalToInexactDTimestamp(args[0].(*tree.DDecimal))
			},
			Info:       "Converts the crdb_internal_mvcc_timestamp column into an approximate timestamp.",
			Volatility: volatility.Immutable,
		},
	),

	"crdb_internal.cluster_id": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Uuid),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.NewDUuid(tree.DUuid{UUID: evalCtx.ClusterID}), nil
			},
			Info:       "Returns the logical cluster ID for this tenant.",
			Volatility: volatility.Stable,
		},
	),

	"crdb_internal.node_id": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				dNodeID := tree.DNull
				if nodeID, ok := evalCtx.NodeID.OptionalNodeID(); ok {
					dNodeID = tree.NewDInt(tree.DInt(nodeID))
				}
				return dNodeID, nil
			},
			Info:       "Returns the node ID.",
			Volatility: volatility.Stable,
		},
	),

	"crdb_internal.cluster_name": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(evalCtx.ClusterName), nil
			},
			Info:       "Returns the cluster name.",
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.create_tenant": makeBuiltin(
		tree.FunctionProperties{
			Category:     builtinconstants.CategoryMultiTenancy,
			Undocumented: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "parameters", Typ: types.Jsonb},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				tid, err := evalCtx.Tenant.CreateTenant(ctx,
					args[0].(*tree.DJSON).JSON.String(),
				)
				if err != nil {
					return nil, err
				}
				if !tid.IsSet() {
					return tree.DNull, nil
				}
				return tree.NewDInt(tree.DInt(tid.ToUint64())), nil
			},
			Info: `Creates a new tenant with the provided parameters. ` +
				`Must be run by the system tenant.`,
			Volatility: volatility.Volatile,
		},
		// This overload is provided for compatibility with CC Serverless
		// v22.2 and previous versions.
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "id", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Body: `SELECT crdb_internal.create_tenant(json_build_object('id', $1, 'service_mode',
 'external'))`,
			Info:       `create_tenant(id) is an alias for create_tenant('{"id": id, "service_mode": "external"}'::jsonb)`,
			Volatility: volatility.Volatile,
			Language:   tree.RoutineLangSQL,
		},
		// This overload is provided for use in tests.
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "id", Typ: types.Int},
				{Name: "name", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Body:       `SELECT crdb_internal.create_tenant(json_build_object('id', $1, 'name', $2))`,
			Info:       `create_tenant(id, name) is an alias for create_tenant('{"id": id, "name": name}'::jsonb)`,
			Volatility: volatility.Volatile,
			Language:   tree.RoutineLangSQL,
		},
		// This overload is deprecated. Use CREATE VIRTUAL CLUSTER instead.
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "name", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Body:       `SELECT crdb_internal.create_tenant(json_build_object('name', $1))`,
			Info: `create_tenant(name) is an alias for create_tenant('{"name": name}'::jsonb).
DO NOT USE -- USE 'CREATE VIRTUAL CLUSTER' INSTEAD`,
			Volatility: volatility.Volatile,
			Language:   tree.RoutineLangSQL,
		},
	),

	// destroy_tenant is preserved for compatibility with CockroachCloud
	// intrusion for v22.2 and previous versions.
	"crdb_internal.destroy_tenant": makeBuiltin(
		tree.FunctionProperties{
			Category:     builtinconstants.CategoryMultiTenancy,
			Undocumented: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "id", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Body:       `SELECT crdb_internal.destroy_tenant($1, false)`,
			Info:       "DO NOT USE -- USE 'DROP VIRTUAL CLUSTER' INSTEAD.",
			Volatility: volatility.Volatile,
			Language:   tree.RoutineLangSQL,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "id", Typ: types.Int},
				{Name: "synchronous", Typ: types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				sTenID, err := mustBeDIntInTenantRange(args[0])
				if err != nil {
					return nil, err
				}
				synchronous := tree.MustBeDBool(args[1])

				// Note: we pass true to ignoreServiceMode for compatibility
				// with CC Serverless pre-v23.1.
				if err := evalCtx.Tenant.DropTenantByID(
					ctx, uint64(sTenID), bool(synchronous), true, /* ignoreServiceMode */
				); err != nil {
					return nil, err
				}
				return args[0], nil
			},
			Info:       "DO NOT USE -- USE 'DROP VIRTUAL CLUSTER IMMEDIATE' INSTEAD.",
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.unsafe_clear_gossip_info": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "key", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				key, ok := tree.AsDString(args[0])
				if !ok {
					return nil, errors.Newf("expected string value, got %T", args[0])
				}
				ok, err := evalCtx.Gossip.TryClearGossipInfo(ctx, string(key))
				if err != nil {
					return nil, err
				}
				return tree.MakeDBool(tree.DBool(ok)), nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.encode_key": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "table_id", Typ: types.Int},
				{Name: "index_id", Typ: types.Int},
				{Name: "row_tuple", Typ: types.AnyElement},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				tableID := catid.DescID(tree.MustBeDInt(args[0]))
				indexID := catid.IndexID(tree.MustBeDInt(args[1]))
				rowDatums, ok := tree.AsDTuple(args[2])
				if !ok {
					return nil, pgerror.Newf(
						pgcode.DatatypeMismatch,
						"expected tuple argument for row_tuple, found %s",
						args[2],
					)
				}
				res, err := evalCtx.CatalogBuiltins.EncodeTableIndexKey(
					ctx, tableID, indexID, rowDatums,
					func(
						ctx context.Context, d tree.Datum, t *types.T,
					) (tree.Datum, error) {
						return eval.PerformCast(ctx, evalCtx, d, t)
					},
				)
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(res)), nil
			},
			Info:       "Generate the key for a row on a particular table and index.",
			Volatility: volatility.Stable,
		},
	),

	"crdb_internal.redact_descriptor": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategorySystemInfo,
			DistsqlBlocklist: true,
			Undocumented:     true,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "descriptor", Typ: types.Bytes}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				s, ok := tree.AsDBytes(args[0])
				if !ok {
					return nil, errors.Newf("expected bytes value, got %T", args[0])
				}
				ret, err := evalCtx.CatalogBuiltins.RedactDescriptor(ctx, []byte(s))
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(ret)), nil
			},
			Info:       "This function is used to redact expressions in descriptors",
			Volatility: volatility.Stable,
		},
	),
	"crdb_internal.descriptor_with_post_deserialization_changes": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategorySystemInfo,
			DistsqlBlocklist: true,
			Undocumented:     true,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "descriptor", Typ: types.Bytes}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				s, ok := tree.AsDBytes(args[0])
				if !ok {
					return nil, errors.Newf("expected bytes value, got %T", args[0])
				}
				descIDAlwaysValid := func(id descpb.ID) bool {
					return true
				}
				jobIDAlwaysValid := func(id jobspb.JobID) bool {
					return true
				}
				roleAlwaysValid := func(username username.SQLUsername) bool {
					return true
				}
				ret, err := evalCtx.CatalogBuiltins.RepairedDescriptor(
					ctx, []byte(s), descIDAlwaysValid, jobIDAlwaysValid, roleAlwaysValid,
				)
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(ret)), nil
			},
			Info:       "This function is used to update descriptor representations",
			Volatility: volatility.Stable,
		},
	),
	"crdb_internal.repaired_descriptor": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategorySystemInfo,
			DistsqlBlocklist: true,
			Undocumented:     true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "descriptor", Typ: types.Bytes},
				{Name: "valid_descriptor_ids", Typ: types.IntArray},
				{Name: "valid_job_ids", Typ: types.IntArray},
				{Name: "valid_roles", Typ: types.StringArray},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				s, ok := tree.AsDBytes(args[0])
				if !ok {
					return nil, errors.Newf("expected bytes value, got %T", args[0])
				}
				descIDMightExist := func(id descpb.ID) bool { return true }
				if args[1] != tree.DNull {
					descIDs, ok := tree.AsDArray(args[1])
					if !ok {
						return nil, errors.Newf("expected array value, got %T", args[1])
					}
					descIDMap := make(map[descpb.ID]struct{}, descIDs.Len())
					for i, n := 0, descIDs.Len(); i < n; i++ {
						id, isInt := tree.AsDInt(descIDs.Array[i])
						if !isInt {
							return nil, errors.Newf("expected int value, got %T", descIDs.Array[i])
						}
						descIDMap[descpb.ID(id)] = struct{}{}
					}
					descIDMightExist = func(id descpb.ID) bool {
						_, found := descIDMap[id]
						return found
					}
				}
				nonTerminalJobIDMightExist := func(id jobspb.JobID) bool { return true }
				if args[2] != tree.DNull {
					jobIDs, ok := tree.AsDArray(args[2])
					if !ok {
						return nil, errors.Newf("expected array value, got %T", args[2])
					}
					jobIDMap := make(map[jobspb.JobID]struct{}, jobIDs.Len())
					for i, n := 0, jobIDs.Len(); i < n; i++ {
						id, isInt := tree.AsDInt(jobIDs.Array[i])
						if !isInt {
							return nil, errors.Newf("expected int value, got %T", jobIDs.Array[i])
						}
						jobIDMap[jobspb.JobID(id)] = struct{}{}
					}
					nonTerminalJobIDMightExist = func(id jobspb.JobID) bool {
						_, found := jobIDMap[id]
						return found
					}
				}

				roleMightExist := func(username username.SQLUsername) bool {
					return true
				}
				if args[3] != tree.DNull {
					roles, ok := tree.AsDArray(args[3])
					if !ok {
						return nil, errors.Newf("expected array value, got %T", args[3])
					}
					roleMap := make(map[username.SQLUsername]struct{})
					for _, roleDatum := range (*roles).Array {
						role := tree.MustBeDString(roleDatum)
						roleName, err := username.MakeSQLUsernameFromUserInput(string(role), username.PurposeValidation)
						if err != nil {
							return nil, err
						}
						roleMap[roleName] = struct{}{}
					}
					roleMightExist = func(username username.SQLUsername) bool {
						_, ok := roleMap[username]
						return ok
					}
				}
				ret, err := evalCtx.CatalogBuiltins.RepairedDescriptor(
					ctx, []byte(s), descIDMightExist, nonTerminalJobIDMightExist, roleMightExist,
				)
				if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(ret)), nil
			},
			Info:       "This function is used to update descriptor representations",
			Volatility: volatility.Stable,
		},
	),
	"crdb_internal.repair_catalog_corruption": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategorySystemRepair,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "descriptor_id", Typ: types.Int},
				{Name: "corruption", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			// See the kv_repairable_catalog_corruptions virtual view definition for
			// details about the different corruption types.
			// Presently, 'descriptor' and 'namespace' are supported by this builtin.
			Body: `
SELECT
	CASE corruption
	WHEN 'descriptor'
	THEN (
		SELECT
			max(
				crdb_internal.unsafe_upsert_descriptor(
					id,
					crdb_internal.repaired_descriptor(
						descriptor,
						(SELECT array_agg(id) AS desc_id_array FROM system.descriptor),
						(
							SELECT
								array_agg(id) AS job_id_array
							FROM
								system.jobs
							WHERE
								status NOT IN ('failed', 'succeeded', 'canceled', 'revert-failed')
						),
						( SELECT
							array_agg(username) as username_array FROM
							(SELECT username
							FROM system.users UNION
							SELECT 'public' as username UNION
							SELECT 'node' as username)
						)
					),
					true
				)
			)
		FROM
			system.descriptor
		WHERE
			id = $1
	)
	WHEN 'namespace'
	THEN (
		SELECT
			max(
				crdb_internal.unsafe_delete_namespace_entry(
					"parentID",
					"parentSchemaID",
					name,
					id,
					true
				)
			)
		FROM
			system.namespace
		WHERE
			id = $1 AND id NOT IN (SELECT id FROM system.descriptor)
	)
	ELSE NULL
	END
`,
			Info: "repair_catalog_corruption(descriptor_id,corruption) attempts to repair corrupt" +
				" records in system tables associated with that descriptor id",
			Volatility: volatility.Volatile,
			Language:   tree.RoutineLangSQL,
		},
	),

	"crdb_internal.force_error": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "errorCode", Typ: types.String}, {Name: "msg", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				s, ok := tree.AsDString(args[0])
				if !ok {
					return nil, errors.Newf("expected string value, got %T", args[0])
				}
				errCode := string(s)
				s, ok = tree.AsDString(args[1])
				if !ok {
					return nil, errors.Newf("expected string value, got %T", args[1])
				}
				msg := string(s)
				// We construct the errors below via %s as the
				// message may contain PII.
				if errCode == "" {
					return nil, errors.Newf("%s", msg)
				}
				return nil, pgerror.Newf(pgcode.MakeCode(errCode), "%s", msg)
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.notice": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "msg", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				s, ok := tree.AsDString(args[0])
				if !ok {
					return nil, errors.Newf("expected string value, got %T", args[0])
				}
				msg := string(s)
				return crdbInternalBufferNotice(ctx, evalCtx, "NOTICE", msg)
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: volatility.Volatile,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "severity", Typ: types.String}, {Name: "msg", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				s, ok := tree.AsDString(args[0])
				if !ok {
					return nil, errors.Newf("expected string value, got %T", args[0])
				}
				severityString := string(s)
				s, ok = tree.AsDString(args[1])
				if !ok {
					return nil, errors.Newf("expected string value, got %T", args[1])
				}
				msg := string(s)
				if _, ok := pgnotice.ParseDisplaySeverity(severityString); !ok {
					return nil, pgerror.Newf(pgcode.InvalidParameterValue, "severity %s is invalid", severityString)
				}
				return crdbInternalBufferNotice(ctx, evalCtx, severityString, msg)
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.force_assertion_error": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "msg", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				s, ok := tree.AsDString(args[0])
				if !ok {
					return nil, errors.Newf("expected string value, got %T", args[0])
				}
				msg := string(s)
				return nil, errors.AssertionFailedf("%s", msg)
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.void_func": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Void),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.DVoidDatum, nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.force_panic": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "msg", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				// The user must have REPAIRCLUSTER to use this builtin.
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}

				s, ok := tree.AsDString(args[0])
				if !ok {
					return nil, errors.Newf("expected string value, got %T", args[0])
				}
				msg := string(s)
				// Use a special method to panic in order to go around the
				// vectorized panic-catcher (which would catch the panic from
				// Golang's 'panic' and would convert it into an internal
				// error).
				colexecerror.NonCatchablePanic(msg)
				// This code is unreachable.
				panic(msg)
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: volatility.Volatile,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "msg", Typ: types.String}, {Name: "mode", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				// The user must have REPAIRCLUSTER to use this builtin.
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}

				s, ok := tree.AsDString(args[0])
				if !ok {
					return nil, errors.Newf("expected string value, got %T", args[0])
				}
				msg := string(s)
				mode, ok := tree.AsDString(args[1])
				if !ok {
					return nil, errors.Newf("expected string value, got %T", args[1])
				}
				switch string(mode) {
				case "internalAssertion":
					err := errors.AssertionFailedf("%s", msg)
					// Panic instead of returning the error. The vectorized panic-catcher
					// will catch the panic and convert it into an internal error.
					colexecerror.InternalError(err)
				case "indexOutOfRange":
					msg += string(msg[math.MaxInt])
				case "divideByZero":
					var foo []int
					msg += strconv.Itoa(len(msg) / len(foo))
				case "contextCanceled":
					panic(context.Canceled)
				default:
					return nil, errors.Newf(
						"expected mode to be one of: internalAssertion, indexOutOfRange, divideByZero, contextCanceled",
					)
				}
				// This code is unreachable.
				panic(msg)
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: volatility.Volatile,
		},
	),
	"crdb_internal.log": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "msg", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Void),

			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				// The user must have REPAIRCLUSTER to use this builtin.
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}

				s, ok := tree.AsDString(args[0])
				if !ok {
					return nil, errors.Newf("expected string value, got %T", args[0])
				}
				msg := string(s)
				log.Infof(ctx, "crdb_internal.log(): %s", msg)
				return tree.DVoidDatum, nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: volatility.Volatile,
		},
	),
	"crdb_internal.force_log_fatal": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "msg", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				// The user must have REPAIRCLUSTER to use this builtin.
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}

				s, ok := tree.AsDString(args[0])
				if !ok {
					return nil, errors.Newf("expected string value, got %T", args[0])
				}
				msg := string(s)
				log.Fatalf(ctx, "force_log_fatal(): %s", msg)
				return nil, nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: volatility.Volatile,
		},
	),

	// If force_retry is called during the specified interval from the beginning
	// of the transaction it returns a retryable error. If not, 0 is returned
	// instead of an error.
	// The second version allows one to create an error intended for a transaction
	// different than the current statement's transaction.
	"crdb_internal.force_retry": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.Interval}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				minDuration := args[0].(*tree.DInterval).Duration
				elapsed := duration.MakeDuration(int64(evalCtx.StmtTimestamp.Sub(evalCtx.TxnTimestamp)), 0, 0)
				if elapsed.Compare(minDuration) < 0 {
					return nil, evalCtx.Txn.GenerateForcedRetryableErr(
						ctx, "forced by crdb_internal.force_retry()")
				}
				return tree.DZero, nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: volatility.Volatile,
		},
	),

	// Fetches the corresponding lease_holder for the request key.
	"crdb_internal.lease_holder": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "key", Typ: types.Bytes}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if evalCtx.Txn == nil { // can occur during backfills
					return nil, pgerror.Newf(pgcode.FeatureNotSupported,
						"cannot use crdb_internal.lease_holder in this context")
				}
				key := []byte(tree.MustBeDBytes(args[0]))
				b := evalCtx.Txn.NewBatch()
				b.AddRawRequest(&kvpb.LeaseInfoRequest{
					RequestHeader: kvpb.RequestHeader{
						Key: key,
					},
				})
				if err := evalCtx.Txn.Run(ctx, b); err != nil {
					return nil, pgerror.Wrap(err, pgcode.InvalidParameterValue, "error fetching leaseholder")
				}
				resp := b.RawResponse().Responses[0].GetInner().(*kvpb.LeaseInfoResponse)

				return tree.NewDInt(tree.DInt(resp.Lease.Replica.StoreID)), nil
			},
			Info:       "This function is used to fetch the leaseholder corresponding to a request key",
			Volatility: volatility.Volatile,
		},
	),

	// Fetches the corresponding lease_holder for the request key. If an error
	// occurs, the query still succeeds and the error is included in the output.
	"crdb_internal.lease_holder_with_errors": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "key", Typ: types.Bytes}},
			ReturnType: tree.FixedReturnType(types.Jsonb),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if evalCtx.Txn == nil { // can occur during backfills
					return nil, pgerror.Newf(pgcode.FeatureNotSupported,
						"cannot use crdb_internal.lease_holder_with_errors in this context")
				}
				key := []byte(tree.MustBeDBytes(args[0]))
				b := evalCtx.Txn.DB().NewBatch()
				b.AddRawRequest(&kvpb.LeaseInfoRequest{
					RequestHeader: kvpb.RequestHeader{
						Key: key,
					},
				})
				type leaseholderAndError struct {
					Leaseholder roachpb.StoreID
					Error       string
				}
				lhae := &leaseholderAndError{}
				if err := evalCtx.Txn.DB().Run(ctx, b); err != nil {
					lhae.Error = err.Error()
				} else {
					resp := b.RawResponse().Responses[0].GetInner().(*kvpb.LeaseInfoResponse)
					lhae.Leaseholder = resp.Lease.Replica.StoreID
				}

				jsonStr, err := gojson.Marshal(lhae)
				if err != nil {
					return nil, err
				}
				jsonDatum, err := tree.ParseDJSON(string(jsonStr))
				if err != nil {
					return nil, err
				}
				return jsonDatum, nil
			},
			Info:       "This function is used to fetch the leaseholder corresponding to a request key",
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.trim_tenant_prefix": makeBuiltin(
		tree.FunctionProperties{
			Category:     builtinconstants.CategoryMultiTenancy,
			Undocumented: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "key", Typ: types.Bytes},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				key := tree.MustBeDBytes(args[0])
				remainder, _, err := keys.DecodeTenantPrefix([]byte(key))
				if errors.Is(err, roachpb.ErrInvalidTenantID) {
					return tree.NewDBytes(key), nil
				} else if err != nil {
					return nil, err
				}
				return tree.NewDBytes(tree.DBytes(remainder)), nil
			},
			Info:       "This function assumes the given bytes are a CockroachDB key and trims any tenant prefix from the key.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "keys", Typ: types.BytesArray},
			},
			ReturnType: tree.FixedReturnType(types.BytesArray),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				arr := tree.MustBeDArray(args[0])
				result := tree.NewDArray(types.Bytes)
				for _, datum := range arr.Array {
					key := tree.MustBeDBytes(datum)
					remainder, _, err := keys.DecodeTenantPrefix([]byte(key))
					if err != nil {
						return nil, err
					}
					if err := result.Append(tree.NewDBytes(tree.DBytes(remainder))); err != nil {
						return nil, err
					}

				}
				return result, nil
			},
			Info:       "This function assumes the given bytes are a CockroachDB key and trims any tenant prefix from the key.",
			Volatility: volatility.Immutable,
		},
	),
	"crdb_internal.tenant_span": makeBuiltin(
		tree.FunctionProperties{
			Category:     builtinconstants.CategoryMultiTenancy,
			Undocumented: true,
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.BytesArray),
			Fn: func(_ context.Context, evalCtx *eval.Context, _ tree.Datums) (tree.Datum, error) {
				return spanToDatum(evalCtx.Codec.TenantSpan())
			},
			Info:       "This function returns the span that contains the keys for the current tenant.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "tenant_id", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.BytesArray),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				sTenID, err := mustBeDIntInTenantRange(args[0])
				if err != nil {
					return nil, err
				}
				codec := keys.MakeSQLCodec(roachpb.MustMakeTenantID(uint64(sTenID)))
				return spanToDatum(codec.TenantSpan())
			},
			Info:       "This function returns the span that contains the keys for the given tenant.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "tenant_name", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.BytesArray),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				name := tree.MustBeDString(args[0])
				tenantName := roachpb.TenantName(name)
				tid, err := evalCtx.Tenant.LookupTenantID(ctx, tenantName)
				if err != nil {
					return nil, err
				}
				return spanToDatum(keys.MakeSQLCodec(tid).TenantSpan())
			},
			Info:       "This function returns the span that contains the keys for the given tenant.",
			Volatility: volatility.Immutable,
		},
	),
	"crdb_internal.table_span": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "table_id", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.BytesArray),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				tabID := uint32(tree.MustBeDInt(args[0]))

				start := evalCtx.Codec.TablePrefix(tabID)
				return spanToDatum(roachpb.Span{
					Key:    start,
					EndKey: start.PrefixEnd(),
				})
			},
			Info:       "This function returns the span that contains the keys for the given table.",
			Volatility: volatility.Leakproof,
		},
	),
	"crdb_internal.index_span": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "table_id", Typ: types.Int},
				{Name: "index_id", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.BytesArray),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				tabID := uint32(tree.MustBeDInt(args[0]))
				indexID := uint32(tree.MustBeDInt(args[1]))

				start := roachpb.Key(rowenc.MakeIndexKeyPrefix(evalCtx.Codec,
					catid.DescID(tabID),
					catid.IndexID(indexID)))
				return spanToDatum(roachpb.Span{
					Key:    start,
					EndKey: start.PrefixEnd(),
				})
			},
			Info:       "This function returns the span that contains the keys for the given index.",
			Volatility: volatility.Leakproof,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "tenant_id", Typ: types.Int},
				{Name: "table_id", Typ: types.Int},
				{Name: "index_id", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.BytesArray),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				tenID := uint64(tree.MustBeDInt(args[0]))
				tabID := uint32(tree.MustBeDInt(args[1]))
				indexID := uint32(tree.MustBeDInt(args[2]))
				tenant, err := roachpb.MakeTenantID(tenID)
				if err != nil {
					return nil, err
				}

				start := roachpb.Key(rowenc.MakeIndexKeyPrefix(keys.MakeSQLCodec(tenant),
					catid.DescID(tabID),
					catid.IndexID(indexID)))
				return spanToDatum(roachpb.Span{
					Key:    start,
					EndKey: start.PrefixEnd(),
				})
			},
			Info:       "This function returns the span that contains the keys for the given index.",
			Volatility: volatility.Leakproof,
		},
	),
	// Return a pretty key for a given raw key, skipping the specified number of
	// fields.
	"crdb_internal.pretty_key": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "raw_key", Typ: types.Bytes},
				{Name: "skip_fields", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(catalogkeys.PrettyKey(
					nil, /* valDirs */
					roachpb.Key(tree.MustBeDBytes(args[0])),
					int(tree.MustBeDInt(args[1])))), nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "raw_key", Typ: types.Bytes},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.NewDString(catalogkeys.PrettyKey(
					nil, /* valDirs */
					roachpb.Key(tree.MustBeDBytes(args[0])),
					-1)), nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: volatility.Immutable,
		},
	),
	// Return if a key belongs to a system table, which should make it to print
	// within redacted output.
	"crdb_internal.is_system_table_key": makeBuiltin(
		tree.FunctionProperties{
			Category:     builtinconstants.CategorySystemInfo,
			Undocumented: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "raw_key", Typ: types.Bytes},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				_, tableID, err := evalCtx.Codec.DecodeTablePrefix(roachpb.Key(tree.MustBeDBytes(args[0])))
				if err != nil {
					// If a key isn't prefixed with a table ID ignore.
					//nolint:returnerrcheck
					return tree.DBoolFalse, nil
				}
				isSystemTable, err := evalCtx.PrivilegedAccessor.IsSystemTable(ctx, int64(tableID))
				if err != nil {
					// If we can't find the descriptor or its not the right type then its
					// not a system table.
					//nolint:returnerrcheck
					return tree.DBoolFalse, nil
				}
				return tree.MakeDBool(tree.DBool(isSystemTable)), nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: volatility.Stable,
		},
	),

	// Return a pretty string for a given span, skipping the specified number of
	// fields.
	"crdb_internal.pretty_span": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "raw_key_start", Typ: types.Bytes},
				{Name: "raw_key_end", Typ: types.Bytes},
				{Name: "skip_fields", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				span := roachpb.Span{
					Key:    roachpb.Key(tree.MustBeDBytes(args[0])),
					EndKey: roachpb.Key(tree.MustBeDBytes(args[1])),
				}
				skip := int(tree.MustBeDInt(args[2]))
				return tree.NewDString(catalogkeys.PrettySpan(nil /* valDirs */, span, skip)), nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: volatility.Immutable,
		},
	),

	"crdb_internal.pretty_value": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				tree.ParamType{Name: "raw_value", Typ: types.Bytes},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (d tree.Datum, err error) {
				var v roachpb.Value
				v.RawBytes = []byte(tree.MustBeDBytes(args[0]))

				return tree.NewDString(v.PrettyPrint()), nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: volatility.Immutable,
		},
	),

	// Return statistics about a range.
	"crdb_internal.range_stats": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "key", Typ: types.Bytes},
			},
			SpecializedVecBuiltin: tree.CrdbInternalRangeStats,
			ReturnType:            tree.FixedReturnType(types.Jsonb),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				resps, err := evalCtx.RangeStatsFetcher.RangeStats(ctx,
					roachpb.Key(tree.MustBeDBytes(args[0])))
				if err != nil {
					return nil, pgerror.Wrap(err, pgcode.InvalidParameterValue, "error fetching range stats")
				}
				jsonStr, err := gojson.Marshal(&resps[0].MVCCStats)
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
			Volatility: volatility.Volatile,
		},
	),

	// Return statistics about a range.
	"crdb_internal.range_stats_with_errors": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "key", Typ: types.Bytes},
			},
			SpecializedVecBuiltin: tree.CrdbInternalRangeStatsWithErrors,
			ReturnType:            tree.FixedReturnType(types.Jsonb),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				// This function is a placeholder and will never be called because
				// CrdbInternalRangeStatsWithErrors overrides it.
				return tree.DNull, nil
			},
			Info:       "This function is used to retrieve range statistics information as a JSON object.",
			Volatility: volatility.Volatile,
		},
	),

	// Returns a namespace_id based on parentID and a given name.
	// Allows a non-admin to query the system.namespace table, but performs
	// the relevant permission checks to ensure secure access.
	// Returns NULL if none is found.
	// Errors if there is no permission for the current user to view the descriptor.
	"crdb_internal.get_namespace_id": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "parent_id", Typ: types.Int}, {Name: "name", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				parentID := tree.MustBeDInt(args[0])
				name := tree.MustBeDString(args[1])
				id, found, err := evalCtx.PrivilegedAccessor.LookupNamespaceID(
					ctx,
					int64(parentID),
					0,
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
			Volatility: volatility.Stable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "parent_id", Typ: types.Int},
				{Name: "parent_schema_id", Typ: types.Int},
				{Name: "name", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				parentID := tree.MustBeDInt(args[0])
				parentSchemaID := tree.MustBeDInt(args[1])
				name := tree.MustBeDString(args[2])
				id, found, err := evalCtx.PrivilegedAccessor.LookupNamespaceID(
					ctx,
					int64(parentID),
					int64(parentSchemaID),
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
			Volatility: volatility.Stable,
		},
	),

	// Returns the descriptor of a database based on its name.
	// Allows a non-admin to query the system.namespace table, but performs
	// the relevant permission checks to ensure secure access.
	// Returns NULL if none is found.
	// Errors if there is no permission for the current user to view the descriptor.
	"crdb_internal.get_database_id": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "name", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				name := tree.MustBeDString(args[0])
				id, found, err := evalCtx.PrivilegedAccessor.LookupNamespaceID(
					ctx,
					int64(0),
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
			Volatility: volatility.Stable,
		},
	),

	// Returns the zone config based on a given namespace id.
	// Returns NULL if a zone configuration is not found.
	// Errors if there is no permission for the current user to view the zone config.
	"crdb_internal.get_zone_config": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "namespace_id", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				id := tree.MustBeDInt(args[0])
				bytes, found, err := evalCtx.PrivilegedAccessor.LookupZoneConfigByNamespaceID(
					ctx,
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
			Volatility: volatility.Stable,
		},
	),

	"crdb_internal.set_vmodule": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "vmodule_string", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				// The user must have REPAIRCLUSTER to use this builtin.
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}

				s, ok := tree.AsDString(args[0])
				if !ok {
					return nil, errors.Newf("expected string value, got %T", args[0])
				}
				vmodule := string(s)
				return tree.DZero, log.SetVModule(vmodule)
			},
			Info: "Set the equivalent of the `--vmodule` flag on the gateway node processing this request; " +
				"it affords control over the logging verbosity of different files. " +
				"Example syntax: `crdb_internal.set_vmodule('recordio=2,file=1,gfs*=3')`. " +
				"Reset with: `crdb_internal.set_vmodule('')`. " +
				"Raising the verbosity can severely affect performance.",
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.get_vmodule": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, _ tree.Datums) (tree.Datum, error) {
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}
				return tree.NewDString(log.GetVModule()), nil
			},
			Info:       "Returns the vmodule configuration on the gateway node processing this request.",
			Volatility: volatility.Volatile,
		},
	),

	// Returns the number of distinct inverted index entries that would be
	// generated for a value.
	"crdb_internal.num_geo_inverted_index_entries": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "table_id", Typ: types.Int},
				{Name: "index_id", Typ: types.Int},
				{Name: "val", Typ: types.Geography},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull || args[1] == tree.DNull || args[2] == tree.DNull {
					return tree.DZero, nil
				}
				tableID := catid.DescID(tree.MustBeDInt(args[0]))
				indexID := catid.IndexID(tree.MustBeDInt(args[1]))
				g := tree.MustBeDGeography(args[2])
				n, err := evalCtx.CatalogBuiltins.NumGeographyInvertedIndexEntries(ctx, tableID, indexID, g)
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(n)), nil
			},
			Info:              "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "table_id", Typ: types.Int},
				{Name: "index_id", Typ: types.Int},
				{Name: "val", Typ: types.Geometry},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull || args[1] == tree.DNull || args[2] == tree.DNull {
					return tree.DZero, nil
				}
				tableID := catid.DescID(tree.MustBeDInt(args[0]))
				indexID := catid.IndexID(tree.MustBeDInt(args[1]))
				g := tree.MustBeDGeometry(args[2])
				n, err := evalCtx.CatalogBuiltins.NumGeometryInvertedIndexEntries(ctx, tableID, indexID, g)
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(n)), nil
			},
			Info:              "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
		}),
	// Returns the number of distinct inverted index entries that would be
	// generated for a value.
	"crdb_internal.num_inverted_index_entries": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.Jsonb}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return jsonNumInvertedIndexEntries(evalCtx, args[0])
			},
			Info:              "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.AnyArray}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return arrayNumInvertedIndexEntries(evalCtx, args[0], tree.DNull)
			},
			Info:              "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "val", Typ: types.Jsonb},
				{Name: "version", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				// The version argument is currently ignored for JSON inverted indexes,
				// since all prior versions of JSON inverted indexes include the same
				// entries. (The version argument was introduced for array indexes,
				// since prior versions of array indexes did not include keys for empty
				// arrays.)
				return jsonNumInvertedIndexEntries(evalCtx, args[0])
			},
			Info:              "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "val", Typ: types.String},
				{Name: "version", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				// TODO(jordan): if we support inverted indexes on more than just trigram
				// indexes, we will need to thread the inverted index kind through
				// the backfiller into this function.
				// The version argument is currently ignored for string inverted indexes.
				if args[0] == tree.DNull {
					return tree.DZero, nil
				}
				s := string(tree.MustBeDString(args[0]))
				return tree.NewDInt(tree.DInt(len(trigram.MakeTrigrams(s, true /* pad */)))), nil
			},
			Info:              "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "val", Typ: types.AnyArray},
				{Name: "version", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return arrayNumInvertedIndexEntries(evalCtx, args[0], args[1])
			},
			Info:              "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "val", Typ: types.TSVector},
				{Name: "version", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.DZero, nil
				}
				val := args[0].(*tree.DTSVector)
				return tree.NewDInt(tree.DInt(len(val.TSVector))), nil
			},
			Info:              "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility:        volatility.Stable,
			CalledOnNullInput: true,
		},
	),

	"crdb_internal.assignment_cast": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "val", Typ: types.AnyElement},
				{Name: "type", Typ: types.AnyElement},
			},
			ReturnType: tree.IdentityReturnType(1),
			FnWithExprs: eval.FnWithExprsOverload(func(
				ctx context.Context, evalCtx *eval.Context, args tree.Exprs,
			) (tree.Datum, error) {
				targetType := args[1].(tree.TypedExpr).ResolvedType()
				val, err := eval.Expr(ctx, evalCtx, args[0].(tree.TypedExpr))
				if err != nil {
					return nil, err
				}
				return eval.PerformAssignmentCast(ctx, evalCtx, val, targetType)
			}),
			Info: "This function is used internally to perform assignment casts during mutations.",
			// The volatility of an assignment cast depends on the argument
			// types, so we set it to the maximum volatility of all casts.
			Volatility: volatility.Stable,
			// The idiomatic usage of this function is to "pass" a target type T
			// by passing NULL::T, so we must allow NULL arguments.
			CalledOnNullInput: true,
		},
	),

	"crdb_internal.round_decimal_values": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "val", Typ: types.Decimal},
				{Name: "scale", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Decimal),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				value := args[0].(*tree.DDecimal)
				scale := int32(tree.MustBeDInt(args[1]))
				return roundDDecimal(value, scale)
			},
			Info:       "This function is used internally to round decimal values during mutations.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "val", Typ: types.DecimalArray},
				{Name: "scale", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.DecimalArray),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Stable,
		},
	),
	"crdb_internal.unsafe_upsert_descriptor": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategorySystemRepair,
			DistsqlBlocklist: true,
			Undocumented:     true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "id", Typ: types.Int},
				{Name: "desc", Typ: types.Bytes},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.Planner.UnsafeUpsertDescriptor(ctx,
					int64(*args[0].(*tree.DInt)),
					[]byte(*args[1].(*tree.DBytes)),
					false /* force */); err != nil {
					return nil, err
				}
				return tree.DBoolTrue, nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: volatility.Volatile,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "id", Typ: types.Int},
				{Name: "desc", Typ: types.Bytes},
				{Name: "force", Typ: types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.Planner.UnsafeUpsertDescriptor(ctx,
					int64(*args[0].(*tree.DInt)),
					[]byte(*args[1].(*tree.DBytes)),
					bool(*args[2].(*tree.DBool))); err != nil {
					return nil, err
				}
				return tree.DBoolTrue, nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: volatility.Volatile,
		},
	),
	"crdb_internal.unsafe_delete_descriptor": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategorySystemRepair,
			DistsqlBlocklist: true,
			Undocumented:     true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "id", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.Planner.UnsafeDeleteDescriptor(ctx,
					int64(*args[0].(*tree.DInt)),
					false, /* force */
				); err != nil {
					return nil, err
				}
				return tree.DBoolTrue, nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: volatility.Volatile,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "id", Typ: types.Int},
				{Name: "force", Typ: types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.Planner.UnsafeDeleteDescriptor(ctx,
					int64(*args[0].(*tree.DInt)),
					bool(*args[1].(*tree.DBool)),
				); err != nil {
					return nil, err
				}
				return tree.DBoolTrue, nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: volatility.Volatile,
		},
	),
	"crdb_internal.unsafe_upsert_namespace_entry": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategorySystemRepair,
			DistsqlBlocklist: true,
			Undocumented:     true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "parent_id", Typ: types.Int},
				{Name: "parent_schema_id", Typ: types.Int},
				{Name: "name", Typ: types.String},
				{Name: "desc_id", Typ: types.Int},
				{Name: "force", Typ: types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.Planner.UnsafeUpsertNamespaceEntry(
					ctx,
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
			Volatility: volatility.Volatile,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "parent_id", Typ: types.Int},
				{Name: "parent_schema_id", Typ: types.Int},
				{Name: "name", Typ: types.String},
				{Name: "desc_id", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.Planner.UnsafeUpsertNamespaceEntry(
					ctx,
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
			Volatility: volatility.Volatile,
		},
	),
	"crdb_internal.unsafe_delete_namespace_entry": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategorySystemRepair,
			DistsqlBlocklist: true,
			Undocumented:     true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "parent_id", Typ: types.Int},
				{Name: "parent_schema_id", Typ: types.Int},
				{Name: "name", Typ: types.String},
				{Name: "desc_id", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.Planner.UnsafeDeleteNamespaceEntry(
					ctx,
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
			Volatility: volatility.Volatile,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "parent_id", Typ: types.Int},
				{Name: "parent_schema_id", Typ: types.Int},
				{Name: "name", Typ: types.String},
				{Name: "desc_id", Typ: types.Int},
				{Name: "force", Typ: types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.Planner.UnsafeDeleteNamespaceEntry(
					ctx,
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
			Volatility: volatility.Volatile,
		},
	),
	"crdb_internal.unsafe_lock_replica": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategoryTesting,
			DistsqlBlocklist: true,
			Undocumented:     true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "range_id", Typ: types.Int},
				{Name: "lock", Typ: types.Bool}, // true to lock, false to unlock
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if !enableUnsafeTestBuiltins {
					return nil, errors.Errorf("requires COCKROACH_ENABLE_UNSAFE_TEST_BUILTINS=true")
				} else if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}

				rangeID := roachpb.RangeID(*args[0].(*tree.DInt))
				lock := *args[1].(*tree.DBool)

				var replicaMu *syncutil.RWMutex
				if err := evalCtx.KVStoresIterator.ForEachStore(func(store kvserverbase.Store) error {
					if replicaMu == nil {
						replicaMu = store.GetReplicaMutexForTesting(rangeID)
					}
					return nil
				}); err != nil {
					return nil, err
				} else if replicaMu == nil {
					return tree.DBoolFalse, nil // return false for easier race handling in tests
				}

				log.Warningf(ctx, "crdb_internal.unsafe_lock_replica on r%d with lock=%t", rangeID, lock)

				if lock {
					replicaMu.Lock() // deadlocks if called twice
				} else {
					// Unlocking a non-locked mutex will irrecoverably fatal the process.
					// We do TryLock() as a best-effort guard against this, but it will be
					// racey. The caller is expected to have locked the mutex first.
					replicaMu.TryLock()
					replicaMu.Unlock()
				}
				return tree.DBoolTrue, nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: volatility.Volatile,
		},
	),
	"crdb_internal.upsert_dropped_relation_gc_ttl": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategorySystemRepair,
			DistsqlBlocklist: true,
			Undocumented:     true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "desc_id", Typ: types.Int},
				{Name: "gc_ttl", Typ: types.Interval},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.Planner.UpsertDroppedRelationGCTTL(
					ctx,
					int64(*args[0].(*tree.DInt)),          // desc_id
					(*args[1].(*tree.DInterval)).Duration, // gc_ttl
				); err != nil {
					return nil, err
				}
				return tree.DBoolTrue, nil
			},
			Info: "Administrators can use this to effectively perform " +
				"ALTER TABLE ... CONFIGURE ZONE USING gc.ttlseconds = ...; on dropped tables",
			Volatility: volatility.Volatile,
		},
	),

	// Generate some objects.
	"crdb_internal.generate_test_objects": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategorySystemInfo,
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "names", Typ: types.String},
				{Name: "number", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Jsonb),
			Body: `SELECT crdb_internal.generate_test_objects(
json_build_object('names', $1, 'counts', array[$2]))`,
			Info: `Generates a number of objects whose name follow the provided pattern.

generate_test_objects(pat, num) is an alias for
generate_test_objects('{"names":pat, "counts":[num]}'::jsonb)
`,
			Volatility: volatility.Volatile,
			Language:   tree.RoutineLangSQL,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "names", Typ: types.String},
				{Name: "counts", Typ: types.IntArray},
			},
			ReturnType: tree.FixedReturnType(types.Jsonb),
			Body: `SELECT crdb_internal.generate_test_objects(
json_build_object('names', $1, 'counts', $2))`,
			Info: `Generates a number of objects whose name follow the provided pattern.

generate_test_objects(pat, counts) is an alias for
generate_test_objects('{"names":pat, "counts":counts}'::jsonb)
`,
			Volatility: volatility.Volatile,
			Language:   tree.RoutineLangSQL,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "parameters", Typ: types.Jsonb},
			},
			ReturnType: tree.FixedReturnType(types.Jsonb),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				j, err := evalCtx.Planner.GenerateTestObjects(ctx,
					args[0].(*tree.DJSON).JSON.String(),
				)
				if err != nil {
					return nil, err
				}
				return tree.ParseDJSON(j)
			},
			Info: `Generates a number of objects whose name follow the provided pattern.

Parameters:` + randgencfg.ConfigDoc,
			Volatility: volatility.Volatile,
		},
	),

	// Returns true iff the given sqlliveness session is not expired.
	"crdb_internal.sql_liveness_is_alive": makeBuiltin(
		tree.FunctionProperties{
			Category:     builtinconstants.CategorySystemInfo,
			Undocumented: true,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "session_id", Typ: types.Bytes}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				sid := sqlliveness.SessionID(*(args[0].(*tree.DBytes)))
				live, err := evalCtx.SQLLivenessReader.IsAlive(ctx, sid)
				if err != nil {
					return tree.MakeDBool(true), err
				}
				return tree.MakeDBool(tree.DBool(live)), nil
			},
			Info:       "Checks if given sqlliveness session id is not expired",
			Volatility: volatility.Stable,
		},
	),

	// Used to configure the tenant token bucket. See UpdateTenantResourceLimits.
	"crdb_internal.update_tenant_resource_limits": makeBuiltin(
		tree.FunctionProperties{
			Category:     builtinconstants.CategoryMultiTenancy,
			Undocumented: true,
		},
		tree.Overload{
			// NOTE: as_of and as_of_consumed_tokens are not used and can be
			// deprecated.
			Types: tree.ParamTypes{
				{Name: "tenant_id", Typ: types.Int},
				{Name: "available_tokens", Typ: types.Float},
				{Name: "refill_rate", Typ: types.Float},
				{Name: "max_burst_tokens", Typ: types.Float},
				{Name: "as_of", Typ: types.Timestamp},
				{Name: "as_of_consumed_tokens", Typ: types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				sTenID, err := mustBeDIntInTenantRange(args[0])
				if err != nil {
					return nil, err
				}
				availableTokens := float64(tree.MustBeDFloat(args[1]))
				refillRate := float64(tree.MustBeDFloat(args[2]))
				maxBurstTokens := float64(tree.MustBeDFloat(args[3]))

				if err := evalCtx.Tenant.UpdateTenantResourceLimits(
					ctx,
					uint64(sTenID),
					availableTokens,
					refillRate,
					maxBurstTokens,
				); err != nil {
					return nil, err
				}
				return args[0], nil
			},
			Info:       "Updates resource limits for the tenant with the provided ID. Must be run by the System tenant.",
			Volatility: volatility.Volatile,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "tenant_name", Typ: types.String},
				{Name: "available_tokens", Typ: types.Float},
				{Name: "refill_rate", Typ: types.Float},
				{Name: "max_burst_tokens", Typ: types.Float},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				tenantName := roachpb.TenantName(tree.MustBeDString(args[0]))
				tenantID, err := evalCtx.Tenant.LookupTenantID(ctx, tenantName)
				if err != nil {
					return nil, err
				}

				availableTokens := float64(tree.MustBeDFloat(args[1]))
				refillRate := float64(tree.MustBeDFloat(args[2]))
				maxBurstTokens := float64(tree.MustBeDFloat(args[3]))

				if err := evalCtx.Tenant.UpdateTenantResourceLimits(
					ctx,
					tenantID.ToUint64(),
					availableTokens,
					refillRate,
					maxBurstTokens,
				); err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt((tenantID.ToUint64()))), nil
			},
			Info:       "Updates resource limits for the tenant with the provided name. Must be run by the System tenant.",
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.compact_engine_span": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategorySystemRepair,
			DistsqlBlocklist: true,
			Undocumented:     true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "node_id", Typ: types.Int},
				{Name: "store_id", Typ: types.Int},
				{Name: "start_key", Typ: types.Bytes},
				{Name: "end_key", Typ: types.Bytes},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}
				nodeID := int32(tree.MustBeDInt(args[0]))
				storeID := int32(tree.MustBeDInt(args[1]))
				startKey := []byte(tree.MustBeDBytes(args[2]))
				endKey := []byte(tree.MustBeDBytes(args[3]))

				if ek, ok := storage.DecodeEngineKey(startKey); !ok || ek.Validate() != nil {
					startKey = storage.EncodeMVCCKey(storage.MVCCKey{Key: startKey})
				}
				if ek, ok := storage.DecodeEngineKey(endKey); !ok || ek.Validate() != nil {
					endKey = storage.EncodeMVCCKey(storage.MVCCKey{Key: endKey})
				}
				log.Infof(ctx, "crdb_internal.compact_engine_span called for nodeID=%d, storeID=%d, range[startKey=%s, endKey=%s]", nodeID, storeID, startKey, endKey)
				if err := evalCtx.CompactEngineSpan(
					ctx, nodeID, storeID, startKey, endKey); err != nil {
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
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.increment_feature_counter": makeBuiltin(
		tree.FunctionProperties{
			Category:     builtinconstants.CategorySystemInfo,
			Undocumented: true,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "feature", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				s, ok := tree.AsDString(args[0])
				if !ok {
					return nil, errors.Newf("expected string value, got %T", args[0])
				}
				feature := string(s)
				telemetry.Inc(sqltelemetry.HashedFeatureCounter(feature))
				return tree.DBoolTrue, nil
			},
			Info: "This function can be used to report the usage of an arbitrary feature. The " +
				"feature name is hashed for privacy purposes.",
			Volatility: volatility.Volatile,
		},
	),

	"num_nulls": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategoryComparison,
		},
		tree.Overload{
			Types: tree.VariadicType{
				VarType: types.AnyElement,
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				var numNulls int
				for _, arg := range args {
					if arg == tree.DNull {
						numNulls++
					}
				}
				return tree.NewDInt(tree.DInt(numNulls)), nil
			},
			Info:              "Returns the number of null arguments.",
			Volatility:        volatility.Immutable,
			CalledOnNullInput: true,
		},
	),
	"num_nonnulls": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategoryComparison,
		},
		tree.Overload{
			Types: tree.VariadicType{
				VarType: types.AnyElement,
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				var numNonNulls int
				for _, arg := range args {
					if arg != tree.DNull {
						numNonNulls++
					}
				}
				return tree.NewDInt(tree.DInt(numNonNulls)), nil
			},
			Info:              "Returns the number of nonnull arguments.",
			Volatility:        volatility.Immutable,
			CalledOnNullInput: true,
		},
	),

	builtinconstants.GatewayRegionBuiltinName: makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategoryMultiRegion,
			// We should always evaluate this built-in at the gateway.
			DistsqlBlocklist: true,
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, arg tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Stable,
		},
	),
	builtinconstants.DefaultToDatabasePrimaryRegionBuiltinName: makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategoryMultiRegion,
			DistsqlBlocklist: true, // applicable only on the gateway
		},
		stringOverload1(
			func(ctx context.Context, evalCtx *eval.Context, s string) (tree.Datum, error) {
				regionConfig, err := evalCtx.Regions.CurrentDatabaseRegionConfig(ctx)
				if err != nil {
					return nil, err
				}
				if regionConfig == nil {
					return nil, pgerror.Newf(
						pgcode.InvalidDatabaseDefinition,
						"current database %s is not multi-region enabled",
						evalCtx.SessionData().Database,
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
			volatility.Stable,
		),
	),
	builtinconstants.RehomeRowBuiltinName: makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategoryMultiRegion,
			DistsqlBlocklist: true, // applicable only on the gateway
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, arg tree.Datums) (tree.Datum, error) {
				regionConfig, err := evalCtx.Regions.CurrentDatabaseRegionConfig(ctx)
				if err != nil {
					return nil, err
				}
				if regionConfig == nil {
					return nil, pgerror.Newf(
						pgcode.InvalidDatabaseDefinition,
						"current database %s is not multi-region enabled",
						evalCtx.SessionData().Database,
					)
				}
				gatewayRegion, found := evalCtx.Locality.Find("region")
				if !found {
					return nil, pgerror.Newf(
						pgcode.ConfigFile,
						"no region set on the locality flag on this node",
					)
				}
				if regionConfig.IsValidRegionNameString(gatewayRegion) {
					return tree.NewDString(gatewayRegion), nil
				}
				primaryRegion := regionConfig.PrimaryRegionString()
				return tree.NewDString(primaryRegion), nil
			},
			Info: `Returns the region of the connection's current node as defined by
the locality flag on node startup. Returns an error if no region is set.`,
			Volatility:       volatility.Stable,
			DistsqlBlocklist: true,
		},
	),
	"crdb_internal.validate_multi_region_zone_configs": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategoryMultiRegion,
			DistsqlBlocklist: true, // applicable only on the gateway
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.Regions.ValidateAllMultiRegionZoneConfigsInCurrentDatabase(
					ctx,
				); err != nil {
					return nil, err
				}
				return tree.MakeDBool(true), nil
			},
			Info: `Validates all multi-region zone configurations are correctly setup
			for the current database, including all tables, indexes and partitions underneath.
			Returns an error if validation fails. This builtin uses un-leased versions of the
			each descriptor, requiring extra round trips.`,
			Volatility: volatility.Volatile,
		},
	),
	"crdb_internal.reset_multi_region_zone_configs_for_table": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategoryMultiRegion,
			DistsqlBlocklist: true, // applicable only on the gateway
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "id", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				id := int64(*args[0].(*tree.DInt))

				if err := evalCtx.Regions.ResetMultiRegionZoneConfigsForTable(
					ctx,
					id,
					false,
				); err != nil {
					return nil, err
				}
				return tree.MakeDBool(true), nil
			},
			Info: `Resets the zone configuration for a multi-region table to
match its original state. No-ops if the given table ID is not a multi-region
table.`,
			Volatility: volatility.Volatile,
		},
	),
	"crdb_internal.reset_multi_region_zone_configs_for_database": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategoryMultiRegion,
			DistsqlBlocklist: true, // applicable only on the gateway
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "id", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				id := int64(*args[0].(*tree.DInt))

				if err := evalCtx.Regions.ResetMultiRegionZoneConfigsForDatabase(
					ctx,
					id,
				); err != nil {
					return nil, err
				}
				return tree.MakeDBool(true), nil
			},
			Info: `Resets the zone configuration for a multi-region database to
match its original state. No-ops if the given database ID is not multi-region
enabled.`,
			Volatility: volatility.Volatile,
		},
	),
	"crdb_internal.filter_multiregion_fields_from_zone_config_sql": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategoryMultiRegion,
			DistsqlBlocklist: true, // applicable only on the gateway
		},
		stringOverload1(
			func(_ context.Context, _ *eval.Context, s string) (tree.Datum, error) {
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
			volatility.Stable,
		),
	),
	"crdb_internal.reset_index_usage_stats": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategorySystemInfo,
			DistsqlBlocklist: true, // applicable only on the gateway
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}
				if evalCtx.IndexUsageStatsController == nil {
					return nil, errors.AssertionFailedf("index usage stats controller not set")
				}
				if err := evalCtx.IndexUsageStatsController.ResetIndexUsageStats(ctx); err != nil {
					return nil, err
				}
				return tree.MakeDBool(true), nil
			},
			Info:       `This function is used to clear the collected index usage statistics.`,
			Volatility: volatility.Volatile,
		},
	),
	"crdb_internal.reset_sql_stats": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategorySystemInfo,
			DistsqlBlocklist: true, // applicable only on the gateway
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}
				if evalCtx.SQLStatsController == nil {
					return nil, errors.AssertionFailedf("sql stats controller not set")
				}
				if err := evalCtx.SQLStatsController.ResetClusterSQLStats(ctx); err != nil {
					return nil, err
				}
				return tree.MakeDBool(true), nil
			},
			Info:       `This function is used to clear the collected SQL statistics.`,
			Volatility: volatility.Volatile,
		},
	),
	"crdb_internal.reset_activity_tables": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategorySystemInfo,
			DistsqlBlocklist: true, // applicable only on the gateway
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}
				if evalCtx.SQLStatsController == nil {
					return nil, errors.AssertionFailedf("sql stats controller not set")
				}
				if err := evalCtx.SQLStatsController.ResetActivityTables(ctx); err != nil {
					return nil, err
				}
				return tree.MakeDBool(true), nil
			},
			Info:       `This function is used to clear the statement and transaction activity system tables.`,
			Volatility: volatility.Volatile,
		},
	),
	"crdb_internal.reset_insights_tables": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategorySystemInfo,
			DistsqlBlocklist: true, // applicable only on the gateway
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}
				if evalCtx.SQLStatsController == nil {
					return nil, errors.AssertionFailedf("sql stats controller not set")
				}
				if err := evalCtx.SQLStatsController.ResetInsightsTables(ctx); err != nil {
					return nil, err
				}
				return tree.MakeDBool(true), nil
			},
			Info:       `This function is used to clear the statement and transaction insights statistics.`,
			Volatility: volatility.Volatile,
		},
	),
	// Deletes the underlying spans backing a table, only
	// if the user provides explicit acknowledgement of the
	// form "I acknowledge this will irrevocably delete all revisions
	// for table %d"
	"crdb_internal.force_delete_table_data": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemRepair,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "id", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				id := int64(*args[0].(*tree.DInt))

				err := evalCtx.Planner.ForceDeleteTableData(ctx, id)
				if err != nil {
					return tree.DBoolFalse, err
				}
				return tree.DBoolTrue, err
			},
			Info:       "This function can be used to clear the data belonging to a table, when the table cannot be dropped.",
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.serialize_session": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return evalCtx.Planner.SerializeSessionState()
			},
			Info:       `This function serializes the variables in the current session.`,
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.deserialize_session": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "session", Typ: types.Bytes}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				state := tree.MustBeDBytes(args[0])
				return evalCtx.Planner.DeserializeSessionState(ctx, tree.NewDBytes(state))
			},
			Info:       `This function deserializes the serialized variables into the current session.`,
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.create_session_revival_token": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return evalCtx.Planner.CreateSessionRevivalToken()
			},
			Info:       `Generate a token that can be used to create a new session for the current user.`,
			Volatility: volatility.Volatile,
		},
	),
	"crdb_internal.validate_session_revival_token": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "token", Typ: types.Bytes}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				token := tree.MustBeDBytes(args[0])
				return evalCtx.Planner.ValidateSessionRevivalToken(&token)
			},
			Info:       `Validate a token that was created by create_session_revival_token. Intended for testing.`,
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.validate_ttl_scheduled_jobs": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Void),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}
				return tree.DVoidDatum, evalCtx.Planner.ValidateTTLScheduledJobsInCurrentDB(ctx)
			},
			Info:       `Validate all TTL tables have a valid scheduled job attached.`,
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.repair_ttl_table_scheduled_job": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "oid", Typ: types.Oid}},
			ReturnType: tree.FixedReturnType(types.Void),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}
				oid := tree.MustBeDOid(args[0])
				if err := evalCtx.Planner.RepairTTLScheduledJobForTable(ctx, int64(oid.Oid)); err != nil {
					return nil, err
				}
				return tree.DVoidDatum, nil
			},
			Info:       `Repairs the scheduled job for a TTL table if it is missing.`,
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.check_password_hash_format": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "password", Typ: types.Bytes}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				arg := []byte(tree.MustBeDBytes(args[0]))
				isHashed, _, _, schemeName, _, err := password.CheckPasswordHashValidity(arg)
				if err != nil {
					return tree.DNull, pgerror.WithCandidateCode(err, pgcode.Syntax)
				}
				if !isHashed {
					return tree.DNull, pgerror.New(pgcode.Syntax, "hash format not recognized")
				}
				return tree.NewDString(schemeName), nil
			},
			Info:       "This function checks whether a string is a precomputed password hash. Returns the hash algorithm.",
			Volatility: volatility.Immutable,
		},
	),

	"crdb_internal.schedule_sql_stats_compaction": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if evalCtx.SQLStatsController == nil {
					return nil, errors.AssertionFailedf("sql stats controller not set")
				}
				if err := evalCtx.SQLStatsController.CreateSQLStatsCompactionSchedule(ctx); err != nil {

					return tree.DNull, err
				}
				return tree.DBoolTrue, nil
			},
			Info:       "This function is used to start a SQL stats compaction job.",
			Volatility: volatility.Volatile,
		},
	),

	builtinconstants.CreateSchemaTelemetryJobBuiltinName: makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if evalCtx.SchemaTelemetryController == nil {
					return nil, errors.AssertionFailedf("schema telemetry controller not set")
				}
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}
				id, err := evalCtx.SchemaTelemetryController.CreateSchemaTelemetryJob(
					ctx,
					builtinconstants.CreateSchemaTelemetryJobBuiltinName,
					int64(evalCtx.NodeID.SQLInstanceID()),
				)
				if err != nil {
					return tree.DNull, err
				}
				return tree.NewDInt(tree.DInt(id)), nil
			},
			Info:       "This function is used to create a schema telemetry job instance.",
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.revalidate_unique_constraints_in_all_tables": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Void),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.Planner.RevalidateUniqueConstraintsInCurrentDB(ctx); err != nil {
					return nil, err
				}
				return tree.DVoidDatum, nil
			},
			Info: `This function is used to revalidate all unique constraints in tables
in the current database. Returns an error if validation fails.`,
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.revalidate_unique_constraints_in_table": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "table_name", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Void),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				name := tree.MustBeDString(args[0])
				dOid, err := eval.ParseDOid(ctx, evalCtx, string(name), types.RegClass)
				if err != nil {
					return nil, err
				}
				if err := evalCtx.Planner.RevalidateUniqueConstraintsInTable(ctx, int(dOid.Oid)); err != nil {
					return nil, err
				}
				return tree.DVoidDatum, nil
			},
			Info: `This function is used to revalidate all unique constraints in the given
table. Returns an error if validation fails.`,
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.revalidate_unique_constraint": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "table_name", Typ: types.String}, {Name: "constraint_name", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Void),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				tableName := tree.MustBeDString(args[0])
				constraintName := tree.MustBeDString(args[1])
				dOid, err := eval.ParseDOid(ctx, evalCtx, string(tableName), types.RegClass)
				if err != nil {
					return nil, err
				}
				if err = evalCtx.Planner.RevalidateUniqueConstraint(
					ctx, int(dOid.Oid), string(constraintName),
				); err != nil {
					return nil, err
				}
				return tree.DVoidDatum, nil
			},
			Info: `This function is used to revalidate the given unique constraint in the given
table. Returns an error if validation fails.`,
			Volatility: volatility.Volatile,
		},
	),
	"crdb_internal.is_constraint_active": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "table_name", Typ: types.String}, {Name: "constraint_name", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				tableName := tree.MustBeDString(args[0])
				constraintName := tree.MustBeDString(args[1])
				dOid, err := eval.ParseDOid(ctx, evalCtx, string(tableName), types.RegClass)
				if err != nil {
					return nil, err
				}
				active, err := evalCtx.Planner.IsConstraintActive(
					ctx, int(dOid.Oid), string(constraintName),
				)
				if err != nil {
					return nil, err
				}
				if active {
					return tree.DBoolTrue, nil
				}
				return tree.DBoolFalse, nil
			},
			Info: `This function is used to determine if a given constraint is currently.
active for the current transaction.`,
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.kv_set_queue_active": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategorySystemRepair,
			DistsqlBlocklist: true, // applicable only on the gateway
			Undocumented:     true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "queue_name", Typ: types.String},
				{Name: "active", Typ: types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}

				queue := string(tree.MustBeDString(args[0]))
				active := bool(tree.MustBeDBool(args[1]))

				if err := evalCtx.KVStoresIterator.ForEachStore(func(store kvserverbase.Store) error {
					return store.SetQueueActive(active, queue)
				}); err != nil {
					return nil, err
				}

				return tree.DBoolTrue, nil
			},
			Info: `Used to enable/disable the named queue on all stores on the node it's run from.
One of 'mvccGC', 'merge', 'split', 'replicate', 'replicaGC', 'raftlog',
'raftsnapshot', 'consistencyChecker', and 'timeSeriesMaintenance'.`,
			Volatility: volatility.Volatile,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "queue_name", Typ: types.String},
				{Name: "active", Typ: types.Bool},
				{Name: "store_id", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}

				queue := string(tree.MustBeDString(args[0]))
				active := bool(tree.MustBeDBool(args[1]))
				storeID := roachpb.StoreID(tree.MustBeDInt(args[2]))

				var foundStore bool
				if err := evalCtx.KVStoresIterator.ForEachStore(func(store kvserverbase.Store) error {
					if storeID == store.StoreID() {
						foundStore = true
						return store.SetQueueActive(active, queue)
					}
					return nil
				}); err != nil {
					return nil, err
				}

				if !foundStore {
					return nil, errors.Errorf("store %s not found on this node", storeID)
				}
				return tree.DBoolTrue, nil
			},
			Info: `Used to enable/disable the named queue on the specified store on the node it's
run from. One of 'mvccGC', 'merge', 'split', 'replicate', 'replicaGC',
'raftlog', 'raftsnapshot', 'consistencyChecker', and 'timeSeriesMaintenance'.`,
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.kv_enqueue_replica": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategorySystemRepair,
			DistsqlBlocklist: true, // applicable only on the gateway
			Undocumented:     true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "range_id", Typ: types.Int},
				{Name: "queue_name", Typ: types.String},
				{Name: "skip_should_queue", Typ: types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}

				rangeID := roachpb.RangeID(tree.MustBeDInt(args[0]))
				queue := string(tree.MustBeDString(args[1]))
				skipShouldQueue := bool(tree.MustBeDBool(args[2]))

				var foundRepl bool
				if err := evalCtx.KVStoresIterator.ForEachStore(func(store kvserverbase.Store) error {
					err := store.Enqueue(ctx, queue, rangeID, skipShouldQueue)
					if err == nil {
						foundRepl = true
						return nil
					}

					if errors.HasType(err, (*kvpb.RangeNotFoundError)(nil)) {
						return nil
					}
					return err
				}); err != nil {
					return nil, err
				}

				if !foundRepl {
					return nil, errors.Errorf("replica with range id %s not found on this node", rangeID)
				}

				return tree.DBoolTrue, nil
			},
			Info: `Enqueue the replica with the given range ID into the named queue, on the
store housing the range on the node it's run from. One of 'mvccGC', 'merge', 'split',
'replicate', 'replicaGC', 'raftlog', 'raftsnapshot', 'consistencyChecker', and
'timeSeriesMaintenance'.`,
			Volatility: volatility.Volatile,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "range_id", Typ: types.Int},
				{Name: "queue_name", Typ: types.String},
				{Name: "skip_should_queue", Typ: types.Bool},
				{Name: "should_return_trace", Typ: types.Bool},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}

				rangeID := roachpb.RangeID(tree.MustBeDInt(args[0]))
				queue := string(tree.MustBeDString(args[1]))
				skipShouldQueue := bool(tree.MustBeDBool(args[2]))
				shouldReturnTrace := bool(tree.MustBeDBool(args[3]))

				var foundRepl bool
				var rec tracingpb.Recording
				if err := evalCtx.KVStoresIterator.ForEachStore(func(store kvserverbase.Store) error {
					var err error
					if shouldReturnTrace {
						traceCtx, trace := tracing.ContextWithRecordingSpan(ctx, evalCtx.Tracer, "trace-enqueue")
						err = store.Enqueue(traceCtx, queue, rangeID, skipShouldQueue)
						rec = trace()
					} else {
						err = store.Enqueue(ctx, queue, rangeID, skipShouldQueue)
					}

					if err == nil {
						foundRepl = true
						return nil
					}

					if errors.HasType(err, (*kvpb.RangeNotFoundError)(nil)) {
						return nil
					}
					return err
				}); err != nil {
					return nil, err
				}

				if !foundRepl {
					return nil, errors.Errorf("replica with range id %s not found on this node", rangeID)
				}

				if shouldReturnTrace {
					return tree.NewDString(rec.String()), nil
				}

				return tree.NewDString("replica enqueued"), nil
			},
			Info: `Enqueue the replica with the given range ID into the named queue, on the
store housing the range on the node it's run from. One of 'mvccGC', 'merge', 'split',
'replicate', 'replicaGC', 'raftlog', 'raftsnapshot', 'consistencyChecker', and
'timeSeriesMaintenance'. Specify if the trace corresponding to the enqueue operation should be rendered.`,
			Volatility: volatility.Volatile,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "range_id", Typ: types.Int},
				{Name: "queue_name", Typ: types.String},
				{Name: "skip_should_queue", Typ: types.Bool},
				{Name: "store_id", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}

				rangeID := roachpb.RangeID(tree.MustBeDInt(args[0]))
				queue := string(tree.MustBeDString(args[1]))
				skipShouldQueue := bool(tree.MustBeDBool(args[2]))
				storeID := roachpb.StoreID(tree.MustBeDInt(args[3]))

				var foundStore bool
				if err := evalCtx.KVStoresIterator.ForEachStore(func(store kvserverbase.Store) error {
					if storeID == store.StoreID() {
						foundStore = true
						err := store.Enqueue(ctx, queue, rangeID, skipShouldQueue)
						return err
					}
					return nil
				}); err != nil {
					return nil, err
				}

				if !foundStore {
					return nil, errors.Errorf("store %s not found on this node", storeID)
				}
				return tree.DBoolTrue, nil
			},
			Info: `Enqueue the replica with the given range ID into the named queue, on the
specified store on the node it's run from. One of 'mvccGC', 'merge', 'split',
'replicate', 'replicaGC', 'raftlog', 'raftsnapshot', 'consistencyChecker', and
'timeSeriesMaintenance'.`,
			Volatility: volatility.Volatile,
		},
	),

	"crdb_internal.request_job_execution_details": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategorySystemInfo,
			DistsqlBlocklist: true, // applicable only on the gateway
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "jobID", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				// TODO(adityamaru): Figure out the correct permissions for collecting a
				// job profiler bundle. For now only allow the VIEWCLUSTERMETADATA privilege.
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}

				jobID := int(tree.MustBeDInt(args[0]))
				if err := evalCtx.JobsProfiler.RequestExecutionDetailFiles(ctx, jobspb.JobID(jobID)); err != nil {
					return nil, err
				}

				return tree.DBoolTrue, nil
			},
			Volatility: volatility.Volatile,
			Info:       `Used to request the collection of execution details for a given job ID`,
		},
	),

	"crdb_internal.request_statement_bundle": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategorySystemInfo,
			DistsqlBlocklist: true, // applicable only on the gateway
		},
		makeRequestStatementBundleBuiltinOverload(false /* withPlanGist */, false /* withAntiPlanGist */, false /* redacted */),
		makeRequestStatementBundleBuiltinOverload(true /* withPlanGist */, false /* withAntiPlanGist */, false /* redacted */),
		makeRequestStatementBundleBuiltinOverload(true /* withPlanGist */, true /* withAntiPlanGist */, false /* redacted */),
		makeRequestStatementBundleBuiltinOverload(false /* withPlanGist */, false /* withAntiPlanGist */, true /* redacted */),
		makeRequestStatementBundleBuiltinOverload(true /* withPlanGist */, false /* withAntiPlanGist */, true /* redacted */),
		makeRequestStatementBundleBuiltinOverload(true /* withPlanGist */, true /* withAntiPlanGist */, true /* redacted */),
	),

	"crdb_internal.set_compaction_concurrency": makeBuiltin(
		tree.FunctionProperties{
			Category:         builtinconstants.CategorySystemRepair,
			DistsqlBlocklist: true,
			Undocumented:     true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "node_id", Typ: types.Int},
				{Name: "store_id", Typ: types.Int},
				{Name: "compaction_concurrency", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}
				nodeID := int32(tree.MustBeDInt(args[0]))
				storeID := int32(tree.MustBeDInt(args[1]))
				compactionConcurrency := tree.MustBeDInt(args[2])
				if compactionConcurrency < 0 {
					return nil, errors.AssertionFailedf("compaction_concurrency must be > 0")
				}
				if err := evalCtx.SetCompactionConcurrency(
					ctx, nodeID, storeID, uint64(compactionConcurrency)); err != nil {
					return nil, err
				}
				return tree.DBoolTrue, nil
			},
			Info: "This function can be used to temporarily change the compaction concurrency of a " +
				"given node and store. " +
				"To change the compaction concurrency of a store one can do: " +
				"SELECT crdb_internal.set_compaction_concurrency(<node_id>, <store_id>, <compaction_concurrency>). " +
				"The store's compaction concurrency will change until the sql command is cancelled. Once cancelled " +
				"the store's compaction concurrency will return to what it was previously. This command isn't safe " +
				"for concurrent use.",
			Volatility: volatility.Volatile,
		},
	),
	"crdb_internal.fingerprint": makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategorySystemInfo,
		},
		tree.Overload{
			// If the second arg is set to true, this overload allows the caller to
			// execute a "stripped" fingerprint on the latest keys in a span. This
			// stripped fingerprint strips each key's timestamp and index prefix
			// before hashing, enabling a user to assert that two different tables
			// have the same latest keys, for example. Because the index prefix is
			// stripped, this option should only get used in the table key space.
			//
			// If the stripped param is set to false, this overload is equivalent to
			// 'crdb_internal.fingerprint(span,NULL,LATEST)'

			Types: tree.ParamTypes{
				{Name: "span", Typ: types.BytesArray},
				{Name: "stripped", Typ: types.Bool},
				// NB: The function can be called with an AOST clause that will be used
				// as the `end_time` when issuing the ExportRequests for the purposes of
				// fingerprinting.
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.SessionAccessor.CheckPrivilege(
					ctx, syntheticprivilege.GlobalPrivilegeObject, privilege.REPAIRCLUSTER,
				); err != nil {
					return nil, err
				}

				if len(args) != 2 {
					return nil, errors.New("argument list must have two elements")
				}
				span, err := parseSpan(args[0])
				if err != nil {
					return nil, err
				}
				skipTimestamp := bool(tree.MustBeDBool(args[1]))
				fingerprint, err := evalCtx.Planner.FingerprintSpan(ctx, span,
					hlc.Timestamp{}, /* startTime */
					false,           /* allRevisions */
					skipTimestamp)
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(fingerprint)), nil
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: volatility.Stable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "span", Typ: types.BytesArray},
				{Name: "start_time", Typ: types.Decimal},
				{Name: "all_revisions", Typ: types.Bool},
				// NB: The function can be called with an AOST clause that will be used
				// as the `end_time` when issuing the ExportRequests for the purposes of
				// fingerprinting.
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return verboseFingerprint(ctx, evalCtx, args)
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: volatility.Stable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "span", Typ: types.BytesArray},
				{Name: "start_time", Typ: types.TimestampTZ},
				{Name: "all_revisions", Typ: types.Bool},
				// NB: The function can be called with an AOST clause that will be used
				// as the `end_time` when issuing the ExportRequests for the purposes of
				// fingerprinting.
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return verboseFingerprint(ctx, evalCtx, args)
			},
			Info:       "This function is used only by CockroachDB's developers for testing purposes.",
			Volatility: volatility.Stable,
		},
	),
	"crdb_internal.hide_sql_constants": makeBuiltin(tree.FunctionProperties{
		Category:     builtinconstants.CategoryString,
		Undocumented: true,
	},
		stringOverload1(
			func(_ context.Context, _ *eval.Context, sql string) (tree.Datum, error) {
				if len(sql) == 0 {
					return tree.NewDString(""), nil
				}

				parsed, err := parser.ParseOne(sql)
				if err != nil {
					// If parsing is unsuccessful, we shouldn't return an error, however
					// we can't return the original stmt since this function is used to
					// hide sensitive information.
					return tree.NewDString(""), nil //nolint:returnerrcheck
				}
				sqlNoConstants := tree.AsStringWithFlags(parsed.AST, tree.FmtHideConstants)
				return tree.NewDString(sqlNoConstants), nil
			},
			types.String,
			"Removes constants from a SQL statement. String provided must contain at most "+
				"1 statement. (Hint: one way to easily quote arbitrary SQL is to use dollar-quotes.)",
			volatility.Immutable,
		),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.StringArray}},
			ReturnType: tree.FixedReturnType(types.StringArray),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				arr := tree.MustBeDArray(args[0])
				result := tree.NewDArray(types.String)

				for _, sqlDatum := range arr.Array {
					if sqlDatum == tree.DNull {
						if err := result.Append(tree.DNull); err != nil {
							return nil, err
						}
						continue
					}

					sql := string(tree.MustBeDString(sqlDatum))
					sqlNoConstants := ""

					if len(sql) != 0 {
						parsed, err := parser.ParseOne(sql)
						// Leave result as empty string on parsing error.
						if err == nil {
							sqlNoConstants = tree.AsStringWithFlags(parsed.AST, tree.FmtHideConstants)
						}
					}

					if err := result.Append(tree.NewDString(sqlNoConstants)); err != nil {
						return nil, err
					}
				}

				return result, nil
			},
			Info: "Hide constants for each element in an array of SQL statements. " +
				"Note that maximum 1 statement is permitted per string element. (Hint: one way to easily " +
				"quote arbitrary SQL is to use dollar-quotes.)",
			Volatility: volatility.Immutable,
		},
	),
	"crdb_internal.humanize_bytes": makeBuiltin(tree.FunctionProperties{
		Category:     builtinconstants.CategoryString,
		Undocumented: true,
	},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.Int}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				b := tree.MustBeDInt(args[0])
				return tree.NewDString(string(humanizeutil.IBytes(int64(b)))), nil
			},
			Info:       "Converts integer size (in bytes) into the human-readable form.",
			Volatility: volatility.Leakproof,
		},
	),

	"crdb_internal.privilege_name": makeBuiltin(tree.FunctionProperties{
		Category:     builtinconstants.CategoryString,
		Undocumented: true,
	},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "internal_key", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				s := tree.MustBeDString(args[0])
				name, err := privilege.KeyToName(string(s))
				if err != nil {
					return nil, err
				}
				return tree.NewDString(name), nil
			},
			Info:       "Converts the internal storage key of a privilege to its display name.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "internal_key", Typ: types.StringArray}},
			ReturnType: tree.FixedReturnType(types.StringArray),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				src := tree.MustBeDArray(args[0])
				dst := *src
				dst.Array = make(tree.Datums, len(src.Array))
				for idx := range src.Array {
					if src.Array[idx] == tree.DNull {
						dst.Array[idx] = tree.DNull
						continue
					}
					storageKey := string(tree.MustBeDString(src.Array[idx]))
					name, err := privilege.KeyToName(storageKey)
					if err != nil {
						return nil, err
					}
					dst.Array[idx] = tree.NewDString(name)
				}
				return &dst, nil
			},
			Info:       "Converts the internal storage key of a privilege to its display name.",
			Volatility: volatility.Immutable,
		},
	),

	"crdb_internal.redactable_sql_constants": makeBuiltin(tree.FunctionProperties{
		Category:     builtinconstants.CategoryString,
		Undocumented: true,
	},
		stringOverload1(
			func(_ context.Context, _ *eval.Context, sql string) (tree.Datum, error) {
				parsed, err := parser.ParseOne(sql)
				if err != nil {
					// If parsing was unsuccessful, mark the entire string as redactable.
					return tree.NewDString(string(redact.Sprintf("%s", sql))), nil //nolint:returnerrcheck
				}
				fmtFlags := tree.FmtMarkRedactionNode | tree.FmtOmitNameRedaction
				sqlRedactable := tree.AsStringWithFlags(parsed.AST, fmtFlags)
				return tree.NewDString(sqlRedactable), nil
			},
			types.String,
			"Surrounds constants in SQL statement with redaction markers. String provided must "+
				"contain at most 1 statement. (Hint: one way to easily quote arbitrary SQL is to use "+
				"dollar-quotes.)",
			volatility.Immutable,
		),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.StringArray}},
			ReturnType: tree.FixedReturnType(types.StringArray),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				arr := tree.MustBeDArray(args[0])
				result := tree.NewDArray(types.String)

				for _, sqlDatum := range arr.Array {
					if sqlDatum == tree.DNull {
						if err := result.Append(tree.DNull); err != nil {
							return nil, err
						}
						continue
					}

					sql := string(tree.MustBeDString(sqlDatum))
					sqlRedactable := ""

					parsed, err := parser.ParseOne(sql)
					if err != nil {
						// If parsing was unsuccessful, mark the entire string as redactable.
						sqlRedactable = string(redact.Sprintf("%s", sql))
					} else {
						fmtFlags := tree.FmtMarkRedactionNode | tree.FmtOmitNameRedaction
						sqlRedactable = tree.AsStringWithFlags(parsed.AST, fmtFlags)
					}
					if err := result.Append(tree.NewDString(sqlRedactable)); err != nil {
						return nil, err
					}
				}

				return result, nil
			},
			Info: "Surrounds constants with redaction markers for each element in an array of SQL " +
				"statements. Note that maximum 1 statement is permitted per string element. (Hint: one " +
				"way to easily quote arbitrary SQL is to use dollar-quotes.)",
			Volatility: volatility.Immutable,
		},
	),
	"crdb_internal.redact": makeBuiltin(tree.FunctionProperties{
		Category:     builtinconstants.CategoryString,
		Undocumented: true,
	},
		stringOverload1(
			func(_ context.Context, _ *eval.Context, redactable string) (tree.Datum, error) {
				return tree.NewDString(string(redact.RedactableString(redactable).Redact())), nil
			},
			types.String,
			"Replaces all occurrences of unsafe substrings (substrings surrounded by the redaction "+
				"markers, '‹' and '›') with the redacted marker, '‹×›'.",
			volatility.Immutable,
		),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "val", Typ: types.StringArray}},
			ReturnType: tree.FixedReturnType(types.StringArray),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				arr := tree.MustBeDArray(args[0])
				result := tree.NewDArray(types.String)
				for _, redactableDatum := range arr.Array {
					if redactableDatum == tree.DNull {
						if err := result.Append(tree.DNull); err != nil {
							return nil, err
						}
						continue
					}
					redactable := string(tree.MustBeDString(redactableDatum))
					redacted := string(redact.RedactableString(redactable).Redact())
					if err := result.Append(tree.NewDString(redacted)); err != nil {
						return nil, err
					}
				}
				return result, nil
			},
			Info: "For each element of the array `val`, replaces all occurrences of unsafe substrings " +
				"(substrings surrounded by the redaction markers, '‹' and '›') with the redacted marker, " +
				"'‹×›'.",
			Volatility: volatility.Immutable,
		},
	),
	"crdb_internal.plpgsql_raise": makeBuiltin(tree.FunctionProperties{
		Category:     builtinconstants.CategoryString,
		Undocumented: true,
	},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "severity", Typ: types.String},
				{Name: "message", Typ: types.String},
				{Name: "detail", Typ: types.String},
				{Name: "hint", Typ: types.String},
				{Name: "code", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				argStrings := make([]string, len(args))
				for i := range args {
					if args[i] == tree.DNull {
						return nil, pgerror.New(
							pgcode.NullValueNotAllowed, "RAISE statement option cannot be null",
						)
					}
					s, ok := tree.AsDString(args[i])
					if !ok {
						return nil, errors.Newf("expected string value, got %T", args[i])
					}
					argStrings[i] = string(s)
				}
				// Build the error.
				severity := strings.ToUpper(argStrings[0])
				if _, ok := pgnotice.ParseDisplaySeverity(severity); !ok {
					return nil, pgerror.Newf(
						pgcode.InvalidParameterValue, "severity %s is invalid", severity,
					)
				}
				message := argStrings[1]
				err := errors.Newf("%s", message)
				err = pgerror.WithSeverity(err, severity)
				if detail := argStrings[2]; detail != "" {
					err = errors.WithDetail(err, detail)
				}
				if hint := argStrings[3]; hint != "" {
					err = errors.WithHint(err, hint)
				}
				if codeString := argStrings[4]; codeString != "" {
					var code string
					if pgcode.IsValidPGCode(codeString) {
						code = codeString
					} else {
						// The supplied string may be a condition name.
						if candidates, ok := pgcode.PLpgSQLConditionNameToCode[codeString]; ok {
							// Some condition names map to more than one code, but postgres
							// seems to just use the first (smallest) one.
							code = candidates[0]
						} else {
							return nil, pgerror.Newf(pgcode.UndefinedObject,
								"unrecognized exception condition: \"%s\"", codeString,
							)
						}
					}
					err = pgerror.WithCandidateCode(err, pgcode.MakeCode(code))
				}
				if severity == "ERROR" {
					// Directly return the error from the function call.
					return nil, err
				}
				// Send the error as a notice to the client, then return NULL.
				if sendErr := crdbInternalSendNotice(ctx, evalCtx, err); sendErr != nil {
					return nil, sendErr
				}
				return tree.DNull, nil
			},
			Info:              "This function is used internally to implement the PLpgSQL RAISE statement.",
			Volatility:        volatility.Volatile,
			CalledOnNullInput: true,
		},
	),
	"bitmask_or": makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategoryString},
		stringOverload2(
			"a",
			"b",
			func(_ context.Context, _ *eval.Context, a, b string) (tree.Datum, error) {
				aBitArray, err := bitarray.Parse(a)
				if err != nil {
					return nil, err
				}

				bBitArray, err := bitarray.Parse(b)
				if err != nil {
					return nil, err
				}

				return bitmaskOr(aBitArray.String(), bBitArray.String())
			},
			types.VarBit,
			"Calculates bitwise OR value of unsigned bit arrays 'a' and 'b' that may have different lengths.",
			volatility.Immutable),
		bitsOverload2("a", "b",
			func(_ context.Context, _ *eval.Context, a, b *tree.DBitArray) (tree.Datum, error) {
				return bitmaskOr(a.BitArray.String(), b.BitArray.String())
			},
			types.VarBit,
			"Calculates bitwise OR value of unsigned bit arrays 'a' and 'b' that may have different lengths.",
			volatility.Immutable,
		),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "a", Typ: types.VarBit},
				{Name: "b", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.VarBit),
			Fn: func(_ context.Context, _ *eval.Context, datums tree.Datums) (tree.Datum, error) {
				a, b := datums[0], datums[1]
				bBitArray, err := bitarray.Parse(string(tree.MustBeDString(b)))
				if err != nil {
					return nil, err
				}

				return bitmaskOr(a.(*tree.DBitArray).BitArray.String(), bBitArray.String())
			},
			Info:       "Calculates bitwise OR value of unsigned bit arrays 'a' and 'b' that may have different lengths.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "a", Typ: types.String},
				{Name: "b", Typ: types.VarBit},
			},
			ReturnType: tree.FixedReturnType(types.VarBit),
			Fn: func(_ context.Context, _ *eval.Context, datums tree.Datums) (tree.Datum, error) {
				a, b := datums[0], datums[1]
				aBitArray, err := bitarray.Parse(string(tree.MustBeDString(a)))
				if err != nil {
					return nil, err
				}

				return bitmaskOr(aBitArray.String(), b.(*tree.DBitArray).BitArray.String())
			},
			Info:       "Calculates bitwise OR value of unsigned bit arrays 'a' and 'b' that may have different lengths.",
			Volatility: volatility.Immutable,
		},
	),
	"bitmask_and": makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategoryString},
		stringOverload2(
			"a",
			"b",
			func(_ context.Context, _ *eval.Context, a, b string) (tree.Datum, error) {
				aBitArray, err := bitarray.Parse(a)
				if err != nil {
					return nil, err
				}

				bBitArray, err := bitarray.Parse(b)
				if err != nil {
					return nil, err
				}

				return bitmaskAnd(aBitArray.String(), bBitArray.String())
			},
			types.VarBit,
			"Calculates bitwise AND value of unsigned bit arrays 'a' and 'b' that may have different lengths.",
			volatility.Immutable),
		bitsOverload2("a", "b",
			func(_ context.Context, _ *eval.Context, a, b *tree.DBitArray) (tree.Datum, error) {
				return bitmaskAnd(a.BitArray.String(), b.BitArray.String())
			},
			types.VarBit,
			"Calculates bitwise AND value of unsigned bit arrays 'a' and 'b' that may have different lengths.",
			volatility.Immutable,
		),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "a", Typ: types.VarBit},
				{Name: "b", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.VarBit),
			Fn: func(_ context.Context, _ *eval.Context, datums tree.Datums) (tree.Datum, error) {
				a, b := datums[0], datums[1]
				bBitArray, err := bitarray.Parse(string(tree.MustBeDString(b)))
				if err != nil {
					return nil, err
				}

				return bitmaskAnd(a.(*tree.DBitArray).BitArray.String(), bBitArray.String())
			},
			Info:       "Calculates bitwise AND value of unsigned bit arrays 'a' and 'b' that may have different lengths.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "a", Typ: types.String},
				{Name: "b", Typ: types.VarBit},
			},
			ReturnType: tree.FixedReturnType(types.VarBit),
			Fn: func(_ context.Context, _ *eval.Context, datums tree.Datums) (tree.Datum, error) {
				a, b := datums[0], datums[1]
				aBitArray, err := bitarray.Parse(string(tree.MustBeDString(a)))
				if err != nil {
					return nil, err
				}

				return bitmaskAnd(aBitArray.String(), b.(*tree.DBitArray).BitArray.String())
			},
			Info:       "Calculates bitwise AND value of unsigned bit arrays 'a' and 'b' that may have different lengths.",
			Volatility: volatility.Immutable,
		},
	),
	"bitmask_xor": makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategoryString},
		stringOverload2(
			"a",
			"b",
			func(_ context.Context, _ *eval.Context, a, b string) (tree.Datum, error) {
				aBitArray, err := bitarray.Parse(a)
				if err != nil {
					return nil, err
				}

				bBitArray, err := bitarray.Parse(b)
				if err != nil {
					return nil, err
				}

				return bitmaskXor(aBitArray.String(), bBitArray.String())
			},
			types.VarBit,
			"Calculates bitwise XOR value of unsigned bit arrays 'a' and 'b' that may have different lengths.",
			volatility.Immutable),
		bitsOverload2("a", "b",
			func(_ context.Context, _ *eval.Context, a, b *tree.DBitArray) (tree.Datum, error) {
				return bitmaskXor(a.BitArray.String(), b.BitArray.String())
			},
			types.VarBit,
			"Calculates bitwise XOR value of unsigned bit arrays 'a' and 'b' that may have different lengths.",
			volatility.Immutable,
		),
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "a", Typ: types.VarBit},
				{Name: "b", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.VarBit),
			Fn: func(_ context.Context, _ *eval.Context, datums tree.Datums) (tree.Datum, error) {
				a, b := datums[0], datums[1]
				bBitArray, err := bitarray.Parse(string(tree.MustBeDString(b)))
				if err != nil {
					return nil, err
				}

				return bitmaskXor(a.(*tree.DBitArray).BitArray.String(), bBitArray.String())
			},
			Info:       "Calculates bitwise XOR value of unsigned bit arrays 'a' and 'b' that may have different lengths.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "a", Typ: types.String},
				{Name: "b", Typ: types.VarBit},
			},
			ReturnType: tree.FixedReturnType(types.VarBit),
			Fn: func(_ context.Context, _ *eval.Context, datums tree.Datums) (tree.Datum, error) {
				a, b := datums[0], datums[1]
				aBitArray, err := bitarray.Parse(string(tree.MustBeDString(a)))
				if err != nil {
					return nil, err
				}

				return bitmaskXor(aBitArray.String(), b.(*tree.DBitArray).BitArray.String())
			},
			Info:       "Calculates bitwise XOR value of unsigned bit arrays 'a' and 'b' that may have different lengths.",
			Volatility: volatility.Immutable,
		},
	),
	"crdb_internal.plpgsql_gen_cursor_name": makeBuiltin(tree.FunctionProperties{
		Category:     builtinconstants.CategoryIDGeneration,
		Undocumented: true,
	},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "name", Typ: types.RefCursor},
			},
			ReturnType: tree.FixedReturnType(types.RefCursor),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					name := evalCtx.Planner.GenUniqueCursorName()
					return tree.NewDRefCursor(string(name)), nil
				}
				return args[0], nil
			},
			Info:              "This function is used internally to generate unique names for PLpgSQL cursors.",
			Volatility:        volatility.Volatile,
			CalledOnNullInput: true,
		},
	),
	"crdb_internal.plpgsql_close": makeBuiltin(tree.FunctionProperties{
		Category:     builtinconstants.CategoryString,
		Undocumented: true,
	},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "name", Typ: types.RefCursor}},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return nil, pgerror.New(
						pgcode.NullValueNotAllowed, "cursor name for CLOSE statement cannot be null",
					)
				}
				return tree.DNull, evalCtx.Planner.PLpgSQLCloseCursor(tree.Name(tree.MustBeDString(args[0])))
			},
			Info:              "This function is used internally to implement the PLpgSQL CLOSE statement.",
			Volatility:        volatility.Volatile,
			CalledOnNullInput: true,
		},
	),
	"crdb_internal.plpgsql_fetch": makeBuiltin(tree.FunctionProperties{
		Category:     builtinconstants.CategoryString,
		Undocumented: true,
	},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "name", Typ: types.RefCursor},
				{Name: "direction", Typ: types.Int},
				{Name: "count", Typ: types.Int},
				{Name: "resultTypes", Typ: types.AnyElement},
			},
			ReturnType: tree.IdentityReturnType(3),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				for i := range args {
					if args[i] == tree.DNull {
						return nil, pgerror.New(
							pgcode.NullValueNotAllowed, "FETCH statement option cannot be null",
						)
					}
				}
				cursorName := tree.MustBeDString(args[0])
				cursorDir := tree.MustBeDInt(args[1])
				cursorCount := tree.MustBeDInt(args[2])
				resultTypes := args[3].(tree.TypedExpr).ResolvedType().TupleContents()
				if cursorDir < 0 || cursorDir > tree.DInt(tree.FetchBackwardAll) {
					return nil, pgerror.Newf(pgcode.InvalidParameterValue, "invalid fetch/move direction: %d", cursorDir)
				}
				cursor := &tree.CursorStmt{
					Name:      tree.Name(cursorName),
					FetchType: tree.FetchType(cursorDir),
					Count:     int64(cursorCount),
				}
				row, err := evalCtx.Planner.PLpgSQLFetchCursor(ctx, cursor)
				if err != nil {
					return nil, err
				}
				res := make(tree.Datums, len(resultTypes))
				for i := 0; i < len(resultTypes); i++ {
					if i < len(row) {
						res[i], err = eval.PerformCastNoTruncate(ctx, evalCtx, row[i], resultTypes[i])
						if err != nil {
							return nil, err
						}
					} else {
						res[i] = tree.DNull
					}
				}
				tup := tree.MakeDTuple(types.MakeTuple(resultTypes), res...)
				return &tup, nil
			},
			Info:              "This function is used internally to implement the PLpgSQL FETCH and MOVE statements.",
			Volatility:        volatility.Volatile,
			CalledOnNullInput: true,
		},
	),
	"crdb_internal.protect_mvcc_history": makeBuiltin(
		tree.FunctionProperties{
			Category:     builtinconstants.CategoryClusterReplication,
			Undocumented: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "timestamp", Typ: types.Decimal},
				{Name: "expiration_window", Typ: types.Interval},
				{Name: "description", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				tsDec := tree.MustBeDDecimal(args[0])
				expiration := tree.MustBeDInterval(args[1])
				desc := string(tree.MustBeDString(args[2]))

				if err := evalCtx.SessionAccessor.CheckPrivilege(ctx,
					syntheticprivilege.GlobalPrivilegeObject,
					privilege.REPLICATION); err != nil {
					return nil, err
				}

				timestamp, err := hlc.DecimalToHLC(&tsDec.Decimal)
				if err != nil {
					return nil, err
				}
				jobID, err := evalCtx.Planner.StartHistoryRetentionJob(ctx, desc, timestamp,
					time.Duration(expiration.Duration.Nanos()))
				if err != nil {
					return nil, err
				}
				return tree.NewDInt(tree.DInt(jobID)), nil
			},
			Info:       `This function is used to create a cluster-wide PTS record and related job`,
			Volatility: volatility.Volatile,
		},
	),
	"crdb_internal.extend_mvcc_history_protection": makeBuiltin(
		tree.FunctionProperties{
			Category:     builtinconstants.CategoryClusterReplication,
			Undocumented: true,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "job_id", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Void),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if err := evalCtx.SessionAccessor.CheckPrivilege(ctx,
					syntheticprivilege.GlobalPrivilegeObject,
					privilege.REPLICATION); err != nil {
					return nil, err
				}

				jobID := jobspb.JobID(tree.MustBeDInt(args[0]))
				return tree.DVoidDatum, evalCtx.Planner.ExtendHistoryRetention(ctx, jobID)
			},
			Info:       `This function is used to extend the life of a cluster-wide PTS record`,
			Volatility: volatility.Volatile,
		},
	),
	"crdb_internal.clear_query_plan_cache": makeBuiltin(
		tree.FunctionProperties{
			Category:     builtinconstants.CategorySystemRepair,
			Undocumented: true,
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Void),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				evalCtx.Planner.ClearQueryPlanCache()
				return tree.DVoidDatum, nil
			},
			Info:       `This function is used to clear the query plan cache on the gateway node`,
			Volatility: volatility.Volatile,
		},
	),
	"crdb_internal.clear_table_stats_cache": makeBuiltin(
		tree.FunctionProperties{
			Category:     builtinconstants.CategorySystemRepair,
			Undocumented: true,
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Void),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				evalCtx.Planner.ClearTableStatsCache()
				return tree.DVoidDatum, nil
			},
			Info:       `This function is used to clear the table statistics cache on the gateway node`,
			Volatility: volatility.Volatile,
		},
	),
	"crdb_internal.get_fully_qualified_table_name": makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategorySystemInfo},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "table_descriptor_id", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Body: `
SELECT fq_name
FROM crdb_internal.fully_qualified_names
WHERE object_id = table_descriptor_id
`,
			Info:       `This function is used to get the fully qualified table name given a table descriptor ID`,
			Volatility: volatility.Stable,
			Language:   tree.RoutineLangSQL,
		},
	),
	"crdb_internal.type_is_indexable": makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "oid", Typ: types.Oid}},
			ReturnType: tree.FixedReturnType(types.Bool),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				oid := tree.MustBeDOid(args[0]).Oid
				var typ *types.T
				if resolvedTyp, ok := types.OidToType[oid]; ok {
					typ = resolvedTyp
				} else {
					var err error
					if typ, err = evalCtx.Planner.ResolveTypeByOID(ctx, oid); err != nil {
						return nil, err
					}
				}
				return tree.MakeDBool(tree.DBool(colinfo.ColumnTypeIsIndexable(typ))), nil
			},
			Info:       "Returns whether the given type OID is indexable.",
			Volatility: volatility.Stable,
		},
	),
	"crdb_internal.backup_compaction": makeBuiltin(
		tree.FunctionProperties{
			Undocumented: true,
			ReturnLabels: []string{"job_id"},
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "collection_uri", Typ: types.StringArray},
				{Name: "full_backup_path", Typ: types.String},
				{Name: "encryption_opts", Typ: types.Bytes},
				{Name: "start_time", Typ: types.Decimal},
				{Name: "end_time", Typ: types.Decimal},
			},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				if StartCompactionJob == nil {
					return nil, errors.Newf("missing CompactBackups")
				}
				ary := *tree.MustBeDArray(args[0])
				collectionURI, ok := darrayToStringSlice(ary)
				if !ok {
					return nil, errors.Newf("expected array value, got %T", args[0])
				}
				var encryption jobspb.BackupEncryptionOptions
				encryptionBytes := []byte(tree.MustBeDBytes(args[2]))
				if len(encryptionBytes) == 0 {
					encryption = jobspb.BackupEncryptionOptions{Mode: jobspb.EncryptionMode_None}
				} else if err := protoutil.Unmarshal([]byte(tree.MustBeDBytes(args[2])), &encryption); err != nil {
					return nil, err
				}
				fullPath := string(tree.MustBeDString(args[1]))
				start := tree.MustBeDDecimal(args[3])
				startTs, err := hlc.DecimalToHLC(&start.Decimal)
				if err != nil {
					return nil, err
				}
				end := tree.MustBeDDecimal(args[4])
				endTs, err := hlc.DecimalToHLC(&end.Decimal)
				if err != nil {
					return nil, err
				}
				evalCtx.Planner.ExecutorConfig()
				jobID, err := StartCompactionJob(
					ctx, evalCtx.Planner, collectionURI, nil, fullPath,
					encryption, startTs, endTs,
				)
				return tree.NewDInt(tree.DInt(jobID)), err
			},
			Info:       "Compacts the chain of incremental backups described by the start and end times (nanosecond epoch).",
			Volatility: volatility.Volatile,
		},
	),
}

var lengthImpls = func(incBitOverload bool) builtinDefinition {
	overloads := []tree.Overload{
		stringOverload1(
			func(_ context.Context, _ *eval.Context, s string) (tree.Datum, error) {
				return tree.NewDInt(tree.DInt(utf8.RuneCountInString(s))), nil
			},
			types.Int,
			"Calculates the number of characters in `val`.",
			volatility.Immutable,
		),
		bytesOverload1(
			func(_ context.Context, _ *eval.Context, s string) (tree.Datum, error) {
				return tree.NewDInt(tree.DInt(len(s))), nil
			},
			types.Int,
			"Calculates the number of bytes in `val`.",
			volatility.Immutable,
		),
	}
	if incBitOverload {
		overloads = append(
			overloads,
			bitsOverload1(
				func(_ context.Context, _ *eval.Context, s *tree.DBitArray) (tree.Datum, error) {
					return tree.NewDInt(tree.DInt(s.BitArray.BitLen())), nil
				}, types.Int, "Calculates the number of bits in `val`.",
				volatility.Immutable,
			),
		)
	}
	return makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategoryString}, overloads...)
}

func makeSubStringImpls() builtinDefinition {
	return makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategoryString},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "input", Typ: types.String},
				{Name: "start_pos", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				substring := getSubstringFromIndex(string(tree.MustBeDString(args[0])), int(tree.MustBeDInt(args[1])))
				return tree.NewDString(substring), nil
			},
			Info:       "Returns a substring of `input` starting at `start_pos` (count starts at 1).",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "input", Typ: types.String},
				{Name: "start_pos", Typ: types.Int},
				{Name: "length", Typ: types.Int},
			},
			SpecializedVecBuiltin: tree.SubstringStringIntInt,
			ReturnType:            tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "input", Typ: types.String},
				{Name: "regex", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				pattern := string(tree.MustBeDString(args[1]))
				return regexpExtract(evalCtx, s, pattern, `\`)
			},
			Info:       "Returns a substring of `input` that matches the regular expression `regex`.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "input", Typ: types.String},
				{Name: "regex", Typ: types.String},
				{Name: "escape_char", Typ: types.String},
			},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				s := string(tree.MustBeDString(args[0]))
				pattern := string(tree.MustBeDString(args[1]))
				escape := string(tree.MustBeDString(args[2]))
				return regexpExtract(evalCtx, s, pattern, escape)
			},
			Info: "Returns a substring of `input` that matches the regular expression `regex` using " +
				"`escape_char` as your escape character instead of `\\`.",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "input", Typ: types.VarBit},
				{Name: "start_pos", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.VarBit),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				bitString := tree.MustBeDBitArray(args[0])
				start := int(tree.MustBeDInt(args[1]))
				substring := getSubstringFromIndex(bitString.BitArray.String(), start)
				return tree.ParseDBitArray(substring)
			},
			Info:       "Returns a bit subarray of `input` starting at `start_pos` (count starts at 1).",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "input", Typ: types.VarBit},
				{Name: "start_pos", Typ: types.Int},
				{Name: "length", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.VarBit),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "input", Typ: types.Bytes},
				{Name: "start_pos", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				byteString := string(*args[0].(*tree.DBytes))
				start := int(tree.MustBeDInt(args[1]))
				substring := getSubstringFromIndexBytes(byteString, start)
				return tree.NewDBytes(tree.DBytes(substring)), nil
			},
			Info:       "Returns a byte subarray of `input` starting at `start_pos` (count starts at 1).",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{
				{Name: "input", Typ: types.Bytes},
				{Name: "start_pos", Typ: types.Int},
				{Name: "length", Typ: types.Int},
			},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
	)
}

var formatImpls = makeBuiltin(tree.FunctionProperties{Category: builtinconstants.CategoryString},
	tree.Overload{
		Types:      tree.VariadicType{FixedTypes: []*types.T{types.String}, VarType: types.AnyElement},
		ReturnType: tree.FixedReturnType(types.String),
		Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
			if args[0] == tree.DNull {
				return tree.DNull, nil
			}
			formatStr := tree.MustBeDString(args[0])
			formatArgs := args[1:]
			str, err := pgformat.Format(ctx, evalCtx, string(formatStr), formatArgs...)
			if err != nil {
				return nil, pgerror.Wrap(err, pgcode.InvalidParameterValue, "error parsing format string")
			}
			return tree.NewDString(str), nil
		},
		Info:              "Interprets the first argument as a format string similar to C sprintf and interpolates the remaining arguments.",
		Volatility:        volatility.Stable,
		CalledOnNullInput: true,
	})

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

func generateRandomUUID4Impl() builtinDefinition {
	return makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategoryIDGeneration,
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Uuid),
			Fn: func(_ context.Context, _ *eval.Context, _ tree.Datums) (tree.Datum, error) {
				uv := uuid.NewV4()
				return tree.NewDUuid(tree.DUuid{UUID: uv}), nil
			},
			Info:       "Generates a random version 4 UUID, and returns it as a value of UUID type.",
			Volatility: volatility.Volatile,
		},
	)
}

func strftimeImpl() builtinDefinition {
	return makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategoryDateAndTime,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "input", Typ: types.Timestamp}, {Name: "extract_format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "input", Typ: types.Date}, {Name: "extract_format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "input", Typ: types.TimestampTZ}, {Name: "extract_format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
	)
}

func strptimeImpl() builtinDefinition {
	return makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategoryDateAndTime,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "input", Typ: types.String}, {Name: "format", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.TimestampTZ),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
			Volatility: volatility.Immutable,
		},
	)
}

func uuidV4Impl() builtinDefinition {
	return makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategoryIDGeneration,
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Bytes),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				return tree.NewDBytes(tree.DBytes(uuid.MakeV4().GetBytes())), nil
			},
			Info:       "Returns a UUID.",
			Volatility: volatility.Volatile,
		},
	)
}

func generateConstantUUIDImpl(id uuid.UUID, info string) builtinDefinition {
	return makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategoryIDGeneration,
		},
		tree.Overload{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Uuid),
			Fn: func(_ context.Context, _ *eval.Context, _ tree.Datums) (tree.Datum, error) {
				return tree.NewDUuid(tree.DUuid{UUID: id}), nil
			},
			Info:       info,
			Volatility: volatility.Immutable,
		},
	)
}

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
	pref := tree.OverloadPreferencePreferred
	tzPref := tree.OverloadPreferenceNone
	if preferTZOverload {
		pref, tzPref = tzPref, pref
	}
	tzAdditionalDesc, noTZAdditionalDesc := getTimeAdditionalDesc(preferTZOverload)
	return []tree.Overload{
		{
			Types:              tree.ParamTypes{},
			ReturnType:         tree.FixedReturnType(types.TimestampTZ),
			OverloadPreference: tzPref,
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return evalCtx.GetTxnTimestamp(time.Microsecond), nil
			},
			Info:       txnTSDoc + tzAdditionalDesc,
			Volatility: volatility.Stable,
		},
		{
			Types:              tree.ParamTypes{},
			ReturnType:         tree.FixedReturnType(types.Timestamp),
			OverloadPreference: pref,
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return evalCtx.GetTxnTimestampNoZone(time.Microsecond), nil
			},
			Info:       txnTSDoc + noTZAdditionalDesc,
			Volatility: volatility.Stable,
		},
		{
			Types:      tree.ParamTypes{},
			ReturnType: tree.FixedReturnType(types.Date),
			Fn:         currentDate,
			Info:       txnTSDoc,
			Volatility: volatility.Stable,
		},
	}
}

func txnTSWithPrecisionOverloads(preferTZOverload bool) []tree.Overload {
	pref := tree.OverloadPreferencePreferred
	tzPref := tree.OverloadPreferenceNone
	if preferTZOverload {
		pref, tzPref = tzPref, pref
	}
	tzAdditionalDesc, noTZAdditionalDesc := getTimeAdditionalDesc(preferTZOverload)
	return append(
		[]tree.Overload{
			{
				Types:              tree.ParamTypes{{Name: "precision", Typ: types.Int}},
				ReturnType:         tree.FixedReturnType(types.TimestampTZ),
				OverloadPreference: tzPref,
				Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
					prec := int32(tree.MustBeDInt(args[0]))
					if prec < 0 || prec > 6 {
						return nil, pgerror.Newf(pgcode.NumericValueOutOfRange, "precision %d out of range", prec)
					}
					return evalCtx.GetTxnTimestamp(tree.TimeFamilyPrecisionToRoundDuration(prec)), nil
				},
				Info:       txnTSDoc + tzAdditionalDesc,
				Volatility: volatility.Stable,
			},
			{
				Types:              tree.ParamTypes{{Name: "precision", Typ: types.Int}},
				ReturnType:         tree.FixedReturnType(types.Timestamp),
				OverloadPreference: pref,
				Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
					prec := int32(tree.MustBeDInt(args[0]))
					if prec < 0 || prec > 6 {
						return nil, pgerror.Newf(pgcode.NumericValueOutOfRange, "precision %d out of range", prec)
					}
					return evalCtx.GetTxnTimestampNoZone(tree.TimeFamilyPrecisionToRoundDuration(prec)), nil
				},
				Info:       txnTSDoc + noTZAdditionalDesc,
				Volatility: volatility.Stable,
			},
			{
				Types:      tree.ParamTypes{{Name: "precision", Typ: types.Int}},
				ReturnType: tree.FixedReturnType(types.Date),
				Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
					prec := int32(tree.MustBeDInt(args[0]))
					if prec < 0 || prec > 6 {
						return nil, pgerror.Newf(pgcode.NumericValueOutOfRange, "precision %d out of range", prec)
					}
					return currentDate(ctx, evalCtx, args)
				},
				Info:       txnTSDoc,
				Volatility: volatility.Stable,
			},
		},
		txnTSOverloads(preferTZOverload)...,
	)
}

func txnTSImplBuiltin(preferTZOverload bool) builtinDefinition {
	return makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategoryDateAndTime,
		},
		txnTSOverloads(preferTZOverload)...,
	)
}

func txnTSWithPrecisionImplBuiltin(preferTZOverload bool) builtinDefinition {
	return makeBuiltin(
		tree.FunctionProperties{
			Category: builtinconstants.CategoryDateAndTime,
		},
		txnTSWithPrecisionOverloads(preferTZOverload)...,
	)
}

func txnTimeWithPrecisionBuiltin(preferTZOverload bool) builtinDefinition {
	pref := tree.OverloadPreferencePreferred
	tzPref := tree.OverloadPreferenceNone
	if preferTZOverload {
		pref, tzPref = tzPref, pref
	}
	tzAdditionalDesc, noTZAdditionalDesc := getTimeAdditionalDesc(preferTZOverload)
	return makeBuiltin(
		defProps(),
		tree.Overload{
			Types:              tree.ParamTypes{},
			ReturnType:         tree.FixedReturnType(types.TimeTZ),
			OverloadPreference: tzPref,
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return evalCtx.GetTxnTime(time.Microsecond), nil
			},
			Info:       "Returns the current transaction's time with time zone." + tzAdditionalDesc,
			Volatility: volatility.Stable,
		},
		tree.Overload{
			Types:              tree.ParamTypes{},
			ReturnType:         tree.FixedReturnType(types.Time),
			OverloadPreference: pref,
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				return evalCtx.GetTxnTimeNoZone(time.Microsecond), nil
			},
			Info:       "Returns the current transaction's time with no time zone." + noTZAdditionalDesc,
			Volatility: volatility.Stable,
		},
		tree.Overload{
			Types:              tree.ParamTypes{{Name: "precision", Typ: types.Int}},
			ReturnType:         tree.FixedReturnType(types.TimeTZ),
			OverloadPreference: tzPref,
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				prec := int32(tree.MustBeDInt(args[0]))
				if prec < 0 || prec > 6 {
					return nil, pgerror.Newf(pgcode.NumericValueOutOfRange, "precision %d out of range", prec)
				}
				return evalCtx.GetTxnTime(tree.TimeFamilyPrecisionToRoundDuration(prec)), nil
			},
			Info:       "Returns the current transaction's time with time zone." + tzAdditionalDesc,
			Volatility: volatility.Stable,
		},
		tree.Overload{
			Types:              tree.ParamTypes{{Name: "precision", Typ: types.Int}},
			ReturnType:         tree.FixedReturnType(types.Time),
			OverloadPreference: pref,
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				prec := int32(tree.MustBeDInt(args[0]))
				if prec < 0 || prec > 6 {
					return nil, pgerror.Newf(pgcode.NumericValueOutOfRange, "precision %d out of range", prec)
				}
				return evalCtx.GetTxnTimeNoZone(tree.TimeFamilyPrecisionToRoundDuration(prec)), nil
			},
			Info:       "Returns the current transaction's time with no time zone." + noTZAdditionalDesc,
			Volatility: volatility.Stable,
		},
	)
}

func verboseFingerprint(
	ctx context.Context, evalCtx *eval.Context, args tree.Datums,
) (tree.Datum, error) {
	if err := evalCtx.SessionAccessor.CheckPrivilege(
		ctx, syntheticprivilege.GlobalPrivilegeObject,
		privilege.VIEWCLUSTERMETADATA,
	); err != nil {
		return nil, pgerror.Wrap(err, pgcode.InsufficientPrivilege, "crdb_internal.fingerprint()")
	}

	if len(args) != 3 {
		return nil, errors.New("argument list must have three elements")
	}
	span, err := parseSpan(args[0])
	if err != nil {
		return nil, err
	}

	// The startTime can either be a timestampTZ or a decimal.
	var startTimestamp hlc.Timestamp
	if parsedDecimal, ok := tree.AsDDecimal(args[1]); ok {
		startTimestamp, err = hlc.DecimalToHLC(&parsedDecimal.Decimal)
		if err != nil {
			return nil, err
		}
	} else {
		startTime := tree.MustBeDTimestampTZ(args[1]).Time
		startTimestamp = hlc.Timestamp{WallTime: startTime.UnixNano()}
	}

	allRevisions := bool(tree.MustBeDBool(args[2]))
	fp, err := evalCtx.Planner.FingerprintSpan(ctx, span, startTimestamp, allRevisions, false /* stripped */)
	if err != nil {
		return nil, err
	}
	return tree.NewDInt(tree.DInt(fp)), nil
}

func currentDate(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
	t := evalCtx.GetTxnTimestamp(time.Microsecond).Time
	t = t.In(evalCtx.GetLocation())
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
	errJSONObjectNotEvenNumberOfElements = pgerror.New(pgcode.ArraySubscript,
		"array must have even number of elements")
	errJSONObjectNullValueForKey = pgerror.New(pgcode.NullValueNotAllowed,
		"null value not allowed for object key")
	errJSONObjectMismatchedArrayDim = pgerror.New(pgcode.InvalidParameterValue,
		"mismatched array dimensions")
)

var jsonExtractPathImpl = tree.Overload{
	Types:      tree.VariadicType{FixedTypes: []*types.T{types.Jsonb}, VarType: types.String},
	ReturnType: tree.FixedReturnType(types.Jsonb),
	Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
	Volatility: volatility.Immutable,
}

var jsonExtractPathTextImpl = tree.Overload{
	Types:      tree.VariadicType{FixedTypes: []*types.T{types.Jsonb}, VarType: types.String},
	ReturnType: tree.FixedReturnType(types.String),
	Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
	Volatility: volatility.Immutable,
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
	Types: tree.ParamTypes{
		{Name: "val", Typ: types.Jsonb},
		{Name: "path", Typ: types.StringArray},
		{Name: "to", Typ: types.Jsonb},
	},
	ReturnType: tree.FixedReturnType(types.Jsonb),
	Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
		return jsonDatumSet(args[0], args[1], args[2], tree.DBoolTrue)
	},
	Info:       "Returns the JSON value pointed to by the variadic arguments.",
	Volatility: volatility.Immutable,
}

var jsonSetWithCreateMissingImpl = tree.Overload{
	Types: tree.ParamTypes{
		{Name: "val", Typ: types.Jsonb},
		{Name: "path", Typ: types.StringArray},
		{Name: "to", Typ: types.Jsonb},
		{Name: "create_missing", Typ: types.Bool},
	},
	ReturnType: tree.FixedReturnType(types.Jsonb),
	Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
		return jsonDatumSet(args[0], args[1], args[2], args[3])
	},
	Info: "Returns the JSON value pointed to by the variadic arguments. " +
		"If `create_missing` is false, new keys will not be inserted to objects " +
		"and values will not be prepended or appended to arrays.",
	Volatility: volatility.Immutable,
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
	Types: tree.ParamTypes{
		{Name: "target", Typ: types.Jsonb},
		{Name: "path", Typ: types.StringArray},
		{Name: "new_val", Typ: types.Jsonb},
	},
	ReturnType: tree.FixedReturnType(types.Jsonb),
	Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
		return insertToJSONDatum(args[0], args[1], args[2], tree.DBoolFalse)
	},
	Info:       "Returns the JSON value pointed to by the variadic arguments. `new_val` will be inserted before path target.",
	Volatility: volatility.Immutable,
}

var jsonInsertWithInsertAfterImpl = tree.Overload{
	Types: tree.ParamTypes{
		{Name: "target", Typ: types.Jsonb},
		{Name: "path", Typ: types.StringArray},
		{Name: "new_val", Typ: types.Jsonb},
		{Name: "insert_after", Typ: types.Bool},
	},
	ReturnType: tree.FixedReturnType(types.Jsonb),
	Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
		return insertToJSONDatum(args[0], args[1], args[2], args[3])
	},
	Info: "Returns the JSON value pointed to by the variadic arguments. " +
		"If `insert_after` is true (default is false), `new_val` will be inserted after path target.",
	Volatility: volatility.Immutable,
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
	Types:      tree.ParamTypes{{Name: "val", Typ: types.Jsonb}},
	ReturnType: tree.FixedReturnType(types.String),
	Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
	Volatility: volatility.Immutable,
}

func jsonProps() tree.FunctionProperties {
	return tree.FunctionProperties{
		Category: builtinconstants.CategoryJSON,
	}
}

var jsonBuildObjectImpl = tree.Overload{
	Types:      tree.VariadicType{VarType: types.AnyElement},
	ReturnType: tree.FixedReturnType(types.Jsonb),
	Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
		if len(args)%2 != 0 {
			return nil, pgerror.New(pgcode.InvalidParameterValue,
				"argument list must have even number of elements")
		}

		builder := json.NewObjectBuilder(len(args) / 2)
		for i := 0; i < len(args); i += 2 {
			if args[i] == tree.DNull {
				return nil, errJSONObjectNullValueForKey
			}

			key, err := asJSONBuildObjectKey(
				args[i],
				evalCtx.SessionData().DataConversionConfig,
				evalCtx.GetLocation(),
			)
			if err != nil {
				return nil, err
			}

			val, err := tree.AsJSON(
				args[i+1],
				evalCtx.SessionData().DataConversionConfig,
				evalCtx.GetLocation(),
			)
			if err != nil {
				return nil, err
			}

			builder.Add(key, val)
		}

		return tree.NewDJSON(builder.Build()), nil
	},
	Info:              "Builds a JSON object out of a variadic argument list.",
	Volatility:        volatility.Stable,
	CalledOnNullInput: true,
}

var toJSONImpl = tree.Overload{
	Types:      tree.ParamTypes{{Name: "val", Typ: types.AnyElement}},
	ReturnType: tree.FixedReturnType(types.Jsonb),
	Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
		return toJSONObject(evalCtx, args[0])
	},
	Info:       "Returns the value as JSON or JSONB.",
	Volatility: volatility.Stable,
}

var prettyPrintNotSupportedError = pgerror.Newf(pgcode.FeatureNotSupported, "pretty printing is not supported")

var arrayToJSONImpls = makeBuiltin(jsonProps(),
	tree.Overload{
		Types:      tree.ParamTypes{{Name: "array", Typ: types.AnyArray}},
		ReturnType: tree.FixedReturnType(types.Jsonb),
		Fn:         toJSONImpl.Fn,
		Info:       "Returns the array as JSON or JSONB.",
		Volatility: volatility.Stable,
	},
	tree.Overload{
		Types:      tree.ParamTypes{{Name: "array", Typ: types.AnyArray}, {Name: "pretty_bool", Typ: types.Bool}},
		ReturnType: tree.FixedReturnType(types.Jsonb),
		Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
			prettyPrint := bool(tree.MustBeDBool(args[1]))
			if prettyPrint {
				return nil, prettyPrintNotSupportedError
			}
			return toJSONObject(evalCtx, args[0])
		},
		Info:       "Returns the array as JSON or JSONB.",
		Volatility: volatility.Stable,
	},
)

var jsonBuildArrayImpl = tree.Overload{
	Types:      tree.VariadicType{VarType: types.AnyElement},
	ReturnType: tree.FixedReturnType(types.Jsonb),
	Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
		builder := json.NewArrayBuilder(len(args))
		for _, arg := range args {
			j, err := tree.AsJSON(
				arg,
				evalCtx.SessionData().DataConversionConfig,
				evalCtx.GetLocation(),
			)
			if err != nil {
				return nil, err
			}
			builder.Add(j)
		}
		return tree.NewDJSON(builder.Build()), nil
	},
	Info:              "Builds a possibly-heterogeneously-typed JSON or JSONB array out of a variadic argument list.",
	Volatility:        volatility.Stable,
	CalledOnNullInput: true,
}

func jsonObjectImpls() builtinDefinition {
	return makeBuiltin(jsonProps(),
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "texts", Typ: types.StringArray}},
			ReturnType: tree.FixedReturnType(types.Jsonb),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
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
						evalCtx.SessionData().DataConversionConfig,
						evalCtx.GetLocation(),
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
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types: tree.ParamTypes{{Name: "keys", Typ: types.StringArray},
				{Name: "values", Typ: types.StringArray}},
			ReturnType: tree.FixedReturnType(types.Jsonb),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
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
						evalCtx.SessionData().DataConversionConfig,
						evalCtx.GetLocation(),
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
			Volatility: volatility.Immutable,
		},
	)
}

var jsonStripNullsImpl = tree.Overload{
	Types:      tree.ParamTypes{{Name: "from_json", Typ: types.Jsonb}},
	ReturnType: tree.FixedReturnType(types.Jsonb),
	Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
		j, _, err := tree.MustBeDJSON(args[0]).StripNulls()
		return tree.NewDJSON(j), err
	},
	Info:       "Returns from_json with all object fields that have null values omitted. Other null values are untouched.",
	Volatility: volatility.Immutable,
}

var jsonArrayLengthImpl = tree.Overload{
	Types:      tree.ParamTypes{{Name: "json", Typ: types.Jsonb}},
	ReturnType: tree.FixedReturnType(types.Int),
	Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
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
	Volatility: volatility.Immutable,
}

func similarOverloads(calledOnNullInput bool) []tree.Overload {
	return []tree.Overload{
		{
			Types:      tree.ParamTypes{{Name: "pattern", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				pattern := string(tree.MustBeDString(args[0]))
				return eval.SimilarPattern(pattern, "")
			},
			Info:              "Converts a SQL regexp `pattern` to a POSIX regexp `pattern`.",
			Volatility:        volatility.Immutable,
			CalledOnNullInput: calledOnNullInput,
		},
		{
			Types:      tree.ParamTypes{{Name: "pattern", Typ: types.String}, {Name: "escape", Typ: types.String}},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				if args[0] == tree.DNull {
					return tree.DNull, nil
				}
				pattern := string(tree.MustBeDString(args[0]))
				if args[1] == tree.DNull {
					return eval.SimilarPattern(pattern, "")
				}
				escape := string(tree.MustBeDString(args[1]))
				return eval.SimilarPattern(pattern, escape)
			},
			Info:              "Converts a SQL regexp `pattern` to a POSIX regexp `pattern` using `escape` as an escape token.",
			Volatility:        volatility.Immutable,
			CalledOnNullInput: calledOnNullInput,
		},
	}
}

// arrayBuiltin defines builtin overloads for all scalar types, enums, and
// tuples as their inputs. If supportsArrayInput is true, then it also includes
// overloads for all of these types used as array elements.
func arrayBuiltin(impl func(*types.T) tree.Overload, supportsArrayInput bool) builtinDefinition {
	overloads := make([]tree.Overload, 0, len(types.Scalar)+2)
	for _, typ := range append(types.Scalar, []*types.T{types.AnyEnum, types.AnyTuple}...) {
		if ok, _ := types.IsValidArrayElementType(typ); ok {
			overload := impl(typ)
			if typ.Family() == types.TupleFamily {
				// Prevent usage in DistSQL because it cannot handle arrays of
				// untyped tuples.
				// TODO(yuzefovich): this restriction might be unnecessary (at
				// least for aggregate builtins), re-evaluate it.
				overload.DistsqlBlocklist = true
			}
			overloads = append(overloads, overload)
			if supportsArrayInput {
				arrayTyp := types.MakeArray(typ)
				overload := impl(arrayTyp)
				// We currently don't have value encoding for nested arrays, so
				// we have to disable distributed evaluation for such overloads.
				overload.DistsqlBlocklist = true
				overloads = append(overloads, overload)
			}
		}
	}
	return makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryArray},
		overloads...,
	)
}

func arrayVariadicBuiltin(impls func(*types.T) []tree.Overload) builtinDefinition {
	overloads := make([]tree.Overload, 0, len(types.Scalar)+2)
	for _, typ := range append(types.Scalar, types.AnyEnum) {
		if ok, _ := types.IsValidArrayElementType(typ); ok {
			overloads = append(overloads, impls(typ)...)
		}
	}
	// Prevent usage in DistSQL because it cannot handle arrays of untyped tuples.
	tupleOverload := impls(types.AnyTuple)
	for i := range tupleOverload {
		tupleOverload[i].DistsqlBlocklist = true
	}
	overloads = append(overloads, tupleOverload...)
	return makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryArray},
		overloads...,
	)
}

func setProps(props tree.FunctionProperties, d builtinDefinition) builtinDefinition {
	d.props = props
	return d
}

func jsonOverload1(
	f func(context.Context, *eval.Context, json.JSON) (tree.Datum, error),
	returnType *types.T,
	info string,
	volatility volatility.V,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ParamTypes{{Name: "val", Typ: types.Jsonb}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
			return f(ctx, evalCtx, tree.MustBeDJSON(args[0]).JSON)
		},
		Info:       info,
		Volatility: volatility,
	}
}

func stringOverload1(
	f func(context.Context, *eval.Context, string) (tree.Datum, error),
	returnType *types.T,
	info string,
	volatility volatility.V,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ParamTypes{{Name: "val", Typ: types.String}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
			return f(ctx, evalCtx, string(tree.MustBeDString(args[0])))
		},
		Info:       info,
		Volatility: volatility,
	}
}

func stringOverload2(
	a, b string,
	f func(context.Context, *eval.Context, string, string) (tree.Datum, error),
	returnType *types.T,
	info string,
	volatility volatility.V,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ParamTypes{{Name: a, Typ: types.String}, {Name: b, Typ: types.String}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
			return f(ctx, evalCtx, string(tree.MustBeDString(args[0])), string(tree.MustBeDString(args[1])))
		},
		Info:       info,
		Volatility: volatility,
	}
}

func stringOverload3(
	a, b, c string,
	f func(context.Context, *eval.Context, string, string, string) (tree.Datum, error),
	returnType *types.T,
	info string,
	volatility volatility.V,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ParamTypes{{Name: a, Typ: types.String}, {Name: b, Typ: types.String}, {Name: c, Typ: types.String}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
			return f(ctx, evalCtx, string(tree.MustBeDString(args[0])), string(tree.MustBeDString(args[1])), string(tree.MustBeDString(args[2])))
		},
		Info:       info,
		Volatility: volatility,
	}
}

func bytesOverload1(
	f func(context.Context, *eval.Context, string) (tree.Datum, error),
	returnType *types.T,
	info string,
	volatility volatility.V,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ParamTypes{{Name: "val", Typ: types.Bytes}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
			return f(ctx, evalCtx, string(*args[0].(*tree.DBytes)))
		},
		Info:       info,
		Volatility: volatility,
	}
}

func bytesOverload2(
	a, b string,
	f func(context.Context, *eval.Context, string, string) (tree.Datum, error),
	returnType *types.T,
	info string,
	volatility volatility.V,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ParamTypes{{Name: a, Typ: types.Bytes}, {Name: b, Typ: types.Bytes}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
			return f(ctx, evalCtx, string(*args[0].(*tree.DBytes)), string(*args[1].(*tree.DBytes)))
		},
		Info:       info,
		Volatility: volatility,
	}
}

func bitsOverload1(
	f func(context.Context, *eval.Context, *tree.DBitArray) (tree.Datum, error),
	returnType *types.T,
	info string,
	volatility volatility.V,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ParamTypes{{Name: "val", Typ: types.VarBit}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
			return f(ctx, evalCtx, args[0].(*tree.DBitArray))
		},
		Info:       info,
		Volatility: volatility,
	}
}

func bitsOverload2(
	a, b string,
	f func(context.Context, *eval.Context, *tree.DBitArray, *tree.DBitArray) (tree.Datum, error),
	returnType *types.T,
	info string,
	volatility volatility.V,
) tree.Overload {
	return tree.Overload{
		Types:      tree.ParamTypes{{Name: a, Typ: types.VarBit}, {Name: b, Typ: types.VarBit}},
		ReturnType: tree.FixedReturnType(returnType),
		Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
			return f(ctx, evalCtx, args[0].(*tree.DBitArray), args[1].(*tree.DBitArray))
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
	return makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.String},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				h := newHash()
				if ok, err := feedHash(h, args); !ok || err != nil {
					return tree.DNull, err
				}
				return tree.NewDString(fmt.Sprintf("%x", h.Sum(nil))), nil
			},
			Info:              info,
			Volatility:        volatility.Leakproof,
			CalledOnNullInput: true,
		},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.Bytes},
			ReturnType: tree.FixedReturnType(types.String),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				h := newHash()
				if ok, err := feedHash(h, args); !ok || err != nil {
					return tree.DNull, err
				}
				return tree.NewDString(fmt.Sprintf("%x", h.Sum(nil))), nil
			},
			Info:              info,
			Volatility:        volatility.Leakproof,
			CalledOnNullInput: true,
		},
	)
}

func hash32Builtin(newHash func() hash.Hash32, info string) builtinDefinition {
	return makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.String},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				h := newHash()
				if ok, err := feedHash(h, args); !ok || err != nil {
					return tree.DNull, err
				}
				return tree.NewDInt(tree.DInt(h.Sum32())), nil
			},
			Info:              info,
			Volatility:        volatility.Leakproof,
			CalledOnNullInput: true,
		},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.Bytes},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				h := newHash()
				if ok, err := feedHash(h, args); !ok || err != nil {
					return tree.DNull, err
				}
				return tree.NewDInt(tree.DInt(h.Sum32())), nil
			},
			Info:              info,
			Volatility:        volatility.Leakproof,
			CalledOnNullInput: true,
		},
	)
}

func hash64Builtin(newHash func() hash.Hash64, info string) builtinDefinition {
	return makeBuiltin(defProps(),
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.String},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				h := newHash()
				if ok, err := feedHash(h, args); !ok || err != nil {
					return tree.DNull, err
				}
				return tree.NewDInt(tree.DInt(h.Sum64())), nil
			},
			Info:              info,
			Volatility:        volatility.Leakproof,
			CalledOnNullInput: true,
		},
		tree.Overload{
			Types:      tree.VariadicType{VarType: types.Bytes},
			ReturnType: tree.FixedReturnType(types.Int),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				h := newHash()
				if ok, err := feedHash(h, args); !ok || err != nil {
					return tree.DNull, err
				}
				return tree.NewDInt(tree.DInt(h.Sum64())), nil
			},
			Info:              info,
			Volatility:        volatility.Leakproof,
			CalledOnNullInput: true,
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

func regexpExtract(evalCtx *eval.Context, s, pattern, escape string) (tree.Datum, error) {
	patternRe, err := evalCtx.ReCache.GetRegexp(regexpEscapeKey{pattern, escape})
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

func regexpSplit(evalCtx *eval.Context, args tree.Datums, hasFlags bool) ([]string, error) {
	s := string(tree.MustBeDString(args[0]))
	pattern := string(tree.MustBeDString(args[1]))
	sqlFlags := ""
	if hasFlags {
		sqlFlags = string(tree.MustBeDString(args[2]))
	}
	patternRe, err := evalCtx.ReCache.GetRegexp(regexpFlagKey{pattern, sqlFlags})
	if err != nil {
		return nil, err
	}
	return patternRe.Split(s, -1), nil
}

func regexpSplitToArray(
	evalCtx *eval.Context, args tree.Datums, hasFlags bool,
) (tree.Datum, error) {
	words, err := regexpSplit(evalCtx, args, hasFlags /* hasFlags */)
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

func regexpReplace(evalCtx *eval.Context, s, pattern, to, sqlFlags string) (tree.Datum, error) {
	patternRe, err := evalCtx.ReCache.GetRegexp(regexpFlagKey{pattern, sqlFlags})
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

func cardinality(arr *tree.DArray) tree.Datum {
	if arr.ParamTyp.Family() != types.ArrayFamily {
		return tree.NewDInt(tree.DInt(arr.Len()))
	}
	card := 0
	for _, a := range arr.Array {
		card += int(tree.MustBeDInt(cardinality(tree.MustBeDArray(a))))
	}
	return tree.NewDInt(tree.DInt(card))
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

func extractBuiltin() builtinDefinition {
	return makeBuiltin(
		tree.FunctionProperties{Category: builtinconstants.CategoryDateAndTime},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "element", Typ: types.String}, {Name: "input", Typ: types.Timestamp}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				// extract timeSpan fromTime.
				fromTS := args[1].(*tree.DTimestamp)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				return extractTimeSpanFromTimestamp(evalCtx, fromTS.Time, timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: millennium, century, decade, year, isoyear,\n" +
				"quarter, month, week, dayofweek, isodow, dayofyear, julian,\n" +
				"hour, minute, second, millisecond, microsecond, epoch",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "element", Typ: types.String}, {Name: "input", Typ: types.Interval}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				fromInterval := args[1].(*tree.DInterval)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				return extractTimeSpanFromInterval(fromInterval, timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: millennium, century, decade, year,\n" +
				"month, day, hour, minute, second, millisecond, microsecond, epoch",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "element", Typ: types.String}, {Name: "input", Typ: types.Date}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				date := args[1].(*tree.DDate)
				fromTime, err := date.ToTime()
				if err != nil {
					return nil, err
				}
				return extractTimeSpanFromTimestamp(evalCtx, fromTime, timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: millennium, century, decade, year, isoyear,\n" +
				"quarter, month, week, dayofweek, isodow, dayofyear, julian,\n" +
				"hour, minute, second, millisecond, microsecond, epoch",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "element", Typ: types.String}, {Name: "input", Typ: types.TimestampTZ}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
				fromTSTZ := args[1].(*tree.DTimestampTZ)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				return extractTimeSpanFromTimestampTZ(evalCtx, fromTSTZ.Time.In(evalCtx.GetLocation()), timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: millennium, century, decade, year, isoyear,\n" +
				"quarter, month, week, dayofweek, isodow, dayofyear, julian,\n" +
				"hour, minute, second, millisecond, microsecond, epoch,\n" +
				"timezone, timezone_hour, timezone_minute",
			Volatility: volatility.Stable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "element", Typ: types.String}, {Name: "input", Typ: types.Time}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				fromTime := args[1].(*tree.DTime)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				return extractTimeSpanFromTime(fromTime, timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: hour, minute, second, millisecond, microsecond, epoch",
			Volatility: volatility.Immutable,
		},
		tree.Overload{
			Types:      tree.ParamTypes{{Name: "element", Typ: types.String}, {Name: "input", Typ: types.TimeTZ}},
			ReturnType: tree.FixedReturnType(types.Float),
			Fn: func(_ context.Context, _ *eval.Context, args tree.Datums) (tree.Datum, error) {
				fromTime := args[1].(*tree.DTimeTZ)
				timeSpan := strings.ToLower(string(tree.MustBeDString(args[0])))
				return extractTimeSpanFromTimeTZ(fromTime, timeSpan)
			},
			Info: "Extracts `element` from `input`.\n\n" +
				"Compatible elements: hour, minute, second, millisecond, microsecond, epoch,\n" +
				"timezone, timezone_hour, timezone_minute",
			Volatility: volatility.Immutable,
		},
	)
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

func extractTimeSpanFromTimestampTZ(
	evalCtx *eval.Context, fromTime time.Time, timeSpan string,
) (tree.Datum, error) {
	_, offsetSecs := fromTime.Zone()
	if ret := extractTimezoneFromOffset(int32(offsetSecs), timeSpan); ret != nil {
		return ret, nil
	}

	switch timeSpan {
	case "epoch":
		return extractTimeSpanFromTimestamp(evalCtx, fromTime, timeSpan)
	default:
		// time.Time's Year(), Month(), Day(), ISOWeek(), etc. all deal in terms
		// of UTC, rather than as the timezone.
		// Remedy this by assuming that the timezone is UTC (to prevent confusion)
		// and offsetting time when using extractTimeSpanFromTimestamp.
		pretendTime := fromTime.In(time.UTC).Add(time.Duration(offsetSecs) * time.Second)
		return extractTimeSpanFromTimestamp(evalCtx, pretendTime, timeSpan)
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
	_ *eval.Context, fromTime time.Time, timeSpan string,
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
		julianDay := float64(pgdate.DateToJulianDay(fromTime.Year(), int(fromTime.Month()), fromTime.Day())) +
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

// makeEnumTypeFunc creates a FnWithExprs for a function that takes an enum but only cares about its
// type.
func makeEnumTypeFunc(impl func(t *types.T) (tree.Datum, error)) tree.FnWithExprsOverload {
	return eval.FnWithExprsOverload(func(
		ctx context.Context, evalCtx *eval.Context, args tree.Exprs,
	) (tree.Datum, error) {
		enumType := args[0].(tree.TypedExpr).ResolvedType()
		if enumType.Family() == types.UnknownFamily || enumType.Identical(types.AnyEnum) {
			return nil, errors.WithHint(pgerror.New(pgcode.InvalidParameterValue, "input expression must always resolve to the same enum type"),
				"Try NULL::yourenumtype")
		}
		// This assertion failure is necessary so that the type resolver knows it needs to hydrate.
		if !enumType.IsHydrated() {
			return nil, errors.AssertionFailedf("unhydrated type %+v", enumType)
		}
		return impl(enumType)
	})
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
	evalCtx *eval.Context, arr *tree.DArray, delim string, nullStr *string,
) (tree.Datum, error) {
	f := evalCtx.FmtCtx(tree.FmtArrayToString)
	arrayToStringHelper(evalCtx, arr, delim, nullStr, f)
	return tree.NewDString(f.CloseAndGetString()), nil
}

func arrayToStringHelper(
	evalCtx *eval.Context, arr *tree.DArray, delim string, nullStr *string, f *tree.FmtCtx,
) {

	for i := range arr.Array {
		if arr.Array[i] == tree.DNull {
			if nullStr == nil {
				continue
			}
			f.WriteString(*nullStr)
		} else {
			if nestedArray, ok := arr.Array[i].(*tree.DArray); ok {
				// "Unpack" nested arrays to be consistent with postgres.
				arrayToStringHelper(evalCtx, nestedArray, delim, nullStr, f)
			} else {
				f.FormatNode(arr.Array[i])
			}
		}
		if i < len(arr.Array)-1 {
			f.WriteString(delim)
		}
	}
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
		fixedOffsetLoc := timeutil.FixedTimeZoneOffsetToLocation(origZoneOffset, "date_trunc")
		fixedOffsetTime := time.Date(year, month, day, hour, min, sec, nsec, fixedOffsetLoc)
		locCorrectedOffsetTime := fixedOffsetTime.In(loc)

		if _, zoneOffset := locCorrectedOffsetTime.Zone(); origZoneOffset == zoneOffset {
			toTime = locCorrectedOffsetTime
		}
	}
	return tree.MakeDTimestampTZ(toTime, time.Microsecond)
}

func truncateInterval(fromInterval *tree.DInterval, timeSpan string) (*tree.DInterval, error) {

	toInterval := tree.DInterval{}

	switch timeSpan {
	case "millennia", "millennium", "millenniums":
		toInterval.Months = fromInterval.Months - fromInterval.Months%(12*1000)

	case "centuries", "century":
		toInterval.Months = fromInterval.Months - fromInterval.Months%(12*100)

	case "decade", "decades":
		toInterval.Months = fromInterval.Months - fromInterval.Months%(12*10)

	case "year", "years":
		toInterval.Months = fromInterval.Months - fromInterval.Months%12

	case "quarter":
		toInterval.Months = fromInterval.Months - fromInterval.Months%3

	case "month", "months":
		toInterval.Months = fromInterval.Months

	case "week", "weeks":
		// not supported by postgres 14 regardless of the fromInterval (error message is always the same)
		return nil, pgerror.Newf(pgcode.FeatureNotSupported, "interval units %q not supported because months usually have fractional weeks", timeSpan)

	case "day", "days":
		toInterval.Months = fromInterval.Months
		toInterval.Days = fromInterval.Days

	case "hour", "hours":
		toInterval.Months = fromInterval.Months
		toInterval.Days = fromInterval.Days
		toInterval.SetNanos(fromInterval.Nanos() - fromInterval.Nanos()%time.Hour.Nanoseconds())

	case "minute", "minutes":
		toInterval.Months = fromInterval.Months
		toInterval.Days = fromInterval.Days
		toInterval.SetNanos(fromInterval.Nanos() - fromInterval.Nanos()%time.Minute.Nanoseconds())

	case "second", "seconds":
		toInterval.Months = fromInterval.Months
		toInterval.Days = fromInterval.Days
		toInterval.SetNanos(fromInterval.Nanos() - fromInterval.Nanos()%time.Second.Nanoseconds())

	case "millisecond", "milliseconds":
		toInterval.Months = fromInterval.Months
		toInterval.Days = fromInterval.Days
		toInterval.SetNanos(fromInterval.Nanos() - fromInterval.Nanos()%time.Millisecond.Nanoseconds())

	case "microsecond", "microseconds":
		toInterval.Months = fromInterval.Months
		toInterval.Days = fromInterval.Days
		toInterval.SetNanos(fromInterval.Nanos() - fromInterval.Nanos()%time.Microsecond.Nanoseconds())

	default:
		return nil, pgerror.Newf(pgcode.InvalidParameterValue, "interval units %q not recognized", timeSpan)
	}

	return &toInterval, nil
}

// Converts a scalar Datum to its string representation
func asJSONBuildObjectKey(
	d tree.Datum, dcc sessiondatapb.DataConversionConfig, loc *time.Location,
) (string, error) {
	switch t := d.(type) {
	case *tree.DArray, *tree.DJSON, *tree.DTuple:
		return "", pgerror.New(pgcode.InvalidParameterValue,
			"key value must be scalar, not array, composite, or json")
	case *tree.DCollatedString:
		return t.Contents, nil
	case *tree.DString:
		return string(*t), nil
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
	case *tree.DBitArray, *tree.DBool, *tree.DBox2D, *tree.DBytes, *tree.DDate,
		*tree.DDecimal, *tree.DEnum, *tree.DFloat, *tree.DGeography,
		*tree.DGeometry, *tree.DIPAddr, *tree.DInt, *tree.DInterval, *tree.DOid,
		*tree.DOidWrapper, *tree.DPGLSN, *tree.DPGVector, *tree.DTime, *tree.DTimeTZ,
		*tree.DTimestamp, *tree.DTSQuery, *tree.DTSVector, *tree.DUuid, *tree.DVoid:
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

func toJSONObject(evalCtx *eval.Context, d tree.Datum) (tree.Datum, error) {
	j, err := tree.AsJSON(d, evalCtx.SessionData().DataConversionConfig, evalCtx.GetLocation())
	if err != nil {
		return nil, err
	}
	return tree.NewDJSON(j), nil
}

func jsonValidate(_ *eval.Context, string tree.DString) *tree.DBool {
	var js interface{}
	return tree.MakeDBool(gojson.Unmarshal([]byte(string), &js) == nil)
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
	if length > builtinconstants.MaxAllocatedStringSize {
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
	if length > builtinconstants.MaxAllocatedStringSize {
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

// EvalFollowerReadOffset is a function used often with AS OF SYSTEM TIME queries
// to determine the appropriate offset from now which is likely to be safe for
// follower reads. It is injected by followerreadsccl. An error may be returned
// if an enterprise license is not installed.
var EvalFollowerReadOffset func(_ *cluster.Settings) (time.Duration, error)

func recentTimestamp(ctx context.Context, evalCtx *eval.Context) (time.Time, error) {
	if EvalFollowerReadOffset == nil {
		telemetry.Inc(sqltelemetry.FollowerReadDisabledCCLCounter)
		evalCtx.ClientNoticeSender.BufferClientNotice(
			ctx,
			pgnotice.Newf("follower reads disabled because you are running a non-CCL distribution"),
		)
		return evalCtx.StmtTimestamp.Add(builtinconstants.DefaultFollowerReadDuration), nil
	}
	offset, err := EvalFollowerReadOffset(evalCtx.Settings)
	if err != nil {
		if code := pgerror.GetPGCode(err); code == pgcode.CCLValidLicenseRequired {
			telemetry.Inc(sqltelemetry.FollowerReadDisabledNoEnterpriseLicense)
			evalCtx.ClientNoticeSender.BufferClientNotice(
				ctx, pgnotice.Newf("follower reads disabled: %s", err.Error()),
			)
			return evalCtx.StmtTimestamp.Add(builtinconstants.DefaultFollowerReadDuration), nil
		}
		return time.Time{}, err
	}
	return evalCtx.StmtTimestamp.Add(offset), nil
}

func followerReadTimestamp(
	ctx context.Context, evalCtx *eval.Context, _ tree.Datums,
) (tree.Datum, error) {
	ts, err := recentTimestamp(ctx, evalCtx)
	if err != nil {
		return nil, err
	}
	return tree.MakeDTimestampTZ(ts, time.Microsecond)
}

var (
	// WithMinTimestamp is an injectable function containing the implementation of the
	// with_min_timestamp builtin.
	WithMinTimestamp = func(_ context.Context, _ *eval.Context, t time.Time) (time.Time, error) {
		return time.Time{}, pgerror.Newf(
			pgcode.CCLRequired,
			"%s can only be used with a CCL distribution",
			asof.WithMinTimestampFunctionName,
		)
	}
	// WithMaxStaleness is an injectable function containing the implementation of the
	// with_max_staleness builtin.
	WithMaxStaleness = func(_ context.Context, _ *eval.Context, d duration.Duration) (time.Time, error) {
		return time.Time{}, pgerror.Newf(
			pgcode.CCLRequired,
			"%s can only be used with a CCL distribution",
			asof.WithMaxStalenessFunctionName,
		)
	}
)

const nearestOnlyInfo = `

If nearest_only is set to true, reads that cannot be served using the nearest
available replica will error.
`

func withMinTimestampInfo(nearestOnly bool) string {
	var nearestOnlyText string
	if nearestOnly {
		nearestOnlyText = nearestOnlyInfo
	}
	return fmt.Sprintf(
		`When used in the AS OF SYSTEM TIME clause of an single-statement,
read-only transaction, CockroachDB chooses the newest timestamp before the min_timestamp
that allows execution of the reads at the nearest available replica without blocking.%s

Note this function requires an enterprise license on a CCL distribution.`,
		nearestOnlyText,
	)
}

func withMaxStalenessInfo(nearestOnly bool) string {
	var nearestOnlyText string
	if nearestOnly {
		nearestOnlyText = nearestOnlyInfo
	}
	return fmt.Sprintf(
		`When used in the AS OF SYSTEM TIME clause of an single-statement,
read-only transaction, CockroachDB chooses the newest timestamp within the staleness
bound that allows execution of the reads at the nearest available replica without blocking.%s

Note this function requires an enterprise license on a CCL distribution.`,
		nearestOnlyText,
	)
}

func withMinTimestamp(ctx context.Context, evalCtx *eval.Context, t time.Time) (tree.Datum, error) {
	t, err := WithMinTimestamp(ctx, evalCtx, t)
	if err != nil {
		return nil, err
	}
	return tree.MakeDTimestampTZ(t, time.Microsecond)
}

func withMaxStaleness(
	ctx context.Context, evalCtx *eval.Context, d duration.Duration,
) (tree.Datum, error) {
	t, err := WithMaxStaleness(ctx, evalCtx, d)
	if err != nil {
		return nil, err
	}
	return tree.MakeDTimestampTZ(t, time.Microsecond)
}

func jsonNumInvertedIndexEntries(_ *eval.Context, val tree.Datum) (tree.Datum, error) {
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
	evalCtx *eval.Context, val, version tree.Datum,
) (tree.Datum, error) {
	if val == tree.DNull {
		return tree.DZero, nil
	}
	arr := tree.MustBeDArray(val)

	v := descpb.EmptyArraysInInvertedIndexesVersion
	if version != tree.DNull {
		v = descpb.IndexDescriptorVersion(tree.MustBeDInt(version))
	}

	keys, err := rowenc.EncodeInvertedIndexTableKeys(arr, nil, v)
	if err != nil {
		return nil, err
	}
	return tree.NewDInt(tree.DInt(len(keys))), nil
}

func parseContextFromDateStyle(
	evalCtx *eval.Context, dateStyleStr string,
) (tree.ParseContext, error) {
	ds, err := pgdate.ParseDateStyle(dateStyleStr, pgdate.DefaultDateStyle())
	if err != nil {
		return nil, err
	}
	if ds.Style != pgdate.Style_ISO {
		return nil, unimplemented.NewWithIssue(41773, "only ISO style is supported")
	}
	return tree.NewParseContext(
		evalCtx.GetTxnTimestamp(time.Microsecond).Time,
		tree.NewParseContextOptionDateStyle(ds),
	), nil
}

func prettyStatementCustomConfig(
	stmt string, lineWidth int, alignMode int, caseSetting int,
) (string, error) {
	cfg := tree.DefaultPrettyCfg()
	cfg.LineWidth = lineWidth
	cfg.Align = tree.PrettyAlignMode(alignMode)
	caseMode := tree.CaseMode(caseSetting)
	if caseMode == tree.LowerCase {
		cfg.Case = func(str string) string { return strings.ToLower(str) }
	} else if caseMode == tree.UpperCase {
		cfg.Case = func(str string) string { return strings.ToUpper(str) }
	}
	return prettyStatement(cfg, stmt)
}

func prettyStatement(p tree.PrettyCfg, stmt string) (string, error) {
	stmts, err := parser.Parse(stmt)
	if err != nil {
		return "", err
	}
	var formattedStmt strings.Builder
	for idx := range stmts {
		p, err := p.Pretty(stmts[idx].AST)
		if errors.Is(err, pretty.ErrPrettyMaxRecursionDepthExceeded) {
			// If pretty-printing the statement fails, use the original
			// statement.
			p = stmt
		} else if err != nil {
			return "", err
		}
		formattedStmt.WriteString(p)
		if len(stmts) > 1 {
			formattedStmt.WriteString(";")
		}
		formattedStmt.WriteString("\n")
	}
	return formattedStmt.String(), nil
}

func parseSpan(arg tree.Datum) (roachpb.Span, error) {
	arr := tree.MustBeDArray(arg)
	if arr.Len() != 2 {
		return roachpb.Span{}, pgerror.New(pgcode.InvalidParameterValue,
			"expected an array of two elements")
	}
	if arr.Array[0] == tree.DNull {
		return roachpb.Span{}, pgerror.New(pgcode.InvalidParameterValue,
			"StartKey cannot be NULL")
	}
	if arr.Array[1] == tree.DNull {
		return roachpb.Span{}, pgerror.New(pgcode.InvalidParameterValue,
			"EndKey cannot be NULL")
	}
	startKey := []byte(tree.MustBeDBytes(arr.Array[0]))
	endKey := []byte(tree.MustBeDBytes(arr.Array[1]))
	return roachpb.Span{Key: startKey, EndKey: endKey}, nil
}

func spanToDatum(span roachpb.Span) (tree.Datum, error) {
	result := tree.NewDArray(types.Bytes)
	if err := result.Append(tree.NewDBytes(tree.DBytes(span.Key))); err != nil {
		return nil, err
	}
	if err := result.Append(tree.NewDBytes(tree.DBytes(span.EndKey))); err != nil {
		return nil, err
	}
	return result, nil
}

func makeRequestStatementBundleBuiltinOverload(
	withPlanGist bool, withAntiPlanGist bool, withRedacted bool,
) tree.Overload {
	typs := tree.ParamTypes{{Name: "stmtFingerprint", Typ: types.String}}
	info := `Used to request statement bundle for a given statement fingerprint
that has execution latency greater than the 'minExecutionLatency'. If the
'expiresAfter' argument is empty, then the statement bundle request never
expires until the statement bundle is collected`
	if withPlanGist {
		typs = append(typs, tree.ParamType{Name: "planGist", Typ: types.String})
		info += `. If 'planGist' argument is
not empty, then only the execution of the statement with the matching plan
will be used`
		if withAntiPlanGist {
			typs = append(typs, tree.ParamType{Name: "antiPlanGist", Typ: types.Bool})
			info += `. If 'antiPlanGist' argument is
true, then any plan other then the specified gist will be used`
		}
	}
	typs = append(typs, tree.ParamTypes{
		{Name: "samplingProbability", Typ: types.Float},
		{Name: "minExecutionLatency", Typ: types.Interval},
		{Name: "expiresAfter", Typ: types.Interval},
	}...)
	if withRedacted {
		typs = append(typs, tree.ParamType{Name: "redacted", Typ: types.Bool})
		info += `. If 'redacted'
argument is true, then the bundle will be redacted`
	}
	return tree.Overload{
		Types:      typs,
		ReturnType: tree.FixedReturnType(types.Bool),
		Fn: func(ctx context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
			if args[0] == tree.DNull {
				return nil, errors.New("stmtFingerprint must be non-NULL")
			}

			stmtFingerprint := string(tree.MustBeDString(args[0]))
			var planGist string
			var antiPlanGist bool
			spIdx, melIdx, eaIdx := 1, 2, 3
			if withPlanGist {
				if args[1] != tree.DNull {
					planGist = string(tree.MustBeDString(args[1]))
				}
				spIdx, melIdx, eaIdx = 2, 3, 4
				if withAntiPlanGist {
					if args[2] != tree.DNull {
						antiPlanGist = bool(tree.MustBeDBool(args[2]))
					}
					spIdx, melIdx, eaIdx = 3, 4, 5
				}
			}
			var samplingProbability float64
			if args[spIdx] != tree.DNull {
				samplingProbability = float64(tree.MustBeDFloat(args[spIdx]))
			}
			var minExecutionLatency, expiresAfter time.Duration
			if args[melIdx] != tree.DNull {
				minExecutionLatency = time.Duration(tree.MustBeDInterval(args[melIdx]).Nanos())
			}
			if args[eaIdx] != tree.DNull {
				expiresAfter = time.Duration(tree.MustBeDInterval(args[eaIdx]).Nanos())
			}
			var redacted bool
			if withRedacted {
				if args[eaIdx+1] != tree.DNull {
					redacted = bool(tree.MustBeDBool(args[eaIdx+1]))
				}
			}

			hasPriv, shouldRedact, err := evalCtx.SessionAccessor.HasViewActivityOrViewActivityRedactedRole(ctx)
			if err != nil {
				return nil, err
			}
			if shouldRedact {
				if !redacted {
					return nil, pgerror.Newf(
						pgcode.InsufficientPrivilege,
						"users with VIEWACTIVITYREDACTED privilege can only request redacted statement bundles",
					)
				}
			} else if !hasPriv {
				return nil, pgerror.Newf(
					pgcode.InsufficientPrivilege,
					"requesting statement bundle requires VIEWACTIVITY privilege",
				)
			}

			if err = evalCtx.StmtDiagnosticsRequestInserter(
				ctx,
				stmtFingerprint,
				planGist,
				antiPlanGist,
				samplingProbability,
				minExecutionLatency,
				expiresAfter,
				redacted,
			); err != nil {
				return nil, err
			}

			return tree.DBoolTrue, nil
		},
		Volatility: volatility.Volatile,
		Info:       info,
	}
}

func bitmaskAnd(aStr, bStr string) (*tree.DBitArray, error) {
	return bitmaskOp(aStr, bStr, func(a, b byte) byte { return a & b })
}

func bitmaskOr(aStr, bStr string) (*tree.DBitArray, error) {
	return bitmaskOp(aStr, bStr, func(a, b byte) byte { return a | b })
}

func bitmaskXor(aStr, bStr string) (*tree.DBitArray, error) {
	return bitmaskOp(aStr, bStr, func(a, b byte) byte { return (a ^ b) + '0' })
}

// Perform bitwise operation on the 2 bit strings that may have different
// lengths. The function applies left padding implicitly with 0s. The function
// also assumes both input strings are only comprised of charactor '0' and '1'.
func bitmaskOp(aStr, bStr string, op func(byte, byte) byte) (*tree.DBitArray, error) {
	aLen, bLen := len(aStr), len(bStr)
	bufLen := max(aLen, bLen)
	buf := make([]byte, bufLen)
	for i := 0; i < bufLen; i++ {
		var a, b byte

		if i >= aLen {
			a = '0'
		} else {
			a = aStr[aLen-i-1]
		}

		if i >= bLen {
			b = '0'
		} else {
			b = bStr[bLen-i-1]
		}

		buf[bufLen-i-1] = op(a, b)
	}

	return tree.ParseDBitArray(string(buf))
}

func makeTimestampStatementBuiltinOverload(withOutputTZ bool, withInputTZ bool) tree.Overload {
	// If we are not creating a timestamp with a timezone, we shouldn't expect an input timezone
	if !withOutputTZ && withInputTZ {
		panic("Creating a timestamp without a timezone should not have an input timestamp attached to it.")
	}
	// For the ISO 8601 standard, the conversion from a negative year to BC changes the year value (ex. -2013 == 2014 BC).
	// https://en.wikipedia.org/wiki/ISO_8601#Years
	info := "Create timestamp (formatted according to ISO 8601) "
	vol := volatility.Immutable
	typs := tree.ParamTypes{{Name: "year", Typ: types.Int}, {Name: "month", Typ: types.Int}, {Name: "day", Typ: types.Int},
		{Name: "hour", Typ: types.Int}, {Name: "min", Typ: types.Int}, {Name: "sec", Typ: types.Float}}
	returnTyp := types.Timestamp
	if withOutputTZ {
		info += "with time zone from year, month, day, hour, minute and seconds fields (negative years signify BC). If " +
			"timezone is not specified, the current time zone is used."
		returnTyp = types.TimestampTZ
		vol = volatility.Stable
		if withInputTZ {
			typs = append(typs, tree.ParamType{Name: "timezone", Typ: types.String})
		}
	} else {
		info += "from year, month, day, hour, minute, and seconds fields (negative years signify BC)."
	}
	return tree.Overload{
		Types:      typs,
		ReturnType: tree.FixedReturnType(returnTyp),
		Fn: func(_ context.Context, evalCtx *eval.Context, args tree.Datums) (tree.Datum, error) {
			year := int(tree.MustBeDInt(args[0]))
			month := time.Month(int(tree.MustBeDInt(args[1])))
			day := int(tree.MustBeDInt(args[2]))
			location := evalCtx.GetLocation()
			var err error
			if withInputTZ && withOutputTZ {
				location, err = timeutil.TimeZoneStringToLocation(string(tree.MustBeDString(args[6])), timeutil.TimeZoneStringToLocationPOSIXStandard)
				if err != nil {
					return nil, pgerror.WithCandidateCode(err, pgcode.InvalidParameterValue)
				}
			}
			if year == 0 {
				return nil, pgerror.New(pgcode.DatetimeFieldOverflow, "year value of 0 is not valid")
			}
			hour := int(tree.MustBeDInt(args[3]))
			min := int(tree.MustBeDInt(args[4]))
			sec := float64(tree.MustBeDFloat(args[5]))
			truncatedSec, remainderSec := math.Modf(sec)
			nsec := remainderSec * float64(time.Second)
			t := time.Date(year, month, day, hour, min, int(truncatedSec), int(nsec), location)
			if withOutputTZ {
				return tree.MakeDTimestampTZ(t, time.Microsecond)
			}
			return tree.MakeDTimestamp(t, time.Microsecond)
		},
		Info:       info,
		Volatility: vol,
	}
}
