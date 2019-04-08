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

package types

import (
	"bytes"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/lib/pq/oid"
)

// T is an instance of a SQL scalar, array, or tuple type. It describes the
// domain of possible values which a column can return, or to which an
// expression can evaluate. The type system does not differentiate between
// nullable and non-nullable types. It is up to the caller to store that
// information separately if it is needed. Here are some example types:
//
//   INT4                     - any 32-bit integer
//   DECIMAL(10, 3)           - any base-10 value with at most 10 digits, with
//                              up to 3 to right of decimal point
//   []FLOAT                  - array of 64-bit IEEE 754 floating-point values
//   TUPLE[TIME, VARCHAR(20)] - any pair of values where first value is a time
//                              of day and the second value is a string having
//                              up to 20 characters
//
// Fundamentally, a type consists of the following attributes, each of which has
// a corresponding accessor method. Some of these attributes are only defined
// for a subset of types. See the method comments for more details.
//
//   SemanticType    - equivalence group of the type (enumeration)
//   Oid             - Postgres Object ID that describes the type (enumeration)
//   Precision       - maximum accuracy of the type (numeric)
//   Width           - maximum size or scale of the type (numeric)
//   Locale          - location which governs sorting, formatting, etc. (string)
//   ArrayDimensions - array bounds ([]int)
//   ArrayContents   - array element type (T)
//   TupleContents   - slice of types of each tuple field ([]T)
//   TupleLabels     - slice of labels of each tuple field ([]string)
//
// Some types are not currently allowed as the type of a column (e.g. nested
// arrays). Other usages of the types package may have similar restrictions.
// Each such caller is responsible for enforcing their own restrictions; it's
// not the concern of the types package.
//
// Implementation-wise, types.T wraps a protobuf-generated InternalType struct.
// The generated protobuf code defines the struct fields, marshals/unmarshals
// them, formats a string representation, etc. Meanwhile, the wrapper types.T
// struct overrides the Marshal/Unmarshal methods in order to map to/from older
// persisted InternalType representations. For example, older versions of
// InternalType (previously called ColumnType) used a VisibleType field to
// represent INT2, whereas newer versions use Width/Oid. Unmarshal upgrades from
// this old format to the new, and Marshal downgrades, thus preserving backwards
// compatibility.
//
// Simple (unary) scalars types
// ----------------------------
//
// | SQL type          | Semantic Type  | Oid           | Precision | Width |
// |-------------------|----------------|---------------|-----------|-------|
// | NULL (unknown)    | UNKNOWN        | T_unknown     | 0         | 0     |
// | BOOL              | BOOL           | T_bool        | 0         | 0     |
// | DATE              | DATE           | T_date        | 0         | 0     |
// | TIMESTAMP         | TIMESTAMP      | T_timestamp   | 0         | 0     |
// | INTERVAL          | INTERVAL       | T_interval    | 0         | 0     |
// | TIMESTAMPTZ       | TIMESTAMPTZ    | T_timestamptz | 0         | 0     |
// | OID               | OID            | T_oid         | 0         | 0     |
// | UUID              | UUID           | T_uuid        | 0         | 0     |
// | INET              | INET           | T_inet        | 0         | 0     |
// | TIME              | TIME           | T_time        | 0         | 0     |
// | JSON              | JSONB          | T_jsonb       | 0         | 0     |
// | JSONB             | JSONB          | T_jsonb       | 0         | 0     |
// |                   |                |               |           |       |
// | BYTES             | BYTES          | T_bytea       | 0         | 0     |
// |                   |                |               |           |       |
// | STRING            | STRING         | T_text        | 0         | 0     |
// | STRING(N)         | STRING         | T_text        | 0         | N     |
// | VARCHAR           | STRING         | T_varchar     | 0         | 0     |
// | VARCHAR(N)        | STRING         | T_varchar     | 0         | N     |
// | CHAR              | STRING         | T_bpchar      | 0         | 1     |
// | CHAR(N)           | STRING         | T_bpchar      | 0         | N     |
// | "char"            | STRING         | T_char        | 0         | 0     |
// | NAME              | STRING         | T_name        | 0         | 0     |
// |                   |                |               |           |       |
// | STRING COLLATE en | COLLATEDSTRING | T_text        | 0         | 0     |
// | STRING(N) COL...  | COLLATEDSTRING | T_text        | 0         | N     |
// | VARCHAR COL...    | COLLATEDSTRING | T_varchar     | 0         | N     |
// | VARCHAR(N) COL... | COLLATEDSTRING | T_varchar     | 0         | N     |
// | CHAR COL...       | COLLATEDSTRING | T_bpchar      | 0         | 1     |
// | CHAR(N) COL...    | COLLATEDSTRING | T_bpchar      | 0         | N     |
// | "char" COL...     | COLLATEDSTRING | T_char        | 0         | 0     |
// |                   |                |               |           |       |
// | DECIMAL           | DECIMAL        | T_decimal     | 0         | 0     |
// | DECIMAL(N)        | DECIMAL        | T_decimal     | N         | 0     |
// | DECIMAL(N,M)      | DECIMAL        | T_decimal     | N         | M     |
// |                   |                |               |           |       |
// | FLOAT8            | FLOAT          | T_float8      | 0         | 0     |
// | FLOAT4            | FLOAT          | T_float4      | 0         | 0     |
// |                   |                |               |           |       |
// | BIT               | BIT            | T_bit         | 0         | 1     |
// | BIT(N)            | BIT            | T_bit         | 0         | N     |
// | VARBIT            | BIT            | T_varbit      | 0         | 0     |
// | VARBIT(N)         | BIT            | T_varbit      | 0         | N     |
// |                   |                |               |           |       |
// | INT,INTEGER       | INT            | T_int8        | 0         | 64    |
// | INT2,SMALLINT     | INT            | T_int2        | 0         | 16    |
// | INT4              | INT            | T_int4        | 0         | 32    |
// | INT8,INT64,BIGINT | INT            | T_int8        | 0         | 64    |
//
// TUPLE type
// ----------
//
// These cannot (yet) be used in tables but are used in DistSQL flow
// processors for queries that have tuple-typed intermediate results.
//
// | Field           | Description                                             |
// |-----------------|---------------------------------------------------------|
// | SemanticType    | TUPLE                                                   |
// | Oid             | T_record                                                |
// | TupleContents   | Contains tuple field types (can be recursively defined) |
// | TupleLabels     | Contains labels for each tuple field                    |
//
// ARRAY type
// ----------
//
// | Field           | Description                                             |
// |-----------------|---------------------------------------------------------|
// | SemanticType    | ARRAY                                                   |
// | Oid             | T__XXX (double underscores), where XXX is the Oid name  |
// |                 | of a scalar type                                        |
// | ArrayContents   | Type of array elements (scalar, array, or tuple)        |
//
// There are two special ARRAY types:
//
// | SQL type          | Semantic Type  | Oid           | ArrayContents |
// |-------------------|----------------|---------------|---------------|
// | INT2VECTOR        | ARRAY          | T_int2vector  | Int           |
// | OIDVECTOR         | ARRAY          | T_oidvector   | Oid           |
//
// When these types are themselves made into arrays, the Oids become T__int2vector and
// T__oidvector, respectively.
//
type T struct {
	// InternalType should never be directly referenced outside this package. The
	// only reason it is exported is because gogoproto panics when printing the
	// string representation of an unexported field. This is a problem when this
	// struct is embedded in a larger struct (like a ColumnDescriptor).
	InternalType InternalType
}

// Convenience list of pre-constructed types. Caller code can use any of these
// types, or use the MakeXXX methods to construct a custom type that is not
// listed here (e.g. if a custom width is needed).
var (
	// Unknown is the type of an expression that statically evaluates to NULL.
	// This type should never be returned for an expression that does not *always*
	// evaluate to NULL.
	Unknown = &T{InternalType: InternalType{
		SemanticType: UNKNOWN, Oid: oid.T_unknown, Locale: &emptyLocale}}

	// Bool is the type of a boolean true/false value.
	Bool = &T{InternalType: InternalType{
		SemanticType: BOOL, Oid: oid.T_bool, Locale: &emptyLocale}}

	// VarBit is the type of an ordered list of bits (0 or 1 valued), with no
	// specified limit on the count of bits.
	VarBit = &T{InternalType: InternalType{
		SemanticType: BIT, Oid: oid.T_varbit, Locale: &emptyLocale}}

	// Int is the type of a 64-bit signed integer. This is the canonical INT
	// semantic type for CRDB.
	Int = &T{InternalType: InternalType{
		SemanticType: INT, Width: 64, Oid: oid.T_int8, Locale: &emptyLocale}}

	// Int4 is the type of a 32-bit signed integer.
	Int4 = &T{InternalType: InternalType{
		SemanticType: INT, Width: 32, Oid: oid.T_int4, Locale: &emptyLocale}}

	// Int2 is the type of a 16-bit signed integer.
	Int2 = &T{InternalType: InternalType{
		SemanticType: INT, Width: 16, Oid: oid.T_int2, Locale: &emptyLocale}}

	// Float is the type of a 64-bit base-2 floating-point number (IEEE 754).
	// This is the canonical FLOAT semantic type for CRDB.
	Float = &T{InternalType: InternalType{
		SemanticType: FLOAT, Width: 64, Oid: oid.T_float8, Locale: &emptyLocale}}

	// Float4 is the type of a 32-bit base-2 floating-point number (IEEE 754).
	Float4 = &T{InternalType: InternalType{
		SemanticType: FLOAT, Width: 32, Oid: oid.T_float4, Locale: &emptyLocale}}

	// Decimal is the type of a base-10 floating-point number, with no specified
	// limit on precision (number of digits) or scale (digits to right of decimal
	// point).
	Decimal = &T{InternalType: InternalType{
		SemanticType: DECIMAL, Oid: oid.T_numeric, Locale: &emptyLocale}}

	// String is the type of a Unicode string, with no specified limit on the
	// count of characters. This is the canonical STRING semantic type for CRDB.
	// It is reported as STRING in SHOW CREATE but "text" in introspection for
	// compatibility with PostgreSQL.
	String = &T{InternalType: InternalType{
		SemanticType: STRING, Oid: oid.T_text, Locale: &emptyLocale}}

	// VarChar is equivalent to String, but has a differing OID (T_varchar),
	// which makes it show up differently when displayed. It is reported as
	// VARCHAR in SHOW CREATE and "character varying" in introspection for
	// compatibility with PostgreSQL.
	VarChar = &T{InternalType: InternalType{
		SemanticType: STRING, Oid: oid.T_varchar, Locale: &emptyLocale}}

	// Name is a type-alias for String with a different OID (T_name). It is
	// reported as NAME in SHOW CREATE and "name" in introspection for
	// compatibility with PostgreSQL.
	Name = &T{InternalType: InternalType{
		SemanticType: STRING, Oid: oid.T_name, Locale: &emptyLocale}}

	// Bytes is the type of a list of raw byte values.
	Bytes = &T{InternalType: InternalType{
		SemanticType: BYTES, Oid: oid.T_bytea, Locale: &emptyLocale}}

	// Date is the type of a value specifying year, month, day (with no time
	// component). There is no timezone associated with it. For example:
	//
	//   YYYY-MM-DD
	//
	Date = &T{InternalType: InternalType{
		SemanticType: DATE, Oid: oid.T_date, Locale: &emptyLocale}}

	// Time is the type of a value specifying hour, minute, second (with no date
	// component). By default, it has microsecond precision. There is no timezone
	// associated with it. For example:
	//
	//   HH:MM:SS.ssssss
	//
	Time = &T{InternalType: InternalType{
		SemanticType: TIME, Oid: oid.T_time, Locale: &emptyLocale}}

	// Timestamp is the type of a value specifying year, month, day, hour, minute,
	// and second, but with no associated timezone. By default, it has microsecond
	// precision. For example:
	//
	//   YYYY-MM-DD HH:MM:SS.ssssss
	//
	Timestamp = &T{InternalType: InternalType{
		SemanticType: TIMESTAMP, Oid: oid.T_timestamp, Locale: &emptyLocale}}

	// TimestampTZ is the type of a value specifying year, month, day, hour,
	// minute, and second, as well as an associated timezone. By default, it has
	// microsecond precision. For example:
	//
	//   YYYY-MM-DD HH:MM:SS.ssssss+-ZZ:ZZ
	//
	TimestampTZ = &T{InternalType: InternalType{
		SemanticType: TIMESTAMPTZ, Oid: oid.T_timestamptz, Locale: &emptyLocale}}

	// Interval is the type of a value describing a duration of time. By default,
	// it has microsecond precision.
	Interval = &T{InternalType: InternalType{
		SemanticType: INTERVAL, Oid: oid.T_interval, Locale: &emptyLocale}}

	// Jsonb is the type of a JavaScript Object Notation (JSON) value that is
	// stored in a decomposed binary format (hence the "b" in jsonb).
	Jsonb = &T{InternalType: InternalType{
		SemanticType: JSON, Oid: oid.T_jsonb, Locale: &emptyLocale}}

	// Uuid is the type of a universally unique identifier (UUID), which is a
	// 128-bit quantity that is very unlikely to ever be generated again, and so
	// can be relied on to be distinct from all other UUID values.
	Uuid = &T{InternalType: InternalType{
		SemanticType: UUID, Oid: oid.T_uuid, Locale: &emptyLocale}}

	// INet is the type of an IPv4 or IPv6 network address. For example:
	//
	//   192.168.100.128/25
	//   FE80:CD00:0:CDE:1257:0:211E:729C
	//
	INet = &T{InternalType: InternalType{
		SemanticType: INET, Oid: oid.T_inet, Locale: &emptyLocale}}

	// Scalar contains all types that meet this criteria:
	//
	//   1. Scalar type (no ARRAY or TUPLE types).
	//   2. Non-ambiguous type (no UNKNOWN or ANY types).
	//   3. Canonical type for one of the semantic types (e.g. for INT, STRING,
	//      DECIMAL, etc).
	//
	Scalar = []*T{
		Bool,
		Int,
		Float,
		Decimal,
		Date,
		Timestamp,
		Interval,
		String,
		Bytes,
		TimestampTZ,
		Oid,
		Uuid,
		INet,
		Time,
		Jsonb,
		VarBit,
	}

	// Any is a special type used only during static analysis as a wildcard type
	// that matches any other type, including scalar, array, and tuple types.
	// Execution-time values should never have this type. As an example of its
	// use, many SQL builtin functions allow an input value to be of any type,
	// and so use this type in their static definitions.
	Any = &T{InternalType: InternalType{
		SemanticType: ANY, Oid: oid.T_anyelement, Locale: &emptyLocale}}

	// AnyArray is a special type used only during static analysis as a wildcard
	// type that matches an array having elements of any (uniform) type (including
	// ARRAY). Execution-time values should never have this type.
	AnyArray = &T{InternalType: InternalType{
		SemanticType: ARRAY, ArrayContents: Any, Oid: oid.T_anyarray, Locale: &emptyLocale}}

	// AnyTuple is a special type used only during static analysis as a wildcard
	// type that matches a tuple with any number of fields of any type (including
	// TUPLE). Execution-time values should never have this type.
	AnyTuple = &T{InternalType: InternalType{
		SemanticType: TUPLE, Oid: oid.T_record, Locale: &emptyLocale}}

	// AnyCollatedString is a special type used only during static analysis as a
	// wildcard type that matches a collated string with any locale. Execution-
	// time values should never have this type.
	AnyCollatedString = &T{InternalType: InternalType{
		SemanticType: COLLATEDSTRING, Oid: oid.T_text, Locale: &emptyLocale}}

	// StringArray is the type of an ARRAY value having String-typed elements.
	StringArray = &T{InternalType: InternalType{
		SemanticType: ARRAY, ArrayContents: String, Oid: oid.T__text, Locale: &emptyLocale}}

	// IntArray is the type of an ARRAY value having Int-typed elements.
	IntArray = &T{InternalType: InternalType{
		SemanticType: ARRAY, ArrayContents: Int, Oid: oid.T__int8, Locale: &emptyLocale}}

	// DecimalArray is the type of an ARRAY value having Decimal-typed elements.
	DecimalArray = &T{InternalType: InternalType{
		SemanticType: ARRAY, ArrayContents: Decimal, Oid: oid.T__numeric, Locale: &emptyLocale}}

	// Int2Vector is a type-alias for an array of Int2 values with a different
	// OID (T_int2vector instead of T__int2). It is a special VECTOR type used
	// by Postgres in system tables.
	Int2Vector = &T{InternalType: InternalType{
		SemanticType: ARRAY, Oid: oid.T_int2vector, ArrayContents: Int2, Locale: &emptyLocale}}
)

// Unexported wrapper types.
var (
	// typeBit is the SQL BIT type. It is not exported to avoid confusion with
	// the VarBit type, and confusion over whether its default Width is
	// unspecified or is 1. More commonly used instead is the VarBit type.
	typeBit = &T{InternalType: InternalType{
		SemanticType: BIT, Oid: oid.T_bit, Locale: &emptyLocale}}

	// typeBpChar is the "standard SQL" string type of fixed length, where "bp"
	// stands for "blank padded". It is not exported to avoid confusion with
	// typeQChar, as well as confusion over its default width.
	//
	// It is reported as CHAR in SHOW CREATE and "character" in introspection for
	// compatibility with PostgreSQL.
	//
	// Its default maximum with is 1. It always has a maximum width.
	typeBpChar = &T{InternalType: InternalType{
		SemanticType: STRING, Oid: oid.T_bpchar, Locale: &emptyLocale}}

	// typeQChar is a special PostgreSQL-only type supported for compatibility.
	// It behaves like VARCHAR, its maximum width cannot be modified, and has a
	// peculiar name in the syntax and introspection. It is not exported to avoid
	// confusion with typeBpChar, as well as confusion over its default width.
	//
	// It is reported as "char" (with double quotes included) in SHOW CREATE and
	// "char" in introspection for compatibility with PostgreSQL.
	typeQChar = &T{InternalType: InternalType{
		SemanticType: STRING, Oid: oid.T_char, Locale: &emptyLocale}}
)

const (
	// Deprecated after 19.1, since it's now represented using the Oid field.
	name SemanticType = 11

	// Deprecated after 19.1, since it's now represented using the Oid field.
	int2vector SemanticType = 200

	// Deprecated after 19.1, since it's now represented using the Oid field.
	oidvector SemanticType = 201

	visibleNONE = 0

	// Deprecated after 2.1, since it's no longer used.
	visibleINTEGER = 1

	// Deprecated after 2.1, since it's now represented using the Width field.
	visibleSMALLINT = 2

	// Deprecated after 2.1, since it's now represented using the Width field.
	visibleBIGINT = 3

	// Deprecated after 2.0, since the original BIT representation was buggy.
	visibleBIT = 4

	// Deprecated after 19.1, since it's now represented using the Width field.
	visibleREAL = 5

	// Deprecated after 2.1, since it's now represented using the Width field.
	visibleDOUBLE = 6

	// Deprecated after 19.1, since it's now represented using the Oid field.
	visibleVARCHAR = 7

	// Deprecated after 19.1, since it's now represented using the Oid field.
	visibleCHAR = 8

	// Deprecated after 19.1, since it's now represented using the Oid field.
	visibleQCHAR = 9

	// Deprecated after 19.1, since it's now represented using the Oid field.
	visibleVARBIT = 10
)

var (
	emptyLocale = ""
)

// MakeScalar constructs a new instance of a scalar type (i.e. not ARRAY or
// TUPLE types) using the provided fields.
func MakeScalar(semTyp SemanticType, o oid.Oid, precision, width int32, locale string) *T {
	t := OidToType[o]
	if semTyp != t.SemanticType() {
		if semTyp != COLLATEDSTRING || STRING != t.SemanticType() {
			panic(pgerror.NewAssertionErrorf(
				"oid %s does not match SemanticType %s", oid.TypeName[o], semTyp))
		}
	}
	if semTyp == ARRAY || semTyp == TUPLE {
		panic(pgerror.NewAssertionErrorf("cannot make non-scalar type %s", semTyp))
	}
	if semTyp != COLLATEDSTRING && locale != "" {
		panic(pgerror.NewAssertionErrorf("non-collation type cannot have locale %s", locale))
	}

	if precision < 0 {
		panic(pgerror.NewAssertionErrorf("negative precision is not allowed"))
	}
	switch semTyp {
	case DECIMAL, TIME, TIMESTAMP, TIMESTAMPTZ:
	default:
		if precision != 0 {
			panic(pgerror.NewAssertionErrorf("type %s cannot have precision", semTyp))
		}
	}

	if width < 0 {
		panic(pgerror.NewAssertionErrorf("negative width is not allowed"))
	}
	switch semTyp {
	case INT:
		switch width {
		case 16, 32, 64:
		default:
			panic(pgerror.NewAssertionErrorf("invalid width %d for INT type", width))
		}
	case FLOAT:
		switch width {
		case 32, 64:
		default:
			panic(pgerror.NewAssertionErrorf("invalid width %d for FLOAT type", width))
		}
	case DECIMAL:
		if width > precision {
			panic(pgerror.NewAssertionErrorf(
				"decimal scale %d cannot be larger than precision %d", width, precision))
		}
	case STRING, BYTES, COLLATEDSTRING, BIT:
		// These types can have any width.
	default:
		if width != 0 {
			panic(pgerror.NewAssertionErrorf("type %s cannot have width", semTyp))
		}
	}

	return &T{InternalType: InternalType{
		SemanticType: semTyp,
		Oid:          o,
		Precision:    precision,
		Width:        width,
		Locale:       &locale,
	}}
}

// MakeBit constructs a new instance of the BIT semantic type (oid = T_bit)
// having the given max # bits (0 = unspecified number).
func MakeBit(width int32) *T {
	if width == 0 {
		return typeBit
	}
	if width < 0 {
		panic(pgerror.NewAssertionErrorf("width %d cannot be negative", width))
	}
	return &T{InternalType: InternalType{
		SemanticType: BIT, Oid: oid.T_bit, Width: width, Locale: &emptyLocale}}
}

// MakeVarBit constructs a new instance of the BIT type (oid = T_varbit) having
// the given max # bits (0 = unspecified number).
func MakeVarBit(width int32) *T {
	if width == 0 {
		return VarBit
	}
	if width < 0 {
		panic(pgerror.NewAssertionErrorf("width %d cannot be negative", width))
	}
	return &T{InternalType: InternalType{
		SemanticType: BIT, Width: width, Oid: oid.T_varbit, Locale: &emptyLocale}}
}

// MakeString constructs a new instance of the STRING type (oid = T_text) having
// the given max # characters (0 = unspecified number).
func MakeString(width int32) *T {
	if width == 0 {
		return String
	}
	if width < 0 {
		panic(pgerror.NewAssertionErrorf("width %d cannot be negative", width))
	}
	return &T{InternalType: InternalType{
		SemanticType: STRING, Oid: oid.T_text, Width: width, Locale: &emptyLocale}}
}

// MakeVarChar constructs a new instance of the VARCHAR type (oid = T_varchar)
// having the given max # characters (0 = unspecified number).
func MakeVarChar(width int32) *T {
	if width == 0 {
		return VarChar
	}
	if width < 0 {
		panic(pgerror.NewAssertionErrorf("width %d cannot be negative", width))
	}
	return &T{InternalType: InternalType{
		SemanticType: STRING, Oid: oid.T_varchar, Width: width, Locale: &emptyLocale}}
}

// MakeChar constructs a new instance of the CHAR type (oid = T_bpchar) having
// the given max # characters (0 = unspecified number).
func MakeChar(width int32) *T {
	if width == 0 {
		return typeBpChar
	}
	if width < 0 {
		panic(pgerror.NewAssertionErrorf("width %d cannot be negative", width))
	}
	return &T{InternalType: InternalType{
		SemanticType: STRING, Oid: oid.T_bpchar, Width: width, Locale: &emptyLocale}}
}

// MakeQChar constructs a new instance of the "char" type (oid = T_char) having
// the given max # characters (0 = unspecified number).
func MakeQChar(width int32) *T {
	if width == 0 {
		return typeQChar
	}
	return &T{InternalType: InternalType{
		SemanticType: STRING, Oid: oid.T_char, Width: width, Locale: &emptyLocale}}
}

// MakeCollatedString constructs a new instance of a COLLATEDSTRING type that is
// collated according to the given locale. The new type is based upon the given
// string type, having the same oid and width values. For example:
//
//   STRING      => STRING COLLATE EN
//   VARCHAR(20) => VARCHAR(20) COLLATE EN
//
func MakeCollatedString(strType *T, locale string) *T {
	switch strType.Oid() {
	case oid.T_text, oid.T_varchar, oid.T_bpchar, oid.T_char:
		return &T{InternalType: InternalType{
			SemanticType: COLLATEDSTRING, Oid: strType.Oid(), Width: strType.Width(), Locale: &locale}}
	}
	panic(pgerror.NewAssertionErrorf("cannot apply collation to non-string type: %s", strType))
}

// MakeDecimal constructs a new instance of a DECIMAL type (oid = T_numeric)
// that has at most "precision" # of decimal digits (0 = unspecified number of
// digits) and at most "scale" # of decimal digits after the decimal point
// (0 = unspecified number of digits). scale must be <= precision.
func MakeDecimal(precision, scale int32) *T {
	if precision == 0 && scale == 0 {
		return Decimal
	}
	if precision < 0 {
		panic(pgerror.NewAssertionErrorf("precision %d cannot be negative", precision))
	}
	if scale < 0 {
		panic(pgerror.NewAssertionErrorf("scale %d cannot be negative", scale))
	}
	if scale > precision {
		panic(pgerror.NewAssertionErrorf(
			"scale %d cannot be larger than precision %d", scale, precision))
	}
	return &T{InternalType: InternalType{
		SemanticType: DECIMAL,
		Oid:          oid.T_numeric,
		Precision:    precision,
		Width:        scale,
		Locale:       &emptyLocale,
	}}
}

// MakeTime constructs a new instance of a TIME type (oid = T_time) that has at
// most the given number of fractional second digits.
func MakeTime(precision int32) *T {
	if precision == 0 {
		return Time
	}
	if precision != 6 {
		panic(pgerror.NewAssertionErrorf("precision %d is not currently supported", precision))
	}
	return &T{InternalType: InternalType{
		SemanticType: TIME, Oid: oid.T_time, Precision: precision, Locale: &emptyLocale}}
}

// MakeTimestamp constructs a new instance of a TIMESTAMP type that has at most
// the given number of fractional second digits.
func MakeTimestamp(precision int32) *T {
	if precision == 0 {
		return Timestamp
	}
	if precision != 6 {
		panic(pgerror.NewAssertionErrorf("precision %d is not currently supported", precision))
	}
	return &T{InternalType: InternalType{
		SemanticType: TIMESTAMP, Oid: oid.T_timestamp, Precision: precision, Locale: &emptyLocale}}
}

// MakeTimestampTZ constructs a new instance of a TIMESTAMPTZ type that has at
// most the given number of fractional second digits.
func MakeTimestampTZ(precision int32) *T {
	if precision == 0 {
		return TimestampTZ
	}
	if precision != 6 {
		panic(pgerror.NewAssertionErrorf("precision %d is not currently supported", precision))
	}
	return &T{InternalType: InternalType{
		SemanticType: TIMESTAMPTZ, Oid: oid.T_timestamptz, Precision: precision, Locale: &emptyLocale}}
}

// MakeArray constructs a new instance of an ARRAY type with the given element
// type (which may itself be an ARRAY type).
func MakeArray(typ *T) *T {
	return &T{InternalType: InternalType{
		SemanticType:  ARRAY,
		Oid:           oidToArrayOid[typ.Oid()],
		ArrayContents: typ,
		Locale:        &emptyLocale,
	}}
}

// MakeTuple constructs a new instance of a TUPLE type with the given field
// types (some/all of which may be other TUPLE types).
func MakeTuple(contents []T) *T {
	return &T{InternalType: InternalType{
		SemanticType: TUPLE, Oid: oid.T_record, TupleContents: contents, Locale: &emptyLocale}}
}

// MakeLabeledTuple constructs a new instance of a TUPLE type with the given
// field types and labels.
func MakeLabeledTuple(contents []T, labels []string) *T {
	if len(contents) != len(labels) && labels != nil {
		panic(pgerror.NewAssertionErrorf(
			"TUPLE contents and labels must be of same length: %v, %v", contents, labels))
	}
	return &T{InternalType: InternalType{
		SemanticType:  TUPLE,
		Oid:           oid.T_record,
		TupleContents: contents,
		TupleLabels:   labels,
		Locale:        &emptyLocale,
	}}
}

// SemanticType specifies a group of types that are compatible with one another.
// Types having the same semantic type can be compared, assigned, etc., but may
// differ from one another in width, precision, locale, and other attributes.
// For example, it is always an error to insert an INT value into a FLOAT
// column, because they do not share a semantic type. However, values of
// different types within the same group are "insert-compatible" with one
// another. Insertion may still result in an error because of width overflow or
// other constraints, but it can at least be attempted.
//
// Semantic types are convenient for performing type switches on types, because
// in most cases it is the type group that matters, not the specific type. For
// example, when CRDB encodes values, it maintains routines for each semantic
// type, since types in the same group encode in very similar ways.
//
// Most semantic types have an associated "canonical type" that is the default
// representative of that group, and which is a superset of all other types in
// that group. Values with other types (in the same group) can always be
// trivially converted to the canonical type with no loss of information. For
// example, the canonical type for INT is Int, which is a 64-bit integer. Both
// 32-bit and 16-bit integers can be trivially converted to it.
//
// Execution operators and functions are permissive in terms of input (allow any
// type within a given group), and typically return only values having
// canonical types as output. For example, the INT Plus operator allows values
// having any INT type as input. But then it will always convert those values to
// 64-bit integers, and return a final 64-bit INT value (types.Int). Doing this
// vastly reduces the required number of operator overloads.
func (t *T) SemanticType() SemanticType {
	return t.InternalType.SemanticType
}

// Oid returns the type's Postgres Object ID. The OID identifies the type more
// specifically than the SemanticType, and is used by the Postgres wire protocol
// various Postgres catalog tables, functions like pg_typeof, etc. Maintaining
// the OID is required for Postgres-compatibility.
func (t *T) Oid() oid.Oid {
	return t.InternalType.Oid
}

// Locale identifies a specific geographical, political, or cultural region that
// impacts various character-based operations such as sorting, pattern matching,
// and builtin functions like lower and upper. It is only defined for the
// COLLATEDSTRING semantic type, and is the empty string for all other types.
func (t *T) Locale() string {
	return *t.InternalType.Locale
}

// Width is the size or scale of the type, such as number of bits or characters.
//
//   INT           : # of bits (64, 32, 16)
//   FLOAT         : # of bits (64, 32)
//   DECIMAL       : max # of digits after decimal point (must be <= Precision)
//   STRING        : max # of characters
//   COLLATEDSTRING: max # of characters
//   BIT           : max # of bits
//
// Width is always 0 for other types.
func (t *T) Width() int32 {
	return t.InternalType.Width
}

// Precision is the accuracy of the data type.
//
//   DECIMAL    : max # digits (must be >= Width/Scale)
//   TIME       : max # fractional second digits
//   TIMESTAMP  : max # fractional second digits
//   TIMESTAMPTZ: max # fractional second digits
//
// Precision is always 0 for other types.
func (t *T) Precision() int32 {
	return t.InternalType.Precision
}

// Scale is an alias method for Width, used for clarity when the semantic type
// is DECIMAL.
func (t *T) Scale() int32 {
	return t.InternalType.Width
}

// ArrayContents returns the type of array elements. This is nil for non-ARRAY
// types.
func (t *T) ArrayContents() *T {
	return t.InternalType.ArrayContents
}

// TupleContents returns a slice containing the type of each tuple field. This
// is nil for non-TUPLE types.
func (t *T) TupleContents() []T {
	return t.InternalType.TupleContents
}

// TupleLabels returns a slice containing the labels of each tuple field. This
// is nil for non-TUPLE types, or if the TUPLE type does not specify labels.
func (t *T) TupleLabels() []string {
	return t.InternalType.TupleLabels
}

// Name returns a single word description of the type that describes it
// succinctly, but without all the details, such as width, locale, etc. The name
// is sometimes the same as the name returned by SQLStandardName, but is more
// CRDB friendly.
//
// TODO(andyk): Should these be changed to be the same as SQLStandardName?
func (t *T) Name() string {
	switch t.SemanticType() {
	case ANY:
		return "anyelement"
	case ARRAY:
		switch t.Oid() {
		case oid.T_oidvector:
			return "oidvector"
		case oid.T_int2vector:
			return "int2vector"
		}
		return t.ArrayContents().Name() + "[]"
	case BIT:
		if t.Oid() == oid.T_varbit {
			return "varbit"
		}
		return "bit"
	case BOOL:
		return "bool"
	case BYTES:
		return "bytes"
	case DATE:
		return "date"
	case DECIMAL:
		return "decimal"
	case FLOAT:
		switch t.Width() {
		case 64:
			return "float"
		case 32:
			return "float4"
		default:
			panic(pgerror.NewAssertionErrorf("programming error: unknown float width: %d", t.Width()))
		}
	case INET:
		return "inet"
	case INT:
		switch t.Width() {
		case 64:
			return "int"
		case 32:
			return "int4"
		case 16:
			return "int2"
		default:
			panic(pgerror.NewAssertionErrorf("programming error: unknown int width: %d", t.Width()))
		}
	case INTERVAL:
		return "interval"
	case JSON:
		return "jsonb"
	case OID:
		return t.SQLStandardName()
	case STRING, COLLATEDSTRING:
		switch t.Oid() {
		case oid.T_text:
			return "string"
		case oid.T_bpchar:
			return "char"
		case oid.T_char:
			// Yes, that's the name. The ways of PostgreSQL are inscrutable.
			return `"char"`
		case oid.T_varchar:
			return "varchar"
		case oid.T_name:
			return "name"
		}
		panic(pgerror.NewAssertionErrorf("unexpected OID: %d", t.Oid()))
	case TIME:
		return "time"
	case TIMESTAMP:
		return "timestamp"
	case TIMESTAMPTZ:
		return "timestamptz"
	case TUPLE:
		// TUPLE is currently an anonymous type, with no name.
		return ""
	case UNKNOWN:
		return "unknown"
	case UUID:
		return "uuid"
	default:
		panic(pgerror.NewAssertionErrorf("unexpected SemanticType: %s", t.SemanticType()))
	}
}

// SQLStandardName returns the type's name as it is specified in the SQL
// standard (or by Postgres for any non-standard types). This can be looked up
// for any type in Postgres using a query similar to this:
//
//   SELECT format_type(pg_typeof(1::int)::regtype, NULL)
//
func (t *T) SQLStandardName() string {
	switch t.SemanticType() {
	case ANY:
		return "anyelement"
	case ARRAY:
		switch t.Oid() {
		case oid.T_oidvector:
			return "oidvector"
		case oid.T_int2vector:
			return "int2vector"
		}
		return t.ArrayContents().SQLStandardName() + "[]"
	case BIT:
		if t.Oid() == oid.T_varbit {
			return "bit varying"
		}
		return "bit"
	case BOOL:
		return "boolean"
	case BYTES:
		return "bytea"
	case DATE:
		return "date"
	case DECIMAL:
		return "numeric"
	case FLOAT:
		switch t.Width() {
		case 32:
			return "real"
		case 64:
			return "double precision"
		default:
			panic(pgerror.NewAssertionErrorf("programming error: unknown float width: %d", t.Width()))
		}
	case INET:
		return "inet"
	case INT:
		switch t.Width() {
		case 16:
			return "smallint"
		case 32:
			// PG shows "integer" for int4.
			return "integer"
		case 64:
			return "bigint"
		default:
			panic(pgerror.NewAssertionErrorf("programming error: unknown int width: %d", t.Width()))
		}
	case INTERVAL:
		return "interval"
	case JSON:
		// Only binary JSON is currently supported.
		return "jsonb"
	case OID:
		switch t.Oid() {
		case oid.T_oid:
			return "oid"
		case oid.T_regclass:
			return "regclass"
		case oid.T_regnamespace:
			return "regnamespace"
		case oid.T_regproc:
			return "regproc"
		case oid.T_regprocedure:
			return "regprocedure"
		case oid.T_regtype:
			return "regtype"
		default:
			panic(pgerror.NewAssertionErrorf("unexpected Oid: %v", log.Safe(t.Oid())))
		}
	case STRING, COLLATEDSTRING:
		switch t.Oid() {
		case oid.T_text:
			return "text"
		case oid.T_varchar:
			return "character varying"
		case oid.T_bpchar:
			return "character"
		case oid.T_char:
			// Not the same as "character". Beware.
			return `"char"`
		case oid.T_name:
			return "name"
		}
		panic(pgerror.NewAssertionErrorf("unexpected OID: %d", t.Oid()))
	case TIME:
		return "time without time zone"
	case TIMESTAMP:
		return "timestamp without time zone"
	case TIMESTAMPTZ:
		return "timestamp with time zone"
	case TUPLE:
		return "record"
	case UNKNOWN:
		return "unknown"
	case UUID:
		return "uuid"
	default:
		panic(pgerror.NewAssertionErrorf("unexpected SemanticType: %v", log.Safe(t.SemanticType())))
	}
}

// InformationSchemaName returns the string suitable to populate the data_type
// column of information_schema.columns.
//
// This is different from SQLString() in that it must report SQL standard names
// that are compatible with PostgreSQL client expectations.
func (t *T) InformationSchemaName() string {
	// This is the same as SQLStandardName, except for the case of arrays.
	if t.SemanticType() == ARRAY {
		return "ARRAY"
	}
	return t.SQLStandardName()
}

// SQLString returns the CockroachDB native SQL string that can be used to
// reproduce the type via parsing the string as a type. It is used in error
// messages and also to produce the output of SHOW CREATE.
func (t *T) SQLString() string {
	switch t.SemanticType() {
	case BIT:
		o := t.Oid()
		typName := "BIT"
		if o == oid.T_varbit {
			typName = "VARBIT"
		}
		// BIT(1) pretty-prints as just BIT.
		if (o != oid.T_varbit && t.Width() > 1) ||
			(o == oid.T_varbit && t.Width() > 0) {
			typName = fmt.Sprintf("%s(%d)", typName, t.Width())
		}
		return typName
	case INT:
		switch t.Width() {
		case 16:
			return "INT2"
		case 32:
			return "INT4"
		case 64:
			return "INT8"
		default:
			panic(pgerror.NewAssertionErrorf("programming error: unknown int width: %d", t.Width()))
		}
	case STRING:
		return t.stringTypeSQL()
	case COLLATEDSTRING:
		return t.collatedStringTypeSQL(false /* isArray */)
	case FLOAT:
		const realName = "FLOAT4"
		const doubleName = "FLOAT8"
		if t.Width() == 32 {
			return realName
		}
		return doubleName
	case DECIMAL:
		if t.Precision() > 0 {
			if t.Width() > 0 {
				return fmt.Sprintf("%s(%d,%d)", t.SemanticType().String(), t.Precision(), t.Scale())
			}
			return fmt.Sprintf("%s(%d)", t.SemanticType().String(), t.Precision())
		}
	case JSON:
		// Only binary JSON is currently supported.
		return "JSONB"
	case TIME, TIMESTAMP, TIMESTAMPTZ:
		if t.Precision() > 0 {
			return fmt.Sprintf("%s(%d)", t.SemanticType().String(), t.Precision())
		}
	case OID:
		if name, ok := oid.TypeName[t.Oid()]; ok {
			return name
		}
	case ARRAY:
		switch t.Oid() {
		case oid.T_oidvector:
			return "OIDVECTOR"
		case oid.T_int2vector:
			return "INT2VECTOR"
		}
		if t.ArrayContents().SemanticType() == COLLATEDSTRING {
			return t.ArrayContents().collatedStringTypeSQL(true /* isArray */)
		}
		return t.ArrayContents().SQLString() + "[]"
	}
	return t.SemanticType().String()
}

// Equivalent returns true if this type is "equivalent" to the given type.
// Equivalent types are compatible with one another: they can be compared,
// assigned, and unioned. Equivalent types must always have the same semantic
// type for the root type and any descendant types (i.e. in case of ARRAY or
// TUPLE). COLLATEDSTRING types must have the same locale. But other attributes
// of equivalent types, such as width, precision, and oid, can be different.
//
// Wildcard types (e.g. Any, AnyArray, AnyTuple, etc) have special equivalence
// behavior. The ANY semantic type matches any other type, including ANY. And a
// wildcard collation (empty string) matches any other collation.
func (t *T) Equivalent(other *T) bool {
	if t.SemanticType() == ANY || other.SemanticType() == ANY {
		return true
	}
	if t.SemanticType() != other.SemanticType() {
		return false
	}

	switch t.SemanticType() {
	case COLLATEDSTRING:
		if t.Locale() != "" && other.Locale() != "" && t.Locale() != other.Locale() {
			return false
		}

	case TUPLE:
		if t.TupleContents() == nil || other.TupleContents() == nil {
			// Tuples that aren't fully specified (have a nil subtype list) are
			// always equivalent to other tuples, to allow overloads to specify
			// that they take an arbitrary tuple type.
			return true
		}
		if len(t.TupleContents()) != len(other.TupleContents()) {
			return false
		}
		for i := range t.TupleContents() {
			if !t.TupleContents()[i].Equivalent(&other.TupleContents()[i]) {
				return false
			}
		}

	case ARRAY:
		if !t.ArrayContents().Equivalent(other.ArrayContents()) {
			return false
		}
	}

	return true
}

// Identical returns true if every field in this ColumnType is exactly the same
// as every corresponding field in the given ColumnType. Identical performs a
// deep comparison, traversing any Tuple or Array contents.
//
// NOTE: Consider whether the desired semantics really require identical types,
// or if Equivalent is the right method to call instead.
func (t *T) Identical(other *T) bool {
	return t.InternalType.Identical(&other.InternalType)
}

// Size returns the size, in bytes, of this type once it has been marshaled to
// a byte buffer. This is typically called to determine the size of the buffer
// that needs to be allocated before calling Marshal.
//
// Marshal is part of the protoutil.Message interface.
func (t *T) Size() (n int) {
	// Need to first downgrade the type before delegating to InternalType,
	// because Marshal will downgrade.
	temp := *t
	err := temp.downgradeType()
	if err != nil {
		panic(pgerror.NewAssertionErrorf("error during Size call: %v", err))
	}
	return temp.InternalType.Size()
}

// Identical is the internal implementation for T.Identical. See that comment
// for details.
func (t *InternalType) Identical(other *InternalType) bool {
	if t.SemanticType != other.SemanticType {
		return false
	}
	if t.Width != other.Width {
		return false
	}
	if t.Precision != other.Precision {
		return false
	}
	if t.Locale != nil && other.Locale != nil {
		if *t.Locale != *other.Locale {
			return false
		}
	} else if t.Locale != nil {
		return false
	} else if other.Locale != nil {
		return false
	}
	if t.VisibleType != other.VisibleType {
		return false
	}
	if t.ArrayContents != nil && other.ArrayContents != nil {
		if !t.ArrayContents.Identical(other.ArrayContents) {
			return false
		}
	} else if t.ArrayContents != nil {
		return false
	} else if other.ArrayContents != nil {
		return false
	}
	if len(t.TupleContents) != len(other.TupleContents) {
		return false
	}
	for i := range t.TupleContents {
		if !t.TupleContents[i].Identical(&other.TupleContents[i]) {
			return false
		}
	}
	if len(t.TupleLabels) != len(other.TupleLabels) {
		return false
	}
	for i := range t.TupleLabels {
		if t.TupleLabels[i] != other.TupleLabels[i] {
			return false
		}
	}
	return t.Oid == other.Oid
}

// ProtoMessage is the protobuf marker method. It is part of the
// protoutil.Message interface.
func (t *T) ProtoMessage() {}

// Reset clears the type instance. It is part of the protoutil.Message
// interface.
func (t *T) Reset() {
	*t = T{}
}

// Unmarshal deserializes a type from the given byte representation using gogo
// protobuf serialization rules. It is backwards-compatible with formats used
// by older versions of CRDB.
//
//   var t T
//   err := protoutil.Unmarshal(data, &t)
//
// Unmarshal is part of the protoutil.Message interface.
func (t *T) Unmarshal(data []byte) error {
	// Unmarshal the internal type, and then perform an upgrade step to convert
	// to the latest format.
	err := protoutil.Unmarshal(data, &t.InternalType)
	if err != nil {
		return err
	}
	return t.upgradeType()
}

// upgradeType assumes its input was just unmarshaled from bytes that may have
// been serialized by any previous version of CRDB. It upgrades the object
// according to the requirements of the latest version by remapping fields and
// setting required values. This is necessary to preserve backwards-
// compatibility with older formats (e.g. restoring database from old backup).
func (t *T) upgradeType() error {
	switch t.SemanticType() {
	case INT:
		// Check VisibleType field that was populated in previous versions.
		switch t.InternalType.VisibleType {
		case visibleSMALLINT:
			t.InternalType.Width = 16
			t.InternalType.Oid = oid.T_int2
		case visibleINTEGER:
			t.InternalType.Width = 32
			t.InternalType.Oid = oid.T_int4
		case visibleBIGINT:
			t.InternalType.Width = 64
			t.InternalType.Oid = oid.T_int8
		case visibleBIT, visibleNONE:
			// Pre-2.1 BIT was using semantic type INT with arbitrary widths. Clamp
			// them to fixed/known widths. See #34161.
			switch t.Width() {
			case 16:
				t.InternalType.Oid = oid.T_int2
			case 32:
				t.InternalType.Oid = oid.T_int4
			default:
				// Assume INT8 if width is 0 or not valid.
				t.InternalType.Oid = oid.T_int8
				t.InternalType.Width = 64
			}
		default:
			return pgerror.NewAssertionErrorf("unexpected visible type: %d", t.InternalType.VisibleType)
		}

	case FLOAT:
		// Map visible REAL type to 32-bit width.
		switch t.InternalType.VisibleType {
		case visibleREAL:
			t.InternalType.Oid = oid.T_float4
			t.InternalType.Width = 32
		case visibleDOUBLE:
			t.InternalType.Oid = oid.T_float8
			t.InternalType.Width = 64
		case visibleNONE:
			switch t.Width() {
			case 32:
				t.InternalType.Oid = oid.T_float4
			case 64:
				t.InternalType.Oid = oid.T_float8
			default:
				// Pre-2.1 (before Width) there were 3 cases:
				// - VisibleType = DOUBLE PRECISION, Width = 0 -> now clearly FLOAT8
				// - VisibleType = NONE, Width = 0 -> now clearly FLOAT8
				// - VisibleType = NONE, Precision > 0 -> we need to derive the width.
				if t.Precision() >= 1 && t.Precision() <= 24 {
					t.InternalType.Oid = oid.T_float4
					t.InternalType.Width = 32
				} else {
					t.InternalType.Oid = oid.T_float8
					t.InternalType.Width = 64
				}
			}
		default:
			return pgerror.NewAssertionErrorf("unexpected visible type: %d", t.InternalType.VisibleType)
		}

		// Precision should always be set to 0 going forward.
		t.InternalType.Precision = 0

	case STRING, COLLATEDSTRING:
		// Map string-related visible types to corresponding Oid values.
		switch t.InternalType.VisibleType {
		case visibleVARCHAR:
			t.InternalType.Oid = oid.T_varchar
		case visibleCHAR:
			t.InternalType.Oid = oid.T_bpchar
		case visibleQCHAR:
			t.InternalType.Oid = oid.T_char
		case visibleNONE:
			t.InternalType.Oid = oid.T_text
		default:
			return pgerror.NewAssertionErrorf("unexpected visible type: %d", t.InternalType.VisibleType)
		}
		if t.InternalType.SemanticType == STRING {
			if t.InternalType.Locale != nil && len(*t.InternalType.Locale) != 0 {
				return pgerror.NewAssertionErrorf(
					"STRING type should not have locale: %s", *t.InternalType.Locale)
			}
		}

	case BIT:
		// Map visible VARBIT type to T_varbit OID value.
		switch t.InternalType.VisibleType {
		case visibleVARBIT:
			t.InternalType.Oid = oid.T_varbit
		case visibleNONE:
			t.InternalType.Oid = oid.T_bit
		default:
			return pgerror.NewAssertionErrorf("unexpected visible type: %d", t.InternalType.VisibleType)
		}

	case ARRAY:
		// If this is ARRAY of ARRAY (nested array), then no need to upgrade,
		// since that's only supported starting with 19.2.
		if t.ArrayContents() == nil || t.ArrayContents().SemanticType() != ARRAY {
			if t.ArrayContents() == nil {
				// This ARRAY type was serialized by a previous version of CRDB,
				// so construct the array contents from scratch.
				arrayContents := *t
				arrayContents.InternalType.SemanticType = *t.InternalType.ArrayElemType
				arrayContents.InternalType.ArrayDimensions = nil
				arrayContents.InternalType.ArrayElemType = nil
				err := arrayContents.upgradeType()
				if err != nil {
					return err
				}
				t.InternalType.ArrayContents = &arrayContents
				t.InternalType.Oid = oidToArrayOid[arrayContents.Oid()]
			}

			// Zero out fields that may have been used to store information about
			// the array element type, or which are no longer in use.
			t.InternalType.Width = 0
			t.InternalType.Precision = 0
			t.InternalType.Locale = &emptyLocale
			t.InternalType.VisibleType = 0
			t.InternalType.ArrayElemType = nil
			t.InternalType.ArrayDimensions = nil
		}

	case int2vector:
		t.InternalType.SemanticType = ARRAY
		t.InternalType.Width = 0
		t.InternalType.Oid = oid.T_int2vector
		t.InternalType.ArrayContents = Int2

	case oidvector:
		t.InternalType.SemanticType = ARRAY
		t.InternalType.Oid = oid.T_oidvector
		t.InternalType.ArrayContents = Oid

	case name:
		t.InternalType.SemanticType = STRING
		t.InternalType.Oid = oid.T_name
		if t.Width() != 0 {
			return pgerror.NewAssertionErrorf("name type cannot have non-zero width: %d", t.Width())
		}

	default:
		if t.InternalType.Oid == 0 {
			t.InternalType.Oid = semanticTypeToOid[t.SemanticType()]
		}
	}

	// Clear the deprecated visible types, since they are now handled by the
	// Width or Oid fields.
	t.InternalType.VisibleType = 0

	// If locale is not set, always set it to the empty string, in order to avoid
	// bothersome deref errors when the Locale method is called.
	if t.InternalType.Locale == nil {
		t.InternalType.Locale = &emptyLocale
	}

	return nil
}

// Marshal serializes a type into a byte representation using gogo protobuf
// serialization rules. It returns the resulting bytes as a slice. The bytes
// are serialized in a format that is backwards-compatible with the previous
// version of CRDB so that clusters can run in mixed version mode during
// upgrade.
//
//   bytes, err := protoutil.Marshal(&typ)
//
func (t *T) Marshal() (data []byte, err error) {
	// First downgrade to a struct that will be serialized in a backwards-
	// compatible bytes format.
	temp := *t
	err = temp.downgradeType()
	if err != nil {
		return nil, err
	}
	return protoutil.Marshal(&temp.InternalType)
}

// MarshalTo behaves like Marshal, except that it deserializes to an existing
// byte slice and returns the number of bytes written to it. The slice must
// already have sufficient capacity. Callers can use the Size method to
// determine how much capacity needs to be allocated.
//
// Marshal is part of the protoutil.Message interface.
func (t *T) MarshalTo(data []byte) (int, error) {
	temp := *t
	err := temp.downgradeType()
	if err != nil {
		return 0, err
	}
	return temp.InternalType.MarshalTo(data)
}

// downgradeType assumes the type's fields are set according to the requirements
// of the latest CRDB version. It updates the fields so that they will be
// marshaled into a format that is compatible with the previous version of
// CRDB. This is necessary to preserve backwards-compatibility in mixed-version
// scenarios, such as during upgrade.
func (t *T) downgradeType() error {
	// Set SemanticType and VisibleType for 19.1 backwards-compatibility.
	switch t.SemanticType() {
	case BIT:
		if t.Oid() == oid.T_varbit {
			t.InternalType.VisibleType = visibleVARBIT
		}

	case FLOAT:
		switch t.Width() {
		case 32:
			t.InternalType.VisibleType = visibleREAL
		}

	case STRING, COLLATEDSTRING:
		switch t.Oid() {
		case oid.T_text:
			// Nothing to do.
		case oid.T_varchar:
			t.InternalType.VisibleType = visibleVARCHAR
		case oid.T_bpchar:
			t.InternalType.VisibleType = visibleCHAR
		case oid.T_char:
			t.InternalType.VisibleType = visibleQCHAR
		case oid.T_name:
			t.InternalType.SemanticType = name
		default:
			return pgerror.NewAssertionErrorf("unexpected Oid: %d", t.Oid())
		}

	case ARRAY:
		// If the array is nested, then no need to change the representation, since
		// support for nested arrays in types.T is new for 19.2. Otherwise,
		// downgrade to ARRAY representation used before 19.2, in which the array
		// type fields specified the width, locale, etc. of the element type.
		if t.ArrayContents().SemanticType() != ARRAY {
			temp := *t.InternalType.ArrayContents
			err := temp.downgradeType()
			if err != nil {
				return err
			}
			t.InternalType.Width = temp.InternalType.Width
			t.InternalType.Precision = temp.InternalType.Precision
			t.InternalType.Locale = temp.InternalType.Locale
			t.InternalType.VisibleType = temp.InternalType.VisibleType
			t.InternalType.ArrayElemType = &t.InternalType.ArrayContents.InternalType.SemanticType

			switch t.Oid() {
			case oid.T_int2vector:
				t.InternalType.SemanticType = int2vector
			case oid.T_oidvector:
				t.InternalType.SemanticType = oidvector
			}
		}
	}

	// Map empty locale to nil.
	if t.InternalType.Locale != nil && len(*t.InternalType.Locale) == 0 {
		t.InternalType.Locale = nil
	}

	return nil
}

// String returns the name of the type, similar to the Name method. However, it
// expands COLLATEDSTRING, ARRAY, and TUPLE types to be more descriptive.
//
// TODO(andyk): It'd be nice to have this return SqlString() method output,
// since that is more descriptive.
func (t *T) String() string {
	switch t.SemanticType() {
	case COLLATEDSTRING:
		if t.Locale() == "" {
			// Used in telemetry.
			return fmt.Sprintf("collated%s{*}", t.Name())
		}
		return fmt.Sprintf("collated%s{%s}", t.Name(), t.Locale())

	case ARRAY:
		switch t.Oid() {
		case oid.T_oidvector, oid.T_int2vector:
			return t.Name()
		}
		return t.ArrayContents().String() + "[]"

	case TUPLE:
		var buf bytes.Buffer
		buf.WriteString("tuple")
		if len(t.TupleContents()) != 0 {
			buf.WriteByte('{')
			for i, typ := range t.TupleContents() {
				if i != 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(typ.String())
				if t.TupleLabels() != nil {
					buf.WriteString(" AS ")
					buf.WriteString(t.InternalType.TupleLabels[i])
				}
			}
			buf.WriteByte('}')
		}
		return buf.String()
	}
	return t.Name()
}

// DebugString returns a detailed dump of the type protobuf struct, suitable for
// debugging scenarios.
func (t *T) DebugString() string {
	return t.InternalType.String()
}

// IsAmbiguous returns true if this type is UNKNOWN or one of the ANY variants.
// Instances of ambiguous types can be NULL or have one of several different
// semantic types. This is important for parameterized types to determine
// whether they are fully concrete or not.
func (t *T) IsAmbiguous() bool {
	switch t.SemanticType() {
	case UNKNOWN, ANY:
		return true
	case COLLATEDSTRING:
		return t.Locale() == ""
	case TUPLE:
		if len(t.TupleContents()) == 0 {
			return true
		}
		for i := range t.TupleContents() {
			if t.TupleContents()[i].IsAmbiguous() {
				return true
			}
		}
		return false
	case ARRAY:
		return t.ArrayContents().IsAmbiguous()
	}
	return false
}

// IsStringType returns true iff the given type is String or a collated string
// type.
func IsStringType(t *T) bool {
	switch t.SemanticType() {
	case STRING, COLLATEDSTRING:
		return true
	default:
		return false
	}
}

// IsValidArrayElementType returns true if the given type can be used as the
// element type of an ARRAY-typed column. If the valid return is false, the
// issue number should be included in the error report to inform the user.
func IsValidArrayElementType(t *T) (valid bool, issueNum int) {
	switch t.SemanticType() {
	case JSON:
		return false, 23468
	default:
		return true, 0
	}
}

// IsDateTimeType returns true if the given type is a date or time-related type.
func IsDateTimeType(t *T) bool {
	switch t.SemanticType() {
	case DATE:
		return true
	case TIME:
		return true
	case TIMESTAMP:
		return true
	case TIMESTAMPTZ:
		return true
	case INTERVAL:
		return true
	default:
		return false
	}
}

// IsAdditiveType returns true if the given type supports addition and
// subtraction.
func IsAdditiveType(t *T) bool {
	switch t.SemanticType() {
	case INT:
		return true
	case FLOAT:
		return true
	case DECIMAL:
		return true
	default:
		return IsDateTimeType(t)
	}
}

// collatedStringTypeSQL returns the string representation of a COLLATEDSTRING
// or []COLLATEDSTRING type. This is tricky in the case of an array of collated
// string, since brackets must precede the COLLATE identifier:
//
//   STRING COLLATE EN
//   VARCHAR(20)[] COLLATE DE
//
func (t *T) collatedStringTypeSQL(isArray bool) string {
	var buf bytes.Buffer
	buf.WriteString(t.stringTypeSQL())
	if isArray {
		buf.WriteString("[] COLLATE ")
	} else {
		buf.WriteString(" COLLATE ")
	}
	lex.EncodeUnrestrictedSQLIdent(&buf, t.Locale(), lex.EncNoFlags)
	return buf.String()
}

// stringTypeSQL returns the visible type name plus any width specifier for the
// STRING/COLLATEDSTRING type.
func (t *T) stringTypeSQL() string {
	typName := "STRING"
	switch t.Oid() {
	case oid.T_varchar:
		typName = "VARCHAR"
	case oid.T_bpchar:
		typName = "CHAR"
	case oid.T_char:
		// Yes, that's the name. The ways of PostgreSQL are inscrutable.
		typName = `"char"`
	case oid.T_name:
		typName = "NAME"
	}

	// In general, if there is a specified width we want to print it next to the
	// type. However, in the specific case of CHAR and "char", the default is 1
	// and the width should be omitted in that case.
	if t.Width() > 0 {
		o := t.Oid()
		if t.Width() != 1 || (o != oid.T_bpchar && o != oid.T_char) {
			typName = fmt.Sprintf("%s(%d)", typName, t.Width())
		}
	}

	return typName
}

var typNameLiterals map[string]*T

func init() {
	typNameLiterals = make(map[string]*T)
	for o, t := range OidToType {
		name := strings.ToLower(oid.TypeName[o])
		if _, ok := typNameLiterals[name]; !ok {
			typNameLiterals[name] = t
		}
	}
}

// TypeForNonKeywordTypeName returns the column type for the string name of a
// type, if one exists. The third return value indicates:
//
//   0 if no error or the type is not known in postgres.
//   -1 if the type is known in postgres.
//  >0 for a github issue number.
func TypeForNonKeywordTypeName(name string) (*T, bool, int) {
	t, ok := typNameLiterals[name]
	if ok {
		return t, ok, 0
	}
	return nil, false, postgresPredefinedTypeIssues[name]
}

// The following map must include all types predefined in PostgreSQL
// that are also not yet defined in CockroachDB and link them to
// github issues. It is also possible, but not necessary, to include
// PostgreSQL types that are already implemented in CockroachDB.
var postgresPredefinedTypeIssues = map[string]int{
	"box":           21286,
	"cidr":          18846,
	"circle":        21286,
	"line":          21286,
	"lseg":          21286,
	"macaddr":       -1,
	"macaddr8":      -1,
	"money":         -1,
	"path":          21286,
	"pg_lsn":        -1,
	"point":         21286,
	"polygon":       21286,
	"tsquery":       7821,
	"tsvector":      7821,
	"txid_snapshot": -1,
	"xml":           -1,
}
