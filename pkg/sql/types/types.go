// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package types

import (
	"bytes"
	"fmt"
	"regexp"
	"runtime/debug"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/oidext"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/errorutil/unimplemented"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/gogo/protobuf/proto"
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
//   FLOAT[]                  - array of 64-bit IEEE 754 floating-point values
//   TUPLE[TIME, VARCHAR(20)] - any pair of values where first value is a time
//                              of day and the second value is a string having
//                              up to 20 characters
//
// Fundamentally, a type consists of the following attributes, each of which has
// a corresponding accessor method. Some of these attributes are only defined
// for a subset of types. See the method comments for more details.
//
//   Family        - equivalence group of the type (enumeration)
//   Oid           - Postgres Object ID that describes the type (enumeration)
//   Precision     - maximum accuracy of the type (numeric)
//   Width         - maximum size or scale of the type (numeric)
//   Locale        - location which governs sorting, formatting, etc. (string)
//   ArrayContents - array element type (T)
//   TupleContents - slice of types of each tuple field ([]*T)
//   TupleLabels   - slice of labels of each tuple field ([]string)
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
// | SQL type          | Family         | Oid           | Precision | Width |
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
// | TIMETZ            | TIMETZ         | T_timetz      | 0         | 0     |
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
// Tuple types
// -----------
//
// These cannot (yet) be used in tables but are used in DistSQL flow
// processors for queries that have tuple-typed intermediate results.
//
// | Field           | Description                                             |
// |-----------------|---------------------------------------------------------|
// | Family          | TupleFamily                                             |
// | Oid             | T_record                                                |
// | TupleContents   | Contains tuple field types (can be recursively defined) |
// | TupleLabels     | Contains labels for each tuple field                    |
//
// Array types
// -----------
//
// | Field           | Description                                             |
// |-----------------|---------------------------------------------------------|
// | Family          | ArrayFamily                                             |
// | Oid             | T__XXX (double underscores), where XXX is the Oid name  |
// |                 | of a scalar type                                        |
// | ArrayContents   | Type of array elements (scalar, array, or tuple)        |
//
// There are two special ARRAY types:
//
// | SQL type          | Family         | Oid           | ArrayContents |
// |-------------------|----------------|---------------|---------------|
// | INT2VECTOR        | ARRAY          | T_int2vector  | Int           |
// | OIDVECTOR         | ARRAY          | T_oidvector   | Oid           |
//
// When these types are themselves made into arrays, the Oids become T__int2vector and
// T__oidvector, respectively.
//
// User defined types
// ------------------
//
// * Enums
// | Field         | Description                                |
// |---------------|--------------------------------------------|
// | Family        | EnumFamily                                 |
// | Oid           | A unique OID generated upon enum creation  |
//
// See types.proto for the corresponding proto definition. Its automatic
// type declaration is suppressed in the proto so that it is possible to
// add additional fields to T without serializing them.
type T struct {
	// InternalType should never be directly referenced outside this package. The
	// only reason it is exported is because gogoproto panics when printing the
	// string representation of an unexported field. This is a problem when this
	// struct is embedded in a larger struct (like a ColumnDescriptor).
	InternalType InternalType

	// Fields that are only populated when hydrating from a user defined
	// type descriptor. It is assumed that a user defined type is only used
	// once its metadata has been hydrated through the process of type resolution.
	TypeMeta UserDefinedTypeMetadata
}

// UserDefinedTypeMetadata contains metadata needed for runtime
// operations on user defined types. The metadata must be read only.
type UserDefinedTypeMetadata struct {
	// Name is the resolved name of this type.
	Name *UserDefinedTypeName

	// Version is the descriptor version of the descriptor used to construct
	// this version of the type metadata.
	Version uint32

	// enumData is non-nil iff the metadata is for an ENUM type.
	EnumData *EnumMetadata
}

// EnumMetadata is metadata about an ENUM needed for evaluation.
type EnumMetadata struct {
	// PhysicalRepresentations is a slice of the byte array
	// physical representations of enum members.
	PhysicalRepresentations [][]byte
	// LogicalRepresentations is a slice of the string logical
	// representations of enum members.
	LogicalRepresentations []string
	// IsMemberReadOnly holds whether the enum member at index i is
	// read only or not.
	IsMemberReadOnly []bool
	// TODO (rohany): For small enums, having a map would be slower
	//  than just an array. Investigate at what point the tradeoff
	//  should occur, if at all.
}

func (e *EnumMetadata) debugString() string {
	return fmt.Sprintf(
		"PhysicalReps: %v; LogicalReps: %s",
		e.PhysicalRepresentations,
		e.LogicalRepresentations,
	)
}

// UserDefinedTypeName is a struct representing a qualified user defined
// type name. We redefine a common struct from higher level packages. We
// do so because proto will panic if any members of a proto struct are
// private. Rather than expose private members of higher level packages,
// we define a separate type here to be safe.
type UserDefinedTypeName struct {
	Catalog        string
	ExplicitSchema bool
	Schema         string
	Name           string
}

// Basename returns the unqualified name.
func (u UserDefinedTypeName) Basename() string {
	return u.Name
}

// FQName returns the fully qualified name.
func (u UserDefinedTypeName) FQName() string {
	var sb strings.Builder
	// Note that cross-database type references are disabled, so we only
	// format the qualified name with the schema.
	if u.ExplicitSchema {
		sb.WriteString(u.Schema)
		sb.WriteString(".")
	}
	sb.WriteString(u.Name)
	return sb.String()
}

// Convenience list of pre-constructed types. Caller code can use any of these
// types, or use the MakeXXX methods to construct a custom type that is not
// listed here (e.g. if a custom width is needed).
var (
	// Unknown is the type of an expression that statically evaluates to NULL.
	// This type should never be returned for an expression that does not *always*
	// evaluate to NULL.
	Unknown = &T{InternalType: InternalType{
		Family: UnknownFamily, Oid: oid.T_unknown, Locale: &emptyLocale}}

	// Bool is the type of a boolean true/false value.
	Bool = &T{InternalType: InternalType{
		Family: BoolFamily, Oid: oid.T_bool, Locale: &emptyLocale}}

	// VarBit is the type of an ordered list of bits (0 or 1 valued), with no
	// specified limit on the count of bits.
	VarBit = &T{InternalType: InternalType{
		Family: BitFamily, Oid: oid.T_varbit, Locale: &emptyLocale}}

	// Int is the type of a 64-bit signed integer. This is the canonical type
	// for IntFamily.
	Int = &T{InternalType: InternalType{
		Family: IntFamily, Width: 64, Oid: oid.T_int8, Locale: &emptyLocale}}

	// Int4 is the type of a 32-bit signed integer.
	Int4 = &T{InternalType: InternalType{
		Family: IntFamily, Width: 32, Oid: oid.T_int4, Locale: &emptyLocale}}

	// Int2 is the type of a 16-bit signed integer.
	Int2 = &T{InternalType: InternalType{
		Family: IntFamily, Width: 16, Oid: oid.T_int2, Locale: &emptyLocale}}

	// Float is the type of a 64-bit base-2 floating-point number (IEEE 754).
	// This is the canonical type for FloatFamily.
	Float = &T{InternalType: InternalType{
		Family: FloatFamily, Width: 64, Oid: oid.T_float8, Locale: &emptyLocale}}

	// Float4 is the type of a 32-bit base-2 floating-point number (IEEE 754).
	Float4 = &T{InternalType: InternalType{
		Family: FloatFamily, Width: 32, Oid: oid.T_float4, Locale: &emptyLocale}}

	// Decimal is the type of a base-10 floating-point number, with no specified
	// limit on precision (number of digits) or scale (digits to right of decimal
	// point).
	Decimal = &T{InternalType: InternalType{
		Family: DecimalFamily, Oid: oid.T_numeric, Locale: &emptyLocale}}

	// String is the type of a Unicode string, with no specified limit on the
	// count of characters. This is the canonical type for StringFamily. It is
	// reported as STRING in SHOW CREATE but "text" in introspection for
	// compatibility with PostgreSQL.
	String = &T{InternalType: InternalType{
		Family: StringFamily, Oid: oid.T_text, Locale: &emptyLocale}}

	// VarChar is equivalent to String, but has a differing OID (T_varchar),
	// which makes it show up differently when displayed. It is reported as
	// VARCHAR in SHOW CREATE and "character varying" in introspection for
	// compatibility with PostgreSQL.
	VarChar = &T{InternalType: InternalType{
		Family: StringFamily, Oid: oid.T_varchar, Locale: &emptyLocale}}

	// QChar is the special "char" type that is a single-character column type.
	// It's used by system tables. It is reported as "char" (with double quotes
	// included) in SHOW CREATE and "char" in introspection for compatibility
	// with PostgreSQL.
	//
	// See https://www.postgresql.org/docs/9.1/static/datatype-character.html
	QChar = &T{InternalType: InternalType{
		Family: StringFamily, Width: 1, Oid: oid.T_char, Locale: &emptyLocale}}

	// Name is a type-alias for String with a different OID (T_name). It is
	// reported as NAME in SHOW CREATE and "name" in introspection for
	// compatibility with PostgreSQL.
	Name = &T{InternalType: InternalType{
		Family: StringFamily, Oid: oid.T_name, Locale: &emptyLocale}}

	// Bytes is the type of a list of raw byte values.
	Bytes = &T{InternalType: InternalType{
		Family: BytesFamily, Oid: oid.T_bytea, Locale: &emptyLocale}}

	// Date is the type of a value specifying year, month, day (with no time
	// component). There is no timezone associated with it. For example:
	//
	//   YYYY-MM-DD
	//
	Date = &T{InternalType: InternalType{
		Family: DateFamily, Oid: oid.T_date, Locale: &emptyLocale}}

	// Time is the type of a value specifying hour, minute, second (with no date
	// component). By default, it has microsecond precision. There is no timezone
	// associated with it. For example:
	//
	//   HH:MM:SS.ssssss
	//
	Time = &T{InternalType: InternalType{
		Family:             TimeFamily,
		Precision:          0,
		TimePrecisionIsSet: false,
		Oid:                oid.T_time,
		Locale:             &emptyLocale,
	}}

	// TimeTZ is the type specifying hour, minute, second and timezone with
	// no date component. By default, it has microsecond precision.
	// For example:
	//
	//   HH:MM:SS.ssssss+-ZZ:ZZ
	TimeTZ = &T{InternalType: InternalType{
		Family:             TimeTZFamily,
		Precision:          0,
		TimePrecisionIsSet: false,
		Oid:                oid.T_timetz,
		Locale:             &emptyLocale,
	}}

	// Timestamp is the type of a value specifying year, month, day, hour, minute,
	// and second, but with no associated timezone. By default, it has microsecond
	// precision. For example:
	//
	//   YYYY-MM-DD HH:MM:SS.ssssss
	//
	Timestamp = &T{InternalType: InternalType{
		Family:             TimestampFamily,
		Precision:          0,
		TimePrecisionIsSet: false,
		Oid:                oid.T_timestamp,
		Locale:             &emptyLocale,
	}}

	// TimestampTZ is the type of a value specifying year, month, day, hour,
	// minute, and second, as well as an associated timezone. By default, it has
	// microsecond precision. For example:
	//
	//   YYYY-MM-DD HH:MM:SS.ssssss+-ZZ:ZZ
	//
	TimestampTZ = &T{InternalType: InternalType{
		Family:             TimestampTZFamily,
		Precision:          0,
		TimePrecisionIsSet: false,
		Oid:                oid.T_timestamptz,
		Locale:             &emptyLocale,
	}}

	// Interval is the type of a value describing a duration of time. By default,
	// it has microsecond precision.
	Interval = &T{InternalType: InternalType{
		Family:                IntervalFamily,
		Precision:             0,
		TimePrecisionIsSet:    false,
		Oid:                   oid.T_interval,
		Locale:                &emptyLocale,
		IntervalDurationField: &IntervalDurationField{},
	}}

	// Jsonb is the type of a JavaScript Object Notation (JSON) value that is
	// stored in a decomposed binary format (hence the "b" in jsonb).
	Jsonb = &T{InternalType: InternalType{
		Family: JsonFamily, Oid: oid.T_jsonb, Locale: &emptyLocale}}

	// Uuid is the type of a universally unique identifier (UUID), which is a
	// 128-bit quantity that is very unlikely to ever be generated again, and so
	// can be relied on to be distinct from all other UUID values.
	Uuid = &T{InternalType: InternalType{
		Family: UuidFamily, Oid: oid.T_uuid, Locale: &emptyLocale}}

	// INet is the type of an IPv4 or IPv6 network address. For example:
	//
	//   192.168.100.128/25
	//   FE80:CD00:0:CDE:1257:0:211E:729C
	//
	INet = &T{InternalType: InternalType{
		Family: INetFamily, Oid: oid.T_inet, Locale: &emptyLocale}}

	// Geometry is the type of a geospatial Geometry object.
	Geometry = &T{
		InternalType: InternalType{
			Family:      GeometryFamily,
			Oid:         oidext.T_geometry,
			Locale:      &emptyLocale,
			GeoMetadata: &GeoMetadata{},
		},
	}

	// Geography is the type of a geospatial Geography object.
	Geography = &T{
		InternalType: InternalType{
			Family:      GeographyFamily,
			Oid:         oidext.T_geography,
			Locale:      &emptyLocale,
			GeoMetadata: &GeoMetadata{},
		},
	}

	// Box2D is the type of a geospatial box2d object.
	Box2D = &T{
		InternalType: InternalType{
			Family: Box2DFamily,
			Oid:    oidext.T_box2d,
			Locale: &emptyLocale,
		},
	}

	// Void is the type representing void.
	Void = &T{
		InternalType: InternalType{
			Family: VoidFamily,
			Oid:    oid.T_void,
			Locale: &emptyLocale,
		},
	}

	// Scalar contains all types that meet this criteria:
	//
	//   1. Scalar type (no ArrayFamily or TupleFamily types).
	//   2. Non-ambiguous type (no UnknownFamily or AnyFamily types).
	//   3. Canonical type for one of the type families.
	//
	Scalar = []*T{
		Bool,
		Box2D,
		Int,
		Float,
		Decimal,
		Date,
		Timestamp,
		Interval,
		Geography,
		Geometry,
		String,
		Bytes,
		TimestampTZ,
		Oid,
		Uuid,
		INet,
		Time,
		TimeTZ,
		Jsonb,
		VarBit,
	}

	// Any is a special type used only during static analysis as a wildcard type
	// that matches any other type, including scalar, array, and tuple types.
	// Execution-time values should never have this type. As an example of its
	// use, many SQL builtin functions allow an input value to be of any type,
	// and so use this type in their static definitions.
	Any = &T{InternalType: InternalType{
		Family: AnyFamily, Oid: oid.T_anyelement, Locale: &emptyLocale}}

	// AnyArray is a special type used only during static analysis as a wildcard
	// type that matches an array having elements of any (uniform) type (including
	// nested array types). Execution-time values should never have this type.
	AnyArray = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: Any, Oid: oid.T_anyarray, Locale: &emptyLocale}}

	// AnyEnum is a special type only used during static analysis as a wildcard
	// type that matches an possible enum value. Execution-time values should
	// never have this type.
	AnyEnum = &T{InternalType: InternalType{
		Family: EnumFamily, Locale: &emptyLocale, Oid: oid.T_anyenum}}

	// AnyTuple is a special type used only during static analysis as a wildcard
	// type that matches a tuple with any number of fields of any type (including
	// tuple types). Execution-time values should never have this type.
	AnyTuple = &T{InternalType: InternalType{
		Family: TupleFamily, TupleContents: []*T{Any}, Oid: oid.T_record, Locale: &emptyLocale}}

	// AnyTupleArray is a special type used only during static analysis as a wildcard
	// type that matches an array of tuples with any number of fields of any type (including
	// tuple types). Execution-time values should never have this type.
	AnyTupleArray = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: AnyTuple, Oid: oid.T__record, Locale: &emptyLocale}}

	// AnyCollatedString is a special type used only during static analysis as a
	// wildcard type that matches a collated string with any locale. Execution-
	// time values should never have this type.
	AnyCollatedString = &T{InternalType: InternalType{
		Family: CollatedStringFamily, Oid: oid.T_text, Locale: &emptyLocale}}

	// EmptyTuple is the tuple type with no fields. Note that this is different
	// than AnyTuple, which is a wildcard type.
	EmptyTuple = &T{InternalType: InternalType{
		Family: TupleFamily, Oid: oid.T_record, Locale: &emptyLocale}}

	// StringArray is the type of an array value having String-typed elements.
	StringArray = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: String, Oid: oid.T__text, Locale: &emptyLocale}}

	// BytesArray is the type of an array value having Byte-typed elements.
	BytesArray = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: Bytes, Oid: oid.T__bytea, Locale: &emptyLocale}}

	// IntArray is the type of an array value having Int-typed elements.
	IntArray = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: Int, Oid: oid.T__int8, Locale: &emptyLocale}}

	// FloatArray is the type of an array value having Float-typed elements.
	FloatArray = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: Float, Oid: oid.T__float8, Locale: &emptyLocale}}

	// DecimalArray is the type of an array value having Decimal-typed elements.
	DecimalArray = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: Decimal, Oid: oid.T__numeric, Locale: &emptyLocale}}

	// BoolArray is the type of an array value having Bool-typed elements.
	BoolArray = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: Bool, Oid: oid.T__bool, Locale: &emptyLocale}}

	// UUIDArray is the type of an array value having UUID-typed elements.
	UUIDArray = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: Uuid, Oid: oid.T__uuid, Locale: &emptyLocale}}

	// TimeArray is the type of an array value having Date-typed elements.
	DateArray = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: Date, Oid: oid.T__date, Locale: &emptyLocale}}

	// TimeArray is the type of an array value having Time-typed elements.
	TimeArray = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: Time, Oid: oid.T__time, Locale: &emptyLocale}}

	// TimeTZArray is the type of an array value having TimeTZ-typed elements.
	TimeTZArray = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: TimeTZ, Oid: oid.T__timetz, Locale: &emptyLocale}}

	// TimestampArray is the type of an array value having Timestamp-typed elements.
	TimestampArray = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: Timestamp, Oid: oid.T__timestamp, Locale: &emptyLocale}}

	// TimestampTZArray is the type of an array value having TimestampTZ-typed elements.
	TimestampTZArray = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: TimestampTZ, Oid: oid.T__timestamptz, Locale: &emptyLocale}}

	// IntervalArray is the type of an array value having Interval-typed elements.
	IntervalArray = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: Interval, Oid: oid.T__interval, Locale: &emptyLocale}}

	// INetArray is the type of an array value having INet-typed elements.
	INetArray = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: INet, Oid: oid.T__inet, Locale: &emptyLocale}}

	// VarBitArray is the type of an array value having VarBit-typed elements.
	VarBitArray = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: VarBit, Oid: oid.T__varbit, Locale: &emptyLocale}}

	// AnyEnumArray is the type of an array value having AnyEnum-typed elements.
	AnyEnumArray = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: AnyEnum, Oid: oid.T_anyarray, Locale: &emptyLocale}}

	// JSONArray is the type of an array value having JSON-typed elements.
	JSONArray = &T{InternalType: InternalType{
		Family: ArrayFamily, ArrayContents: Jsonb, Oid: oid.T__jsonb, Locale: &emptyLocale}}

	// Int2Vector is a type-alias for an array of Int2 values with a different
	// OID (T_int2vector instead of T__int2). It is a special VECTOR type used
	// by Postgres in system tables. Int2vectors are 0-indexed, unlike normal arrays.
	Int2Vector = &T{InternalType: InternalType{
		Family: ArrayFamily, Oid: oid.T_int2vector, ArrayContents: Int2, Locale: &emptyLocale}}
)

// Unexported wrapper types.
var (
	// typeBit is the SQL BIT type. It is not exported to avoid confusion with
	// the VarBit type, and confusion over whether its default Width is
	// unspecified or is 1. More commonly used instead is the VarBit type.
	typeBit = &T{InternalType: InternalType{
		Family: BitFamily, Oid: oid.T_bit, Locale: &emptyLocale}}

	// typeBpChar is a CHAR type with an unspecified width. "bp" stands for
	// "blank padded". It is not exported to avoid confusion with QChar, as well
	// as confusion over CHAR's default width of 1.
	//
	// It is reported as CHAR in SHOW CREATE and "character" in introspection for
	// compatibility with PostgreSQL.
	typeBpChar = &T{InternalType: InternalType{
		Family: StringFamily, Oid: oid.T_bpchar, Locale: &emptyLocale}}

	// typeQChar is a "char" type with an unspecified width. It is not exported
	// to avoid confusion with QChar. The "char" type should always have a width
	// of one. A "char" type with an unspecified width is only used when the
	// length of a "char" value cannot be determined, for example a placeholder
	// typed as a "char" should have an unspecified width.
	typeQChar = &T{InternalType: InternalType{
		Family: StringFamily, Oid: oid.T_char, Locale: &emptyLocale}}
)

const (
	// Deprecated after 19.1, since it's now represented using the Oid field.
	name Family = 11

	// Deprecated after 19.1, since it's now represented using the Oid field.
	int2vector Family = 200

	// Deprecated after 19.1, since it's now represented using the Oid field.
	oidvector Family = 201

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

	// OID returned for the unknown[] array type. PG has no OID for this case.
	unknownArrayOid = 0
)

const (
	// defaultTimePrecision is the default precision to return for time families
	// if time is not set.
	defaultTimePrecision = 6
)

var (
	emptyLocale = ""
)

// MakeScalar constructs a new instance of a scalar type (i.e. not array or
// tuple types) using the provided fields.
func MakeScalar(family Family, o oid.Oid, precision, width int32, locale string) *T {
	t := OidToType[o]
	if family != t.Family() {
		if family != CollatedStringFamily || StringFamily != t.Family() {
			panic(errors.AssertionFailedf("oid %d does not match %s", o, family))
		}
	}
	if family == ArrayFamily || family == TupleFamily {
		panic(errors.AssertionFailedf("cannot make non-scalar type %s", family))
	}
	if family != CollatedStringFamily && locale != "" {
		panic(errors.AssertionFailedf("non-collation type cannot have locale %s", locale))
	}

	timePrecisionIsSet := false
	var intervalDurationField *IntervalDurationField
	var geoMetadata *GeoMetadata
	switch family {
	case IntervalFamily:
		intervalDurationField = &IntervalDurationField{}
		if precision < 0 || precision > 6 {
			panic(errors.AssertionFailedf("precision must be between 0 and 6 inclusive"))
		}
		timePrecisionIsSet = true
	case TimestampFamily, TimestampTZFamily, TimeFamily, TimeTZFamily:
		if precision < 0 || precision > 6 {
			panic(errors.AssertionFailedf("precision must be between 0 and 6 inclusive"))
		}
		timePrecisionIsSet = true
	case DecimalFamily:
		if precision < 0 {
			panic(errors.AssertionFailedf("negative precision is not allowed"))
		}
	default:
		if precision != 0 {
			panic(errors.AssertionFailedf("type %s cannot have precision", family))
		}
	}

	if width < 0 {
		panic(errors.AssertionFailedf("negative width is not allowed"))
	}
	switch family {
	case IntFamily:
		switch width {
		case 16, 32, 64:
		default:
			panic(errors.AssertionFailedf("invalid width %d for IntFamily type", width))
		}
	case FloatFamily:
		switch width {
		case 32, 64:
		default:
			panic(errors.AssertionFailedf("invalid width %d for FloatFamily type", width))
		}
	case DecimalFamily:
		if width > precision {
			panic(errors.AssertionFailedf(
				"decimal scale %d cannot be larger than precision %d", width, precision))
		}
	case StringFamily, BytesFamily, CollatedStringFamily, BitFamily:
		// These types can have any width.
	case GeometryFamily:
		geoMetadata = &GeoMetadata{}
	case GeographyFamily:
		geoMetadata = &GeoMetadata{}
	default:
		if width != 0 {
			panic(errors.AssertionFailedf("type %s cannot have width", family))
		}
	}

	return &T{InternalType: InternalType{
		Family:                family,
		Oid:                   o,
		Precision:             precision,
		TimePrecisionIsSet:    timePrecisionIsSet,
		Width:                 width,
		Locale:                &locale,
		IntervalDurationField: intervalDurationField,
		GeoMetadata:           geoMetadata,
	}}
}

// MakeBit constructs a new instance of the BIT type (oid = T_bit) having the
// given max # bits (0 = unspecified number).
func MakeBit(width int32) *T {
	if width == 0 {
		return typeBit
	}
	if width < 0 {
		panic(errors.AssertionFailedf("width %d cannot be negative", width))
	}
	return &T{InternalType: InternalType{
		Family: BitFamily, Oid: oid.T_bit, Width: width, Locale: &emptyLocale}}
}

// MakeVarBit constructs a new instance of the BIT type (oid = T_varbit) having
// the given max # bits (0 = unspecified number).
func MakeVarBit(width int32) *T {
	if width == 0 {
		return VarBit
	}
	if width < 0 {
		panic(errors.AssertionFailedf("width %d cannot be negative", width))
	}
	return &T{InternalType: InternalType{
		Family: BitFamily, Width: width, Oid: oid.T_varbit, Locale: &emptyLocale}}
}

// MakeString constructs a new instance of the STRING type (oid = T_text) having
// the given max # characters (0 = unspecified number).
func MakeString(width int32) *T {
	if width == 0 {
		return String
	}
	if width < 0 {
		panic(errors.AssertionFailedf("width %d cannot be negative", width))
	}
	return &T{InternalType: InternalType{
		Family: StringFamily, Oid: oid.T_text, Width: width, Locale: &emptyLocale}}
}

// MakeVarChar constructs a new instance of the VARCHAR type (oid = T_varchar)
// having the given max # characters (0 = unspecified number).
func MakeVarChar(width int32) *T {
	if width == 0 {
		return VarChar
	}
	if width < 0 {
		panic(errors.AssertionFailedf("width %d cannot be negative", width))
	}
	return &T{InternalType: InternalType{
		Family: StringFamily, Oid: oid.T_varchar, Width: width, Locale: &emptyLocale}}
}

// MakeChar constructs a new instance of the CHAR type (oid = T_bpchar) having
// the given max # characters (0 = unspecified number).
func MakeChar(width int32) *T {
	if width == 0 {
		return typeBpChar
	}
	if width < 0 {
		panic(errors.AssertionFailedf("width %d cannot be negative", width))
	}
	return &T{InternalType: InternalType{
		Family: StringFamily, Oid: oid.T_bpchar, Width: width, Locale: &emptyLocale}}
}

// oidCanBeCollatedString returns true if the given oid is can be a CollatedString.
func oidCanBeCollatedString(o oid.Oid) bool {
	switch o {
	case oid.T_text, oid.T_varchar, oid.T_bpchar, oid.T_char, oid.T_name:
		return true
	}
	return false
}

// MakeCollatedString constructs a new instance of a CollatedStringFamily type
// that is collated according to the given locale. The new type is based upon
// the given string type, having the same oid and width values. For example:
//
//   STRING      => STRING COLLATE EN
//   VARCHAR(20) => VARCHAR(20) COLLATE EN
//
func MakeCollatedString(strType *T, locale string) *T {
	if oidCanBeCollatedString(strType.Oid()) {
		return &T{InternalType: InternalType{
			Family: CollatedStringFamily, Oid: strType.Oid(), Width: strType.Width(), Locale: &locale}}
	}
	panic(errors.AssertionFailedf("cannot apply collation to non-string type: %s", strType))
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
		panic(errors.AssertionFailedf("precision %d cannot be negative", precision))
	}
	if scale < 0 {
		panic(errors.AssertionFailedf("scale %d cannot be negative", scale))
	}
	if scale > precision {
		panic(errors.AssertionFailedf(
			"scale %d cannot be larger than precision %d", scale, precision))
	}
	return &T{InternalType: InternalType{
		Family:    DecimalFamily,
		Oid:       oid.T_numeric,
		Precision: precision,
		Width:     scale,
		Locale:    &emptyLocale,
	}}
}

// MakeTime constructs a new instance of a TIME type (oid = T_time) that has at
// most the given number of fractional second digits.
//
// To use the default precision, use the `Time` variable.
func MakeTime(precision int32) *T {
	return &T{InternalType: InternalType{
		Family:             TimeFamily,
		Oid:                oid.T_time,
		Precision:          precision,
		TimePrecisionIsSet: true,
		Locale:             &emptyLocale,
	}}
}

// MakeTimeTZ constructs a new instance of a TIMETZ type (oid = T_timetz) that
// has at most the given number of fractional second digits.
//
// To use the default precision, use the `TimeTZ` variable.
func MakeTimeTZ(precision int32) *T {
	return &T{InternalType: InternalType{
		Family:             TimeTZFamily,
		Oid:                oid.T_timetz,
		Precision:          precision,
		TimePrecisionIsSet: true,
		Locale:             &emptyLocale,
	}}
}

// MakeGeometry constructs a new instance of a GEOMETRY type (oid = T_geometry)
// that has the given shape and SRID.
func MakeGeometry(shape geopb.ShapeType, srid geopb.SRID) *T {
	// Negative values are promoted to 0.
	if srid < 0 {
		srid = 0
	}
	return &T{InternalType: InternalType{
		Family: GeometryFamily,
		Oid:    oidext.T_geometry,
		Locale: &emptyLocale,
		GeoMetadata: &GeoMetadata{
			ShapeType: shape,
			SRID:      srid,
		},
	}}
}

// MakeGeography constructs a new instance of a geography-related type.
func MakeGeography(shape geopb.ShapeType, srid geopb.SRID) *T {
	// Negative values are promoted to 0.
	if srid < 0 {
		srid = 0
	}
	return &T{InternalType: InternalType{
		Family: GeographyFamily,
		Oid:    oidext.T_geography,
		Locale: &emptyLocale,
		GeoMetadata: &GeoMetadata{
			ShapeType: shape,
			SRID:      srid,
		},
	}}
}

// GeoMetadata returns the GeoMetadata of the type object if it exists.
// This should only exist on Geometry and Geography types.
func (t *T) GeoMetadata() (*GeoMetadata, error) {
	if t.InternalType.GeoMetadata == nil {
		return nil, errors.Newf("GeoMetadata does not exist on type")
	}
	return t.InternalType.GeoMetadata, nil
}

// GeoSRIDOrZero returns the geo SRID of the type object if it exists.
// This should only exist on a subset of Geometry and Geography types.
func (t *T) GeoSRIDOrZero() geopb.SRID {
	if t.InternalType.GeoMetadata != nil {
		return t.InternalType.GeoMetadata.SRID
	}
	return 0
}

var (
	// DefaultIntervalTypeMetadata returns a duration field that is unset,
	// using INTERVAL or INTERVAL ( iconst32 ) syntax instead of INTERVAL
	// with a qualifier afterwards.
	DefaultIntervalTypeMetadata = IntervalTypeMetadata{}
)

// IntervalTypeMetadata is metadata pertinent for intervals.
type IntervalTypeMetadata struct {
	// DurationField represents the duration field definition.
	DurationField IntervalDurationField
	// Precision is the precision to use - note this matches InternalType rules.
	Precision int32
	// PrecisionIsSet indicates whether Precision is explicitly set.
	PrecisionIsSet bool
}

// IsMinuteToSecond returns whether the IntervalDurationField represents
// the MINUTE TO SECOND interval qualifier.
func (m *IntervalDurationField) IsMinuteToSecond() bool {
	return m.FromDurationType == IntervalDurationType_MINUTE &&
		m.DurationType == IntervalDurationType_SECOND
}

// IsDayToHour returns whether the IntervalDurationField represents
// the DAY TO HOUR interval qualifier.
func (m *IntervalDurationField) IsDayToHour() bool {
	return m.FromDurationType == IntervalDurationType_DAY &&
		m.DurationType == IntervalDurationType_HOUR
}

// IntervalTypeMetadata returns the IntervalTypeMetadata for interval types.
func (t *T) IntervalTypeMetadata() (IntervalTypeMetadata, error) {
	if t.Family() != IntervalFamily {
		return IntervalTypeMetadata{}, errors.Newf("cannot call IntervalTypeMetadata on non-intervals")
	}
	return IntervalTypeMetadata{
		DurationField:  *t.InternalType.IntervalDurationField,
		Precision:      t.InternalType.Precision,
		PrecisionIsSet: t.InternalType.TimePrecisionIsSet,
	}, nil
}

// MakeInterval constructs a new instance of a
// INTERVAL type (oid = T_interval) with a duration field.
//
// To use the default precision and field, use the `Interval` variable.
func MakeInterval(itm IntervalTypeMetadata) *T {
	switch itm.DurationField.DurationType {
	case IntervalDurationType_SECOND, IntervalDurationType_UNSET:
	default:
		if itm.PrecisionIsSet {
			panic(errors.Errorf("cannot set precision for duration type %s", itm.DurationField.DurationType))
		}
	}
	if itm.Precision > 0 && !itm.PrecisionIsSet {
		panic(errors.Errorf("precision must be set if Precision > 0"))
	}

	return &T{InternalType: InternalType{
		Family:                IntervalFamily,
		Oid:                   oid.T_interval,
		Locale:                &emptyLocale,
		Precision:             itm.Precision,
		TimePrecisionIsSet:    itm.PrecisionIsSet,
		IntervalDurationField: &itm.DurationField,
	}}
}

// MakeTimestamp constructs a new instance of a TIMESTAMP type that has at most
// the given number of fractional second digits.
//
// To use the default precision, use the `Timestamp` variable.
func MakeTimestamp(precision int32) *T {
	return &T{InternalType: InternalType{
		Family:             TimestampFamily,
		Oid:                oid.T_timestamp,
		Precision:          precision,
		TimePrecisionIsSet: true,
		Locale:             &emptyLocale,
	}}
}

// MakeTimestampTZ constructs a new instance of a TIMESTAMPTZ type that has at
// most the given number of fractional second digits.
//
// To use the default precision, use the `TimestampTZ` variable.
func MakeTimestampTZ(precision int32) *T {
	return &T{InternalType: InternalType{
		Family:             TimestampTZFamily,
		Oid:                oid.T_timestamptz,
		Precision:          precision,
		TimePrecisionIsSet: true,
		Locale:             &emptyLocale,
	}}
}

// MakeEnum constructs a new instance of an EnumFamily type with the given
// stable type ID. Note that it does not hydrate cached fields on the type.
func MakeEnum(typeOID, arrayTypeOID oid.Oid) *T {
	return &T{InternalType: InternalType{
		Family: EnumFamily,
		Oid:    typeOID,
		Locale: &emptyLocale,
		UDTMetadata: &PersistentUserDefinedTypeMetadata{
			ArrayTypeOID: arrayTypeOID,
		},
	}}
}

// MakeArray constructs a new instance of an ArrayFamily type with the given
// element type (which may itself be an ArrayFamily type).
func MakeArray(typ *T) *T {
	// Do not make an array of type unknown[]. Follow Postgres' behavior and
	// convert this to type string[].
	if typ.Family() == UnknownFamily {
		typ = String
	}
	arr := &T{InternalType: InternalType{
		Family:        ArrayFamily,
		Oid:           CalcArrayOid(typ),
		ArrayContents: typ,
		Locale:        &emptyLocale,
	}}
	return arr
}

// MakeTuple constructs a new instance of a TupleFamily type with the given
// field types (some/all of which may be other TupleFamily types).
//
// Warning: the contents slice is used directly; the caller should not modify it
// after calling this function.
func MakeTuple(contents []*T) *T {
	return &T{InternalType: InternalType{
		Family: TupleFamily, Oid: oid.T_record, TupleContents: contents, Locale: &emptyLocale,
	}}
}

// MakeLabeledTuple constructs a new instance of a TupleFamily type with the
// given field types and labels.
func MakeLabeledTuple(contents []*T, labels []string) *T {
	if len(contents) != len(labels) && labels != nil {
		panic(errors.AssertionFailedf(
			"tuple contents and labels must be of same length: %v, %v", contents, labels))
	}
	return &T{InternalType: InternalType{
		Family:        TupleFamily,
		Oid:           oid.T_record,
		TupleContents: contents,
		TupleLabels:   labels,
		Locale:        &emptyLocale,
	}}
}

// Family specifies a group of types that are compatible with one another. Types
// in the same family can be compared, assigned, etc., but may differ from one
// another in width, precision, locale, and other attributes. For example, it is
// always an error to insert an INT value into a FLOAT column, because they are
// not in the same family. However, values of different types within the same
// family are "insert-compatible" with one another. Insertion may still result
// in an error because of width overflow or other constraints, but it can at
// least be attempted.
//
// Families are convenient for performing type switches on types, because in
// most cases it is the type family that matters, not the specific type. For
// example, when CRDB encodes values, it maintains routines for each family,
// since types in the same family encode in very similar ways.
//
// Most type families have an associated "canonical type" that is the default
// representative of that family, and which is a superset of all other types in
// that family. Values with other types (in the same family) can always be
// trivially converted to the canonical type with no loss of information. For
// example, the canonical type for IntFamily is Int, which is a 64-bit integer.
// Both 32-bit and 16-bit integers can be trivially converted to it.
//
// Execution operators and functions are permissive in terms of input (allow any
// type within a given family), and typically return only values having
// canonical types as output. For example, the IntFamily Plus operator allows
// values having any IntFamily type as input. But then it will always convert
// those values to 64-bit integers, and return a final 64-bit integer value
// (types.Int). Doing this vastly reduces the required number of operator
// overloads.
func (t *T) Family() Family {
	return t.InternalType.Family
}

// Oid returns the type's Postgres Object ID. The OID identifies the type more
// specifically than the type family, and is used by the Postgres wire protocol
// various Postgres catalog tables, functions like pg_typeof, etc. Maintaining
// the OID is required for Postgres-compatibility.
func (t *T) Oid() oid.Oid {
	return t.InternalType.Oid
}

// Locale identifies a specific geographical, political, or cultural region that
// impacts various character-based operations such as sorting, pattern matching,
// and builtin functions like lower and upper. It is only defined for the
// types in the CollatedStringFamily, and is the empty string for all other
// types.
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
//   INTERVAL   : max # fractional second digits
//   TIME       : max # fractional second digits
//   TIMETZ     : max # fractional second digits
//   TIMESTAMP  : max # fractional second digits
//   TIMESTAMPTZ: max # fractional second digits
//
// Precision for time-related families has special rules for 0 -- see
// `precision_is_set` on the `InternalType` proto.
//
// Precision is always 0 for other types.
func (t *T) Precision() int32 {
	switch t.InternalType.Family {
	case IntervalFamily, TimestampFamily, TimestampTZFamily, TimeFamily, TimeTZFamily:
		if t.InternalType.Precision == 0 && !t.InternalType.TimePrecisionIsSet {
			return defaultTimePrecision
		}
	}
	return t.InternalType.Precision
}

// TypeModifier returns the type modifier of the type. This corresponds to the
// pg_attribute.atttypmod column. atttypmod records type-specific data supplied
// at table creation time (for example, the maximum length of a varchar column).
// Array types have the same type modifier as the contents of the array.
// The value will be -1 for types that do not need atttypmod.
func (t *T) TypeModifier() int32 {
	if t.Family() == ArrayFamily {
		return t.ArrayContents().TypeModifier()
	}
	// The type modifier for "char" is always -1.
	if t.Oid() == oid.T_char {
		return int32(-1)
	}

	switch t.Family() {
	case StringFamily, CollatedStringFamily:
		if width := t.Width(); width != 0 {
			// Postgres adds 4 to the attypmod for bounded string types, the
			// var header size.
			return width + 4
		}
	case BitFamily:
		if width := t.Width(); width != 0 {
			return width
		}
	case DecimalFamily:
		// attTypMod is calculated by putting the precision in the upper
		// bits and the scale in the lower bits of a 32-bit int, and adding
		// 4 (the var header size). We mock this for clients' sake. See
		// https://github.com/postgres/postgres/blob/5a2832465fd8984d089e8c44c094e6900d987fcd/src/backend/utils/adt/numeric.c#L1242.
		if width, precision := t.Width(), t.Precision(); precision != 0 || width != 0 {
			return ((precision << 16) | width) + 4
		}
	}
	return int32(-1)
}

// WithoutTypeModifiers returns a copy of the given type with the type modifiers
// reset, if the type has modifiers. The returned type has arbitrary width and
// precision, or for some types, like timestamps, the maximum allowed width and
// precision. If the given type already has no type modifiers, it is returned
// unchanged and the function does not allocate a new type.
func (t *T) WithoutTypeModifiers() *T {
	switch t.Family() {
	case ArrayFamily:
		// Remove type modifiers of the array content type.
		newContents := t.ArrayContents().WithoutTypeModifiers()
		if newContents == t.ArrayContents() {
			return t
		}
		return MakeArray(newContents)
	case TupleFamily:
		// Remove type modifiers for each of the tuple content types.
		oldContents := t.TupleContents()
		newContents := make([]*T, len(oldContents))
		changed := false
		for i := range newContents {
			newContents[i] = oldContents[i].WithoutTypeModifiers()
			if newContents[i] != oldContents[i] {
				changed = true
			}
		}
		if !changed {
			return t
		}
		return MakeTuple(newContents)
	case EnumFamily:
		// Enums have no type modifiers.
		return t
	}

	// For types that can be a collated string, we copy the type and set the width
	// to 0 rather than returning the default OidToType type so that we retain the
	// locale value if the type is collated.
	if oidCanBeCollatedString(t.Oid()) {
		newT := *t
		newT.InternalType.Width = 0
		return &newT
	}

	t, ok := OidToType[t.Oid()]
	if !ok {
		panic(errors.AssertionFailedf("unexpected OID: %d", t.Oid()))
	}
	return t
}

// Scale is an alias method for Width, used for clarity for types in
// DecimalFamily.
func (t *T) Scale() int32 {
	return t.InternalType.Width
}

// ArrayContents returns the type of array elements. This is nil for types that
// are not in the ArrayFamily.
func (t *T) ArrayContents() *T {
	return t.InternalType.ArrayContents
}

// TupleContents returns a slice containing the type of each tuple field. This
// is nil for non-TupleFamily types.
func (t *T) TupleContents() []*T {
	return t.InternalType.TupleContents
}

// TupleLabels returns a slice containing the labels of each tuple field. This
// is nil for types not in the TupleFamily, or if the tuple type does not
// specify labels.
func (t *T) TupleLabels() []string {
	return t.InternalType.TupleLabels
}

// UserDefinedArrayOID returns the OID of the array type that corresponds to
// this user defined type. This function only can only be called on user
// defined types and returns non-zero data only for user defined types that
// aren't arrays.
func (t *T) UserDefinedArrayOID() oid.Oid {
	if t.InternalType.UDTMetadata == nil {
		return 0
	}
	return t.InternalType.UDTMetadata.ArrayTypeOID
}

// RemapUserDefinedTypeOIDs is used to remap OIDs stored within a types.T
// that is a user defined type. The newArrayOID argument is ignored if the
// input type is an Array type. It mutates the input types.T and should only
// be used when type is known to not be shared. If the input oid values are
// 0 then the RemapUserDefinedTypeOIDs has no effect.
func RemapUserDefinedTypeOIDs(t *T, newOID, newArrayOID oid.Oid) {
	if newOID != 0 {
		t.InternalType.Oid = newOID
	}
	if t.Family() != ArrayFamily && newArrayOID != 0 {
		t.InternalType.UDTMetadata.ArrayTypeOID = newArrayOID
	}
}

// UserDefined returns whether or not t is a user defined type.
func (t *T) UserDefined() bool {
	return IsOIDUserDefinedType(t.Oid())
}

// IsOIDUserDefinedType returns whether or not o corresponds to a user
// defined type.
func IsOIDUserDefinedType(o oid.Oid) bool {
	// Types with OIDs larger than the predefined max are user defined.
	return o > oidext.CockroachPredefinedOIDMax
}

var familyNames = map[Family]string{
	AnyFamily:            "any",
	ArrayFamily:          "array",
	BitFamily:            "bit",
	BoolFamily:           "bool",
	Box2DFamily:          "box2d",
	BytesFamily:          "bytes",
	CollatedStringFamily: "collatedstring",
	DateFamily:           "date",
	DecimalFamily:        "decimal",
	EnumFamily:           "enum",
	FloatFamily:          "float",
	GeographyFamily:      "geography",
	GeometryFamily:       "geometry",
	INetFamily:           "inet",
	IntFamily:            "int",
	IntervalFamily:       "interval",
	JsonFamily:           "jsonb",
	OidFamily:            "oid",
	StringFamily:         "string",
	TimeFamily:           "time",
	TimestampFamily:      "timestamp",
	TimestampTZFamily:    "timestamptz",
	TimeTZFamily:         "timetz",
	TupleFamily:          "tuple",
	UnknownFamily:        "unknown",
	UuidFamily:           "uuid",
	VoidFamily:           "void",
}

// Name returns a user-friendly word indicating the family type.
//
// TODO(radu): investigate whether anything breaks if we use
// enumvalue_customname and use String() instead.
func (f Family) Name() string {
	ret, ok := familyNames[f]
	if !ok {
		panic(errors.AssertionFailedf("unexpected Family: %d", f))
	}
	return ret
}

// Name returns a single word description of the type that describes it
// succinctly, but without all the details, such as width, locale, etc. The name
// is sometimes the same as the name returned by SQLStandardName, but is more
// CRDB friendly.
//
// TODO(andyk): Should these be changed to be the same as SQLStandardName?
func (t *T) Name() string {
	switch fam := t.Family(); fam {
	case AnyFamily:
		return "anyelement"

	case ArrayFamily:
		switch t.Oid() {
		case oid.T_oidvector:
			return "oidvector"
		case oid.T_int2vector:
			return "int2vector"
		}
		return t.ArrayContents().Name() + "[]"

	case BitFamily:
		if t.Oid() == oid.T_varbit {
			return "varbit"
		}
		return "bit"

	case FloatFamily:
		switch t.Width() {
		case 64:
			return "float"
		case 32:
			return "float4"
		default:
			panic(errors.AssertionFailedf("programming error: unknown float width: %d", t.Width()))
		}

	case IntFamily:
		switch t.Width() {
		case 64:
			return "int"
		case 32:
			return "int4"
		case 16:
			return "int2"
		default:
			panic(errors.AssertionFailedf("programming error: unknown int width: %d", t.Width()))
		}

	case OidFamily:
		return t.SQLStandardName()

	case StringFamily, CollatedStringFamily:
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
		panic(errors.AssertionFailedf("unexpected OID: %d", t.Oid()))

	case TupleFamily:
		return t.SQLStandardName()

	case EnumFamily:
		if t.Oid() == oid.T_anyenum {
			return "anyenum"
		}
		// This can be nil during unit testing.
		if t.TypeMeta.Name == nil {
			return "unknown_enum"
		}
		return t.TypeMeta.Name.Basename()

	default:
		return fam.Name()
	}
}

// PGName returns the Postgres name for the type. This is sometimes different
// than the native CRDB name for it (i.e. the Name function). It is used when
// compatibility with PG is important. Examples of differences:
//
//   Name()       PGName()
//   --------------------------
//   char         bpchar
//   "char"       char
//   bytes        bytea
//   int4[]       _int4
//
func (t *T) PGName() string {
	name, ok := oidext.TypeName(t.Oid())
	if ok {
		return strings.ToLower(name)
	}

	// For user defined types, return the basename.
	if t.UserDefined() {
		return t.TypeMeta.Name.Basename()
	}

	// Postgres does not have an UNKNOWN[] type. However, CRDB does, so
	// manufacture a name for it.
	if t.Family() != ArrayFamily || t.ArrayContents().Family() != UnknownFamily {
		panic(errors.AssertionFailedf("unknown PG name for oid %d", t.Oid()))
	}
	return "_unknown"
}

// SQLStandardName returns the type's name as it is specified in the SQL
// standard (or by Postgres for any non-standard types). This can be looked up
// for any type in Postgres using a query similar to this:
//
//   SELECT format_type(pg_typeof(1::int)::regtype, NULL)
//
func (t *T) SQLStandardName() string {
	return t.SQLStandardNameWithTypmod(false, 0)
}

var telemetryNameReplaceRegex = regexp.MustCompile("[^a-zA-Z0-9]")

// TelemetryName returns a name that is friendly for telemetry.
func (t *T) TelemetryName() string {
	return strings.ToLower(telemetryNameReplaceRegex.ReplaceAllString(t.SQLString(), "_"))
}

// SQLStandardNameWithTypmod is like SQLStandardName but it also accepts a
// typmod argument, and a boolean which indicates whether or not a typmod was
// even specified. The expected results of this function should be, in Postgres:
//
//   SELECT format_type('thetype'::regype, typmod)
//
// Generally, what this does with a non-0 typmod is append the scale, precision
// or length of a datatype to the name of the datatype. For example, a
// varchar(20) would appear as character varying(20) when provided the typmod
// value for varchar(20), which happens to be 24.
//
// This function is full of special cases. See backend/utils/adt/format_type.c
// in Postgres.
func (t *T) SQLStandardNameWithTypmod(haveTypmod bool, typmod int) string {
	var buf strings.Builder
	switch t.Family() {
	case AnyFamily:
		return "anyelement"
	case ArrayFamily:
		switch t.Oid() {
		case oid.T_oidvector:
			return "oidvector"
		case oid.T_int2vector:
			return "int2vector"
		}
		return t.ArrayContents().SQLStandardName() + "[]"
	case BitFamily:
		if t.Oid() == oid.T_varbit {
			buf.WriteString("bit varying")
		} else {
			buf.WriteString("bit")
		}
		if !haveTypmod || typmod <= 0 {
			return buf.String()
		}
		buf.WriteString(fmt.Sprintf("(%d)", typmod))
		return buf.String()
	case BoolFamily:
		return "boolean"
	case Box2DFamily:
		return "box2d"
	case BytesFamily:
		return "bytea"
	case DateFamily:
		return "date"
	case DecimalFamily:
		if !haveTypmod || typmod <= 0 {
			return "numeric"
		}
		// The typmod of a numeric has the precision in the upper bits and the
		// scale in the lower bits of a 32-bit int, after subtracting 4 (the var
		// header size). See numeric.c.
		typmod -= 4
		return fmt.Sprintf(
			"numeric(%d,%d)",
			(typmod>>16)&0xffff,
			typmod&0xffff,
		)

	case FloatFamily:
		switch t.Width() {
		case 32:
			return "real"
		case 64:
			return "double precision"
		default:
			panic(errors.AssertionFailedf("programming error: unknown float width: %d", t.Width()))
		}
	case GeometryFamily, GeographyFamily:
		return t.Name() + t.InternalType.GeoMetadata.SQLString()
	case INetFamily:
		return "inet"
	case IntFamily:
		switch t.Width() {
		case 16:
			return "smallint"
		case 32:
			// PG shows "integer" for int4.
			return "integer"
		case 64:
			return "bigint"
		default:
			panic(errors.AssertionFailedf("programming error: unknown int width: %d", t.Width()))
		}
	case IntervalFamily:
		// TODO(jordan): intervals can have typmods, but we don't support them in the same way.
		// Masking is used to extract the precision (src/include/utils/timestamp.h), whereas
		// we store it as `IntervalDurationField`.
		return "interval"
	case JsonFamily:
		// Only binary JSON is currently supported.
		return "jsonb"
	case OidFamily:
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
		case oid.T_regrole:
			return "regrole"
		case oid.T_regtype:
			return "regtype"
		default:
			panic(errors.AssertionFailedf("unexpected Oid: %v", errors.Safe(t.Oid())))
		}
	case StringFamily, CollatedStringFamily:
		switch t.Oid() {
		case oid.T_text:
			buf.WriteString("text")
		case oid.T_varchar:
			buf.WriteString("character varying")
		case oid.T_bpchar:
			if haveTypmod && typmod < 0 {
				// Special case. Run `select format_type('bpchar'::regtype, -1);` in pg.
				return "bpchar"
			}
			buf.WriteString("character")
		case oid.T_char:
			// Type modifiers not allowed for "char".
			return `"char"`
		case oid.T_name:
			// Type modifiers not allowed for name.
			return "name"
		default:
			panic(errors.AssertionFailedf("unexpected OID: %d", t.Oid()))
		}
		if !haveTypmod {
			return buf.String()
		}

		// Typmod gets subtracted by 4 for all non-text string-like types to produce
		// the length.
		if t.Oid() != oid.T_text {
			typmod -= 4
		}
		if typmod <= 0 {
			// In this case, we don't print any modifier.
			return buf.String()
		}
		buf.WriteString(fmt.Sprintf("(%d)", typmod))
		return buf.String()

	case TimeFamily:
		if !haveTypmod || typmod < 0 {
			return "time without time zone"
		}
		return fmt.Sprintf("time(%d) without time zone", typmod)
	case TimeTZFamily:
		if !haveTypmod || typmod < 0 {
			return "time with time zone"
		}
		return fmt.Sprintf("time(%d) with time zone", typmod)
	case TimestampFamily:
		if !haveTypmod || typmod < 0 {
			return "timestamp without time zone"
		}
		return fmt.Sprintf("timestamp(%d) without time zone", typmod)
	case TimestampTZFamily:
		if !haveTypmod || typmod < 0 {
			return "timestamp with time zone"
		}
		return fmt.Sprintf("timestamp(%d) with time zone", typmod)
	case TupleFamily:
		if t.UserDefined() {
			// If we have a user-defined tuple type, use its user-defined name.
			return t.TypeMeta.Name.Basename()
		}
		return "record"
	case UnknownFamily:
		return "unknown"
	case UuidFamily:
		return "uuid"
	case EnumFamily:
		return t.TypeMeta.Name.Basename()
	default:
		panic(errors.AssertionFailedf("unexpected Family: %v", errors.Safe(t.Family())))
	}
}

// InformationSchemaName returns the string suitable to populate the data_type
// column of information_schema.columns.
//
// This is different from SQLString() in that it must report SQL standard names
// that are compatible with PostgreSQL client expectations.
func (t *T) InformationSchemaName() string {
	// This is the same as SQLStandardName, except for the case of arrays.
	if t.Family() == ArrayFamily {
		return "ARRAY"
	}
	// TypeMeta attributes are populated only when it is user defined type.
	if t.TypeMeta.Name != nil {
		return "USER-DEFINED"
	}
	return t.SQLStandardName()
}

// SQLString returns the CockroachDB native SQL string that can be used to
// reproduce the type via parsing the string as a type. It is used in error
// messages and also to produce the output of SHOW CREATE.
func (t *T) SQLString() string {
	switch t.Family() {
	case BitFamily:
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
	case IntFamily:
		switch t.Width() {
		case 16:
			return "INT2"
		case 32:
			return "INT4"
		case 64:
			return "INT8"
		default:
			panic(errors.AssertionFailedf("programming error: unknown int width: %d", t.Width()))
		}
	case StringFamily:
		return t.stringTypeSQL()
	case CollatedStringFamily:
		return t.collatedStringTypeSQL(false /* isArray */)
	case FloatFamily:
		const realName = "FLOAT4"
		const doubleName = "FLOAT8"
		if t.Width() == 32 {
			return realName
		}
		return doubleName
	case DecimalFamily:
		if t.Precision() > 0 {
			if t.Width() > 0 {
				return fmt.Sprintf("DECIMAL(%d,%d)", t.Precision(), t.Scale())
			}
			return fmt.Sprintf("DECIMAL(%d)", t.Precision())
		}
	case JsonFamily:
		// Only binary JSON is currently supported.
		return "JSONB"
	case TimestampFamily, TimestampTZFamily, TimeFamily, TimeTZFamily:
		if t.InternalType.Precision > 0 || t.InternalType.TimePrecisionIsSet {
			return fmt.Sprintf("%s(%d)", strings.ToUpper(t.Name()), t.Precision())
		}
	case GeometryFamily, GeographyFamily:
		return strings.ToUpper(t.Name() + t.InternalType.GeoMetadata.SQLString())
	case IntervalFamily:
		switch t.InternalType.IntervalDurationField.DurationType {
		case IntervalDurationType_UNSET:
			if t.InternalType.Precision > 0 || t.InternalType.TimePrecisionIsSet {
				return fmt.Sprintf("%s(%d)", strings.ToUpper(t.Name()), t.Precision())
			}
		default:
			fromStr := ""
			if t.InternalType.IntervalDurationField.FromDurationType != IntervalDurationType_UNSET {
				fromStr = fmt.Sprintf("%s TO ", t.InternalType.IntervalDurationField.FromDurationType.String())
			}
			precisionStr := ""
			if t.InternalType.Precision > 0 || t.InternalType.TimePrecisionIsSet {
				precisionStr = fmt.Sprintf("(%d)", t.Precision())
			}
			return fmt.Sprintf(
				"%s %s%s%s",
				strings.ToUpper(t.Name()),
				fromStr,
				t.InternalType.IntervalDurationField.DurationType.String(),
				precisionStr,
			)
		}
	case OidFamily:
		if name, ok := oidext.TypeName(t.Oid()); ok {
			return name
		}
	case ArrayFamily:
		switch t.Oid() {
		case oid.T_oidvector:
			return "OIDVECTOR"
		case oid.T_int2vector:
			return "INT2VECTOR"
		}
		if t.ArrayContents().Family() == CollatedStringFamily {
			return t.ArrayContents().collatedStringTypeSQL(true /* isArray */)
		}
		return t.ArrayContents().SQLString() + "[]"
	case EnumFamily:
		if t.Oid() == oid.T_anyenum {
			return "anyenum"
		}
		return t.TypeMeta.Name.FQName()
	}
	return strings.ToUpper(t.Name())
}

// Equivalent returns true if this type is "equivalent" to the given type.
// Equivalent types are compatible with one another: they can be compared,
// assigned, and unioned. Equivalent types must always have the same type family
// for the root type and any descendant types (i.e. in case of array or tuple
// types). Types in the CollatedStringFamily must have the same locale. But
// other attributes of equivalent types, such as width, precision, and oid, can
// be different.
//
// Wildcard types (e.g. Any, AnyArray, AnyTuple, etc) have special equivalence
// behavior. AnyFamily types match any other type, including other AnyFamily
// types. And a wildcard collation (empty string) matches any other collation.
func (t *T) Equivalent(other *T) bool {
	if t.Family() == AnyFamily || other.Family() == AnyFamily {
		return true
	}
	if t.Family() != other.Family() {
		return false
	}

	switch t.Family() {
	case CollatedStringFamily:
		// CockroachDB differs from Postgres by comparing collation names
		// case-insensitively and equating hyphens/underscores.
		if t.Locale() != "" && other.Locale() != "" {
			if !lex.LocaleNamesAreEqual(t.Locale(), other.Locale()) {
				return false
			}
		}

	case TupleFamily:
		// If either tuple is the wildcard tuple, it's equivalent to any other
		// tuple type. This allows overloads to specify that they take an arbitrary
		// tuple type.
		if IsWildcardTupleType(t) || IsWildcardTupleType(other) {
			return true
		}
		if len(t.TupleContents()) != len(other.TupleContents()) {
			return false
		}
		for i := range t.TupleContents() {
			if !t.TupleContents()[i].Equivalent(other.TupleContents()[i]) {
				return false
			}
		}

	case ArrayFamily:
		if !t.ArrayContents().Equivalent(other.ArrayContents()) {
			return false
		}

	case EnumFamily:
		// If one of the types is anyenum, then allow the comparison to
		// go through -- anyenum is used when matching overloads.
		if t.Oid() == oid.T_anyenum || other.Oid() == oid.T_anyenum {
			return true
		}
		if t.Oid() != other.Oid() {
			return false
		}
	}

	return true
}

// EquivalentOrNull is the same as Equivalent, except it returns true if:
// * `t` is Unknown (i.e., NULL) AND (allowNullTupleEquivalence OR `other` is not a tuple),
// * `t` is a tuple with all non-Unknown elements matching the types in `other`.
func (t *T) EquivalentOrNull(other *T, allowNullTupleEquivalence bool) bool {
	// Check normal equivalency first, then check for Null
	normalEquivalency := t.Equivalent(other)
	if normalEquivalency {
		return true
	}

	switch t.Family() {
	case UnknownFamily:
		return allowNullTupleEquivalence || other.Family() != TupleFamily

	case TupleFamily:
		if other.Family() != TupleFamily {
			return false
		}
		// If either tuple is the wildcard tuple, it's equivalent to any other
		// tuple type. This allows overloads to specify that they take an arbitrary
		// tuple type.
		if IsWildcardTupleType(t) || IsWildcardTupleType(other) {
			return true
		}
		if len(t.TupleContents()) != len(other.TupleContents()) {
			return false
		}
		for i := range t.TupleContents() {
			if !t.TupleContents()[i].EquivalentOrNull(other.TupleContents()[i], allowNullTupleEquivalence) {
				return false
			}
		}
		return true

	default:
		return normalEquivalency
	}
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

// Equal is for use in generated protocol buffer code only.
func (t *T) Equal(other *T) bool {
	return t.Identical(other)
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
		panic(errors.NewAssertionErrorWithWrappedErrf(err, "error during Size call"))
	}
	return temp.InternalType.Size()
}

// Identical is the internal implementation for T.Identical. See that comment
// for details.
func (t *InternalType) Identical(other *InternalType) bool {
	if t.Family != other.Family {
		return false
	}
	if t.Width != other.Width {
		return false
	}
	if t.Precision != other.Precision {
		return false
	}
	if t.TimePrecisionIsSet != other.TimePrecisionIsSet {
		return false
	}
	if t.IntervalDurationField != nil && other.IntervalDurationField != nil {
		if *t.IntervalDurationField != *other.IntervalDurationField {
			return false
		}
	} else if t.IntervalDurationField != nil {
		return false
	} else if other.IntervalDurationField != nil {
		return false
	}
	if t.GeoMetadata != nil && other.GeoMetadata != nil {
		if t.GeoMetadata.ShapeType != other.GeoMetadata.ShapeType {
			return false
		}
		if t.GeoMetadata.SRID != other.GeoMetadata.SRID {
			return false
		}
	} else if t.GeoMetadata != nil {
		return false
	} else if other.GeoMetadata != nil {
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
		if !t.TupleContents[i].Identical(other.TupleContents[i]) {
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
	if t.UDTMetadata != nil && other.UDTMetadata != nil {
		if t.UDTMetadata.ArrayTypeOID != other.UDTMetadata.ArrayTypeOID {
			return false
		}
	} else if t.UDTMetadata != nil {
		return false
	} else if other.UDTMetadata != nil {
		return false
	}
	return t.Oid == other.Oid
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
	switch t.Family() {
	case IntFamily:
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
			// Pre-2.1 BIT was using IntFamily with arbitrary widths. Clamp them
			// to fixed/known widths. See #34161.
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
			return errors.AssertionFailedf("unexpected visible type: %d", t.InternalType.VisibleType)
		}

	case FloatFamily:
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
			return errors.AssertionFailedf("unexpected visible type: %d", t.InternalType.VisibleType)
		}

		// Precision should always be set to 0 going forward.
		t.InternalType.Precision = 0

	case TimestampFamily, TimestampTZFamily, TimeFamily, TimeTZFamily:
		// Some bad/experimental versions of master had precision stored as `-1`.
		// This represented a default - so upgrade this to 0 with TimePrecisionIsSet = false.
		if t.InternalType.Precision == -1 {
			t.InternalType.Precision = 0
			t.InternalType.TimePrecisionIsSet = false
		}
		// Going forwards after 19.2, we want `TimePrecisionIsSet` to be explicitly set
		// if Precision is > 0.
		if t.InternalType.Precision > 0 {
			t.InternalType.TimePrecisionIsSet = true
		}
	case IntervalFamily:
		// Fill in the IntervalDurationField here.
		if t.InternalType.IntervalDurationField == nil {
			t.InternalType.IntervalDurationField = &IntervalDurationField{}
		}
		// Going forwards after 19.2, we want `TimePrecisionIsSet` to be explicitly set
		// if Precision is > 0.
		if t.InternalType.Precision > 0 {
			t.InternalType.TimePrecisionIsSet = true
		}
	case StringFamily, CollatedStringFamily:
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
			return errors.AssertionFailedf("unexpected visible type: %d", t.InternalType.VisibleType)
		}
		if t.InternalType.Family == StringFamily {
			if t.InternalType.Locale != nil && len(*t.InternalType.Locale) != 0 {
				return errors.AssertionFailedf(
					"STRING type should not have locale: %s", *t.InternalType.Locale)
			}
		}

	case BitFamily:
		// Map visible VARBIT type to T_varbit OID value.
		switch t.InternalType.VisibleType {
		case visibleVARBIT:
			t.InternalType.Oid = oid.T_varbit
		case visibleNONE:
			t.InternalType.Oid = oid.T_bit
		default:
			return errors.AssertionFailedf("unexpected visible type: %d", t.InternalType.VisibleType)
		}

	case ArrayFamily:
		if t.ArrayContents() == nil {
			// This array type was serialized by a previous version of CRDB,
			// so construct the array contents from scratch.
			arrayContents := *t
			arrayContents.InternalType.Family = *t.InternalType.ArrayElemType
			arrayContents.InternalType.ArrayDimensions = nil
			arrayContents.InternalType.ArrayElemType = nil
			if err := arrayContents.upgradeType(); err != nil {
				return err
			}
			t.InternalType.ArrayContents = &arrayContents
			t.InternalType.Oid = CalcArrayOid(t.ArrayContents())
		}

		// Marshaling/unmarshaling nested arrays is not yet supported.
		if t.ArrayContents().Family() == ArrayFamily {
			return errors.AssertionFailedf("nested array should never be unmarshaled")
		}

		// Zero out fields that may have been used to store information about
		// the array element type, or which are no longer in use.
		t.InternalType.Width = 0
		t.InternalType.Precision = 0
		t.InternalType.Locale = nil
		t.InternalType.VisibleType = 0
		t.InternalType.ArrayElemType = nil
		t.InternalType.ArrayDimensions = nil

	case int2vector:
		t.InternalType.Family = ArrayFamily
		t.InternalType.Width = 0
		t.InternalType.Oid = oid.T_int2vector
		t.InternalType.ArrayContents = Int2

	case oidvector:
		t.InternalType.Family = ArrayFamily
		t.InternalType.Oid = oid.T_oidvector
		t.InternalType.ArrayContents = Oid

	case name:
		if t.InternalType.Locale != nil {
			t.InternalType.Family = CollatedStringFamily
		} else {
			t.InternalType.Family = StringFamily
		}
		t.InternalType.Oid = oid.T_name
		if t.Width() != 0 {
			return errors.AssertionFailedf("name type cannot have non-zero width: %d", t.Width())
		}
	}

	if t.InternalType.Oid == 0 {
		t.InternalType.Oid = familyToOid[t.Family()]
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
	if err := temp.downgradeType(); err != nil {
		return nil, err
	}
	return protoutil.Marshal(&temp.InternalType)
}

// MarshalToSizedBuffer is like Mashal, except that it deserializes to
// an existing byte slice with exactly enough remaining space for
// Size().
//
// Marshal is part of the protoutil.Message interface.
func (t *T) MarshalToSizedBuffer(data []byte) (int, error) {
	temp := *t
	if err := temp.downgradeType(); err != nil {
		return 0, err
	}
	return temp.InternalType.MarshalToSizedBuffer(data)
}

// MarshalTo behaves like Marshal, except that it deserializes to an existing
// byte slice and returns the number of bytes written to it. The slice must
// already have sufficient capacity. Callers can use the Size method to
// determine how much capacity needs to be allocated.
//
// Marshal is part of the protoutil.Message interface.
func (t *T) MarshalTo(data []byte) (int, error) {
	temp := *t
	if err := temp.downgradeType(); err != nil {
		return 0, err
	}
	return temp.InternalType.MarshalTo(data)
}

// of the latest CRDB version. It updates the fields so that they will be
// marshaled into a format that is compatible with the previous version of
// CRDB. This is necessary to preserve backwards-compatibility in mixed-version
// scenarios, such as during upgrade.
func (t *T) downgradeType() error {
	// Set Family and VisibleType for 19.1 backwards-compatibility.
	switch t.Family() {
	case BitFamily:
		if t.Oid() == oid.T_varbit {
			t.InternalType.VisibleType = visibleVARBIT
		}

	case FloatFamily:
		switch t.Width() {
		case 32:
			t.InternalType.VisibleType = visibleREAL
		}

	case StringFamily, CollatedStringFamily:
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
			t.InternalType.Family = name
		default:
			return errors.AssertionFailedf("unexpected Oid: %d", t.Oid())
		}

	case ArrayFamily:
		// Marshaling/unmarshaling nested arrays is not yet supported.
		if t.ArrayContents().Family() == ArrayFamily {
			return errors.AssertionFailedf("nested array should never be marshaled")
		}

		// Downgrade to array representation used before 19.2, in which the array
		// type fields specified the width, locale, etc. of the element type.
		temp := *t.InternalType.ArrayContents
		if err := temp.downgradeType(); err != nil {
			return err
		}
		t.InternalType.Width = temp.InternalType.Width
		t.InternalType.Precision = temp.InternalType.Precision
		t.InternalType.Locale = temp.InternalType.Locale
		t.InternalType.VisibleType = temp.InternalType.VisibleType
		t.InternalType.ArrayElemType = &t.InternalType.ArrayContents.InternalType.Family

		switch t.Oid() {
		case oid.T_int2vector:
			t.InternalType.Family = int2vector
		case oid.T_oidvector:
			t.InternalType.Family = oidvector
		}
	}

	// Map empty locale to nil.
	if t.InternalType.Locale != nil && len(*t.InternalType.Locale) == 0 {
		t.InternalType.Locale = nil
	}

	return nil
}

// String returns the name of the type, similar to the Name method. However, it
// expands CollatedStringFamily, ArrayFamily, and TupleFamily types to be more
// descriptive.
//
// TODO(andyk): It'd be nice to have this return SqlString() method output,
// since that is more descriptive.
func (t *T) String() string {
	switch t.Family() {
	case CollatedStringFamily:
		if t.Locale() == "" {
			// Used in telemetry.
			return fmt.Sprintf("collated%s{*}", t.Name())
		}
		return fmt.Sprintf("collated%s{%s}", t.Name(), t.Locale())

	case ArrayFamily:
		switch t.Oid() {
		case oid.T_oidvector, oid.T_int2vector:
			return t.Name()
		}
		return t.ArrayContents().String() + "[]"

	case TupleFamily:
		var buf bytes.Buffer
		buf.WriteString("tuple")
		if len(t.TupleContents()) != 0 && !IsWildcardTupleType(t) {
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
	case IntervalFamily, TimestampFamily, TimestampTZFamily, TimeFamily, TimeTZFamily:
		if t.InternalType.Precision > 0 || t.InternalType.TimePrecisionIsSet {
			return fmt.Sprintf("%s(%d)", t.Name(), t.Precision())
		}
	}
	return t.Name()
}

// MarshalText is implemented here so that gogo/protobuf know how to text marshal
// protobuf struct directly/indirectly depends on types.T without panic.
func (t *T) MarshalText() (text []byte, err error) {
	var buf bytes.Buffer
	if err := proto.MarshalText(&buf, &t.InternalType); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// DebugString returns a detailed dump of the type protobuf struct, suitable for
// debugging scenarios.
func (t *T) DebugString() string {
	if t.Family() == ArrayFamily && t.ArrayContents().UserDefined() {
		// In the case that this type is an array that contains a user defined
		// type, the introspection that protobuf performs to generate a string
		// representation will panic if the UserDefinedTypeMetadata field of the
		// type is populated. It seems to not understand that fields in the element
		// T could be not generated by proto, and panics trying to operate on the
		// TypeMeta field of the T. To get around this, we just deep copy the T and
		// zero out the type metadata to placate proto. See the following issue for
		// details: https://github.com/gogo/protobuf/issues/693.
		internalTypeCopy := protoutil.Clone(&t.InternalType).(*InternalType)
		internalTypeCopy.ArrayContents.TypeMeta = UserDefinedTypeMetadata{}
		return internalTypeCopy.String()
	}
	return t.InternalType.String()
}

// IsAmbiguous returns true if this type is in UnknownFamily or AnyFamily.
// Instances of ambiguous types can be NULL or be in one of several different
// type families. This is important for parameterized types to determine whether
// they are fully concrete or not.
func (t *T) IsAmbiguous() bool {
	switch t.Family() {
	case UnknownFamily, AnyFamily:
		return true
	case CollatedStringFamily:
		return t.Locale() == ""
	case TupleFamily:
		if len(t.TupleContents()) == 0 {
			return true
		}
		for i := range t.TupleContents() {
			if t.TupleContents()[i].IsAmbiguous() {
				return true
			}
		}
		return false
	case ArrayFamily:
		return t.ArrayContents().IsAmbiguous()
	case EnumFamily:
		return t.Oid() == oid.T_anyenum
	}
	return false
}

// IsNumeric returns true iff this type is an integer, float, or decimal.
func (t *T) IsNumeric() bool {
	switch t.Family() {
	case IntFamily, FloatFamily, DecimalFamily:
		return true
	default:
		return false
	}
}

// EnumGetIdxOfPhysical returns the index within the TypeMeta's slice of
// enum physical representations that matches the input byte slice.
func (t *T) EnumGetIdxOfPhysical(phys []byte) (int, error) {
	t.ensureHydratedEnum()
	// TODO (rohany): We can either use a map or just binary search here
	//  since the physical representations are sorted.
	reps := t.TypeMeta.EnumData.PhysicalRepresentations
	for i := range reps {
		if bytes.Equal(phys, reps[i]) {
			return i, nil
		}
	}
	err := errors.Newf(
		"could not find %v in enum %q representation %s %s",
		phys,
		t.TypeMeta.Name.FQName(),
		t.TypeMeta.EnumData.debugString(),
		debug.Stack(),
	)
	return 0, err
}

// EnumGetIdxOfLogical returns the index within the TypeMeta's slice of
// enum logical representations that matches the input string.
func (t *T) EnumGetIdxOfLogical(logical string) (int, error) {
	t.ensureHydratedEnum()
	reps := t.TypeMeta.EnumData.LogicalRepresentations
	for i := range reps {
		if reps[i] == logical {
			return i, nil
		}
	}
	return 0, pgerror.Newf(
		pgcode.InvalidTextRepresentation, "invalid input value for enum %s: %q", t, logical)
}

func (t *T) ensureHydratedEnum() {
	if t.TypeMeta.EnumData == nil {
		panic(errors.AssertionFailedf("use of enum metadata before hydration as an enum: %v %p", t, t))
	}
}

// IsStringType returns true iff the given type is String or a collated string
// type.
func IsStringType(t *T) bool {
	switch t.Family() {
	case StringFamily, CollatedStringFamily:
		return true
	default:
		return false
	}
}

// IsValidArrayElementType returns true if the given type can be used as the
// element type of an ArrayFamily-typed column. If the valid return is false,
// the issue number should be included in the error report to inform the user.
func IsValidArrayElementType(t *T) (valid bool, issueNum int) {
	switch t.Family() {
	default:
		return true, 0
	}
}

// CheckArrayElementType ensures that the given type can be used as the element
// type of an ArrayFamily-typed column. If not, it returns an error.
func CheckArrayElementType(t *T) error {
	if ok, issueNum := IsValidArrayElementType(t); !ok {
		return unimplemented.NewWithIssueDetailf(issueNum, t.String(),
			"arrays of %s not allowed", t)
	}
	return nil
}

// IsDateTimeType returns true if the given type is a date or time-related type.
func IsDateTimeType(t *T) bool {
	switch t.Family() {
	case DateFamily:
		return true
	case TimeFamily:
		return true
	case TimeTZFamily:
		return true
	case TimestampFamily:
		return true
	case TimestampTZFamily:
		return true
	case IntervalFamily:
		return true
	default:
		return false
	}
}

// IsAdditiveType returns true if the given type supports addition and
// subtraction.
func IsAdditiveType(t *T) bool {
	switch t.Family() {
	case IntFamily:
		return true
	case FloatFamily:
		return true
	case DecimalFamily:
		return true
	default:
		return IsDateTimeType(t)
	}
}

// IsWildcardTupleType returns true if this is the wildcard AnyTuple type. The
// wildcard type matches a tuple type having any number of fields (including 0).
func IsWildcardTupleType(t *T) bool {
	return len(t.TupleContents()) == 1 && t.TupleContents()[0].Family() == AnyFamily
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
	lex.EncodeLocaleName(&buf, t.Locale())
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

// IsHydrated returns true if this is a user-defined type and the TypeMeta
// is hydrated.
func (t *T) IsHydrated() bool {
	return t.UserDefined() && t.TypeMeta != (UserDefinedTypeMetadata{})
}

var typNameLiterals map[string]*T

func init() {
	typNameLiterals = make(map[string]*T)
	for o, t := range OidToType {
		name, ok := oidext.TypeName(o)
		if !ok {
			panic(errors.AssertionFailedf("oid %d has no type name", o))
		}
		name = strings.ToLower(name)
		if _, ok := typNameLiterals[name]; !ok {
			typNameLiterals[name] = t
		}
	}
	for name, t := range unreservedTypeTokens {
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

// The SERIAL types are pseudo-types that are only used during parsing. After
// that, they should behave identically to INT columns. They are declared
// as INT types, but using different instances than types.Int, types.Int2, etc.
// so that they can be compared by pointer to differentiate them from the
// singleton INT types. While the usual requirement is that == is never used to
// compare types, this is one case where it's allowed.
var (
	Serial2Type = *Int2
	Serial4Type = *Int4
	Serial8Type = *Int
)

// IsSerialType returns whether or not the input type is a SERIAL type.
// This function should only be used during parsing.
func IsSerialType(typ *T) bool {
	// This is a special case where == is used to compare types, since the SERIAL
	// types are pseudo-types.
	return typ == &Serial2Type || typ == &Serial4Type || typ == &Serial8Type
}

// unreservedTypeTokens contain type alias that we resolve during parsing.
// Instead of adding a new token to the parser, add the type here.
var unreservedTypeTokens = map[string]*T{
	"blob":       Bytes,
	"bool":       Bool,
	"bytea":      Bytes,
	"bytes":      Bytes,
	"date":       Date,
	"float4":     Float,
	"float8":     Float,
	"inet":       INet,
	"int2":       Int2,
	"int4":       Int4,
	"int8":       Int,
	"int64":      Int,
	"int2vector": Int2Vector,
	"json":       Jsonb,
	"jsonb":      Jsonb,
	"name":       Name,
	"oid":        Oid,
	"oidvector":  OidVector,
	// Postgres OID pseudo-types. See https://www.postgresql.org/docs/9.4/static/datatype-oid.html.
	"regclass":     RegClass,
	"regnamespace": RegNamespace,
	"regproc":      RegProc,
	"regprocedure": RegProcedure,
	"regrole":      RegRole,
	"regtype":      RegType,

	"serial2":     &Serial2Type,
	"serial4":     &Serial4Type,
	"serial8":     &Serial8Type,
	"smallserial": &Serial2Type,
	"bigserial":   &Serial8Type,

	"string": String,
	"uuid":   Uuid,
}

// The following map must include all types predefined in PostgreSQL
// that are also not yet defined in CockroachDB and link them to
// github issues. It is also possible, but not necessary, to include
// PostgreSQL types that are already implemented in CockroachDB.
var postgresPredefinedTypeIssues = map[string]int{
	"box":           21286,
	"cidr":          18846,
	"circle":        21286,
	"jsonpath":      22513,
	"line":          21286,
	"lseg":          21286,
	"macaddr":       45813,
	"macaddr8":      45813,
	"money":         41578,
	"path":          21286,
	"pg_lsn":        -1,
	"tsquery":       7821,
	"tsvector":      7821,
	"txid_snapshot": -1,
	"xml":           43355,
}

// SQLString outputs the GeoMetadata in a SQL-compatible string.
func (m *GeoMetadata) SQLString() string {
	// If SRID is available, display both shape and SRID.
	// If shape is available but not SRID, just display shape.
	if m.SRID != 0 {
		shapeName := strings.ToLower(m.ShapeType.String())
		if m.ShapeType == geopb.ShapeType_Unset {
			shapeName = "geometry"
		}
		return fmt.Sprintf("(%s,%d)", shapeName, m.SRID)
	} else if m.ShapeType != geopb.ShapeType_Unset {
		return fmt.Sprintf("(%s)", m.ShapeType)
	}
	return ""
}
