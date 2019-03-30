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
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/lex"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/lib/pq/oid"
)

const (
	// Deprecated after 19.1, since it's now represented using the Oid field.
	name SemanticType = 11

	// Deprecated after 19.1, since it's now represented using the Oid field.
	int2vector SemanticType = 200

	// Deprecated after 19.1, since it's now represented using the Oid field.
	oidvector SemanticType = 201

	// Deprecated after 2.1, since it's no longer used.
	visibleINTEGER = 1

	// Deprecated after 2.1, since it's now represented using the Width field.
	visibleSMALLINT = 2

	// Deprecated after 19.1, since it's now represented using the Width field.
	visibleREAL = 5

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

// ColumnType wraps the Protobuf-generated InternalColumnType so that it can
// override the Marshal/Unmarshal methods in order to map to/from older
// persisted ColumnType representations.
type ColumnType InternalColumnType

// T is a temporary alias for ColumnType.
// TODO(andyk): Remove in future commit.
type T = ColumnType

// Oid returns the type's Postgres object ID.
func (t *ColumnType) Oid() oid.Oid {
	if t.ZZZ_Oid != 0 {
		return t.ZZZ_Oid
	}
	switch t.SemanticType {
	case ARRAY:
		o := t.ArrayContents.Oid()
		switch t.ArrayContents.SemanticType {
		case ARRAY:
			// Postgres nested arrays return the OID of the nested array (i.e. the
			// OID doesn't change no matter how many levels of nesting there are),
			// except in the special-case of the vector types.
			switch o {
			case oid.T_int2vector, oid.T_oidvector:
				// Vector types have their own array OID types.
			default:
				return o
			}

		case UNKNOWN:
			// Postgres doesn't have an OID for ARRAY of UNKNOWN, since it's not
			// possible to create that in Postgres. But CRDB does allow that, so
			// return 0 for that case (since there's no T__unknown). This is what
			// previous versions of CRDB returned for this case.
			return unknownArrayOid
		}

		// Map the OID of the array element type to the corresponding array OID.
		// This should always be possible for all other OIDs (checked in oid.go
		// init method).
		o = oidToArrayOid[o]
		if o == 0 {
			panic(pgerror.NewAssertionErrorf("oid %d couldn't be mapped to array oid", o))
		}
		return o

	case INT:
		switch t.Width {
		case 16:
			return oid.T_int2
		case 32:
			return oid.T_int4
		default:
			return oid.T_int8
		}

	case FLOAT:
		switch t.Width {
		case 32:
			return oid.T_float4
		default:
			return oid.T_float8
		}
	}
	return semanticTypeToOid[t.SemanticType]
}

// Size delegates to InternalColumnType.
func (t *ColumnType) Size() (n int) {
	temp := *t
	if err := temp.downgradeType(); err != nil {
		panic(pgerror.NewAssertionErrorf("error during Size call: %v", err))
	}
	return (*InternalColumnType)(&temp).Size()
}

// Identical returns true if every field in this ColumnType is exactly the same
// as every corresponding field in the given ColumnType. Identical performs a
// deep comparison, traversing any Tuple or Array contents.
//
// Identical is complementary to Equivalent. Equivalent should be used when
// scalar types in the same family (with same locale, in case of the collated
// string family) should be considered equal, regardless of other attributes of
// the type, like width and oid. Identical should be used when all the
// attributes of the type are important to compare for differences.
//
// As an example, Equivalent is used when checking if two SQL expressions can be
// compared to one another. But Identical is used by the optimizer when testing
// whether a CAST expression can be discarded (i.e. because source expression's
// type is identical to the CAST target type in all ways).
func (t *ColumnType) Identical(other *ColumnType) bool {
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
	return t.ZZZ_Oid == other.ZZZ_Oid
}

// Equivalent returns whether the receiver and the other type are equivalent.
// We say that two type patterns are "equivalent" when they are structurally
// equivalent given that a wildcard is equivalent to any type. When neither
// Type is ambiguous (see IsAmbiguous), equivalency is the same as type
// equality.
func (t *ColumnType) Equivalent(other *ColumnType) bool {
	if t.SemanticType == ANY || other.SemanticType == ANY {
		return true
	}
	if t.SemanticType != other.SemanticType {
		return false
	}

	switch t.SemanticType {
	case COLLATEDSTRING:
		if *t.Locale != "" && *other.Locale != "" && *t.Locale != *other.Locale {
			return false
		}

	case TUPLE:
		// If either tuple is the wildcard tuple, it's equivalent to any other
		// tuple type. This allows overloads to specify that they take an arbitrary
		// tuple type.
		if IsWildcardTupleType(t) || IsWildcardTupleType(other) {
			return true
		}
		if len(t.TupleContents) != len(other.TupleContents) {
			return false
		}
		for i := range t.TupleContents {
			if !t.TupleContents[i].Equivalent(&other.TupleContents[i]) {
				return false
			}
		}

	case ARRAY:
		if !t.ArrayContents.Equivalent(other.ArrayContents) {
			return false
		}
	}

	return true
}

// Unmarshal deserializes a ColumnType from the given bytes.
func (t *ColumnType) Unmarshal(data []byte) error {
	err := protoutil.Unmarshal(data, (*InternalColumnType)(t))
	if err != nil {
		return err
	}
	return t.upgradeType()
}

func (t *ColumnType) upgradeType() error {
	switch t.SemanticType {
	case INT:
		// Check VisibleType field that was populated in previous versions.
		switch t.ZZZ_VisibleType {
		case visibleSMALLINT:
			t.Width = 16
		case visibleINTEGER:
			t.Width = 32
		default:
			// Pre-2.1 BIT was using column type INT with arbitrary widths. Clamp
			// them to fixed/known widths. See #34161.
			switch t.Width {
			case 0, 16, 32, 64:
			default:
				// Assume INT8 if width is not valid.
				t.Width = 64
			}
		}

	case FLOAT:
		// Map visible REAL type to 32-bit width.
		if t.ZZZ_VisibleType == visibleREAL {
			t.Width = 32
		} else {
			switch t.Width {
			case 32, 64:
			default:
				// Pre-2.1 (before Width) there were 3 cases:
				// - VisibleType = DOUBLE PRECISION, Width = 0 -> now clearly FLOAT8
				// - VisibleType = NONE, Width = 0 -> now clearly FLOAT8
				// - VisibleType = NONE, Precision > 0 -> we need to derive the width.
				if t.Precision >= 1 && t.Precision <= 24 {
					t.Width = 32
				} else {
					t.Width = 64
				}
			}
		}

		// Precision should always be set to 0 going forward.
		t.Precision = 0

	case STRING, COLLATEDSTRING:
		switch t.ZZZ_VisibleType {
		case visibleVARCHAR:
			t.ZZZ_Oid = oid.T_varchar
		case visibleCHAR:
			t.ZZZ_Oid = oid.T_bpchar
		case visibleQCHAR:
			t.ZZZ_Oid = oid.T_char
		}

	case BIT:
		if t.ZZZ_VisibleType == visibleVARBIT {
			t.ZZZ_Oid = oid.T_varbit
		}

	case ARRAY:
		if t.ArrayContents == nil {
			arrayContents := *t
			arrayContents.SemanticType = *t.ZZZ_ArrayElemType
			arrayContents.ZZZ_ArrayDimensions = nil
			arrayContents.ZZZ_ArrayElemType = nil
			if err := arrayContents.upgradeType(); err != nil {
				return err
			}
			t.ArrayContents = &arrayContents
		}

		// Marshaling/unmarshaling nested arrays is not yet supported.
		if t.ArrayContents.SemanticType == ARRAY {
			return pgerror.NewAssertionErrorf("nested array should never be unmarshaled")
		}

		// Zero out fields that may have been used to store information about
		// the array element type, or which are no longer in use.
		t.Width = 0
		t.Precision = 0
		t.Locale = nil
		t.ZZZ_VisibleType = 0
		t.ZZZ_ArrayElemType = nil
		t.ZZZ_ArrayDimensions = nil

	case int2vector:
		t.SemanticType = ARRAY
		t.Width = 0
		t.ZZZ_Oid = oid.T_int2vector
		t.ArrayContents = Int2
	case oidvector:
		t.SemanticType = ARRAY
		t.ZZZ_Oid = oid.T_oidvector
		t.ArrayContents = Oid

	case name:
		t.SemanticType = STRING
		t.ZZZ_Oid = oid.T_name
	}

	// Clear any visible type, since they are all now handled by the Width or
	// Oid fields.
	t.ZZZ_VisibleType = VisibleType_NONE

	return nil
}

// Marshal serializes the ColumnType to bytes.
func (t *ColumnType) Marshal() (data []byte, err error) {
	temp := *t
	if err := temp.downgradeType(); err != nil {
		return nil, err
	}
	return protoutil.Marshal((*InternalColumnType)(&temp))
}

// MarshalTo serializes the ColumnType to the given byte slice.
func (t *ColumnType) MarshalTo(data []byte) (int, error) {
	temp := *t
	if err := temp.downgradeType(); err != nil {
		return 0, err
	}
	return (*InternalColumnType)(&temp).MarshalTo(data)
}

func (t *ColumnType) downgradeType() error {
	// Set SemanticType and VisibleType for 19.1 backwards-compatibility.
	switch t.SemanticType {
	case BIT:
		if t.ZZZ_Oid == oid.T_varbit {
			t.ZZZ_VisibleType = visibleVARBIT
		}

	case FLOAT:
		switch t.Width {
		case 32:
			t.ZZZ_VisibleType = visibleREAL
		}

	case STRING, COLLATEDSTRING:
		switch t.ZZZ_Oid {
		case oid.T_varchar:
			t.ZZZ_VisibleType = visibleVARCHAR
		case oid.T_bpchar:
			t.ZZZ_VisibleType = visibleCHAR
		case oid.T_char:
			t.ZZZ_VisibleType = visibleQCHAR
		case oid.T_name:
			t.SemanticType = name
		}

	case ARRAY:
		// Marshaling/unmarshaling nested arrays is not yet supported.
		if t.ArrayContents.SemanticType == ARRAY {
			return pgerror.NewAssertionErrorf("nested array should never be marshaled")
		}

		// Downgrade to ARRAY representation used before 19.2, in which the array
		// type fields specified the width, locale, etc. of the element type.
		temp := *t.ArrayContents
		if err := temp.downgradeType(); err != nil {
			return err
		}
		t.Width = temp.Width
		t.Precision = temp.Precision
		t.Locale = temp.Locale
		t.ZZZ_VisibleType = temp.ZZZ_VisibleType
		t.ZZZ_ArrayElemType = &t.ArrayContents.SemanticType

		switch t.Oid() {
		case oid.T_int2vector:
			t.SemanticType = int2vector
		case oid.T_oidvector:
			t.SemanticType = oidvector
		}
	}

	return nil
}

// DebugString returns a detailed dump of the type protobuf struct, suitable for
// debugging scenarios.
func (t *ColumnType) DebugString() string {
	return (*InternalColumnType)(t).String()
}

// Name returns a single word description of the type that describes it
// succinctly, but without all the details, such as width, locale, etc. The name
// is sometimes the same as the name returned by SQLStandardName, but is more
// CRDB friendly.
//
// TODO(andyk): Should these be changed to be the same as SQLStandardName?
func (t *ColumnType) Name() string {
	switch t.SemanticType {
	case UNKNOWN:
		return "unknown"
	case BOOL:
		return "bool"
	case INT:
		switch t.Width {
		case 16:
			return "int2"
		case 32:
			return "int4"
		}
		return "int"
	case STRING, COLLATEDSTRING:
		switch t.Oid() {
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
		return "string"
	case BIT:
		if t.Oid() == oid.T_varbit {
			return "varbit"
		}
		return "bit"
	case FLOAT:
		return "float"
	case DECIMAL:
		return "decimal"
	case BYTES:
		return "bytes"
	case DATE:
		return "date"
	case TIME:
		return "time"
	case TIMESTAMP:
		return "timestamp"
	case TIMESTAMPTZ:
		return "timestamptz"
	case INTERVAL:
		return "interval"
	case JSON:
		return "jsonb"
	case UUID:
		return "uuid"
	case INET:
		return "inet"
	case TUPLE:
		// TUPLE is currently an anonymous type, with no name.
		return ""
	case ARRAY:
		switch t.Oid() {
		case oid.T_oidvector:
			return "oidvector"
		case oid.T_int2vector:
			return "int2vector"
		}
		return t.ArrayContents.String() + "[]"
	case ANY:
		return "anyelement"
	case OID:
		return t.SQLStandardName()
	}

	panic(pgerror.NewAssertionErrorf("unexpected SemanticType: %s", t.SemanticType))
}

func (t *ColumnType) String() string {
	switch t.SemanticType {
	case COLLATEDSTRING:
		if *t.Locale == "" {
			// Used in telemetry.
			return fmt.Sprintf("collated%s{*}", t.Name())
		}
		return fmt.Sprintf("collated%s{%s}", t.Name(), *t.Locale)

	case TUPLE:
		var buf bytes.Buffer
		buf.WriteString("tuple")
		if len(t.TupleContents) != 0 && !IsWildcardTupleType(t) {
			buf.WriteByte('{')
			for i, typ := range t.TupleContents {
				if i != 0 {
					buf.WriteString(", ")
				}
				buf.WriteString(typ.String())
				if t.TupleLabels != nil {
					buf.WriteString(" AS ")
					buf.WriteString(t.TupleLabels[i])
				}
			}
			buf.WriteByte('}')
		}
		return buf.String()
	}
	return t.Name()
}

var (
	// Unknown is the type of an expression that statically evaluates to NULL.
	// This type should never be returned for an expression that does not *always*
	// evaluate to NULL.
	Unknown = &T{SemanticType: UNKNOWN}

	// Bool is the type of a boolean true/false value.
	Bool = &T{SemanticType: BOOL}

	// VarBit is the type of an ordered list of bits (0 or 1 valued), with no
	// specified limit on the count of bits.
	VarBit = &T{SemanticType: BIT, ZZZ_Oid: oid.T_varbit}

	// Int is the type of a 64-bit signed integer. This is the canonical INT
	// semantic type for CRDB.
	Int = &T{SemanticType: INT, Width: 64}

	// Int4 is the type of a 32-bit signed integer.
	Int4 = &T{SemanticType: INT, Width: 32}

	// Int2 is the type of a 16-bit signed integer.
	Int2 = &T{SemanticType: INT, Width: 16}

	// Float is the type of a 64-bit base-2 floating-point number (IEEE 754).
	// This is the canonical FLOAT semantic type for CRDB.
	Float = &T{SemanticType: FLOAT, Width: 64}

	// Float4 is the type of a 32-bit base-2 floating-point number (IEEE 754).
	Float4 = &T{SemanticType: FLOAT, Width: 32}

	// Decimal is the type of a base-10 floating-point number, with no specified
	// limit on precision (number of digits) or scale (digits to right of decimal
	// point).
	Decimal = &T{SemanticType: DECIMAL}

	// String is the type of a Unicode string, with no specified limit on the
	// count of characters. This is the canonical STRING semantic type for CRDB.
	// It is reported as STRING in SHOW CREATE but "text" in introspection for
	// compatibility with PostgreSQL.
	String = &T{SemanticType: STRING}

	// VarChar is equivalent to String, but has a differing OID (T_varchar),
	// which makes it show up differently when displayed. It is reported as
	// VARCHAR in SHOW CREATE and "character varying" in introspection for
	// compatibility with PostgreSQL.
	VarChar = &T{SemanticType: STRING, ZZZ_Oid: oid.T_varchar}

	// Bytes is the type of a list of raw byte values.
	Bytes = &T{SemanticType: BYTES}

	// Date is the type of a value specifying year, month, day (with no time
	// component). There is no timezone associated with it. For example:
	//
	//   YYYY-MM-DD
	//
	Date = &T{SemanticType: DATE}

	// Time is the type of a value specifying hour, minute, second (with no date
	// component). By default, it has microsecond precision. There is no timezone
	// associated with it. For example:
	//
	//   HH:MM:SS.ssssss
	//
	Time = &T{SemanticType: TIME}

	// Timestamp is the type of a value specifying year, month, day, hour, minute,
	// and second, but with no associated timezone. By default, it has microsecond
	// precision. For example:
	//
	//   YYYY-MM-DD HH:MM:SS.ssssss
	//
	Timestamp = &T{SemanticType: TIMESTAMP}

	// TimestampTZ is the type of a value specifying year, month, day, hour,
	// minute, and second, as well as an associated timezone. By default, it has
	// microsecond precision. For example:
	//
	//   YYYY-MM-DD HH:MM:SS.ssssss+-ZZ:ZZ
	//
	TimestampTZ = &T{SemanticType: TIMESTAMPTZ}

	// Interval is the type of a value describing a duration of time. By default,
	// it has microsecond precision.
	Interval = &T{SemanticType: INTERVAL}

	// Jsonb is the type of a JavaScript Object Notation (JSON) value that is
	// stored in a decomposed binary format (hence the "b" in jsonb).
	Jsonb = &T{SemanticType: JSON}

	// Uuid is the type of a universally unique identifier (UUID), which is a
	// 128-bit quantity that is very unlikely to ever be generated again, and so
	// can be relied on to be distinct from all other UUID values.
	Uuid = &T{SemanticType: UUID}

	// INet is the type of an IPv4 or IPv6 network address. For example:
	//
	//   192.168.100.128/25
	//   FE80:CD00:0:CDE:1257:0:211E:729C
	//
	INet = &T{SemanticType: INET}

	// Any is a special type used only during static analysis as a wildcard type
	// that matches any other type, including scalar, array, and tuple types.
	// Execution-time values should never have this type. As an example of its
	// use, many SQL builtin functions allow an input value to be of any type,
	// and so use this type in their static definitions.
	Any = &T{SemanticType: ANY}

	// AnyArray is a special type used only during static analysis as a wildcard
	// type that matches an array having elements of any (uniform) type (including
	// ARRAY). Execution-time values should never have this type.
	AnyArray = &T{SemanticType: ARRAY, ArrayContents: Any}

	// AnyTuple is a special type used only during static analysis as a wildcard
	// type that matches a tuple with any number of fields of any type (including
	// TUPLE). Execution-time values should never have this type.
	AnyTuple = &T{SemanticType: TUPLE, TupleContents: []ColumnType{*Any}}

	// AnyCollatedString is the collated string type with a nil locale. It is
	// used as a wildcard parameterized type that matches any locale.
	AnyCollatedString = &T{SemanticType: COLLATEDSTRING, Locale: &emptyLocale}

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

	// EmptyTuple is the tuple type with no fields. Note that this is different
	// than AnyTuple, which is a wildcard type.
	EmptyTuple = &T{SemanticType: TUPLE}

	// StringArray is the type of an ARRAY value having String-typed elements.
	StringArray = &T{SemanticType: ARRAY, ArrayContents: String}

	// IntArray is the type of an ARRAY value having Int-typed elements.
	IntArray = &T{SemanticType: ARRAY, ArrayContents: Int}

	// DecimalArray is the type of an ARRAY value having Decimal-typed elements.
	DecimalArray = &T{SemanticType: ARRAY, ArrayContents: Decimal}
)

var (
	emptyLocale = ""
)

// Unexported wrapper types.
var (
	// typeBit is not exported to avoid confusion over whether its default Width
	// is unspecified or is 1.
	typeBit = &T{SemanticType: BIT}

	// typeBpChar is the "standard SQL" string type of fixed length, where "bp"
	// stands for "blank padded". It is not exported to avoid confusion with
	// typeQChar, as well as confusion over its default width.
	//
	// It is reported as CHAR in SHOW CREATE and "character" in introspection for
	// compatibility with PostgreSQL.
	//
	// Its default maximum with is 1. It always has a maximum width.
	typeBpChar = &T{SemanticType: STRING, ZZZ_Oid: oid.T_bpchar}

	// typeQChar is a special PostgreSQL-only type supported for compatibility.
	// It behaves like VARCHAR, its maximum width cannot be modified, and has a
	// peculiar name in the syntax and introspection. It is not exported to avoid
	// confusion with typeBpChar, as well as confusion over its default width.
	//
	// It is reported as "char" (with double quotes included) in SHOW CREATE and
	// "char" in introspection for compatibility with PostgreSQL.
	typeQChar = &T{SemanticType: STRING, ZZZ_Oid: oid.T_char}
)

// MakeBit constructs a new instance of the BIT semantic type (oid = T_bit)
// having the given max # bits (0 = unspecified number).
func MakeBit(width int32) *T {
	return &T{SemanticType: BIT, Width: width}
}

// MakeInt constructs a new instance of the INT semantic type that is either 16,
// 32, or 64 bits in width.
func MakeInt(width int32) *T {
	switch width {
	case 16:
		return Int2
	case 32:
		return Int4
	}
	return Int
}

// MakeVarBit constructs a new instance of the BIT type (oid = T_varbit) having
// the given max # bits (0 = unspecified number).
func MakeVarBit(width int32) *T {
	return &T{SemanticType: BIT, Width: width, ZZZ_Oid: oid.T_varbit}
}

// MakeString constructs a new instance of the STRING type (oid = T_text) having
// the given max # characters (0 = unspecified number).
func MakeString(width int32) *T {
	return &T{SemanticType: STRING, Width: width}
}

// MakeVarChar constructs a new instance of the VARCHAR type (oid = T_varchar)
// having the given max # characters (0 = unspecified number).
func MakeVarChar(width int32) *T {
	return &T{SemanticType: STRING, Width: width, ZZZ_Oid: oid.T_varchar}
}

// MakeChar constructs a new instance of the CHAR type (oid = T_bpchar) having
// the given max # characters (0 = unspecified number).
func MakeChar(width int32) *T {
	return &T{SemanticType: STRING, Width: width, ZZZ_Oid: oid.T_bpchar}
}

// MakeQChar constructs a new instance of the "char" type (oid = T_char) having
// the given max # characters (0 = unspecified number).
func MakeQChar(width int32) *T {
	return &T{SemanticType: STRING, Width: width, ZZZ_Oid: oid.T_char}
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
	case oid.T_text:
		// TODO(andyk): In future commit, always fill in Oid field.
		return &T{
			SemanticType: COLLATEDSTRING,
			Width:        strType.Width,
			Locale:       &locale,
		}
	case oid.T_varchar, oid.T_bpchar, oid.T_char:
		return &T{
			SemanticType: COLLATEDSTRING,
			ZZZ_Oid:      strType.Oid(),
			Width:        strType.Width,
			Locale:       &locale,
		}
	}
	panic(pgerror.NewAssertionErrorf("cannot apply collation to non-string type: %s", strType))
}

// MakeDecimal constructs a new instance of a DECIMAL type (oid = T_numeric)
// that has at most "precision" # of decimal digits (0 = unspecified number of
// digits) and at most "scale" # of decimal digits after the decimal point
// (0 = unspecified number of digits). scale must be <= precision.
func MakeDecimal(precision, scale int32) *T {
	return &T{SemanticType: DECIMAL, Precision: precision, Width: scale}
}

// MakeTime constructs a new instance of a TIME type (oid = T_time) that has at
// most the given number of fractional second digits.
func MakeTime(precision int32) *T {
	return &T{SemanticType: TIME, Precision: precision}
}

// MakeTimestamp constructs a new instance of a TIMESTAMP type that has at most
// the given number of fractional second digits.
func MakeTimestamp(precision int32) *T {
	return &T{SemanticType: TIMESTAMP, Precision: precision}
}

// MakeTimestampTZ constructs a new instance of a TIMESTAMPTZ type that has at
// most the given number of fractional second digits.
func MakeTimestampTZ(precision int32) *T {
	return &T{SemanticType: TIMESTAMPTZ, Precision: precision}
}

// MakeArray constructs a new instance of an ARRAY type with the given element
// type (which may itself be an ARRAY type).
func MakeArray(typ *T) *T {
	return &T{SemanticType: ARRAY, ArrayContents: typ}
}

// MakeTuple constructs a new instance of a TUPLE type with the given field
// types (some/all of which may be other TUPLE types).
func MakeTuple(contents []T) *T {
	return &T{SemanticType: TUPLE, TupleContents: contents}
}

// MakeLabeledTuple constructs a new instance of a TUPLE type with the given
// field types and labels.
func MakeLabeledTuple(contents []T, labels []string) *T {
	return &T{SemanticType: TUPLE, TupleContents: contents, TupleLabels: labels}
}

// IsAmbiguous returns whether the type is ambiguous or fully defined. This is
// important for parameterized types to determine whether they are fully
// concrete type specification or not.
func (t *ColumnType) IsAmbiguous() bool {
	switch t.SemanticType {
	case UNKNOWN, ANY:
		return true
	case COLLATEDSTRING:
		return *t.Locale == ""
	case TUPLE:
		if len(t.TupleContents) == 0 {
			return true
		}
		for i := range t.TupleContents {
			if t.TupleContents[i].IsAmbiguous() {
				return true
			}
		}
		return false
	case ARRAY:
		return (*t.ArrayContents).IsAmbiguous()
	}
	return false
}

// IsStringType returns true iff t is String
// or a collated string type.
func IsStringType(t *T) bool {
	switch t.SemanticType {
	case STRING, COLLATEDSTRING:
		return true
	default:
		return false
	}
}

// IsValidArrayElementType returns true if the T
// can be used in TArray.
// If the valid return is false, the issue number should
// be included in the error report to inform the user.
func IsValidArrayElementType(t *T) (valid bool, issueNum int) {
	switch t.SemanticType {
	case JSON:
		return false, 23468
	default:
		return true, 0
	}
}

// CheckArrayElementType ensures that the given type can be used as the element
// type of an ARRAY-typed column. If not, it returns an error.
func CheckArrayElementType(t *T) error {
	if ok, issueNum := IsValidArrayElementType(t); !ok {
		return pgerror.UnimplementedWithIssueDetailErrorf(issueNum, t.String(),
			"arrays of %s not allowed", t)
	}
	return nil
}

// IsDateTimeType returns true if the T is
// date- or time-related type.
func IsDateTimeType(t *T) bool {
	switch t.SemanticType {
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

// IsAdditiveType returns true if the T
// supports addition and subtraction.
func IsAdditiveType(t *T) bool {
	switch t.SemanticType {
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

// IsWildcardTupleType returns true if this is the wildcard AnyTuple type. The
// wildcard type matches a tuple type having any number of fields (including 0).
func IsWildcardTupleType(t *T) bool {
	return len(t.TupleContents) == 1 && t.TupleContents[0].SemanticType == ANY
}

func (t *ColumnType) collatedStringTypeSQL(isArray bool) string {
	var buf bytes.Buffer
	buf.WriteString(t.stringTypeSQL())
	if isArray {
		// Brackets must precede the COLLATE identifier.
		buf.WriteString("[] COLLATE ")
	} else {
		buf.WriteString(" COLLATE ")
	}
	lex.EncodeUnrestrictedSQLIdent(&buf, *t.Locale, lex.EncNoFlags)
	return buf.String()
}

// stringTypeSQL returns the visible type name plus any width specifier for the
// given STRING/COLLATEDSTRING column type.
func (t *ColumnType) stringTypeSQL() string {
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
	if t.Width > 0 {
		o := t.Oid()
		if t.Width != 1 || (o != oid.T_bpchar && o != oid.T_char) {
			typName = fmt.Sprintf("%s(%d)", typName, t.Width)
		}
	}

	return typName
}

// SQLStandardName returns the type's name as it is specified in the SQL
// standard. This can be looked up for a type `t` in postgres using this query:
//
//   SELECT format_type(t::regtype, NULL)
//
// TODO(andyk): Reconcile this with InformationSchemaName.
func (t *ColumnType) SQLStandardName() string {
	switch t.SemanticType {
	case BOOL:
		return "boolean"
	case INT:
		return "bigint"
	case FLOAT:
		return "double precision"
	case DECIMAL:
		return "numeric"
	case DATE:
		return "date"
	case TIMESTAMP:
		return "timestamp without time zone"
	case INTERVAL:
		return "interval"
	case STRING, COLLATEDSTRING:
		return "text"
	case BYTES:
		return "bytea"
	case TIMESTAMPTZ:
		return "timestamp with time zone"
	case UNKNOWN:
		return "unknown"
	case UUID:
		return "uuid"
	case ARRAY:
		return t.ArrayContents.SQLStandardName() + "[]"
	case INET:
		return "inet"
	case TIME:
		return "time"
	case JSON:
		return "json"
	case TUPLE:
		return "record"
	case BIT:
		return "bit varying"
	case ANY:
		return "anyelement"
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
	default:
		panic(pgerror.NewAssertionErrorf("unexpected SemanticType: %v", log.Safe(t.SemanticType)))
	}
}

// SQLString returns the CockroachDB native SQL string that can be
// used to reproduce the T (via parsing -> coltypes.T ->
// CastTargetToColumnType -> PopulateAttrs).
//
// Is is used in error messages and also to produce the output
// of SHOW CREATE.
//
// See also InformationSchemaName() below.
func (t *ColumnType) SQLString() string {
	switch t.SemanticType {
	case BIT:
		typName := "BIT"
		if t.Oid() == oid.T_varbit {
			typName = "VARBIT"
		}
		// BIT(1) pretty-prints as just BIT.
		if (t.Oid() != oid.T_varbit && t.Width > 1) ||
			(t.Oid() == oid.T_varbit && t.Width > 0) {
			typName = fmt.Sprintf("%s(%d)", typName, t.Width)
		}
		return typName
	case INT:
		// Pre-2.1 BIT was using column type INT with arbitrary width We
		// map this to INT now. See #34161.
		width := t.Width
		if width != 0 && width != 64 && width != 32 && width != 16 {
			width = 64
		}
		if name, ok := integerTypeNames[int(width)]; ok {
			return name
		}
	case STRING:
		return t.stringTypeSQL()
	case COLLATEDSTRING:
		return t.collatedStringTypeSQL(false /* isArray */)
	case FLOAT:
		const realName = "FLOAT4"
		const doubleName = "FLOAT8"
		if t.Width == 32 {
			return realName
		}
		return doubleName
	case DECIMAL:
		if t.Precision > 0 {
			if t.Width > 0 {
				return fmt.Sprintf("%s(%d,%d)", t.SemanticType.String(), t.Precision, t.Width)
			}
			return fmt.Sprintf("%s(%d)", t.SemanticType.String(), t.Precision)
		}
	case JSON:
		// Only binary JSON is currently supported.
		return "JSONB"
	case TIME, TIMESTAMP, TIMESTAMPTZ:
		if t.Precision > 0 {
			return fmt.Sprintf("%s(%d)", t.SemanticType.String(), t.Precision)
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
		if t.ArrayContents.SemanticType == COLLATEDSTRING {
			return t.ArrayContents.collatedStringTypeSQL(true /* isArray */)
		}
		return t.ArrayContents.SQLString() + "[]"
	}
	return t.SemanticType.String()
}

// InformationSchemaName returns the string suitable to populate the data_type
// column of information_schema.columns.
//
// This is different from SQLString() in that it must report SQL standard names
// that are compatible with PostgreSQL client expectations.
func (t *ColumnType) InformationSchemaName() string {
	switch t.SemanticType {
	case BOOL:
		return "boolean"

	case BIT:
		if t.Oid() == oid.T_varbit {
			return "bit varying"
		}
		return "bit"

	case INT:
		switch t.Width {
		case 16:
			return "smallint"
		case 64:
			return "bigint"
		default:
			// We report "integer" both for int4 and int.  This is probably
			// lying a bit, but it will appease clients that feed "int" into
			// their CREATE TABLE and expect the pg "integer" name to come
			// up in information_schema.
			return "integer"
		}

	case STRING, COLLATEDSTRING:
		switch t.Oid() {
		case oid.T_varchar:
			return "character varying"
		case oid.T_bpchar:
			return "character"
		case oid.T_char:
			// Not the same as "character". Beware.
			return `"char"`
		}
		return "text"

	case FLOAT:
		switch t.Width {
		case 32:
			return "real"
		case 64:
			return "double precision"
		default:
			panic(fmt.Sprintf("programming error: unknown float width: %d", t.Width))
		}

	case DECIMAL:
		return "numeric"
	case TIMESTAMP:
		return "timestamp without time zone"
	case TIMESTAMPTZ:
		return "timestamp with time zone"
	case TIME:
		return "time without time zone"
	case BYTES:
		return "bytea"
	case JSON:
		// Only binary JSON is currently supported.
		return "jsonb"
	case UNKNOWN:
		return "unknown"
	case TUPLE:
		return "record"
	case ARRAY:
		return "ARRAY"
	}

	// The name of the remaining semantic type constants are suitable
	// for the data_type column in information_schema.columns.
	return strings.ToLower(t.SemanticType.String())
}

// MaxCharacterLength returns the declared maximum length of
// characters if the T is a character or bit string data
// type. Returns false if the data type is not a character or bit
// string, or if the string's length is not bounded.
//
// This is used to populate information_schema.columns.character_maximum_length;
// do not modify this function unless you also check that the values
// generated in information_schema are compatible with client
// expectations.
func (t *ColumnType) MaxCharacterLength() (int32, bool) {
	switch t.SemanticType {
	case STRING, COLLATEDSTRING, BIT:
		if t.Width > 0 {
			return t.Width, true
		}
	}
	return 0, false
}

// MaxOctetLength returns the maximum possible length in
// octets of a datum if the T is a character string. Returns
// false if the data type is not a character string, or if the
// string's length is not bounded.
//
// This is used to populate information_schema.columns.character_octet_length;
// do not modify this function unless you also check that the values
// generated in information_schema are compatible with client
// expectations.
func (t *ColumnType) MaxOctetLength() (int32, bool) {
	switch t.SemanticType {
	case STRING, COLLATEDSTRING:
		if t.Width > 0 {
			return t.Width * utf8.UTFMax, true
		}
	}
	return 0, false
}

// NumericPrecision returns the declared or implicit precision of numeric
// data types. Returns false if the data type is not numeric, or if the precision
// of the numeric type is not bounded.
//
// This is used to populate information_schema.columns.numeric_precision;
// do not modify this function unless you also check that the values
// generated in information_schema are compatible with client
// expectations.
func (t *ColumnType) NumericPrecision() (int32, bool) {
	switch t.SemanticType {
	case INT:
		// Assume 64-bit integer if no width is specified.
		if t.Width == 0 {
			return 64, true
		}
		return t.Width, true
	case FLOAT:
		if t.Width == 32 {
			return 24, true
		}
		return 53, true
	case DECIMAL:
		if t.Precision > 0 {
			return t.Precision, true
		}
	}
	return 0, false
}

// NumericPrecisionRadix returns the implicit precision radix of
// numeric data types. Returns false if the data type is not numeric.
//
// This is used to populate information_schema.columns.numeric_precision_radix;
// do not modify this function unless you also check that the values
// generated in information_schema are compatible with client
// expectations.
func (t *ColumnType) NumericPrecisionRadix() (int32, bool) {
	switch t.SemanticType {
	case INT:
		return 2, true
	case FLOAT:
		return 2, true
	case DECIMAL:
		return 10, true
	}
	return 0, false
}

// NumericScale returns the declared or implicit precision of exact numeric
// data types. Returns false if the data type is not an exact numeric, or if the
// scale of the exact numeric type is not bounded.
//
// This is used to populate information_schema.columns.numeric_scale;
// do not modify this function unless you also check that the values
// generated in information_schema are compatible with client
// expectations.
func (t *ColumnType) NumericScale() (int32, bool) {
	switch t.SemanticType {
	case INT:
		return 0, true
	case DECIMAL:
		if t.Precision > 0 {
			return t.Width, true
		}
	}
	return 0, false
}

// IntegerTypeNames maps a TInt data width to a canonical type name.
var integerTypeNames = map[int]string{
	0:  "INT",
	16: "INT2",
	32: "INT4",
	64: "INT8",
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
// 0 if no error or the type is not known in postgres.
// -1 if the type is known in postgres.
// >0 for a github issue number.
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
