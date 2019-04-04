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

// T wraps the Protobuf-generated InternalType so that it can override the
// Marshal/Unmarshal methods in order to map to/from older persisted ColumnType
// representations.
type T struct {
	// InternalType should never be directly referenced outside this package. The
	// only reason it is exported is because gogoproto panics when printing the
	// string representation of an unexported field. This is a problem when this
	// struct is embedded in a larger struct (like a ColumnDescriptor).
	InternalType InternalType
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
func (t *T) Identical(other *T) bool {
	return t.InternalType.Identical(&other.InternalType)
}

// Equivalent returns whether the receiver and the other type are equivalent.
// We say that two type patterns are "equivalent" when they are structurally
// equivalent given that a wildcard is equivalent to any type. When neither
// Type is ambiguous (see IsAmbiguous), equivalency is the same as type
// equality.
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

// Size delegates to InternalType.
func (t *T) Size() (n int) {
	temp := *t
	if err := temp.downgradeType(); err != nil {
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

// Unmarshal deserializes a ColumnType from the given bytes.
func (t *T) Unmarshal(data []byte) error {
	err := protoutil.Unmarshal(data, &t.InternalType)
	if err != nil {
		return err
	}
	return t.upgradeType()
}

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
		default:
			// Pre-2.1 BIT was using column type INT with arbitrary widths. Clamp
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
		}

	case FLOAT:
		// Map visible REAL type to 32-bit width.
		if t.InternalType.VisibleType == visibleREAL {
			t.InternalType.Oid = oid.T_float4
			t.InternalType.Width = 32
		} else {
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
		}

		// Precision should always be set to 0 going forward.
		t.InternalType.Precision = 0

	case STRING, COLLATEDSTRING:
		switch t.InternalType.VisibleType {
		case visibleVARCHAR:
			t.InternalType.Oid = oid.T_varchar
		case visibleCHAR:
			t.InternalType.Oid = oid.T_bpchar
		case visibleQCHAR:
			t.InternalType.Oid = oid.T_char
		default:
			t.InternalType.Oid = oid.T_text
		}

	case BIT:
		if t.InternalType.VisibleType == visibleVARBIT {
			t.InternalType.Oid = oid.T_varbit
		} else {
			t.InternalType.Oid = oid.T_bit
		}

	case ARRAY:
		if t.ArrayContents() == nil {
			arrayContents := *t
			arrayContents.InternalType.SemanticType = *t.InternalType.ArrayElemType
			arrayContents.InternalType.ArrayDimensions = nil
			arrayContents.InternalType.ArrayElemType = nil
			if err := arrayContents.upgradeType(); err != nil {
				return err
			}
			t.InternalType.ArrayContents = &arrayContents
		}

		// Marshaling/unmarshaling nested arrays is not yet supported.
		if t.ArrayContents().SemanticType() == ARRAY {
			return pgerror.NewAssertionErrorf("nested array should never be unmarshaled")
		}

		// Zero out fields that may have been used to store information about
		// the array element type, or which are no longer in use.
		t.InternalType.Width = 0
		t.InternalType.Precision = 0
		t.InternalType.Locale = nil
		t.InternalType.VisibleType = 0
		t.InternalType.ArrayElemType = nil
		t.InternalType.ArrayDimensions = nil
		t.InternalType.Oid = calcArrayOid(t.ArrayContents())

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

	default:
		t.InternalType.Oid = semanticTypeToOid[t.SemanticType()]
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

// Marshal serializes the ColumnType to bytes.
func (t *T) Marshal() (data []byte, err error) {
	temp := *t
	if err := temp.downgradeType(); err != nil {
		return nil, err
	}
	return protoutil.Marshal(&temp.InternalType)
}

// MarshalTo serializes the ColumnType to the given byte slice.
func (t *T) MarshalTo(data []byte) (int, error) {
	temp := *t
	if err := temp.downgradeType(); err != nil {
		return 0, err
	}
	return temp.InternalType.MarshalTo(data)
}

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
		case oid.T_varchar:
			t.InternalType.VisibleType = visibleVARCHAR
		case oid.T_bpchar:
			t.InternalType.VisibleType = visibleCHAR
		case oid.T_char:
			t.InternalType.VisibleType = visibleQCHAR
		case oid.T_name:
			t.InternalType.SemanticType = name
		}

	case ARRAY:
		// Marshaling/unmarshaling nested arrays is not yet supported.
		if t.ArrayContents().SemanticType() == ARRAY {
			return pgerror.NewAssertionErrorf("nested array should never be marshaled")
		}

		// Downgrade to ARRAY representation used before 19.2, in which the array
		// type fields specified the width, locale, etc. of the element type.
		temp := *t.InternalType.ArrayContents
		if err := temp.downgradeType(); err != nil {
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

	// Map empty locale to nil.
	if t.InternalType.Locale != nil && len(*t.InternalType.Locale) == 0 {
		t.InternalType.Locale = nil
	}

	return nil
}

// DebugString returns a detailed dump of the type protobuf struct, suitable for
// debugging scenarios.
func (t *T) DebugString() string {
	return t.InternalType.String()
}

// Name returns a single word description of the type that describes it
// succinctly, but without all the details, such as width, locale, etc. The name
// is sometimes the same as the name returned by SQLStandardName, but is more
// CRDB friendly.
//
// TODO(andyk): Should these be changed to be the same as SQLStandardName?
func (t *T) Name() string {
	switch t.SemanticType() {
	case UNKNOWN:
		return "unknown"
	case BOOL:
		return "bool"
	case INT:
		switch t.Width() {
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
		return t.ArrayContents().Name() + "[]"
	case ANY:
		return "anyelement"
	case OID:
		return t.SQLStandardName()
	}

	panic(pgerror.NewAssertionErrorf("unexpected SemanticType: %s", t.SemanticType()))
}

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
	}
	return t.Name()
}

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
		SemanticType: TUPLE, Oid: oid.T_record, TupleContents: []T{*Any}, Locale: &emptyLocale}}

	// AnyCollatedString is the collated string type with a nil locale. It is
	// used as a wildcard parameterized type that matches any locale.
	AnyCollatedString = &T{InternalType: InternalType{
		SemanticType: COLLATEDSTRING, Oid: oid.T_text, Locale: &emptyLocale}}

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
	EmptyTuple = &T{InternalType: InternalType{
		SemanticType: TUPLE, Oid: oid.T_record, Locale: &emptyLocale}}

	// StringArray is the type of an ARRAY value having String-typed elements.
	StringArray = &T{InternalType: InternalType{
		SemanticType: ARRAY, ArrayContents: String, Oid: oid.T__text, Locale: &emptyLocale}}

	// IntArray is the type of an ARRAY value having Int-typed elements.
	IntArray = &T{InternalType: InternalType{
		SemanticType: ARRAY, ArrayContents: Int, Oid: oid.T__int8, Locale: &emptyLocale}}

	// DecimalArray is the type of an ARRAY value having Decimal-typed elements.
	DecimalArray = &T{InternalType: InternalType{
		SemanticType: ARRAY, ArrayContents: Decimal, Oid: oid.T__numeric, Locale: &emptyLocale}}
)

var (
	emptyLocale = ""
)

// Unexported wrapper types.
var (
	// typeBit is not exported to avoid confusion over whether its default Width
	// is unspecified or is 1.
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

// MakeScalar constructs a new instance of a scalar type (i.e. not ARRAY or
// TUPLE types) using the provided fields.
func MakeScalar(semTyp SemanticType, o oid.Oid, precision, width int32, locale string) *T {
	t := OidToType[o]
	if semTyp != t.SemanticType() {
		panic(pgerror.NewAssertionErrorf(
			"oid %s does not match SemanticType %s", oid.TypeName[o], semTyp))
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
	return &T{InternalType: InternalType{
		SemanticType: BIT, Oid: oid.T_bit, Width: width, Locale: &emptyLocale}}
}

// MakeVarBit constructs a new instance of the BIT type (oid = T_varbit) having
// the given max # bits (0 = unspecified number).
func MakeVarBit(width int32) *T {
	if width == 0 {
		return VarBit
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
	return &T{InternalType: InternalType{
		SemanticType: STRING, Oid: oid.T_text, Width: width, Locale: &emptyLocale}}
}

// MakeVarChar constructs a new instance of the VARCHAR type (oid = T_varchar)
// having the given max # characters (0 = unspecified number).
func MakeVarChar(width int32) *T {
	if width == 0 {
		return VarChar
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
	return &T{InternalType: InternalType{
		SemanticType: TIME, Oid: oid.T_time, Precision: precision, Locale: &emptyLocale}}
}

// MakeTimestamp constructs a new instance of a TIMESTAMP type that has at most
// the given number of fractional second digits.
func MakeTimestamp(precision int32) *T {
	if precision == 0 {
		return Timestamp
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
	return &T{InternalType: InternalType{
		SemanticType:  TUPLE,
		Oid:           oid.T_record,
		TupleContents: contents,
		TupleLabels:   labels,
		Locale:        &emptyLocale,
	}}
}

// IsAmbiguous returns whether the type is ambiguous or fully defined. This is
// important for parameterized types to determine whether they are fully
// concrete type specification or not.
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

// IsStringType returns true iff t is String
// or a collated string type.
func IsStringType(t *T) bool {
	switch t.SemanticType() {
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
	switch t.SemanticType() {
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

// IsAdditiveType returns true if the T
// supports addition and subtraction.
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

// IsWildcardTupleType returns true if this is the wildcard AnyTuple type. The
// wildcard type matches a tuple type having any number of fields (including 0).
func IsWildcardTupleType(t *T) bool {
	return len(t.TupleContents()) == 1 && t.TupleContents()[0].SemanticType() == ANY
}

func (t *T) collatedStringTypeSQL(isArray bool) string {
	var buf bytes.Buffer
	buf.WriteString(t.stringTypeSQL())
	if isArray {
		// Brackets must precede the COLLATE identifier.
		buf.WriteString("[] COLLATE ")
	} else {
		buf.WriteString(" COLLATE ")
	}
	lex.EncodeUnrestrictedSQLIdent(&buf, t.Locale(), lex.EncNoFlags)
	return buf.String()
}

// stringTypeSQL returns the visible type name plus any width specifier for the
// given STRING/COLLATEDSTRING column type.
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

// SQLStandardName returns the type's name as it is specified in the SQL
// standard. This can be looked up for a type `t` in postgres using this query:
//
//   SELECT format_type(t::regtype, NULL)
//
// TODO(andyk): Reconcile this with InformationSchemaName.
func (t *T) SQLStandardName() string {
	switch t.SemanticType() {
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
		return t.ArrayContents().SQLStandardName() + "[]"
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
		panic(pgerror.NewAssertionErrorf(
			"unexpected SemanticType: %v", log.Safe(t.SemanticType())))
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
		default:
			return "INT8"
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

// InformationSchemaName returns the string suitable to populate the data_type
// column of information_schema.columns.
//
// This is different from SQLString() in that it must report SQL standard names
// that are compatible with PostgreSQL client expectations.
func (t *T) InformationSchemaName() string {
	switch t.SemanticType() {
	case BOOL:
		return "boolean"

	case BIT:
		if t.Oid() == oid.T_varbit {
			return "bit varying"
		}
		return "bit"

	case INT:
		switch t.Width() {
		case 16:
			return "smallint"
		case 32:
			// PG shows "integer" for int4.
			return "integer"
		case 64:
			return "bigint"
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
		switch t.Width() {
		case 32:
			return "real"
		case 64:
			return "double precision"
		default:
			panic(fmt.Sprintf("programming error: unknown float width: %d", t.Width()))
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
	return strings.ToLower(t.SemanticType().String())
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
func (t *T) MaxCharacterLength() (int32, bool) {
	switch t.SemanticType() {
	case STRING, COLLATEDSTRING, BIT:
		if t.Width() > 0 {
			return t.Width(), true
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
func (t *T) MaxOctetLength() (int32, bool) {
	switch t.SemanticType() {
	case STRING, COLLATEDSTRING:
		if t.Width() > 0 {
			return t.Width() * utf8.UTFMax, true
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
func (t *T) NumericPrecision() (int32, bool) {
	switch t.SemanticType() {
	case INT:
		return t.Width(), true
	case FLOAT:
		if t.Width() == 32 {
			return 24, true
		}
		return 53, true
	case DECIMAL:
		if t.Precision() > 0 {
			return t.Precision(), true
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
func (t *T) NumericPrecisionRadix() (int32, bool) {
	switch t.SemanticType() {
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
func (t *T) NumericScale() (int32, bool) {
	switch t.SemanticType() {
	case INT:
		return 0, true
	case DECIMAL:
		if t.Precision() > 0 {
			return t.Width(), true
		}
	}
	return 0, false
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
