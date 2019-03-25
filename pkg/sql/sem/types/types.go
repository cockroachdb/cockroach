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
	"math"
	"strings"
	"unicode/utf8"

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
	visibleType_INTEGER = 1

	// Deprecated after 2.1, since it's now represented using the Width field.
	visibleType_SMALLINT = 2

	// Deprecated after 19.1, since it's now represented using the Width field.
	visibleType_REAL = 5

	// Deprecated after 19.1, since it's now represented using the Oid field.
	visibleType_VARCHAR = 7

	// Deprecated after 19.1, since it's now represented using the Oid field.
	visibleType_CHAR = 8

	// Deprecated after 19.1, since it's now represented using the Oid field.
	visibleType_QCHAR = 9

	// Deprecated after 19.1, since it's now represented using the Oid field.
	visibleType_VARBIT = 10
)

// T represents a SQL type.
type T interface {
	fmt.Stringer

	// SemanticType is temporary.
	// TODO(andyk): Remove in future commit.
	SemanticType() SemanticType

	// Equivalent returns whether the receiver and the other type are equivalent.
	// We say that two type patterns are "equivalent" when they are structurally
	// equivalent given that a wildcard is equivalent to any type. When neither
	// Type is ambiguous (see IsAmbiguous), equivalency is the same as type equality.
	Equivalent(other T) bool

	// Oid returns the type's Postgres object ID.
	Oid() oid.Oid

	// SQLName returns the type's SQL standard name. This can be looked up for a
	// type `t` in postgres by running `SELECT format_type(t::regtype, NULL)`.
	SQLName() string

	// IsAmbiguous returns whether the type is ambiguous or fully defined. This
	// is important for parameterized types to determine whether they are fully
	// concrete type specification or not.
	IsAmbiguous() bool
}

// ColumnType wraps the Protobuf-generated InternalColumnType so that it can
// override the Marshal/Unmarshal methods in order to map to/from older
// persisted ColumnType representations.
type ColumnType InternalColumnType

// Oid returns the type's Postgres object ID.
func (t *ColumnType) Oid() oid.Oid {
	if t.XXX_Oid != 0 {
		return t.XXX_Oid
	}
	switch t.SemanticType {
	case ARRAY:
		// TODO(andyk): Temporary hack for this commit; will be removed.
		temp := *t
		temp.SemanticType = *t.ArrayContents
		return oidToArrayOid[temp.Oid()]
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
	return (*InternalColumnType)(t).Size()
}

// Identical returns true if every field in this ColumnType is exactly the same
// as every corresponding field in the given ColumnType. Identical performs a
// deep comparison, traversing any Tuple or Array contents.
func (c *ColumnType) Identical(other *ColumnType) bool {
	if c.SemanticType != other.SemanticType {
		return false
	}
	if c.Width != other.Width {
		return false
	}
	if c.Precision != other.Precision {
		return false
	}
	if len(c.ArrayDimensions) != len(other.ArrayDimensions) {
		return false
	}
	for i := range c.ArrayDimensions {
		if c.ArrayDimensions[i] != other.ArrayDimensions[i] {
			return false
		}
	}
	if c.Locale != nil && other.Locale != nil {
		if *c.Locale != *other.Locale {
			return false
		}
	} else if c.Locale != nil {
		return false
	} else if other.Locale != nil {
		return false
	}
	if c.ArrayContents != nil && other.ArrayContents != nil {
		if *c.ArrayContents != *other.ArrayContents {
			return false
		}
	} else if c.ArrayContents != nil {
		return false
	} else if other.ArrayContents != nil {
		return false
	}
	if len(c.TupleContents) != len(other.TupleContents) {
		return false
	}
	for i := range c.TupleContents {
		if !c.TupleContents[i].Identical(&other.TupleContents[i]) {
			return false
		}
	}
	if len(c.TupleLabels) != len(other.TupleLabels) {
		return false
	}
	for i := range c.TupleLabels {
		if c.TupleLabels[i] != other.TupleLabels[i] {
			return false
		}
	}
	if c.XXX_Oid != other.XXX_Oid {
		return false
	}
	return true
}

// Unmarshal deserializes a ColumnType from the given bytes.
func (t *ColumnType) Unmarshal(data []byte) error {
	err := (*InternalColumnType)(t).Unmarshal(data)
	if err != nil {
		return err
	}

	switch t.SemanticType {
	case INT:
		// Check VisibleType field that was populated in previous versions.
		switch t.XXX_VisibleType {
		case visibleType_SMALLINT:
			t.Width = 16
		case visibleType_INTEGER:
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
		if t.XXX_VisibleType == visibleType_REAL {
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
	case STRING:
		switch t.XXX_VisibleType {
		case visibleType_VARCHAR:
			t.XXX_Oid = oid.T_varchar
		case visibleType_CHAR:
			t.XXX_Oid = oid.T_bpchar
		case visibleType_QCHAR:
			t.XXX_Oid = oid.T_char
		}
	case BIT:
		if t.XXX_VisibleType == visibleType_VARBIT {
			t.XXX_Oid = oid.T_varbit
		}
	case int2vector:
		t.SemanticType = ARRAY
		t.XXX_Oid = oid.T_int2vector
		contents := INT
		t.ArrayContents = &contents
	case oidvector:
		t.SemanticType = ARRAY
		t.XXX_Oid = oid.T_oidvector
		contents := OID
		t.ArrayContents = &contents
	case name:
		t.SemanticType = STRING
		t.XXX_Oid = oid.T_name
	}

	// Clear any visible type, since they are all now handled by the Width or
	// Oid fields.
	t.XXX_VisibleType = VisibleType_NONE

	return nil
}

// Marsh serializes the ColumnType to bytes.
func (t *ColumnType) Marshal() (data []byte, err error) {
	size := (*InternalColumnType)(t).Size()
	data = make([]byte, size)
	n, err := t.MarshalTo(data)
	if err != nil {
		return nil, err
	}
	return data[:n], nil
}

// Marsh serializes the ColumnType to the given byte slice.
func (t *ColumnType) MarshalTo(data []byte) (int, error) {
	temp := *t

	// Set SemanticType and VisibleType for 19.1 backwards-compatibility.
	switch temp.SemanticType {
	case BIT:
		if temp.XXX_Oid == oid.T_varbit {
			temp.XXX_VisibleType = visibleType_VARBIT
		}
	case FLOAT:
		switch temp.Width {
		case 32:
			temp.XXX_VisibleType = visibleType_REAL
		}
	case STRING:
		switch temp.XXX_Oid {
		case oid.T_varchar:
			temp.XXX_VisibleType = visibleType_VARCHAR
		case oid.T_bpchar:
			temp.XXX_VisibleType = visibleType_CHAR
		case oid.T_char:
			temp.XXX_VisibleType = visibleType_QCHAR
		case oid.T_name:
			temp.SemanticType = name
		}
	case ARRAY:
		switch temp.XXX_Oid {
		case oid.T_int2vector:
			temp.SemanticType = int2vector
		case oid.T_oidvector:
			temp.SemanticType = oidvector
		}
	}

	return (*InternalColumnType)(&temp).MarshalTo(data)
}

func (t *ColumnType) String() string {
	return (*InternalColumnType)(t).String()
}

var (
	// Unknown is the type of an expression that statically evaluates to
	// NULL.
	Unknown T = tUnknown{}
	// Bool is the type of a DBool.
	Bool T = tBool{}
	// BitArray is the type of a DBitArray.
	BitArray T = tBitArray{}
	// Int is the type of a DInt.
	Int T = tInt{}
	// Float is the type of a DFloat.
	Float T = tFloat{}
	// Decimal is the type of a DDecimal.
	Decimal T = tDecimal{}

	// String is the canonical CockroachDB string type.
	//
	// It is reported as STRING in SHOW CREATE but "text" in introspection for
	// compatibility with PostgreSQL.
	//
	// It has no default maximum width.
	String T = tString{}

	// Bytes is the type of a DBytes.
	Bytes T = tBytes{}
	// Date is the type of a DDate.
	Date T = tDate{}
	// Time is the type of a DTime.
	Time T = tTime{}
	// Timestamp is the type of a DTimestamp. Can be compared with ==.
	Timestamp T = tTimestamp{}
	// TimestampTZ is the type of a DTimestampTZ. Can be compared with ==.
	TimestampTZ T = tTimestampTZ{}
	// Interval is the type of a DInterval. Can be compared with ==.
	Interval T = tInterval{}
	// Jsonb is the type of a DJSON. Can be compared with ==.
	Jsonb T = tJSON{}
	// UUID is the type of a DUuid. Can be compared with ==.
	Uuid T = tUUID{}
	// INet is the type of a DIPAddr. Can be compared with ==.
	INet T = tINet{}
	// AnyArray is the type of a DArray with a wildcard parameterized type.
	// Can be compared with ==.
	AnyArray T = TArray{Any}
	// Any can be any type. Can be compared with ==.
	Any T = tAny{}

	// AnyNonArray contains all non-array types.
	AnyNonArray = []T{
		Bool,
		BitArray,
		Int,
		Float,
		Decimal,
		String,
		Bytes,
		Date,
		Time,
		Timestamp,
		TimestampTZ,
		Interval,
		Uuid,
		INet,
		Jsonb,
		Oid,
	}

	// EmptyTuple is the tuple type with no fields.
	EmptyTuple T = TTuple{}

	// EmptyCollatedString is the collated string type with an empty locale.
	EmptyCollatedString T = TCollatedString{}
)

func isTypeOrAny(typ, isTyp SemanticType) bool {
	return typ == isTyp || typ == ANY
}

// Do not instantiate the tXxx types elsewhere. The variables above are intended
// to be singletons.
type tUnknown struct{}

func (tUnknown) SemanticType() SemanticType { return NULL }
func (tUnknown) String() string             { return "unknown" }
func (tUnknown) Equivalent(other T) bool    { return isTypeOrAny(other.SemanticType(), NULL) }
func (tUnknown) Oid() oid.Oid               { return oid.T_unknown }
func (tUnknown) SQLName() string            { return "unknown" }
func (tUnknown) IsAmbiguous() bool          { return true }

type tBool struct{}

func (tBool) SemanticType() SemanticType { return BOOL }
func (tBool) String() string             { return "bool" }
func (tBool) Equivalent(other T) bool    { return isTypeOrAny(other.SemanticType(), BOOL) }
func (tBool) Oid() oid.Oid               { return oid.T_bool }
func (tBool) SQLName() string            { return "boolean" }
func (tBool) IsAmbiguous() bool          { return false }

type tInt struct{}

func (tInt) SemanticType() SemanticType { return INT }
func (tInt) String() string             { return "int" }
func (tInt) Equivalent(other T) bool    { return isTypeOrAny(other.SemanticType(), INT) }
func (tInt) Oid() oid.Oid               { return oid.T_int8 }
func (tInt) SQLName() string            { return "bigint" }
func (tInt) IsAmbiguous() bool          { return false }

type tBitArray struct{}

func (tBitArray) SemanticType() SemanticType { return BIT }
func (tBitArray) String() string             { return "varbit" }
func (tBitArray) Equivalent(other T) bool    { return isTypeOrAny(other.SemanticType(), BIT) }
func (tBitArray) Oid() oid.Oid               { return oid.T_varbit }
func (tBitArray) SQLName() string            { return "bit varying" }
func (tBitArray) IsAmbiguous() bool          { return false }

type tFloat struct{}

func (tFloat) SemanticType() SemanticType { return FLOAT }
func (tFloat) String() string             { return "float" }
func (tFloat) Equivalent(other T) bool    { return isTypeOrAny(other.SemanticType(), FLOAT) }
func (tFloat) Oid() oid.Oid               { return oid.T_float8 }
func (tFloat) SQLName() string            { return "double precision" }
func (tFloat) IsAmbiguous() bool          { return false }

type tDecimal struct{}

func (tDecimal) SemanticType() SemanticType { return DECIMAL }
func (tDecimal) String() string             { return "decimal" }
func (tDecimal) Equivalent(other T) bool    { return isTypeOrAny(other.SemanticType(), DECIMAL) }
func (tDecimal) Oid() oid.Oid               { return oid.T_numeric }
func (tDecimal) SQLName() string            { return "numeric" }
func (tDecimal) IsAmbiguous() bool          { return false }

type tString struct{}

func (tString) SemanticType() SemanticType { return STRING }
func (tString) String() string             { return "string" }
func (tString) Equivalent(other T) bool    { return isTypeOrAny(other.SemanticType(), STRING) }
func (tString) Oid() oid.Oid               { return oid.T_text }
func (tString) SQLName() string            { return "text" }
func (tString) IsAmbiguous() bool          { return false }

// TCollatedString is the type of strings with a locale.
type TCollatedString struct {
	Locale string
}

func (TCollatedString) SemanticType() SemanticType { return COLLATEDSTRING }

// String implements the fmt.Stringer interface.
func (t TCollatedString) String() string {
	if t.Locale == "" {
		// Used in telemetry.
		return "collatedstring{*}"
	}
	return fmt.Sprintf("collatedstring{%s}", t.Locale)
}

// Equivalent implements the T interface.
func (t TCollatedString) Equivalent(other T) bool {
	if other.SemanticType() == ANY {
		return true
	}
	u, ok := UnwrapType(other).(TCollatedString)
	if ok {
		return t.Locale == "" || u.Locale == "" || t.Locale == u.Locale
	}
	return false
}

// Oid implements the T interface.
func (TCollatedString) Oid() oid.Oid { return oid.T_text }

// SQLName implements the T interface.
func (TCollatedString) SQLName() string { return "text" }

// IsAmbiguous implements the T interface.
func (t TCollatedString) IsAmbiguous() bool {
	return t.Locale == ""
}

type tBytes struct{}

func (tBytes) SemanticType() SemanticType { return BYTES }
func (tBytes) String() string             { return "bytes" }
func (tBytes) Equivalent(other T) bool    { return isTypeOrAny(other.SemanticType(), BYTES) }
func (tBytes) Oid() oid.Oid               { return oid.T_bytea }
func (tBytes) SQLName() string            { return "bytea" }
func (tBytes) IsAmbiguous() bool          { return false }

type tDate struct{}

func (tDate) SemanticType() SemanticType { return DATE }
func (tDate) String() string             { return "date" }
func (tDate) Equivalent(other T) bool    { return isTypeOrAny(other.SemanticType(), DATE) }
func (tDate) Oid() oid.Oid               { return oid.T_date }
func (tDate) SQLName() string            { return "date" }
func (tDate) IsAmbiguous() bool          { return false }

type tTime struct{}

func (tTime) SemanticType() SemanticType { return TIME }
func (tTime) String() string             { return "time" }
func (tTime) Equivalent(other T) bool    { return isTypeOrAny(other.SemanticType(), TIME) }
func (tTime) Oid() oid.Oid               { return oid.T_time }
func (tTime) SQLName() string            { return "time" }
func (tTime) IsAmbiguous() bool          { return false }

type tTimestamp struct{}

func (tTimestamp) SemanticType() SemanticType { return TIMESTAMP }
func (tTimestamp) String() string             { return "timestamp" }
func (tTimestamp) Equivalent(other T) bool    { return isTypeOrAny(other.SemanticType(), TIMESTAMP) }
func (tTimestamp) Oid() oid.Oid               { return oid.T_timestamp }
func (tTimestamp) SQLName() string            { return "timestamp without time zone" }
func (tTimestamp) IsAmbiguous() bool          { return false }

type tTimestampTZ struct{}

func (tTimestampTZ) SemanticType() SemanticType { return TIMESTAMPTZ }
func (tTimestampTZ) String() string             { return "timestamptz" }
func (tTimestampTZ) Equivalent(other T) bool    { return isTypeOrAny(other.SemanticType(), TIMESTAMPTZ) }
func (tTimestampTZ) Oid() oid.Oid               { return oid.T_timestamptz }
func (tTimestampTZ) SQLName() string            { return "timestamp with time zone" }
func (tTimestampTZ) IsAmbiguous() bool          { return false }

type tInterval struct{}

func (tInterval) SemanticType() SemanticType { return INTERVAL }
func (tInterval) String() string             { return "interval" }
func (tInterval) Equivalent(other T) bool    { return isTypeOrAny(other.SemanticType(), INTERVAL) }
func (tInterval) Oid() oid.Oid               { return oid.T_interval }
func (tInterval) SQLName() string            { return "interval" }
func (tInterval) IsAmbiguous() bool          { return false }

type tJSON struct{}

func (tJSON) SemanticType() SemanticType { return JSON }
func (tJSON) String() string             { return "jsonb" }
func (tJSON) Equivalent(other T) bool    { return isTypeOrAny(other.SemanticType(), JSON) }
func (tJSON) Oid() oid.Oid               { return oid.T_jsonb }
func (tJSON) SQLName() string            { return "json" }
func (tJSON) IsAmbiguous() bool          { return false }

type tUUID struct{}

func (tUUID) SemanticType() SemanticType { return UUID }
func (tUUID) String() string             { return "uuid" }
func (tUUID) Equivalent(other T) bool    { return isTypeOrAny(other.SemanticType(), UUID) }
func (tUUID) Oid() oid.Oid               { return oid.T_uuid }
func (tUUID) SQLName() string            { return "uuid" }
func (tUUID) IsAmbiguous() bool          { return false }

type tINet struct{}

func (tINet) SemanticType() SemanticType { return INET }
func (tINet) String() string             { return "inet" }
func (tINet) Equivalent(other T) bool    { return isTypeOrAny(other.SemanticType(), INET) }
func (tINet) Oid() oid.Oid               { return oid.T_inet }
func (tINet) SQLName() string            { return "inet" }
func (tINet) IsAmbiguous() bool          { return false }

// TTuple is the type of a DTuple.
type TTuple struct {
	Types  []T
	Labels []string
}

func (TTuple) SemanticType() SemanticType { return TUPLE }

// String implements the fmt.Stringer interface.
func (t TTuple) String() string {
	var buf bytes.Buffer
	buf.WriteString("tuple")
	if t.Types != nil {
		buf.WriteByte('{')
		for i, typ := range t.Types {
			if i != 0 {
				buf.WriteString(", ")
			}
			buf.WriteString(typ.String())
			if t.Labels != nil {
				buf.WriteString(" AS ")
				buf.WriteString(t.Labels[i])
			}
		}
		buf.WriteByte('}')
	}
	return buf.String()
}

// Equivalent implements the T interface.
func (t TTuple) Equivalent(other T) bool {
	if other.SemanticType() == ANY {
		return true
	}
	u, ok := UnwrapType(other).(TTuple)
	if !ok {
		return false
	}
	if len(t.Types) == 0 || len(u.Types) == 0 {
		// Tuples that aren't fully specified (have a nil subtype list) are always
		// equivalent to other tuples, to allow overloads to specify that they take
		// an arbitrary tuple type.
		return true
	}
	if len(t.Types) != len(u.Types) {
		return false
	}
	for i, typ := range t.Types {
		if !typ.Equivalent(u.Types[i]) {
			return false
		}
	}
	return true
}

// Oid implements the T interface.
func (TTuple) Oid() oid.Oid { return oid.T_record }

// SQLName implements the T interface.
func (TTuple) SQLName() string { return "record" }

// IsAmbiguous implements the T interface.
func (t TTuple) IsAmbiguous() bool {
	for _, typ := range t.Types {
		if typ == nil || typ.IsAmbiguous() {
			return true
		}
	}
	return len(t.Types) == 0
}

// PlaceholderIdx is the 0-based index of a placeholder. Placeholder "$1"
// has PlaceholderIdx=0.
type PlaceholderIdx uint16

// MaxPlaceholderIdx is the maximum allowed value of a PlaceholderIdx.
// The pgwire protocol is limited to 2^16 placeholders, so we limit the IDs to
// this range as well.
const MaxPlaceholderIdx = math.MaxUint16

// String returns the index as a placeholder string representation ($1, $2 etc).
func (idx PlaceholderIdx) String() string {
	return fmt.Sprintf("$%d", idx+1)
}

// TPlaceholder is the type of a placeholder.
type TPlaceholder struct {
	Idx PlaceholderIdx
}

func (TPlaceholder) SemanticType() SemanticType { return ANY }

// String implements the fmt.Stringer interface.
func (t TPlaceholder) String() string { return fmt.Sprintf("placeholder{%d}", t.Idx+1) }

// Equivalent implements the T interface.
func (t TPlaceholder) Equivalent(other T) bool {
	if other.SemanticType() == ANY {
		return true
	}
	if other.IsAmbiguous() {
		return true
	}
	u, ok := UnwrapType(other).(TPlaceholder)
	return ok && t.Idx == u.Idx
}

// Oid implements the T interface.
func (TPlaceholder) Oid() oid.Oid { panic("TPlaceholder.Oid() is undefined") }

// SQLName implements the T interface.
func (TPlaceholder) SQLName() string { panic("TPlaceholder.SQLName() is undefined") }

// IsAmbiguous implements the T interface.
func (TPlaceholder) IsAmbiguous() bool { panic("TPlaceholder.IsAmbiguous() is undefined") }

// TArray is the type of a DArray.
type TArray struct{ Typ T }

func (TArray) SemanticType() SemanticType { return ARRAY }

func (a TArray) String() string {
	if a.Typ == nil {
		// Used in telemetry.
		return "*[]"
	}
	return a.Typ.String() + "[]"
}

// Equivalent implements the T interface.
func (a TArray) Equivalent(other T) bool {
	if other.SemanticType() == ANY {
		return true
	}
	if u, ok := UnwrapType(other).(TArray); ok {
		return a.Typ.Equivalent(u.Typ)
	}
	return false
}

const noArrayType = 0

// ArrayOids is a set of all oids which correspond to an array type.
var ArrayOids = map[oid.Oid]struct{}{}

func init() {
	for _, v := range oidToArrayOid {
		ArrayOids[v] = struct{}{}
	}
}

// Oid implements the T interface.
func (a TArray) Oid() oid.Oid {
	if o, ok := oidToArrayOid[a.Typ.Oid()]; ok {
		return o
	}
	return noArrayType
}

// SQLName implements the T interface.
func (a TArray) SQLName() string {
	return a.Typ.SQLName() + "[]"
}

// IsAmbiguous implements the T interface.
func (a TArray) IsAmbiguous() bool {
	return a.Typ == nil || a.Typ.IsAmbiguous()
}

type tAny struct{}

func (tAny) SemanticType() SemanticType { return ANY }
func (tAny) String() string             { return "anyelement" }
func (tAny) Equivalent(other T) bool    { return true }
func (tAny) Oid() oid.Oid               { return oid.T_anyelement }
func (tAny) SQLName() string            { return "anyelement" }
func (tAny) IsAmbiguous() bool          { return true }

// IsStringType returns true iff t is String
// or a collated string type.
func IsStringType(t T) bool {
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
func IsValidArrayElementType(t T) (valid bool, issueNum int) {
	switch t.SemanticType() {
	case JSON:
		return false, 23468
	default:
		return true, 0
	}
}

// IsDateTimeType returns true if the T is
// date- or time-related type.
func IsDateTimeType(t T) bool {
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
func IsAdditiveType(t T) bool {
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

// stringTypeName returns the visible type name for the given
// STRING/COLLATEDSTRING column type.
func (c *ColumnType) stringTypeName() string {
	typName := "STRING"
	switch c.Oid() {
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
	return typName
}

// SQLString returns the CockroachDB native SQL string that can be
// used to reproduce the ColumnType (via parsing -> coltypes.T ->
// CastTargetToColumnType -> PopulateAttrs).
//
// Is is used in error messages and also to produce the output
// of SHOW CREATE.
//
// See also InformationSchemaName() below.
func (c *ColumnType) SQLString() string {
	switch c.SemanticType {
	case BIT:
		typName := "BIT"
		if c.Oid() == oid.T_varbit {
			typName = "VARBIT"
		}
		if (c.Oid() != oid.T_varbit && c.Width > 1) ||
			(c.Oid() == oid.T_varbit && c.Width > 0) {
			typName = fmt.Sprintf("%s(%d)", typName, c.Width)
		}
		return typName
	case INT:
		// Pre-2.1 BIT was using column type INT with arbitrary width We
		// map this to INT now. See #34161.
		width := c.Width
		if width != 0 && width != 64 && width != 32 && width != 16 {
			width = 64
		}
		if name, ok := IntegerTypeNames[int(width)]; ok {
			return name
		}
	case STRING, COLLATEDSTRING:
		typName := c.stringTypeName()
		// In general, if there is a specified width we want to print it next
		// to the type. However, in the specific case of CHAR and "char", the
		// default is 1 and the width should be omitted in that case.
		if c.Width > 0 {
			o := c.Oid()
			if c.Width != 1 || (o != oid.T_bpchar && o != oid.T_char) {
				typName = fmt.Sprintf("%s(%d)", typName, c.Width)
			}
		}
		if c.SemanticType == COLLATEDSTRING {
			if c.Locale == nil {
				panic("locale is required for COLLATEDSTRING")
			}
			typName = fmt.Sprintf("%s COLLATE %s", typName, *c.Locale)
		}
		return typName
	case FLOAT:
		const realName = "FLOAT4"
		const doubleName = "FLOAT8"
		if c.Width == 32 {
			return realName
		}
		return doubleName
	case DECIMAL:
		if c.Precision > 0 {
			if c.Width > 0 {
				return fmt.Sprintf("%s(%d,%d)", c.SemanticType.String(), c.Precision, c.Width)
			}
			return fmt.Sprintf("%s(%d)", c.SemanticType.String(), c.Precision)
		}
	case JSON:
		// Only binary JSON is currently supported.
		return "JSONB"
	case ARRAY:
		return c.ElementColumnType().SQLString() + "[]"
	}
	return c.SemanticType.String()
}

// InformationSchemaName returns the string suitable to populate the data_type
// column of information_schema.columns.
//
// This is different from SQLString() in that it must report SQL standard names
// that are compatible with PostgreSQL client expectations.
func (c *ColumnType) InformationSchemaName() string {
	switch c.SemanticType {
	case BOOL:
		return "boolean"

	case BIT:
		if c.Oid() == oid.T_varbit {
			return "bit varying"
		}
		return "bit"

	case INT:
		switch c.Width {
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
		switch c.Oid() {
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
		switch c.Width {
		case 32:
			return "real"
		case 64:
			return "double precision"
		default:
			panic(fmt.Sprintf("programming error: unknown float width: %d", c.Width))
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
	case NULL:
		return "unknown"
	case TUPLE:
		return "record"
	case ARRAY:
		return "ARRAY"
	}

	// The name of the remaining semantic type constants are suitable
	// for the data_type column in information_schema.columns.
	return strings.ToLower(c.SemanticType.String())
}

// MaxCharacterLength returns the declared maximum length of
// characters if the ColumnType is a character or bit string data
// type. Returns false if the data type is not a character or bit
// string, or if the string's length is not bounded.
//
// This is used to populate information_schema.columns.character_maximum_length;
// do not modify this function unless you also check that the values
// generated in information_schema are compatible with client
// expectations.
func (c *ColumnType) MaxCharacterLength() (int32, bool) {
	switch c.SemanticType {
	case STRING, COLLATEDSTRING, BIT:
		if c.Width > 0 {
			return c.Width, true
		}
	}
	return 0, false
}

// MaxOctetLength returns the maximum possible length in
// octets of a datum if the ColumnType is a character string. Returns
// false if the data type is not a character string, or if the
// string's length is not bounded.
//
// This is used to populate information_schema.columns.character_octet_length;
// do not modify this function unless you also check that the values
// generated in information_schema are compatible with client
// expectations.
func (c *ColumnType) MaxOctetLength() (int32, bool) {
	switch c.SemanticType {
	case STRING, COLLATEDSTRING:
		if c.Width > 0 {
			return c.Width * utf8.UTFMax, true
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
func (c *ColumnType) NumericPrecision() (int32, bool) {
	switch c.SemanticType {
	case INT:
		// Assume 64-bit integer if no width is specified.
		if c.Width == 0 {
			return 64, true
		}
		return c.Width, true
	case FLOAT:
		if c.Width == 32 {
			return 24, true
		}
		return 53, true
	case DECIMAL:
		if c.Precision > 0 {
			return c.Precision, true
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
func (c *ColumnType) NumericPrecisionRadix() (int32, bool) {
	switch c.SemanticType {
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
func (c *ColumnType) NumericScale() (int32, bool) {
	switch c.SemanticType {
	case INT:
		return 0, true
	case DECIMAL:
		if c.Precision > 0 {
			return c.Width, true
		}
	}
	return 0, false
}

// ColumnSemanticTypeToDatumType determines a types.T that can be used
// to instantiate an in-memory representation of values for the given
// column type.
func ColumnSemanticTypeToDatumType(c *ColumnType, k SemanticType) T {
	switch k {
	case BIT:
		return BitArray
	case BOOL:
		return Bool
	case INT:
		return Int
	case FLOAT:
		return Float
	case DECIMAL:
		return Decimal
	case STRING:
		if c.Oid() == oid.T_name {
			return Name
		}
		return String
	case BYTES:
		return Bytes
	case DATE:
		return Date
	case TIME:
		return Time
	case TIMESTAMP:
		return Timestamp
	case TIMESTAMPTZ:
		return TimestampTZ
	case INTERVAL:
		return Interval
	case UUID:
		return Uuid
	case INET:
		return INet
	case JSON:
		return Jsonb
	case TUPLE:
		return EmptyTuple
	case COLLATEDSTRING:
		if c.Locale == nil {
			panic("locale is required for COLLATEDSTRING")
		}
		return TCollatedString{Locale: *c.Locale}
	case OID:
		return Oid
	case NULL:
		return Unknown
	case ARRAY:
		switch c.Oid() {
		case oid.T_int2vector:
			return IntVector
		case oid.T_oidvector:
			return OidVector
		}
	}
	return nil
}

// ToDatumType converts the ColumnType to a types.T (type of in-memory
// representations). It returns nil if there is no such type.
//
// This is a lossy conversion: some type attributes are not preserved.
func (c *ColumnType) ToDatumType() T {
	switch c.SemanticType {
	case ARRAY:
		return TArray{Typ: ColumnSemanticTypeToDatumType(c, *c.ArrayContents)}
	case TUPLE:
		datums := TTuple{
			Types:  make([]T, len(c.TupleContents)),
			Labels: c.TupleLabels,
		}
		for i := range c.TupleContents {
			datums.Types[i] = c.TupleContents[i].ToDatumType()
		}
		return datums
	default:
		return ColumnSemanticTypeToDatumType(c, c.SemanticType)
	}
}

// ElementColumnType works on a ColumnType with semantic type ARRAY
// and retrieves the ColumnType of the elements of the array.
//
// This is used by LimitValueWidth() and SQLType().
//
// TODO(knz): make this return a bool and avoid a heap allocation.
func (c *ColumnType) ElementColumnType() *ColumnType {
	if c.SemanticType != ARRAY {
		return nil
	}
	result := *c
	result.SemanticType = *c.ArrayContents
	result.ArrayContents = nil
	return &result
}

// ColumnTypesToDatumTypes converts a slice of ColumnTypes to a slice of
// datum types.
func ColumnTypesToDatumTypes(colTypes []ColumnType) []T {
	res := make([]T, len(colTypes))
	for i, t := range colTypes {
		res[i] = t.ToDatumType()
	}
	return res
}

// IntegerTypeNames maps a TInt data width to a canonical type name.
var IntegerTypeNames = map[int]string{
	0:  "INT",
	16: "INT2",
	32: "INT4",
	64: "INT8",
}
