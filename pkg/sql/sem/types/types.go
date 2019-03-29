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

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/util/log"
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

// ColumnType wraps the Protobuf-generated InternalColumnType so that it can
// override the Marshal/Unmarshal methods in order to map to/from older
// persisted ColumnType representations.
type ColumnType InternalColumnType

type T = ColumnType

// Oid returns the type's Postgres object ID.
func (t *ColumnType) Oid() oid.Oid {
	if t.XXX_Oid != 0 {
		return t.XXX_Oid
	}
	switch t.SemanticType {
	case ARRAY:
		// If array element OID can't be found, return 0.
		return oidToArrayOid[t.ArrayContents.Oid()]
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
	temp.downgradeType()
	return (*InternalColumnType)(&temp).Size()
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
		if !c.ArrayContents.Identical(other.ArrayContents) {
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
		if t.TupleContents == nil || other.TupleContents == nil {
			// Tuples that aren't fully specified (have a nil subtype list) are
			// always equivalent to other tuples, to allow overloads to specify
			// that they take an arbitrary tuple type.
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
	err := (*InternalColumnType)(t).Unmarshal(data)
	if err != nil {
		return err
	}
	t.upgradeType()
	return nil
}

func (t *ColumnType) upgradeType() {
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
	case STRING, COLLATEDSTRING:
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
	case ARRAY:
		// If this is ARRAY of ARRAY (nested array), then no need to upgrade,
		// since that's only supported starting with 19.2.
		if t.ArrayContents == nil || t.ArrayContents.SemanticType != ARRAY {
			if t.ArrayContents == nil {
				arrayContents := *t
				arrayContents.SemanticType = *t.XXX_ArrayElemType
				arrayContents.XXX_ArrayElemType = nil
				arrayContents.upgradeType()
				t.ArrayContents = &arrayContents
			}
			t.Width = 0
			t.Precision = 0
			t.Locale = nil
			t.XXX_VisibleType = 0
			t.XXX_ArrayElemType = nil
			t.XXX_ArrayDimensions = nil
		}
	case int2vector:
		t.SemanticType = ARRAY
		t.Width = 0
		t.XXX_Oid = oid.T_int2vector
		t.ArrayContents = typeInt2
	case oidvector:
		t.SemanticType = ARRAY
		t.XXX_Oid = oid.T_oidvector
		t.ArrayContents = Oid
	case name:
		t.SemanticType = STRING
		t.XXX_Oid = oid.T_name
	}

	// Clear any visible type, since they are all now handled by the Width or
	// Oid fields.
	t.XXX_VisibleType = VisibleType_NONE
}

// Marshal serializes the ColumnType to bytes.
func (t *ColumnType) Marshal() (data []byte, err error) {
	temp := *t
	temp.downgradeType()
	return (*InternalColumnType)(&temp).Marshal()
}

// MarshalTo serializes the ColumnType to the given byte slice.
func (t *ColumnType) MarshalTo(data []byte) (int, error) {
	temp := *t
	temp.downgradeType()
	return (*InternalColumnType)(&temp).MarshalTo(data)
}

func (t *ColumnType) downgradeType() {
	// Set SemanticType and VisibleType for 19.1 backwards-compatibility.
	switch t.SemanticType {
	case BIT:
		if t.XXX_Oid == oid.T_varbit {
			t.XXX_VisibleType = visibleType_VARBIT
		}
	case FLOAT:
		switch t.Width {
		case 32:
			t.XXX_VisibleType = visibleType_REAL
		}
	case STRING, COLLATEDSTRING:
		switch t.XXX_Oid {
		case oid.T_varchar:
			t.XXX_VisibleType = visibleType_VARCHAR
		case oid.T_bpchar:
			t.XXX_VisibleType = visibleType_CHAR
		case oid.T_char:
			t.XXX_VisibleType = visibleType_QCHAR
		case oid.T_name:
			t.SemanticType = name
		}
	case ARRAY:
		// If the array is nested, then no need to change the representation, since
		// support for nested arrays in types.T is new for 19.2. Otherwise,
		// downgrade to ARRAY representation used before 19.2, in which the array
		// type fields specified the width, locale, etc. of the element type.
		if t.ArrayContents.SemanticType != ARRAY {
			temp := *t.ArrayContents
			temp.downgradeType()
			t.Width = temp.Width
			t.Precision = temp.Precision
			t.Locale = temp.Locale
			t.XXX_VisibleType = temp.XXX_VisibleType
			t.XXX_ArrayElemType = &t.ArrayContents.SemanticType

			switch t.Oid() {
			case oid.T_int2vector:
				t.SemanticType = int2vector
			case oid.T_oidvector:
				t.SemanticType = oidvector
			}
		}
	}
}

func (t *ColumnType) DebugString() string {
	return (*InternalColumnType)(t).String()
}

func (t *ColumnType) String() string {
	switch t.Oid() {
	case oid.T_name:
		return "name"
	}

	switch t.SemanticType {
	case NULL:
		return "unknown"
	case BOOL:
		return "bool"
	case INT:
		return "int"
	case STRING:
		return "string"
	case BIT:
		return "varbit"
	case FLOAT:
		return "float"
	case DECIMAL:
		return "decimal"
	case COLLATEDSTRING:
		if *t.Locale == "" {
			// Used in telemetry.
			return "collatedstring{*}"
		}
		return fmt.Sprintf("collatedstring{%s}", *t.Locale)
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
		var buf bytes.Buffer
		buf.WriteString("tuple")
		if len(t.TupleContents) != 0 {
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
	case ARRAY:
		return t.ArrayContents.String() + "[]"
	case ANY:
		return "anyelement"
	case OID:
		return t.SQLStandardName()
	}

	panic(pgerror.NewAssertionErrorf("unexpected SemanticType: %s", t.SemanticType))
}

var (
	// Unknown is the type of an expression that statically evaluates to NULL.
	Unknown = &T{SemanticType: NULL}
	Bool    = &T{SemanticType: BOOL}
	Bit     = &T{SemanticType: BIT}
	VarBit  = &T{SemanticType: BIT, XXX_Oid: oid.T_varbit}
	Int     = &T{SemanticType: INT, Width: 64}
	Float   = &T{SemanticType: FLOAT, Width: 64}
	Decimal = &T{SemanticType: DECIMAL}

	// String is the canonical CockroachDB string type.
	//
	// It is reported as STRING in SHOW CREATE but "text" in introspection for
	// compatibility with PostgreSQL.
	//
	// It has no default maximum width.
	String      = &T{SemanticType: STRING}
	Bytes       = &T{SemanticType: BYTES}
	Date        = &T{SemanticType: DATE}
	Time        = &T{SemanticType: TIME}
	Timestamp   = &T{SemanticType: TIMESTAMP}
	TimestampTZ = &T{SemanticType: TIMESTAMPTZ}
	Interval    = &T{SemanticType: INTERVAL}
	Jsonb       = &T{SemanticType: JSON}
	Uuid        = &T{SemanticType: UUID}
	INet        = &T{SemanticType: INET}
	Any         = &T{SemanticType: ANY}
	AnyArray    = &T{SemanticType: ARRAY, ArrayContents: Any}
	// AnyTuple is the tuple type with no fields. It is used as a wildcard
	// parameterized type that matches any tuple type.
	AnyTuple = &T{SemanticType: TUPLE}
	// AnyCollatedString is the collated string type with a nil locale. It is
	// used as a wildcard parameterized type that matches any locale.
	emptyLocale       = ""
	AnyCollatedString = &T{SemanticType: COLLATEDSTRING, Locale: &emptyLocale}

	// AnyNonArray contains all non-array types.
	AnyNonArray = []*T{
		Bool,
		VarBit,
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

	StringArray = &T{SemanticType: ARRAY, ArrayContents: String}

	IntArray = &T{SemanticType: ARRAY, ArrayContents: Int}

	DecimalArray = &T{SemanticType: ARRAY, ArrayContents: Decimal}
)

func MakeArray(typ *T) *T {
	return &T{SemanticType: ARRAY, ArrayContents: typ}
}

func MakeTuple(contents []T) *T {
	return &T{SemanticType: TUPLE, TupleContents: contents}
}

func MakeLabeledTuple(contents []T, labels []string) *T {
	return &T{SemanticType: TUPLE, TupleContents: contents, TupleLabels: labels}
}

func MakeCollatedString(locale string) *T {
	return &T{SemanticType: COLLATEDSTRING, Locale: &locale}
}

// IsAmbiguous returns whether the type is ambiguous or fully defined. This is
// important for parameterized types to determine whether they are fully
// concrete type specification or not.
func (t *ColumnType) IsAmbiguous() bool {
	switch t.SemanticType {
	case NULL, ANY:
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

// stringTypeName returns the visible type name for the given
// STRING/COLLATEDSTRING column type.
func (t *ColumnType) stringTypeName() string {
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
	case NULL:
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
	case STRING, COLLATEDSTRING:
		typName := t.stringTypeName()
		// In general, if there is a specified width we want to print it next
		// to the type. However, in the specific case of CHAR and "char", the
		// default is 1 and the width should be omitted in that case.
		if t.Width > 0 {
			o := t.Oid()
			if t.Width != 1 || (o != oid.T_bpchar && o != oid.T_char) {
				typName = fmt.Sprintf("%s(%d)", typName, t.Width)
			}
		}
		if t.SemanticType == COLLATEDSTRING {
			if t.Locale == nil {
				panic("locale is required for COLLATEDSTRING")
			}
			typName = fmt.Sprintf("%s COLLATE %s", typName, *t.Locale)
		}
		return typName
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
	case ARRAY:
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
	case NULL:
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
