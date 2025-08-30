// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package types

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/lib/pq/oid"
)

func TestCanonical(t *testing.T) {
	testCases := []struct {
		name      string
		inputType *T
		expected  *T
	}{
		// BoolFamily
		{
			name:      "Bool",
			inputType: Bool,
			expected:  Bool,
		},

		// IntFamily - all int types should map to canonical Int
		{
			name:      "Int (canonical)",
			inputType: Int,
			expected:  Int,
		},
		{
			name:      "Int4",
			inputType: Int4,
			expected:  Int,
		},
		{
			name:      "Int2",
			inputType: Int2,
			expected:  Int,
		},

		// FloatFamily - all float types should map to canonical Float
		{
			name:      "Float (canonical)",
			inputType: Float,
			expected:  Float,
		},
		{
			name:      "Float4",
			inputType: Float4,
			expected:  Float,
		},

		// DecimalFamily
		{
			name:      "Decimal (canonical)",
			inputType: Decimal,
			expected:  Decimal,
		},
		{
			name:      "Decimal with precision",
			inputType: MakeDecimal(10, 3),
			expected:  Decimal,
		},

		// DateFamily
		{
			name:      "Date",
			inputType: Date,
			expected:  Date,
		},

		// TimestampFamily
		{
			name:      "Timestamp (canonical)",
			inputType: Timestamp,
			expected:  Timestamp,
		},
		{
			name:      "Timestamp with precision",
			inputType: MakeTimestamp(3),
			expected:  Timestamp,
		},

		// IntervalFamily
		{
			name:      "Interval (canonical)",
			inputType: Interval,
			expected:  Interval,
		},
		{
			name:      "Interval with precision",
			inputType: MakeInterval(IntervalTypeMetadata{Precision: 3, PrecisionIsSet: true}),
			expected:  Interval,
		},

		// StringFamily and CollatedStringFamily - both map to canonical String
		{
			name:      "String (canonical)",
			inputType: String,
			expected:  String,
		},
		{
			name:      "VarChar",
			inputType: VarChar,
			expected:  String,
		},
		{
			name:      "String with width",
			inputType: MakeString(20),
			expected:  String,
		},
		{
			name:      "VarChar with width",
			inputType: MakeVarChar(20),
			expected:  String,
		},
		{
			name:      "BPChar",
			inputType: BPChar,
			expected:  String,
		},
		{
			name:      "Char with width",
			inputType: MakeChar(20),
			expected:  String,
		},
		{
			name:      "QChar",
			inputType: QChar,
			expected:  String,
		},
		{
			name:      "Name",
			inputType: Name,
			expected:  String,
		},
		{
			name:      "CollatedString",
			inputType: MakeCollatedString(String, "en"),
			expected:  String,
		},
		{
			name:      "CollatedString VarChar",
			inputType: MakeCollatedString(VarChar, "de"),
			expected:  String,
		},
		{
			name:      "CIText",
			inputType: CIText,
			expected:  String,
		},

		// BytesFamily
		{
			name:      "Bytes",
			inputType: Bytes,
			expected:  Bytes,
		},

		// TimestampTZFamily
		{
			name:      "TimestampTZ (canonical)",
			inputType: TimestampTZ,
			expected:  TimestampTZ,
		},
		{
			name:      "TimestampTZ with precision",
			inputType: MakeTimestampTZ(3),
			expected:  TimestampTZ,
		},

		// OidFamily
		{
			name:      "Oid",
			inputType: Oid,
			expected:  Oid,
		},
		{
			name:      "RegClass",
			inputType: RegClass,
			expected:  Oid,
		},
		{
			name:      "RegProc",
			inputType: RegProc,
			expected:  Oid,
		},
		{
			name:      "RegType",
			inputType: RegType,
			expected:  Oid,
		},

		// UnknownFamily
		{
			name:      "Unknown",
			inputType: Unknown,
			expected:  Unknown,
		},

		// UuidFamily
		{
			name:      "Uuid",
			inputType: Uuid,
			expected:  Uuid,
		},

		// INetFamily
		{
			name:      "INet",
			inputType: INet,
			expected:  INet,
		},

		// TimeFamily
		{
			name:      "Time (canonical)",
			inputType: Time,
			expected:  Time,
		},
		{
			name:      "Time with precision",
			inputType: MakeTime(3),
			expected:  Time,
		},

		// JsonFamily - note that Json and Jsonb both map to Jsonb
		{
			name:      "Jsonb (canonical)",
			inputType: Jsonb,
			expected:  Jsonb,
		},
		{
			name:      "Json",
			inputType: Json,
			expected:  Jsonb,
		},

		// TimeTZFamily
		{
			name:      "TimeTZ (canonical)",
			inputType: TimeTZ,
			expected:  TimeTZ,
		},
		{
			name:      "TimeTZ with precision",
			inputType: MakeTimeTZ(3),
			expected:  TimeTZ,
		},

		// BitFamily
		{
			name:      "VarBit (canonical)",
			inputType: VarBit,
			expected:  VarBit,
		},
		{
			name:      "Bit",
			inputType: MakeBit(1),
			expected:  VarBit,
		},
		{
			name:      "VarBit with width",
			inputType: MakeVarBit(20),
			expected:  VarBit,
		},

		// GeometryFamily
		{
			name:      "Geometry (canonical)",
			inputType: Geometry,
			expected:  Geometry,
		},
		{
			name:      "Geometry with shape and SRID",
			inputType: MakeGeometry(geopb.ShapeType_Point, 4326),
			expected:  Geometry,
		},

		// GeographyFamily
		{
			name:      "Geography (canonical)",
			inputType: Geography,
			expected:  Geography,
		},
		{
			name:      "Geography with shape and SRID",
			inputType: MakeGeography(geopb.ShapeType_Point, 4326),
			expected:  Geography,
		},

		// Box2DFamily
		{
			name:      "Box2D",
			inputType: Box2D,
			expected:  Box2D,
		},

		// VoidFamily
		{
			name:      "Void",
			inputType: Void,
			expected:  Void,
		},

		// EncodedKeyFamily
		{
			name:      "EncodedKey",
			inputType: EncodedKey,
			expected:  EncodedKey,
		},

		// TSQueryFamily
		{
			name:      "TSQuery",
			inputType: TSQuery,
			expected:  TSQuery,
		},

		// TSVectorFamily
		{
			name:      "TSVector",
			inputType: TSVector,
			expected:  TSVector,
		},

		// PGLSNFamily
		{
			name:      "PGLSN",
			inputType: PGLSN,
			expected:  PGLSN,
		},

		// RefCursorFamily
		{
			name:      "RefCursor",
			inputType: RefCursor,
			expected:  RefCursor,
		},

		// PGVectorFamily
		{
			name:      "PGVector (canonical)",
			inputType: PGVector,
			expected:  PGVector,
		},
		{
			name:      "PGVector with dimensions",
			inputType: MakePGVector(128),
			expected:  PGVector,
		},

		// TriggerFamily
		{
			name:      "Trigger",
			inputType: Trigger,
			expected:  Trigger,
		},

		// JsonpathFamily
		{
			name:      "Jsonpath",
			inputType: Jsonpath,
			expected:  Jsonpath,
		},

		// AnyFamily
		{
			name:      "Any",
			inputType: Any,
			expected:  Any,
		},
		{
			name:      "AnyElement",
			inputType: AnyElement,
			expected:  Any,
		},

		// ArrayFamily - these types are canonical (return themselves)
		{
			name:      "IntArray",
			inputType: IntArray,
			expected:  IntArray,
		},
		{
			name:      "StringArray",
			inputType: StringArray,
			expected:  StringArray,
		},
		{
			name:      "Array of custom type",
			inputType: MakeArray(MakeDecimal(10, 3)),
			expected:  MakeArray(MakeDecimal(10, 3)),
		},

		// TupleFamily - these types are canonical (return themselves)
		{
			name:      "EmptyTuple",
			inputType: EmptyTuple,
			expected:  EmptyTuple,
		},
		{
			name:      "Tuple with contents",
			inputType: MakeTuple([]*T{Int, String}),
			expected:  MakeTuple([]*T{Int, String}),
		},
		{
			name:      "Labeled tuple",
			inputType: MakeLabeledTuple([]*T{Int, String}, []string{"a", "b"}),
			expected:  MakeLabeledTuple([]*T{Int, String}, []string{"a", "b"}),
		},

		// EnumFamily - these types are canonical (return themselves)
		{
			name:      "Enum",
			inputType: MakeEnum(100, 101),
			expected:  MakeEnum(100, 101),
		},
		{
			name:      "AnyEnum",
			inputType: AnyEnum,
			expected:  AnyEnum,
		},

		// LTreeFamily
		{
			name:      "LTree",
			inputType: LTree,
			expected:  LTree,
		},

		// Unknown family type that should map to Unknown
		{
			name: "Unrecognized family defaults to Unknown",
			inputType: &T{InternalType: InternalType{
				Family: Family(9999), // Non-existent family
				Oid:    oid.T_unknown,
				Locale: &emptyLocale,
			}},
			expected: Unknown,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			canonical := tc.inputType.Canonical()

			// Check that the canonical type is the expected one
			if !canonical.Identical(tc.expected) {
				t.Errorf("Canonical() = %v, expected %v", canonical.DebugString(), tc.expected.DebugString())
			}

			// Check that applying Canonical to the result is idempotent
			canonicalTwice := canonical.Canonical()
			if !canonical.Identical(canonicalTwice) {
				t.Errorf("Canonical() is not idempotent: %v != %v", canonical.DebugString(), canonicalTwice.DebugString())
			}
		})
	}
}
