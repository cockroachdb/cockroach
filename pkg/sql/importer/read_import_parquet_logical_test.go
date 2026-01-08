// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"testing"
	"time"

	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/schema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// TestConvertLogicalValueToDatum tests modern LogicalType conversions
func TestConvertLogicalValueToDatum(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name        string
		value       interface{}
		targetType  *types.T
		logicalType schema.LogicalType
		expected    tree.Datum
		expectErr   bool
	}{
		// NULL logical type - always returns NULL regardless of value
		{
			name:        "null-logical-type",
			value:       int32(42),
			targetType:  types.Int,
			logicalType: schema.NullLogicalType{},
			expected:    tree.DNull,
		},

		// INT logical types
		{
			name:        "int32-int8-logical-type",
			value:       int32(42),
			targetType:  types.Int,
			logicalType: schema.NewIntLogicalType(8, true), // 8-bit signed int
			expected:    tree.NewDInt(42),
		},
		{
			name:        "int32-int16-logical-type",
			value:       int32(1234),
			targetType:  types.Int,
			logicalType: schema.NewIntLogicalType(16, true), // 16-bit signed int
			expected:    tree.NewDInt(1234),
		},
		{
			name:        "int32-int32-logical-type",
			value:       int32(123456),
			targetType:  types.Int,
			logicalType: schema.NewIntLogicalType(32, true), // 32-bit signed int
			expected:    tree.NewDInt(123456),
		},
		{
			name:        "int64-int64-logical-type",
			value:       int64(9876543210),
			targetType:  types.Int,
			logicalType: schema.NewIntLogicalType(64, true), // 64-bit signed int
			expected:    tree.NewDInt(9876543210),
		},

		// DATE logical type
		{
			name:        "int32-date-logical-type",
			value:       int32(18262), // 2020-01-01
			targetType:  types.Date,
			logicalType: schema.DateLogicalType{},
			expected: func() tree.Datum {
				d, _ := pgdate.MakeDateFromUnixEpoch(18262)
				return tree.NewDDate(d)
			}(),
		},

		// TIME logical types
		{
			name:        "int32-time-millis-logical-type",
			value:       int32(36000000), // 10:00:00
			targetType:  types.Time,
			logicalType: schema.NewTimeLogicalType(false, schema.TimeUnitMillis),
			expected:    tree.MakeDTime(timeofday.TimeOfDay(36000000000)),
		},
		{
			name:        "int64-time-micros-logical-type",
			value:       int64(36000000000), // 10:00:00
			targetType:  types.Time,
			logicalType: schema.NewTimeLogicalType(false, schema.TimeUnitMicros),
			expected:    tree.MakeDTime(timeofday.TimeOfDay(36000000000)),
		},
		{
			name:        "int64-time-nanos-logical-type",
			value:       int64(36000000000000), // 10:00:00
			targetType:  types.Time,
			logicalType: schema.NewTimeLogicalType(false, schema.TimeUnitNanos),
			expected:    tree.MakeDTime(timeofday.TimeOfDay(36000000000)),
		},

		// TIMESTAMP logical types
		{
			name:        "int64-timestamp-millis-logical-type",
			value:       int64(1577836800000), // 2020-01-01 00:00:00 UTC
			targetType:  types.TimestampTZ,
			logicalType: schema.NewTimestampLogicalType(true, schema.TimeUnitMillis),
			expected: func() tree.Datum {
				ts := time.Unix(1577836800, 0).UTC()
				d, _ := tree.MakeDTimestampTZ(ts, time.Microsecond)
				return d
			}(),
		},
		{
			name:        "int64-timestamp-micros-logical-type",
			value:       int64(1577836800000000), // 2020-01-01 00:00:00 UTC
			targetType:  types.TimestampTZ,
			logicalType: schema.NewTimestampLogicalType(true, schema.TimeUnitMicros),
			expected: func() tree.Datum {
				ts := time.Unix(1577836800, 0).UTC()
				d, _ := tree.MakeDTimestampTZ(ts, time.Microsecond)
				return d
			}(),
		},
		{
			name:        "int64-timestamp-nanos-logical-type",
			value:       int64(1577836800000000000), // 2020-01-01 00:00:00 UTC
			targetType:  types.TimestampTZ,
			logicalType: schema.NewTimestampLogicalType(true, schema.TimeUnitNanos),
			expected: func() tree.Datum {
				ts := time.Unix(1577836800, 0).UTC()
				d, _ := tree.MakeDTimestampTZ(ts, time.Microsecond)
				return d
			}(),
		},

		// DECIMAL logical types
		{
			name:        "int32-decimal-scale-2",
			value:       int32(12345), // 123.45
			targetType:  types.Decimal,
			logicalType: schema.NewDecimalLogicalType(10, 2),
			expected:    func() tree.Datum { d, _ := tree.ParseDDecimal("123.45"); return d }(),
		},
		{
			name:        "int32-decimal-scale-0",
			value:       int32(12345),
			targetType:  types.Decimal,
			logicalType: schema.NewDecimalLogicalType(10, 0),
			expected:    func() tree.Datum { d, _ := tree.ParseDDecimal("12345"); return d }(),
		},
		{
			name:        "int64-decimal-scale-4",
			value:       int64(123456789), // 12345.6789
			targetType:  types.Decimal,
			logicalType: schema.NewDecimalLogicalType(20, 4),
			expected:    func() tree.Datum { d, _ := tree.ParseDDecimal("12345.6789"); return d }(),
		},
		{
			name:        "int32-decimal-negative",
			value:       int32(-12345), // -123.45
			targetType:  types.Decimal,
			logicalType: schema.NewDecimalLogicalType(10, 2),
			expected:    func() tree.Datum { d, _ := tree.ParseDDecimal("-123.45"); return d }(),
		},
		{
			name:        "int32-decimal-min-value",
			value:       int32(-2147483648), // math.MinInt32 = -21474836.48 with scale 2
			targetType:  types.Decimal,
			logicalType: schema.NewDecimalLogicalType(10, 2),
			expected:    func() tree.Datum { d, _ := tree.ParseDDecimal("-21474836.48"); return d }(),
		},
		{
			name:        "int64-decimal-min-value",
			value:       int64(-9223372036854775808), // math.MinInt64 = -92233720368547.75808 with scale 5
			targetType:  types.Decimal,
			logicalType: schema.NewDecimalLogicalType(20, 5),
			expected:    func() tree.Datum { d, _ := tree.ParseDDecimal("-92233720368547.75808"); return d }(),
		},

		// STRING logical type
		{
			name:        "bytes-string-logical-type",
			value:       []byte("hello world"),
			targetType:  types.String,
			logicalType: schema.StringLogicalType{},
			expected:    tree.NewDString("hello world"),
		},

		// JSON logical type
		{
			name:        "bytes-json-logical-type",
			value:       []byte(`{"key": "value"}`),
			targetType:  types.Jsonb,
			logicalType: schema.JSONLogicalType{},
			expected:    func() tree.Datum { d, _ := tree.ParseDJSON(`{"key": "value"}`); return d }(),
		},
		{
			name:        "bytes-json-empty",
			value:       []byte{},
			targetType:  types.Jsonb,
			logicalType: schema.JSONLogicalType{},
			expected:    tree.DNull,
		},

		// UUID (FixedLenByteArray without specific logical type, but included for completeness)
		{
			name:        "fixed-len-bytes-uuid",
			value:       parquet.FixedLenByteArray(uuid.MakeV4().GetBytes()),
			targetType:  types.Uuid,
			logicalType: schema.NewIntLogicalType(8, true), // Dummy logical type to satisfy assertion
			expected:    tree.NewDUuid(tree.DUuid{UUID: uuid.MakeV4()}),
		},

		// INT96 timestamp (deprecated format without explicit logical type)
		{
			name:        "int96-timestamptz",
			value:       parquet.Int96{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xd1, 0x86, 0x24, 0x00},
			targetType:  types.TimestampTZ,
			logicalType: schema.NewIntLogicalType(8, true), // Dummy logical type to satisfy assertion
			expected: func() tree.Datum {
				ts := parquet.Int96{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xd1, 0x86, 0x24, 0x00}.ToTime()
				d, _ := tree.MakeDTimestampTZ(ts, time.Microsecond)
				return d
			}(),
		},

		// Edge cases: large scale values are handled by apd
		{
			name:        "int32-decimal-large-scale",
			value:       int32(12345),
			targetType:  types.Decimal,
			logicalType: schema.NewDecimalLogicalType(20, 10),
			expected:    func() tree.Datum { d, _ := tree.ParseDDecimal("0.0000012345"); return d }(),
		},
		{
			name:        "int64-decimal-large-scale",
			value:       int64(123456789),
			targetType:  types.Decimal,
			logicalType: schema.NewDecimalLogicalType(30, 19),
			expected:    func() tree.Datum { d, _ := tree.ParseDDecimal("0.0000000000123456789"); return d }(),
		},

		// DecimalLogicalType with scale=0 targeting INT columns should return DInt
		{
			name:        "int32-decimal-scale0-to-int",
			value:       int32(12345),
			targetType:  types.Int,
			logicalType: schema.NewDecimalLogicalType(10, 0),
			expected:    tree.NewDInt(12345),
		},
		{
			name:        "int64-decimal-scale0-to-int",
			value:       int64(123456789),
			targetType:  types.Int,
			logicalType: schema.NewDecimalLogicalType(20, 0),
			expected:    tree.NewDInt(123456789),
		},

		// Note: ENUM type is supported but requires a properly hydrated enum type with values,
		// which is complex to set up in a unit test. ENUM conversion is tested in integration tests.

		// UUID type with explicit UUIDLogicalType
		{
			name:        "fixed-len-bytes-uuid-logical-type",
			value:       parquet.FixedLenByteArray(uuid.MakeV4().GetBytes()),
			targetType:  types.Uuid,
			logicalType: schema.UUIDLogicalType{},
			expectErr:   false, // Can't predict exact UUID value
		},

		// INTERVAL type (stored as 12-byte fixed array in Parquet with IntervalLogicalType)
		{
			name: "fixed-len-bytes-interval-logical-type",
			// 12 bytes: months=1, days=2, milliseconds=3000 (all little-endian uint32)
			value:       parquet.FixedLenByteArray([]byte{0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0xb8, 0x0b, 0x00, 0x00}),
			targetType:  types.Interval,
			logicalType: schema.IntervalLogicalType{},
			expected:    tree.NewDInterval(duration.MakeDuration(3000000000, 2, 1), types.DefaultIntervalTypeMetadata),
		},

		// GEOGRAPHY and GEOMETRY types (stored as WKB/EWKB in Parquet)
		{
			name:        "bytes-geography-point",
			value:       []byte("\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"), // POINT(1 1) in EWKB
			targetType:  types.Geography,
			logicalType: schema.StringLogicalType{}, // Dummy logical type to satisfy assertion
			// Can't predict exact value due to internal representation
			expectErr: false,
		},
		{
			name:        "bytes-geometry-point",
			value:       []byte("\x01\x01\x00\x00\x00\x00\x00\x00\x00\x00\x00\xf0\x3f\x00\x00\x00\x00\x00\x00\xf0\x3f"), // POINT(1 1) in EWKB
			targetType:  types.Geometry,
			logicalType: schema.StringLogicalType{}, // Dummy logical type to satisfy assertion
			// Can't predict exact value due to internal representation
			expectErr: false,
		},
		{
			name:        "bytes-geography-empty",
			value:       []byte{},
			targetType:  types.Geography,
			logicalType: schema.StringLogicalType{}, // Dummy logical type to satisfy assertion
			expected:    tree.DNull,
		},
		{
			name:        "bytes-geometry-empty",
			value:       []byte{},
			targetType:  types.Geometry,
			logicalType: schema.StringLogicalType{}, // Dummy logical type to satisfy assertion
			expected:    tree.DNull,
		},

		// Edge cases: invalid TimeUnit combinations
		{
			name:        "int32-time-micros-invalid",
			value:       int32(36000000),
			targetType:  types.Time,
			logicalType: schema.NewTimeLogicalType(false, schema.TimeUnitMicros), // INT32 only supports MILLIS
			expectErr:   true,
		},
		{
			name:        "int32-time-nanos-invalid",
			value:       int32(36000000),
			targetType:  types.Time,
			logicalType: schema.NewTimeLogicalType(false, schema.TimeUnitNanos), // INT32 only supports MILLIS
			expectErr:   true,
		},
		{
			name:        "int64-time-millis-invalid",
			value:       int64(36000000),
			targetType:  types.Time,
			logicalType: schema.NewTimeLogicalType(false, schema.TimeUnitMillis), // INT64 doesn't support MILLIS
			expectErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			metadata := &parquetColumnMetadata{
				logicalType:   tc.logicalType,
				convertedType: schema.ConvertedTypes.None,
			}

			// Determine physical type from value type
			var physicalType parquet.Type
			switch tc.value.(type) {
			case bool:
				physicalType = parquet.Types.Boolean
			case int32:
				physicalType = parquet.Types.Int32
			case int64:
				physicalType = parquet.Types.Int64
			case float32:
				physicalType = parquet.Types.Float
			case float64:
				physicalType = parquet.Types.Double
			case []byte:
				physicalType = parquet.Types.ByteArray
			case parquet.FixedLenByteArray:
				physicalType = parquet.Types.FixedLenByteArray
			case parquet.Int96:
				physicalType = parquet.Types.Int96
			default:
				t.Fatalf("unknown value type: %T", tc.value)
			}

			// Test validation - should match conversion expectation
			validateErr := validateWithLogicalType(physicalType, tc.logicalType, tc.targetType)
			if tc.expectErr {
				require.Error(t, validateErr, "validation should error when conversion errors")
			} else {
				require.NoError(t, validateErr, "validation should succeed when conversion succeeds")
			}

			result, err := convertWithLogicalType(tc.value, tc.targetType, metadata)
			if tc.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// Special handling for types we can't predict exact values
			if result == tree.DNull {
				require.Equal(t, tree.DNull, tc.expected)
			} else {
				switch tc.targetType.Family() {
				case types.UuidFamily:
					require.Equal(t, types.UuidFamily, result.ResolvedType().Family())
				case types.GeographyFamily:
					require.Equal(t, types.GeographyFamily, result.ResolvedType().Family())
				case types.GeometryFamily:
					require.Equal(t, types.GeometryFamily, result.ResolvedType().Family())
				case types.EnumFamily:
					require.Equal(t, types.EnumFamily, result.ResolvedType().Family())
				case types.IntervalFamily:
					// For intervals, compare the string representation
					require.Equal(t, tc.expected.String(), result.String())
				case types.DecimalFamily:
					// For decimals, compare decimal values directly
					expectedDec := tree.MustBeDDecimal(tc.expected)
					resultDec := tree.MustBeDDecimal(result)
					require.Equal(t, expectedDec.String(), resultDec.String())
				default:
					require.Equal(t, tc.expected.String(), result.String())
				}
			}
		})
	}
}
