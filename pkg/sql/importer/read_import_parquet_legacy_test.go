// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/schema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/stretchr/testify/require"
)

// TestConvertParquetValueToDatumLegacy tests that legacy ConvertedType annotations
// are correctly converted to LogicalType via ToLogicalType() and produce correct results.
func TestConvertParquetValueToDatumLegacy(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		name          string
		value         interface{}
		targetType    *types.T
		convertedType schema.ConvertedType
		expected      tree.Datum
		expectErr     bool
	}{
		// Basic types also work with ConvertedType (even if None)
		{
			name:          "bool-true",
			value:         true,
			targetType:    types.Bool,
			convertedType: schema.ConvertedTypes.None,
			expected:      tree.MakeDBool(true),
		},
		{
			name:          "int32-to-int",
			value:         int32(42),
			targetType:    types.Int,
			convertedType: schema.ConvertedTypes.None,
			expected:      tree.NewDInt(42),
		},
		{
			name:          "bytes-to-string",
			value:         []byte("hello"),
			targetType:    types.String,
			convertedType: schema.ConvertedTypes.None,
			expected:      tree.NewDString("hello"),
		},

		// ConvertedType-specific tests
		{
			name:          "int32-date-converted-type",
			value:         int32(0), // 1970-01-01 (epoch)
			targetType:    types.Date,
			convertedType: schema.ConvertedTypes.Date,
			expected: func() tree.Datum {
				d, _ := pgdate.MakeDateFromUnixEpoch(0)
				return tree.NewDDate(d)
			}(),
		},
		{
			name:          "int32-time-millis-converted-type",
			value:         int32(3600000), // 01:00:00
			targetType:    types.Time,
			convertedType: schema.ConvertedTypes.TimeMillis,
			expected:      tree.MakeDTime(timeofday.TimeOfDay(3600000000)),
		},
		{
			name:          "int64-time-micros-converted-type",
			value:         int64(3600000000), // 01:00:00
			targetType:    types.Time,
			convertedType: schema.ConvertedTypes.TimeMicros,
			expected:      tree.MakeDTime(timeofday.TimeOfDay(3600000000)),
		},
		{
			name:          "int64-timestamp-millis-converted-type",
			value:         int64(1577836800000), // 2020-01-01 00:00:00 UTC
			targetType:    types.TimestampTZ,
			convertedType: schema.ConvertedTypes.TimestampMillis,
			expected: func() tree.Datum {
				ts := time.Unix(1577836800, 0).UTC()
				d, _ := tree.MakeDTimestampTZ(ts, time.Microsecond)
				return d
			}(),
		},
		{
			name:          "int64-timestamp-micros-converted-type",
			value:         int64(1577836800000000), // 2020-01-01 00:00:00 UTC
			targetType:    types.TimestampTZ,
			convertedType: schema.ConvertedTypes.TimestampMicros,
			expected: func() tree.Datum {
				ts := time.Unix(1577836800, 0).UTC()
				d, _ := tree.MakeDTimestampTZ(ts, time.Microsecond)
				return d
			}(),
		},
		{
			name:          "int96-to-timestamptz",
			value:         parquet.Int96{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xd1, 0x86, 0x24, 0x00}, // Epoch timestamp
			targetType:    types.TimestampTZ,
			convertedType: schema.ConvertedTypes.None,
			expected: func() tree.Datum {
				// INT96 with Julian day 2440209 (0x002486d1) represents Jan 1, 1970
				ts := parquet.Int96{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xd1, 0x86, 0x24, 0x00}.ToTime()
				d, _ := tree.MakeDTimestampTZ(ts, time.Microsecond)
				return d
			}(),
		},
		{
			name:          "bytes-utf8-converted-type",
			value:         []byte("hello"),
			targetType:    types.String,
			convertedType: schema.ConvertedTypes.UTF8,
			expected:      tree.NewDString("hello"),
		},
		{
			name:          "bytes-json-converted-type",
			value:         []byte(`{"key":"value"}`),
			targetType:    types.Jsonb,
			convertedType: schema.ConvertedTypes.JSON,
			expected: func() tree.Datum {
				j, _ := tree.ParseDJSON(`{"key":"value"}`)
				return j
			}(),
		},
		{
			name:       "int32-decimal-converted-type",
			value:      int32(12345),
			targetType: types.Decimal,
			// Decimal ConvertedType requires DecimalMetadata, handled separately below
			convertedType: schema.ConvertedTypes.Decimal,
			expected: func() tree.Datum {
				d, _ := tree.ParseDDecimal("123.45")
				return d
			}(),
		},
		{
			name:       "int64-decimal-converted-type",
			value:      int64(123456789),
			targetType: types.Decimal,
			// Decimal ConvertedType requires DecimalMetadata, handled separately below
			convertedType: schema.ConvertedTypes.Decimal,
			expected: func() tree.Datum {
				d, _ := tree.ParseDDecimal("1234.56789")
				return d
			}(),
		},
		// Enum ConvertedType skipped - enums require complex hydration setup (see read_import_parquet_logical_test.go)
		{
			name:          "fixed-interval-converted-type",
			value:         parquet.FixedLenByteArray{0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0xE8, 0x03, 0x00, 0x00}, // 1 month, 2 days, 1000 millis
			targetType:    types.Interval,
			convertedType: schema.ConvertedTypes.Interval,
			expected: func() tree.Datum {
				// 1 month, 2 days, 1000 milliseconds = 1 second
				d, _ := tree.ParseDInterval(duration.IntervalStyle_POSTGRES, "1 month 2 days 00:00:01")
				return d
			}(),
		},
		{
			name:          "int32-int8-converted-type",
			value:         int32(127),
			targetType:    types.Int,
			convertedType: schema.ConvertedTypes.Int8,
			expected:      tree.NewDInt(127),
		},
		{
			name:          "int32-int16-converted-type",
			value:         int32(32767),
			targetType:    types.Int,
			convertedType: schema.ConvertedTypes.Int16,
			expected:      tree.NewDInt(32767),
		},
		{
			name:          "int32-int32-converted-type",
			value:         int32(2147483647),
			targetType:    types.Int,
			convertedType: schema.ConvertedTypes.Int32,
			expected:      tree.NewDInt(2147483647),
		},
		{
			name:          "int64-int64-converted-type",
			value:         int64(9223372036854775807),
			targetType:    types.Int,
			convertedType: schema.ConvertedTypes.Int64,
			expected:      tree.NewDInt(9223372036854775807),
		},
		{
			name:          "int32-uint8-converted-type",
			value:         int32(255),
			targetType:    types.Int,
			convertedType: schema.ConvertedTypes.Uint8,
			expected:      tree.NewDInt(255),
		},
		{
			name:          "int32-uint16-converted-type",
			value:         int32(65535),
			targetType:    types.Int,
			convertedType: schema.ConvertedTypes.Uint16,
			expected:      tree.NewDInt(65535),
		},
		{
			name:          "int32-uint32-converted-type",
			value:         int32(2147483647), // Max positive int32 (simulating uint32)
			targetType:    types.Int,
			convertedType: schema.ConvertedTypes.Uint32,
			expected:      tree.NewDInt(2147483647),
		},
		{
			name:          "int64-uint64-converted-type",
			value:         int64(9223372036854775807), // Max positive int64 (simulating uint64)
			targetType:    types.Int,
			convertedType: schema.ConvertedTypes.Uint64,
			expected:      tree.NewDInt(9223372036854775807),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Convert ConvertedType to LogicalType using Apache Arrow's standard conversion
			// This tests that ToLogicalType() produces correct results for legacy files
			var logicalType schema.LogicalType

			// Decimal ConvertedType requires DecimalMetadata (precision and scale)
			if tc.convertedType == schema.ConvertedTypes.Decimal {
				var scale int32
				if tc.name == "int32-decimal-converted-type" {
					scale = 2 // 12345 with scale 2 = 123.45
				} else if tc.name == "int64-decimal-converted-type" {
					scale = 5 // 123456789 with scale 5 = 1234.56789
				}
				decimalMeta := schema.DecimalMetadata{Scale: scale, Precision: 10}
				logicalType = tc.convertedType.ToLogicalType(decimalMeta)
			} else {
				logicalType = tc.convertedType.ToLogicalType(schema.DecimalMetadata{})
			}

			metadata := &parquetColumnMetadata{
				logicalType:   logicalType,
				convertedType: tc.convertedType,
			}

			result, err := convertWithLogicalType(tc.value, tc.targetType, metadata)
			if tc.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			if result.ResolvedType().Family() == types.BytesFamily {
				expectedBytes := tree.MustBeDBytes(tc.expected)
				resultBytes := tree.MustBeDBytes(result)
				require.Equal(t, []byte(expectedBytes), []byte(resultBytes))
			} else if result.ResolvedType().Family() == types.DecimalFamily {
				expectedDec := tree.MustBeDDecimal(tc.expected)
				resultDec := tree.MustBeDDecimal(result)
				require.Equal(t, expectedDec.String(), resultDec.String())
			} else {
				require.Equal(t, tc.expected.String(), result.String())
			}
		})
	}
}

// TestParquetReadLegacyTitanicFile verifies we can read legacy Parquet files
// that use ConvertedType annotations instead of modern LogicalType.
func TestParquetReadLegacyTitanicFile(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Open the legacy Titanic file from testdata
	dir := datapathutils.TestDataPath(t, "parquet")
	legacyPath := filepath.Join(dir, "titanic_legacy.parquet")

	f, err := os.Open(legacyPath)
	require.NoError(t, err)
	defer f.Close()

	// Get file size
	stat, err := f.Stat()
	require.NoError(t, err)

	// Create fileReader
	fileReader := &fileReader{
		Reader:   f,
		ReaderAt: f,
		Seeker:   f,
		total:    stat.Size(),
	}

	// Create producer
	producer, err := newParquetRowProducer(fileReader, nil)
	require.NoError(t, err)
	require.NotNil(t, producer)

	// Verify basic metadata
	t.Logf("Total rows: %d", producer.totalRows)
	t.Logf("Num columns: %d", producer.numColumns)
	t.Logf("Total row groups: %d", producer.totalRowGroups)

	// The Titanic dataset has 891 rows
	require.Greater(t, producer.totalRows, int64(0), "should have rows")
	require.Equal(t, 12, producer.numColumns, "Titanic has 12 columns")

	// Scan through some rows to verify we can read the data
	rowCount := 0
	for producer.Scan() && rowCount < 10 {
		row, err := producer.Row()
		require.NoError(t, err)
		require.NotNil(t, row)
		rowCount++
	}

	require.NoError(t, producer.Err())
	require.Equal(t, 10, rowCount, "should read first 10 rows successfully")

	t.Logf("Successfully read %d rows from legacy ConvertedType Parquet file", rowCount)
}
