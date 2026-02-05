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
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			// Convert ConvertedType to LogicalType using Apache Arrow's standard conversion
			// This tests that ToLogicalType() produces correct results for legacy files
			logicalType := tc.convertedType.ToLogicalType(schema.DecimalMetadata{})

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
