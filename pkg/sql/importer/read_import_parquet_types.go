// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"encoding/binary"
	"time"

	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/schema"
	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil/pgdate"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// parquetConversionFunc is a function that converts a Parquet value to a CRDB datum.
// The function is determined once per column based on the Parquet file's type annotations
// (LogicalType for modern files, ConvertedType for legacy files).
type parquetConversionFunc func(value any, targetType *types.T, metadata *parquetColumnMetadata) (tree.Datum, error)

// parquetColumnMetadata caches immutable schema information for a column.
// This is populated once when the file is opened and reused across all batches.
type parquetColumnMetadata struct {
	logicalType   schema.LogicalType
	convertedType schema.ConvertedType
}

// Helper functions for common Parquet type conversions.
// These are used by both LogicalType and ConvertedType conversion paths.
// These functions are tested indirectly through read_import_parquet_logical_test.go.

// convertDateFromInt32 converts an int32 value representing days since Unix epoch to a Date datum.
func convertDateFromInt32(v int32) (tree.Datum, error) {
	d, err := pgdate.MakeDateFromUnixEpoch(int64(v))
	if err != nil {
		return nil, err
	}
	return tree.NewDDate(d), nil
}

// convertTimeMillisFromInt32 converts an int32 value representing milliseconds since midnight to a Time datum.
func convertTimeMillisFromInt32(v int32) (tree.Datum, error) {
	micros := int64(v) * 1000 // Convert milliseconds to microseconds
	return tree.MakeDTime(timeofday.TimeOfDay(micros)), nil
}

// convertTimeMicrosFromInt64 converts an int64 value representing microseconds since midnight to a Time datum.
func convertTimeMicrosFromInt64(v int64) (tree.Datum, error) {
	return tree.MakeDTime(timeofday.TimeOfDay(v)), nil
}

// convertTimeNanosFromInt64 converts an int64 value representing nanoseconds since midnight to a Time datum.
func convertTimeNanosFromInt64(v int64) (tree.Datum, error) {
	micros := v / 1000 // Convert nanoseconds to microseconds
	return tree.MakeDTime(timeofday.TimeOfDay(micros)), nil
}

// convertTimestampMillisFromInt64 converts an int64 value representing milliseconds since Unix epoch to a TimestampTZ datum.
func convertTimestampMillisFromInt64(v int64) (tree.Datum, error) {
	ts := timeutil.Unix(v/1000, (v%1000)*1000000).UTC()
	return tree.MakeDTimestampTZ(ts, time.Microsecond)
}

// convertTimestampMicrosFromInt64 converts an int64 value representing microseconds since Unix epoch to a TimestampTZ datum.
func convertTimestampMicrosFromInt64(v int64) (tree.Datum, error) {
	ts := timeutil.Unix(v/1000000, (v%1000000)*1000).UTC()
	return tree.MakeDTimestampTZ(ts, time.Microsecond)
}

// convertTimestampNanosFromInt64 converts an int64 value representing nanoseconds since Unix epoch to a TimestampTZ datum.
func convertTimestampNanosFromInt64(v int64) (tree.Datum, error) {
	ts := timeutil.Unix(v/1000000000, v%1000000000).UTC()
	return tree.MakeDTimestampTZ(ts, time.Microsecond)
}

// convertTimestampFromInt96 converts a Parquet INT96 value to a TimestampTZ datum.
// INT96 is a deprecated timestamp format used by older Spark versions.
// Format: 12 bytes = 8 bytes nanoseconds since midnight + 4 bytes Julian day number.
func convertTimestampFromInt96(v parquet.Int96) (tree.Datum, error) {
	// Use the built-in ToTime() method provided by the Parquet library
	ts := v.ToTime()
	return tree.MakeDTimestampTZ(ts, time.Microsecond)
}

// convertUuidFromFixedLenByteArray converts a Parquet UUID value to a DUuid datum.
// Parquet UUID format: 16 bytes in standard UUID binary representation.
func convertUuidFromFixedLenByteArray(v []byte) (tree.Datum, error) {
	if len(v) != 16 {
		return nil, errors.Newf("UUID must be 16 bytes, got %d", len(v))
	}
	uid, err := uuid.FromBytes(v)
	if err != nil {
		return nil, err
	}
	return tree.NewDUuid(tree.DUuid{UUID: uid}), nil
}

// convertIntervalFromFixedLenByteArray converts a Parquet INTERVAL value to a DInterval datum.
// Parquet INTERVAL format: 12 bytes = 4 bytes months + 4 bytes days + 4 bytes milliseconds (all little-endian uint32).
func convertIntervalFromFixedLenByteArray(v []byte) (tree.Datum, error) {
	if len(v) != 12 {
		return nil, errors.Newf("interval must be 12 bytes, got %d", len(v))
	}

	// Read 3 uint32 values in little-endian format
	months := binary.LittleEndian.Uint32(v[0:4])
	days := binary.LittleEndian.Uint32(v[4:8])
	milliseconds := binary.LittleEndian.Uint32(v[8:12])

	// Convert milliseconds to nanoseconds
	nanos := int64(milliseconds) * 1000000

	// Create duration (months and days are signed in CockroachDB, but unsigned in Parquet)
	d := duration.MakeDuration(nanos, int64(days), int64(months))
	return tree.NewDInterval(d, types.DefaultIntervalTypeMetadata), nil
}

// formatDecimalFromInt32 formats an int32 value as a decimal string with the given scale.
// Uses the apd library to handle all edge cases including math.MinInt32.
func formatDecimalFromInt32(v int32, scale int32) string {
	// Create decimal from int32 with negative exponent equal to scale
	// e.g., v=12345, scale=2 -> 12345 * 10^-2 = 123.45
	// apd stores the exponent directly without computing 10^scale, so there's no overflow risk
	d := apd.New(int64(v), -scale)
	return d.Text('f')
}

// formatDecimalFromInt64 formats an int64 value as a decimal string with the given scale.
// Uses the apd library to handle all edge cases including math.MinInt64.
func formatDecimalFromInt64(v int64, scale int32) string {
	// Create decimal from int64 with negative exponent equal to scale
	// e.g., v=123456789, scale=4 -> 123456789 * 10^-4 = 12345.6789
	// apd stores the exponent directly without computing 10^scale, so there's no overflow risk
	d := apd.New(v, -scale)
	return d.Text('f')
}

// convertBytesBasedOnTargetType converts a byte array to a datum based on the target column type.
// This handles the common fallback logic when no specific logical/converted type is available.
func convertBytesBasedOnTargetType(v []byte, targetType *types.T) (tree.Datum, error) {
	switch targetType.Family() {
	case types.StringFamily:
		return tree.NewDString(string(v)), nil
	case types.BytesFamily:
		return tree.NewDBytes(tree.DBytes(v)), nil
	case types.TimestampFamily:
		if len(v) == 0 {
			return tree.DNull, nil
		}
		ts, _, err := tree.ParseDTimestamp(nil, string(v), time.Microsecond)
		if err != nil {
			return nil, err
		}
		return ts, nil
	case types.DecimalFamily:
		if len(v) == 0 {
			return tree.DNull, nil
		}
		return tree.ParseDDecimal(string(v))
	case types.JsonFamily:
		if len(v) == 0 {
			return tree.DNull, nil
		}
		return tree.ParseDJSON(string(v))
	case types.GeographyFamily:
		if len(v) == 0 {
			return tree.DNull, nil
		}
		// Parquet stores spatial data as WKB/EWKB (Well-Known Binary)
		g, err := geo.ParseGeographyFromEWKB(geopb.EWKB(v))
		if err != nil {
			return nil, err
		}
		return tree.NewDGeography(g), nil
	case types.GeometryFamily:
		if len(v) == 0 {
			return tree.DNull, nil
		}
		// Parquet stores spatial data as WKB/EWKB (Well-Known Binary)
		g, err := geo.ParseGeometryFromEWKB(geopb.EWKB(v))
		if err != nil {
			return nil, err
		}
		return tree.NewDGeometry(g), nil
	case types.EnumFamily:
		if len(v) == 0 {
			return tree.DNull, nil
		}
		// Parquet stores enums as string representation
		enum, err := tree.MakeDEnumFromLogicalRepresentation(targetType, string(v))
		if err != nil {
			return nil, err
		}
		return tree.NewDEnum(enum), nil
	case types.IntervalFamily:
		if len(v) == 0 {
			return tree.DNull, nil
		}
		// Fallback: BYTE_ARRAY (not fixed-length) targeting INTERVAL
		// Standard Parquet intervals use FIXED_LEN_BYTE_ARRAY(12) with IntervalLogicalType,
		// but some files may store intervals as string representation (e.g., "1 year 2 months")
		return tree.ParseDInterval(duration.IntervalStyle_POSTGRES, string(v))
	default:
		return tree.NewDString(string(v)), nil
	}
}
