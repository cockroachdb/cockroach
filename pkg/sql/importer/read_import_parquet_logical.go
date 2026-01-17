// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package importer

import (
	"fmt"

	"github.com/apache/arrow/go/v11/parquet"
	"github.com/apache/arrow/go/v11/parquet/schema"
	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

// convertWithLogicalType implements parquetConversionFunc for Parquet files using
// LogicalType annotations. This is used for files written by recent tools (Apache Arrow,
// modern Spark, etc.).
//
// Supported LogicalTypes:
//   - StringLogicalType, JSONLogicalType, EnumLogicalType (BYTE_ARRAY)
//   - IntLogicalType (INT32, INT64)
//   - DecimalLogicalType (INT32, INT64)
//   - DateLogicalType (INT32)
//   - TimeLogicalType (INT32 for MILLIS, INT64 for MICROS/NANOS)
//   - TimestampLogicalType (INT64)
//   - UUIDLogicalType (FIXED_LEN_BYTE_ARRAY(16))
//   - IntervalLogicalType (FIXED_LEN_BYTE_ARRAY(12))
//
// Unsupported LogicalTypes (would require nested column chunk handling):
//   - ListLogicalType - Nested lists (could map to JSONB)
//   - MapLogicalType - Nested maps (could map to JSONB)
//
// Unsupported LogicalTypes (low priority):
//   - BSONLogicalType - Binary JSON (could convert to JSONB)
var convertWithLogicalType parquetConversionFunc = func(
	value any, targetType *types.T, metadata *parquetColumnMetadata,
) (tree.Datum, error) {
	if metadata.logicalType == nil {
		return nil, errors.AssertionFailedf("convertWithLogicalType called with nil logicalType")
	}

	// NullLogicalType represents a column that only contains null values
	if _, ok := metadata.logicalType.(schema.NullLogicalType); ok {
		return tree.DNull, nil
	}

	switch v := value.(type) {
	case bool:
		return tree.MakeDBool(tree.DBool(v)), nil

	case int32:
		switch lt := metadata.logicalType.(type) {
		case schema.DateLogicalType:
			return convertDateFromInt32(v)
		case *schema.TimeLogicalType:
			if lt.TimeUnit() == schema.TimeUnitMillis {
				return convertTimeMillisFromInt32(v)
			}
			// INT32 only supports TIME(MILLIS), other units are invalid
			return nil, errors.Newf("INT32 TIME logical type only supports MILLIS unit, got %v", lt.TimeUnit())
		case *schema.DecimalLogicalType:
			// For scale=0 decimals targeting INT columns, return DInt
			if lt.Scale() == 0 && targetType.Family() == types.IntFamily {
				return tree.NewDInt(tree.DInt(v)), nil
			}
			decStr := formatDecimalFromInt32(v, lt.Scale())
			return tree.ParseDDecimal(decStr)
		}
		// Fallback: check if target is decimal
		if targetType.Family() == types.DecimalFamily {
			return &tree.DDecimal{Decimal: *apd.New(int64(v), 0)}, nil
		}
		return tree.NewDInt(tree.DInt(v)), nil

	case int64:
		switch lt := metadata.logicalType.(type) {
		case *schema.TimestampLogicalType:
			switch lt.TimeUnit() {
			case schema.TimeUnitMillis:
				return convertTimestampMillisFromInt64(v)
			case schema.TimeUnitMicros:
				return convertTimestampMicrosFromInt64(v)
			case schema.TimeUnitNanos:
				return convertTimestampNanosFromInt64(v)
			default:
				return nil, errors.Newf("unsupported TIMESTAMP time unit: %v", lt.TimeUnit())
			}
		case *schema.TimeLogicalType:
			switch lt.TimeUnit() {
			case schema.TimeUnitMicros:
				return convertTimeMicrosFromInt64(v)
			case schema.TimeUnitNanos:
				return convertTimeNanosFromInt64(v)
			default:
				// INT64 only supports TIME(MICROS) and TIME(NANOS), MILLIS is invalid
				return nil, errors.Newf("INT64 TIME logical type only supports MICROS/NANOS units, got %v", lt.TimeUnit())
			}
		case *schema.DecimalLogicalType:
			// For scale=0 decimals targeting INT columns, return DInt
			if lt.Scale() == 0 && targetType.Family() == types.IntFamily {
				return tree.NewDInt(tree.DInt(v)), nil
			}
			decStr := formatDecimalFromInt64(v, lt.Scale())
			return tree.ParseDDecimal(decStr)
		}
		// Fallback: check if target is decimal
		if targetType.Family() == types.DecimalFamily {
			return &tree.DDecimal{Decimal: *apd.New(v, 0)}, nil
		}
		return tree.NewDInt(tree.DInt(v)), nil

	case float32:
		if targetType.Family() == types.DecimalFamily {
			return tree.ParseDDecimal(fmt.Sprintf("%g", v))
		}
		return tree.NewDFloat(tree.DFloat(v)), nil

	case float64:
		if targetType.Family() == types.DecimalFamily {
			return tree.ParseDDecimal(fmt.Sprintf("%g", v))
		}
		return tree.NewDFloat(tree.DFloat(v)), nil

	case []byte:
		// Check for specific logical types
		if _, ok := metadata.logicalType.(schema.StringLogicalType); ok {
			if targetType.Family() == types.StringFamily {
				return tree.NewDString(string(v)), nil
			}
			// StringLogicalType with non-string target: fallthrough to target-based conversion
		}
		if _, ok := metadata.logicalType.(schema.JSONLogicalType); ok {
			if len(v) == 0 {
				return tree.DNull, nil
			}
			return tree.ParseDJSON(string(v))
		}
		if _, ok := metadata.logicalType.(schema.EnumLogicalType); ok {
			if len(v) == 0 {
				return tree.DNull, nil
			}
			// Enum stored as string representation
			enum, err := tree.MakeDEnumFromLogicalRepresentation(targetType, string(v))
			if err != nil {
				return nil, err
			}
			return tree.NewDEnum(enum), nil
		}

		// Fall back to target column type
		return convertBytesBasedOnTargetType(v, targetType)

	case parquet.FixedLenByteArray:
		// Check for UUID logical type
		if _, ok := metadata.logicalType.(schema.UUIDLogicalType); ok {
			return convertUuidFromFixedLenByteArray(v)
		}
		// Check for INTERVAL logical type
		if _, ok := metadata.logicalType.(schema.IntervalLogicalType); ok {
			return convertIntervalFromFixedLenByteArray(v)
		}
		// Fallback to target type
		if targetType.Family() == types.UuidFamily {
			return convertUuidFromFixedLenByteArray(v)
		}
		if targetType.Family() == types.IntervalFamily {
			// IntervalLogicalType may not be set, but target is INTERVAL
			if len(v) == 12 {
				return convertIntervalFromFixedLenByteArray(v)
			}
		}
		return tree.NewDBytes(tree.DBytes(v)), nil

	case parquet.Int96:
		// INT96 is a deprecated timestamp format (12 bytes: nanoseconds + Julian day).
		// Convert to timestamp using the built-in ToTime() method.
		return convertTimestampFromInt96(v)

	default:
		return nil, errors.Newf("unsupported Parquet value type: %T", value)
	}
}

// validateWithLogicalType is called when a parquet file is first opened to
// validate the type compatibility of the LogicalType annotations against
// the intended target database type.
func validateWithLogicalType(
	physicalType parquet.Type, logicalType schema.LogicalType, targetType *types.T,
) error {
	if logicalType == nil {
		return errors.AssertionFailedf("validateWithLogicalType called with nil logicalType")
	}

	// NullLogicalType represents columns that only contain null values - always valid
	if _, ok := logicalType.(schema.NullLogicalType); ok {
		return nil
	}

	// Check for unsupported LogicalTypes that require nested column handling
	if _, ok := logicalType.(schema.ListLogicalType); ok {
		return errors.Newf("ListLogicalType is not supported (requires nested column chunk handling)")
	}
	if _, ok := logicalType.(schema.MapLogicalType); ok {
		return errors.Newf("MapLogicalType is not supported (requires nested column chunk handling)")
	}
	if _, ok := logicalType.(schema.BSONLogicalType); ok {
		return errors.Newf("BSONLogicalType is not supported")
	}

	// UnknownLogicalType is allowed - we'll use fallback conversions based on physical type

	switch physicalType {
	case parquet.Types.Boolean:
		// Boolean can only go to Bool
		if targetType.Family() != types.BoolFamily {
			return errors.Newf("boolean type cannot be converted to %s", targetType.Family())
		}

	case parquet.Types.Int32:
		// Check for special logical types first
		if _, ok := logicalType.(schema.DateLogicalType); ok {
			if targetType.Family() != types.DateFamily {
				return errors.Newf("Date logical type requires DATE target, got %s", targetType.Family())
			}
			return nil
		}
		if timeType, ok := logicalType.(*schema.TimeLogicalType); ok {
			// INT32 only supports TIME(MILLIS)
			if timeType.TimeUnit() != schema.TimeUnitMillis {
				return errors.Newf("INT32 TIME logical type only supports MILLIS unit, got %v", timeType.TimeUnit())
			}
			if targetType.Family() != types.TimeFamily {
				return errors.Newf("Time logical type requires TIME target, got %s", targetType.Family())
			}
			return nil
		}
		if _, ok := logicalType.(*schema.DecimalLogicalType); ok {
			if targetType.Family() != types.DecimalFamily && targetType.Family() != types.IntFamily {
				return errors.Newf("Decimal logical type requires DECIMAL or INT target, got %s", targetType.Family())
			}
			return nil
		}

		// Plain int32 - can go to Int or Decimal
		if targetType.Family() != types.IntFamily && targetType.Family() != types.DecimalFamily {
			return errors.Newf("int32 type can only be converted to INT or DECIMAL, got %s", targetType.Family())
		}

	case parquet.Types.Int64:
		// Check for special logical types
		if _, ok := logicalType.(*schema.TimestampLogicalType); ok {
			if targetType.Family() != types.TimestampFamily && targetType.Family() != types.TimestampTZFamily {
				return errors.Newf("Timestamp logical type requires TIMESTAMP/TIMESTAMPTZ target, got %s", targetType.Family())
			}
			return nil
		}
		if timeType, ok := logicalType.(*schema.TimeLogicalType); ok {
			// INT64 only supports TIME(MICROS) and TIME(NANOS)
			if timeType.TimeUnit() != schema.TimeUnitMicros && timeType.TimeUnit() != schema.TimeUnitNanos {
				return errors.Newf("INT64 TIME logical type only supports MICROS/NANOS units, got %v", timeType.TimeUnit())
			}
			if targetType.Family() != types.TimeFamily {
				return errors.Newf("Time logical type requires TIME target, got %s", targetType.Family())
			}
			return nil
		}
		if _, ok := logicalType.(*schema.DecimalLogicalType); ok {
			if targetType.Family() != types.DecimalFamily && targetType.Family() != types.IntFamily {
				return errors.Newf("Decimal logical type requires DECIMAL or INT target, got %s", targetType.Family())
			}
			return nil
		}

		// Plain int64 - can go to Int or Decimal
		if targetType.Family() != types.IntFamily && targetType.Family() != types.DecimalFamily {
			return errors.Newf("int64 type can only be converted to INT or DECIMAL, got %s", targetType.Family())
		}

	case parquet.Types.Float:
		// Float32 can go to Float or Decimal
		if targetType.Family() != types.FloatFamily && targetType.Family() != types.DecimalFamily {
			return errors.Newf("float type can only be converted to FLOAT or DECIMAL, got %s", targetType.Family())
		}

	case parquet.Types.Double:
		// Float64 can go to Float or Decimal
		if targetType.Family() != types.FloatFamily && targetType.Family() != types.DecimalFamily {
			return errors.Newf("double type can only be converted to FLOAT or DECIMAL, got %s", targetType.Family())
		}

	case parquet.Types.ByteArray:
		// ByteArray is very flexible
		// Check for specific logical types
		if _, ok := logicalType.(schema.StringLogicalType); ok {
			// StringLogicalType prefers STRING/BYTES but can fall through to other target types
			// for flexible conversions (e.g., Geography, Geometry stored as WKB)
			if targetType.Family() == types.StringFamily || targetType.Family() == types.BytesFamily {
				return nil
			}
			// Fall through to general ByteArray validation
		}
		if _, ok := logicalType.(schema.JSONLogicalType); ok {
			if targetType.Family() != types.JsonFamily {
				return errors.Newf("JSON logical type requires JSONB target, got %s", targetType.Family())
			}
			return nil
		}

		// Plain ByteArray can go to String, Bytes, or be parsed as Timestamp/Decimal/JSON/Geography/Geometry/Enum/Interval
		// We allow these flexible conversions
		validFamilies := []types.Family{
			types.StringFamily,
			types.BytesFamily,
			types.TimestampFamily,
			types.TimestampTZFamily,
			types.DecimalFamily,
			types.JsonFamily,
			types.GeographyFamily,
			types.GeometryFamily,
			types.EnumFamily,
			types.IntervalFamily,
		}
		for _, family := range validFamilies {
			if targetType.Family() == family {
				return nil
			}
		}
		return errors.Newf("byte array type cannot be converted to %s", targetType.Family())

	case parquet.Types.FixedLenByteArray:
		// FixedLenByteArray can go to UUID, Bytes, String, or Interval (12 bytes)
		validFamilies := []types.Family{
			types.UuidFamily,
			types.BytesFamily,
			types.StringFamily,
			types.IntervalFamily,
		}
		for _, family := range validFamilies {
			if targetType.Family() == family {
				return nil
			}
		}
		return errors.Newf("fixed-length byte array type cannot be converted to %s", targetType.Family())

	case parquet.Types.Int96:
		// INT96 is a deprecated timestamp format used by legacy Spark files.
		// It can only be converted to Timestamp/TimestampTZ types.
		if targetType.Family() != types.TimestampFamily && targetType.Family() != types.TimestampTZFamily {
			return errors.Newf("INT96 timestamp type requires TIMESTAMP/TIMESTAMPTZ target, got %s", targetType.Family())
		}

	default:
		return errors.Newf("unsupported Parquet physical type: %v", physicalType)
	}

	return nil
}
