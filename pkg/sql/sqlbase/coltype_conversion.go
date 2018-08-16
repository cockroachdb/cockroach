// Copyright 2018 The Cockroach Authors.
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

package sqlbase

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
	"github.com/pkg/errors"
)

// DatumTypeToColumnType converts a parser Type to a ColumnType.
//
// After calling this, the caller must also call PopulateTypeAttrs
// below.
func DatumTypeToColumnType(ptyp types.T) (ColumnType, error) {
	var ctyp ColumnType
	switch t := ptyp.(type) {
	case types.TCollatedString:
		ctyp.SemanticType = ColumnType_COLLATEDSTRING
		ctyp.Locale = &t.Locale

	case types.TArray:
		ctyp.SemanticType = ColumnType_ARRAY
		contents, err := DatumTypeToColumnSemanticType(t.Typ)
		if err != nil {
			return ColumnType{}, err
		}
		ctyp.ArrayContents = &contents
		if t.Typ.FamilyEqual(types.FamCollatedString) {
			cs := t.Typ.(types.TCollatedString)
			ctyp.Locale = &cs.Locale
		}

	case types.TTuple:
		ctyp.SemanticType = ColumnType_TUPLE
		ctyp.TupleContents = make([]ColumnType, len(t.Types))
		for i, tc := range t.Types {
			var err error
			ctyp.TupleContents[i], err = DatumTypeToColumnType(tc)
			if err != nil {
				return ColumnType{}, err
			}
		}
		ctyp.TupleLabels = t.Labels
		return ctyp, nil

	default:
		semanticType, err := DatumTypeToColumnSemanticType(ptyp)
		if err != nil {
			return ColumnType{}, err
		}
		ctyp.SemanticType = semanticType

	}
	return ctyp, nil
}

// PopulateTypeAttrs set other attributes of col.Type and performs
// type-specific verification.
//
// It must be called after coltypes.CastTargetToDatumType() and
// DatumTypeToColumnType().
func PopulateTypeAttrs(base ColumnType, typ coltypes.T) (ColumnType, error) {
	switch t := typ.(type) {
	case *coltypes.TInt:
		base.Width = int32(t.Width)
		if t.IsBit {
			// TODO(knz): This needs to change because of #20911.
			base.VisibleType = ColumnType_BIT
		} else {
			// Populate Precision for information_schema.columns.
			base.Precision = int32(t.Width)
			switch t.Width {
			case 16:
				base.VisibleType = ColumnType_SMALLINT
			case 32:
				// TODO(knz): we're lying here. This needs to change.
				base.VisibleType = ColumnType_INTEGER
			case 0:
				// TODO(knz): we're lying here. This needs to change.
				base.VisibleType = ColumnType_INTEGER
			case 64:
				base.VisibleType = ColumnType_BIGINT
			default:
				return ColumnType{}, pgerror.NewErrorf(pgerror.CodeInternalError,
					"programming error: unknown int width %d", t.Width)
			}
		}

	case *coltypes.TFloat:
		base.Width = int32(t.Width)
		// Populate Precision for compatibility with pre-2.1 nodes and
		// also information_schema.columns.
		// Also for compatibility with pre-2.1 nodes: populate the
		// VisibleType field. We don't use this post-2.1.
		// TODO(knz): Remove post-2.2.
		switch t.Width {
		case 32:
			base.VisibleType = ColumnType_REAL
			base.Precision = 24
		case 64:
			base.VisibleType = ColumnType_DOUBLE_PRECISION
			base.Precision = 54
		default:
			return ColumnType{}, pgerror.NewErrorf(pgerror.CodeInternalError,
				"programming error: unknown float precision %d", t.Width)
		}

	case *coltypes.TDecimal:
		base.Width = int32(t.Scale)
		base.Precision = int32(t.Prec)

		switch {
		case base.Precision == 0 && base.Width > 0:
			// TODO (seif): Find right range for error message.
			return ColumnType{}, errors.New("invalid NUMERIC precision 0")
		case base.Precision < base.Width:
			return ColumnType{}, fmt.Errorf("NUMERIC scale %d must be between 0 and precision %d",
				base.Width, base.Precision)
		}

	case *coltypes.TString:
		base.Width = int32(t.N)

	case *coltypes.TCollatedString:
		base.Width = int32(t.N)

	case *coltypes.TArray:
		base.ArrayDimensions = t.Bounds
		var err error
		base, err = PopulateTypeAttrs(base, t.ParamType)
		if err != nil {
			return ColumnType{}, err
		}

	case *coltypes.TVector:
		switch t.ParamType.(type) {
		case *coltypes.TInt, *coltypes.TOid:
		default:
			return ColumnType{}, errors.Errorf("vectors of type %s are unsupported", t.ParamType)
		}

	case *coltypes.TBool:
	case *coltypes.TDate:
	case *coltypes.TTime:
	case *coltypes.TTimestamp:
	case *coltypes.TTimestampTZ:
	case *coltypes.TInterval:
	case *coltypes.TUUID:
	case *coltypes.TIPAddr:
	case *coltypes.TJSON:
	case *coltypes.TName:
	case *coltypes.TBytes:
	case *coltypes.TOid:
		// Nothing to do.

	default:
		return ColumnType{}, errors.Errorf("unexpected type %T", t)
	}
	return base, nil
}

// InfoSchemaColumnType returns the string suitable to populate the data_type column
// of information_schema.columns.
func (c *ColumnType) InfoSchemaColumnType() string {
	switch c.SemanticType {
	case ColumnType_BOOL:
		return "boolean"

	case ColumnType_INT:
		if c.VisibleType == ColumnType_BIT {
			return "bit"
		}
		switch c.Width {
		case 0:
			return "integer"
		case 16:
			return "smallint"
		case 32:
			return "int4"
		case 64:
			return "bigint"
		default:
			panic(fmt.Sprintf("programming error: unknown integer width: %d", c.Width))
		}

	case ColumnType_STRING, ColumnType_COLLATEDSTRING:
		return "text"

	case ColumnType_FLOAT:
		width, _ := c.FloatProperties()

		switch width {
		case 64:
			return "double precision"
		case 32:
			return "real"
		default:
			panic(fmt.Sprintf("programming error: unknown float width: %d", width))
		}

	case ColumnType_DECIMAL:
		if c.Precision > 0 {
			if c.Width > 0 {
				return fmt.Sprintf("numeric(%d,%d)", c.Precision, c.Width)
			}
			return fmt.Sprintf("numeric(%d)", c.Precision)
		}

	case ColumnType_TIMESTAMPTZ:
		return "timestamp with time zone"
	case ColumnType_BYTES:
		return "byta"
	case ColumnType_JSON:
		return "jsonb"
	case ColumnType_NULL:
		return "unknown"
	case ColumnType_TUPLE:
		return "record"
	case ColumnType_ARRAY:
		return "ARRAY"
	}

	// date, timestamp, interval, int2vector, oidvector, inet, oid, uuid
	return strings.ToLower(c.SemanticType.String())
}

// SQLString returns the SQL string corresponding to the type. This
// uses CockroachDB-specific native types and is suitable to print out
// SQL syntax that can be re-entered in CockroachDB.
//
// It is *not* suitable for use in introspection.
//
func (c *ColumnType) SQLString() string {
	switch c.SemanticType {
	case ColumnType_INT:
		if c.VisibleType == ColumnType_BIT {
			// TODO(knz): This needs to change because of #20911.
			return fmt.Sprintf("BIT(%d)", c.Width)
		}
		switch c.Width {
		case 0:
			return "INT"
		case 16:
			return "INT2"
		case 32:
			return "INT4"
		case 64:
			return "INT8"
		default:
			panic(fmt.Sprintf("programming error: unknown integer width: %d", c.Width))
		}
	case ColumnType_STRING:
		if c.Width > 0 {
			return fmt.Sprintf("%s(%d)", c.SemanticType.String(), c.Width)
		}

	case ColumnType_FLOAT:
		width := c.Width
		if width == 0 {
			// Pre-2.1 columns: the width is not set yet and instead there
			// is a precision. Reverse-engineer the width from that.
			if c.Precision < 1 {
				panic(fmt.Sprintf("programming error: invalid float precision: %d", c.Precision))
			} else if c.Precision <= 24 {
				width = 32
			} else if c.Precision <= 54 {
				width = 64
			} else {
				panic(fmt.Sprintf("programming error: invalid float precision: %d", c.Precision))
			}
		}

		switch width {
		case 64:
			return "FLOAT8"
		case 32:
			return "FLOAT4"
		default:
			panic(fmt.Sprintf("programming error: unknown float width: %d", width))
		}

	case ColumnType_DECIMAL:
		if c.Precision > 0 {
			if c.Width > 0 {
				return fmt.Sprintf("%s(%d,%d)", c.SemanticType.String(), c.Precision, c.Width)
			}
			return fmt.Sprintf("%s(%d)", c.SemanticType.String(), c.Precision)
		}

	case ColumnType_COLLATEDSTRING:
		if c.Locale == nil {
			panic("locale is required for COLLATEDSTRING")
		}
		if c.Width > 0 {
			return fmt.Sprintf("%s(%d) COLLATE %s", ColumnType_STRING.String(), c.Width, *c.Locale)
		}
		return fmt.Sprintf("%s COLLATE %s", ColumnType_STRING.String(), *c.Locale)

	case ColumnType_ARRAY:
		return c.elementColumnType().SQLString() + "[]"
	}

	return c.SemanticType.String()
}

// DatumTypeToColumnSemanticType converts a types.T to a SemanticType.
func DatumTypeToColumnSemanticType(ptyp types.T) (ColumnType_SemanticType, error) {
	switch ptyp {
	case types.Bool:
		return ColumnType_BOOL, nil
	case types.Int:
		return ColumnType_INT, nil
	case types.Float:
		return ColumnType_FLOAT, nil
	case types.Decimal:
		return ColumnType_DECIMAL, nil
	case types.Bytes:
		return ColumnType_BYTES, nil
	case types.String:
		return ColumnType_STRING, nil
	case types.Name:
		return ColumnType_NAME, nil
	case types.Date:
		return ColumnType_DATE, nil
	case types.Time:
		return ColumnType_TIME, nil
	case types.Timestamp:
		return ColumnType_TIMESTAMP, nil
	case types.TimestampTZ:
		return ColumnType_TIMESTAMPTZ, nil
	case types.Interval:
		return ColumnType_INTERVAL, nil
	case types.UUID:
		return ColumnType_UUID, nil
	case types.INet:
		return ColumnType_INET, nil
	case types.Oid, types.RegClass, types.RegNamespace, types.RegProc, types.RegType, types.RegProcedure:
		return ColumnType_OID, nil
	case types.Unknown:
		return ColumnType_NULL, nil
	case types.IntVector:
		return ColumnType_INT2VECTOR, nil
	case types.OidVector:
		return ColumnType_OIDVECTOR, nil
	case types.JSON:
		return ColumnType_JSON, nil
	default:
		if ptyp.FamilyEqual(types.FamCollatedString) {
			return ColumnType_COLLATEDSTRING, nil
		}
		if ptyp.FamilyEqual(types.FamTuple) {
			return ColumnType_TUPLE, nil
		}
		if wrapper, ok := ptyp.(types.TOidWrapper); ok {
			return DatumTypeToColumnSemanticType(wrapper.T)
		}
		return -1, pgerror.NewErrorf(pgerror.CodeFeatureNotSupportedError,
			"unsupported result type: %s, %T, %+v", ptyp, ptyp, ptyp)
	}
}

// ToDatumType converts the ColumnType to a Datum type, or nil if
// there is no correspondence.
func (c *ColumnType) ToDatumType() types.T {
	switch c.SemanticType {
	case ColumnType_ARRAY:
		return types.TArray{Typ: columnSemanticTypeToDatumType(c, *c.ArrayContents)}

	case ColumnType_TUPLE:
		datums := types.TTuple{
			Types:  make([]types.T, len(c.TupleContents)),
			Labels: c.TupleLabels,
		}
		for i := range c.TupleContents {
			datums.Types[i] = c.TupleContents[i].ToDatumType()
		}
		return datums

	default:
		return columnSemanticTypeToDatumType(c, c.SemanticType)
	}
}

func columnSemanticTypeToDatumType(c *ColumnType, k ColumnType_SemanticType) types.T {
	switch k {
	case ColumnType_BOOL:
		return types.Bool
	case ColumnType_INT:
		return types.Int
	case ColumnType_FLOAT:
		return types.Float
	case ColumnType_DECIMAL:
		return types.Decimal
	case ColumnType_STRING:
		return types.String
	case ColumnType_BYTES:
		return types.Bytes
	case ColumnType_DATE:
		return types.Date
	case ColumnType_TIME:
		return types.Time
	case ColumnType_TIMESTAMP:
		return types.Timestamp
	case ColumnType_TIMESTAMPTZ:
		return types.TimestampTZ
	case ColumnType_INTERVAL:
		return types.Interval
	case ColumnType_UUID:
		return types.UUID
	case ColumnType_INET:
		return types.INet
	case ColumnType_JSON:
		return types.JSON
	case ColumnType_TUPLE:
		return types.FamTuple
	case ColumnType_COLLATEDSTRING:
		if c.Locale == nil {
			panic("locale is required for COLLATEDSTRING")
		}
		return types.TCollatedString{Locale: *c.Locale}
	case ColumnType_NAME:
		return types.Name
	case ColumnType_OID:
		return types.Oid
	case ColumnType_NULL:
		return types.Unknown
	case ColumnType_INT2VECTOR:
		return types.IntVector
	case ColumnType_OIDVECTOR:
		return types.OidVector
	}
	panic(fmt.Sprintf("unhandled semantic type: %s", k))
}
