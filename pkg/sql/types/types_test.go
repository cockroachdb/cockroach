// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package types

import (
	"bytes"
	"fmt"
	"reflect"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/oidext"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/lib/pq/oid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTypes(t *testing.T) {
	enCollate := "en"

	testCases := []struct {
		actual   *T
		expected *T
	}{
		// ARRAY
		{MakeArray(Any), AnyArray},
		{MakeArray(Any), &T{InternalType: InternalType{
			Family: ArrayFamily, ArrayContents: Any, Oid: oid.T_anyarray, Locale: &emptyLocale}}},

		{MakeArray(Float), FloatArray},
		{MakeArray(Float), &T{InternalType: InternalType{
			Family: ArrayFamily, ArrayContents: Float, Oid: oid.T__float8, Locale: &emptyLocale}}},

		{MakeArray(Decimal), DecimalArray},
		{MakeArray(Decimal), &T{InternalType: InternalType{
			Family: ArrayFamily, ArrayContents: Decimal, Oid: oid.T__numeric, Locale: &emptyLocale}}},

		{MakeArray(Int), IntArray},
		{MakeArray(Int), &T{InternalType: InternalType{
			Family: ArrayFamily, ArrayContents: Int, Oid: oid.T__int8, Locale: &emptyLocale}}},
		{MakeArray(MakeArray(Int)), &T{InternalType: InternalType{
			Family: ArrayFamily, ArrayContents: IntArray, Oid: oid.T__int8, Locale: &emptyLocale}}},

		{Int2Vector, &T{InternalType: InternalType{
			Family: ArrayFamily, Oid: oid.T_int2vector, ArrayContents: Int2, Locale: &emptyLocale}}},
		{MakeArray(Int2Vector), &T{InternalType: InternalType{
			Family: ArrayFamily, Oid: oid.T__int2vector, ArrayContents: Int2Vector, Locale: &emptyLocale}}},

		{OidVector, &T{InternalType: InternalType{
			Family: ArrayFamily, Oid: oid.T_oidvector, ArrayContents: Oid, Locale: &emptyLocale}}},
		{MakeArray(OidVector), &T{InternalType: InternalType{
			Family: ArrayFamily, Oid: oid.T__oidvector, ArrayContents: OidVector, Locale: &emptyLocale}}},

		{MakeArray(String), StringArray},
		{MakeArray(String), &T{InternalType: InternalType{
			Family: ArrayFamily, ArrayContents: String, Oid: oid.T__text, Locale: &emptyLocale}}},
		{MakeArray(String), &T{InternalType: InternalType{
			Family: ArrayFamily, ArrayContents: String, Oid: oid.T__text, Locale: &emptyLocale}}},
		{MakeArray(MakeArray(String)), &T{InternalType: InternalType{
			Family: ArrayFamily, ArrayContents: StringArray, Oid: oid.T__text, Locale: &emptyLocale}}},

		{MakeArray(AnyTuple), &T{InternalType: InternalType{
			Family: ArrayFamily, ArrayContents: AnyTuple, Oid: oid.T__record, Locale: &emptyLocale}}},

		// BIT
		{MakeBit(0), typeBit},
		{MakeBit(0), &T{InternalType: InternalType{
			Family: BitFamily, Oid: oid.T_bit, Locale: &emptyLocale}}},
		{MakeBit(100), &T{InternalType: InternalType{
			Family: BitFamily, Oid: oid.T_bit, Width: 100, Locale: &emptyLocale}}},
		{MakeBit(100), MakeScalar(BitFamily, oid.T_bit, 0, 100, emptyLocale)},

		{MakeVarBit(0), VarBit},
		{MakeVarBit(0), &T{InternalType: InternalType{
			Family: BitFamily, Oid: oid.T_varbit, Locale: &emptyLocale}}},
		{MakeVarBit(100), &T{InternalType: InternalType{
			Family: BitFamily, Oid: oid.T_varbit, Width: 100, Locale: &emptyLocale}}},
		{MakeVarBit(100), MakeScalar(BitFamily, oid.T_varbit, 0, 100, emptyLocale)},

		// BOOL
		{Bool, &T{InternalType: InternalType{
			Family: BoolFamily, Oid: oid.T_bool, Locale: &emptyLocale}}},
		{Bool, MakeScalar(BoolFamily, oid.T_bool, 0, 0, emptyLocale)},

		// BYTES
		{Bytes, &T{InternalType: InternalType{
			Family: BytesFamily, Oid: oid.T_bytea, Locale: &emptyLocale}}},
		{Bytes, MakeScalar(BytesFamily, oid.T_bytea, 0, 0, emptyLocale)},

		// COLLATEDSTRING
		{MakeCollatedString(String, ""), AnyCollatedString},
		{MakeCollatedString(String, enCollate), &T{InternalType: InternalType{
			Family: CollatedStringFamily, Oid: oid.T_text, Locale: &enCollate}}},
		{MakeCollatedString(MakeString(20), enCollate), &T{InternalType: InternalType{
			Family: CollatedStringFamily, Oid: oid.T_text, Width: 20, Locale: &enCollate}}},
		{MakeCollatedString(MakeString(20), enCollate),
			MakeScalar(CollatedStringFamily, oid.T_text, 0, 20, enCollate)},

		{MakeCollatedString(VarChar, enCollate), &T{InternalType: InternalType{
			Family: CollatedStringFamily, Oid: oid.T_varchar, Locale: &enCollate}}},
		{MakeCollatedString(MakeVarChar(20), enCollate), &T{InternalType: InternalType{
			Family: CollatedStringFamily, Oid: oid.T_varchar, Width: 20, Locale: &enCollate}}},
		{MakeCollatedString(MakeVarChar(20), enCollate),
			MakeScalar(CollatedStringFamily, oid.T_varchar, 0, 20, enCollate)},

		{MakeCollatedString(typeBpChar, enCollate), &T{InternalType: InternalType{
			Family: CollatedStringFamily, Oid: oid.T_bpchar, Locale: &enCollate}}},
		{MakeCollatedString(MakeChar(20), enCollate), &T{InternalType: InternalType{
			Family: CollatedStringFamily, Oid: oid.T_bpchar, Width: 20, Locale: &enCollate}}},
		{MakeCollatedString(MakeChar(20), enCollate),
			MakeScalar(CollatedStringFamily, oid.T_bpchar, 0, 20, enCollate)},

		{MakeCollatedString(typeQChar, enCollate), &T{InternalType: InternalType{
			Family: CollatedStringFamily, Oid: oid.T_char, Locale: &enCollate}}},
		{MakeCollatedString(MakeQChar(20), enCollate), &T{InternalType: InternalType{
			Family: CollatedStringFamily, Oid: oid.T_char, Width: 20, Locale: &enCollate}}},
		{MakeCollatedString(MakeQChar(20), enCollate),
			MakeScalar(CollatedStringFamily, oid.T_char, 0, 20, enCollate)},

		{MakeCollatedString(Name, enCollate), &T{InternalType: InternalType{
			Family: CollatedStringFamily, Oid: oid.T_name, Locale: &enCollate}}},

		// DATE
		{Date, &T{InternalType: InternalType{
			Family: DateFamily, Oid: oid.T_date, Locale: &emptyLocale}}},
		{Date, MakeScalar(DateFamily, oid.T_date, 0, 0, emptyLocale)},

		// DECIMAL
		{MakeDecimal(0, 0), Decimal},
		{MakeDecimal(0, 0), &T{InternalType: InternalType{
			Family: DecimalFamily, Oid: oid.T_numeric, Locale: &emptyLocale}}},
		{MakeDecimal(10, 3), &T{InternalType: InternalType{
			Family: DecimalFamily, Oid: oid.T_numeric, Precision: 10, Width: 3, Locale: &emptyLocale}}},
		{MakeDecimal(10, 3), MakeScalar(DecimalFamily, oid.T_numeric, 10, 3, emptyLocale)},

		// FLOAT
		{Float, &T{InternalType: InternalType{
			Family: FloatFamily, Width: 64, Oid: oid.T_float8, Locale: &emptyLocale}}},
		{Float4, &T{InternalType: InternalType{
			Family: FloatFamily, Width: 32, Oid: oid.T_float4, Locale: &emptyLocale}}},
		{Float4, MakeScalar(FloatFamily, oid.T_float4, 0, 32, emptyLocale)},

		// GEOGRAPHY
		{
			Geography,
			&T{
				InternalType: InternalType{
					Family: GeographyFamily,
					Oid:    oidext.T_geography,
					Locale: &emptyLocale,
					GeoMetadata: &GeoMetadata{
						SRID:      0,
						ShapeType: geopb.ShapeType_Unset,
					},
				},
			},
		},
		{
			Geography,
			MakeScalar(GeographyFamily, oidext.T_geography, 0, 0, emptyLocale),
		},
		{
			&T{
				InternalType: InternalType{
					Family: GeographyFamily,
					Oid:    oidext.T_geography,
					Locale: &emptyLocale,
					GeoMetadata: &GeoMetadata{
						SRID:      4325,
						ShapeType: geopb.ShapeType_MultiPoint,
					},
				},
			},
			MakeGeography(geopb.ShapeType_MultiPoint, 4325),
		},

		// GEOMETRY
		{
			Geometry,
			&T{
				InternalType: InternalType{
					Family: GeometryFamily,
					Oid:    oidext.T_geometry,
					Locale: &emptyLocale,
					GeoMetadata: &GeoMetadata{
						SRID:      0,
						ShapeType: geopb.ShapeType_Unset,
					},
				},
			},
		},
		{
			Geometry,
			MakeScalar(GeometryFamily, oidext.T_geometry, 0, 0, emptyLocale),
		},
		{
			&T{
				InternalType: InternalType{
					Family: GeometryFamily,
					Oid:    oidext.T_geometry,
					Locale: &emptyLocale,
					GeoMetadata: &GeoMetadata{
						SRID:      4325,
						ShapeType: geopb.ShapeType_MultiPoint,
					},
				},
			},
			MakeGeometry(geopb.ShapeType_MultiPoint, 4325),
		},
		// BOX2D
		{
			Box2D,
			&T{InternalType: InternalType{Family: Box2DFamily, Oid: oidext.T_box2d, Locale: &emptyLocale}},
		},
		{
			Box2D,
			MakeScalar(Box2DFamily, oidext.T_box2d, 0, 0, emptyLocale),
		},

		// INET
		{INet, &T{InternalType: InternalType{
			Family: INetFamily, Oid: oid.T_inet, Locale: &emptyLocale}}},
		{INet, MakeScalar(INetFamily, oid.T_inet, 0, 0, emptyLocale)},

		// INT
		{Int, &T{InternalType: InternalType{
			Family: IntFamily, Width: 64, Oid: oid.T_int8, Locale: &emptyLocale}}},
		{Int4, &T{InternalType: InternalType{
			Family: IntFamily, Width: 32, Oid: oid.T_int4, Locale: &emptyLocale}}},
		{Int2, &T{InternalType: InternalType{
			Family: IntFamily, Width: 16, Oid: oid.T_int2, Locale: &emptyLocale}}},
		{Int2, MakeScalar(IntFamily, oid.T_int2, 0, 16, emptyLocale)},

		// INTERVAL
		{
			Interval,
			&T{
				InternalType: InternalType{
					Family:                IntervalFamily,
					Oid:                   oid.T_interval,
					Locale:                &emptyLocale,
					IntervalDurationField: &IntervalDurationField{},
					// Precision and PrecisionIsSet is not set.
				},
			},
		},
		{
			MakeInterval(IntervalTypeMetadata{Precision: 0, PrecisionIsSet: true}),
			MakeScalar(IntervalFamily, oid.T_interval, 0, 0, emptyLocale),
		},
		{
			MakeInterval(IntervalTypeMetadata{Precision: 0, PrecisionIsSet: true}),
			&T{
				InternalType: InternalType{
					Family:                IntervalFamily,
					Precision:             0,
					TimePrecisionIsSet:    true,
					Oid:                   oid.T_interval,
					Locale:                &emptyLocale,
					IntervalDurationField: &IntervalDurationField{},
				},
			},
		},
		{
			MakeInterval(IntervalTypeMetadata{Precision: 3, PrecisionIsSet: true}),
			&T{
				InternalType: InternalType{
					Family:                IntervalFamily,
					Oid:                   oid.T_interval,
					Precision:             3,
					TimePrecisionIsSet:    true,
					Locale:                &emptyLocale,
					IntervalDurationField: &IntervalDurationField{},
				},
			},
		},
		{
			MakeInterval(IntervalTypeMetadata{Precision: 3, PrecisionIsSet: true}),
			MakeScalar(IntervalFamily, oid.T_interval, 3, 0, emptyLocale),
		},
		{
			MakeInterval(IntervalTypeMetadata{Precision: 6, PrecisionIsSet: true}),
			&T{
				InternalType: InternalType{
					Family:                IntervalFamily,
					Oid:                   oid.T_interval,
					Precision:             6,
					TimePrecisionIsSet:    true,
					Locale:                &emptyLocale,
					IntervalDurationField: &IntervalDurationField{},
				},
			},
		},
		{
			MakeInterval(IntervalTypeMetadata{Precision: 6, PrecisionIsSet: true}),
			MakeScalar(IntervalFamily, oid.T_interval, 6, 0, emptyLocale)},
		{
			MakeInterval(IntervalTypeMetadata{
				DurationField: IntervalDurationField{
					DurationType: IntervalDurationType_SECOND,
				},
			}),
			&T{
				InternalType: InternalType{
					Family: IntervalFamily,
					Oid:    oid.T_interval,
					Locale: &emptyLocale,
					IntervalDurationField: &IntervalDurationField{
						DurationType: IntervalDurationType_SECOND,
					},
				},
			},
		},
		{
			MakeInterval(IntervalTypeMetadata{
				DurationField: IntervalDurationField{
					DurationType:     IntervalDurationType_SECOND,
					FromDurationType: IntervalDurationType_MONTH,
				},
				Precision:      3,
				PrecisionIsSet: true,
			}),
			&T{
				InternalType: InternalType{
					Family: IntervalFamily,
					Oid:    oid.T_interval,
					Locale: &emptyLocale,
					IntervalDurationField: &IntervalDurationField{
						DurationType:     IntervalDurationType_SECOND,
						FromDurationType: IntervalDurationType_MONTH,
					},
					Precision:          3,
					TimePrecisionIsSet: true,
				}},
		},

		// JSON
		{Jsonb, &T{InternalType: InternalType{
			Family: JsonFamily, Oid: oid.T_jsonb, Locale: &emptyLocale}}},
		{Jsonb, MakeScalar(JsonFamily, oid.T_jsonb, 0, 0, emptyLocale)},

		// OID
		{Oid, &T{InternalType: InternalType{
			Family: OidFamily, Oid: oid.T_oid, Locale: &emptyLocale}}},
		{RegClass, &T{InternalType: InternalType{
			Family: OidFamily, Oid: oid.T_regclass, Locale: &emptyLocale}}},
		{RegNamespace, &T{InternalType: InternalType{
			Family: OidFamily, Oid: oid.T_regnamespace, Locale: &emptyLocale}}},
		{RegProc, &T{InternalType: InternalType{
			Family: OidFamily, Oid: oid.T_regproc, Locale: &emptyLocale}}},
		{RegProcedure, &T{InternalType: InternalType{
			Family: OidFamily, Oid: oid.T_regprocedure, Locale: &emptyLocale}}},
		{RegType, &T{InternalType: InternalType{
			Family: OidFamily, Oid: oid.T_regtype, Locale: &emptyLocale}}},
		{Oid, MakeScalar(OidFamily, oid.T_oid, 0, 0, emptyLocale)},
		{RegClass, MakeScalar(OidFamily, oid.T_regclass, 0, 0, emptyLocale)},

		// STRING
		{MakeString(0), String},
		{MakeString(0), &T{InternalType: InternalType{
			Family: StringFamily, Oid: oid.T_text, Locale: &emptyLocale}}},
		{MakeString(20), &T{InternalType: InternalType{
			Family: StringFamily, Oid: oid.T_text, Width: 20, Locale: &emptyLocale}}},
		{MakeString(20), MakeScalar(StringFamily, oid.T_text, 0, 20, emptyLocale)},

		{MakeVarChar(0), VarChar},
		{MakeVarChar(0), &T{InternalType: InternalType{
			Family: StringFamily, Oid: oid.T_varchar, Locale: &emptyLocale}}},
		{MakeVarChar(20), &T{InternalType: InternalType{
			Family: StringFamily, Oid: oid.T_varchar, Width: 20, Locale: &emptyLocale}}},
		{MakeVarChar(20), MakeScalar(StringFamily, oid.T_varchar, 0, 20, emptyLocale)},

		{MakeChar(0), typeBpChar},
		{MakeChar(0), &T{InternalType: InternalType{
			Family: StringFamily, Oid: oid.T_bpchar, Locale: &emptyLocale}}},
		{MakeChar(20), &T{InternalType: InternalType{
			Family: StringFamily, Oid: oid.T_bpchar, Width: 20, Locale: &emptyLocale}}},
		{MakeChar(20), MakeScalar(StringFamily, oid.T_bpchar, 0, 20, emptyLocale)},

		{MakeQChar(0), typeQChar},
		{MakeQChar(0), &T{InternalType: InternalType{
			Family: StringFamily, Oid: oid.T_char, Locale: &emptyLocale}}},
		{MakeQChar(20), &T{InternalType: InternalType{
			Family: StringFamily, Oid: oid.T_char, Width: 20, Locale: &emptyLocale}}},
		{MakeQChar(20), MakeScalar(StringFamily, oid.T_char, 0, 20, emptyLocale)},

		{Name, &T{InternalType: InternalType{
			Family: StringFamily, Oid: oid.T_name, Locale: &emptyLocale}}},
		{Name, MakeScalar(StringFamily, oid.T_name, 0, 0, emptyLocale)},

		// TIME
		{Time, &T{InternalType: InternalType{
			Family: TimeFamily,
			Oid:    oid.T_time,
			Locale: &emptyLocale,
			// Precision and PrecisionIsSet is not set.
		}}},
		{MakeTime(0), MakeScalar(TimeFamily, oid.T_time, 0, 0, emptyLocale)},
		{MakeTime(0), &T{InternalType: InternalType{
			Family:             TimeFamily,
			Precision:          0,
			TimePrecisionIsSet: true,
			Oid:                oid.T_time,
			Locale:             &emptyLocale,
		}}},
		{MakeTime(3), &T{InternalType: InternalType{
			Family: TimeFamily, Oid: oid.T_time, Precision: 3, TimePrecisionIsSet: true, Locale: &emptyLocale}}},
		{MakeTime(3), MakeScalar(TimeFamily, oid.T_time, 3, 0, emptyLocale)},
		{MakeTime(6), &T{InternalType: InternalType{
			Family: TimeFamily, Oid: oid.T_time, Precision: 6, TimePrecisionIsSet: true, Locale: &emptyLocale}}},
		{MakeTime(6), MakeScalar(TimeFamily, oid.T_time, 6, 0, emptyLocale)},

		// TIMETZ
		{TimeTZ, &T{InternalType: InternalType{
			Family: TimeTZFamily,
			Oid:    oid.T_timetz,
			Locale: &emptyLocale,
			// Precision and PrecisionIsSet is not set.
		}}},
		{MakeTimeTZ(0), MakeScalar(TimeTZFamily, oid.T_timetz, 0, 0, emptyLocale)},
		{MakeTimeTZ(0), &T{InternalType: InternalType{
			Family:             TimeTZFamily,
			Precision:          0,
			TimePrecisionIsSet: true,
			Oid:                oid.T_timetz,
			Locale:             &emptyLocale,
		}}},
		{MakeTimeTZ(3), &T{InternalType: InternalType{
			Family: TimeTZFamily, Oid: oid.T_timetz, Precision: 3, TimePrecisionIsSet: true, Locale: &emptyLocale}}},
		{MakeTimeTZ(3), MakeScalar(TimeTZFamily, oid.T_timetz, 3, 0, emptyLocale)},
		{MakeTimeTZ(6), &T{InternalType: InternalType{
			Family: TimeTZFamily, Oid: oid.T_timetz, Precision: 6, TimePrecisionIsSet: true, Locale: &emptyLocale}}},
		{MakeTimeTZ(6), MakeScalar(TimeTZFamily, oid.T_timetz, 6, 0, emptyLocale)},

		// TIMESTAMP
		{Timestamp, &T{InternalType: InternalType{
			Family: TimestampFamily,
			Oid:    oid.T_timestamp,
			Locale: &emptyLocale,
			// Precision and PrecisionIsSet is not set.
		}}},
		{MakeTimestamp(0), MakeScalar(TimestampFamily, oid.T_timestamp, 0, 0, emptyLocale)},
		{MakeTimestamp(0), &T{InternalType: InternalType{
			Family:             TimestampFamily,
			Precision:          0,
			TimePrecisionIsSet: true,
			Oid:                oid.T_timestamp,
			Locale:             &emptyLocale,
		}}},
		{MakeTimestamp(3), &T{InternalType: InternalType{
			Family: TimestampFamily, Oid: oid.T_timestamp, Precision: 3, TimePrecisionIsSet: true, Locale: &emptyLocale}}},
		{MakeTimestamp(3), MakeScalar(TimestampFamily, oid.T_timestamp, 3, 0, emptyLocale)},
		{MakeTimestamp(6), &T{InternalType: InternalType{
			Family: TimestampFamily, Oid: oid.T_timestamp, Precision: 6, TimePrecisionIsSet: true, Locale: &emptyLocale}}},
		{MakeTimestamp(6), MakeScalar(TimestampFamily, oid.T_timestamp, 6, 0, emptyLocale)},

		// TIMESTAMPTZ
		{TimestampTZ, &T{InternalType: InternalType{
			Family: TimestampTZFamily,
			Oid:    oid.T_timestamptz,
			Locale: &emptyLocale,
			// Precision and PrecisionIsSet is not set.
		}}},
		{MakeTimestampTZ(0), MakeScalar(TimestampTZFamily, oid.T_timestamptz, 0, 0, emptyLocale)},
		{MakeTimestampTZ(0), &T{InternalType: InternalType{
			Family:             TimestampTZFamily,
			Precision:          0,
			TimePrecisionIsSet: true,
			Oid:                oid.T_timestamptz,
			Locale:             &emptyLocale,
		}}},
		{MakeTimestampTZ(3), &T{InternalType: InternalType{
			Family: TimestampTZFamily, Oid: oid.T_timestamptz, Precision: 3, TimePrecisionIsSet: true, Locale: &emptyLocale}}},
		{MakeTimestampTZ(3), MakeScalar(TimestampTZFamily, oid.T_timestamptz, 3, 0, emptyLocale)},
		{MakeTimestampTZ(6), &T{InternalType: InternalType{
			Family: TimestampTZFamily, Oid: oid.T_timestamptz, Precision: 6, TimePrecisionIsSet: true, Locale: &emptyLocale}}},
		{MakeTimestampTZ(6), MakeScalar(TimestampTZFamily, oid.T_timestamptz, 6, 0, emptyLocale)},

		// TUPLE
		{MakeTuple(nil), EmptyTuple},
		{MakeTuple([]*T{Any}), AnyTuple},
		{MakeTuple([]*T{Int}), &T{InternalType: InternalType{
			Family: TupleFamily, Oid: oid.T_record, TupleContents: []*T{Int}, Locale: &emptyLocale}}},
		{MakeTuple([]*T{Int, String}), &T{InternalType: InternalType{
			Family: TupleFamily, Oid: oid.T_record, TupleContents: []*T{Int, String}, Locale: &emptyLocale}}},

		{MakeLabeledTuple([]*T{Int, String}, []string{"foo", "bar"}), &T{InternalType: InternalType{
			Family: TupleFamily, Oid: oid.T_record, TupleContents: []*T{Int, String},
			TupleLabels: []string{"foo", "bar"}, Locale: &emptyLocale}}},

		// UNKNOWN
		{Unknown, &T{InternalType: InternalType{
			Family: UnknownFamily, Oid: oid.T_unknown, Locale: &emptyLocale}}},
		{Unknown, MakeScalar(UnknownFamily, oid.T_unknown, 0, 0, emptyLocale)},

		// UUID
		{Uuid, &T{InternalType: InternalType{
			Family: UuidFamily, Oid: oid.T_uuid, Locale: &emptyLocale}}},
		{Uuid, MakeScalar(UuidFamily, oid.T_uuid, 0, 0, emptyLocale)},

		// ENUMs
		{MakeEnum(15210, 15213), &T{InternalType: InternalType{
			Family: EnumFamily,
			Locale: &emptyLocale,
			Oid:    15210,
			UDTMetadata: &PersistentUserDefinedTypeMetadata{
				ArrayTypeOID: 15213,
			},
		}}},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("#%d: %s", i, tc.actual.String()), func(t *testing.T) {
			// Test that actual, expected types are identical.
			if !tc.actual.Identical(tc.expected) {
				t.Errorf("expected <%v>, got <%v>", tc.expected.DebugString(), tc.actual.DebugString())
			}
			if !reflect.DeepEqual(tc.actual, tc.expected) {
				t.Errorf("expected <%v>, got <%v>", tc.expected.DebugString(), tc.actual.DebugString())
			}

			// Roundtrip type by marshaling, then unmarshaling. Only do this for non-
			// nested array types (since we don't yet support marshaling/ummarshaling
			// nested arrays).
			if tc.actual.Family() == ArrayFamily && tc.actual.ArrayContents().Family() == ArrayFamily {
				return
			}

			data, err := protoutil.Marshal(tc.actual)
			if err != nil {
				t.Errorf("error during marshal of type <%v>: %v", tc.actual.DebugString(), err)
			}
			if len(data) != tc.actual.Size() {
				t.Errorf("expected %d bytes, got %d bytes", len(data), tc.actual.Size())
			}

			data2 := make([]byte, len(data))
			i, err := tc.actual.MarshalTo(data2)
			if err != nil {
				t.Errorf("error during marshal of type <%v>: %v", tc.actual.DebugString(), err)
			}
			if i != len(data) {
				t.Errorf("expected %d bytes, got %d bytes", len(data), i)
			}
			if !bytes.Equal(data, data2) {
				t.Error("Marshal and MarshalTo bytes are not equal")
			}

			var roundtrip T
			err = protoutil.Unmarshal(data, &roundtrip)
			if err != nil {
				t.Errorf("error during unmarshal of type <%v>: %v", tc.actual.DebugString(), err)
			}
			if !tc.actual.Identical(&roundtrip) {
				t.Errorf("expected <%v>, got <%v>", tc.actual.DebugString(), roundtrip.DebugString())
			}
		})
	}
}

func TestEquivalent(t *testing.T) {
	testCases := []struct {
		typ1  *T
		typ2  *T
		equiv bool
	}{
		// ARRAY
		{Int2Vector, IntArray, true},
		{OidVector, MakeArray(Oid), true},
		{MakeArray(Int), MakeArray(Int4), true},
		{MakeArray(String), MakeArray(MakeChar(10)), true},
		{IntArray, MakeArray(Float), false},
		{MakeArray(String), MakeArray(MakeArray(String)), false},
		{MakeArray(IntArray), IntArray, false},

		// BIT
		{MakeBit(1), MakeBit(2), true},
		{MakeBit(1), MakeVarBit(2), true},
		{MakeVarBit(10), Any, true},
		{VarBit, Bytes, false},

		// COLLATEDSTRING
		{MakeCollatedString(String, "en"), MakeCollatedString(MakeVarChar(10), "en"), true},
		{MakeCollatedString(String, "en"), AnyCollatedString, true},
		{AnyCollatedString, MakeCollatedString(String, "en"), true},
		{MakeCollatedString(String, "en"), MakeCollatedString(String, "de"), false},
		{MakeCollatedString(String, "en"), String, false},

		// DECIMAL
		{Decimal, MakeDecimal(3, 2), true},
		{MakeDecimal(3, 2), MakeDecimal(3, 0), true},
		{Any, MakeDecimal(10, 0), true},
		{Decimal, Float, false},

		// INT
		{Int2, Int4, true},
		{Int4, Int, true},
		{Int, Any, true},
		{Int, IntArray, false},

		// TUPLE
		{MakeTuple([]*T{}), MakeTuple([]*T{}), true},
		{MakeTuple([]*T{Int, String}), MakeTuple([]*T{Int4, VarChar}), true},
		{MakeTuple([]*T{Int, String}), AnyTuple, true},
		{AnyTuple, MakeTuple([]*T{Int, String}), true},
		{MakeTuple([]*T{Int, String}),
			MakeLabeledTuple([]*T{Int4, VarChar}, []string{"label2", "label1"}), true},
		{MakeLabeledTuple([]*T{Int, String}, []string{"label1", "label2"}),
			MakeLabeledTuple([]*T{Int4, VarChar}, []string{"label2", "label1"}), true},
		{MakeTuple([]*T{String, Int}), MakeTuple([]*T{Int, String}), false},

		// ENUM
		{MakeEnum(15210, 15213), MakeEnum(15210, 15213), true},
		{MakeEnum(15210, 15213), MakeEnum(15150, 15213), false},

		// UNKNOWN
		{Unknown, &T{InternalType: InternalType{
			Family: UnknownFamily, Oid: oid.T_unknown, Locale: &emptyLocale}}, true},
		{Any, Unknown, true},
		{Unknown, Int, false},
	}

	for _, tc := range testCases {
		if tc.equiv && !tc.typ1.Equivalent(tc.typ2) {
			t.Errorf("expected <%v> to be equivalent to <%v>",
				tc.typ1.DebugString(), tc.typ2.DebugString())
		}
		if !tc.equiv && tc.typ1.Equivalent(tc.typ2) {
			t.Errorf("expected <%v> to not be equivalent to <%v>",
				tc.typ1.DebugString(), tc.typ2.DebugString())
		}

		// Test equivalent values that are not identical.
		if !reflect.DeepEqual(tc.typ1, tc.typ2) {
			if tc.typ1.Identical(tc.typ2) {
				t.Errorf("expected <%v> to not be identical to <%v>",
					tc.typ1.DebugString(), tc.typ2.DebugString())
			}
		}
	}
}

// TestMarshalCompat tests backwards-compatibility during marshal.
func TestMarshalCompat(t *testing.T) {
	intElemType := IntFamily
	oidElemType := OidFamily
	strElemType := StringFamily
	collStrElemType := CollatedStringFamily
	enLocale := "en"

	testCases := []struct {
		from *T
		to   InternalType
	}{
		// ARRAY
		{Int2Vector, InternalType{Family: int2vector, Oid: oid.T_int2vector, Width: 16,
			ArrayElemType: &intElemType, ArrayContents: Int2}},
		{OidVector, InternalType{Family: oidvector, Oid: oid.T_oidvector,
			ArrayElemType: &oidElemType, ArrayContents: Oid}},
		{IntArray, InternalType{Family: ArrayFamily, Oid: oid.T__int8, Width: 64,
			ArrayElemType: &intElemType, ArrayContents: Int}},
		{MakeArray(MakeVarChar(10)), InternalType{Family: ArrayFamily, Oid: oid.T__varchar, Width: 10, VisibleType: visibleVARCHAR,
			ArrayElemType: &strElemType, ArrayContents: MakeVarChar(10)}},
		{MakeArray(MakeCollatedString(String, enLocale)), InternalType{Family: ArrayFamily, Oid: oid.T__text, Locale: &enLocale,
			ArrayElemType: &collStrElemType, ArrayContents: MakeCollatedString(String, enLocale)}},

		// BIT
		{typeBit, InternalType{Family: BitFamily, Oid: oid.T_bit}},
		{MakeVarBit(10), InternalType{Family: BitFamily, Oid: oid.T_varbit, Width: 10, VisibleType: visibleVARBIT}},

		// COLLATEDSTRING
		{MakeCollatedString(MakeVarChar(10), enLocale),
			InternalType{Family: CollatedStringFamily, Oid: oid.T_varchar, Width: 10, VisibleType: visibleVARCHAR, Locale: &enLocale}},

		// FLOAT
		{Float, InternalType{Family: FloatFamily, Oid: oid.T_float8, Width: 64}},
		{Float4, InternalType{Family: FloatFamily, Oid: oid.T_float4, Width: 32, VisibleType: visibleREAL}},

		// STRING
		{MakeString(10), InternalType{Family: StringFamily, Oid: oid.T_text, Width: 10}},
		{VarChar, InternalType{Family: StringFamily, Oid: oid.T_varchar, VisibleType: visibleVARCHAR}},
		{MakeChar(10), InternalType{Family: StringFamily, Oid: oid.T_bpchar, Width: 10, VisibleType: visibleCHAR}},
		{MakeQChar(1), InternalType{Family: StringFamily, Oid: oid.T_char, Width: 1, VisibleType: visibleQCHAR}},
		{Name, InternalType{Family: name, Oid: oid.T_name}},
	}

	for _, tc := range testCases {
		data, err := protoutil.Marshal(tc.from)
		if err != nil {
			t.Errorf("error during marshal of type <%v>: %v", tc.from.DebugString(), err)
		}

		var actual InternalType
		err = protoutil.Unmarshal(data, &actual)
		if err != nil {
			t.Errorf("error during unmarshal of type <%v>: %v", tc.from.DebugString(), err)
		}

		if !reflect.DeepEqual(actual, tc.to) {
			t.Errorf("expected <%v>, got <%v>", tc.to.String(), actual.String())
		}
	}
}

// TestMarshalCompat tests backwards-compatibility during unmarshal. Unmarshal
// needs to handle all formats ever used by CRDB in the past.
func TestUnmarshalCompat(t *testing.T) {
	intElemType := IntFamily
	floatElemType := FloatFamily

	testCases := []struct {
		from InternalType
		to   *T
	}{
		// ARRAY
		{InternalType{Family: ArrayFamily, ArrayElemType: &intElemType, VisibleType: visibleSMALLINT},
			MakeArray(Int2)},
		{InternalType{Family: ArrayFamily, ArrayElemType: &floatElemType, VisibleType: visibleDOUBLE},
			MakeArray(Float)},

		// BIT
		{InternalType{Family: BitFamily, VisibleType: visibleVARBIT}, VarBit},
		{InternalType{Family: BitFamily, VisibleType: visibleVARBIT, Width: 20}, MakeVarBit(20)},

		// FLOAT
		{InternalType{Family: FloatFamily}, Float},
		{InternalType{Family: FloatFamily, VisibleType: visibleREAL}, Float4},
		{InternalType{Family: FloatFamily, VisibleType: visibleDOUBLE}, Float},
		{InternalType{Family: FloatFamily, Precision: 1}, Float4},
		{InternalType{Family: FloatFamily, Precision: 24}, Float4},
		{InternalType{Family: FloatFamily, Precision: 25}, Float},
		{InternalType{Family: FloatFamily, Precision: 60}, Float},

		// INT
		{InternalType{Family: IntFamily, VisibleType: visibleSMALLINT}, Int2},
		{InternalType{Family: IntFamily, VisibleType: visibleINTEGER}, Int4},
		{InternalType{Family: IntFamily, VisibleType: visibleBIGINT}, Int},
		{InternalType{Family: IntFamily, VisibleType: visibleBIT}, Int},
		{InternalType{Family: IntFamily, Width: 20}, Int},
		{InternalType{Family: IntFamily}, Int},

		// STRING
		{InternalType{Family: StringFamily}, String},
		{InternalType{Family: StringFamily, VisibleType: visibleVARCHAR}, VarChar},
		{InternalType{Family: StringFamily, VisibleType: visibleVARCHAR, Width: 20}, MakeVarChar(20)},
		{InternalType{Family: StringFamily, VisibleType: visibleCHAR}, typeBpChar},
		{InternalType{Family: StringFamily, VisibleType: visibleQCHAR}, typeQChar},
	}

	for _, tc := range testCases {
		data, err := protoutil.Marshal(&tc.from)
		if err != nil {
			t.Errorf("error during marshal of type <%v>: %v", tc.from.String(), err)
		}

		var actual T
		err = protoutil.Unmarshal(data, &actual)
		if err != nil {
			t.Errorf("error during unmarshal of type <%v>: %v", tc.from.String(), err)
		}

		if !actual.Identical(tc.to) {
			t.Errorf("expected <%v>, got <%v>", tc.to.DebugString(), actual.DebugString())
		}
	}
}

func TestOids(t *testing.T) {
	for o, typ := range OidToType {
		if typ.Oid() != o {
			t.Errorf("expected OID %d, got %d", o, typ.Oid())
		}
	}

	for o := range ArrayOids {
		typ := OidToType[o]
		if typ.Family() != ArrayFamily {
			t.Errorf("expected ARRAY type, got %s", typ.Family())
		}
	}
}

func TestUpgradeType(t *testing.T) {
	testCases := []struct {
		desc     string
		input    *T
		expected *T
	}{
		{
			desc: "upgrading -1 timestamp precision to default precision for time",
			input: &T{InternalType: InternalType{
				Family:    TimestampFamily,
				Precision: -1,
				Oid:       oid.T_timestamp,
				Locale:    &emptyLocale,
			}},
			expected: &T{InternalType: InternalType{
				Family:    TimestampFamily,
				Precision: 0,
				Oid:       oid.T_timestamp,
				Locale:    &emptyLocale,
			}},
		},
		{
			desc: "upgrading default timestamp precision pre-20.1 to default precision",
			input: &T{InternalType: InternalType{
				Family:    TimestampFamily,
				Precision: 0,
				Oid:       oid.T_timestamp,
				Locale:    &emptyLocale,
			}},
			expected: &T{InternalType: InternalType{
				Family:             TimestampFamily,
				Precision:          0,
				TimePrecisionIsSet: false,
				Oid:                oid.T_timestamp,
				Locale:             &emptyLocale,
			}},
		},
		{
			desc: "upgrading 6 timestamp precision pre-20.1 to default precision",
			input: &T{InternalType: InternalType{
				Family:    TimestampFamily,
				Precision: 6,
				Oid:       oid.T_timestamp,
				Locale:    &emptyLocale,
			}},
			expected: &T{InternalType: InternalType{
				Family:             TimestampFamily,
				Precision:          6,
				TimePrecisionIsSet: true,
				Oid:                oid.T_timestamp,
				Locale:             &emptyLocale,
			}},
		},
		{
			desc: "idempotent for timestamp precision(3) set objects",
			input: &T{InternalType: InternalType{
				Family:             TimestampFamily,
				Precision:          3,
				TimePrecisionIsSet: true,
				Oid:                oid.T_timestamp,
				Locale:             &emptyLocale,
			}},
			expected: &T{InternalType: InternalType{
				Family:             TimestampFamily,
				Precision:          3,
				TimePrecisionIsSet: true,
				Oid:                oid.T_timestamp,
				Locale:             &emptyLocale,
			}},
		},
		{
			desc: "idempotent for timestamp precision(0) set objects",
			input: &T{InternalType: InternalType{
				Family:             TimestampFamily,
				Precision:          0,
				TimePrecisionIsSet: true,
				Oid:                oid.T_timestamp,
				Locale:             &emptyLocale,
			}},
			expected: &T{InternalType: InternalType{
				Family:             TimestampFamily,
				Precision:          0,
				TimePrecisionIsSet: true,
				Oid:                oid.T_timestamp,
				Locale:             &emptyLocale,
			}},
		},
		{
			desc: "idempotent for timestamp precision unset objects",
			input: &T{InternalType: InternalType{
				Family:             TimestampFamily,
				Precision:          0,
				TimePrecisionIsSet: false,
				Oid:                oid.T_timestamp,
				Locale:             &emptyLocale,
			}},
			expected: &T{InternalType: InternalType{
				Family:             TimestampFamily,
				Precision:          0,
				TimePrecisionIsSet: false,
				Oid:                oid.T_timestamp,
				Locale:             &emptyLocale,
			}},
		},
		{
			desc: "intervals upgrading from 19.2 to 20.1 with no precision set",
			input: &T{InternalType: InternalType{
				Family: IntervalFamily,
				Oid:    oid.T_interval,
				Locale: &emptyLocale,
			}},
			expected: &T{InternalType: InternalType{
				Family:                IntervalFamily,
				Precision:             0,
				TimePrecisionIsSet:    false,
				Oid:                   oid.T_interval,
				Locale:                &emptyLocale,
				IntervalDurationField: &IntervalDurationField{},
			}},
		},
		{
			desc: "intervals are idempotent after 20.1",
			input: &T{InternalType: InternalType{
				Family:             IntervalFamily,
				Oid:                oid.T_interval,
				Locale:             &emptyLocale,
				Precision:          4,
				TimePrecisionIsSet: true,
				IntervalDurationField: &IntervalDurationField{
					DurationType: IntervalDurationType_SECOND,
				},
			}},
			expected: &T{InternalType: InternalType{
				Family:             IntervalFamily,
				Oid:                oid.T_interval,
				Locale:             &emptyLocale,
				Precision:          4,
				TimePrecisionIsSet: true,
				IntervalDurationField: &IntervalDurationField{
					DurationType: IntervalDurationType_SECOND,
				},
			}},
		},
		{
			desc: "varbit types are not assigned the default family Oid value",
			input: &T{InternalType: InternalType{
				Family:      BitFamily,
				VisibleType: visibleVARBIT,
				Locale:      &emptyLocale,
			}},
			expected: &T{InternalType: InternalType{
				Family: BitFamily,
				Oid:    oid.T_varbit,
				Locale: &emptyLocale,
			}},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			err := tc.input.upgradeType()
			assert.NoError(t, err)
			assert.Equal(t, tc.expected, tc.input)
		})
	}
}

func TestOidSetDuringUpgrade(t *testing.T) {
	for family, Oid := range familyToOid {
		t.Run(fmt.Sprintf("family-%s", Family_name[int32(family)]), func(t *testing.T) {
			input := &T{InternalType: InternalType{
				Family: family,
			}}
			if family == ArrayFamily {
				// This is not material to this test, but needs to be set to avoid
				// panic.
				input.InternalType.ArrayContents = &T{InternalType: InternalType{
					Family: BoolFamily,
				}}
			}
			err := input.upgradeType()
			assert.NoError(t, err)
			assert.Equal(t, Oid, input.Oid())
		})
	}
}

func TestSQLStandardName(t *testing.T) {
	for _, typ := range Scalar {
		t.Run(typ.Name(), func(t *testing.T) {
			require.NotEmpty(t, typ.SQLStandardName())
		})
	}
}
