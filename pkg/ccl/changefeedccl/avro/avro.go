// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package avro

import (
	"encoding/json"
	"math/big"
	"time"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/geo/geopb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/bitarray"
	"github.com/cockroachdb/cockroach/pkg/util/duration"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeofday"
	"github.com/cockroachdb/errors"
	"github.com/linkedin/goavro/v2"
)

// The file contains a very specific marriage between avro and our SQL schemas.
// It's not intended to be a general purpose avro utility.
//
// Avro is a spec for data schemas, a binary format for encoding a record
// conforming to a given schema, and various container formats for those encoded
// records. It also has rules for determining backward and forward compatibility
// of schemas as they evolve.
//
// The Confluent ecosystem, Kafka plus other things, has first-class support for
// Avro, including a server for registering schemas and referencing which
// registered schema a Kafka record conforms to.
//
// We map a SQL table schema to an Avro record with 1:1 mapping between table
// columns and Avro fields. The type of the column is mapped to a native Avro
// type as faithfully as possible. This is then used to make an "optional" Avro
// field for that column by unioning with null and explicitly specifying null as
// default, regardless of whether the sql column allows NULLs. This may seem an
// odd choice, but it allows for all adjacent Avro schemas for a given SQL table
// to be backward and forward compatible with each other. Forward and backward
// compatibility drastically eases use of the resulting data by downstream
// systems, especially when working with long histories of archived data across
// many schema changes (such as a data lake).
//
// One downside of the above is that it's not possible to recover the original
// SQL table schema from an Avro one. (This is also true for other reasons, such
// as lossy mappings from sql types to avro types.) To partially address this,
// the SQL column type is embedded as metadata in the Avro field schema in a way
// that Avro ignores it but passes it along.

// SchemaType is one of the set of avro primitive types.
type SchemaType interface{}

const (
	SchemaTypeArray   = `array`
	SchemaTypeBoolean = `boolean`
	SchemaTypeBytes   = `bytes`
	SchemaTypeDouble  = `double`
	SchemaTypeInt     = `int`
	SchemaTypeLong    = `long`
	SchemaTypeNull    = `null`
	SchemaTypeString  = `string`
)

type logicalType struct {
	SchemaType  SchemaType `json:"type"`
	LogicalType string     `json:"logicalType"`
	Precision   *int       `json:"precision,omitempty"`
	Scale       *int       `json:"scale,omitempty"`
}

type arrayType struct {
	SchemaType SchemaType `json:"type"`
	Items      SchemaType `json:"items"`
}

func unionKey(t SchemaType) string {
	switch s := t.(type) {
	case string:
		return s
	case logicalType:
		return unionKey(s.SchemaType) + `.` + s.LogicalType
	case arrayType:
		return unionKey(s.SchemaType)
	case *Record:
		if s.Namespace == "" {
			return s.Name
		}
		return s.Namespace + `.` + s.Name
	default:
		panic(errors.AssertionFailedf(`unsupported type %T %v`, t, t))
	}
}

// memo is either nil or a previously-returned value
// that can be safely overwritten to save allocs.
type datumToNativeFn func(datum tree.Datum, memo interface{}) (interface{}, error)

// SchemaField is our representation of the schema of a field in an avro
// record. Serializing it to JSON gives the standard schema representation.
type SchemaField struct {
	SchemaType SchemaType `json:"type"`
	Name       string     `json:"name"`
	Default    *string    `json:"default"`
	Metadata   string     `json:"__crdb__,omitempty"`
	Namespace  string     `json:"namespace,omitempty"`

	typ *types.T

	// encodeFn encodes specified tree.Datum as go "native" interface value.
	// This function may memoize results to save allocations.
	encodeFn func(datum tree.Datum) (interface{}, error)

	// encodeDatum encodes specified datum as go "native" interface value.
	// encodeDatum is a low level encoding function -- it should not memoize
	// on its own, but may use the passed-in memo.
	encodeDatum datumToNativeFn

	// decodeFn decodes specified go "native" value into tree.Datum.
	decodeFn func(interface{}) (tree.Datum, error)

	// Avro encoder treats every field as optional -- that is, we always
	// allow null values.  As such, every value returned by encodeFn is an
	// avro record represented as a map with a single "union" key (see avroUnionKey())
	// and the value either null or the actual encoded value.
	// nativeEncoded is a map that's returned by encodeFn.  We allocate
	// this map once to avoid repeated map allocations.  We simply update
	// "union key" value. nativeEncodedSecondaryType supports unions of two types (plus null).
	nativeEncoded              map[string]interface{}
	nativeEncodedSecondaryType map[string]interface{}
}

// Record is our representation of the schema of an avro record. Serializing
// it to JSON gives the standard schema representation.
type Record struct {
	SchemaType string         `json:"type"`
	Name       string         `json:"name"`
	Fields     []*SchemaField `json:"fields"`
	Namespace  string         `json:"namespace,omitempty"`
	codec      *goavro.Codec
}

// Schema returns the schema of the record as a JSON string, suitable for
// passing to a schema registry.
func (r *Record) Schema() string {
	return r.codec.Schema()
}

// DataRecord is a `Record` that represents the schema of a SQL table
// or index.
type DataRecord struct {
	Record

	colIdxByFieldIdx map[int]int
	fieldIdxByName   map[string]int
	// Allocate Go native representation once, to avoid repeated map allocation
	// when encoding.
	native map[string]interface{}
}

type FunctionalRecord struct {
	// Use the Record just for schema info
	Record

	nativeFromRowFn func(row cdcevent.Row) (map[string]any, error)
}

func NewFunctionalRecord(
	name, namespace string,
	fields []*SchemaField,
	nativeFromRowFn func(row cdcevent.Row) (map[string]any, error),
) (*FunctionalRecord, error) {
	schema := &FunctionalRecord{
		Record: Record{
			Name:       name,
			SchemaType: `record`,
			Namespace:  namespace,
			Fields:     fields,
		},
		nativeFromRowFn: nativeFromRowFn,
	}

	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		return nil, err
	}
	schema.codec, err = goavro.NewCodec(string(schemaJSON))
	if err != nil {
		return nil, err
	}
	return schema, nil
}

// Metadata is the `EnvelopeRecord` metadata.
type Metadata map[string]interface{}

// EnvelopeOpts controls which fields in EnvelopeRecord are set.
type EnvelopeOpts struct {
	BeforeField, AfterField, RecordField bool
	UpdatedField, ResolvedField          bool
	MVCCTimestampField                   bool
	OpField, TsField, SourceField        bool
}

// EnvelopeRecord is an `avroRecord` that wraps a changed SQL row and some
// metadata.
type EnvelopeRecord struct {
	Record

	// NOTE: this struct gets serialized to json, but we still need to be able
	// to access these fields outside this package, so we hide them.
	Opts               EnvelopeOpts      `json:"-"`
	Before, After, Rec *DataRecord       `json:"-"`
	Source             *FunctionalRecord `json:"-"`
}

// typeToSchema converts a database type to an avro field
func typeToSchema(typ *types.T) (*SchemaField, error) {
	schema := &SchemaField{
		typ: typ,
	}

	// Make every field optional by unioning it with null, so that all schema
	// evolutions for a table are considered "backward compatible" by avro. This
	// means that the Avro type doesn't mirror the column's nullability, but it
	// makes it much easier to work with long histories of table data afterward,
	// especially for things like loading into analytics databases.
	setNullable := func(
		avroType SchemaType,
		encoder datumToNativeFn,
		decoder func(interface{}) (tree.Datum, error),
	) {
		// The default for a union type is the default for the first element of
		// the union.
		schema.SchemaType = []SchemaType{SchemaTypeNull, avroType}
		unionKey := unionKey(avroType)
		schema.nativeEncoded = map[string]interface{}{unionKey: nil}
		schema.encodeDatum = encoder

		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			if d == tree.DNull {
				return nil /* value */, nil
			}
			encoded, err := encoder(d, schema.nativeEncoded[unionKey])
			if err != nil {
				return nil, err
			}
			schema.nativeEncoded[unionKey] = encoded
			return schema.nativeEncoded, nil
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			if x == nil {
				return tree.DNull, nil
			}
			return decoder(x.(map[string]interface{})[unionKey])
		}
	}

	// Handles types that mostly encode to non-strings,
	// but have special cases like Infinity that encode as strings.
	setNullableWithStringFallback := func(
		avroType SchemaType,
		encoder datumToNativeFn,
		decoder func(interface{}) (tree.Datum, error),
	) {
		schema.SchemaType = []SchemaType{SchemaTypeNull, avroType, SchemaTypeString}
		mainUnionKey := unionKey(avroType)
		stringUnionKey := unionKey(SchemaTypeString)
		schema.nativeEncoded = map[string]interface{}{mainUnionKey: nil}
		schema.nativeEncodedSecondaryType = map[string]interface{}{stringUnionKey: nil}
		schema.encodeDatum = encoder

		schema.encodeFn = func(d tree.Datum) (interface{}, error) {
			if d == tree.DNull {
				return nil /* value */, nil
			}
			encoded, err := encoder(d, schema.nativeEncoded[mainUnionKey])
			if err != nil {
				return nil, err
			}
			_, isString := encoded.(string)
			if isString {
				schema.nativeEncodedSecondaryType[stringUnionKey] = encoded
				return schema.nativeEncodedSecondaryType, nil
			}
			schema.nativeEncoded[mainUnionKey] = encoded
			return schema.nativeEncoded, nil
		}
		schema.decodeFn = func(x interface{}) (tree.Datum, error) {
			if x == nil {
				return tree.DNull, nil
			}
			return decoder(x.(map[string]interface{}))
		}
	}

	switch typ.Family() {
	case types.IntFamily:
		setNullable(
			SchemaTypeLong,
			func(d tree.Datum, _ interface{}) (interface{}, error) {
				return int64(*d.(*tree.DInt)), nil
			},
			func(x interface{}) (tree.Datum, error) {
				return tree.NewDInt(tree.DInt(x.(int64))), nil
			},
		)
	case types.BoolFamily:
		setNullable(
			SchemaTypeBoolean,
			func(d tree.Datum, _ interface{}) (interface{}, error) {
				return bool(*d.(*tree.DBool)), nil
			},
			func(x interface{}) (tree.Datum, error) {
				return tree.MakeDBool(tree.DBool(x.(bool))), nil
			},
		)
	case types.BitFamily:
		setNullable(
			arrayType{
				SchemaType: SchemaTypeArray,
				Items:      SchemaTypeLong,
			},
			func(d tree.Datum, memo interface{}) (interface{}, error) {
				uints, lastBitsUsed := d.(*tree.DBitArray).EncodingParts()
				var signedLongs []interface{}
				// reuse a previously allocated array if it exists
				// and is long enough
				if memo != nil {
					signedLongs = memo.([]interface{})
					if len(signedLongs) > len(uints)+1 {
						signedLongs = signedLongs[:len(uints)+1]
					}
				}
				if signedLongs == nil {
					signedLongs = make([]interface{}, len(uints)+1)
				}
				signedLongs[0] = int64(lastBitsUsed)
				for idx, word := range uints {
					signedLongs[idx+1] = int64(word)
				}
				return signedLongs, nil
			},
			func(x interface{}) (tree.Datum, error) {
				arr := x.([]interface{})
				lastBitsUsed, ints := arr[0], arr[1:]
				uints := make([]uint64, len(ints))
				for idx, word := range ints {
					uints[idx] = uint64(word.(int64))
				}
				ba, err := bitarray.FromEncodingParts(uints, uint64(lastBitsUsed.(int64)))
				return &tree.DBitArray{BitArray: ba}, err
			},
		)
	case types.FloatFamily:
		setNullable(
			SchemaTypeDouble,
			func(d tree.Datum, _ interface{}) (interface{}, error) {
				return float64(*d.(*tree.DFloat)), nil
			},
			func(x interface{}) (tree.Datum, error) {
				return tree.NewDFloat(tree.DFloat(x.(float64))), nil
			},
		)
	case types.PGLSNFamily:
		setNullable(
			SchemaTypeString,
			func(d tree.Datum, _ interface{}) (interface{}, error) {
				return d.(*tree.DPGLSN).LSN.String(), nil
			},
			func(x interface{}) (tree.Datum, error) {
				return tree.ParseDPGLSN(x.(string))
			},
		)
	case types.RefCursorFamily:
		setNullable(
			SchemaTypeString,
			func(d tree.Datum, _ interface{}) (interface{}, error) {
				return string(tree.MustBeDString(d)), nil
			},
			func(x interface{}) (tree.Datum, error) {
				return tree.NewDRefCursor(x.(string)), nil
			},
		)
	case types.Box2DFamily:
		setNullable(
			SchemaTypeString,
			func(d tree.Datum, _ interface{}) (interface{}, error) {
				return d.(*tree.DBox2D).CartesianBoundingBox.Repr(), nil
			},
			func(x interface{}) (tree.Datum, error) {
				b, err := geo.ParseCartesianBoundingBox(x.(string))
				if err != nil {
					return nil, err
				}
				return tree.NewDBox2D(b), nil
			},
		)
	case types.GeographyFamily:
		setNullable(
			SchemaTypeBytes,
			func(d tree.Datum, _ interface{}) (interface{}, error) {
				return []byte(d.(*tree.DGeography).EWKB()), nil
			},
			func(x interface{}) (tree.Datum, error) {
				g, err := geo.ParseGeographyFromEWKBUnsafe(geopb.EWKB(x.([]byte)))
				if err != nil {
					return nil, err
				}
				return &tree.DGeography{Geography: g}, nil
			},
		)
	case types.GeometryFamily:
		setNullable(
			SchemaTypeBytes,
			func(d tree.Datum, _ interface{}) (interface{}, error) {
				return []byte(d.(*tree.DGeometry).EWKB()), nil
			},
			func(x interface{}) (tree.Datum, error) {
				g, err := geo.ParseGeometryFromEWKBUnsafe(geopb.EWKB(x.([]byte)))
				if err != nil {
					return nil, err
				}
				return &tree.DGeometry{Geometry: g}, nil
			},
		)
	case types.StringFamily:
		setNullable(
			SchemaTypeString,
			func(d tree.Datum, _ interface{}) (interface{}, error) {
				return string(*d.(*tree.DString)), nil
			},
			func(x interface{}) (tree.Datum, error) {
				return tree.NewDString(x.(string)), nil
			},
		)
	case types.CollatedStringFamily:
		setNullable(
			SchemaTypeString,
			func(d tree.Datum, _ interface{}) (interface{}, error) {
				return d.(*tree.DCollatedString).Contents, nil
			},
			func(x interface{}) (tree.Datum, error) {
				return tree.NewDCollatedString(x.(string), typ.Locale(), &tree.CollationEnvironment{})
			},
		)
	case types.BytesFamily:
		setNullable(
			SchemaTypeBytes,
			func(d tree.Datum, _ interface{}) (interface{}, error) {
				return []byte(*d.(*tree.DBytes)), nil
			},
			func(x interface{}) (tree.Datum, error) {
				return tree.NewDBytes(tree.DBytes(x.([]byte))), nil
			},
		)
	case types.DateFamily:
		setNullable(
			logicalType{
				SchemaType:  SchemaTypeInt,
				LogicalType: `date`,
			},
			func(d tree.Datum, _ interface{}) (interface{}, error) {
				date := *d.(*tree.DDate)
				if !date.IsFinite() {
					return nil, changefeedbase.WithTerminalError(errors.Errorf(
						`infinite date not yet supported with avro`))
				}
				// The avro library requires us to return this as a time.Time.
				return date.ToTime()
			},
			func(x interface{}) (tree.Datum, error) {
				// The avro library hands this back as a time.Time.
				return tree.NewDDateFromTime(x.(time.Time))
			},
		)
	case types.TimeFamily:
		setNullable(
			logicalType{
				SchemaType:  SchemaTypeLong,
				LogicalType: `time-micros`,
			},
			func(d tree.Datum, _ interface{}) (interface{}, error) {
				// Time of day is stored in microseconds since midnight,
				// which is also the avro format
				dt := d.(*tree.DTime)
				return int64(*dt), nil
			},
			func(x interface{}) (tree.Datum, error) {
				// The avro library hands this back as a time.Duration.
				micros := x.(time.Duration) / time.Microsecond
				return tree.MakeDTime(timeofday.TimeOfDay(micros)), nil
			},
		)
	case types.TimeTZFamily:
		setNullable(
			SchemaTypeString,
			// We cannot encode this as a long, as it does not encode
			// timezone correctly.
			func(d tree.Datum, _ interface{}) (interface{}, error) {
				return d.(*tree.DTimeTZ).TimeTZ.String(), nil
			},
			func(x interface{}) (tree.Datum, error) {
				d, _, err := tree.ParseDTimeTZ(nil, x.(string), time.Microsecond)
				return d, err
			},
		)
	case types.TimestampFamily:
		setNullable(
			logicalType{
				SchemaType:  SchemaTypeLong,
				LogicalType: `timestamp-micros`,
			},
			func(d tree.Datum, _ interface{}) (interface{}, error) {
				return d.(*tree.DTimestamp).Time, nil
			},
			func(x interface{}) (tree.Datum, error) {
				return tree.MakeDTimestamp(x.(time.Time), time.Microsecond)
			},
		)
	case types.TimestampTZFamily:
		setNullable(
			logicalType{
				SchemaType:  SchemaTypeLong,
				LogicalType: `timestamp-micros`,
			},
			func(d tree.Datum, _ interface{}) (interface{}, error) {
				return d.(*tree.DTimestampTZ).Time, nil
			},
			func(x interface{}) (tree.Datum, error) {
				return tree.MakeDTimestampTZ(x.(time.Time), time.Microsecond)
			},
		)
	case types.IntervalFamily:
		setNullable(
			// This would ideally be the avro Duration logical type
			// However, the spec is not implemented in most tooling
			// and is problematic--it requires 32-bit integers
			// representing months, days, and milliseconds, meaning
			// it can't encode everything we can with our int64 years.
			// String encoding is still fairly terse and arguably the
			// only semantically exact representation.
			// Using ISO 8601 format (https://en.wikipedia.org/wiki/ISO_8601#Durations)
			// because it's the tersest of the input formats we support
			// and isn't golang-specific.
			SchemaTypeString,
			func(d tree.Datum, _ interface{}) (interface{}, error) {
				return d.(*tree.DInterval).ValueAsISO8601String(), nil
			},
			func(x interface{}) (tree.Datum, error) {
				return tree.ParseDInterval(duration.IntervalStyle_ISO_8601, x.(string))
			},
		)
	case types.DecimalFamily:
		if typ.Precision() == 0 {
			return nil, changefeedbase.WithTerminalError(errors.Errorf(
				`decimal with no precision not yet supported with avro`))
		}

		width := int(typ.Width())
		prec := int(typ.Precision())
		decimalType := logicalType{
			SchemaType:  SchemaTypeBytes,
			LogicalType: `decimal`,
			Precision:   &prec,
			Scale:       &width,
		}
		setNullableWithStringFallback(
			decimalType,
			func(d tree.Datum, _ interface{}) (interface{}, error) {
				dec := d.(*tree.DDecimal).Decimal

				if dec.Form != apd.Finite {
					return d.String(), nil
				}

				// If the decimal happens to fit a smaller width than the
				// column allows, add trailing zeroes so the scale is constant
				if typ.Width() > -dec.Exponent {
					_, err := tree.DecimalCtx.WithPrecision(uint32(prec)).Quantize(&dec, &dec, -int32(width))
					if err != nil {
						// This should always be possible without rounding since we're using the column def,
						// but if it's not, WithPrecision will force it to error.
						return nil, err
					}
				}

				// TODO(dan): For the cases that the avro defined decimal format
				// would not roundtrip, serialize the decimal as a string. Also
				// support the unspecified precision/scale case in this branch. We
				// can't currently do this without surgery to the avro library we're
				// using and that's too scary leading up to 2.1.0.
				rat, err := decimalToRat(dec, int32(width))
				if err != nil {
					return nil, err
				}
				return &rat, nil
			},
			func(x interface{}) (tree.Datum, error) {
				unionMap := x.(map[string]interface{})
				rat, ok := unionMap[unionKey(decimalType)]
				if ok {
					return &tree.DDecimal{Decimal: ratToDecimal(*rat.(*big.Rat), int32(width))}, nil
				}
				return tree.ParseDDecimal(unionMap[unionKey(SchemaTypeString)].(string))
			},
		)
	case types.UuidFamily:
		// Should be logical type of "uuid", but the avro library doesn't support
		// that yet.
		setNullable(
			SchemaTypeString,
			func(d tree.Datum, _ interface{}) (interface{}, error) {
				return d.(*tree.DUuid).UUID.String(), nil
			},
			func(x interface{}) (tree.Datum, error) {
				return tree.ParseDUuidFromString(x.(string))
			},
		)
	case types.INetFamily:
		setNullable(
			SchemaTypeString,
			func(d tree.Datum, _ interface{}) (interface{}, error) {
				return d.(*tree.DIPAddr).IPAddr.String(), nil
			},
			func(x interface{}) (tree.Datum, error) {
				return tree.ParseDIPAddrFromINetString(x.(string))
			},
		)
	case types.JsonFamily:
		setNullable(
			SchemaTypeString,
			func(d tree.Datum, _ interface{}) (interface{}, error) {
				return d.(*tree.DJSON).JSON.String(), nil
			},
			func(x interface{}) (tree.Datum, error) {
				return tree.ParseDJSON(x.(string))
			},
		)
	// case types.JsonpathFamily:
	// We do not support Jsonpath yet, but should be able to support it easily.
	case types.TSQueryFamily:
		setNullable(
			SchemaTypeString,
			func(d tree.Datum, _ interface{}) (interface{}, error) {
				return d.(*tree.DTSQuery).TSQuery.String(), nil
			},
			func(x interface{}) (tree.Datum, error) {
				return tree.ParseDTSQuery(x.(string))
			},
		)
	case types.TSVectorFamily:
		setNullable(
			SchemaTypeString,
			func(d tree.Datum, _ interface{}) (interface{}, error) {
				return d.(*tree.DTSVector).TSVector.String(), nil
			},
			func(x interface{}) (tree.Datum, error) {
				return tree.ParseDTSVector(x.(string))
			},
		)
	// case types.PGVectorFamily:
	//
	// We could have easily supported PGVector type via stringification, but it
	// would probably be quite inefficient; thus, in order to not back ourselves
	// into a corner with compatibility, we choose to return an error instead
	// for now.
	case types.EnumFamily:
		setNullable(
			SchemaTypeString,
			func(d tree.Datum, _ interface{}) (interface{}, error) {
				return d.(*tree.DEnum).LogicalRep, nil
			},
			func(x interface{}) (tree.Datum, error) {
				e, err := tree.MakeDEnumFromLogicalRepresentation(typ, x.(string))
				if err != nil {
					return nil, err
				}
				return tree.NewDEnum(e), nil
			},
		)
	case types.ArrayFamily:
		itemSchema, err := typeToSchema(typ.ArrayContents())
		if err != nil {
			return nil, changefeedbase.WithTerminalError(
				errors.Wrapf(err, `could not create item schema for %s`, typ))
		}
		itemUnionKey := unionKey(itemSchema.SchemaType.([]SchemaType)[1])

		setNullable(
			arrayType{
				SchemaType: SchemaTypeArray,
				Items:      itemSchema.SchemaType,
			},
			func(d tree.Datum, memo interface{}) (interface{}, error) {
				datumArr := d.(*tree.DArray)

				var avroArr []interface{}
				if memo != nil {
					avroArr = memo.([]interface{})
					if len(avroArr) > datumArr.Len() {
						avroArr = avroArr[:datumArr.Len()]
					}
				} else {
					avroArr = make([]interface{}, 0, datumArr.Len())
				}
				for i, elt := range datumArr.Array {
					var encoded interface{}
					if elt == tree.DNull {
						encoded = nil
					} else {
						var encErr error
						if i < len(avroArr) && avroArr[i] != nil {
							encoded, encErr = itemSchema.encodeDatum(elt, avroArr[i].(map[string]interface{})[itemUnionKey])
						} else {
							encoded, encErr = itemSchema.encodeDatum(elt, nil)
						}
						if encErr != nil {
							return nil, encErr
						}
					}

					if i < len(avroArr) {
						// We have previously memoized array value.
						if encoded == nil {
							avroArr[i] = encoded
						} else if itemMap, ok := avroArr[i].(map[string]interface{}); ok {
							// encoded is not nil and previous value wasn't nil either.
							itemMap[itemUnionKey] = encoded
						} else {
							// encoded is not nil, but previous value was.
							encMap := make(map[string]interface{})
							encMap[itemUnionKey] = encoded
							avroArr[i] = encMap
						}
					} else {
						if encoded == nil {
							avroArr = append(avroArr, encoded)
						} else {
							encMap := make(map[string]interface{})
							encMap[itemUnionKey] = encoded
							avroArr = append(avroArr, encMap)
						}
					}
				}
				return avroArr, nil
			},
			func(x interface{}) (tree.Datum, error) {
				datumArr := tree.NewDArray(itemSchema.typ)
				avroArr := x.([]interface{})
				for _, item := range avroArr {
					itemDatum, err := itemSchema.decodeFn(item)
					if err != nil {
						return nil, err
					}
					err = datumArr.Append(itemDatum)
					if err != nil {
						return nil, err
					}
				}
				return datumArr, nil
			},
		)

	default:
		return nil, changefeedbase.WithTerminalError(
			errors.Errorf(`type %s not yet supported with avro`, typ.SQLString()))
	}

	return schema, nil
}

// columnToAvroSchema converts a column descriptor into its corresponding
// avro field schema.
func columnToAvroSchema(col cdcevent.ResultColumn) (*SchemaField, error) {
	schema, err := typeToSchema(col.Typ)
	if err != nil {
		return nil, changefeedbase.WithTerminalError(errors.Wrapf(err, "column %s", col.Name))
	}
	schema.Name = changefeedbase.SQLNameToAvroName(col.Name)
	schema.Metadata = col.SQLStringNotHumanReadable()
	schema.Default = nil

	return schema, nil
}

// NewSchemaForRow constructs avro schema for the Row.
// Only columns returned by Iterator as used to popoulate schema fields.
// sqlName can be any string but should uniquely identify a schema.
func NewSchemaForRow(it cdcevent.Iterator, sqlName string, namespace string) (*DataRecord, error) {
	schema := &DataRecord{
		Record: Record{
			Name:       sqlName,
			SchemaType: `record`,
			Namespace:  namespace,
		},
		fieldIdxByName:   make(map[string]int),
		colIdxByFieldIdx: make(map[int]int),
	}

	if err := it.Col(func(col cdcevent.ResultColumn) error {
		field, err := columnToAvroSchema(col)
		if err != nil {
			return err
		}
		// Set column index -> ColumnID mapping.  We set PGAttributeNum to be the
		// column ordinal position.  This information is only used to test that avro
		// encoding is round trip-able.
		schema.colIdxByFieldIdx[len(schema.Fields)] = col.Ordinal()
		schema.fieldIdxByName[field.Name] = len(schema.Fields)
		schema.Fields = append(schema.Fields, field)
		return nil
	}); err != nil {
		return nil, err
	}

	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		return nil, err
	}
	schema.codec, err = goavro.NewCodec(string(schemaJSON))
	if err != nil {
		return nil, err
	}
	return schema, nil
}

// PrimaryIndexToAvroSchema constructs schema for primary index.
func PrimaryIndexToAvroSchema(
	row cdcevent.Row, sqlName string, namespace string,
) (*DataRecord, error) {
	return NewSchemaForRow(row.ForEachKeyColumn(), changefeedbase.SQLNameToAvroName(sqlName), namespace)
}

const (
	// SchemaNoSuffix can be passed to TableToAvroSchema to indicate that
	// no suffix should be appended to the avro record's name.
	SchemaNoSuffix = ``
)

// TableToAvroSchema constructs avro schema for the event values.
// If a name suffix is provided (as opposed to avroSchemaNoSuffix), it will be
// appended to the end of the avro record's name.
func TableToAvroSchema(row cdcevent.Row, nameSuffix string, namespace string) (*DataRecord, error) {
	var sqlName string
	// Even though we now always specify a family,
	// for backwards compatibility schemas for tables with only one family
	// don't get family-specific names.
	if row.HasOtherFamilies {
		sqlName = changefeedbase.SQLNameToAvroName(row.TableName + "." + row.FamilyName)
	} else {
		sqlName = changefeedbase.SQLNameToAvroName(row.TableName)
	}
	if nameSuffix != SchemaNoSuffix {
		sqlName = sqlName + `_` + nameSuffix
	}
	return NewSchemaForRow(row.ForEachColumn(), sqlName, namespace)
}

// BinaryFromRow encodes the given row data into avro's defined binary format.
func (r *DataRecord) BinaryFromRow(buf []byte, it cdcevent.Iterator) ([]byte, error) {
	native, err := r.nativeFromRow(it)
	if err != nil {
		return nil, err
	}
	return r.codec.BinaryFromNative(buf, native)
}

// rowFromTextual decodes the given row data from avro's defined JSON format.
func (r *DataRecord) rowFromTextual(buf []byte) (rowenc.EncDatumRow, error) {
	native, newBuf, err := r.codec.NativeFromTextual(buf)
	if err != nil {
		return nil, err
	}
	if len(newBuf) > 0 {
		return nil, changefeedbase.WithTerminalError(errors.New(`only one row was expected`))
	}
	return r.rowFromNative(native)
}

// textualFromRow encodes the given row data into avro's defined JSON format.
func (r *DataRecord) textualFromRow(row cdcevent.Row) ([]byte, error) {
	native, err := r.nativeFromRow(row.ForEachColumn())
	if err != nil {
		return nil, err
	}
	return r.codec.TextualFromNative(nil /* buf */, native)
}

// RowFromBinary decodes the given row data from avro's defined binary format.
func (r *DataRecord) RowFromBinary(buf []byte) (rowenc.EncDatumRow, error) {
	native, newBuf, err := r.codec.NativeFromBinary(buf)
	if err != nil {
		return nil, err
	}
	if len(newBuf) > 0 {
		return nil, changefeedbase.WithTerminalError(errors.New(`only one row was expected`))
	}
	return r.rowFromNative(native)
}

func (r *DataRecord) nativeFromRow(it cdcevent.Iterator) (interface{}, error) {
	if r.native == nil {
		// Note that it's safe to reuse r.native without clearing it because all records will
		// contain the same complete set of fields.
		r.native = make(map[string]interface{}, len(r.Fields))
	}

	if err := it.Datum(func(d tree.Datum, col cdcevent.ResultColumn) (err error) {
		fieldIdx, ok := r.fieldIdxByName[col.Name]
		if !ok {
			return changefeedbase.WithTerminalError(
				errors.AssertionFailedf("could not find avro field for column %s", col.Name))
		}
		r.native[col.Name], err = r.Fields[fieldIdx].encodeFn(d)
		return err
	}); err != nil {
		return nil, err
	}

	return r.native, nil
}

func (r *DataRecord) rowFromNative(native interface{}) (rowenc.EncDatumRow, error) {
	avroDatums, ok := native.(map[string]interface{})
	if !ok {
		return nil, changefeedbase.WithTerminalError(
			errors.Errorf(`unknown avro native type: %T`, native))
	}
	if len(r.Fields) != len(avroDatums) {
		return nil, changefeedbase.WithTerminalError(errors.Errorf(
			`expected row with %d columns got %d`, len(r.Fields), len(avroDatums)))
	}

	row := make(rowenc.EncDatumRow, len(r.Fields))
	for fieldName, avroDatum := range avroDatums {
		fieldIdx := r.fieldIdxByName[fieldName]
		field := r.Fields[fieldIdx]
		decoded, err := field.decodeFn(avroDatum)
		if err != nil {
			return nil, err
		}
		row[r.colIdxByFieldIdx[fieldIdx]] = rowenc.DatumToEncDatum(field.typ, decoded)
	}
	return row, nil
}

// NewEnvelopeRecord creates an avro record schema for an envelope containing
// before and after versions of a row change and metadata about that row change.
// before is optional, and after can instead be record.
func NewEnvelopeRecord(
	topic string,
	opts EnvelopeOpts,
	before, after, record *DataRecord,
	source *FunctionalRecord,
	namespace string,
) (*EnvelopeRecord, error) {
	schema := &EnvelopeRecord{
		Record: Record{
			Name:       changefeedbase.SQLNameToAvroName(topic) + `_envelope`,
			SchemaType: `record`,
			Namespace:  namespace,
		},
		Opts: opts,
	}

	if opts.BeforeField {
		schema.Before = before
		beforeField := &SchemaField{
			Name:       `before`,
			SchemaType: []SchemaType{SchemaTypeNull, before},
			Default:    nil,
		}
		schema.Fields = append(schema.Fields, beforeField)
	}
	if opts.AfterField {
		schema.After = after
		afterField := &SchemaField{
			Name:       `after`,
			SchemaType: []SchemaType{SchemaTypeNull, after},
			Default:    nil,
		}
		schema.Fields = append(schema.Fields, afterField)
	}
	if opts.UpdatedField {
		updatedField := &SchemaField{
			SchemaType: []SchemaType{SchemaTypeNull, SchemaTypeString},
			Name:       `updated`,
			Default:    nil,
		}
		schema.Fields = append(schema.Fields, updatedField)
	}
	if opts.MVCCTimestampField {
		mvccTimestampField := &SchemaField{
			SchemaType: []SchemaType{SchemaTypeNull, SchemaTypeString},
			Name:       `mvcc_timestamp`,
			Default:    nil,
		}
		schema.Fields = append(schema.Fields, mvccTimestampField)
	}
	if opts.ResolvedField {
		resolvedField := &SchemaField{
			SchemaType: []SchemaType{SchemaTypeNull, SchemaTypeString},
			Name:       `resolved`,
			Default:    nil,
		}
		schema.Fields = append(schema.Fields, resolvedField)
	}
	if opts.RecordField {
		schema.Rec = record
		recordField := &SchemaField{
			Name:       `record`,
			SchemaType: []SchemaType{SchemaTypeNull, record},
			Default:    nil,
		}
		schema.Fields = append(schema.Fields, recordField)
	}

	if opts.SourceField {
		schema.Source = source
		sourceField := &SchemaField{
			Name:       `source`,
			SchemaType: []SchemaType{SchemaTypeNull, source},
			Default:    nil,
		}
		schema.Fields = append(schema.Fields, sourceField)
	}
	if opts.TsField {
		tsNsField := &SchemaField{
			Name:       `ts_ns`,
			SchemaType: []SchemaType{SchemaTypeNull, SchemaTypeLong},
			Default:    nil,
		}
		schema.Fields = append(schema.Fields, tsNsField)
	}
	if opts.OpField {
		opField := &SchemaField{
			Name:       `op`,
			SchemaType: []SchemaType{SchemaTypeNull, SchemaTypeString},
			Default:    nil,
		}
		schema.Fields = append(schema.Fields, opField)
	}

	schemaJSON, err := json.Marshal(schema)
	if err != nil {
		return nil, err
	}
	schema.codec, err = goavro.NewCodec(string(schemaJSON))
	if err != nil {
		return nil, err
	}
	return schema, nil
}

// BinaryFromRow encodes the given metadata and row data into avro's defined
// binary format.
func (r *EnvelopeRecord) BinaryFromRow(
	buf []byte, meta Metadata, beforeRow, afterRow, recordRow cdcevent.Row,
) ([]byte, error) {
	native := map[string]interface{}{}
	if r.Opts.BeforeField {
		if beforeRow.HasValues() && !beforeRow.IsDeleted() {
			beforeNative, err := r.Before.nativeFromRow(beforeRow.ForEachColumn())
			if err != nil {
				return nil, err
			}
			native[`before`] = goavro.Union(unionKey(&r.Before.Record), beforeNative)
		} else {
			native[`before`] = nil
		}
	}

	if r.Opts.AfterField {
		if afterRow.HasValues() && !afterRow.IsDeleted() {
			afterNative, err := r.After.nativeFromRow(afterRow.ForEachColumn())
			if err != nil {
				return nil, err
			}
			native[`after`] = goavro.Union(unionKey(&r.After.Record), afterNative)
		} else {
			native[`after`] = nil
		}
	}

	if r.Opts.RecordField {
		if recordRow.HasValues() {
			recordNative, err := r.Rec.nativeFromRow(recordRow.ForEachColumn())
			if err != nil {
				return nil, err
			}
			native[`record`] = goavro.Union(unionKey(&r.Rec.Record), recordNative)
		} else {
			native[`record`] = nil
		}
	}

	if r.Opts.UpdatedField {
		native[`updated`] = nil
		if u, ok := meta[`updated`]; ok {
			delete(meta, `updated`)
			ts, ok := u.(hlc.Timestamp)
			if !ok {
				return nil, changefeedbase.WithTerminalError(
					errors.Errorf(`unknown metadata timestamp type: %T`, u))
			}
			native[`updated`] = goavro.Union(unionKey(SchemaTypeString), ts.AsOfSystemTime())
		}
	}

	if r.Opts.MVCCTimestampField {
		native[`mvcc_timestamp`] = nil
		if u, ok := meta[`mvcc_timestamp`]; ok {
			delete(meta, `mvcc_timestamp`)
			ts, ok := u.(hlc.Timestamp)
			if !ok {
				return nil, changefeedbase.WithTerminalError(
					errors.Errorf(`unknown metadata timestamp type: %T`, u))
			}
			native[`mvcc_timestamp`] = goavro.Union(unionKey(SchemaTypeString), ts.AsOfSystemTime())
		}
	}

	if r.Opts.ResolvedField {
		native[`resolved`] = nil
		if u, ok := meta[`resolved`]; ok {
			delete(meta, `resolved`)
			ts, ok := u.(hlc.Timestamp)
			if !ok {
				return nil, changefeedbase.WithTerminalError(
					errors.Errorf(`unknown metadata timestamp type: %T`, u))
			}
			native[`resolved`] = goavro.Union(unionKey(SchemaTypeString), ts.AsOfSystemTime())
		}
	}

	if r.Opts.SourceField {
		sourceNative, err := r.Source.nativeFromRowFn(recordRow)
		if err != nil {
			return nil, err
		}
		native[`source`] = goavro.Union(unionKey(&r.Source.Record), sourceNative)
	}
	if r.Opts.TsField {
		native[`ts_ns`] = nil
		if u, ok := meta[`ts_ns`]; ok {
			delete(meta, `ts_ns`)
			ts, ok := u.(int64)
			if !ok {
				return nil, changefeedbase.WithTerminalError(
					errors.Errorf(`unknown metadata timestamp type: %T`, u))
			}
			native[`ts_ns`] = goavro.Union(unionKey(SchemaTypeLong), ts)
		}
	}
	if r.Opts.OpField {
		native[`op`] = nil
		if u, ok := meta[`op`]; ok {
			delete(meta, `op`)
			op, ok := u.(string)
			if !ok {
				return nil, changefeedbase.WithTerminalError(
					errors.Errorf(`unknown metadata operation type: %T`, u))
			}
			native[`op`] = goavro.Union(unionKey(SchemaTypeString), op)
		}
	}

	for k := range meta {
		return nil, changefeedbase.WithTerminalError(errors.AssertionFailedf(`unhandled meta key: %s`, k))
	}
	buf, err := r.codec.BinaryFromNative(buf, native)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// Refresh the metadata for user-defined types on a cached schema
// The only user-defined type is enum, so this is usually a no-op.
func (r *DataRecord) RefreshTypeMetadata(row cdcevent.Row) error {
	return row.ForEachUDTColumn().Col(func(col cdcevent.ResultColumn) error {
		if fieldIdx, ok := r.fieldIdxByName[col.Name]; ok {
			r.Fields[fieldIdx].typ = col.Typ
		}
		return nil
	})
}

// decimalToRat converts one of our apd decimals to the format expected by the
// avro library we use. If the column has a fixed scale (which is always true if
// precision is set) this is roundtripable without information loss.
func decimalToRat(dec apd.Decimal, scale int32) (big.Rat, error) {
	if dec.Form != apd.Finite {
		return big.Rat{}, changefeedbase.WithTerminalError(
			errors.Errorf(`cannot convert %s form decimal`, dec.Form))
	}
	if scale > 0 && scale != -dec.Exponent {
		return big.Rat{}, changefeedbase.WithTerminalError(
			errors.Errorf(`%s will not roundtrip at scale %d`, &dec, scale))
	}
	var r big.Rat
	if dec.Exponent >= 0 {
		exp := big.NewInt(10)
		exp = exp.Exp(exp, big.NewInt(int64(dec.Exponent)), nil)
		coeff := dec.Coeff.MathBigInt()
		r.SetFrac(coeff.Mul(coeff, exp), big.NewInt(1))
	} else {
		exp := big.NewInt(10)
		exp = exp.Exp(exp, big.NewInt(int64(-dec.Exponent)), nil)
		coeff := dec.Coeff.MathBigInt()
		r.SetFrac(coeff, exp)
	}
	if dec.Negative {
		r.Mul(&r, big.NewRat(-1, 1))
	}
	return r, nil
}

// ratToDecimal converts the output of decimalToRat back into the original apd
// decimal, given a fixed column scale. NB: big.Rat is lossy-compared to apd
// decimal, so this is not possible when the scale is not fixed.
func ratToDecimal(rat big.Rat, scale int32) apd.Decimal {
	num, denom := rat.Num(), rat.Denom()
	exp := big.NewInt(10)
	exp = exp.Exp(exp, big.NewInt(int64(scale)), nil)
	sf := denom.Div(exp, denom)
	var coeff apd.BigInt
	coeff.SetMathBigInt(num.Mul(num, sf))
	dec := apd.NewWithBigInt(&coeff, -scale)
	return *dec
}
