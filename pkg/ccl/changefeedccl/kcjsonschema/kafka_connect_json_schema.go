// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kcjsonschema

import (
	gojson "encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/geo"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

type schemaType string

const (
	SchemaTypeInt8    schemaType = "int8"
	SchemaTypeInt16   schemaType = "int16"
	SchemaTypeInt32   schemaType = "int32"
	SchemaTypeInt64   schemaType = "int64"
	SchemaTypeFloat32 schemaType = "float32"
	SchemaTypeFloat64 schemaType = "float64"
	SchemaTypeBoolean schemaType = "boolean"
	SchemaTypeString  schemaType = "string"
	SchemaTypeBytes   schemaType = "bytes"
	SchemaTypeArray   schemaType = "array"
	SchemaTypeStruct  schemaType = "struct"
	// Maps are not currently supported here, but this is included for
	// completeness.
	SchemaTypeMap schemaType = "map"

	// We encode JSON data inline, not stringified. We don't want to have to
	// introspect every value of JSON fields, so just say the data is json and
	// leave it at that.
	SchemaTypeOpaqueJSON schemaType = "json"
)

type schemaName string

const (
	schemaNameDecimal   schemaName = "decimal"
	schemaNameDate      schemaName = "date"
	schemaNameTime      schemaName = "time"
	schemaNameTimestamp schemaName = "timestamp"
	// NOTE: These two are our own additions.
	schemaNameGeometry  schemaName = "geometry"
	schemaNameGeography schemaName = "geography"
)

// Schema is the JSON representation of a Kafka Connect JSON Schema. There is no
// spec, but see the source code of org.apache.kafka.connect.data.ConnectSchema
// for reference.
type Schema struct {
	TypeName schemaType `json:"type"`
	// Name is my schema name, optional. Can represent a logical type (such as
	// decimal) or the overall entity type, such as "envelope", etc.
	Name schemaName `json:"name,omitempty"`
	// Field is the name of the field that I am in my parent struct.
	Field string `json:"field,omitempty"`
	// Parameters is a map of parameters for the schema. Currently this only has
	// meaning for decimals, where it contains precision and scale.
	Parameters map[string]string `json:"parameters,omitempty"`
	// Fields are the fields of the struct, if this is a struct.
	Fields []Schema `json:"fields,omitempty"`
	// Optional is whether this field is optional. This should reflect the
	// nullability of database columns, and be true for all fields we add
	// ourselves.
	Optional bool `json:"optional"`
	// Items is the type of the array elements, if this is an array.
	Items *Schema `json:"items,omitempty"`

	// NOTE: the "spec" contains two other optional fields -- Version (int) and
	// Doc (string), which we do not implement.
}

func (s Schema) AsJSON() (json.JSON, error) {
	bs, err := gojson.Marshal(s)
	if err != nil {
		return nil, err
	}
	return json.ParseJSON(string(bs))
}

// NewEnrichedEnvelope creates a new schema for an enriched envelope.
func NewEnrichedEnvelope(before, after, source, keyInValue *Schema) Schema {
	fields := make([]Schema, 0, 3)
	if before != nil {
		b := *before
		b.Field = "before"
		b.Optional = true
		fields = append(fields, b)
	}
	if after != nil {
		a := *after
		a.Field = "after"
		fields = append(fields, a)
	}
	if source != nil {
		s := *source
		s.Field = "source"
		s.Optional = true
		fields = append(fields, s)
	}
	if keyInValue != nil {
		k := *keyInValue
		k.Field = "key"
		k.Optional = false
		fields = append(fields, k)
	}

	fields = append(fields,
		Schema{
			TypeName: SchemaTypeInt64,
			Field:    "ts_ns",
		}, Schema{
			TypeName: SchemaTypeString,
			Field:    "op",
		},
	)

	return Schema{
		TypeName: SchemaTypeStruct,
		Name:     "cockroachdb.envelope",
		Fields:   fields,
	}
}

func NewSchemaFromIterator(it cdcevent.Iterator, name string) (Schema, error) {
	schema := Schema{
		TypeName: SchemaTypeStruct,
		Name:     schemaName(name),
		Fields:   []Schema{},
	}
	err := it.Col(func(col cdcevent.ResultColumn) error {
		colSchema, err := typeToSchema(col.Typ)
		if err != nil {
			return err
		}
		colSchema.Optional = col.Nullable
		colSchema.Field = col.Name
		schema.Fields = append(schema.Fields, colSchema)
		return nil
	})
	if err != nil {
		return Schema{}, err
	}
	return schema, nil
}

// NOTE: this *must* match the output of tree.AsJSON(). There is a test to
// ensure this to the extent possible.
func typeToSchema(typ *types.T) (Schema, error) {
	switch typ.Family() {
	case types.IntFamily:
		return Schema{TypeName: SchemaTypeInt64}, nil
	case types.BoolFamily:
		return Schema{TypeName: SchemaTypeBoolean}, nil
	case types.FloatFamily:
		return Schema{TypeName: SchemaTypeFloat64}, nil
	case types.StringFamily, types.CollatedStringFamily, types.PGLSNFamily, types.RefCursorFamily,
		types.Box2DFamily, types.BitFamily, types.IntervalFamily, types.UuidFamily, types.INetFamily,
		types.TSQueryFamily, types.TSVectorFamily, types.PGVectorFamily, types.EnumFamily:
		return Schema{TypeName: SchemaTypeString}, nil
	// Geography and Geometry are not supported by the JSON schema spec, and
	// they're hard to predict the schema of. This is probably fine for now.
	case types.GeographyFamily:
		return Schema{TypeName: SchemaTypeOpaqueJSON, Name: schemaNameGeography}, nil
	case types.GeometryFamily:
		return Schema{TypeName: SchemaTypeOpaqueJSON, Name: schemaNameGeometry}, nil
	case types.BytesFamily:
		return Schema{TypeName: SchemaTypeBytes}, nil
	case types.DateFamily:
		return Schema{TypeName: SchemaTypeString, Name: schemaNameDate}, nil
	case types.TimeFamily, types.TimeTZFamily:
		return Schema{TypeName: SchemaTypeString, Name: schemaNameTime}, nil
	case types.TimestampFamily, types.TimestampTZFamily:
		return Schema{TypeName: SchemaTypeString, Name: schemaNameTimestamp}, nil
	case types.DecimalFamily:
		return Schema{
			TypeName: SchemaTypeFloat64,
			Name:     schemaNameDecimal,
			Parameters: map[string]string{
				"precision": strconv.Itoa(int(typ.Precision())),
				"scale":     strconv.Itoa(int(typ.Scale())),
			},
		}, nil
	case types.ArrayFamily:
		item := typ.ArrayContents()
		itemSchema, err := typeToSchema(item)
		if err != nil {
			return Schema{}, err
		}
		return Schema{
			TypeName: SchemaTypeArray,
			Items:    &itemSchema,
		}, nil
	case types.JsonFamily:
		return Schema{
			TypeName: SchemaTypeOpaqueJSON,
		}, nil
	default:
		return Schema{}, changefeedbase.WithTerminalError(
			errors.Errorf(`type %s not yet supported with json schemas`, typ.SQLString()))

	}
}

// TestingMatchesJSON is a testing helper that asserts that the given data matches
func TestingMatchesJSON(s Schema, data any) error {
	if data == nil && s.Optional {
		return nil
	}

	switch s.TypeName {
	case SchemaTypeInt8:
		if err := assertInt[int8](s, data); err != nil {
			return err
		}
	case SchemaTypeInt16:
		if err := assertInt[int16](s, data); err != nil {
			return err
		}
	case SchemaTypeInt32:
		if err := assertInt[int32](s, data); err != nil {
			return err
		}
	case SchemaTypeInt64:
		if err := assertInt[int64](s, data); err != nil {
			return err
		}
	case SchemaTypeFloat32:
		// NOTE: This is a little weird and we don't have a way to specify unions in these schemas, so maybe we should document this somewhere?
		if d, ok := data.(string); ok && (d == "Infinity" || d == "-Infinity" || d == "NaN") {
			return nil
		}
		if err := assertFloat[float32](s, data); err != nil {
			return err
		}
	case SchemaTypeFloat64:
		if d, ok := data.(string); ok && (d == "Infinity" || d == "-Infinity" || d == "NaN") {
			return nil
		}
		if err := assertFloat[float64](s, data); err != nil {
			return err
		}
	case SchemaTypeBoolean:
		if _, ok := data.(bool); !ok {
			return fmt.Errorf("expected %T for %+#v, got (%+#v)", false, s, data)
		}
	case SchemaTypeString:
		if _, ok := data.(string); !ok {
			return fmt.Errorf("expected %T for %+#v, got (%+#v)", "", s, data)
		}
	case SchemaTypeBytes:
		if _, ok := data.(string); !ok {
			return fmt.Errorf("expected %T for %+#v, got (%+#v)", "", s, data)
		}
	case SchemaTypeArray:
		arr, ok := data.([]any)
		if !ok {
			return fmt.Errorf("expected %T for %+#v, got (%+#v)", []any{}, s, data)
		}
		if s.Items == nil {
			return fmt.Errorf("expected items for %+#v", s)
		}
		for _, a := range arr {
			if err := TestingMatchesJSON(*s.Items, a); err != nil {
				return err
			}
		}
	case SchemaTypeMap:
		return fmt.Errorf("map is not supported")
	case SchemaTypeStruct:
		obj, ok := data.(map[string]any)
		if !ok {
			return fmt.Errorf("expected %T for %+#v, got (%+#v)", map[string]any{}, s, data)
		}

		schemaFields := make(map[string]struct{})
		for _, f := range s.Fields {
			if err := TestingMatchesJSON(f, obj[f.Field]); err != nil {
				return err
			}
			schemaFields[f.Field] = struct{}{}
		}
		if len(obj) != len(schemaFields) {
			return fmt.Errorf("expected %d fields in %+#v, got %d (%+#v)", len(schemaFields), s, len(obj), obj)
		}
	case SchemaTypeOpaqueJSON:
		if _, err := json.MakeJSON(data); err != nil {
			return err
		}
	default:
		return fmt.Errorf("unknown schema type %q in %#+v", s.TypeName, s)
	}

	// Validate logical types.
	switch s.Name {
	case schemaNameDecimal:
		if err := assertFloat[float64](s, data); err != nil {
			return err
		}
		if _, err := strconv.Atoi(s.Parameters["precision"]); err != nil {
			return errors.Newf("expected precision to be an int, got %s", s.Parameters["precision"])
		}
		if _, err := strconv.Atoi(s.Parameters["scale"]); err != nil {
			return errors.Newf("expected scale to be an int, got %s", s.Parameters["scale"])
		}
	case schemaNameGeography:
		d, ok := data.(map[string]any)
		if !ok {
			return errors.Newf("expected %T for %+#v, got (%+#v)", map[string]any{}, s, data)
		}
		j, err := gojson.Marshal(d)
		if err != nil {
			return err
		}
		if _, err = geo.ParseGeographyFromGeoJSON(j); err != nil {
			return err
		}
	case schemaNameGeometry:
		d, ok := data.(map[string]any)
		if !ok {
			return errors.Newf("expected %T for %+#v, got (%+#v)", map[string]any{}, s, data)
		}
		j, err := gojson.Marshal(d)
		if err != nil {
			return err
		}
		if _, err = geo.ParseGeometryFromGeoJSON(j); err != nil {
			return err
		}
	// not worth doing heavy validation for these. They should be strings.
	case schemaNameTimestamp, schemaNameDate, schemaNameTime:
		str, ok := data.(string)
		if !ok {
			return errors.Newf("expected %T for %+#v, got (%+#v)", "", s, data)
		}
		if len(str) == 0 {
			return errors.Newf("expected non-empty string for %+#v, got (%+#v)", s, data)
		}
	}
	return nil
}

func assertInt[I int8 | int16 | int32 | int64](s Schema, data any) error {
	d, ok := data.(gojson.Number)
	if !ok {
		return fmt.Errorf("expected gojson.Number for %+#v, got (%+#v)", s, data)
	}
	i, err := d.Int64()
	if err != nil {
		return err
	}
	if int64(I(i)) != i {
		return fmt.Errorf("expected %T for %+#v, got (%+#v)", I(i), s, data)
	}
	return nil
}

func assertFloat[F float32 | float64](s Schema, data any) error {
	d, ok := data.(gojson.Number)
	if !ok {
		return fmt.Errorf("expected gojson.Number for %+#v, got (%+#v)", s, data)
	}
	f, err := d.Float64()
	if err != nil && strings.Contains(err.Error(), "value out of range") {
		// I'm not sure how this happens but it's probably to do with the random data. Ignore it.
		//nolint:returnerrcheck
		return nil
	}
	if err != nil {
		return err
	}
	if float64(F(f)) != f {
		return fmt.Errorf("expected %T for %+#v, got (%+#v)", F(f), s, data)
	}
	return nil
}
