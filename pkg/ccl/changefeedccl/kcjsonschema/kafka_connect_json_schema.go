// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kcjsonschema

import (
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/pkg/errors"
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
	// TODO: These two are additions to the spec that we don't need to make if we don't want to.
	schemaNameGeometry  schemaName = "geometry"
	schemaNameGeography schemaName = "geography"
)

// Schema is the JSON representation of a Kafka Connect JSON Schema.
type Schema struct {
	TypeName schemaType
	// Name is my schema name, optional. Can represent a logical type (such as
	// decimal) or the overall entity type, such as "envelope", etc.
	Name schemaName
	// Field is the name of the field that I am in my parent struct.
	Field string
	// Parameters is a map of parameters for the schema. Currently this only has
	// meaning for decimals, where it contains precision and scale.
	Parameters map[string]string
	// Fields are the fields of the struct, if this is a struct.
	Fields []Schema
	// Optional is whether this field is optional. In practice we always set
	// this to true.
	// TODO: should we? this thought is a holdover from the avro
	// impl tbh. We should probably be accurate right?
	Optional bool
	// Items is the type of the array elements, if this is an array.
	Items *Schema

	// NOTE: the "spec" contains two other optional fields -- Version (int) and
	// Doc (string), which we do not implement.
}

// AsJSON returns the JSON representation of the schema. There is nothing
// surprising here, and I wish we could use something generated via struct tags,
// but our json package doesn't seem to support that.
func (s Schema) AsJSON() json.JSON {
	b := json.NewObjectBuilder(2)
	b.Add("type", json.FromString(string(s.TypeName)))
	if s.Name != "" {
		b.Add("name", json.FromString(string(s.Name)))
	}
	if s.Field != "" {
		b.Add("field", json.FromString(s.Field))
	}
	if len(s.Parameters) > 0 {
		params := json.NewObjectBuilder(len(s.Parameters))
		for k, v := range s.Parameters {
			params.Add(k, json.FromString(v))
		}
		b.Add("parameters", params.Build())
	}
	if len(s.Fields) > 0 {
		fields := json.NewArrayBuilder(len(s.Fields))
		for _, f := range s.Fields {
			fields.Add(f.AsJSON())
		}
		b.Add("fields", fields.Build())
	}
	if s.Optional {
		b.Add("optional", json.TrueJSONValue)
	}
	if s.Items != nil {
		b.Add("items", s.Items.AsJSON())
	}
	return b.Build()
}

func NewEnrichedEnvelope(before, after, source *Schema) Schema {
	fields := make([]Schema, 0, 3)
	if before != nil {
		before.Field = "before"
		fields = append(fields, *before)
	}
	if after != nil {
		after.Field = "after"
		fields = append(fields, *after)
	}
	if source != nil {
		source.Field = "source"
		fields = append(fields, *source)
	}

	fields = append(fields,
		Schema{
			TypeName: SchemaTypeInt64,
			Field:    "ts_ns",
			Optional: true,
		}, Schema{
			TypeName: SchemaTypeString,
			Field:    "op",
			Optional: true,
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
		Optional: true,
	}
	err := it.Col(func(col cdcevent.ResultColumn) error {
		colSchema, err := typeToSchema(col.Typ)
		if err != nil {
			return err
		}
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
	schema := Schema{Optional: true}
	switch typ.Family() {
	case types.IntFamily:
		schema.TypeName = SchemaTypeInt64
	case types.BoolFamily:
		schema.TypeName = SchemaTypeBoolean
	case types.FloatFamily:
		schema.TypeName = SchemaTypeFloat64
	case types.StringFamily, types.CollatedStringFamily, types.PGLSNFamily, types.RefCursorFamily,
		types.Box2DFamily, types.BitFamily, types.IntervalFamily, types.UuidFamily, types.INetFamily,
		types.TSQueryFamily, types.TSVectorFamily, types.PGVectorFamily, types.EnumFamily:
		schema.TypeName = SchemaTypeString
	case types.GeographyFamily:
		schema.TypeName = SchemaTypeStruct
		schema.Name = schemaNameGeography
	case types.GeometryFamily:
		schema.TypeName = SchemaTypeStruct
		schema.Name = schemaNameGeometry
	case types.BytesFamily:
		schema.TypeName = SchemaTypeBytes
	case types.DateFamily:
		schema.TypeName = SchemaTypeString
		schema.Name = schemaNameDate
	case types.TimeFamily, types.TimeTZFamily:
		schema.TypeName = SchemaTypeString
		schema.Name = schemaNameTime
	case types.TimestampFamily, types.TimestampTZFamily:
		schema.TypeName = SchemaTypeString
		schema.Name = schemaNameTimestamp
	case types.DecimalFamily:
		schema.TypeName = SchemaTypeFloat64
		schema.Name = schemaNameDecimal
		schema.Parameters = map[string]string{
			"precision": strconv.Itoa(int(typ.Precision())),
			"scale":     strconv.Itoa(int(typ.Scale())),
		}
	case types.ArrayFamily:
		item := typ.ArrayContents()
		itemSchema, err := typeToSchema(item)
		if err != nil {
			return Schema{}, err
		}
		schema.TypeName = SchemaTypeArray
		schema.Items = &itemSchema
	case types.JsonFamily:
		schema.TypeName = SchemaTypeOpaqueJSON
	default:
		return Schema{}, changefeedbase.WithTerminalError(
			errors.Errorf(`type %s not yet supported with json schemas`, typ.SQLString()))

	}
	return schema, nil
}
