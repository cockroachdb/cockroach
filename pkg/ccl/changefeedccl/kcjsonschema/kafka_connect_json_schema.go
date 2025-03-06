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
	// Optional is whether this field is optional. This should reflect the
	// nullability of database columns, and be true for all fields we add
	// ourselves.
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
	b.Add("optional", json.FromBool(s.Optional))
	if s.Items != nil {
		b.Add("items", s.Items.AsJSON())
	}
	return b.Build()
}

// NewEnrichedEnvelope creates a new schema for an enriched envelope.
func NewEnrichedEnvelope(before, after, source *Schema) Schema {
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
	case types.GeographyFamily:
		return Schema{TypeName: SchemaTypeStruct, Name: schemaNameGeography}, nil
	case types.GeometryFamily:
		return Schema{TypeName: SchemaTypeStruct, Name: schemaNameGeometry}, nil
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
