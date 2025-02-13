// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package avro

import (
	"github.com/cockroachdb/cockroach/pkg/util/json"
	"github.com/cockroachdb/errors"
)

// JSONSchema writes a kafka connect-compatible JSON schema for the record into
// the given object builder.
func (r *Record) JSONSchema(b *json.ObjectBuilder) error {
	b.Add("type", json.FromString("struct"))
	b.Add("field", json.FromString(r.Name))
	fields := json.NewArrayBuilder(len(r.Fields))
	for _, f := range r.Fields {
		fb := json.NewObjectBuilder(3)
		fb.Add("field", json.FromString(f.Name))
		if err := addSchemaTypeJSONSchema(f.SchemaType, fb); err != nil {
			return err
		}
		fields.Add(fb.Build())
	}
	b.Add("fields", fields.Build())
	return nil
}

func stripOptional(t SchemaType) (SchemaType, bool) {
	if a, ok := t.([]SchemaType); ok && len(a) == 2 {
		if a[0] == SchemaTypeNull {
			return a[1], true
		}
	}
	return t, false
}

type jsonSchemer interface {
	JSONSchema(b *json.ObjectBuilder) error
}

func addSchemaTypeJSONSchema(t SchemaType, b *json.ObjectBuilder) error {
	typ, optional := stripOptional(t)

	b.Add("optional", json.FromBool(optional))

	switch t := typ.(type) {
	case logicalType:
		if err := addSchemaTypeJSONSchema(t.SchemaType, b); err != nil {
			return err
		}
		b.Add("name", json.FromString(t.LogicalType))
		paramsBuilder := json.NewObjectBuilder(2)
		if t.Precision != nil {
			paramsBuilder.Add("precision", json.FromInt(*t.Precision))
		}
		if t.Scale != nil {
			paramsBuilder.Add("scale", json.FromInt(*t.Scale))
		}
		if params := paramsBuilder.Build(); params.Len() > 0 {
			b.Add("parameters", params)
		}

	case arrayType:
		// TODO: figure out if this is right, against debezium.
		if err := addSchemaTypeJSONSchema(t.Items, b); err != nil {
			return err
		}
		st, err := simpleSchemaTypeToJSONSchemaType(t.Items)
		if err != nil {
			return err
		}
		b.Add("items", json.FromString(st))

	case jsonSchemer:
		return t.JSONSchema(b)
	// Non-composite types.
	case string:
		st, err := simpleSchemaTypeToJSONSchemaType(t)
		if err != nil {
			return err
		}
		b.Add("type", json.FromString(st))
	}
	return nil
}

func simpleSchemaTypeToJSONSchemaType(t SchemaType) (string, error) {
	switch t {
	case SchemaTypeArray:
		return "array", nil
	case SchemaTypeBoolean:
		return "boolean", nil
	case SchemaTypeBytes:
		return "bytes", nil
	case SchemaTypeDouble:
		return "float64", nil
	case SchemaTypeInt:
		return "int32", nil // TODO: is this right?
	case SchemaTypeLong:
		return "int64", nil
	case SchemaTypeNull:
		return "null", nil
	case SchemaTypeString:
		return "string", nil
	default:
		return "", errors.AssertionFailedf(`unknown simple schema type type %#+v`, t)
	}
}
