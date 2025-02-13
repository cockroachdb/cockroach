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
		if err := addSchemaTypeJSONSchema(fb, f.SchemaType); err != nil {
			return err
		}
		fields.Add(fb.Build())
	}
	b.Add("fields", fields.Build())
	return nil
}

// NOTE: string fallbacks aren't supported in the JSON schema output. Today the
// only type that uses this is Decimal, and it's not clear how to represent that
// in the JSON schema output.
func stripOptionalWithStringFallback(t SchemaType) (inner SchemaType, optional bool, stringFallback bool) {
	if a, ok := t.([]SchemaType); ok && len(a) == 3 {
		if a[0] == SchemaTypeNull && a[2] == SchemaTypeString {
			return a[1], true, false
		}
	} else if a, ok := t.([]SchemaType); ok && len(a) == 2 {
		if a[0] == SchemaTypeNull {
			return a[1], true, true
		}
	}
	return t, false, false
}

type jsonSchemer interface {
	JSONSchema(b *json.ObjectBuilder) error
}

func addSchemaTypeJSONSchema(b *json.ObjectBuilder, t SchemaType) error {
	typ, optional, _ := stripOptionalWithStringFallback(t)
	b.Add("optional", json.FromBool(optional))

	// SchemaType is the empty interface and there are a few different types that
	// are used in it:
	// - logicalType - this is used for things like Decimal where there's a "real" type and a "logical" type.
	// - arrayType - this is used for arrays.
	// - something implementing jsonSchemer - currently only a *Record and things embedding one.
	// - a string - this is used for simple types like int, string, etc.
	switch t := typ.(type) {
	case logicalType:
		if err := addSchemaTypeJSONSchema(b, t.SchemaType); err != nil {
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
		b.Add("type", json.FromString("array"))
		itemsB := json.NewObjectBuilder(2)
		if err := addSchemaTypeJSONSchema(itemsB, t.Items); err != nil {
			return err
		}
		b.Add("items", itemsB.Build())
	case jsonSchemer:
		return t.JSONSchema(b)
	case string:
		st, err := simpleSchemaTypeToJSONSchemaType(t)
		if err != nil {
			return err
		}
		b.Add("type", json.FromString(st))
	default:
		return errors.AssertionFailedf(`unknown schema type %#+v`, t)
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
		return "", errors.AssertionFailedf(`unknown simple schema type: %#+v`, t)
	}
}
