// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package serde

import (
	"fmt"
	"reflect"

	"gopkg.in/yaml.v3"
)

type (
	TypeMap map[string]reflect.StructField

	TypeWrapper struct {
		Type string
		Val  interface{}
	}
)

// ToTypeMap takes a pointer to a struct that contains types that need to be
// registered in a TypeMap. The returned map contains the type name and the
// `reflect.StructField` for each field in the struct.
func ToTypeMap(typeStructPtr any) TypeMap {
	typeMap := make(map[string]reflect.StructField)
	tValue := reflect.ValueOf(typeStructPtr).Type().Elem()
	for i := 0; i < tValue.NumField(); i++ {
		field := tValue.Field(i)
		typeMap[field.Type.Name()] = field
	}
	return typeMap
}

// UnmarshalYAML implements the [yaml.Unmarshaler] interface. It serializes the
// TypeWrapper into a YAML node. Its purpose is to unmarshal a dynamic type
// using the type name, supplied by the wrapper, and the provided type registry
// map.
func (t *TypeWrapper) UnmarshalYAML(typeMap TypeMap, value *yaml.Node) error {
	typeName := value.Content[1].Value
	objType, ok := typeMap[typeName]
	if !ok {
		return fmt.Errorf("unknown type %q not found in %T", typeName, typeMap)
	}
	objValuePtr := reflect.New(objType.Type).Interface()
	if err := value.Content[3].Decode(objValuePtr); err != nil {
		return err
	}
	objValue := reflect.ValueOf(objValuePtr).Elem().Interface()
	if objType.Tag.Get("type") == "pointer" {
		objValue = reflect.ValueOf(objValuePtr).Interface()
	}
	*t = TypeWrapper{
		Type: typeName,
		Val:  objValue,
	}
	return nil
}

// GetTypeName is a generic method that retrieves the type name of a dynamic type
// from a struct that contains registered types. It validates that the dynamic
// type matches the expected type and that the validation reference is a field
// of the struct. It returns the type name as a string or an error if any
// validation fails.
func GetTypeName(typesStruct any, dynamicType any, validationRef any) (string, error) {
	if reflect.TypeOf(validationRef).Kind() != reflect.Pointer {
		return "", fmt.Errorf("validationRef %T must be passed as a pointer type", validationRef)
	}
	if !StructContainsTarget(reflect.ValueOf(typesStruct), reflect.ValueOf(validationRef)) {
		return "", fmt.Errorf("validationRef %T is not a field of %T", validationRef, typesStruct)
	}
	if reflect.TypeOf(dynamicType) != reflect.TypeOf(validationRef).Elem() {
		return "", fmt.Errorf("type mismatch: %T is not the same as validation type %T", dynamicType, validationRef)
	}
	return reflect.TypeOf(dynamicType).Name(), nil
}

// StructContainsTarget checks if the structValue contains a field that matches
// the targetValue. It compares the addresses of the fields to determine if they
// are the same. This is useful for validating that a dynamic type is part of a
// struct that contains registered types.
func StructContainsTarget(structValue reflect.Value, targetValue reflect.Value) bool {
	structValue = structValue.Elem()
	targetValue = targetValue.Elem()
	for i := 0; i < structValue.NumField(); i++ {
		field := structValue.Field(i)
		fieldPtr := field.Addr().Interface()
		if fieldPtr == targetValue.Addr().Interface() {
			return true
		}
	}
	return false
}
