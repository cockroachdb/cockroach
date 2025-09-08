// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package codec

import (
	"fmt"
	"path"
	"reflect"
	"strings"

	"gopkg.in/yaml.v3"
)

type (
	// TypeMap is a map of type names with their corresponding `reflect.Type`.
	// This is used to resolve type names to `reflect.Type` during decoding.
	TypeMap map[TypeName]reflect.Type

	// Metadata is an internal type used to encode and decode dynamic types with
	// their type name.
	metadata struct {
		FieldType fieldType   `yaml:"type"`
		Val       DynamicType `yaml:"val"`
	}

	// DynamicType is an interface used to encode and decode dynamic types.
	// This is used to allow for custom types to be encoded and decoded.
	DynamicType interface {
		GetTypeName() TypeName
	}

	// Wrapper wraps a dynamic type for encoding and decoding.
	Wrapper[T DynamicType] struct {
		Val T `yaml:",inline"`
	}

	// ListWrapper wraps a list of dynamic types for encoding and decoding.
	ListWrapper[T DynamicType] struct {
		Val []T `yaml:",inline"`
	}

	// TypeName stores the package and name of a type.
	TypeName struct {
		pkg  string
		name string
	}

	// fieldType provides the type information during encoding and decoding. It
	// stores a string representation of the TypeName and whether the type should
	// be decoded as a pointer.
	// Ex. `util.Tree` or `util.*Tree`
	fieldType string
)

var typeRegistry = make(TypeMap)

// Register registers a dynamic type with the codec package. All dynamic types
// must be registered before they can be encoded and decoded.
func Register(t DynamicType) {
	typeRegistry[t.GetTypeName()] = ResolveElem(reflect.TypeOf(t))
}

// Wrap is a convenience function for wrapping a dynamic type.
func Wrap[T DynamicType](t T) Wrapper[T] {
	return Wrapper[T]{Val: t}
}

// WrapList is a convenience function for wrapping a list of dynamic types.
func WrapList[T DynamicType](t []T) ListWrapper[T] {
	return ListWrapper[T]{Val: t}
}

func (w *Wrapper[T]) Get() T {
	return w.Val
}

func (w *ListWrapper[T]) Get() []T {
	return w.Val
}

// ResolveElem unwraps a pointer type to get the underlying type or returns the
// type itself if it is not a pointer.
func ResolveElem(t reflect.Type) reflect.Type {
	if t.Kind() == reflect.Ptr {
		return t.Elem()
	}
	return t
}

// ResolveTypeName resolves the type name of a dynamic type.
func ResolveTypeName(t DynamicType) TypeName {
	tType := ResolveElem(reflect.TypeOf(t))
	return TypeName{
		pkg:  path.Base(tType.PkgPath()),
		name: tType.Name(),
	}
}

// toFieldType encodes the type name as a fieldType (string), including the
// package path, and if the type should later on be decoded as a pointer.
func (t TypeName) toFieldType(pointer bool) fieldType {
	name := t.name
	if pointer {
		name = "*" + name
	}
	return fieldType(t.pkg + "." + name)
}

// name returns the package and name of the type.
func (t fieldType) name() (TypeName, error) {
	s := strings.Split(string(t), ".")
	if len(s) != 2 {
		return TypeName{}, fmt.Errorf("invalid type name %q", t)
	}
	// The `typeName` should not include the pointer prefix, as this is only used
	// during decoding to determine if the value should be decoded as a pointer.
	s[1] = strings.TrimPrefix(s[1], "*")
	return TypeName{
		pkg:  s[0],
		name: s[1],
	}, nil
}

// isPointer returns true if the type is a pointer.
func (t fieldType) isPointer() bool {
	return strings.Contains(string(t), "*")
}

// MarshalYAML implements the [yaml.Marshaler] interface. The wrapper is
// inspected to get its type information, which is then encoded with the
// metadata type.
func (w Wrapper[T]) MarshalYAML() (any, error) {
	typeName := w.Get().GetTypeName()
	dynType := reflect.TypeOf(w.Get())
	pointer := false
	if dynType.Kind() == reflect.Ptr {
		pointer = true
	}
	return metadata{
		FieldType: typeName.toFieldType(pointer),
		Val:       w.Val,
	}, nil
}

// UnmarshalYAML implements the [yaml.Unmarshaler] interface. Since the wrapper
// was stored with metadata, we decode the type information and then decode the
// value using the type information.
func (w *Wrapper[T]) UnmarshalYAML(value *yaml.Node) error {
	ft := fieldType(value.Content[1].Value)
	typeName, err := ft.name()
	if err != nil {
		return err
	}
	objType, ok := typeRegistry[typeName]
	if !ok {
		return fmt.Errorf("unknown type %s.%s", typeName.pkg, typeName.name)
	}
	objValuePtr := reflect.New(objType).Interface()
	if err := value.Content[3].Decode(objValuePtr); err != nil {
		return err
	}
	objValue := reflect.ValueOf(objValuePtr).Elem().Interface()
	if ft.isPointer() {
		objValue = reflect.ValueOf(objValuePtr).Interface()
	}
	*w = Wrapper[T]{
		objValue.(T),
	}
	return nil
}

// MarshalYAML implements the [yaml.Marshaler] interface. This is a convenience
// function for encoding a list of dynamic types and leverages the Wrapper type
// to encode each dynamic type.
func (w ListWrapper[T]) MarshalYAML() (any, error) {
	wrappers := make([]Wrapper[T], len(w.Val))
	for i, v := range w.Val {
		wrappers[i] = Wrap(v)
	}
	return wrappers, nil
}

// UnmarshalYAML implements the [yaml.Unmarshaler] interface. This is a
// convenience function for decoding a list of dynamic types and leverages the
// Wrapper type to encode each dynamic type.
func (w *ListWrapper[T]) UnmarshalYAML(value *yaml.Node) error {
	*w = ListWrapper[T]{
		Val: make([]T, len(value.Content)),
	}
	for i, v := range value.Content {
		var e Wrapper[T]
		if err := v.Decode(&e); err != nil {
			return err
		}
		w.Val[i] = e.Val
	}
	return nil
}
