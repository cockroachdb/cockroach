// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package codec

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type (
	Animal interface {
		DynamicType
		Speak() string
	}

	Dog struct {
		Name string
	}

	Cat struct {
		Name string
	}
)

func (d Dog) Speak() string {
	return d.Name + " woofs"
}

func (c Cat) Speak() string {
	return c.Name + " meows"
}

func TestTypeInfo(t *testing.T) {
	typeName := ResolveTypeName(new(Cat))
	ft := typeName.toFieldType(false)
	ftPtr := typeName.toFieldType(true)
	require.Equal(t, TypeName{
		name: "Cat",
		pkg:  "codec",
	}, typeName)
	require.Equal(t, "codec.Cat", string(ft))
	require.Equal(t, "codec.*Cat", string(ftPtr))
	require.True(t, ftPtr.isPointer())
}

func TestDynamicTypes(t *testing.T) {
	var animals = []Animal{
		Dog{Name: "Java"},
		&Cat{Name: "Mely"},
	}

	data, err := yaml.Marshal(WrapList(animals))
	require.NoError(t, err)

	var decoded ListWrapper[Animal]
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	err = dec.Decode(&decoded)
	require.NoError(t, err)

	// Verify equality of the decoded list and the original list.
	require.Equal(t, animals, decoded.Get())
	// Verify pointer types are decoded as pointers.
	require.Equal(t, reflect.TypeOf(decoded.Get()[1]).Kind(), reflect.Pointer)
	// Verify dynamic types have the correct underlying struct type.
	require.Equal(t, decoded.Get()[0].Speak(), "Java woofs")
	require.Equal(t, decoded.Get()[1].Speak(), "Mely meows")
}
