// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package reltest

import (
	"runtime"
	"strings"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// Registry stores entities by name. It helps to serialize things in an
// easier-to-consume manner by replacing structs by their name. It is
// leveraged as part of building a suite.
type Registry struct {
	names       []string
	valueToName map[interface{}]string
	nameToValue map[string]interface{}

	// Some entities are initialized from yaml literals, preserve those
	// for formatting later as they are nicer.
	nameToYAML map[string]string
}

// NewRegistry constructs a Registry.
func NewRegistry() *Registry {
	return &Registry{
		valueToName: make(map[interface{}]string),
		nameToValue: make(map[string]interface{}),
		nameToYAML:  make(map[string]string),
	}
}

// Register the entity v with the provided name, asserting, there is not
// already such a registration, and returning the value for easier use in
// var blocks.
func (r *Registry) Register(name string, v interface{}) interface{} {
	if existing, exists := r.nameToValue[name]; exists {
		panic(errors.AssertionFailedf(
			"entity with name %s already registered %v, trying to register %v",
			name, existing, v,
		))
	}
	r.names = append(r.names, name)
	r.nameToValue[name] = v
	r.valueToName[v] = name
	return v
}

// FromYAML takes a yaml literal, unmarshals it into dest, registers it with
// name, and returns dest for use in a var block. Using FromYAML helps make
// more terse yaml encodings.
func (r *Registry) FromYAML(name, yamlData string, dest interface{}) interface{} {
	if err := yaml.NewDecoder(strings.NewReader(yamlData)).Decode(dest); err != nil {
		panic(err)
	}
	r.Register(name, dest)
	r.nameToYAML[name] = yamlData
	return dest
}

// MustGetByName gets the registered entity with the given name.
func (r *Registry) MustGetByName(t *testing.T, k string) interface{} {
	got, ok := r.nameToValue[k]
	require.Truef(t, ok, "MustGetByName(%s)", k)
	return got
}

// MustGetName gets the name with a given registered entity.
func (r *Registry) MustGetName(t *testing.T, v interface{}) string {
	got, ok := r.GetName(v)
	require.Truef(t, ok, "MustGetName(%v)", v)
	return got
}

// GetName is like MustGetName but does not enforce that it exists.
func (r *Registry) GetName(i interface{}) (string, bool) {
	defer func() {
		r := recover()
		if r == nil {
			return
		}
		if re, ok := r.(runtime.Error); ok &&
			strings.Contains(re.Error(), "hash of unhashable type") {
			return
		}
		panic(r)
	}()
	got, ok := r.valueToName[i]
	return got, ok
}

func (r *Registry) valueToYAML(t *testing.T, name string) *yaml.Node {
	if yamlStr, hasToYAML := r.nameToYAML[name]; hasToYAML {
		var v interface{}
		require.NoError(t, yaml.NewDecoder(strings.NewReader(yamlStr)).Decode(&v))
		var n yaml.Node
		require.NoError(t, n.Encode(v))
		n.Style = yaml.FlowStyle
		return &n
	}
	return r.EncodeToYAML(t, r.MustGetByName(t, name))
}

// RegistryYAMLEncoder is an interface which tests can use to override
// how entities get marshaled to yaml in the context of EncodeToYAML.
type RegistryYAMLEncoder interface {
	EncodeToYAML(t *testing.T, r *Registry) interface{}
}

// EncodeToYAML can be used to encode an entity to yaml. This call respects
// RegistryYAMLEncoder.
func (r *Registry) EncodeToYAML(t *testing.T, v interface{}) *yaml.Node {
	toEncode := v
	if encoder, ok := v.(RegistryYAMLEncoder); ok {
		toEncode = encoder.EncodeToYAML(t, r)
	}
	var n yaml.Node
	require.NoError(t, n.Encode(toEncode))
	return &n
}
