// Copyright 2015-2018 trivago N.V.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package treflect

import (
	"fmt"
	"reflect"
	"strings"
)

// TypeRegistry is a name to type registry used to create objects by name.
type TypeRegistry struct {
	namedType map[string]reflect.Type
}

// NewTypeRegistry creates a new TypeRegistry. Note that there is a global type
// registry available in the main tgo package (tgo.TypeRegistry).
func NewTypeRegistry() TypeRegistry {
	return TypeRegistry{
		namedType: make(map[string]reflect.Type),
	}
}

// Register a plugin to the TypeRegistry by passing an uninitialized object.
func (registry TypeRegistry) Register(typeInstance interface{}) {
	registry.RegisterWithDepth(typeInstance, 1)
}

// RegisterWithDepth to register a plugin to the TypeRegistry by passing an uninitialized object.
func (registry TypeRegistry) RegisterWithDepth(typeInstance interface{}, depth int) {
	structType := reflect.TypeOf(typeInstance)
	packageName := structType.PkgPath()
	typeName := structType.Name()

	pathTokens := strings.Split(packageName, "/")
	maxDepth := 3
	if len(pathTokens) < maxDepth {
		maxDepth = len(pathTokens)
	}

	for n := depth; n <= maxDepth; n++ {
		shortTypeName := strings.Join(pathTokens[len(pathTokens)-n:], ".") + "." + typeName
		registry.namedType[shortTypeName] = structType
	}
}

// New creates an uninitialized object by class name.
// The class name has to be "package.class" or "package/subpackage.class".
// The gollum package is omitted from the package path.
func (registry TypeRegistry) New(typeName string) (interface{}, error) {
	structType, exists := registry.namedType[typeName]
	if exists {
		return reflect.New(structType).Interface(), nil
	}
	return nil, fmt.Errorf("Unknown class: %s", typeName)
}

// GetTypeOf returns only the type asscociated with the given name.
// If the name is not registered, nil is returned.
// The type returned will be a pointer type.
func (registry TypeRegistry) GetTypeOf(typeName string) reflect.Type {
	if structType, exists := registry.namedType[typeName]; exists {
		return reflect.PtrTo(structType)
	}
	return nil
}

// IsTypeRegistered returns true if a type is registered to this registry.
// Note that GetTypeOf can do the same thing by checking for nil but also
// returns the type, so in many cases you will want to call this function.
func (registry TypeRegistry) IsTypeRegistered(typeName string) bool {
	_, exists := registry.namedType[typeName]
	return exists
}

// GetRegistered returns the names of all registered types for a given package
func (registry TypeRegistry) GetRegistered(packageName string) []string {
	var result []string
	for key := range registry.namedType {
		if strings.HasPrefix(key, packageName) {
			result = append(result, key)
		}
	}
	return result
}
