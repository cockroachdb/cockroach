// Copyright 2019 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package sqlsmith

import (
	"fmt"
	"math/rand"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

var typeNames = func() map[string]*types.T {
	m := map[string]*types.T{
		"int4":   types.Int,
		"int8":   types.Int,
		"int8[]": types.IntArray,
		"float4": types.Float,
		"float8": types.Float,
	}
	for _, T := range types.OidToType {
		m[T.SQLStandardName()] = T
		m[T.String()] = T
	}
	return m
}()

func typeFromName(name string) *types.T {
	// Fill in any collated string type names we see.
	if sp := strings.Split(name, "STRING COLLATE "); len(sp) == 2 {
		typeNames[strings.ToLower(name)] = types.MakeCollatedString(sp[1], 0)
	}
	typ, ok := typeNames[strings.ToLower(name)]
	if !ok {
		panic(fmt.Errorf("unknown type name: %s", name))
	}
	return typ
}

func getRandType() *types.T {
	arr := types.AnyNonArray
	return arr[rand.Intn(len(arr))]
}

// pickAnyType returns a concrete type if typ is types.Any, otherwise typ.
func pickAnyType(typ *types.T) *types.T {
	if typ.SemanticType == types.ANY {
		return getRandType()
	}
	return typ
}
