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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
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
		typeNames[strings.ToLower(name)] = types.MakeCollatedString(types.String, sp[1])
	}
	typ, ok := typeNames[strings.ToLower(name)]
	if !ok {
		panic(fmt.Errorf("unknown type name: %s", name))
	}
	return typ
}

// pickAnyType returns a concrete type if typ is types.Any or types.AnyArray,
// otherwise typ.
func pickAnyType(s *scope, typ *types.T) *types.T {
	switch typ.Family() {
	case types.AnyFamily:
		return sqlbase.RandType(s.schema.rnd)
	case types.ArrayFamily:
		if typ.ArrayContents().Family() == types.AnyFamily {
			return sqlbase.RandArrayContentsType(s.schema.rnd)
		}
	}
	return typ
}
