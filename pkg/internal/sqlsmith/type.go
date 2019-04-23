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
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
)

func typeFromName(name string) *types.T {
	typ, err := parser.ParseType(name)
	if err != nil {
		panic(pgerror.AssertionFailedf("failed to parse type: %v", name))
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
