// Copyright 2018 The Cockroach Authors.
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

package testutils

import (
	"github.com/cockroachdb/cockroach/pkg/sql/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// ParseType parses a string describing a type.
func ParseType(typeStr string) (types.T, error) {
	colType, err := parser.ParseType(typeStr)
	if err != nil {
		return nil, err
	}
	return coltypes.CastTargetToDatumType(colType), nil
}

// ParseTypes parses a list of types.
func ParseTypes(colStrs []string) ([]types.T, error) {
	res := make([]types.T, len(colStrs))
	for i, s := range colStrs {
		var err error
		res[i], err = ParseType(s)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}

// ParseScalarExpr parses a scalar expression and converts it to a
// tree.TypedExpr.
func ParseScalarExpr(sql string, ivc tree.IndexedVarContainer) (tree.TypedExpr, error) {
	expr, err := parser.ParseExpr(sql)
	if err != nil {
		return nil, err
	}

	sema := tree.MakeSemaContext(false /* privileged */)
	sema.IVarContainer = ivc

	return expr.TypeCheck(&sema, types.Any)
}
