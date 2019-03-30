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

package exprgen

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/types"
)

// ParseType parses a string describing a type.
// It supports tuples using the syntax "tuple{<type>, <type>, ...}" but does not
// support tuples of tuples.
func ParseType(typeStr string) (*types.T, error) {
	// Special case for tuples for which there is no SQL syntax.
	if strings.HasPrefix(typeStr, "tuple{") && strings.HasSuffix(typeStr, "}") {
		s := strings.TrimPrefix(typeStr, "tuple{")
		s = strings.TrimSuffix(s, "}")
		// Hijack the PREPARE syntax which takes a list of types.
		// TODO(radu): this won't work for tuples of tuples; we would need to add
		// some special syntax.
		parsed, err := parser.ParseOne(fmt.Sprintf("PREPARE x ( %s ) AS SELECT 1", s))
		if err != nil {
			return nil, fmt.Errorf("cannot parse %s as a type: %s", typeStr, err)
		}
		colTypes := parsed.AST.(*tree.Prepare).Types
		res := &types.T{SemanticType: types.TUPLE, TupleContents: make([]types.T, len(colTypes))}
		for i := range colTypes {
			res.TupleContents[i] = *colTypes[i]
		}
		return res, nil
	}
	return parser.ParseType(typeStr)
}

// ParseTypes parses a list of types.
func ParseTypes(colStrs []string) ([]*types.T, error) {
	res := make([]*types.T, len(colStrs))
	for i, s := range colStrs {
		var err error
		res[i], err = ParseType(s)
		if err != nil {
			return nil, err
		}
	}
	return res, nil
}
