// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package exprgen

import (
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
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
			return nil, errors.Wrapf(err, "cannot parse %s as a type", typeStr)
		}
		colTypesRefs := parsed.AST.(*tree.Prepare).Types
		colTypes := make([]*types.T, len(colTypesRefs))
		for i := range colTypesRefs {
			colTypes[i] = tree.MustBeStaticallyKnownType(colTypesRefs[i])
		}
		return types.MakeTuple(colTypes), nil
	}
	typ, err := parser.GetTypeFromValidSQLSyntax(typeStr)
	if err != nil {
		return nil, err
	}
	return tree.MustBeStaticallyKnownType(typ), nil
}
