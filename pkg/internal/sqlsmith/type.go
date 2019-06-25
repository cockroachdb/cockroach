// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sqlsmith

import (
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func typeFromName(name string) *types.T {
	typ, err := parser.ParseType(name)
	if err != nil {
		panic(errors.AssertionFailedf("failed to parse type: %v", name))
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
