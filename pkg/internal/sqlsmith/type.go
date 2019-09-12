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
	"github.com/cockroachdb/cockroach/pkg/col/coltypes"
	"github.com/cockroachdb/cockroach/pkg/sql/colexec/typeconv"
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
func (s *Smither) pickAnyType(typ *types.T) (_ *types.T, ok bool) {
	switch typ.Family() {
	case types.AnyFamily:
		typ = s.randType()
	case types.ArrayFamily:
		if typ.ArrayContents().Family() == types.AnyFamily {
			typ = sqlbase.RandArrayContentsType(s.rnd)
		}
	}
	return typ, s.allowedType(typ)
}

func (s *Smither) randScalarType() *types.T {
	for {
		t := sqlbase.RandScalarType(s.rnd)
		if !s.allowedType(t) {
			continue
		}
		return t
	}
}

func (s *Smither) randType() *types.T {
	for {
		t := sqlbase.RandType(s.rnd)
		if !s.allowedType(t) {
			continue
		}
		return t
	}
}

// allowedType returns whether t is ok to be used. This is useful to filter
// out undesirable types to enable certain execution paths to be taken (like
// vectorization).
func (s *Smither) allowedType(types ...*types.T) bool {
	for _, t := range types {
		if s.vectorizable && typeconv.FromColumnType(t) == coltypes.Unhandled {
			return false
		}
	}
	return true
}

func (s *Smither) makeDesiredTypes() []*types.T {
	var typs []*types.T
	for {
		typs = append(typs, s.randType())
		if s.d6() < 2 || !s.canRecurse() {
			break
		}
	}
	return typs
}
