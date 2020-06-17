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
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
)

func (s *Smither) typeFromName(name string) (*types.T, error) {
	typRef, err := parser.ParseType(name)
	if err != nil {
		return nil, errors.AssertionFailedf("failed to parse type: %v", name)
	}
	typ, err := tree.ResolveType(context.TODO(), typRef, s)
	if err != nil {
		return nil, err
	}
	return typ, nil
}

// pickAnyType returns a concrete type if typ is types.Any or types.AnyArray,
// otherwise typ.
func (s *Smither) pickAnyType(typ *types.T) *types.T {
	switch typ.Family() {
	case types.AnyFamily:
		typ = s.randType()
	case types.ArrayFamily:
		if typ.ArrayContents().Family() == types.AnyFamily {
			typ = sqlbase.RandArrayContentsType(s.rnd)
		}
	}
	return typ
}

func (s *Smither) randScalarType() *types.T {
	var udts []*types.T
	for _, t := range s.userDefinedTypes {
		udts = append(udts, t)
	}
	return sqlbase.RandTypeFromSlice(s.rnd, append(udts, types.Scalar...))
}

func (s *Smither) randType() *types.T {
	var udts []*types.T
	for _, t := range s.userDefinedTypes {
		udts = append(udts, t)
	}
	return sqlbase.RandTypeFromSlice(s.rnd, append(udts, sqlbase.SeedTypes...))
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

// ResolveType implements the tree.TypeReferenceResolver interface.
func (s *Smither) ResolveType(
	_ context.Context, name *tree.UnresolvedObjectName,
) (*types.T, error) {
	if name.NumParts > 1 {
		return nil, errors.AssertionFailedf("smither cannot resolve qualified names %s", name)
	}
	res, ok := s.userDefinedTypes[name.Object()]
	if !ok {
		return nil, errors.Newf("type name %s not found by smither", name.Object())
	}
	return res, nil
}

// ResolveTypeByID implements the tree.TypeReferenceResolver interface.
func (s *Smither) ResolveTypeByID(context.Context, uint32) (*types.T, error) {
	return nil, errors.AssertionFailedf("smither cannot resolve types by ID")
}
