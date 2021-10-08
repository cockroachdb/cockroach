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
	"github.com/cockroachdb/cockroach/pkg/sql/randgen"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

func (s *Smither) typeFromSQLTypeSyntax(typeStr string) (*types.T, error) {
	typRef, err := parser.GetTypeFromValidSQLSyntax(typeStr)
	if err != nil {
		return nil, errors.AssertionFailedf("failed to parse type: %v", typeStr)
	}
	typ, err := tree.ResolveType(context.Background(), typRef, s)
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
			typ = randgen.RandArrayContentsType(s.rnd)
		}
	}
	return typ
}

func (s *Smither) randScalarType() *types.T {
	s.lock.RLock()
	defer s.lock.RUnlock()
	scalarTypes := types.Scalar
	if s.types != nil {
		scalarTypes = s.types.scalarTypes
	}
	return randgen.RandTypeFromSlice(s.rnd, scalarTypes)
}

func (s *Smither) randType() *types.T {
	s.lock.RLock()
	defer s.lock.RUnlock()
	seedTypes := randgen.SeedTypes
	if s.types != nil {
		seedTypes = s.types.seedTypes
	}
	return randgen.RandTypeFromSlice(s.rnd, seedTypes)
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

type typeInfo struct {
	udts        map[tree.TypeName]*types.T
	seedTypes   []*types.T
	scalarTypes []*types.T
}

// ResolveType implements the tree.TypeReferenceResolver interface.
func (s *Smither) ResolveType(
	_ context.Context, name *tree.UnresolvedObjectName,
) (*types.T, error) {
	key := tree.MakeSchemaQualifiedTypeName(name.Schema(), name.Object())
	res, ok := s.types.udts[key]
	if !ok {
		return nil, errors.Newf("type name %s not found by smither", name.Object())
	}
	return res, nil
}

// ResolveTypeByOID implements the tree.TypeReferenceResolver interface.
func (s *Smither) ResolveTypeByOID(context.Context, oid.Oid) (*types.T, error) {
	return nil, errors.AssertionFailedf("smither cannot resolve types by OID")
}
