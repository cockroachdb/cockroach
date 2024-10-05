// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	case types.DecimalFamily:
		if s.disableDecimals {
			typ = s.randType()
		}
	case types.OidFamily:
		if s.disableOIDs {
			typ = s.randType()
		}
	}
	return typ
}

var simpleScalarTypes = func() (typs []*types.T) {
	for _, t := range types.Scalar {
		switch t {
		case types.Box2D, types.Geography, types.Geometry, types.INet, types.PGLSN,
			types.RefCursor, types.TSQuery, types.TSVector:
			// Skip fancy types.
		default:
			typs = append(typs, t)
		}
	}
	return typs
}()

func isSimpleSeedType(typ *types.T) bool {
	switch typ.Family() {
	case types.BoolFamily, types.IntFamily, types.DecimalFamily, types.FloatFamily, types.StringFamily,
		types.BytesFamily, types.DateFamily, types.TimestampFamily, types.IntervalFamily, types.TimeFamily, types.TimeTZFamily:
		return true
	case types.ArrayFamily:
		return isSimpleSeedType(typ.ArrayContents())
	case types.TupleFamily:
		for _, t := range typ.TupleContents() {
			if !isSimpleSeedType(t) {
				return false
			}
		}
		return true
	default:
		return false
	}
}

func (s *Smither) randScalarType() *types.T {
	s.lock.RLock()
	defer s.lock.RUnlock()
	scalarTypes := types.Scalar
	if s.simpleScalarTypes {
		scalarTypes = simpleScalarTypes
	}
	if s.types != nil {
		scalarTypes = s.types.scalarTypes
	}
	var typ *types.T
	for {
		typ = randgen.RandTypeFromSlice(s.rnd, scalarTypes)
		if (s.disableDecimals && typ.Family() == types.DecimalFamily) ||
			(s.disableOIDs && typ.Family() == types.OidFamily) {
			continue
		}
		break
	}
	return typ
}

// isScalarType returns true if t is a member of types.Scalar, or a user defined
// enum.
// Requires s.lock to be held.
func (s *Smither) isScalarType(t *types.T) bool {
	s.lock.AssertRHeld()
	scalarTypes := types.Scalar
	if s.simpleScalarTypes {
		scalarTypes = simpleScalarTypes
	}
	if s.types != nil {
		scalarTypes = s.types.scalarTypes
	}
	for i := range scalarTypes {
		if t.Equivalent(scalarTypes[i]) {
			return true
		}
	}
	return false
}

func (s *Smither) randType() *types.T {
	s.lock.RLock()
	defer s.lock.RUnlock()
	seedTypes := randgen.SeedTypes
	if s.types != nil {
		seedTypes = s.types.seedTypes
	}
	var typ *types.T
	for {
		typ = randgen.RandTypeFromSlice(s.rnd, seedTypes)
		if s.disableDecimals && typ.Family() == types.DecimalFamily ||
			(s.disableOIDs && typ.Family() == types.OidFamily) {
			continue
		}
		if s.postgres && typ.Identical(types.Name) {
			// Name type in CRDB doesn't match Postgres behavior. Exclude for tests
			// which compare CRDB behavior to Postgres.
			continue
		}
		if s.simpleScalarTypes && !isSimpleSeedType(typ) {
			continue
		}
		break
	}
	return typ
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
	udts        []*types.T
	udtNames    []tree.TypeName
	seedTypes   []*types.T
	scalarTypes []*types.T
}

// ResolveType implements the tree.TypeReferenceResolver interface.
func (s *Smither) ResolveType(
	_ context.Context, name *tree.UnresolvedObjectName,
) (*types.T, error) {
	key := tree.MakeSchemaQualifiedTypeName(name.Schema(), name.Object())
	for i, typeName := range s.types.udtNames {
		if typeName == key {
			return s.types.udts[i], nil
		}
	}
	return nil, errors.Newf("type name %s not found by smither", name.Object())
}

// ResolveTypeByOID implements the tree.TypeReferenceResolver interface.
func (s *Smither) ResolveTypeByOID(context.Context, oid.Oid) (*types.T, error) {
	return nil, errors.AssertionFailedf("smither cannot resolve types by OID")
}
