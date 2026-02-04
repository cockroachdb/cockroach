// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/errors"
	"github.com/lib/pq/oid"
)

// DistSQLTypeResolver is a TypeResolver that accesses TypeDescriptors through
// a given descs.Collection and transaction.
type DistSQLTypeResolver struct {
	g ByIDGetter
}

// NewDistSQLTypeResolver creates a new DistSQLTypeResolver.
func NewDistSQLTypeResolver(descs *Collection, txn *kv.Txn) DistSQLTypeResolver {
	return DistSQLTypeResolver{
		g: descs.ByIDWithLeased(txn).Get(),
	}
}

// ResolveType implements the tree.TypeReferenceResolver interface.
func (dt *DistSQLTypeResolver) ResolveType(
	context.Context, *tree.UnresolvedObjectName,
) (*types.T, error) {
	return nil, errors.AssertionFailedf("cannot resolve types in DistSQL by name")
}

// ResolveTypeByOID implements the tree.TypeReferenceResolver interface.
func (dt *DistSQLTypeResolver) ResolveTypeByOID(
	ctx context.Context, oid oid.Oid,
) (*types.T, error) {
	return typedesc.ResolveHydratedTByOID(ctx, oid, dt)
}

// GetTypeDescriptor implements the catalog.TypeDescriptorResolver interface.
func (dt *DistSQLTypeResolver) GetTypeDescriptor(
	ctx context.Context, id descpb.ID,
) (tree.TypeName, catalog.TypeDescriptor, error) {
	desc, err := dt.g.Desc(ctx, id)
	if err != nil {
		return tree.TypeName{}, nil, err
	}

	// Get the schema name for proper qualification. This ensures type names
	// include the schema qualifier (e.g., "public.greeting" instead of just
	// "greeting") which is important for error messages and display.
	var name tree.TypeName
	if parentSchemaID := desc.GetParentSchemaID(); parentSchemaID != descpb.InvalidID {
		schemaDesc, err := dt.g.Desc(ctx, parentSchemaID)
		if err != nil {
			// If the schema descriptor isn't found, it may have been dropped or this
			// is an inconsistent state. Fall back to unqualified name rather than
			// failing entirely, since the type name is often used in error messages.
			if !errors.Is(err, catalog.ErrDescriptorNotFound) {
				// For non-"not found" errors, propagate them.
				return tree.TypeName{}, nil, err
			}
			name = tree.MakeUnqualifiedTypeName(desc.GetName())
		} else {
			name = tree.MakeSchemaQualifiedTypeName(schemaDesc.GetName(), desc.GetName())
		}
	} else {
		name = tree.MakeUnqualifiedTypeName(desc.GetName())
	}

	switch t := desc.(type) {
	case catalog.TypeDescriptor:
		// User-defined type.
		return name, t, nil
	case catalog.TableDescriptor:
		typ, err := typedesc.CreateImplicitRecordTypeFromTableDesc(t)
		if err != nil {
			return tree.TypeName{}, nil, err
		}
		return name, typ, nil
	default:
		return tree.TypeName{}, nil, pgerror.Newf(pgcode.WrongObjectType,
			"descriptor %d is a %s not a %s", id, t.DescriptorType(), catalog.Type)
	}
}

// HydrateTypeSlice installs metadata into a slice of types.T's.
func (dt *DistSQLTypeResolver) HydrateTypeSlice(ctx context.Context, typs []*types.T) error {
	for _, t := range typs {
		if err := typedesc.EnsureTypeIsHydrated(ctx, t, dt); err != nil {
			return err
		}
	}
	return nil
}
