// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scbuild

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scbuild/scbuildctx"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

var _ scbuildctx.NameResolver = buildCtx{}

// ResolveDatabase implements the NameResolver interface.
func (b buildCtx) ResolveDatabase(
	ctx context.Context, name tree.Name, p scbuildctx.ResolveParams,
) catalog.DatabaseDescriptor {
	db := b.CatalogReader().MayResolveDatabase(ctx, name)
	if db == nil {
		if p.IsExistenceOptional {
			return nil
		}
		panic(sqlerrors.NewUndefinedDatabaseError(name.String()))
	}
	if err := b.AuthorizationAccessor().CheckPrivilege(ctx, db, p.RequiredPrivilege); err != nil {
		panic(err)
	}
	return db
}

// ResolveSchema implements the NameResolver interface.
func (b buildCtx) ResolveSchema(
	ctx context.Context, name tree.ObjectNamePrefix, p scbuildctx.ResolveParams,
) (catalog.DatabaseDescriptor, catalog.SchemaDescriptor) {
	db, sc := b.CatalogReader().MayResolveSchema(ctx, name)
	if sc == nil {
		if p.IsExistenceOptional {
			return db, nil
		}
		panic(sqlerrors.NewUndefinedSchemaError(name.String()))
	}
	switch sc.SchemaKind() {
	case catalog.SchemaPublic, catalog.SchemaVirtual, catalog.SchemaTemporary:
		panic(pgerror.Newf(pgcode.InsufficientPrivilege,
			"%s permission denied for schema %q", p.RequiredPrivilege.String(), name))
	case catalog.SchemaUserDefined:
		b.MustOwn(ctx, sc)
	default:
		panic(errors.AssertionFailedf("unknown schema kind %d", sc.SchemaKind()))
	}
	return db, sc
}

// ResolveType implements the NameResolver interface.
func (b buildCtx) ResolveType(
	ctx context.Context, name *tree.UnresolvedObjectName, p scbuildctx.ResolveParams,
) (catalog.ResolvedObjectPrefix, catalog.TypeDescriptor) {
	prefix, typ := b.CatalogReader().MayResolveType(ctx, *name)
	if typ == nil {
		if p.IsExistenceOptional {
			return prefix, nil
		}
		panic(sqlerrors.NewUndefinedTypeError(name))
	}
	switch typ.GetKind() {
	case descpb.TypeDescriptor_ALIAS:
		// The implicit array types are not directly modifiable.
		panic(pgerror.Newf(pgcode.DependentObjectsStillExist,
			"%q is an implicit array type and cannot be modified", typ.GetName()))
	case descpb.TypeDescriptor_MULTIREGION_ENUM:
		// Multi-region enums are not directly modifiable.
		panic(errors.WithHintf(
			pgerror.Newf(pgcode.DependentObjectsStillExist,
				"%q is a multi-region enum and cannot be modified directly", typ.GetName()),
			"try ALTER DATABASE %s DROP REGION %s", prefix.Database.GetName(), typ.GetName()))
	case descpb.TypeDescriptor_ENUM:
		b.MustOwn(ctx, typ)
	case descpb.TypeDescriptor_TABLE_IMPLICIT_RECORD_TYPE:
		// Implicit record types are not directly modifiable.
		panic(pgerror.Newf(
			pgcode.DependentObjectsStillExist,
			"cannot drop type %q because table %q requires it",
			typ.GetName(), typ.GetName()))
	default:
		panic(errors.AssertionFailedf("unknown type kind %s", typ.GetKind()))
	}
	return prefix, typ
}

// ResolveRelation implements the NameResolver interface.
func (b buildCtx) ResolveRelation(
	ctx context.Context, name *tree.UnresolvedObjectName, p scbuildctx.ResolveParams,
) (catalog.ResolvedObjectPrefix, catalog.TableDescriptor) {
	prefix, rel := b.CatalogReader().MayResolveTable(ctx, *name)
	if rel == nil {
		if p.IsExistenceOptional {
			return prefix, nil
		}
		panic(sqlerrors.NewUndefinedRelationError(name))
	}
	if err := b.AuthorizationAccessor().CheckPrivilege(ctx, rel, p.RequiredPrivilege); err != nil {
		panic(err)
	}
	return prefix, rel
}

// ResolveTable implements the NameResolver interface.
func (b buildCtx) ResolveTable(
	ctx context.Context, name *tree.UnresolvedObjectName, p scbuildctx.ResolveParams,
) (catalog.ResolvedObjectPrefix, catalog.TableDescriptor) {
	prefix, rel := b.ResolveRelation(ctx, name, p)
	if rel == nil {
		return prefix, nil
	}
	if !rel.IsTable() {
		panic(pgerror.Newf(pgcode.WrongObjectType, "%q is not a table", rel.GetName()))
	}
	return prefix, rel
}

// ResolveSequence implements the NameResolver interface.
func (b buildCtx) ResolveSequence(
	ctx context.Context, name *tree.UnresolvedObjectName, p scbuildctx.ResolveParams,
) (catalog.ResolvedObjectPrefix, catalog.TableDescriptor) {
	prefix, rel := b.ResolveRelation(ctx, name, p)
	if rel == nil {
		return prefix, nil
	}
	if !rel.IsSequence() {
		panic(pgerror.Newf(pgcode.WrongObjectType, "%q is not a sequence", rel.GetName()))
	}
	return prefix, rel
}

// ResolveView implements the NameResolver interface.
func (b buildCtx) ResolveView(
	ctx context.Context, name *tree.UnresolvedObjectName, p scbuildctx.ResolveParams,
) (catalog.ResolvedObjectPrefix, catalog.TableDescriptor) {
	prefix, rel := b.ResolveRelation(ctx, name, p)
	if rel == nil {
		return prefix, nil
	}
	if !rel.IsView() {
		panic(pgerror.Newf(pgcode.WrongObjectType, "%q is not a view", rel.GetName()))
	}
	return prefix, rel
}

// ResolveIndex implements the NameResolver interface.
func (b buildCtx) ResolveIndex(
	ctx context.Context,
	relationName *tree.UnresolvedObjectName,
	indexName tree.Name,
	p scbuildctx.ResolveParams,
) (catalog.ResolvedObjectPrefix, catalog.TableDescriptor, catalog.Index) {
	prefix, rel := b.ResolveRelation(ctx, relationName, scbuildctx.ResolveParams{
		IsExistenceOptional: false,
		RequiredPrivilege:   p.RequiredPrivilege,
	})
	if !rel.IsPhysicalTable() || rel.IsSequence() {
		panic(pgerror.Newf(pgcode.WrongObjectType, "%q is not an indexable table or a materialized view", rel.GetName()))
	}
	idx, _ := rel.FindIndexWithName(indexName.String())
	if idx == nil {
		if indexName == "" || indexName == tabledesc.LegacyPrimaryKeyIndexName {
			// Fallback to primary index
			return prefix, rel, rel.GetPrimaryIndex()
		}
		if p.IsExistenceOptional {
			return prefix, rel, nil
		}
		panic(pgerror.Newf(pgcode.UndefinedObject, "index %q not found in relation %q", indexName, rel.GetName()))
	}
	return prefix, rel, idx
}
