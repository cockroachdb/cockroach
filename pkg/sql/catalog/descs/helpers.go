// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

// GetObjectName fetches the full name for the given table or type descriptor.
func GetObjectName(
	ctx context.Context, txn *kv.Txn, tc *Collection, obj catalog.Descriptor,
) (tree.ObjectName, error) {
	g := tc.ByIDWithLeased(txn).Get()
	sc, err := g.Schema(ctx, obj.GetParentSchemaID())
	if err != nil {
		return nil, err
	}
	db, err := g.Database(ctx, obj.GetParentID())
	if err != nil {
		return nil, err
	}
	tn := tree.NewTableNameWithSchema(
		tree.Name(db.GetName()), tree.Name(sc.GetName()), tree.Name(obj.GetName()),
	)
	return tn, nil
}

// GetDescriptorCollidingWithObjectName returns the descriptor which collides
// with the desired name if it exists.
func GetDescriptorCollidingWithObjectName(
	ctx context.Context, tc *Collection, txn *kv.Txn, parentID, parentSchemaID descpb.ID, name string,
) (catalog.Descriptor, error) {
	id, err := tc.LookupObjectID(ctx, txn, parentID, parentSchemaID, name)
	if err != nil || id == descpb.InvalidID {
		return nil, err
	}
	// At this point the ID is already in use by another object.
	desc, err := ByIDGetter(makeGetterBase(txn, tc, defaultUnleasedFlags())).Desc(ctx, id)
	if errors.Is(err, catalog.ErrDescriptorNotFound) {
		// Since the ID exists the descriptor should absolutely exist.
		err = errors.NewAssertionErrorWithWrappedErrf(err,
			"parentID=%d parentSchemaID=%d name=%q has ID=%d",
			parentID, parentSchemaID, name, id)
	}
	return desc, err
}

// CheckObjectNameCollision returns an error if the object name is already used.
func CheckObjectNameCollision(
	ctx context.Context,
	tc *Collection,
	txn *kv.Txn,
	parentID, parentSchemaID descpb.ID,
	name tree.ObjectName,
) error {
	d, err := GetDescriptorCollidingWithObjectName(ctx, tc, txn, parentID, parentSchemaID, name.Object())
	if err != nil || d == nil {
		return err
	}
	maybeQualifiedName := name.Object()
	if name.Catalog() != "" && name.Schema() != "" {
		maybeQualifiedName = name.FQString()
	}
	return sqlerrors.MakeObjectAlreadyExistsError(d.DescriptorProto(), maybeQualifiedName)
}

func getObjectPrefix(
	ctx context.Context, g ByNameGetter, dbName, scName string,
) (prefix catalog.ResolvedObjectPrefix, err error) {
	if g.flags.isMutable {
		g.flags.layerFilters.withoutLeased = true
	}
	// If we're reading the object descriptor from the store,
	// we should read its parents from the store too to ensure
	// that subsequent name resolution finds the latest name
	// in the face of a concurrent rename.
	if dbName != "" {
		prefix.Database, err = g.Database(ctx, dbName)
		if err != nil || prefix.Database == nil {
			return prefix, err
		}
	}
	prefix.Schema, err = g.Schema(ctx, prefix.Database, scName)
	return prefix, err
}

// PrefixAndType looks up an immutable type descriptor by its full name.
func PrefixAndType(
	ctx context.Context, g ByNameGetter, name *tree.TypeName,
) (catalog.ResolvedObjectPrefix, catalog.TypeDescriptor, error) {
	p, err := getObjectPrefix(ctx, g, name.Catalog(), name.Schema())
	if err != nil || p.Schema == nil {
		return p, nil, err
	}
	typ, err := g.Type(ctx, p.Database, p.Schema, name.Object())
	return p, typ, err
}

// PrefixAndMutableType looks up a mutable type descriptor by its full name.
func PrefixAndMutableType(
	ctx context.Context, g MutableByNameGetter, name *tree.TypeName,
) (catalog.ResolvedObjectPrefix, *typedesc.Mutable, error) {
	p, err := getObjectPrefix(ctx, ByNameGetter(g), name.Catalog(), name.Schema())
	if err != nil || p.Schema == nil {
		return p, nil, err
	}
	typ, err := g.Type(ctx, p.Database, p.Schema, name.Object())
	return p, typ, err
}

// PrefixAndTable looks up an immutable table descriptor by its full name.
func PrefixAndTable(
	ctx context.Context, g ByNameGetter, name *tree.TableName,
) (catalog.ResolvedObjectPrefix, catalog.TableDescriptor, error) {
	p, err := getObjectPrefix(ctx, g, name.Catalog(), name.Schema())
	if err != nil || p.Schema == nil {
		return p, nil, err
	}
	tbl, err := g.Table(ctx, p.Database, p.Schema, name.Object())
	return p, tbl, err
}

// PrefixAndMutableTable looks up a mutable table descriptor by its full name.
func PrefixAndMutableTable(
	ctx context.Context, g MutableByNameGetter, name *tree.TableName,
) (catalog.ResolvedObjectPrefix, *tabledesc.Mutable, error) {
	p, err := getObjectPrefix(ctx, ByNameGetter(g), name.Catalog(), name.Schema())
	if err != nil || p.Schema == nil {
		return p, nil, err
	}
	tbl, err := g.Table(ctx, p.Database, p.Schema, name.Object())
	return p, tbl, err
}

// AsZoneConfigHydrationHelper returns the collection as a
// catalog.ZoneConfigHydrationHelper
func AsZoneConfigHydrationHelper(tc *Collection) catalog.ZoneConfigHydrationHelper {
	return &zcHelper{Collection: tc}
}

type zcHelper struct {
	*Collection
}

var _ catalog.ZoneConfigHydrationHelper = &zcHelper{}

// MaybeGetTable implements the catalog.ZoneConfigHydrationHelper interface.
func (tc *zcHelper) MaybeGetTable(
	ctx context.Context, txn *kv.Txn, id descpb.ID,
) (catalog.TableDescriptor, error) {
	// Ignore IDs without a descriptor.
	if id == keys.RootNamespaceID || keys.IsPseudoTableID(uint32(id)) {
		return nil, nil
	}
	g := ByIDGetter(makeGetterBase(txn, tc.Collection, defaultUnleasedFlags()))
	desc, err := g.Desc(ctx, id)
	if err != nil {
		return nil, err
	}
	if desc.DescriptorType() == catalog.Table {
		return desc.(catalog.TableDescriptor), nil
	}
	return nil, nil
}

// MaybeGetZoneConfig implements the catalog.ZoneConfigHydrationHelper
// interface.
func (tc *zcHelper) MaybeGetZoneConfig(
	ctx context.Context, txn *kv.Txn, id descpb.ID,
) (catalog.ZoneConfig, error) {
	return tc.GetZoneConfig(ctx, txn, id)
}
