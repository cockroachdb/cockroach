// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descs

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/funcdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/tabledesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catid"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

// ByIDGetter looks up immutable descriptors by ID.
type ByIDGetter getterBase

// Descs looks up immutable descriptors by ID.
func (g ByIDGetter) Descs(ctx context.Context, ids []descpb.ID) ([]catalog.Descriptor, error) {
	ret := make([]catalog.Descriptor, len(ids))
	if err := getDescriptorsByID(ctx, g.Descriptors(), g.KV(), g.flags, ret, ids...); err != nil {
		return nil, err
	}
	return ret, nil
}

// Desc looks up an immutable descriptor by ID.
func (g ByIDGetter) Desc(ctx context.Context, id descpb.ID) (catalog.Descriptor, error) {
	var arr [1]catalog.Descriptor
	if err := getDescriptorsByID(ctx, g.Descriptors(), g.KV(), g.flags, arr[:], id); err != nil {
		return nil, err
	}
	return arr[0], nil
}

// Database looks up an immutable database descriptor by ID.
func (g ByIDGetter) Database(
	ctx context.Context, id descpb.ID,
) (catalog.DatabaseDescriptor, error) {
	desc, err := g.Desc(ctx, id)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil, sqlerrors.NewUndefinedDatabaseError(fmt.Sprintf("[%d]", id))
		}
		return nil, err
	}
	db, ok := desc.(catalog.DatabaseDescriptor)
	if !ok {
		return nil, sqlerrors.NewUndefinedDatabaseError(fmt.Sprintf("[%d]", id))
	}
	return db, nil
}

// Schema looks up an immutable schema descriptor by ID.
func (g ByIDGetter) Schema(ctx context.Context, id descpb.ID) (catalog.SchemaDescriptor, error) {
	desc, err := g.Desc(ctx, id)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil, sqlerrors.NewUndefinedSchemaError(fmt.Sprintf("[%d]", id))
		}
		return nil, err
	}
	sc, ok := desc.(catalog.SchemaDescriptor)
	if !ok {
		return nil, sqlerrors.NewUndefinedSchemaError(fmt.Sprintf("[%d]", id))
	}
	return sc, nil
}

// Table looks up an immutable table descriptor by ID.
func (g ByIDGetter) Table(ctx context.Context, id descpb.ID) (catalog.TableDescriptor, error) {
	desc, err := g.Desc(ctx, id)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil, sqlerrors.NewUndefinedRelationError(&tree.TableRef{TableID: int64(id)})
		}
		return nil, err
	}
	tbl, ok := desc.(catalog.TableDescriptor)
	if !ok {
		return nil, sqlerrors.NewUndefinedRelationError(&tree.TableRef{TableID: int64(id)})
	}
	return tbl, nil
}

// Type looks up an immutable type descriptor by ID.
func (g ByIDGetter) Type(ctx context.Context, id descpb.ID) (catalog.TypeDescriptor, error) {
	desc, err := g.Desc(ctx, id)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil, pgerror.Newf(
				pgcode.UndefinedObject, "type with ID %d does not exist", id)
		}
		return nil, err
	}
	switch t := desc.(type) {
	case catalog.TypeDescriptor:
		return t, nil
	case catalog.TableDescriptor:
		// A given table name can resolve to either a type descriptor or a table
		// descriptor, because every table descriptor also defines an implicit
		// record type with the same name as the table...
		if g.flags.isMutable {
			// ...except if the type descriptor needs to be mutable.
			// We don't have the capability of returning a mutable type
			// descriptor for a table's implicit record type.
			return nil, errors.Wrapf(ErrMutableTableImplicitType,
				"cannot modify table record type %q", t.GetName())
		}
		return typedesc.CreateImplicitRecordTypeFromTableDesc(t)
	}
	return nil, pgerror.Newf(
		pgcode.UndefinedObject, "type with ID %d does not exist", id)
}

// ErrMutableTableImplicitType indicates that a table implicit type was fetched
// as a mutable, which is not allowed.
var ErrMutableTableImplicitType = pgerror.Newf(pgcode.DependentObjectsStillExist, "table implicit type not mutable")

// Function looks up an immutable function descriptor by ID.
func (g ByIDGetter) Function(
	ctx context.Context, id descpb.ID,
) (catalog.FunctionDescriptor, error) {
	desc, err := g.Desc(ctx, id)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil, errors.Mark(
				pgerror.Newf(pgcode.UndefinedFunction, "function %d does not exist", id),
				tree.ErrRoutineUndefined,
			)
		}
		return nil, err
	}
	fn, ok := desc.(catalog.FunctionDescriptor)
	if !ok {
		return nil, errors.Mark(
			pgerror.Newf(pgcode.UndefinedFunction, "function %d does not exist", id),
			tree.ErrRoutineUndefined,
		)
	}
	return fn, nil
}

// MutableByIDGetter looks up mutable descriptors by ID.
type MutableByIDGetter getterBase

// AsByIDGetter returns this object as a ByIDGetter, which performs in
// exactly the same way except for the return types.
func (g MutableByIDGetter) AsByIDGetter() ByIDGetter {
	return ByIDGetter(g)
}

// Descs looks up mutable descriptors by ID.
func (g MutableByIDGetter) Descs(
	ctx context.Context, ids []descpb.ID,
) ([]catalog.MutableDescriptor, error) {
	descs, err := g.AsByIDGetter().Descs(ctx, ids)
	if err != nil {
		return nil, err
	}
	ret := make([]catalog.MutableDescriptor, len(descs))
	for i, desc := range descs {
		ret[i] = desc.(catalog.MutableDescriptor)
	}
	return ret, err
}

// Desc looks up a mutable descriptor by ID.
func (g MutableByIDGetter) Desc(
	ctx context.Context, id descpb.ID,
) (catalog.MutableDescriptor, error) {
	desc, err := g.AsByIDGetter().Desc(ctx, id)
	if err != nil {
		return nil, err
	}
	return desc.(catalog.MutableDescriptor), nil
}

// Database looks up a mutable database descriptor by ID.
func (g MutableByIDGetter) Database(ctx context.Context, id descpb.ID) (*dbdesc.Mutable, error) {
	db, err := g.AsByIDGetter().Database(ctx, id)
	if err != nil {
		return nil, err
	}
	return db.(*dbdesc.Mutable), nil
}

// Schema looks up a mutable schema descriptor by ID.
func (g MutableByIDGetter) Schema(ctx context.Context, id descpb.ID) (*schemadesc.Mutable, error) {
	sc, err := g.AsByIDGetter().Schema(ctx, id)
	if err != nil {
		return nil, err
	}
	return sc.(*schemadesc.Mutable), nil
}

// Table looks up a mutable table descriptor by ID.
func (g MutableByIDGetter) Table(ctx context.Context, id descpb.ID) (*tabledesc.Mutable, error) {
	tbl, err := g.AsByIDGetter().Table(ctx, id)
	if err != nil {
		return nil, err
	}
	return tbl.(*tabledesc.Mutable), nil
}

// Type looks up a mutable type descriptor by ID.
func (g MutableByIDGetter) Type(ctx context.Context, id descpb.ID) (*typedesc.Mutable, error) {
	typ, err := g.AsByIDGetter().Type(ctx, id)
	if err != nil {
		return nil, err
	}
	return typ.(*typedesc.Mutable), nil
}

// Function looks up a mutable function descriptor by ID.
func (g MutableByIDGetter) Function(ctx context.Context, id descpb.ID) (*funcdesc.Mutable, error) {
	fn, err := g.AsByIDGetter().Function(ctx, id)
	if err != nil {
		return nil, err
	}
	return fn.(*funcdesc.Mutable), nil
}

// ByNameGetter looks up immutable descriptors by name.
type ByNameGetter getterBase

// Database looks up an immutable database descriptor by name.
func (g ByNameGetter) Database(
	ctx context.Context, name string,
) (catalog.DatabaseDescriptor, error) {
	if name == "" {
		if g.flags.isOptional {
			return nil, nil
		}
		return nil, sqlerrors.ErrEmptyDatabaseName
	}
	desc, err := getDescriptorByName(
		ctx, g.KV(), g.Descriptors(), nil /* db */, nil /* sc */, name, g.flags, catalog.Database,
	)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		if g.flags.isOptional {
			return nil, nil
		}
		return nil, sqlerrors.NewUndefinedDatabaseError(name)
	}
	db, ok := desc.(catalog.DatabaseDescriptor)
	if !ok {
		if g.flags.isOptional {
			return nil, nil
		}
		return nil, sqlerrors.NewUndefinedDatabaseError(name)
	}
	return db, nil
}

// Schema looks up an immutable schema descriptor by name.
func (g ByNameGetter) Schema(
	ctx context.Context, db catalog.DatabaseDescriptor, name string,
) (catalog.SchemaDescriptor, error) {
	desc, err := getDescriptorByName(
		ctx, g.KV(), g.Descriptors(), db, nil /* sc */, name, g.flags, catalog.Schema,
	)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		if g.flags.isOptional {
			return nil, nil
		}
		return nil, sqlerrors.NewUndefinedSchemaError(name)
	}
	schema, ok := desc.(catalog.SchemaDescriptor)
	if !ok {
		if g.flags.isOptional {
			return nil, nil
		}
		return nil, sqlerrors.NewUndefinedSchemaError(name)
	}
	return schema, nil
}

// Table looks up an immutable table descriptor by name.
func (g ByNameGetter) Table(
	ctx context.Context, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor, name string,
) (catalog.TableDescriptor, error) {
	desc, err := getDescriptorByName(
		ctx, g.KV(), g.Descriptors(), db, sc, name, g.flags, catalog.Table,
	)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		if g.flags.isOptional {
			return nil, nil
		}
		tn := tree.MakeTableNameWithSchema(
			tree.Name(db.GetName()), tree.Name(sc.GetName()), tree.Name(name),
		)
		return nil, sqlerrors.NewUndefinedRelationError(&tn)
	}
	return catalog.AsTableDescriptor(desc)
}

// Type looks up an immutable type descriptor by name.
func (g ByNameGetter) Type(
	ctx context.Context, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor, name string,
) (catalog.TypeDescriptor, error) {
	desc, err := getDescriptorByName(ctx, g.KV(), g.Descriptors(), db, sc, name, g.flags, catalog.Any)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		if g.flags.isOptional {
			return nil, nil
		}
		tn := tree.MakeTableNameWithSchema(
			tree.Name(db.GetName()), tree.Name(sc.GetName()), tree.Name(name),
		)
		return nil, sqlerrors.NewUndefinedTypeError(&tn)
	}
	if tbl, ok := desc.(catalog.TableDescriptor); ok {
		// A given table name can resolve to either a type descriptor or a table
		// descriptor, because every table descriptor also defines an implicit
		// record type with the same name as the table...
		if g.flags.isMutable {
			// ...except if the type descriptor needs to be mutable.
			// We don't have the capability of returning a mutable type
			// descriptor for a table's implicit record type.
			return nil, pgerror.Newf(pgcode.InsufficientPrivilege,
				"cannot modify table record type %q", name)
		}
		return typedesc.CreateImplicitRecordTypeFromTableDesc(tbl)
	}
	return catalog.AsTypeDescriptor(desc)
}

// MutableByNameGetter looks up mutable descriptors by name.
type MutableByNameGetter getterBase

// AsByNameGetter returns this object as a ByNameGetter, which performs in
// exactly the same way except for the return types.
func (g MutableByNameGetter) AsByNameGetter() ByNameGetter {
	return ByNameGetter(g)
}

// Database looks up a mutable database descriptor by name.
func (g MutableByNameGetter) Database(ctx context.Context, name string) (*dbdesc.Mutable, error) {
	db, err := g.AsByNameGetter().Database(ctx, name)
	if err != nil || db == nil {
		return nil, err
	}
	return db.(*dbdesc.Mutable), nil
}

// Schema looks up a mutable schema descriptor by name.
func (g MutableByNameGetter) Schema(
	ctx context.Context, db catalog.DatabaseDescriptor, name string,
) (*schemadesc.Mutable, error) {
	sc, err := g.AsByNameGetter().Schema(ctx, db, name)
	if err != nil || sc == nil {
		return nil, err
	}
	return sc.(*schemadesc.Mutable), nil
}

// Table looks up a mutable table descriptor by name.
func (g MutableByNameGetter) Table(
	ctx context.Context, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor, name string,
) (*tabledesc.Mutable, error) {
	tbl, err := g.AsByNameGetter().Table(ctx, db, sc, name)
	if err != nil || tbl == nil {
		return nil, err
	}
	return tbl.(*tabledesc.Mutable), nil
}

// Type looks up a mutable type descriptor by name.
func (g MutableByNameGetter) Type(
	ctx context.Context, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor, name string,
) (*typedesc.Mutable, error) {
	typ, err := g.AsByNameGetter().Type(ctx, db, sc, name)
	if err != nil || typ == nil {
		return nil, err
	}
	return typ.(*typedesc.Mutable), nil
}

func makeGetterBase(txn *kv.Txn, col *Collection, flags getterFlags) getterBase {
	return getterBase{
		txnWrapper: txnWrapper{Txn: txn, Collection: col},
		flags:      flags,
	}
}

type getterBase struct {
	txnWrapper
	flags getterFlags
}

type txnWrapper struct {
	*kv.Txn
	*Collection
}

func (w txnWrapper) KV() *kv.Txn {
	return w.Txn
}

func (w txnWrapper) Descriptors() *Collection {
	return w.Collection
}

// getterFlags are the flags which power by-ID and by-name descriptor lookups
// in the Collection. The zero value of this struct is not a sane default.
//
// In any case, for historical reasons, some flags get overridden in
// inconsistent and sometimes bizarre ways depending on how the descriptors
// are looked up.
// TODO(postamar): clean up inconsistencies, enforce sane defaults.
type getterFlags struct {
	contextFlags
	layerFilters layerFilters
	descFilters  descFilters
}

type contextFlags struct {
	// isOptional specifies that the descriptor is being looked up on
	// a best-effort basis.
	isOptional bool
	// isMutable specifies that a mutable descriptor is to be returned.
	isMutable bool
}

type layerFilters struct {
	// withoutSynthetic specifies bypassing the synthetic descriptor layer.
	withoutSynthetic bool
	// withoutLeased specifies bypassing the leased descriptor layer.
	withoutLeased bool
	// withoutStorage specifies avoiding any queries to the KV layer.
	withoutStorage bool
	// withoutHydration specifies avoiding hydrating the descriptors.
	// This can be set to true only when looking up descriptors when hydrating
	// another group of descriptors. The purpose is to avoid potential infinite
	// recursion loop when trying to hydrate a descriptor which would lead to
	// the hydration of another descriptor which depends on it.
	// TODO(postamar): untangle the hydration mess
	withoutHydration bool
}

type descFilters struct {
	// withoutDropped specifies to raise an error if the looked-up descriptor
	// is in the DROP state.
	withoutDropped bool
	// withoutOffline specifies to raise an error if the looked-up descriptor
	// is in the OFFLINE state.
	withoutOffline bool
	// withoutCommittedAdding specifies if committed descriptors in the
	// adding state will be ignored.
	withoutCommittedAdding bool
	// maybeParentID specifies, when set, that the looked-up descriptor
	// should have the same parent ID, when set.
	maybeParentID descpb.ID
}

func defaultUnleasedFlags() (f getterFlags) {
	f.layerFilters.withoutLeased = true
	return f
}

// ByIDWithoutLeased returns a ByIDGetterBuilder set up to look up descriptors by ID
// in all layers except the leased descriptors layer. To opt in to the
// leased descriptors, use ByIDWithLeased instead.
func (tc *Collection) ByIDWithoutLeased(txn *kv.Txn) ByIDGetterBuilder {
	return ByIDGetterBuilder(makeGetterBase(txn, tc, getterFlags{
		layerFilters: layerFilters{
			withoutLeased: true,
		},
	}))
}

// ByIDWithLeased is like ByIDWithoutLeased but also looks up in the leased descriptors
// layer. This may save a round-trip to KV at the expense of the descriptor
// being slightly stale (one version off).
func (tc *Collection) ByIDWithLeased(txn *kv.Txn) ByIDGetterBuilder {
	return ByIDGetterBuilder(makeGetterBase(txn, tc, getterFlags{}))
}

// MutableByID returns a MutableByIDGetter.
// This convenience method exists because mutable lookups never require
// any customization.
func (tc *Collection) MutableByID(txn *kv.Txn) MutableByIDGetter {
	return MutableByIDGetter(makeGetterBase(txn, tc, getterFlags{
		contextFlags: contextFlags{
			isMutable: true,
		},
		layerFilters: layerFilters{
			withoutLeased: true,
		},
	}))
}

// ByIDGetterBuilder is a builder object for ByIDGetter and MutableByIDGetter.
type ByIDGetterBuilder getterBase

// WithoutSynthetic configures the ByIDGetterBuilder to bypass the synthetic
// layer. This is useful mainly for the declarative schema changer, which is
// the main client of this layer.
func (b ByIDGetterBuilder) WithoutSynthetic() ByIDGetterBuilder {
	b.flags.layerFilters.withoutSynthetic = true
	return b
}

// WithoutDropped configures the ByIDGetterBuilder to error on descriptors
// which are in a dropped state.
func (b ByIDGetterBuilder) WithoutDropped() ByIDGetterBuilder {
	b.flags.descFilters.withoutDropped = true
	return b
}

// WithoutOffline configures the ByIDGetterBuilder to error on descriptors
// which are in an offline state.
func (b ByIDGetterBuilder) WithoutOffline() ByIDGetterBuilder {
	b.flags.descFilters.withoutOffline = true
	return b
}

// WithoutNonPublic configures the ByIDGetterBuilder to error on descriptors
// which are not in a public state: dropped, offline, etc.
func (b ByIDGetterBuilder) WithoutNonPublic() ByIDGetterBuilder {
	b.flags.descFilters.withoutDropped = true
	b.flags.descFilters.withoutOffline = true
	return b
}

// WithoutOtherParent configures the ByIDGetterBuilder to error on descriptors
// which, if they have a parent ID, have one different than the argument.
// The argument must be non-zero for this filter to be effective.
func (b ByIDGetterBuilder) WithoutOtherParent(parentID catid.DescID) ByIDGetterBuilder {
	b.flags.descFilters.maybeParentID = parentID
	return b
}

// Get builds a ByIDGetter.
func (b ByIDGetterBuilder) Get() ByIDGetter {
	if b.flags.isMutable {
		b.flags.layerFilters.withoutLeased = true
		b.flags.isMutable = false
	}
	return ByIDGetter(b)
}

// ByName returns a ByNameGetterBuilder set up to look up
// descriptors by name in all layers except the leased descriptors layer.
// To opt in to the leased descriptors, use ByNameWithLeased instead.
func (tc *Collection) ByName(txn *kv.Txn) ByNameGetterBuilder {
	return ByNameGetterBuilder(makeGetterBase(txn, tc, getterFlags{
		layerFilters: layerFilters{
			withoutLeased: true,
		},
		descFilters: descFilters{
			withoutOffline: true,
			withoutDropped: true,
		},
	}))
}

// ByNameWithLeased is like ByName but also looks up in the
// leased descriptors layer. This may save a round-trip to KV at the expense
// of the descriptor being slightly stale (one version off).
func (tc *Collection) ByNameWithLeased(txn *kv.Txn) ByNameGetterBuilder {
	return ByNameGetterBuilder(makeGetterBase(txn, tc, getterFlags{
		descFilters: descFilters{
			withoutOffline: true,
			withoutDropped: true,
		},
	}))
}

// MutableByName returns a MutableByNameGetter.
// This convenience method exists because mutable lookups never require
// any customization.
func (tc *Collection) MutableByName(txn *kv.Txn) MutableByNameGetter {
	return MutableByNameGetter(makeGetterBase(txn, tc, getterFlags{
		contextFlags: contextFlags{
			isMutable: true,
		},
		layerFilters: layerFilters{
			withoutLeased: true,
		},
		descFilters: descFilters{
			withoutOffline: true,
			withoutDropped: true,
		},
	}))
}

// ByNameGetterBuilder is a builder object for ByNameGetter and MutableByNameGetter.
type ByNameGetterBuilder getterBase

// WithOffline configures the ByNameGetterBuilder to allow lookups
// of offline descriptors.
func (b ByNameGetterBuilder) WithOffline() ByNameGetterBuilder {
	b.flags.descFilters.withoutOffline = false
	return b
}

// Get builds a ByNameGetter.
func (b ByNameGetterBuilder) Get() ByNameGetter {
	return ByNameGetter(b)
}

// MaybeGet builds a ByNameGetter which returns a nil descriptor instead of
// an error when the descriptor is not found.
func (b ByNameGetterBuilder) MaybeGet() ByNameGetter {
	b.flags.contextFlags.isOptional = true
	return ByNameGetter(b)
}
