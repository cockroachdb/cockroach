// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlerrors"
	"github.com/cockroachdb/errors"
)

// ByIDGetter looks up immutable descriptors by ID.
type ByIDGetter interface {

	// Descs looks up immutable descriptors by ID.
	Descs(ctx context.Context, ids []descpb.ID) ([]catalog.Descriptor, error)

	// Desc looks up an immutable descriptor by ID.
	Desc(ctx context.Context, id descpb.ID) (catalog.Descriptor, error)

	// Database looks up an immutable database descriptor by ID.
	Database(ctx context.Context, id descpb.ID) (catalog.DatabaseDescriptor, error)

	// Schema looks up an immutable schema descriptor by ID.
	Schema(ctx context.Context, id descpb.ID) (catalog.SchemaDescriptor, error)

	// Table looks up an immutable table descriptor by ID.
	Table(ctx context.Context, id descpb.ID) (catalog.TableDescriptor, error)

	// Type looks up an immutable type descriptor by ID.
	Type(ctx context.Context, id descpb.ID) (catalog.TypeDescriptor, error)

	// Function looks up an immutable function descriptor by ID.
	Function(ctx context.Context, id descpb.ID) (catalog.FunctionDescriptor, error)
}

type byIDGetter struct {
	getterBase
}

func (tc *Collection) newByIDGetter(txn *kv.Txn, flags getterFlags) *byIDGetter {
	flags.context.isMutable = false
	return &byIDGetter{getterBase: makeGetterBase(txn, tc, flags)}
}

var _ ByIDGetter = &byIDGetter{}

// Descs is part of the ByIDGetter interface.
func (g byIDGetter) Descs(ctx context.Context, ids []descpb.ID) ([]catalog.Descriptor, error) {
	ret := make([]catalog.Descriptor, len(ids))
	if err := getDescriptorsByID(ctx, g.Descriptors(), g.KV(), g.flags, ret, ids...); err != nil {
		return nil, err
	}
	return ret, nil
}

// Desc is part of the ByIDGetter interface.
func (g byIDGetter) Desc(ctx context.Context, id descpb.ID) (catalog.Descriptor, error) {
	var arr [1]catalog.Descriptor
	if err := getDescriptorsByID(ctx, g.Descriptors(), g.KV(), g.flags, arr[:], id); err != nil {
		return nil, err
	}
	return arr[0], nil
}

// Database is part of the ByIDGetter interface.
func (g byIDGetter) Database(
	ctx context.Context, id descpb.ID,
) (catalog.DatabaseDescriptor, error) {
	desc, err := g.Desc(ctx, id)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			if g.flags.context.isOptional {
				return nil, nil
			}
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

// Schema is part of the ByIDGetter interface.
func (g byIDGetter) Schema(ctx context.Context, id descpb.ID) (catalog.SchemaDescriptor, error) {
	desc, err := g.Desc(ctx, id)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			if g.flags.context.isOptional {
				return nil, nil
			}
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

// Table is part of the ByIDGetter interface.
func (g byIDGetter) Table(ctx context.Context, id descpb.ID) (catalog.TableDescriptor, error) {
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

// Type is part of the ByIDGetter interface.
func (g byIDGetter) Type(ctx context.Context, id descpb.ID) (catalog.TypeDescriptor, error) {
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
		if g.flags.context.isMutable {
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

// Function is part of the ByIDGetter interface.
func (g byIDGetter) Function(
	ctx context.Context, id descpb.ID,
) (catalog.FunctionDescriptor, error) {
	desc, err := g.Desc(ctx, id)
	if err != nil {
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			return nil, errors.Wrapf(tree.ErrFunctionUndefined, "function %d does not exist", id)
		}
		return nil, err
	}
	fn, ok := desc.(catalog.FunctionDescriptor)
	if !ok {
		return nil, errors.Wrapf(tree.ErrFunctionUndefined, "function %d does not exist", id)
	}
	return fn, nil
}

// MutableByIDGetter looks up mutable descriptors by ID.
type MutableByIDGetter interface {

	// AsByIDGetter returns this object as a ByIDGetter, which performs in
	// exactly the same way except for the return types.
	AsByIDGetter() ByIDGetter

	// Descs looks up mutable descriptors by ID.
	Descs(ctx context.Context, ids []descpb.ID) ([]catalog.MutableDescriptor, error)

	// Desc looks up a mutable descriptor by ID.
	Desc(ctx context.Context, id descpb.ID) (catalog.MutableDescriptor, error)

	// Database looks up a mutable database descriptor by ID.
	Database(ctx context.Context, id descpb.ID) (*dbdesc.Mutable, error)

	// Schema looks up a mutable schema descriptor by ID.
	Schema(ctx context.Context, id descpb.ID) (*schemadesc.Mutable, error)

	// Table looks up a mutable table descriptor by ID.
	Table(ctx context.Context, id descpb.ID) (*tabledesc.Mutable, error)

	// Type looks up a mutable type descriptor by ID.
	Type(ctx context.Context, id descpb.ID) (*typedesc.Mutable, error)

	// Function looks up a mutable function descriptor by ID.
	Function(ctx context.Context, id descpb.ID) (*funcdesc.Mutable, error)
}

type mutableByIDGetter struct {
	*byIDGetter
}

func (tc *Collection) newMutableByIDGetter(txn *kv.Txn, flags getterFlags) *mutableByIDGetter {
	flags.context.isOptional = false
	flags.context.isMutable = true
	flags.layerFilters.withoutLeased = true
	flags.descFilters.withoutDropped = false
	flags.descFilters.withoutOffline = false
	return &mutableByIDGetter{byIDGetter: &byIDGetter{getterBase: makeGetterBase(txn, tc, flags)}}
}

var _ MutableByIDGetter = &mutableByIDGetter{}

// AsByIDGetter is part of the MutableByIDGetter interface.
func (g mutableByIDGetter) AsByIDGetter() ByIDGetter {
	return g.byIDGetter
}

// Descs is part of the MutableByIDGetter interface.
func (g mutableByIDGetter) Descs(
	ctx context.Context, ids []descpb.ID,
) ([]catalog.MutableDescriptor, error) {
	descs, err := g.byIDGetter.Descs(ctx, ids)
	if err != nil {
		return nil, err
	}
	ret := make([]catalog.MutableDescriptor, len(descs))
	for i, desc := range descs {
		ret[i] = desc.(catalog.MutableDescriptor)
	}
	return ret, err
}

// Desc is part of the MutableByIDGetter interface.
func (g mutableByIDGetter) Desc(
	ctx context.Context, id descpb.ID,
) (catalog.MutableDescriptor, error) {
	desc, err := g.byIDGetter.Desc(ctx, id)
	if err != nil {
		return nil, err
	}
	return desc.(catalog.MutableDescriptor), nil
}

// Database is part of the MutableByIDGetter interface.
func (g mutableByIDGetter) Database(ctx context.Context, id descpb.ID) (*dbdesc.Mutable, error) {
	db, err := g.byIDGetter.Database(ctx, id)
	if err != nil {
		return nil, err
	}
	return db.(*dbdesc.Mutable), nil
}

// Schema is part of the MutableByIDGetter interface.
func (g mutableByIDGetter) Schema(ctx context.Context, id descpb.ID) (*schemadesc.Mutable, error) {
	sc, err := g.byIDGetter.Schema(ctx, id)
	if err != nil {
		return nil, err
	}
	return sc.(*schemadesc.Mutable), nil
}

// Table is part of the MutableByIDGetter interface.
func (g mutableByIDGetter) Table(ctx context.Context, id descpb.ID) (*tabledesc.Mutable, error) {
	tbl, err := g.byIDGetter.Table(ctx, id)
	if err != nil {
		return nil, err
	}
	return tbl.(*tabledesc.Mutable), nil
}

// Type is part of the MutableByIDGetter interface.
func (g mutableByIDGetter) Type(ctx context.Context, id descpb.ID) (*typedesc.Mutable, error) {
	typ, err := g.byIDGetter.Type(ctx, id)
	if err != nil {
		return nil, err
	}
	return typ.(*typedesc.Mutable), nil
}

// Function is part of the MutableByIDGetter interface.
func (g mutableByIDGetter) Function(ctx context.Context, id descpb.ID) (*funcdesc.Mutable, error) {
	fn, err := g.byIDGetter.Function(ctx, id)
	if err != nil {
		return nil, err
	}
	return fn.(*funcdesc.Mutable), nil
}

// ByNameGetter looks up immutable descriptors by name.
type ByNameGetter interface {

	// Database looks up an immutable database descriptor by name.
	Database(ctx context.Context, name string) (catalog.DatabaseDescriptor, error)

	// Schema looks up an immutable schema descriptor by name.
	Schema(
		ctx context.Context, db catalog.DatabaseDescriptor, name string,
	) (catalog.SchemaDescriptor, error)

	// Table looks up an immutable table descriptor by name.
	Table(
		ctx context.Context, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor, name string,
	) (catalog.TableDescriptor, error)

	// Type looks up an immutable type descriptor by name.
	Type(
		ctx context.Context, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor, name string,
	) (catalog.TypeDescriptor, error)
}

type byNameGetter struct {
	getterBase
}

var _ ByNameGetter = &byNameGetter{}

func (tc *Collection) newByNameGetter(txn *kv.Txn, flags getterFlags) *byNameGetter {
	flags.context.isMutable = false
	flags.descFilters.withoutDropped = true
	return &byNameGetter{getterBase: makeGetterBase(txn, tc, flags)}
}

// Database is part of the ByNameGetter interface.
func (g byNameGetter) Database(
	ctx context.Context, name string,
) (catalog.DatabaseDescriptor, error) {
	desc, err := getDescriptorByName(
		ctx, g.KV(), g.Descriptors(), nil /* db */, nil /* sc */, name, g.flags, catalog.Database,
	)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		if g.flags.context.isOptional {
			return nil, nil
		}
		return nil, sqlerrors.NewUndefinedDatabaseError(name)
	}
	db, ok := desc.(catalog.DatabaseDescriptor)
	if !ok {
		if g.flags.context.isOptional {
			return nil, nil
		}
		return nil, sqlerrors.NewUndefinedDatabaseError(name)
	}
	return db, nil
}

// Schema is part of the ByNameGetter interface.
func (g byNameGetter) Schema(
	ctx context.Context, db catalog.DatabaseDescriptor, name string,
) (catalog.SchemaDescriptor, error) {
	desc, err := getDescriptorByName(
		ctx, g.KV(), g.Descriptors(), db, nil /* sc */, name, g.flags, catalog.Schema,
	)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		if g.flags.context.isOptional {
			return nil, nil
		}
		return nil, sqlerrors.NewUndefinedSchemaError(name)
	}
	schema, ok := desc.(catalog.SchemaDescriptor)
	if !ok {
		if g.flags.context.isOptional {
			return nil, nil
		}
		return nil, sqlerrors.NewUndefinedSchemaError(name)
	}
	return schema, nil
}

// Table is part of the ByNameGetter interface.
func (g byNameGetter) Table(
	ctx context.Context, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor, name string,
) (catalog.TableDescriptor, error) {
	desc, err := getDescriptorByName(
		ctx, g.KV(), g.Descriptors(), db, sc, name, g.flags, catalog.Table,
	)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		if g.flags.context.isOptional {
			return nil, nil
		}
		tn := tree.MakeTableNameWithSchema(
			tree.Name(db.GetName()), tree.Name(sc.GetName()), tree.Name(name),
		)
		return nil, sqlerrors.NewUndefinedRelationError(&tn)
	}
	return catalog.AsTableDescriptor(desc)
}

// Type is part of the ByNameGetter interface.
func (g byNameGetter) Type(
	ctx context.Context, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor, name string,
) (catalog.TypeDescriptor, error) {
	desc, err := getDescriptorByName(ctx, g.KV(), g.Descriptors(), db, sc, name, g.flags, catalog.Any)
	if err != nil {
		return nil, err
	}
	if desc == nil {
		if g.flags.context.isOptional {
			return nil, nil
		}
		tn := tree.MakeTableNameWithSchema(
			tree.Name(db.GetName()), tree.Name(sc.GetName()), tree.Name(name),
		)
		return nil, sqlerrors.NewUndefinedRelationError(&tn)
	}
	if tbl, ok := desc.(catalog.TableDescriptor); ok {
		// A given table name can resolve to either a type descriptor or a table
		// descriptor, because every table descriptor also defines an implicit
		// record type with the same name as the table...
		if g.flags.context.isMutable {
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
type MutableByNameGetter interface {

	// AsByNameGetter returns this object as a ByNameGetter, which performs in
	// exactly the same way except for the return types.
	AsByNameGetter() ByNameGetter

	// Database looks up a mutable database descriptor by name.
	Database(ctx context.Context, name string) (*dbdesc.Mutable, error)

	// Schema looks up a mutable schema descriptor by name.
	Schema(
		ctx context.Context, db catalog.DatabaseDescriptor, name string,
	) (*schemadesc.Mutable, error)

	// Table looks up a mutable table descriptor by name.
	Table(
		ctx context.Context, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor, name string,
	) (*tabledesc.Mutable, error)

	// Type looks up a mutable type descriptor by name.
	Type(
		ctx context.Context, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor, name string,
	) (*typedesc.Mutable, error)
}

type mutableByNameGetter struct {
	*byNameGetter
}

func (tc *Collection) newMutableByNameGetter(txn *kv.Txn, flags getterFlags) *mutableByNameGetter {
	flags.context.isMutable = true
	flags.layerFilters.withoutLeased = true
	flags.descFilters.withoutDropped = true
	return &mutableByNameGetter{byNameGetter: &byNameGetter{getterBase: makeGetterBase(txn, tc, flags)}}
}

var _ MutableByNameGetter = &mutableByNameGetter{}

// AsByNameGetter is part of the MutableByNameGetter interface.
func (g mutableByNameGetter) AsByNameGetter() ByNameGetter {
	return g.byNameGetter
}

// Database is part of the MutableByNameGetter interface.
func (g mutableByNameGetter) Database(ctx context.Context, name string) (*dbdesc.Mutable, error) {
	db, err := g.byNameGetter.Database(ctx, name)
	if err != nil || db == nil {
		return nil, err
	}
	return db.(*dbdesc.Mutable), nil
}

// Schema is part of the MutableByNameGetter interface.
func (g mutableByNameGetter) Schema(
	ctx context.Context, db catalog.DatabaseDescriptor, name string,
) (*schemadesc.Mutable, error) {
	sc, err := g.byNameGetter.Schema(ctx, db, name)
	if err != nil || sc == nil {
		return nil, err
	}
	return sc.(*schemadesc.Mutable), nil
}

// Table is part of the MutableByNameGetter interface.
func (g mutableByNameGetter) Table(
	ctx context.Context, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor, name string,
) (*tabledesc.Mutable, error) {
	tbl, err := g.byNameGetter.Table(ctx, db, sc, name)
	if err != nil || tbl == nil {
		return nil, err
	}
	return tbl.(*tabledesc.Mutable), nil
}

// Type is part of the MutableByNameGetter interface.
func (g mutableByNameGetter) Type(
	ctx context.Context, db catalog.DatabaseDescriptor, sc catalog.SchemaDescriptor, name string,
) (*typedesc.Mutable, error) {
	typ, err := g.byNameGetter.Type(ctx, db, sc, name)
	if err != nil || typ == nil {
		return nil, err
	}
	return typ.(*typedesc.Mutable), nil
}

func makeGetterBase(txn *kv.Txn, col *Collection, flags getterFlags) getterBase {
	return getterBase{
		txn:   &txnWrapper{Txn: txn, Collection: col},
		flags: flags,
	}
}

type getterBase struct {
	txn
	flags getterFlags
}

type (
	txn interface {
		KV() *kv.Txn
		Descriptors() *Collection
	}
	txnWrapper struct {
		*kv.Txn
		*Collection
	}
)

var _ txn = &txnWrapper{}

func (w *txnWrapper) KV() *kv.Txn {
	return w.Txn
}

func (w *txnWrapper) Descriptors() *Collection {
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
	context      contextFlags
	layerFilters layerFilters
	descFilters  descFilters
}

type contextFlags struct {
	// isOptional specifies that the descriptor is being looked up on
	// a best-effort basis.
	//
	// Presently, for historical reasons, this is overridden to true for
	// all mutable by-ID lookups, and for all immutable by-ID object lookups.
	// TODO(postamar): clean this up
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
}

type descFilters struct {
	// withoutDropped specifies to raise an error if the looked-up descriptor
	// is in the DROP state.
	//
	// Presently, for historical reasons, this is overridden everywhere except
	// for immutable by-ID lookups: to true for by-name lookups and to false for
	// mutable by-ID lookups.
	// TODO(postamar): clean this up
	withoutDropped bool
	// withoutOffline specifies to raise an error if the looked-up descriptor
	// is in the OFFLINE state.
	//
	// Presently, for historical reasons, this is overridden to true for mutable
	// by-ID lookups.
	// TODO(postamar): clean this up
	withoutOffline bool
	// withoutCommittedAdding specifies if committed descriptors in the
	// adding state will be ignored.
	withoutCommittedAdding bool
	// maybeParentID specifies, when set, that the looked-up descriptor
	// should have the same parent ID, when set.
	maybeParentID descpb.ID
}

func fromCommonFlags(flags tree.CommonLookupFlags) (f getterFlags) {
	return getterFlags{context: contextFlags{
		isOptional: !flags.Required,
		isMutable:  flags.RequireMutable,
	},
		layerFilters: layerFilters{
			withoutSynthetic: flags.AvoidSynthetic,
			withoutLeased:    flags.AvoidLeased,
		},
		descFilters: descFilters{
			withoutDropped: !flags.IncludeDropped,
			withoutOffline: !flags.IncludeOffline,
			maybeParentID:  flags.ParentID,
		},
	}
}

func fromObjectFlags(flags tree.ObjectLookupFlags) getterFlags {
	return fromCommonFlags(flags.CommonLookupFlags)
}

func defaultFlags() getterFlags {
	return fromCommonFlags(tree.CommonLookupFlags{})
}

func defaultUnleasedFlags() (f getterFlags) {
	f.layerFilters.withoutLeased = true
	return f
}

// ByID returns a ByIDGetterBuilder.
func (tc *Collection) ByID(txn *kv.Txn) ByIDGetterBuilder {
	return ByIDGetterBuilder{gb: makeGetterBase(txn, tc, defaultFlags())}
}

// ByIDGetterBuilder is a builder object for ByIDGetter and MutableByIDGetter.
type ByIDGetterBuilder struct {
	gb getterBase
}

// WithFlags configures the ByIDGetterBuilder with the given flags.
func (b ByIDGetterBuilder) WithFlags(flags tree.CommonLookupFlags) ByIDGetterBuilder {
	gb := b.gb
	gb.flags = fromCommonFlags(flags)
	return ByIDGetterBuilder{gb: gb}
}

// WithObjFlags configures the ByIDGetterBuilder with the given object flags.
func (b ByIDGetterBuilder) WithObjFlags(flags tree.ObjectLookupFlags) ByIDGetterBuilder {
	gb := b.gb
	gb.flags = fromObjectFlags(flags)
	return ByIDGetterBuilder{gb: gb}
}

// Mutable builds a MutableByIDGetter.
func (b ByIDGetterBuilder) Mutable() MutableByIDGetter {
	return b.gb.txn.Descriptors().newMutableByIDGetter(b.gb.txn.KV(), b.gb.flags)
}

// Immutable builds a ByIDGetter.
func (b ByIDGetterBuilder) Immutable() ByIDGetter {
	f := b.gb.flags
	f.layerFilters.withoutLeased = f.layerFilters.withoutLeased || f.context.isMutable
	return b.gb.txn.Descriptors().newByIDGetter(b.gb.txn.KV(), b.gb.flags)
}

// ByName returns a ByNameGetterBuilder.
func (tc *Collection) ByName(txn *kv.Txn) ByNameGetterBuilder {
	return ByNameGetterBuilder{gb: makeGetterBase(txn, tc, defaultFlags())}
}

// ByNameGetterBuilder is a builder object for ByNameGetter and MutableByNameGetter.
type ByNameGetterBuilder struct {
	gb getterBase
}

// WithFlags configures the ByIDGetterBuilder with the given flags.
func (b ByNameGetterBuilder) WithFlags(flags tree.CommonLookupFlags) ByNameGetterBuilder {
	gb := b.gb
	gb.flags = fromCommonFlags(flags)
	return ByNameGetterBuilder{gb: gb}
}

// WithObjFlags configures the ByNameGetterBuilder with the given object flags.
func (b ByNameGetterBuilder) WithObjFlags(flags tree.ObjectLookupFlags) ByNameGetterBuilder {
	gb := b.gb
	gb.flags = fromObjectFlags(flags)
	return ByNameGetterBuilder{gb: gb}
}

// Mutable builds a MutableByNameGetter.
func (b ByNameGetterBuilder) Mutable() MutableByNameGetter {
	return b.gb.txn.Descriptors().newMutableByNameGetter(b.gb.txn.KV(), b.gb.flags)
}

// Immutable builds a ByNameGetter.
func (b ByNameGetterBuilder) Immutable() ByNameGetter {
	f := b.gb.flags
	f.layerFilters.withoutLeased = f.layerFilters.withoutLeased || f.context.isMutable
	return b.gb.txn.Descriptors().newByNameGetter(b.gb.txn.KV(), f)
}
