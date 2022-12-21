// Copyright 2021 The Cockroach Authors.
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
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/internal/validate"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// GetMutableDescriptorsByID returns a mutable implementation of the descriptors
// with the requested ids. An error is returned if no descriptor exists.
func (tc *Collection) GetMutableDescriptorsByID(
	ctx context.Context, txn *kv.Txn, ids ...descpb.ID,
) ([]catalog.MutableDescriptor, error) {
	flags := tree.CommonLookupFlags{
		RequireMutable: true,
		IncludeOffline: true,
		IncludeDropped: true,
	}
	descs, err := tc.getDescriptorsByID(ctx, txn, flags, ids...)
	if err != nil {
		return nil, err
	}
	ret := make([]catalog.MutableDescriptor, len(descs))
	for i, desc := range descs {
		ret[i] = desc.(catalog.MutableDescriptor)
	}
	return ret, nil
}

// GetMutableDescriptorByID delegates to GetMutableDescriptorsByID.
func (tc *Collection) GetMutableDescriptorByID(
	ctx context.Context, txn *kv.Txn, id descpb.ID,
) (catalog.MutableDescriptor, error) {
	descs, err := tc.GetMutableDescriptorsByID(ctx, txn, id)
	if err != nil {
		return nil, err
	}
	return descs[0], nil
}

// GetImmutableDescriptorsByID returns an immutable implementation of the
// descriptors with the requested ids. An error is returned if no descriptor
// exists, regardless of whether the Required flag is set or not.
func (tc *Collection) GetImmutableDescriptorsByID(
	ctx context.Context, txn *kv.Txn, flags tree.CommonLookupFlags, ids ...descpb.ID,
) ([]catalog.Descriptor, error) {
	flags.RequireMutable = false
	return tc.getDescriptorsByID(ctx, txn, flags, ids...)
}

// GetImmutableDescriptorByID delegates to GetImmutableDescriptorsByID.
func (tc *Collection) GetImmutableDescriptorByID(
	ctx context.Context, txn *kv.Txn, id descpb.ID, flags tree.CommonLookupFlags,
) (catalog.Descriptor, error) {
	flags.RequireMutable = false
	return tc.getDescriptorByID(ctx, txn, flags, id)
}

// getDescriptorsByID returns a descriptor by ID according to the provided
// lookup flags.
//
// The Required flag is ignored and always overridden.
func (tc *Collection) getDescriptorByID(
	ctx context.Context, txn *kv.Txn, flags tree.CommonLookupFlags, ids ...descpb.ID,
) (catalog.Descriptor, error) {
	var arr [1]catalog.Descriptor
	if err := getDescriptorsByID(
		ctx, tc, txn, flags, arr[:], ids...,
	); err != nil {
		return nil, err
	}
	return arr[0], nil
}

// getDescriptorsByID returns a slice of descriptors by ID according to the
// provided lookup flags.
//
// The Required flag is ignored and always overridden.
func (tc *Collection) getDescriptorsByID(
	ctx context.Context, txn *kv.Txn, flags tree.CommonLookupFlags, ids ...descpb.ID,
) ([]catalog.Descriptor, error) {
	descs := make([]catalog.Descriptor, len(ids))
	if err := getDescriptorsByID(
		ctx, tc, txn, flags, descs, ids...,
	); err != nil {
		return nil, err
	}
	return descs, nil
}

// getDescriptorsByID implements the Collection method of the same name.
// It takes a slice into which the retrieved descriptors will be stored.
// That slice must be the same length as the ids. This allows callers
// seeking to get just one descriptor to avoid an allocation by using a
// fixed-size array.
func getDescriptorsByID(
	ctx context.Context,
	tc *Collection,
	txn *kv.Txn,
	flags tree.CommonLookupFlags,
	descs []catalog.Descriptor,
	ids ...descpb.ID,
) (err error) {
	// Override flags.
	flags.Required = true
	if log.ExpensiveLogEnabled(ctx, 2) {
		// Copy the ids to a new slice to prevent the backing array from
		// escaping and forcing IDs to escape on this hot path.
		idsForLog := append(make([]descpb.ID, 0, len(ids)), ids...)
		log.VEventf(ctx, 2, "looking up descriptors for ids %v", idsForLog)
	}

	// We want to avoid the allocation in the case that there is exactly one
	// descriptor to resolve. This is the common case. The array stays on the
	// stack.
	var vls []catalog.ValidationLevel
	switch len(ids) {
	case 1:
		//gcassert:noescape
		var arr [1]catalog.ValidationLevel
		vls = arr[:]
	default:
		vls = make([]catalog.ValidationLevel, len(ids))
	}
	{
		// Look up the descriptors in all layers except the storage layer on a
		// best-effort basis.
		q := byIDLookupContext{
			ctx:   ctx,
			txn:   txn,
			tc:    tc,
			flags: flags,
		}
		type lookupFunc = func(
			id descpb.ID,
		) (catalog.Descriptor, catalog.ValidationLevel, error)
		for _, fn := range []lookupFunc{
			q.lookupVirtual,
			q.lookupTemporary,
			q.lookupSynthetic,
			q.lookupUncommitted,
			q.lookupCached,
			q.lookupLeased,
		} {
			for i, id := range ids {
				if descs[i] != nil {
					continue
				}
				desc, vl, err := fn(id)
				if err != nil {
					return err
				}
				if desc == nil {
					continue
				}
				descs[i] = desc
				vls[i] = vl
			}
		}
	}

	// Read any missing descriptors from storage and add them to the slice.
	var readIDs catalog.DescriptorIDSet
	for i, id := range ids {
		if descs[i] == nil {
			readIDs.Add(id)
		}
	}
	if !readIDs.Empty() {
		if err = tc.stored.EnsureFromStorageByIDs(ctx, txn, readIDs, catalog.Any); err != nil {
			return err
		}
		for i, id := range ids {
			if descs[i] == nil {
				descs[i] = tc.stored.GetCachedByID(id)
				vls[i] = tc.stored.GetValidationLevelByID(id)
			}
		}
	}

	// At this point, all descriptors are in the slice, finalize and hydrate them.
	if err := tc.finalizeDescriptors(ctx, txn, flags, descs, vls); err != nil {
		return err
	}
	// Hydration is skipped if "SkipHydration" flag is true.
	if err := tc.hydrateDescriptors(ctx, txn, flags, descs); err != nil {
		return err
	}
	for _, desc := range descs {
		if err := catalog.FilterDescriptorState(desc, flags); err != nil {
			return err
		}
	}
	return nil
}

// byIDLookupContext is a helper struct for getDescriptorsByID which contains
// the parameters for looking up descriptors by ID at various levels in the
// Collection.
type byIDLookupContext struct {
	ctx   context.Context
	txn   *kv.Txn
	tc    *Collection
	flags tree.CommonLookupFlags
}

func (q *byIDLookupContext) lookupVirtual(
	id descpb.ID,
) (catalog.Descriptor, catalog.ValidationLevel, error) {
	// TODO(postamar): get rid of descriptorless public schemas
	if id == keys.PublicSchemaID {
		if q.flags.RequireMutable {
			err := catalog.NewMutableAccessToVirtualSchemaError(schemadesc.GetPublicSchema())
			return nil, catalog.NoValidation, err
		}
		return schemadesc.GetPublicSchema(), validate.Write, nil
	}
	desc, err := q.tc.virtual.getByID(q.ctx, id, q.flags.RequireMutable)
	if err != nil || desc == nil {
		return nil, catalog.NoValidation, err
	}
	return desc, validate.Write, nil
}

func (q *byIDLookupContext) lookupTemporary(
	id descpb.ID,
) (catalog.Descriptor, catalog.ValidationLevel, error) {
	td := q.tc.temporary.getSchemaByID(id)
	if td == nil {
		return nil, catalog.NoValidation, nil
	}
	if q.flags.RequireMutable {
		err := catalog.NewMutableAccessToVirtualSchemaError(schemadesc.GetPublicSchema())
		return nil, catalog.NoValidation, err
	}
	return td, validate.Write, nil
}

func (q *byIDLookupContext) lookupSynthetic(
	id descpb.ID,
) (catalog.Descriptor, catalog.ValidationLevel, error) {
	if q.flags.AvoidSynthetic {
		return nil, catalog.NoValidation, nil
	}
	sd := q.tc.synthetic.getSyntheticByID(id)
	if sd == nil {
		return nil, catalog.NoValidation, nil
	}
	if q.flags.RequireMutable {
		return nil, catalog.NoValidation, newMutableSyntheticDescriptorAssertionError(sd.GetID())
	}
	return sd, validate.Write, nil
}

func (q *byIDLookupContext) lookupCached(
	id descpb.ID,
) (catalog.Descriptor, catalog.ValidationLevel, error) {
	if desc := q.tc.stored.GetCachedByID(id); desc != nil {
		return desc, q.tc.stored.GetValidationLevelByID(id), nil
	}
	return nil, catalog.NoValidation, nil
}

func (q *byIDLookupContext) lookupUncommitted(
	id descpb.ID,
) (catalog.Descriptor, catalog.ValidationLevel, error) {
	if desc := q.tc.uncommitted.getUncommittedByID(id); desc != nil {
		return desc, validate.MutableRead, nil
	}
	return nil, catalog.NoValidation, nil
}

func (q *byIDLookupContext) lookupLeased(
	id descpb.ID,
) (catalog.Descriptor, catalog.ValidationLevel, error) {
	if q.flags.AvoidLeased || q.flags.RequireMutable || lease.TestingTableLeasesAreDisabled() {
		return nil, catalog.NoValidation, nil
	}
	// If we have already read all of the descriptors, use it as a negative
	// cache to short-circuit a lookup we know will be doomed to fail.
	if q.tc.stored.IsIDKnownToNotExist(id, q.flags.ParentID) {
		return nil, catalog.NoValidation, catalog.ErrDescriptorNotFound
	}
	desc, shouldReadFromStore, err := q.tc.leased.getByID(q.ctx, q.tc.deadlineHolder(q.txn), id)
	if err != nil || shouldReadFromStore {
		return nil, catalog.NoValidation, err
	}
	return desc, validate.ImmutableRead, nil
}

// getDescriptorByName looks up a descriptor by name.
//
// The Required and AvoidCommittedAdding flags are ignored and overridden.
func (tc *Collection) getDescriptorByName(
	ctx context.Context,
	txn *kv.Txn,
	db catalog.DatabaseDescriptor,
	sc catalog.SchemaDescriptor,
	name string,
	flags tree.CommonLookupFlags,
	requestedType catalog.DescriptorType,
) (catalog.Descriptor, error) {
	mustBeVirtual, vd, err := tc.getVirtualDescriptorByName(sc, name, flags.RequireMutable, requestedType)
	if mustBeVirtual || vd != nil || err != nil || (db == nil && sc != nil) {
		return vd, err
	}
	id, err := tc.getNonVirtualDescriptorID(ctx, txn, db, sc, name, flags)
	if err != nil || id == descpb.InvalidID {
		return nil, err
	}
	// When looking up descriptors by name, then descriptors in the adding state
	// must be uncommitted to be visible (among other things).
	flags.AvoidCommittedAdding = true

	desc, err := tc.getDescriptorByID(ctx, txn, flags, id)
	if err != nil {
		// Swallow error if the descriptor is dropped.
		if errors.Is(err, catalog.ErrDescriptorDropped) {
			return nil, nil
		}
		if errors.Is(err, catalog.ErrDescriptorNotFound) {
			// Special case for temporary schemas, which can't always be resolved by
			// ID alone.
			if db != nil && sc == nil && isTemporarySchema(name) {
				return schemadesc.NewTemporarySchema(name, id, db.GetID()), nil
			}
			// In all other cases, having an ID should imply having a descriptor.
			return nil, errors.WithAssertionFailure(err)
		}
		return nil, err
	}
	if desc.GetName() != name && !(desc.DescriptorType() == catalog.Schema && isTemporarySchema(name)) {
		// TODO(postamar): make Collection aware of name ops
		//
		// We're prevented from removing this check until the Collection mediates
		// name changes in the system.namespace table similarly to how it mediates
		// descriptor changes in system.descriptor via the uncommitted descriptors
		// layer and the WriteDescsToBatch method.
		return nil, nil
	}
	return desc, nil
}

type continueOrHalt bool

const (
	continueLookups continueOrHalt = false
	haltLookups     continueOrHalt = true
)

// getVirtualDescriptorByName looks up a virtual descriptor by name.
//
// Virtual descriptors do not always have an ID set, so they need to be treated
// separately from getNonVirtualDescriptorID. Also, validation, type hydration
// and state filtering are irrelevant here.
func (tc *Collection) getVirtualDescriptorByName(
	sc catalog.SchemaDescriptor,
	name string,
	isMutableRequired bool,
	requestedType catalog.DescriptorType,
) (continueOrHalt, catalog.Descriptor, error) {
	objFlags := tree.ObjectLookupFlags{
		CommonLookupFlags: tree.CommonLookupFlags{
			RequireMutable: isMutableRequired,
		},
	}
	switch requestedType {
	case catalog.Schema:
		if vs := tc.virtual.getSchemaByName(name); vs != nil {
			return haltLookups, vs, nil
		}
	case catalog.Type:
		objFlags.DesiredObjectKind = tree.TypeObject
		fallthrough
	case catalog.Table:
		isVirtual, vd, err := tc.virtual.getObjectByName(sc.GetName(), name, objFlags)
		if isVirtual || vd != nil || err != nil {
			return haltLookups, vd, err
		}
	}
	return continueLookups, nil, nil
}

// getNonVirtualDescriptorID looks up a non-virtual descriptor ID by name by
// going through layers in sequence.
//
// All flags except AvoidLeased, RequireMutable and AvoidSynthetic are ignored.
func (tc *Collection) getNonVirtualDescriptorID(
	ctx context.Context,
	txn *kv.Txn,
	db catalog.DatabaseDescriptor,
	sc catalog.SchemaDescriptor,
	name string,
	flags tree.CommonLookupFlags,
) (descpb.ID, error) {
	flags = tree.CommonLookupFlags{
		AvoidLeased:    flags.AvoidLeased,
		RequireMutable: flags.RequireMutable,
		AvoidSynthetic: flags.AvoidSynthetic,
	}
	var parentID, parentSchemaID descpb.ID
	var isSchema bool
	if db != nil {
		parentID = db.GetID()
		if sc != nil {
			parentSchemaID = sc.GetID()
		} else {
			isSchema = true
		}
	}

	// Define the lookup functions for each layer.
	lookupTemporarySchemaID := func() (continueOrHalt, descpb.ID, error) {
		if !isSchema || !isTemporarySchema(name) {
			return continueLookups, descpb.InvalidID, nil
		}
		avoidFurtherLookups, td := tc.temporary.getSchemaByName(ctx, parentID, name)
		if td != nil {
			return haltLookups, td.GetID(), nil
		}
		if avoidFurtherLookups {
			return haltLookups, descpb.InvalidID, nil
		}
		return continueLookups, descpb.InvalidID, nil
	}
	lookupSchemaID := func() (continueOrHalt, descpb.ID, error) {
		if !isSchema {
			return continueLookups, descpb.InvalidID, nil
		}
		// Getting a schema by name uses a special resolution path which can avoid
		// a namespace lookup because the mapping of database to schema is stored on
		// the database itself. This is an important optimization in the case when
		// the schema does not exist.
		//
		if !db.HasPublicSchemaWithDescriptor() && name == catconstants.PublicSchemaName {
			return haltLookups, keys.PublicSchemaID, nil
		}
		if id := db.GetSchemaID(name); id != descpb.InvalidID {
			return haltLookups, id, nil
		}
		if isTemporarySchema(name) {
			// Look for temporary schema IDs in other layers.
			return continueLookups, descpb.InvalidID, nil
		}
		return haltLookups, descpb.InvalidID, nil
	}
	lookupSyntheticID := func() (continueOrHalt, descpb.ID, error) {
		if flags.AvoidSynthetic {
			return continueLookups, descpb.InvalidID, nil
		}
		if sd := tc.synthetic.getSyntheticByName(parentID, parentSchemaID, name); sd != nil {
			return haltLookups, sd.GetID(), nil
		}
		return continueLookups, descpb.InvalidID, nil
	}
	lookupUncommittedID := func() (continueOrHalt, descpb.ID, error) {
		if ud := tc.uncommitted.getUncommittedByName(parentID, parentSchemaID, name); ud != nil {
			return haltLookups, ud.GetID(), nil
		}
		return continueLookups, descpb.InvalidID, nil
	}
	lookupStoreCacheID := func() (continueOrHalt, descpb.ID, error) {
		if cd := tc.stored.GetCachedByName(parentID, parentSchemaID, name); cd != nil {
			return haltLookups, cd.GetID(), nil
		}
		return continueLookups, descpb.InvalidID, nil
	}
	lookupLeasedID := func() (continueOrHalt, descpb.ID, error) {
		if flags.AvoidLeased || flags.RequireMutable || lease.TestingTableLeasesAreDisabled() {
			return continueLookups, descpb.InvalidID, nil
		}
		if isSchema && isTemporarySchema(name) {
			return continueLookups, descpb.InvalidID, nil
		}
		ld, shouldReadFromStore, err := tc.leased.getByName(
			ctx, tc.deadlineHolder(txn), parentID, parentSchemaID, name,
		)
		if err != nil {
			return haltLookups, descpb.InvalidID, err
		}
		if shouldReadFromStore {
			return continueLookups, descpb.InvalidID, nil
		}
		return haltLookups, ld.GetID(), nil
	}
	lookupStoredID := func() (continueOrHalt, descpb.ID, error) {
		id, err := tc.stored.LookupDescriptorID(ctx, txn, parentID, parentSchemaID, name)
		return haltLookups, id, err
	}

	// Iterate through each layer until an ID is conclusively found or not, or an
	// error is thrown.
	for _, fn := range []func() (continueOrHalt, descpb.ID, error){
		lookupTemporarySchemaID,
		lookupSchemaID,
		lookupSyntheticID,
		lookupUncommittedID,
		lookupStoreCacheID,
		lookupLeasedID,
		lookupStoredID,
	} {
		isDone, id, err := fn()
		if err != nil {
			return descpb.InvalidID, err
		}
		if isDone {
			return id, nil
		}
	}
	return descpb.InvalidID, nil
}

// finalizeDescriptors ensures that all descriptors are (1) properly validated
// and (2) if mutable descriptors are requested, these are present in the
// uncommitted descriptors layer.
func (tc *Collection) finalizeDescriptors(
	ctx context.Context,
	txn *kv.Txn,
	flags tree.CommonLookupFlags,
	descs []catalog.Descriptor,
	validationLevels []catalog.ValidationLevel,
) error {
	// Add the descriptors to the uncommitted layer if we want them to be mutable.
	if flags.RequireMutable {
		for i, desc := range descs {
			mut, err := tc.uncommitted.ensureMutable(ctx, desc)
			if err != nil {
				return err
			}
			descs[i] = mut
		}
	}
	// Ensure that all descriptors are sufficiently validated.
	requiredLevel := validate.MutableRead
	if !flags.RequireMutable && !flags.AvoidLeased {
		requiredLevel = validate.ImmutableRead
	}
	var toValidate []catalog.Descriptor
	for i := range descs {
		if validationLevels[i] < requiredLevel {
			toValidate = append(toValidate, descs[i])
		}
	}
	if len(toValidate) > 0 {
		if err := tc.Validate(ctx, txn, catalog.ValidationReadTelemetry, requiredLevel, toValidate...); err != nil {
			return err
		}
		for _, desc := range toValidate {
			tc.stored.UpdateValidationLevel(desc, requiredLevel)
		}
	}
	return nil
}

func (tc *Collection) deadlineHolder(txn *kv.Txn) deadlineHolder {
	if tc.maxTimestampBoundDeadlineHolder.maxTimestampBound.IsEmpty() {
		return txn
	}
	return &tc.maxTimestampBoundDeadlineHolder
}

func isTemporarySchema(name string) bool {
	return strings.HasPrefix(name, catconstants.PgTempSchemaName)
}
