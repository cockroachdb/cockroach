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
		Required:       true,
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
	descs, err := tc.GetImmutableDescriptorsByID(ctx, txn, flags, id)
	if err != nil {
		return nil, err
	}
	return descs[0], nil
}

// getDescriptorsByID returns a slice of descriptors by ID according to the
// provided lookup flags. Note that flags.Required is ignored, and an error is
// always returned if no descriptor with the ID exists.
func (tc *Collection) getDescriptorsByID(
	ctx context.Context, txn *kv.Txn, flags tree.CommonLookupFlags, ids ...descpb.ID,
) (descs []catalog.Descriptor, err error) {
	flags.Required = true
	flags.IncludeAdding = true
	log.VEventf(ctx, 2, "looking up descriptors for ids %v", ids)
	descs = make([]catalog.Descriptor, len(ids))
	vls := make([]catalog.ValidationLevel, len(ids))
	{
		// Look up the descriptors in all layers except the storage layer on a
		// best-effort basis.
		q := byIDLookupContext{
			ctx:   ctx,
			txn:   txn,
			tc:    tc,
			flags: flags,
		}
		for _, fn := range []func(id descpb.ID) (catalog.Descriptor, catalog.ValidationLevel, error){
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
					return nil, err
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
			return nil, err
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
		return nil, err
	}
	if err := tc.hydrateDescriptors(ctx, txn, flags, descs); err != nil {
		return nil, err
	}
	for _, desc := range descs {
		if err := catalog.FilterDescriptorState(desc, flags); err != nil {
			return nil, err
		}
	}
	return descs, nil
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
		return schemadesc.GetPublicSchema(), validate.Write, nil
	}
	desc, err := q.tc.virtual.getByID(q.ctx, id, q.flags.RequireMutable)
	return desc, validate.Write, err
}

// TODO(postamar): correct handling of mutable schema descriptors
func (q *byIDLookupContext) lookupTemporary(
	id descpb.ID,
) (catalog.Descriptor, catalog.ValidationLevel, error) {
	desc := q.tc.temporary.getSchemaByID(id)
	return desc, validate.Write, nil
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
	if q.tc.stored.IsIDKnownToNotExist(id) {
		return nil, catalog.NoValidation, catalog.ErrDescriptorNotFound
	}
	desc, shouldReadFromStore, err := q.tc.leased.getByID(q.ctx, q.tc.deadlineHolder(q.txn), id)
	if err != nil || shouldReadFromStore {
		return nil, catalog.NoValidation, err
	}
	return desc, validate.ImmutableRead, nil
}

// getByName looks up a descriptor by name.
func (tc *Collection) getByName(
	ctx context.Context,
	txn *kv.Txn,
	prefix catalog.ResolvedObjectPrefix,
	name string,
	flags tree.CommonLookupFlags,
) (catalog.Descriptor, error) {
	id, err := tc.getDescriptorID(ctx, txn, prefix, name, flags)
	if err != nil || id == descpb.InvalidID {
		return nil, err
	}
	descs, err := tc.getDescriptorsByID(ctx, txn, flags, id)
	if err != nil {
		return nil, err
	}
	desc := descs[0]
	if desc == nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(catalog.ErrDescriptorNotFound,
			"no descriptor found for ID #%d, resolved from name (%s, %s)",
			id, prefix.NamePrefix(), name)
	}
	if desc.GetName() != name && desc.DescriptorType() != catalog.Schema && !isTemporarySchema(name) {
		// TODO(postamar): make Collection aware of name ops
		return nil, nil
	}
	return desc, nil
}

// getDescriptorID looks up a descriptor ID by name in a sequence of layers.
// All flags except AvoidLeased, RequireMutable and AvoidSynthetic are ignored.
func (tc *Collection) getDescriptorID(
	ctx context.Context,
	txn *kv.Txn,
	prefix catalog.ResolvedObjectPrefix,
	name string,
	flags tree.CommonLookupFlags,
) (descpb.ID, error) {
	flags = tree.CommonLookupFlags{
		AvoidLeased:    flags.AvoidLeased,
		RequireMutable: flags.RequireMutable,
		AvoidSynthetic: flags.AvoidSynthetic,
	}
	db := prefix.Database
	sc := prefix.Schema
	var parentID, parentSchemaID descpb.ID
	var avoidNonVirtual bool
	if db != nil {
		parentID = db.GetID()
	} else if prefix.ExplicitDatabase {
		// Special case where we're looking up only virtual schemas or objects.
		avoidNonVirtual = true
	}
	if sc != nil {
		parentSchemaID = sc.GetID()
	}

	type continueOrHalt bool
	const continueLookups continueOrHalt = false
	const haltLookups continueOrHalt = true

	// Define the lookup functions for each layer.
	lookupVirtualID := func() (continueOrHalt, descpb.ID, error) {
		if sc == nil {
			// Looking up a virtual schema.
			vd := tc.virtual.getSchemaByName(name)
			if vd != nil {
				return haltLookups, vd.GetID(), nil
			}
		} else {
			// Looking up a virtual object inside a schema.
			var dbName string
			if db != nil {
				dbName = db.GetName()
			}
			objFlags := tree.ObjectLookupFlags{CommonLookupFlags: flags}
			isVirtual, vd, err := tc.virtual.getObjectByName(sc.GetName(), name, objFlags, dbName)
			if err != nil {
				return haltLookups, descpb.InvalidID, err
			}
			if vd != nil {
				return haltLookups, vd.GetID(), nil
			}
			if isVirtual {
				return haltLookups, descpb.InvalidID, nil
			}
		}
		if avoidNonVirtual {
			return haltLookups, descpb.InvalidID, nil
		}
		return continueLookups, descpb.InvalidID, nil
	}
	lookupTemporarySchemaID := func() (continueOrHalt, descpb.ID, error) {
		if db == nil || sc != nil || !isTemporarySchema(name) {
			return continueLookups, descpb.InvalidID, nil
		}
		avoidFurtherLookups, td := tc.temporary.getSchemaByName(ctx, db.GetID(), name)
		if td != nil {
			return haltLookups, td.GetID(), nil
		}
		if avoidFurtherLookups {
			return haltLookups, descpb.InvalidID, nil
		}
		return continueLookups, descpb.InvalidID, nil
	}
	lookupSchemaID := func() (continueOrHalt, descpb.ID, error) {
		if db == nil || sc != nil {
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
		return haltLookups, db.GetSchemaID(name), nil
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
		lookupVirtualID,
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
// Known validation levels can optionally be provided via validationLevels.
// If none are provided, finalizeDescriptors seeks them out in the appropriate
// layer (stored or uncommitted).

// nil safe defaults are used instead. In any case, after validation is
// performed the known levels are raised accordingly.
func (tc *Collection) finalizeDescriptors(
	ctx context.Context,
	txn *kv.Txn,
	flags tree.CommonLookupFlags,
	descs []catalog.Descriptor,
	validationLevels []catalog.ValidationLevel,
) error {
	if validationLevels == nil {
		validationLevels = make([]catalog.ValidationLevel, len(descs))
		for i, desc := range descs {
			if tc.synthetic.getSyntheticByID(desc.GetID()) != nil {
				// Synthetic descriptors are assumed to have been validated.
				validationLevels[i] = validate.Write
			} else if tc.uncommitted.getUncommittedByID(desc.GetID()) != nil {
				// Uncommitted descriptors should, by definition, already have been
				// validated at least at the MutableRead level. This effectively
				// excludes them from being validated again right now.
				//
				// In any case, they will be fully validated when the transaction
				// commits.
				validationLevels[i] = validate.MutableRead
			} else {
				validationLevels[i] = tc.stored.GetValidationLevelByID(desc.GetID())
			}
		}
	}
	if len(validationLevels) != len(descs) {
		return errors.AssertionFailedf(
			"len(validationLevels) = %d should be equal to len(descs) = %d",
			len(validationLevels), len(descs))
	}
	// Ensure that all descriptors are sufficiently validated.
	requiredLevel := validate.MutableRead
	if !flags.RequireMutable && !flags.AvoidLeased {
		requiredLevel = validate.ImmutableRead
	}
	var toValidate []catalog.Descriptor
	for i, vl := range validationLevels {
		if vl < requiredLevel {
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
	// Add the descriptors to the uncommitted layer if we want them to be mutable.
	if !flags.RequireMutable {
		return nil
	}
	for i, desc := range descs {
		mut, err := tc.uncommitted.ensureMutable(ctx, desc)
		if err != nil {
			return err
		}
		if mut != nil {
			descs[i] = mut
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
