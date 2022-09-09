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
	defer func() {
		if err == nil {
			err = filterDescriptorsStates(descs, flags)
		}
		if err != nil {
			descs = nil
		}
	}()

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
	desc, err := q.tc.virtual.getByID(q.ctx, id, q.flags.RequireMutable)
	return desc, validate.Write, err
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

// filterDescriptorsStates is a helper function for getDescriptorsByID.
func filterDescriptorsStates(descs []catalog.Descriptor, flags tree.CommonLookupFlags) error {
	for _, desc := range descs {
		// The first return value can safely be ignored, it will always be false
		// because the required flag is set.
		_, err := filterDescriptorState(desc, true /* required */, flags)
		if err == nil {
			continue
		}
		if desc.Adding() && (desc.IsUncommittedVersion() || flags.AvoidLeased || flags.RequireMutable) {
			// This is a special case for tables in the adding state: Roughly speaking,
			// we always need to resolve tables in the adding state by ID when they were
			// newly created in the transaction for DDL statements and for some
			// information queries (but not for ordinary name resolution for queries/
			// DML), but we also need to make these tables public in the schema change
			// job in a separate transaction.
			// TODO (lucy): We need something like an IncludeAdding flag so that callers
			// can specify this behavior, instead of having the collection infer the
			// desired behavior based on the flags (and likely producing unintended
			// behavior). See the similar comment on etDescriptorByName, which covers
			// the ordinary name resolution path as well as DDL statements.
			continue
		}
		return err
	}
	return nil
}

func (tc *Collection) getByName(
	ctx context.Context,
	txn *kv.Txn,
	db catalog.DatabaseDescriptor,
	sc catalog.SchemaDescriptor,
	name string,
	avoidLeased, mutable, avoidSynthetic bool,
	alwaysLookupLeasedPublicSchema bool, // passed through to getSchemaByName
) (found bool, desc catalog.Descriptor, err error) {
	var parentID, parentSchemaID descpb.ID
	if db != nil {
		if sc == nil {
			// Schema descriptors are handled in a special way, see getSchemaByName
			// function declaration for details.
			return getSchemaByName(
				ctx, tc, txn, db, name, avoidLeased, mutable, avoidSynthetic,
				alwaysLookupLeasedPublicSchema,
			)
		}
		parentID, parentSchemaID = db.GetID(), sc.GetID()
	}

	if sd := tc.synthetic.getSyntheticByName(parentID, parentSchemaID, name); sd != nil && !avoidSynthetic {
		if mutable {
			return false, nil, newMutableSyntheticDescriptorAssertionError(sd.GetID())
		}
		return true, sd, nil
	}

	desc = tc.uncommitted.getUncommittedByName(parentID, parentSchemaID, name)

	// Look up descriptor in store cache.
	if desc == nil {
		if cd := tc.stored.GetCachedByName(parentID, parentSchemaID, name); cd != nil {
			desc = cd
			log.VEventf(ctx, 2, "found cached descriptor %d", desc.GetID())
		}
	}

	// Look up leased descriptor.
	if desc == nil && !avoidLeased && !mutable && !lease.TestingTableLeasesAreDisabled() {
		leasedDesc, shouldReadFromStore, err := tc.leased.getByName(ctx, tc.deadlineHolder(txn), parentID, parentSchemaID, name)
		if err != nil {
			return false, nil, err
		}
		if !shouldReadFromStore {
			return leasedDesc != nil, leasedDesc, nil
		}
	}

	// Look up descriptor in storage.
	if desc == nil {
		desc, err = tc.stored.GetByName(ctx, txn, parentID, parentSchemaID, name)
		if err != nil || desc == nil {
			return false, nil, err
		}
	}

	// At this point the descriptor exists.
	// Finalize it and return it.
	{
		ret := []catalog.Descriptor{desc}
		flags := tree.CommonLookupFlags{RequireMutable: mutable}
		if err = tc.finalizeDescriptors(ctx, txn, flags, ret, nil /* validationLevels */); err != nil {
			return false, nil, err
		}
		desc = ret[0]
		return desc != nil, desc, err
	}
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
			if tc.uncommitted.getUncommittedByID(desc.GetID()) != nil {
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
	return nil
}

// hydrateDescriptors ensures that the descriptors in the slice are hydrated.
//
// Callers expect the descriptors to come back hydrated.
// In practice, array types here are not hydrated, and that's a bummer.
// Nobody presently is upset about it, but it's not a good thing.
// Ideally we'd have a clearer contract regarding hydration and the values
// stored in the various maps inside the collection. One might want to
// store only hydrated values in the various maps. This turns out to be
// somewhat tricky because we'd need to make sure to properly re-hydrate
// all the relevant descriptors when a type descriptor change. Leased
// descriptors are at least as tricky, plus, there we have a cache that
// works relatively well.
//
// TODO(ajwerner): Sort out the hydration mess; define clearly what is
// hydrated where and test the API boundary accordingly.
func (tc *Collection) hydrateDescriptors(
	ctx context.Context, txn *kv.Txn, flags tree.CommonLookupFlags, descs []catalog.Descriptor,
) error {
	for i, desc := range descs {
		hd, isHydratable := desc.(catalog.HydratableDescriptor)
		if !isHydratable {
			continue
		}
		var err error
		descs[i], err = tc.hydrateTypesInDescWithOptions(ctx, txn, hd, flags.IncludeOffline, flags.AvoidLeased)
		if err != nil {
			return err
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

// Getting a schema by name uses a special resolution path which can avoid
// a namespace lookup because the mapping of database to schema is stored on
// the database itself. This is an important optimization in the case when
// the schema does not exist.
//
// TODO(ajwerner): Understand and rationalize the namespace lookup given the
// schema lookup by ID path only returns descriptors owned by this session.
//
// The alwaysLookupLeasedPublicSchema parameter indicates that a missing public
// schema entry in the database descriptor should not be interpreted to
// mean that the public schema is the synthetic public schema, and, instead
// the public schema should be looked up via the lease manager by name.
// This is a workaround activated during the public schema migration to
// avoid a situation where the database does not know about the new public
// schema but the table in the lease manager does.
//
// TODO(ajwerner): Remove alwaysLookupLeasedPublicSchema in 22.2.
func getSchemaByName(
	ctx context.Context,
	tc *Collection,
	txn *kv.Txn,
	db catalog.DatabaseDescriptor,
	name string,
	avoidLeased, mutable, avoidSynthetic bool,
	alwaysLookupLeasedPublicSchema bool,
) (bool, catalog.Descriptor, error) {
	if !db.HasPublicSchemaWithDescriptor() && name == tree.PublicSchema {
		// TODO(ajwerner): Remove alwaysLookupLeasedPublicSchema in 22.2.
		if alwaysLookupLeasedPublicSchema {
			desc, _, err := tc.leased.getByName(ctx, txn, db.GetID(), 0, catconstants.PublicSchemaName)
			if err != nil {
				return false, desc, err
			}
			return true, desc, nil
		}
		return true, schemadesc.GetPublicSchema(), nil
	}
	if sc := tc.virtual.getSchemaByName(name); sc != nil {
		return true, sc, nil
	}
	if isTemporarySchema(name) {
		if isDone, sc := tc.temporary.getSchemaByName(ctx, db.GetID(), name); sc != nil || isDone {
			return sc != nil, sc, nil
		}
		scID, err := tc.stored.LookupDescriptorID(ctx, txn, db.GetID(), keys.RootNamespaceID, name)
		if err != nil || scID == descpb.InvalidID {
			return false, nil, err
		}
		return true, schemadesc.NewTemporarySchema(name, scID, db.GetID()), nil
	}
	if id := db.GetSchemaID(name); id != descpb.InvalidID {
		// TODO(ajwerner): Fill in flags here or, more likely, get rid of
		// it on this path.
		sc, err := tc.getSchemaByID(ctx, txn, id, tree.SchemaLookupFlags{
			RequireMutable: mutable,
			AvoidLeased:    avoidLeased,
			AvoidSynthetic: avoidSynthetic,
		})
		if errors.Is(err, catalog.ErrDescriptorDropped) {
			err = nil
		}
		return sc != nil, sc, err
	}
	return false, nil, nil
}

func isTemporarySchema(name string) bool {
	return strings.HasPrefix(name, catconstants.PgTempSchemaName)
}

// filterDescriptorState wraps the more general catalog function to swallow
// the error if the descriptor is being dropped and the descriptor is not
// required. In that case, dropped will be true. A return value of false, nil
// means this descriptor is okay given the flags.
// TODO (lucy): We would like the ByID methods to ignore the Required flag and
// unconditionally return an error for dropped descriptors if IncludeDropped is
// not set, so we can't just pass the flags passed into the methods into this
// function, hence the boolean argument. This is the only user of
// catalog.FilterDescriptorState which needs to pass in nontrivial flags, at
// time of writing, so we should clean up the interface around this bit of
// functionality.
func filterDescriptorState(
	desc catalog.Descriptor, required bool, flags tree.CommonLookupFlags,
) (dropped bool, _ error) {
	flags = tree.CommonLookupFlags{
		Required:       required,
		IncludeOffline: flags.IncludeOffline,
		IncludeDropped: flags.IncludeDropped,
	}
	if err := catalog.FilterDescriptorState(desc, flags); err != nil {
		if required || !errors.Is(err, catalog.ErrDescriptorDropped) {
			return false, err
		}
		return true, nil
	}
	return false, nil
}
