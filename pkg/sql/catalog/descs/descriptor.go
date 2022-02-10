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
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catconstants"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/schemadesc"
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

	log.VEventf(ctx, 2, "looking up descriptors for ids %d", ids)
	descs = make([]catalog.Descriptor, len(ids))
	{
		// Look up the descriptors in all layers except the KV layer on a
		// best-effort basis.
		q := byIDLookupContext{
			ctx:   ctx,
			txn:   txn,
			tc:    tc,
			flags: flags,
		}
		for _, fn := range []func(id descpb.ID) (catalog.Descriptor, error){
			q.lookupVirtual,
			q.lookupSynthetic,
			q.lookupUncommitted,
			q.lookupLeased,
		} {
			for i, id := range ids {
				if descs[i] != nil {
					continue
				}
				desc, err := fn(id)
				if err != nil {
					return nil, err
				}
				descs[i] = desc
			}
		}
	}

	kvIDs := make([]descpb.ID, 0, len(ids))
	indexes := make([]int, 0, len(ids))
	for i, id := range ids {
		if descs[i] != nil {
			continue
		}
		kvIDs = append(kvIDs, id)
		indexes = append(indexes, i)
	}
	if len(kvIDs) == 0 {
		// No KV lookup necessary, return early.
		return descs, nil
	}
	kvDescs, err := tc.withReadFromStore(flags.RequireMutable, func() ([]catalog.MutableDescriptor, error) {
		return tc.kv.getByIDs(ctx, txn, tc.version, kvIDs)
	})
	if err != nil {
		return nil, err
	}
	for j, desc := range kvDescs {
		descs[indexes[j]] = desc
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

func (q *byIDLookupContext) lookupVirtual(id descpb.ID) (catalog.Descriptor, error) {
	return q.tc.virtual.getByID(q.ctx, id, q.flags.RequireMutable)
}

func (q *byIDLookupContext) lookupSynthetic(id descpb.ID) (catalog.Descriptor, error) {
	if q.flags.AvoidSynthetic {
		return nil, nil
	}
	_, sd := q.tc.synthetic.getByID(id)
	if sd == nil {
		return nil, nil
	}
	if q.flags.RequireMutable {
		return nil, newMutableSyntheticDescriptorAssertionError(sd.GetID())
	}
	return sd, nil
}

func (q *byIDLookupContext) lookupUncommitted(id descpb.ID) (_ catalog.Descriptor, err error) {
	ud := q.tc.uncommitted.getByID(id)
	if ud == nil {
		return nil, nil
	}
	log.VEventf(q.ctx, 2, "found uncommitted descriptor %d", id)
	if !q.flags.RequireMutable {
		return ud, nil
	}
	return q.tc.uncommitted.checkOut(id)
}

func (q *byIDLookupContext) lookupLeased(id descpb.ID) (catalog.Descriptor, error) {
	if q.flags.AvoidLeased || q.flags.RequireMutable || lease.TestingTableLeasesAreDisabled() {
		return nil, nil
	}
	// If we have already read all of the descriptors, use it as a negative
	// cache to short-circuit a lookup we know will be doomed to fail.
	//
	// TODO(ajwerner): More generally leverage this set of kv descriptors on
	// the resolution path.
	if q.tc.kv.idDefinitelyDoesNotExist(id) {
		return nil, catalog.ErrDescriptorNotFound
	}
	desc, shouldReadFromStore, err := q.tc.leased.getByID(q.ctx, q.tc.deadlineHolder(q.txn), id)
	if err != nil || shouldReadFromStore {
		return nil, err
	}
	return desc, nil
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
) (found bool, desc catalog.Descriptor, err error) {
	var parentID, parentSchemaID descpb.ID
	if db != nil {
		if sc == nil {
			// Schema descriptors are handled in a special way, see getSchemaByName
			// function declaration for details.
			return getSchemaByName(ctx, tc, txn, db, name, avoidLeased, mutable, avoidSynthetic)
		}
		parentID, parentSchemaID = db.GetID(), sc.GetID()
	}

	if found, sd := tc.synthetic.getByName(parentID, parentSchemaID, name); found && !avoidSynthetic {
		if mutable {
			return false, nil, newMutableSyntheticDescriptorAssertionError(sd.GetID())
		}
		return true, sd, nil
	}

	{
		refuseFurtherLookup, ud := tc.uncommitted.getByName(parentID, parentSchemaID, name)
		if ud != nil {
			log.VEventf(ctx, 2, "found uncommitted descriptor %d", ud.GetID())
			if mutable {
				ud, err = tc.uncommitted.checkOut(ud.GetID())
				if err != nil {
					return false, nil, err
				}
			}
			return true, ud, nil
		}
		if refuseFurtherLookup {
			return false, nil, nil
		}
	}

	if !avoidLeased && !mutable && !lease.TestingTableLeasesAreDisabled() {
		var shouldReadFromStore bool
		desc, shouldReadFromStore, err = tc.leased.getByName(ctx, tc.deadlineHolder(txn), parentID, parentSchemaID, name)
		if err != nil {
			return false, nil, err
		}
		if !shouldReadFromStore {
			return desc != nil, desc, nil
		}
	}

	var descs []catalog.Descriptor
	descs, err = tc.withReadFromStore(mutable, func() ([]catalog.MutableDescriptor, error) {
		uncommittedDB, _ := tc.uncommitted.getByID(parentID).(catalog.DatabaseDescriptor)
		version := tc.settings.Version.ActiveVersion(ctx)
		desc, err := tc.kv.getByName(
			ctx,
			txn,
			version,
			uncommittedDB,
			parentID,
			parentSchemaID,
			name)
		if err != nil {
			return nil, err
		}
		return []catalog.MutableDescriptor{desc}, nil
	})
	if err != nil {
		return false, nil, err
	}
	return true, descs[0], err
}

// withReadFromStore updates the state of the Collection, especially its
// uncommitted descriptors layer, after reading a descriptor from the storage
// layer. The logic is the same regardless of whether the descriptor was read
// by name or by ID.
func (tc *Collection) withReadFromStore(
	requireMutable bool, readFn func() ([]catalog.MutableDescriptor, error),
) (descs []catalog.Descriptor, _ error) {
	muts, err := readFn()
	if err != nil {
		return nil, err
	}
	descs = make([]catalog.Descriptor, len(muts))
	for i, mut := range muts {
		if mut == nil {
			continue
		}
		desc, err := tc.uncommitted.add(mut)
		if err != nil {
			return nil, err
		}
		if requireMutable {
			desc, err = tc.uncommitted.checkOut(desc.GetID())
			if err != nil {
				return nil, err
			}
		}
		descs[i] = desc
	}
	return descs, nil
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
func getSchemaByName(
	ctx context.Context,
	tc *Collection,
	txn *kv.Txn,
	db catalog.DatabaseDescriptor,
	name string,
	avoidLeased, mutable, avoidSynthetic bool,
) (bool, catalog.Descriptor, error) {
	if !db.HasPublicSchemaWithDescriptor() && name == tree.PublicSchema {
		return true, schemadesc.GetPublicSchema(), nil
	}
	if sc := tc.virtual.getSchemaByName(name); sc != nil {
		return true, sc, nil
	}
	if isTemporarySchema(name) {
		if isDone, sc := tc.temporary.getSchemaByName(ctx, db.GetID(), name); sc != nil || isDone {
			return sc != nil, sc, nil
		}
		scID, err := tc.kv.lookupName(ctx, txn, nil /* maybeDB */, db.GetID(), keys.RootNamespaceID, name)
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
