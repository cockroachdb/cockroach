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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/dbdesc"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/errors"
)

// uncommittedDescriptor is a descriptor that has been modified in the current
// transaction.
type uncommittedDescriptor struct {
	immutable catalog.Descriptor

	// mutable generally holds the descriptor as it was read from the database.
	// In the rare case that this struct corresponds to a singleton which is
	// added to optimize for special cases of system descriptors.
	// It should be accessed through getMutable() which will construct a new
	// value in cases where it is nil.
	mutable catalog.MutableDescriptor
}

// GetName implements the catalog.NameEntry interface.
func (u *uncommittedDescriptor) GetName() string {
	return u.immutable.GetName()
}

// GetParentID implements the catalog.NameEntry interface.
func (u *uncommittedDescriptor) GetParentID() descpb.ID {
	return u.immutable.GetParentID()
}

// GetParentSchemaID implements the catalog.NameEntry interface.
func (u uncommittedDescriptor) GetParentSchemaID() descpb.ID {
	return u.immutable.GetParentSchemaID()
}

// GetID implements the catalog.NameEntry interface.
func (u uncommittedDescriptor) GetID() descpb.ID {
	return u.immutable.GetID()
}

// getMutable is how the mutable descriptor should be accessed. It constructs
// a new descriptor in the case that this descriptor is a cached, in-memory
// singleton for a system descriptor.
func (u *uncommittedDescriptor) getMutable() catalog.MutableDescriptor {
	if u.mutable != nil {
		return u.mutable
	}
	return u.immutable.NewBuilder().BuildExistingMutable()
}

var _ catalog.NameEntry = (*uncommittedDescriptor)(nil)

// uncommittedDescriptors is the data structure holding all
// uncommittedDescriptor objects for a Collection.
//
// Immutable descriptors can be freely looked up.
// Mutable descriptors can be:
// 1. added into it,
// 2. checked out of it,
// 3. checked back in to it.
//
// An error will be triggered by:
// - checking out a mutable descriptor that hasn't yet been added,
// - checking in a descriptor that has been added but not yet checked out,
// - any checked-out-but-not-checked-in mutable descriptors at commit time.
//
type uncommittedDescriptors struct {

	// Descriptors modified by the uncommitted transaction affiliated with this
	// Collection. This allows a transaction to see its own modifications while
	// bypassing the descriptor lease mechanism. The lease mechanism will have its
	// own transaction to read the descriptor and will hang waiting for the
	// uncommitted changes to the descriptor. These descriptors are local to this
	// Collection and invisible to other transactions.
	descs nstree.Map

	// descNames is the set of names which a read or written
	// descriptor took on at some point in its lifetime. Everything added to
	// uncommittedDescriptors is added to descNames as well
	// as all of the known draining names. The idea is that if we find that
	// a name is not in the above map but is in the set, then we can avoid
	// doing a lookup.
	//
	// TODO(postamar): better uncommitted namespace changes handling after 22.1.
	descNames nstree.Set

	// addedSystemDatabase is used to mark whether the optimization to add the
	// system database to the set of uncommitted descriptors has occurred.
	addedSystemDatabase bool
}

func (ud *uncommittedDescriptors) reset() {
	ud.descs.Clear()
	ud.descNames.Clear()
	ud.addedSystemDatabase = false
}

// add adds a descriptor to the set of uncommitted descriptors and returns
// an immutable copy of that descriptor.
func (ud *uncommittedDescriptors) add(mut catalog.MutableDescriptor) (catalog.Descriptor, error) {
	uNew, err := makeUncommittedDescriptor(mut)
	if err != nil {
		return nil, err
	}
	for _, n := range uNew.immutable.GetDrainingNames() {
		ud.descNames.Add(n)
	}
	ud.descs.Upsert(uNew)
	return uNew.immutable, err
}

// checkOut checks out an uncommitted mutable descriptor for use in the
// transaction. This descriptor should later be checked in again.
func (ud *uncommittedDescriptors) checkOut(id descpb.ID) (catalog.MutableDescriptor, error) {
	if id == keys.SystemDatabaseID {
		ud.maybeAddSystemDatabase()
	}
	entry := ud.descs.GetByID(id)
	if entry == nil {
		return nil, errors.NewAssertionErrorWithWrappedErrf(
			errors.New("descriptor hasn't been added yet"),
			"cannot check in uncommitted descriptor with ID %d",
			id)

	}
	u := entry.(*uncommittedDescriptor)
	return u.getMutable(), nil
}

// checkIn checks in an uncommitted mutable descriptor that was previously
// checked out.
func (ud *uncommittedDescriptors) checkIn(mut catalog.MutableDescriptor) error {
	// TODO(postamar): actually check that the descriptor has been checked out.
	_, err := ud.add(mut)
	return err
}

func makeUncommittedDescriptor(desc catalog.MutableDescriptor) (*uncommittedDescriptor, error) {
	version := desc.GetVersion()
	origVersion := desc.OriginalVersion()
	if version != origVersion && version != origVersion+1 {
		return nil, errors.AssertionFailedf(
			"descriptor %d version %d not compatible with cluster version %d",
			desc.GetID(), version, origVersion)
	}

	mutable, err := maybeRefreshCachedFieldsOnTypeDescriptor(desc)
	if err != nil {
		return nil, err
	}

	return &uncommittedDescriptor{
		mutable:   mutable,
		immutable: mutable.ImmutableCopy(),
	}, nil
}

// maybeRefreshCachedFieldsOnTypeDescriptor refreshes the cached fields on a
// Mutable if the given descriptor is a type descriptor and works as a pass
// through for all other descriptors. Mutable type descriptors are refreshed to
// reconstruct enumMetadata. This ensures that tables hydration following a
// type descriptor update (in the same txn) happens using the modified fields.
func maybeRefreshCachedFieldsOnTypeDescriptor(
	desc catalog.MutableDescriptor,
) (catalog.MutableDescriptor, error) {
	typeDesc, ok := desc.(catalog.TypeDescriptor)
	if ok {
		return typedesc.UpdateCachedFieldsOnModifiedMutable(typeDesc)
	}
	return desc, nil
}

// getByID looks up an uncommitted descriptor by ID.
func (ud *uncommittedDescriptors) getByID(id descpb.ID) catalog.Descriptor {
	if id == keys.SystemDatabaseID && !ud.addedSystemDatabase {
		ud.maybeAddSystemDatabase()
	}
	entry := ud.descs.GetByID(id)
	if entry == nil {
		return nil
	}
	return entry.(*uncommittedDescriptor).immutable
}

// getByName returns a descriptor for the requested name if the requested name
// is for a descriptor modified within the transaction affiliated with the
// Collection.
//
// The first return value "hasKnownRename" is true when there is a known
// rename of that descriptor, so it would be invalid to miss the cache and go to
// KV (where the descriptor prior to the rename may still exist).
func (ud *uncommittedDescriptors) getByName(
	dbID descpb.ID, schemaID descpb.ID, name string,
) (hasKnownRename bool, desc catalog.Descriptor) {
	if dbID == 0 && schemaID == 0 && name == systemschema.SystemDatabaseName {
		ud.maybeAddSystemDatabase()
	}
	// Walk latest to earliest so that a DROP followed by a CREATE with the same
	// name will result in the CREATE being seen.
	if got := ud.descs.GetByName(dbID, schemaID, name); got != nil {
		return false, got.(*uncommittedDescriptor).immutable
	}
	// Check whether the set is empty to avoid allocating the NameInfo.
	if ud.descNames.Empty() {
		return false, nil
	}
	return ud.descNames.Contains(descpb.NameInfo{
		ParentID:       dbID,
		ParentSchemaID: schemaID,
		Name:           name,
	}), nil
}

func (ud *uncommittedDescriptors) iterateNewVersionByID(
	fn func(entry catalog.NameEntry, originalVersion lease.IDVersion) error,
) error {
	return ud.descs.IterateByID(func(entry catalog.NameEntry) error {
		mut := entry.(*uncommittedDescriptor).mutable
		if mut == nil || mut.IsNew() || !mut.IsUncommittedVersion() {
			return nil
		}
		return fn(entry, lease.NewIDVersionPrev(mut.OriginalName(), mut.OriginalID(), mut.OriginalVersion()))
	})
}

func (ud *uncommittedDescriptors) iterateImmutableByID(
	fn func(imm catalog.Descriptor) error,
) error {
	return ud.descs.IterateByID(func(entry catalog.NameEntry) error {
		return fn(entry.(*uncommittedDescriptor).immutable)
	})
}

func (ud *uncommittedDescriptors) getUncommittedTables() (tables []catalog.TableDescriptor) {
	_ = ud.iterateImmutableByID(func(imm catalog.Descriptor) error {
		table, ok := imm.(catalog.TableDescriptor)
		if ok && imm.IsUncommittedVersion() {
			tables = append(tables, table)
		}
		return nil
	})
	return tables
}

func (ud *uncommittedDescriptors) getUncommittedDescriptorsForValidation() (
	descs []catalog.Descriptor,
) {
	_ = ud.iterateImmutableByID(func(imm catalog.Descriptor) error {
		// TODO(postamar): only return descriptors with !IsUncommittedVersion()
		// This requires safeguard mechanisms like actually enforcing uncommitted
		// descriptor check=out and check-in rules.
		descs = append(descs, imm)
		return nil
	})
	return descs
}

func (ud *uncommittedDescriptors) hasUncommittedTables() (has bool) {
	_ = ud.iterateImmutableByID(func(desc catalog.Descriptor) error {
		if _, has = desc.(catalog.TableDescriptor); has {
			return iterutil.StopIteration()
		}
		return nil
	})
	return has
}

func (ud *uncommittedDescriptors) hasUncommittedTypes() (has bool) {
	_ = ud.iterateImmutableByID(func(desc catalog.Descriptor) error {
		if _, has = desc.(catalog.TypeDescriptor); has {
			return iterutil.StopIteration()
		}
		return nil
	})
	return has
}

var systemUncommittedDatabase = &uncommittedDescriptor{
	immutable: dbdesc.NewBuilder(systemschema.SystemDB.DatabaseDesc()).
		BuildImmutableDatabase(),
	// Note that the mutable field is left as nil. We'll generate a new
	// value lazily when this is needed, which ought to be exceedingly rare.
}

func (ud *uncommittedDescriptors) maybeAddSystemDatabase() {
	if !ud.addedSystemDatabase {
		ud.addedSystemDatabase = true
		ud.descs.Upsert(systemUncommittedDatabase)
	}
}
