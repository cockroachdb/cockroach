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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/typedesc"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/errors"
)

// uncommittedDescriptors is the data structure holding all uncommitted
// descriptors for a Collection.
// This allows a transaction to see its own modifications while bypassing the
// descriptor lease mechanism. The lease mechanism will have its own transaction
// to read the descriptor and will hang waiting for the uncommitted changes to
// the descriptor if this transaction is PRIORITY HIGH. These descriptors are
// local to this Collection and their state is thus not visible to other
// transactions.
type uncommittedDescriptors struct {

	// These maps store the descriptors which have undergone mutations in the
	// current transaction:
	// - original stores the original state of such descriptors in storage, unless
	//   they are new.
	// - uncommitted stores the uncommitted state of such descriptors, that is to
	//   say the state they were in when the AddUncommittedDescriptor method was
	//   called, which is done (among other things) when the mutated descriptor
	//   is persisted to storage via WriteDescToBatch. These descriptors are
	//   immutable and this map serves as the reference when querying this layer.
	// - mutable stores the mutable descriptors associated with the uncommitted
	//   descriptors in this collection. These are always the same in-memory
	//   objects for each ID for the duration of the transaction.
	uncommitted       nstree.NameMap
	original, mutable nstree.IDMap

	// memAcc is the actual account of an injected, upstream monitor
	// to track memory usage of uncommittedDescriptors.
	memAcc mon.BoundAccount
}

func makeUncommittedDescriptors(monitor *mon.BytesMonitor) uncommittedDescriptors {
	return uncommittedDescriptors{
		memAcc: monitor.MakeBoundAccount(),
	}
}

// reset zeroes the object for re-use in a new transaction.
func (ud *uncommittedDescriptors) reset(ctx context.Context) {
	ud.original.Clear()
	ud.uncommitted.Clear()
	ud.mutable.Clear()
	ud.memAcc.Clear(ctx)
	old := *ud
	*ud = uncommittedDescriptors{
		original:    old.original,
		uncommitted: old.uncommitted,
		mutable:     old.mutable,
		memAcc:      old.memAcc,
	}
}

// getUncommittedByID returns the uncommitted descriptor for this ID, if it
// exists.
func (ud *uncommittedDescriptors) getUncommittedByID(id descpb.ID) catalog.Descriptor {
	if e := ud.uncommitted.GetByID(id); e != nil {
		return e.(catalog.Descriptor)
	}
	return nil
}

// getUncommittedMutableByID returns the original descriptor as well as the
// uncommitted mutable descriptor for this ID if they exist.
func (ud *uncommittedDescriptors) getUncommittedMutableByID(
	id descpb.ID,
) (original catalog.Descriptor, mutable catalog.MutableDescriptor) {
	if ud.uncommitted.GetByID(id) == nil {
		return nil, nil
	}
	if me := ud.mutable.Get(id); me != nil {
		mutable = me.(catalog.MutableDescriptor)
	}
	if oe := ud.original.Get(id); oe != nil {
		original = oe.(catalog.Descriptor)
	}
	return original, mutable
}

// getUncommittedByName returns the uncommitted descriptor for this name key, if
// it exists.
func (ud *uncommittedDescriptors) getUncommittedByName(
	parentID, parentSchemaID descpb.ID, name string,
) catalog.Descriptor {
	if e := ud.uncommitted.GetByName(parentID, parentSchemaID, name); e != nil {
		return e.(catalog.Descriptor)
	}
	return nil
}

// iterateNewVersionByID applies fn to each lease.IDVersion from the original
// descriptor, if it exists, for each uncommitted descriptor, in ascending
// sequence of IDs.
func (ud *uncommittedDescriptors) iterateNewVersionByID(
	fn func(originalVersion lease.IDVersion) error,
) error {
	return ud.uncommitted.IterateByID(func(entry catalog.NameEntry) error {
		if o := ud.original.Get(entry.GetID()); o != nil {
			return fn(lease.NewIDVersionPrev(o.GetName(), o.GetID(), o.(catalog.Descriptor).GetVersion()))
		}
		return nil
	})
}

// iterateUncommittedByID applies fn to the uncommitted descriptors in ascending
// sequence of IDs.
func (ud *uncommittedDescriptors) iterateUncommittedByID(
	fn func(desc catalog.Descriptor) error,
) error {
	return ud.uncommitted.IterateByID(func(entry catalog.NameEntry) error {
		return fn(entry.(catalog.Descriptor))
	})
}

// ensureMutable is called when mutable descriptors are queried by the
// collection. This is a prerequisite to accepting any subsequent
// AddUncommittedDescriptor calls for any of these descriptors.
// Calling this does not change the results of any queries on this layer.
func (ud *uncommittedDescriptors) ensureMutable(
	ctx context.Context, original catalog.Descriptor,
) (catalog.MutableDescriptor, error) {
	if e := ud.mutable.Get(original.GetID()); e != nil {
		return e.(catalog.MutableDescriptor), nil
	}
	mut := original.NewBuilder().BuildExistingMutable()
	if original.GetID() == keys.SystemDatabaseID {
		return mut, nil
	}
	if err := ud.memAcc.Grow(ctx, mut.ByteSize()); err != nil {
		return nil, errors.Wrap(err, "Memory usage exceeds limit for uncommittedDescriptors")
	}
	ud.original.Upsert(original)
	ud.mutable.Upsert(mut)
	return mut, nil
}

// upsert adds an uncommitted descriptor to this layer.
// This is called exclusively by the Collection's AddUncommittedDescriptor
// method.
func (ud *uncommittedDescriptors) upsert(
	ctx context.Context, original catalog.Descriptor, mut catalog.MutableDescriptor,
) (err error) {
	// Refresh the cached fields on mutable type descriptors.
	if typ, ok := mut.(*typedesc.Mutable); ok {
		mut, err = typedesc.UpdateCachedFieldsOnModifiedMutable(typ)
		if err != nil {
			return err
		}
	}
	// Add the descriptors to the uncommitted descriptors layer.
	imm := mut.ImmutableCopy()
	newBytes := imm.ByteSize()
	if mut.IsNew() {
		newBytes += mut.ByteSize()
	}
	if err = ud.memAcc.Grow(ctx, newBytes); err != nil {
		return errors.Wrap(err, "Memory usage exceeds limit for uncommittedDescriptors")
	}
	ud.mutable.Upsert(mut)
	ud.uncommitted.Upsert(imm, imm.SkipNamespace())
	if original != nil {
		ud.original.Upsert(original)
	}
	return nil
}
