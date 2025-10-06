// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descs

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
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

// getOriginalByID returns the original version of the uncommitted descriptor
// with this ID, if it exists.
func (ud *uncommittedDescriptors) getOriginalByID(id descpb.ID) catalog.Descriptor {
	if e := ud.original.Get(id); e != nil {
		return e.(catalog.Descriptor)
	}
	return nil
}

// getUncommittedByID returns the uncommitted descriptor for this ID, if it
// exists.
func (ud *uncommittedDescriptors) getUncommittedByID(id descpb.ID) catalog.Descriptor {
	if e := ud.uncommitted.GetByID(id); e != nil {
		return e.(catalog.Descriptor)
	}
	return nil
}

// getUncommittedMutableByID returns the uncommitted descriptor for this ID, if
// it exists, in mutable form. This mutable descriptor is owned by the
// collection.
func (ud *uncommittedDescriptors) getUncommittedMutableByID(
	id descpb.ID,
) catalog.MutableDescriptor {
	if me := ud.mutable.Get(id); me != nil && ud.uncommitted.GetByID(id) != nil {
		return me.(catalog.MutableDescriptor)
	}
	return nil
}

// getUncommittedByName returns the uncommitted descriptor for this name key, if
// it exists.
func (ud *uncommittedDescriptors) getUncommittedByName(
	parentID, parentSchemaID descpb.ID, name string,
) catalog.NameEntry {
	return ud.uncommitted.GetByName(parentID, parentSchemaID, name)
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
	newBytes := mut.ByteSize()
	ud.mutable.Upsert(mut)
	original = original.NewBuilder().BuildImmutable()
	newBytes += original.ByteSize()
	ud.original.Upsert(original)
	if err := ud.memAcc.Grow(ctx, newBytes); err != nil {
		return nil, errors.Wrap(err, "memory usage exceeds limit for uncommittedDescriptors")
	}
	return mut, nil
}

// upsert adds an uncommitted descriptor to this layer.
// This is called exclusively by the Collection's AddUncommittedDescriptor
// method.
func (ud *uncommittedDescriptors) upsert(
	ctx context.Context, mut catalog.MutableDescriptor,
) (err error) {
	original := ud.getOriginalByID(mut.GetID())
	// Perform some sanity checks to ensure the version counters are correct.
	if original == nil {
		if !mut.IsNew() {
			return errors.AssertionFailedf("non-new descriptor does not exist in storage yet")
		}
		if mut.GetVersion() != 1 {
			return errors.New("new descriptor version should be 1")
		}
	} else {
		if mut.IsNew() {
			return errors.AssertionFailedf("new descriptor already exists in storage")
		}
		if mut.GetVersion() != original.GetVersion()+1 {
			return errors.AssertionFailedf("expected uncommitted version %d, instead got %d",
				original.GetVersion()+1, mut.GetVersion())
		}
	}
	// Refresh the cached fields on mutable type descriptors.
	if typ, ok := mut.(*typedesc.Mutable); ok {
		mut, err = typedesc.UpdateCachedFieldsOnModifiedMutable(typ)
		if err != nil {
			return err
		}
	}
	// Add the descriptors to the uncommitted descriptors layer.
	var newBytes int64
	if mut.IsNew() {
		newBytes += mut.ByteSize()
	}
	ud.mutable.Upsert(mut)
	// Upserting a descriptor into the "uncommitted" set implies
	// this descriptor is going to be written to storage very soon. We
	// compute what the raw bytes of this descriptor in storage is going to
	// look like when that write happens, so that any further update to this
	// descriptor, which will be retrieved from the "uncommitted" set, will
	// carry the correct raw bytes in storage with it.
	var val roachpb.Value
	if err = val.SetProto(mut.DescriptorProto()); err != nil {
		return err
	}
	rawBytesInStorageAfterPendingWrite := val.TagAndDataBytes()
	uncommittedBuilder := mut.NewBuilder()
	uncommittedBuilder.SetRawBytesInStorage(rawBytesInStorageAfterPendingWrite)
	uncommitted := uncommittedBuilder.BuildImmutable()
	newBytes += uncommitted.ByteSize()
	ud.uncommitted.Upsert(uncommitted, uncommitted.SkipNamespace())
	if err = ud.memAcc.Grow(ctx, newBytes); err != nil {
		return errors.Wrap(err, "memory usage exceeds limit for uncommittedDescriptors")
	}
	return nil
}
