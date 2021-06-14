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

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type leaseManager interface {
	AcquireByName(
		ctx context.Context,
		timestamp hlc.Timestamp,
		parentID descpb.ID,
		parentSchemaID descpb.ID,
		name string,
	) (lease.LeasedDescriptor, error)

	Acquire(
		ctx context.Context, timestamp hlc.Timestamp, id descpb.ID,
	) (lease.LeasedDescriptor, error)
}

type deadlineHolder interface {
	ReadTimestamp() hlc.Timestamp
	UpdateDeadline(ctx context.Context, deadline hlc.Timestamp) error
}

func makeLeasedDescriptors(lm leaseManager) leasedDescriptors {
	return leasedDescriptors{
		lm:    lm,
		cache: nstree.MakeMap(),
	}
}

// leasedDescriptors holds references to all the descriptors leased in the
// transaction, and supports access by name and by ID.
type leasedDescriptors struct {
	lm    leaseManager
	cache nstree.Map
}

// getLeasedDescriptorByName return a leased descriptor valid for the
// transaction, acquiring one if necessary. Due to a bug in lease acquisition
// for dropped descriptors, the descriptor may have to be read from the store,
// in which case shouldReadFromStore will be true.
func (ld *leasedDescriptors) getByName(
	ctx context.Context,
	txn deadlineHolder,
	parentID descpb.ID,
	parentSchemaID descpb.ID,
	name string,
) (desc catalog.Descriptor, shouldReadFromStore bool, err error) {
	// First, look to see if we already have the descriptor.
	// This ensures that, once a SQL transaction resolved name N to id X, it will
	// continue to use N to refer to X even if N is renamed during the
	// transaction.
	if cached := ld.cache.GetByName(parentID, parentSchemaID, name); cached != nil {
		if log.V(2) {
			log.Eventf(ctx, "found descriptor in collection for (%d, %d, '%s'): %d",
				parentID, parentSchemaID, name, cached.GetID())
		}
		return cached.(lease.LeasedDescriptor).Underlying(), false, nil
	}

	readTimestamp := txn.ReadTimestamp()
	ldesc, err := ld.lm.AcquireByName(ctx, readTimestamp, parentID, parentSchemaID, name)
	const setTxnDeadline = true
	return ld.getResult(ctx, txn, setTxnDeadline, ldesc, err)
}

// getByID return a leased descriptor valid for the transaction,
// acquiring one if necessary.
// We set a deadline on the transaction based on the lease expiration, which is
// the usual case, unless setTxnDeadline is false.
func (ld *leasedDescriptors) getByID(
	ctx context.Context, txn deadlineHolder, id descpb.ID, setTxnDeadline bool,
) (_ catalog.Descriptor, shouldReadFromStore bool, _ error) {
	// First, look to see if we already have the table in the shared cache.
	if cached := ld.cache.GetByID(id); cached != nil {
		if log.V(2) {
			log.Eventf(ctx, "found descriptor in collection for (%d, %d, '%s'): %d",
				cached.GetParentID(), cached.GetParentSchemaID(), cached.GetName(), id)
		}
		return cached.(lease.LeasedDescriptor).Underlying(), false, nil
	}

	readTimestamp := txn.ReadTimestamp()
	desc, err := ld.lm.Acquire(ctx, readTimestamp, id)
	return ld.getResult(ctx, txn, setTxnDeadline, desc, err)
}

// getResult is a helper to deal with the result that comes back from Acquire
// or AcquireByName.
func (ld *leasedDescriptors) getResult(
	ctx context.Context,
	txn deadlineHolder,
	setDeadline bool,
	ldesc lease.LeasedDescriptor,
	err error,
) (_ catalog.Descriptor, shouldReadFromStore bool, _ error) {
	if err != nil {
		// Read the descriptor from the store in the face of some specific errors
		// because of a known limitation of AcquireByName. See the known
		// limitations of AcquireByName for details.
		if shouldReadFromStore =
			(catalog.HasInactiveDescriptorError(err) &&
				errors.Is(err, catalog.ErrDescriptorDropped)) ||
				errors.Is(err, catalog.ErrDescriptorNotFound); shouldReadFromStore {
			return nil, true, nil
		}
		// Lease acquisition failed with some other error. This we don't
		// know how to deal with, so propagate the error.
		return nil, false, err
	}

	expiration := ldesc.Expiration()
	readTimestamp := txn.ReadTimestamp()
	if expiration.LessEq(txn.ReadTimestamp()) {
		log.Fatalf(ctx, "bad descriptor for T=%s, expiration=%s", readTimestamp, expiration)
	}

	ld.cache.Upsert(ldesc)
	if log.V(2) {
		log.Eventf(ctx, "added descriptor '%s' to collection: %+v", ldesc.GetName(), ldesc.Underlying())
	}

	// If the descriptor we just acquired expires before the txn's deadline,
	// reduce the deadline. We use ReadTimestamp() that doesn't return the commit
	// timestamp, so we need to set a deadline on the transaction to prevent it
	// from committing beyond the version's expiration time.
	if setDeadline {
		if err := ld.maybeUpdateDeadline(ctx, txn); err != nil {
			return nil, false, err
		}
	}
	return ldesc.Underlying(), false, nil
}

func (ld *leasedDescriptors) maybeUpdateDeadline(ctx context.Context, txn deadlineHolder) error {
	if deadline, haveDeadline := ld.getDeadline(); haveDeadline {
		return txn.UpdateDeadline(ctx, deadline)
	}
	return nil
}

func (ld *leasedDescriptors) getDeadline() (deadline hlc.Timestamp, haveDeadline bool) {
	_ = ld.cache.IterateByID(func(descriptor catalog.NameEntry) error {
		expiration := descriptor.(lease.LeasedDescriptor).Expiration()
		if !haveDeadline || expiration.Less(deadline) {
			deadline, haveDeadline = expiration, true
		}
		return nil
	})
	return deadline, haveDeadline
}

func (ld *leasedDescriptors) releaseAll(ctx context.Context) {
	log.VEventf(ctx, 2, "releasing %d descriptors", ld.numDescriptors())
	_ = ld.cache.IterateByID(func(descriptor catalog.NameEntry) error {
		descriptor.(lease.LeasedDescriptor).Release(ctx)
		return nil
	})
	ld.cache.Clear()
}

func (ld *leasedDescriptors) release(ctx context.Context, descs []lease.IDVersion) {
	for _, idv := range descs {
		if removed := ld.cache.Remove(idv.ID); removed != nil {
			removed.(lease.LeasedDescriptor).Release(ctx)
		}
	}
}

func (ld *leasedDescriptors) numDescriptors() int {
	return ld.cache.Len()
}
