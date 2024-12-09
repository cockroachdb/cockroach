// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package descs

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/nstree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

type LeaseManager interface {
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

	IncGaugeAfterLeaseDuration(
		gaugeType lease.AfterLeaseDurationGauge,
	) (decrAfterWait func())

	GetSafeReplicationTS() hlc.Timestamp

	GetLeaseGeneration() int64
}

type deadlineHolder interface {
	ReadTimestamp() hlc.Timestamp
	UpdateDeadline(ctx context.Context, deadline hlc.Timestamp) error
}

// maxTimestampBoundDeadlineHolder is an implementation of deadlineHolder
// which is intended for use during bounded staleness reads.
type maxTimestampBoundDeadlineHolder struct {
	maxTimestampBound hlc.Timestamp
}

// ReadTimestamp implements the deadlineHolder interface.
func (m maxTimestampBoundDeadlineHolder) ReadTimestamp() hlc.Timestamp {
	// We return .Prev() because maxTimestampBound is an exclusive upper bound.
	return m.maxTimestampBound.Prev()
}

// UpdateDeadline implements the deadlineHolder interface.
func (m maxTimestampBoundDeadlineHolder) UpdateDeadline(
	ctx context.Context, deadline hlc.Timestamp,
) error {
	return nil
}

func makeLeasedDescriptors(lm LeaseManager) leasedDescriptors {
	return leasedDescriptors{
		lm: lm,
	}
}

// leasedDescriptors holds references to all the descriptors leased in the
// transaction, and supports access by name and by ID.
type leasedDescriptors struct {
	lm    LeaseManager
	cache nstree.NameMap
}

// mismatchedExternalDataRowTimestamp is generated when the external row data timestamps
// within a descriptor do not match.
type mismatchedExternalDataRowTimestamp struct {
	newDescName      string
	newDescID        descpb.ID
	newDescTS        hlc.Timestamp
	existingDescName string
	existingDescID   descpb.ID
	existingDescTS   hlc.Timestamp
}

func newMismatchedExternalDataRowTimestampError(
	newDesc catalog.TableDescriptor, existingDesc catalog.TableDescriptor,
) *mismatchedExternalDataRowTimestamp {
	return &mismatchedExternalDataRowTimestamp{
		newDescName:      newDesc.GetName(),
		newDescID:        newDesc.GetID(),
		newDescTS:        newDesc.ExternalRowData().AsOf,
		existingDescName: existingDesc.GetName(),
		existingDescID:   existingDesc.GetID(),
		existingDescTS:   existingDesc.ExternalRowData().AsOf,
	}
}

// ClientVisibleRetryError implements the ClientVisibleRetryError interface.
func (e *mismatchedExternalDataRowTimestamp) ClientVisibleRetryError() {
}

func (e *mismatchedExternalDataRowTimestamp) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("PCR reader timestamp has moved forward, "+
		"existing descriptor %s(%d) and timestamp: %s "+
		"new descritpor %s(%d) and timestamp: %s",
		e.newDescName,
		e.newDescID,
		e.newDescTS,
		e.existingDescName,
		e.existingDescID,
		e.existingDescTS)
	return nil
}

func (e *mismatchedExternalDataRowTimestamp) Error() string {
	return fmt.Sprint(errors.Formattable(e))
}

var _ errors.SafeFormatter = (*mismatchedExternalDataRowTimestamp)(nil)

// maybeAssertExternalRowDataTS asserts if the descriptor references external
// row data, then the timestamp across the entire collection *must* match.
func (ld *leasedDescriptors) maybeAssertExternalRowDataTS(desc catalog.Descriptor) error {
	tableDesc, ok := desc.(catalog.TableDescriptor)
	if !ok {
		return nil
	}
	if tableDesc.ExternalRowData() == nil {
		return nil
	}
	currentTS := tableDesc.ExternalRowData().AsOf
	return ld.cache.IterateByID(func(entry catalog.NameEntry) error {
		// Skip databases / schemas.
		if entry.GetParentID() == descpb.InvalidID ||
			entry.GetParentSchemaID() == descpb.InvalidID ||
			entry.GetID() == tableDesc.GetID() {
			return nil
		}
		// Next get the underlying descriptor.
		otherDesc := entry.(lease.LeasedDescriptor).Underlying()
		if otherTableDesc, ok := otherDesc.(catalog.TableDescriptor); ok {
			// Skip conventional descriptors.
			if otherTableDesc.ExternalRowData() == nil {
				return nil
			}
			// Confirm the timestamps match the most recent descriptor.
			if !otherTableDesc.ExternalRowData().AsOf.Equal(currentTS) {
				// Normally the PCR catalog reader will run with AOST timestamps,
				// if during the setup of the connection executor we were able to
				// lease the system database descriptor and confirm that it is for
				// a PCR reader catalog. If we were not able to lease the system database
				// descriptor, then its possible no AOST timestamp is set. Otherwise,
				// this error should *never* happen.
				return newMismatchedExternalDataRowTimestampError(
					tableDesc,
					otherTableDesc)
			}
			// Otherwise, we expect all other timestamps to match as well.
			return iterutil.StopIteration()
		}
		return nil
	})
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
		desc = cached.(lease.LeasedDescriptor).Underlying()
		return desc, false, nil
	}

	readTimestamp := txn.ReadTimestamp()
	ldesc, err := ld.lm.AcquireByName(ctx, readTimestamp, parentID, parentSchemaID, name)
	const setTxnDeadline = true
	return ld.getResult(ctx, txn, setTxnDeadline, ldesc, err)
}

// getByID return a leased descriptor valid for the transaction,
// acquiring one if necessary.
func (ld *leasedDescriptors) getByID(
	ctx context.Context, txn deadlineHolder, id descpb.ID,
) (_ catalog.Descriptor, shouldReadFromStore bool, _ error) {
	// First, look to see if we already have the table in the shared cache.
	if cached := ld.getCachedByID(ctx, id); cached != nil {
		return cached, false, nil
	}

	readTimestamp := txn.ReadTimestamp()
	desc, err := ld.lm.Acquire(ctx, readTimestamp, id)
	const setTxnDeadline = false
	return ld.getResult(ctx, txn, setTxnDeadline, desc, err)
}

func (ld *leasedDescriptors) getCachedByID(ctx context.Context, id descpb.ID) catalog.Descriptor {
	cached := ld.cache.GetByID(id)
	if cached == nil {
		return nil
	}
	if log.V(2) {
		log.Eventf(ctx, "found descriptor in collection for (%d, %d, '%s'): %d",
			cached.GetParentID(), cached.GetParentSchemaID(), cached.GetName(), id)
	}
	return cached.(lease.LeasedDescriptor).Underlying()
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
		_, isBoundedStalenessRead := txn.(*maxTimestampBoundDeadlineHolder)
		// Read the descriptor from the store in the face of some specific errors
		// because of a known limitation of AcquireByName. See the known
		// limitations of AcquireByName for details.
		// Note we never should read from store during a bounded staleness read,
		// as it is safe to return the schema as non-existent.
		if shouldReadFromStore =
			!isBoundedStalenessRead && ((catalog.HasInactiveDescriptorError(err) &&
				errors.Is(err, catalog.ErrDescriptorDropped)) ||
				errors.Is(err, catalog.ErrDescriptorNotFound)); shouldReadFromStore {
			return nil, true, nil
		}
		// Lease acquisition failed with some other error. This we don't
		// know how to deal with, so propagate the error.
		return nil, false, err
	}

	expiration := ldesc.Expiration(ctx)
	readTimestamp := txn.ReadTimestamp()
	if expiration.LessEq(txn.ReadTimestamp()) {
		return nil, false, errors.AssertionFailedf("bad descriptor for id=%d readTimestamp=%s, expiration=%s", ldesc.GetID(), readTimestamp, expiration)
	}

	ld.cache.Upsert(ldesc, ldesc.Underlying().SkipNamespace())
	if log.V(2) {
		log.Eventf(ctx, "added descriptor '%s' to collection: %+v", ldesc.GetName(), ldesc.Underlying())
	}

	// If the descriptor we just acquired expires before the txn's deadline,
	// reduce the deadline. We use ReadTimestamp() that doesn't return the commit
	// timestamp, so we need to set a deadline on the transaction to prevent it
	// from committing beyond the version's expiration time.
	if setDeadline {
		if err := ld.maybeUpdateDeadline(ctx, txn, nil); err != nil {
			return nil, false, err
		}
	}

	desc := ldesc.Underlying()
	if err = ld.maybeAssertExternalRowDataTS(desc); err != nil {
		return nil, false, err
	}
	return desc, false, nil
}

func (ld *leasedDescriptors) maybeUpdateDeadline(
	ctx context.Context, txn deadlineHolder, session sqlliveness.Session,
) error {
	// Set the transaction deadline to the minimum of the leased descriptor deadline
	// and session expiration. The sqlliveness.Session will only be set in the
	// multi-tenant environment for controlling transactions associated with ephemeral
	// SQL pods.
	//
	// TODO(andrei,ajwerner): Using the session expiration here makes no sense at
	// the moment. This was done with the mistaken impression that it'll do
	// something for transactions that use the unique_rowid() function, but it
	// doesn't (since that function cares about wall time, not the transaction's
	// commit timestamp). We've left this code in place, though, because we intend
	// to tie descriptor leases to sessions, at which point using the session
	// expiration as the deadline will serve a purpose.
	var deadline hlc.Timestamp
	if session != nil {
		deadline = session.Expiration()
	}
	if leaseDeadline, ok := ld.getDeadline(ctx); ok && (deadline.IsEmpty() || leaseDeadline.Less(deadline)) {
		// Set the deadline to the lease deadline if session expiration is empty
		// or lease deadline is less than the session expiration.
		deadline = leaseDeadline
	}
	// If the deadline has been set, update the transaction deadline.
	if !deadline.IsEmpty() {
		// If the deadline certainly cannot be met, return an error which will
		// be retried explicitly.
		if txnTs := txn.ReadTimestamp(); deadline.LessEq(txnTs) {
			return &deadlineExpiredError{
				txnTS:      txnTs,
				expiration: deadline,
			}
		}
		return txn.UpdateDeadline(ctx, deadline)
	}
	return nil
}

// deadlineExpiredError is returned when the deadline from either a descriptor
// lease or a sqlliveness session is before the current transaction timestamp.
// The error is a user-visible retry.
type deadlineExpiredError struct {
	txnTS, expiration hlc.Timestamp
}

func (e *deadlineExpiredError) SafeFormatError(p errors.Printer) (next error) {
	p.Printf("liveness session expired %v before transaction",
		e.txnTS.GoTime().Sub(e.expiration.GoTime()))
	return nil
}

func (e *deadlineExpiredError) ClientVisibleRetryError() {}

func (e *deadlineExpiredError) Error() string {
	return fmt.Sprint(errors.Formattable(e))
}

var _ errors.SafeFormatter = (*deadlineExpiredError)(nil)

func (ld *leasedDescriptors) getDeadline(
	ctx context.Context,
) (deadline hlc.Timestamp, haveDeadline bool) {
	_ = ld.cache.IterateByID(func(descriptor catalog.NameEntry) error {
		expiration := descriptor.(lease.LeasedDescriptor).Expiration(ctx)
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
