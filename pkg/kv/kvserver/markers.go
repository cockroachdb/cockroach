// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/leases"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// errMarkSnapshotError is used as an error mark for errors that get returned
// to the initiator of a snapshot. This generally classifies errors as transient,
// i.e. communicates an intention for the caller to retry.
//
// NB: don't change the string here; this will cause cross-version issues
// since this singleton is used as a marker.
var errMarkSnapshotError = errors.New("snapshot failed")

// isSnapshotError returns true iff the error indicates that a snapshot failed.
func isSnapshotError(err error) bool {
	return errors.Is(err, errMarkSnapshotError)
}

// NB: don't change the string here; this will cause cross-version issues
// since this singleton is used as a marker.
var errMarkCanRetryReplicationChangeWithUpdatedDesc = errors.New("should retry with updated descriptor")

// IsRetriableReplicationChangeError detects whether an error (which is
// assumed to have been emitted by the KV layer during a replication change
// operation) is likely to succeed when retried with an updated descriptor.
func IsRetriableReplicationChangeError(err error) bool {
	return errors.Is(err, errMarkCanRetryReplicationChangeWithUpdatedDesc) ||
		IsLeaseTransferRejectedBecauseTargetMayNeedSnapshotError(err) ||
		IsLeaseTransferRejectedBecauseTargetHasSendQueueError(err) ||
		IsLeaseTransferRejectedBecauseTargetCannotReceiveLease(err) ||
		isSnapshotError(err)
}

const (
	descChangedErrorFmt              = "descriptor changed: [expected] %s != [actual] %s"
	descChangedRangeReplacedErrorFmt = "descriptor changed: [expected] %s != [actual] %s  (range replaced)"
	descChangedRangeSubsumedErrorFmt = "descriptor changed: [expected] %s != [actual] nil (range subsumed)"
)

func newDescChangedError(desc, actualDesc *roachpb.RangeDescriptor) error {
	var err error
	if actualDesc == nil {
		err = errors.Newf(descChangedRangeSubsumedErrorFmt, desc)
	} else if desc.RangeID != actualDesc.RangeID {
		err = errors.Newf(descChangedRangeReplacedErrorFmt, desc, actualDesc)
	} else {
		err = errors.Newf(descChangedErrorFmt, desc, actualDesc)
	}
	return errors.Mark(err, errMarkCanRetryReplicationChangeWithUpdatedDesc)
}

func wrapDescChangedError(err error, desc, actualDesc *roachpb.RangeDescriptor) error {
	var wrapped error
	if actualDesc == nil {
		wrapped = errors.Wrapf(err, descChangedRangeSubsumedErrorFmt, desc)
	} else if desc.RangeID != actualDesc.RangeID {
		wrapped = errors.Wrapf(err, descChangedRangeReplacedErrorFmt, desc, actualDesc)
	} else {
		wrapped = errors.Wrapf(err, descChangedErrorFmt, desc, actualDesc)
	}
	return errors.Mark(wrapped, errMarkCanRetryReplicationChangeWithUpdatedDesc)
}

// NB: don't change the string here; this will cause cross-version issues
// since this singleton is used as a marker.
var errMarkInvalidReplicationChange = errors.New("invalid replication change")

// IsIllegalReplicationChangeError detects whether an error (assumed to have been emitted
// by a replication change) indicates that the replication change is illegal, meaning
// that it's unlikely to be handled through a retry. Examples of this are attempts to add
// a store that is already a member of the supplied descriptor. A non-example is a change
// detected in the descriptor at the KV layer relative to that supplied as input to the
// replication change, which would likely benefit from a retry.
func IsIllegalReplicationChangeError(err error) bool {
	return errors.Is(err, errMarkInvalidReplicationChange)
}

var errMarkReplicationChangeInProgress = errors.New("replication change in progress")

// IsReplicationChangeInProgressError detects whether an error (assumed to have
// been emitted by a replication change) indicates that the replication change
// failed because another replication change was in progress on the range.
func IsReplicationChangeInProgressError(err error) bool {
	return errors.Is(err, errMarkReplicationChangeInProgress)
}

// IsLeaseTransferRejectedBecauseTargetMayNeedSnapshotError detects whether an
// error (assumed to have been emitted by a lease transfer request) indicates
// that the lease transfer failed because the current leaseholder could not
// prove that the lease transfer target did not need a Raft snapshot. In order
// to prove this, the current leaseholder must also be the Raft leader, which is
// periodically requested in maybeTransferRaftLeadershipToLeaseholderLocked.
func IsLeaseTransferRejectedBecauseTargetMayNeedSnapshotError(err error) bool {
	return errors.Is(err, leases.ErrMarkLeaseTransferRejectedBecauseTargetMayNeedSnapshot)
}

// IsLeaseTransferRejectedBecauseTargetHasSendQueueError detects whether an
// error (assumed to have been emitted by a lease transfer request) indicates
// that the lease transfer failed because the lease transfer target has a send
// queue, meaning it is behind on its raft log.
func IsLeaseTransferRejectedBecauseTargetHasSendQueueError(err error) bool {
	return errors.Is(err, leases.ErrMarkLeaseTransferRejectedBecauseTargetHasSendQueue)
}

// IsLeaseTransferRejectedBecauseTargetCannotReceiveLease returns true if err
// (assumed to have been emitted by the current leaseholder when processing a
// lease transfer request) indicates that the target replica is not qualified to
// receive the lease. This could be because the current leaseholder has an
// outdated view of the target replica's state.
func IsLeaseTransferRejectedBecauseTargetCannotReceiveLease(err error) bool {
	return errors.Is(err, roachpb.ErrReplicaNotFound) ||
		errors.Is(err, roachpb.ErrReplicaCannotHoldLease)
}
