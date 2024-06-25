// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package leases

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftutil"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// ErrMarkLeaseTransferRejectedBecauseTargetMayNeedSnapshot indicates that the
// lease transfer failed because the current leaseholder could not prove that
// the lease transfer target did not need a Raft snapshot. In order to prove
// this, the current leaseholder must also be the Raft leader, which is
// periodically requested in maybeTransferRaftLeadershipToLeaseholderLocked.
var ErrMarkLeaseTransferRejectedBecauseTargetMayNeedSnapshot = errors.New(
	"lease transfer rejected because the target may need a snapshot")

// NewLeaseTransferRejectedBecauseTargetMayNeedSnapshotError return an error
// indicating that a lease transfer failed because the current leaseholder could
// not prove that the lease transfer target did not need a Raft snapshot.
func NewLeaseTransferRejectedBecauseTargetMayNeedSnapshotError(
	target roachpb.ReplicaDescriptor, snapStatus raftutil.ReplicaNeedsSnapshotStatus,
) error {
	err := errors.Errorf("refusing to transfer lease to %d because target may need a Raft snapshot: %s",
		target, snapStatus)
	return errors.Mark(err, ErrMarkLeaseTransferRejectedBecauseTargetMayNeedSnapshot)
}
