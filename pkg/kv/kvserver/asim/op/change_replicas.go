// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package op

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// ChangeReplicasOp contains the information for a change replicas operation.
type ChangeReplicasOp struct {
	baseOp
	rangeID state.RangeID
	changes kvpb.ReplicationChanges
}

// NewChangeReplicasOp returns a new ChangeReplicasOp.
// TODO(wenyihu6): unused for now - will be integrated with mma simulation
func NewChangeReplicasOp(
	tick time.Time, rangeID roachpb.RangeID, changes kvpb.ReplicationChanges,
) *ChangeReplicasOp {
	return &ChangeReplicasOp{
		baseOp:  newBaseOp(tick),
		rangeID: state.RangeID(rangeID),
		changes: changes,
	}
}

func (cro *ChangeReplicasOp) error(err error) {
	augmentedErr := errors.Wrapf(err, "Unable to change replicas for range_id=%d, changes=%v",
		cro.rangeID, cro.changes)
	cro.errs = append(cro.errs, augmentedErr)
}
