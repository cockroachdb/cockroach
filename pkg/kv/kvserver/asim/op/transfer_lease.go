// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package op

import (
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/types"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// TransferLeaseOp conatins the information for a transfer lease operation.
type TransferLeaseOp struct {
	baseOp
	source, target state.StoreID
	rangeID        state.RangeID
}

// NewTransferLeaseOp returns a new TransferLeaseOp.
func NewTransferLeaseOp(
	tick types.Tick, rangeID roachpb.RangeID, source, target roachpb.StoreID,
) *TransferLeaseOp {
	return &TransferLeaseOp{
		baseOp:  newBaseOp(tick),
		source:  state.StoreID(source),
		target:  state.StoreID(target),
		rangeID: state.RangeID(rangeID),
	}
}

func (tlo *TransferLeaseOp) error(err error) {
	augmentedErr := errors.Wrapf(err, "Unable to transfer lease r=%d source=%d,target=%d",
		tlo.rangeID, tlo.source, tlo.target)
	tlo.errs = append(tlo.errs, augmentedErr)
}
