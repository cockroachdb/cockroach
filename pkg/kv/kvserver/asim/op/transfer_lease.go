// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package op

import (
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/allocator"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

// TransferLeaseOp conatins the information for a transfer lease operation.
type TransferLeaseOp struct {
	baseOp
	source, target state.StoreID
	rangeID        state.RangeID
	usage          allocator.RangeUsageInfo
}

// NewTransferLeaseOp returns a new TransferLeaseOp.
func NewTransferLeaseOp(
	tick time.Time,
	rangeID roachpb.RangeID,
	source, target roachpb.StoreID,
	usage allocator.RangeUsageInfo,
) *TransferLeaseOp {
	return &TransferLeaseOp{
		baseOp:  newBaseOp(tick),
		source:  state.StoreID(source),
		target:  state.StoreID(target),
		rangeID: state.RangeID(rangeID),
		usage:   usage,
	}
}

func (tlo *TransferLeaseOp) error(err error) {
	augmentedErr := errors.Wrapf(err, "Unable to transfer lease r=%d source=%d,target=%d",
		tlo.rangeID, tlo.source, tlo.target)
	tlo.errs = append(tlo.errs, augmentedErr)
}
