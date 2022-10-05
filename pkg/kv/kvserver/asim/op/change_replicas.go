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

// import (
// 	"time"
//
// 	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/asim/state"
// 	"github.com/cockroachdb/cockroach/pkg/roachpb"
// 	"github.com/cockroachdb/errors"
// )
//
// // ChangeReplicasOp conatins the information for a change replicas operation.
// type ChangeReplicasOp struct {
// 	baseOp
// 	add, remove state.StoreID
// 	rangeID     state.RangeID
// 	rangeSize   int64
// }
//
// // NewChangeReplicasOp returns a new ChangeReplicasOp.
// func NewChangeReplicasOp(
// 	tick time.Time, rangeID roachpb.RangeID, add, remove roachpb.StoreID, rangeSize int64,
// ) *ChangeReplicasOp {
// 	return &ChangeReplicasOp{
// 		baseOp:  newBaseOp(tick),
// 		add:     state.StoreID(add),
// 		remove:  state.StoreID(remove),
// 		rangeID: state.RangeID(remove),
// 	}
// }
//
// func (cro *ChangeReplicasOp) error(err error) {
// 	augmentedErr := errors.Wrapf(err, "Unable to change replicas r=%d add=%d,remove=%d",
// 		cro.rangeID, cro.add, cro.remove)
// 	cro.errs = append(cro.errs, augmentedErr)
// }
