// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvstorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

type RangeTombstoneChecker struct {
	key roachpb.Key
	eng storage.Reader
}

func NewRangeTombstoneChecker(id roachpb.RangeID, eng storage.Reader) RangeTombstoneChecker {
	return RangeTombstoneChecker{key: keys.RangeTombstoneKey(id), eng: eng}
}

// Check checks.
// NB: id is never nil. Equivalently, id 0 is always deleted.
func (r *RangeTombstoneChecker) Check(
	ctx context.Context, id roachpb.ReplicaID,
) (roachpb.ReplicaID, error) {
	var tombstone roachpb.RangeTombstone
	if ok, err := storage.MVCCGetProto(
		ctx, r.eng, r.key, hlc.Timestamp{}, &tombstone, storage.MVCCGetOptions{},
	); err != nil {
		return 0, err
	} else if ok && id < tombstone.NextReplicaID {
		return tombstone.NextReplicaID, &roachpb.RaftGroupDeletedError{}
	}
	return tombstone.NextReplicaID, nil
}
