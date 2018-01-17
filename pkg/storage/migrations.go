// Copyright 2017 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package storage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// clearLegacyTombstone removes the legacy tombstone for the given rangeID, taking
// care to do this without reading from the engine (as is required by one of the
// callers).
func clearLegacyTombstone(eng engine.Writer, rangeID roachpb.RangeID) error {
	return eng.Clear(engine.MakeMVCCMetadataKey(keys.RaftTombstoneIncorrectLegacyKey(rangeID)))
}

// migrateLegacyTombstones rewrites all legacy tombstones into the correct key.
// It can be removed in binaries post v2.1.
func migrateLegacyTombstones(ctx context.Context, eng engine.Engine) error {
	var tombstone roachpb.RaftTombstone
	handleTombstone := func(rangeID roachpb.RangeID) (more bool, _ error) {
		batch := eng.NewBatch()
		defer batch.Close()

		tombstoneKey := keys.RaftTombstoneKey(rangeID)

		{
			// If there's already a new-style tombstone, pick the larger NextReplicaID
			// (this will be the new-style tombstone's).
			var exTombstone roachpb.RaftTombstone
			ok, err := engine.MVCCGetProto(ctx, batch, tombstoneKey,
				hlc.Timestamp{}, true, nil, &exTombstone)
			if err != nil {
				return false, err
			}
			if ok && exTombstone.NextReplicaID > tombstone.NextReplicaID {
				tombstone.NextReplicaID = exTombstone.NextReplicaID
			}
		}

		if err := engine.MVCCPutProto(ctx, batch, nil, tombstoneKey,
			hlc.Timestamp{}, nil, &tombstone); err != nil {
			return false, err
		}
		if err := clearLegacyTombstone(batch, rangeID); err != nil {
			return false, err
		}
		// Specify sync==false because we don't want to sync individually,
		// but see the end of the surrounding method where we sync explicitly.
		err := batch.Commit(false /* sync */)
		return err == nil, err
	}
	err := IterateIDPrefixKeys(ctx, eng, keys.RaftTombstoneIncorrectLegacyKey, &tombstone,
		handleTombstone)
	if err != nil {
		return err
	}

	// Write a final bogus batch so that we get to do a sync commit, which
	// implicitly also syncs everything written before.
	batch := eng.NewBatch()
	defer batch.Close()

	if err := clearLegacyTombstone(batch, 1 /* rangeID */); err != nil {
		return err
	}
	return batch.Commit(true /* sync */)
}
