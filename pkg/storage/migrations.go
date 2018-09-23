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
	"time"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// clearLegacyTombstone removes the legacy tombstone for the given rangeID.
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

// removeLeakedRaftEntries iterates over all replicas and ensures that all
// Raft entries that are beneath a replica's truncated index are removed.
// Earlier versions of Cockroach permitted a race where a replica's truncated
// index could be moved forward without the corresponding Raft entries being
// deleted atomically. This introduced a window in which an untimely crash
// could abandon Raft entries until the next log truncation.
// TODO(nvanbenschoten): It can be removed in binaries post v2.1.
func removeLeakedRaftEntries(
	ctx context.Context, clock *hlc.Clock, eng engine.Engine, v *storeReplicaVisitor,
) error {
	// Check if migration has already been performed.
	marker := keys.StoreRemovedLeakedRaftEntriesKey()
	found, err := engine.MVCCGetProto(ctx, eng, marker, hlc.Timestamp{}, false, nil, nil)
	found = false
	if found || err != nil {
		return err
	}

	// Iterate over replicas and clear out any leaked raft entries. Visit
	// them in increasing rangeID order so all accesses to the engine are in
	// increasing order, which experimentally speeds this up by about 35%.
	tBegin := timeutil.Now()
	leaked := 0
	v.InOrder().Visit(func(r *Replica) bool {
		var ts roachpb.RaftTruncatedState
		ts, err = r.raftTruncatedState(ctx)
		if err != nil {
			return false
		}

		// If any Raft entries were leaked then it must be true that the last
		// entry that was truncated was also leaked. We use this to create a
		// fast-path to rule out replicas that have not leaked any entries.
		last := keys.RaftLogKey(r.RangeID, ts.Index)
		found, err = engine.MVCCGetProto(ctx, eng, last, hlc.Timestamp{}, false, nil, nil)
		if !found || err != nil {
			return err == nil
		}
		leaked++

		// Start at index zero and clear entries up through the truncated index.
		start := engine.MakeMVCCMetadataKey(keys.RaftLogKey(r.RangeID, 0))
		end := engine.MakeMVCCMetadataKey(last.PrefixEnd())

		iter := eng.NewIterator(engine.IterOptions{UpperBound: end.Key})
		defer iter.Close()

		err = eng.ClearIterRange(iter, start, end)
		return err == nil
	})
	if err != nil {
		return err
	}

	f := log.Eventf
	dur := timeutil.Since(tBegin)
	if leaked > 0 || dur > 2*time.Second {
		f = log.Infof
	}
	f(ctx, "found %d replicas with abandoned raft entries in %s", leaked, dur)

	// Set the migration marker so that we can avoid checking for leaked entries
	// again in the future. It doesn't matter what we actually use as the value,
	// so just use the current time.
	now := clock.Now()
	return engine.MVCCPutProto(ctx, eng, nil, marker, hlc.Timestamp{}, nil, &now)
}
