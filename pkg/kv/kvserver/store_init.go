// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvstorage"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// WriteInitialClusterData writes initialization data to an engine. It creates
// system ranges (filling in meta1 and meta2) and the default zone config.
//
// Args:
// eng: the engine to which data is to be written.
// initialValues: an optional list of k/v to be written as well after each
//
//	value's checksum is initialized.
//
// bootstrapVersion: the version at which the cluster is bootstrapped.
// numStores: the number of stores this node will have.
// splits: an optional list of split points. Range addressing will be created
//
//	for all the splits. The list needs to be sorted.
//
// nowNanos: the timestamp at which to write the initial engine data.
func WriteInitialClusterData(
	ctx context.Context,
	eng storage.Engine,
	initialValues []roachpb.KeyValue,
	bootstrapVersion roachpb.Version,
	numStores int,
	splits []roachpb.RKey,
	nowNanos int64,
	knobs StoreTestingKnobs,
) error {
	// Bootstrap version information. We'll add the "bootstrap version" to the
	// list of initialValues, so that we don't have to handle it specially
	// (particularly since we don't want to manually figure out which range it
	// falls into).
	bootstrapVal := roachpb.Value{}
	if err := bootstrapVal.SetProto(&bootstrapVersion); err != nil {
		return err
	}
	initialValues = append(initialValues,
		roachpb.KeyValue{Key: keys.BootstrapVersionKey, Value: bootstrapVal})

	// Initialize various sequence generators.
	var nodeIDVal, storeIDVal, rangeIDVal, livenessVal roachpb.Value

	nodeIDVal.SetInt(int64(kvstorage.FirstNodeID))
	// The caller will initialize the stores with ids FirstStoreID, ..., FirstStoreID+numStores-1.
	storeIDVal.SetInt(int64(kvstorage.FirstStoreID) + int64(numStores) - 1)
	// The last range has id = len(splits) + 1
	rangeIDVal.SetInt(int64(len(splits) + 1))

	// We're the first node in the cluster, let's seed our liveness record.
	// It's crucial that we do to maintain the invariant that there's always a
	// liveness record for a given node. We'll do something similar through the
	// join RPC when adding new nodes to an already bootstrapped cluster [1].
	//
	// We start off at epoch=0; when nodes heartbeat their liveness records for
	// the first time it'll get incremented to epoch=1 [2].
	//
	// [1]: See `(*NodeLiveness).CreateLivenessRecord` and usages for where that happens.
	// [2]: See `(*NodeLiveness).Start` for where that happens.
	livenessRecord := livenesspb.Liveness{NodeID: kvstorage.FirstNodeID, Epoch: 0}
	if err := livenessVal.SetProto(&livenessRecord); err != nil {
		return err
	}
	initialValues = append(initialValues,
		roachpb.KeyValue{Key: keys.NodeIDGenerator, Value: nodeIDVal},
		roachpb.KeyValue{Key: keys.StoreIDGenerator, Value: storeIDVal},
		roachpb.KeyValue{Key: keys.RangeIDGenerator, Value: rangeIDVal},
		roachpb.KeyValue{Key: keys.NodeLivenessKey(kvstorage.FirstNodeID), Value: livenessVal})

	// secondRangeMS is going to accumulate the stats for the first range, as we
	// write the meta records for all the other ranges.
	secondRangeMS := &enginepb.MVCCStats{}

	// filter initial values for a given descriptor, returning only the ones that
	// pertain to the respective range.
	filterInitialValues := func(desc *roachpb.RangeDescriptor) []roachpb.KeyValue {
		var r []roachpb.KeyValue
		for _, kv := range initialValues {
			if desc.ContainsKey(roachpb.RKey(kv.Key)) {
				r = append(r, kv)
			}
		}
		return r
	}

	initialReplicaVersion := bootstrapVersion
	if knobs.InitialReplicaVersionOverride != nil {
		initialReplicaVersion = *knobs.InitialReplicaVersionOverride
	}

	// We iterate through the ranges backwards, since they all need to contribute
	// to the stats of the second range (i.e. because they all write meta2 records
	// in the second range), and so we want to create the second range in the end
	// so that the stats we compute for it are correct.
	startKey := roachpb.RKeyMax
	for i := len(splits) - 1; i >= -1; i-- {
		endKey := startKey
		if i >= 0 {
			startKey = splits[i]
		} else {
			startKey = roachpb.RKeyMin
		}

		rangeID := roachpb.RangeID(i + 1) // RangeIDs are 1-based.
		if startKey.Equal(keys.Meta2Prefix) {
			rangeID = roachpb.RangeID(len(splits) + 1)
		}

		if i == -1 {
			rangeID = roachpb.RangeID(1)
		}

		desc := &roachpb.RangeDescriptor{
			RangeID:       rangeID,
			StartKey:      startKey,
			EndKey:        endKey,
			NextReplicaID: 2,
		}
		const firstReplicaID = 1
		replicas := []roachpb.ReplicaDescriptor{
			{
				NodeID:    kvstorage.FirstNodeID,
				StoreID:   kvstorage.FirstStoreID,
				ReplicaID: firstReplicaID,
			},
		}
		desc.SetReplicas(roachpb.MakeReplicaSet(replicas))
		if err := desc.Validate(); err != nil {
			return err
		}
		rangeInitialValues := filterInitialValues(desc)
		log.VEventf(
			ctx, 2, "creating range %d [%s, %s). Initial values: %d",
			desc.RangeID, desc.StartKey, desc.EndKey, len(rangeInitialValues))

		err := func() error {
			batch := eng.NewBatch()
			defer batch.Close()

			now := hlc.Timestamp{
				WallTime: nowNanos,
				Logical:  0,
			}

			// NOTE: We don't do stats computations in any of the puts below. Instead,
			// we write everything and then compute the stats over the whole range.

			// If requested, write an MVCC range tombstone at the bottom of the
			// keyspace, for performance and correctness testing.
			if knobs.GlobalMVCCRangeTombstone {
				if err := writeGlobalMVCCRangeTombstone(ctx, batch, desc, now.Prev()); err != nil {
					return err
				}
			}

			// Range descriptor.
			if err := storage.MVCCPutProto(
				ctx, batch, keys.RangeDescriptorKey(desc.StartKey),
				now, desc, storage.MVCCWriteOptions{},
			); err != nil {
				return err
			}

			// Replica GC timestamp.
			if err := storage.MVCCPutProto(
				ctx, batch, keys.RangeLastReplicaGCTimestampKey(desc.RangeID),
				hlc.Timestamp{}, &now, storage.MVCCWriteOptions{},
			); err != nil {
				return err
			}

			// Set the last processed timestamp for the consistency checker as "now".
			// This helps delay running the consistency checker for
			// 'server.consistency_check.interval'. Note that splitting this range
			// will copy the last processed timestamp to the right hand side, so newly
			// split ranges will also delay running the consistency checker. This
			// should improve the performance in workloads that cause many range
			// splits by delaying the consistency checker.
			if err := storage.MVCCPutProto(
				ctx, batch, keys.QueueLastProcessedKey(desc.StartKey, "consistencyChecker"),
				hlc.Timestamp{}, &now, storage.MVCCWriteOptions{},
			); err != nil {
				return err
			}

			// Range addressing for meta2.
			meta2Key := keys.RangeMetaKey(endKey)
			//fmt.Printf("meta2Key: %s, desc: %+v\n", meta2Key, desc)
			if err := storage.MVCCPutProto(
				ctx, batch, meta2Key.AsRawKey(),
				now, desc, storage.MVCCWriteOptions{Stats: secondRangeMS},
			); err != nil {
				return err
			}

			// Write the meta1 record for the range containing meta2 keys.
			// - If meta1 and meta2 are split (Meta2Prefix in splits list), this is the
			//   range starting at Meta2Prefix.
			// - If there's only one range (empty splits list), this is the range starting
			//   at /Min which contains everything including meta1 and meta2.
			// We need to check if Meta2Prefix is a split point to distinguish these cases.
			//
			// Note: We do NOT write r1 (the meta1 range) to meta1, as it's the root of
			// the addressing hierarchy. Only r2 (meta2 range) and beyond get indexed.
			shouldWriteMeta1 := false
			if startKey.Equal(keys.Meta2Prefix) {
				// This is the meta2 range in a split setup.
				shouldWriteMeta1 = true
			} else if startKey.Equal(roachpb.RKeyMin) {
				// This is the first range. Only write meta1 if Meta2Prefix is NOT a split
				// (i.e., this range contains both meta1 and meta2).
				isMeta2Split := false
				for _, split := range splits {
					if split.Equal(keys.Meta2Prefix) {
						isMeta2Split = true
						break
					}
				}
				shouldWriteMeta1 = !isMeta2Split
			}

			if shouldWriteMeta1 {
				// The range descriptor is stored in meta1.
				meta1Key := keys.RangeMetaKey(roachpb.RKey(keys.Meta2KeyMax)) // range addressing for meta1
				//fmt.Printf("meta1Key: %s, desc: %+v\n", meta1Key, desc)
				log.KvExec.Infof(ctx, "writing meta1 record for range %d at key %s", rangeID, meta1Key)
				if err := storage.MVCCPutProto(
					ctx, batch, meta1Key.AsRawKey(), now, desc, storage.MVCCWriteOptions{},
				); err != nil {
					return err
				}
			}

			// Now add all passed-in default entries.
			for _, kv := range rangeInitialValues {
				// Initialize the checksums.
				kv.Value.InitChecksum(kv.Key)
				if _, err := storage.MVCCPut(
					ctx, batch, kv.Key, now, kv.Value, storage.MVCCWriteOptions{},
				); err != nil {
					return err
				}
			}

			if err := stateloader.WriteInitialRangeState(
				ctx, batch, *desc, firstReplicaID, initialReplicaVersion); err != nil {
				return err
			}
			computedStats, err := rditer.ComputeStatsForRange(ctx, desc, batch, now.WallTime)
			if err != nil {
				return err
			}

			sl := stateloader.Make(rangeID)
			if err := sl.SetMVCCStats(ctx, batch, &computedStats); err != nil {
				return err
			}

			return batch.Commit(true /* sync */)
		}()
		if err != nil {
			return err
		}
	}

	return nil
}

// writeGlobalMVCCRangeTombstone writes an MVCC range tombstone across the
// entire table data keyspace of the range. This is used to test that storage
// operations are correct and performant in the presence of range tombstones. An
// MVCC range tombstone below all other data should in principle not affect
// anything at all.
func writeGlobalMVCCRangeTombstone(
	ctx context.Context, w storage.Writer, desc *roachpb.RangeDescriptor, ts hlc.Timestamp,
) error {
	rangeKey := storage.MVCCRangeKey{
		StartKey:  desc.StartKey.AsRawKey(),
		EndKey:    desc.EndKey.AsRawKey(),
		Timestamp: ts,
	}
	if rangeKey.EndKey.Compare(keys.TableDataMin) <= 0 {
		return nil
	}
	if rangeKey.StartKey.Compare(keys.TableDataMin) < 0 {
		rangeKey.StartKey = keys.TableDataMin
	}
	if err := w.PutMVCCRangeKey(rangeKey, storage.MVCCValue{}); err != nil {
		return err
	}
	log.KvDistribution.Warningf(ctx, "wrote global MVCC range tombstone %s", rangeKey)
	return nil
}
