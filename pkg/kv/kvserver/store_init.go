// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rditer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// InitEngine writes a new store ident to the underlying engine. To
// ensure that no crufty data already exists in the engine, it scans
// the engine contents before writing the new store ident. The engine
// should be completely empty save for a cluster version, which must
// already have been persisted to it. Returns an error if this is not
// the case.
func InitEngine(ctx context.Context, eng storage.Engine, ident roachpb.StoreIdent) error {
	exIdent, err := ReadStoreIdent(ctx, eng)
	if err == nil {
		return errors.Errorf("engine %s is already initialized with ident %s", eng, exIdent.String())
	}
	if !errors.HasType(err, (*NotBootstrappedError)(nil)) {
		return err
	}

	if err := checkCanInitializeEngine(ctx, eng); err != nil {
		return errors.Wrap(err, "while trying to initialize engine")
	}

	batch := eng.NewBatch()
	if err := storage.MVCCPutProto(
		ctx,
		batch,
		nil,
		keys.StoreIdentKey(),
		hlc.Timestamp{},
		nil,
		&ident,
	); err != nil {
		batch.Close()
		return err
	}
	if err := batch.Commit(true /* sync */); err != nil {
		return errors.Wrap(err, "persisting engine initialization data")
	}

	return nil
}

// WriteInitialClusterData writes initialization data to an engine. It creates
// system ranges (filling in meta1 and meta2) and the default zone config.
//
// Args:
// eng: the engine to which data is to be written.
// initialValues: an optional list of k/v to be written as well after each
//   value's checksum is initialized.
// bootstrapVersion: the version at which the cluster is bootstrapped.
// numStores: the number of stores this node will have.
// splits: an optional list of split points. Range addressing will be created
//   for all the splits. The list needs to be sorted.
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

	const firstNodeID = 1 // This node has id 1.
	nodeIDVal.SetInt(firstNodeID)
	// The caller will initialize the stores with ids 1..numStores.
	storeIDVal.SetInt(int64(numStores))
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
	livenessRecord := livenesspb.Liveness{NodeID: 1, Epoch: 0}
	if err := livenessVal.SetProto(&livenessRecord); err != nil {
		return err
	}
	initialValues = append(initialValues,
		roachpb.KeyValue{Key: keys.NodeIDGenerator, Value: nodeIDVal},
		roachpb.KeyValue{Key: keys.StoreIDGenerator, Value: storeIDVal},
		roachpb.KeyValue{Key: keys.RangeIDGenerator, Value: rangeIDVal},
		roachpb.KeyValue{Key: keys.NodeLivenessKey(firstNodeID), Value: livenessVal})

	// firstRangeMS is going to accumulate the stats for the first range, as we
	// write the meta records for all the other ranges.
	firstRangeMS := &enginepb.MVCCStats{}

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
	// to the stats of the first range (i.e. because they all write meta2 records
	// in the first range), and so we want to create the first range last so that
	// the stats we compute for it are correct.
	startKey := roachpb.RKeyMax
	for i := len(splits) - 1; i >= -1; i-- {
		endKey := startKey
		rangeID := roachpb.RangeID(i + 2) // RangeIDs are 1-based.
		if i >= 0 {
			startKey = splits[i]
		} else {
			startKey = roachpb.RKeyMin
		}

		desc := &roachpb.RangeDescriptor{
			RangeID:       rangeID,
			StartKey:      startKey,
			EndKey:        endKey,
			NextReplicaID: 2,
		}
		replicas := []roachpb.ReplicaDescriptor{
			{
				NodeID:    1,
				StoreID:   1,
				ReplicaID: 1,
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
		batch := eng.NewBatch()
		defer batch.Close()

		now := hlc.Timestamp{
			WallTime: nowNanos,
			Logical:  0,
		}

		// NOTE: We don't do stats computations in any of the puts below. Instead,
		// we write everything and then compute the stats over the whole range.

		// Range descriptor.
		if err := storage.MVCCPutProto(
			ctx, batch, nil /* ms */, keys.RangeDescriptorKey(desc.StartKey),
			now, nil /* txn */, desc,
		); err != nil {
			return err
		}

		// Replica GC timestamp.
		if err := storage.MVCCPutProto(
			ctx, batch, nil /* ms */, keys.RangeLastReplicaGCTimestampKey(desc.RangeID),
			hlc.Timestamp{}, nil /* txn */, &now,
		); err != nil {
			return err
		}
		// Range addressing for meta2.
		meta2Key := keys.RangeMetaKey(endKey)
		if err := storage.MVCCPutProto(ctx, batch, firstRangeMS, meta2Key.AsRawKey(),
			now, nil /* txn */, desc,
		); err != nil {
			return err
		}

		// The first range gets some special treatment.
		if startKey.Equal(roachpb.RKeyMin) {
			// Range addressing for meta1.
			meta1Key := keys.RangeMetaKey(keys.RangeMetaKey(roachpb.RKeyMax))
			if err := storage.MVCCPutProto(
				ctx, batch, nil /* ms */, meta1Key.AsRawKey(), now, nil /* txn */, desc,
			); err != nil {
				return err
			}
		}

		// Now add all passed-in default entries.
		for _, kv := range rangeInitialValues {
			// Initialize the checksums.
			kv.Value.InitChecksum(kv.Key)
			if err := storage.MVCCPut(
				ctx, batch, nil /* ms */, kv.Key, now, kv.Value, nil, /* txn */
			); err != nil {
				return err
			}
		}

		if tt := knobs.TruncatedStateTypeOverride; tt != nil {
			if err := stateloader.WriteInitialRangeStateWithTruncatedState(
				ctx, batch, *desc, initialReplicaVersion, *tt,
			); err != nil {
				return err
			}
		} else {
			if err := stateloader.WriteInitialRangeState(ctx, batch, *desc, initialReplicaVersion); err != nil {
				return err
			}
		}
		computedStats, err := rditer.ComputeStatsForRange(desc, batch, now.WallTime)
		if err != nil {
			return err
		}

		sl := stateloader.Make(rangeID)
		if err := sl.SetMVCCStats(ctx, batch, &computedStats); err != nil {
			return err
		}

		if err := batch.Commit(true /* sync */); err != nil {
			return err
		}
	}

	return nil
}
