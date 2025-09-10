// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"fmt"
	math "math"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestSpanConfigUpdatesBlockedByRangeSizeBackpressureOnDefaultRangesWithKVAccessor(
	t *testing.T,
) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	const (
		overloadMaxRangeBytes = 64 << 20 // Set to 64 MiB, a saner value than the default of 512 MiB.
		overloadMinRangeBytes = 16 << 10
		numWrites             = 64 << 10 // 65,536 writes, this was calculated by (64 MiB / 2 KiB) * 2
		// (2 KiB is the size of the spanconfig record `spanConfig2Kib`).
		// See func exceedsMultipleOfSplitSize in /pkg/kv/kvserver/replica_metrics.go for the logic.
		defaultMaxBytes = 512 << 20 // Default max bytes for a range.
	)

	tc, _, _ := serverutils.StartServer(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableMergeQueue: true,
				// Keep split queue enabled to see natural behavior.
			}},
	})
	defer tc.Stopper().Stop(ctx)

	store, err := tc.GetStores().(*kvserver.Stores).GetStore(tc.GetFirstStoreID())
	require.NoError(t, err)

	waitForSpanConfig := func(t *testing.T, tc serverutils.TestServerInterface, tablePrefix roachpb.Key, exp int64) {
		testutils.SucceedsSoon(t, func() error {
			_, r := getFirstStoreReplica(t, tc, tablePrefix)
			conf, err := r.LoadSpanConfig(ctx)
			if err != nil {
				return err
			}
			if log.V(1) {
				log.Dev.Infof(ctx, "RangeMaxBytes for tablePrefix %s: %d\n", tablePrefix, conf.RangeMaxBytes)
			}
			if conf.RangeMaxBytes != exp {
				return fmt.Errorf("expected %d, got %d", exp, conf.RangeMaxBytes)
			}
			return nil
		})
	}

	// System spanconfig to set the range max bytes to 64 MiB.
	systemSpanConfig := roachpb.SpanConfig{
		RangeMaxBytes: 64 << 20, // 64 MiB.
		RangeMinBytes: 16 << 20, // 16 MiB.
	}

	// This is a fat spanconfig with all fields set to maximum int64 and int32 values.
	// This is done to have a spanconfig that is large enough to trigger backpressure
	// without having to write a million records.
	// Having this be 2 KiB gives us (64 << 20 / 2 << 10) * 2 = 65,536 writes.
	// See func exceedsMultipleOfSplitSize in /pkg/kv/kvserver/replica_metrics.go for the logic.
	spanConfig2KiB := roachpb.SpanConfig{ // 2078 bytes ~ 2 KiB.
		RangeMaxBytes: math.MaxInt64, // Maximum int64 value.
		RangeMinBytes: math.MaxInt64, // Maximum int64 value.
		GCPolicy: roachpb.GCPolicy{
			TTLSeconds: math.MaxInt32, // Maximum int32 value.
			ProtectionPolicies: []roachpb.ProtectionPolicy{
				{
					ProtectedTimestamp: hlc.MaxTimestamp,
				},
				{
					ProtectedTimestamp: hlc.MaxTimestamp,
				},
			},
		},
		NumReplicas: math.MaxInt32, // Maximum int32 value.
		GlobalReads: true,
		NumVoters:   math.MaxInt32,
		VoterConstraints: []roachpb.ConstraintsConjunction{
			{
				Constraints: []roachpb.Constraint{
					{Key: "max_key", Value: strings.Repeat("x", 1000)}, // Very long constraint value.
				},
			},
		},
		LeasePreferences: []roachpb.LeasePreference{
			{
				Constraints: []roachpb.Constraint{
					{Key: "max_key", Value: strings.Repeat("y", 1000)}, // Very long constraint value.
				},
			},
		},
	}

	configBytes, err := protoutil.Marshal(&spanConfig2KiB)
	require.NoError(t, err)

	log.Dev.Infof(ctx, "Size of configBytes: %d bytes (%d KiB)\n", len(configBytes), len(configBytes)>>10)

	spanConfigTablePrefix := keys.SystemSQLCodec.TablePrefix(keys.SpanConfigurationsTableID)

	log.Dev.Infof(ctx, "Targeting span_configurations table at key: %s (table ID %d)\n",
		spanConfigTablePrefix, keys.SpanConfigurationsTableID)

	log.Dev.Infof(ctx, "Configuring span_configurations table with custom zone settings...\n")

	testKey, err := tc.ScratchRange()
	require.NoError(t, err)

	testutils.SucceedsSoon(t, func() error {
		repl := store.LookupReplica(roachpb.RKey(testKey))
		if got := repl.GetMaxBytes(ctx); got != defaultMaxBytes {
			return errors.Errorf("range max bytes values did not start at %d; got %d", defaultMaxBytes, got)
		}
		return nil
	})

	tableSpan := roachpb.Span{Key: spanConfigTablePrefix, EndKey: spanConfigTablePrefix.PrefixEnd()}

	target := spanconfig.MakeTargetFromSpan(tableSpan)
	record, err := spanconfig.MakeRecord(target, systemSpanConfig)
	require.NoError(t, err)

	kvaccessor := tc.SpanConfigKVAccessor().(spanconfig.KVAccessor)

	err = kvaccessor.UpdateSpanConfigRecords(ctx, []spanconfig.Target{target}, []spanconfig.Record{record}, hlc.MinTimestamp, hlc.MaxTimestamp)
	require.NoError(t, err)

	waitForSpanConfig(t, tc, spanConfigTablePrefix, overloadMaxRangeBytes)

	// Wait for the zone configuration to be applied.
	log.Dev.Infof(ctx, "Waiting for zone configuration to be applied...\n")
	testutils.SucceedsSoon(t, func() error {
		repl := store.LookupReplica(keys.MustAddr(spanConfigTablePrefix))
		if repl == nil {
			return fmt.Errorf("replica not found")
		}
		conf, err := repl.LoadSpanConfig(ctx)
		if err != nil {
			return err
		}
		if conf.RangeMaxBytes != overloadMaxRangeBytes {
			return fmt.Errorf("expected RangeMaxBytes %d, got %d", overloadMaxRangeBytes, conf.RangeMaxBytes)
		}
		return nil
	})

	log.Dev.Infof(ctx, "Zone configuration successfully applied!\n")

	// Check if the range is using our custom config.
	repl := store.LookupReplica(keys.MustAddr(spanConfigTablePrefix))
	if repl != nil {
		conf, err := repl.LoadSpanConfig(ctx)
		if err != nil {
			log.Dev.Infof(ctx, "Error loading span config: %v\n", err)
		} else {
			log.Dev.Infof(ctx, "Current range config - RangeMaxBytes: %d bytes (%d MiB), RangeMinBytes: %d bytes (%d MiB)\n",
				conf.RangeMaxBytes, conf.RangeMaxBytes>>20,
				conf.RangeMinBytes, conf.RangeMinBytes>>20)
		}

		stats := repl.GetMVCCStats()
		log.Dev.Infof(ctx, "Current range size: %d bytes (%d MiB)\n", stats.Total(), stats.Total()>>20)
	}

	log.Dev.Infof(ctx, "Targeting span_configurations table at key: %s (table ID %d)\n",
		spanConfigTablePrefix, keys.SpanConfigurationsTableID)
	log.Dev.Infof(ctx, "Direct KV writes to span_configurations table range %d times...\n", numWrites)

	// Create a single target for the scratch range (this will be stored in system.span_configurations)
	testTargetKey := testKey // Use the scratch range key we got earlier.
	testTarget := spanconfig.MakeTargetFromSpan(roachpb.Span{
		Key:    testTargetKey,
		EndKey: testTargetKey.PrefixEnd(),
	})

	// Create a record with the span configuration.
	testRecord, err := spanconfig.MakeRecord(testTarget, spanConfig2KiB)
	require.NoError(t, err)

	// Write span configurations using KVAccessor.
	// We expect this to fail due to backpressure.
	var i int
	for i = 0; i < numWrites; i++ {
		// Use KVAccessor to update span configurations.
		err = kvaccessor.UpdateSpanConfigRecords(ctx, nil, []spanconfig.Record{testRecord}, hlc.MinTimestamp, hlc.MaxTimestamp)
		if log.V(1) {
			log.Dev.Infof(ctx, "KVAccessor write %d/%d: target=%q\n", i+1, numWrites, testTargetKey)
		}
		if err != nil {
			log.Dev.Infof(ctx, "ERROR! BREAKING OUT OF LOOP, numWrites successful: %d, error: %+v\n", i, err)
			break
		}
	}

	// Assert that the operation failed due to backpressure.
	require.Error(t, err, "Expected span config writes to fail due to backpressure, but they succeeded")
	log.Dev.Infof(ctx, "Verified that span config writes fail due to backpressure: %v\n", err)

	log.Dev.Infof(ctx, "Completed %d direct KV writes\n", i)

	repl = store.LookupReplica(keys.MustAddr(spanConfigTablePrefix))
	if repl != nil {
		stats := repl.GetMVCCStats()
		log.Dev.Infof(ctx, "Range size after all writes: %d bytes (KeyCount: %d, LiveBytes: %d)\n", stats.Total(), stats.KeyCount, stats.LiveBytes)
	}

	// Try various writes to test backpressure bypass logic.
	log.Dev.Infof(ctx, "Testing various KV writes for backpressure bypass...\n")

	// Test Case 1: Keep TTLSeconds as math.MaxInt32, delete one PTS (should succeed - PTS deletion)
	spanConfigDeleteOnePTS := roachpb.SpanConfig{
		GCPolicy: roachpb.GCPolicy{
			TTLSeconds: math.MaxInt32, // Keep same TTL.
			ProtectionPolicies: []roachpb.ProtectionPolicy{
				{
					ProtectedTimestamp: hlc.MaxTimestamp,
				},
				// Removed one PTS - this should bypass backpressure.
			},
		},
	}

	deleteOnePTSRecord, err := spanconfig.MakeRecord(testTarget, spanConfigDeleteOnePTS)
	require.NoError(t, err)

	deleteOnePTSErr := kvaccessor.UpdateSpanConfigRecords(ctx, []spanconfig.Target{testTarget}, []spanconfig.Record{deleteOnePTSRecord}, hlc.MinTimestamp, hlc.MaxTimestamp)
	require.NoError(t, deleteOnePTSErr, "Expected deleteOnePTS write to succeed (PTS deletion should bypass backpressure)")
	log.Dev.Infof(ctx, "SUCCESS: deleteOnePTS write succeeded (PTS deletion bypassed backpressure)\n")

	// Test Case 2: Keep TTLSeconds as math.MaxInt32, remove all PTS (should succeed - PTS deletion)
	spanConfigDeleteAllPTS := roachpb.SpanConfig{
		GCPolicy: roachpb.GCPolicy{
			TTLSeconds:         math.MaxInt32,                // Keep same TTL.
			ProtectionPolicies: []roachpb.ProtectionPolicy{}, // Remove all PTS.
		},
	}

	deleteAllPTSRecord, err := spanconfig.MakeRecord(testTarget, spanConfigDeleteAllPTS)
	require.NoError(t, err)

	deleteAllPTSErr := kvaccessor.UpdateSpanConfigRecords(ctx, []spanconfig.Target{testTarget}, []spanconfig.Record{deleteAllPTSRecord}, hlc.MinTimestamp, hlc.MaxTimestamp)
	require.NoError(t, deleteAllPTSErr, "Expected deleteAllPTS write to succeed (PTS deletion should bypass backpressure)")
	log.Dev.Infof(ctx, "SUCCESS: deleteAllPTS write succeeded (PTS deletion bypassed backpressure)\n")

	// Test Case 3: Change GCTTL and no PTS (should succeed - GCTTL change).
	spanConfigChangedGCTTLNoPTS := roachpb.SpanConfig{
		GCPolicy: roachpb.GCPolicy{
			TTLSeconds:         0,                            // Changed GCTTL.
			ProtectionPolicies: []roachpb.ProtectionPolicy{}, // No PTS..
		},
	}

	changedGCTTLNoPTSRecord, err := spanconfig.MakeRecord(testTarget, spanConfigChangedGCTTLNoPTS)
	require.NoError(t, err)

	changedGCTTLNoPTSErr := kvaccessor.UpdateSpanConfigRecords(ctx, []spanconfig.Target{testTarget}, []spanconfig.Record{changedGCTTLNoPTSRecord}, hlc.MinTimestamp, hlc.MaxTimestamp)
	require.NoError(t, changedGCTTLNoPTSErr, "Expected changedGCTTLNoPTS write to succeed (GCTTL change should bypass backpressure)")
	log.Dev.Infof(ctx, "SUCCESS: changedGCTTLNoPTS write succeeded (GCTTL change bypassed backpressure)\n")

	// Test Case 4: Add extra PTS (should fail - PTS addition should NOT bypass backpressure).
	spanConfigAddExtraPTS := roachpb.SpanConfig{
		GCPolicy: roachpb.GCPolicy{
			TTLSeconds: 0, // Keep same TTL from previous spanconfig.
			ProtectionPolicies: []roachpb.ProtectionPolicy{
				{
					ProtectedTimestamp: hlc.MaxTimestamp,
				},
				{
					ProtectedTimestamp: hlc.MaxTimestamp,
				},
				{
					ProtectedTimestamp: hlc.MaxTimestamp,
				},
				// Added extra PTS - this should NOT bypass backpressure.
			},
		},
	}

	addExtraPTSRecord, err := spanconfig.MakeRecord(testTarget, spanConfigAddExtraPTS)
	require.NoError(t, err)

	addExtraPTSErr := kvaccessor.UpdateSpanConfigRecords(ctx, []spanconfig.Target{testTarget}, []spanconfig.Record{addExtraPTSRecord}, hlc.MinTimestamp, hlc.MaxTimestamp)
	require.Error(t, addExtraPTSErr, "Expected addExtraPTS write to fail (PTS addition should NOT bypass backpressure)")
	log.Dev.Infof(ctx, "SUCCESS: addExtraPTS write failed as expected (PTS addition did not bypass backpressure): %v\n", addExtraPTSErr)

	log.Dev.Infof(ctx, "All backpressure bypass tests completed successfully!\n")

}
