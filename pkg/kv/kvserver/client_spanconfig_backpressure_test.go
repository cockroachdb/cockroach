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

// TestSpanConfigUpdatesBlockedByRangeSizeBackpressureOnDefaultRanges
// verifies that spanconfig updates are blocked by backpressure when the
// `system.span_configurations` table range becomes full, recreating the issue.
//
// Test strategy:
//  1. Configure `system.span_configurations` table range the smallest possible
//     max size (64 MiB).
//  2. Write many large spanconfig records (2 KiB each) to fill up the range.
//  3. Verify spanconfig updates fail due to backpressure when the range is full,
//  4. This test recreates the scenario where spanconfig updates are blocked by
//     backpressure.
func TestSpanConfigUpdatesBlockedByRangeSizeBackpressureOnDefaultRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	const (
		overloadMaxRangeBytes = 64 << 20 // set to 64 MiB, a saner value than the default of 512 MiB
		overloadMinRangeBytes = 16 << 10
		numWrites             = 64 << 10 // 65,536 writes, this was calculated by (64 MiB / 2 KiB) * 2
		// (2 KiB is the size of the spanconfig record `spanConfig2KiB`).
		// See func exceedsMultipleOfSplitSize in /pkg/kv/kvserver/replica_metrics.go for the logic.
		defaultMaxBytes = 512 << 20 // Default max bytes for a range.
	)
	s := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			Store: &kvserver.StoreTestingKnobs{
				DisableMergeQueue: true,
				// keep split queue enabled to see natural behaviour
			}},
	})
	defer s.Stopper().Stop(ctx)

	store, err := s.GetStores().(*kvserver.Stores).GetStore(s.GetFirstStoreID())
	require.NoError(t, err)

	waitForSpanConfig := func(t *testing.T, tc serverutils.TestServerInterface,
		tablePrefix roachpb.Key, expRangeMaxBytes int64) {
		testutils.SucceedsSoon(t, func() error {
			_, r := getFirstStoreReplica(t, tc, tablePrefix)
			conf, err := r.LoadSpanConfig(ctx)
			if err != nil {
				return err
			}
			if conf.RangeMaxBytes != expRangeMaxBytes {
				return fmt.Errorf("expected RangeMaxBytes %d, got %d",
					expRangeMaxBytes, conf.RangeMaxBytes)
			}
			return nil
		})
	}

	spanConfigTablePrefix := keys.SystemSQLCodec.TablePrefix(keys.SpanConfigurationsTableID)

	t.Logf("Targeting span_configurations table at key: %s (table ID %d)\n",
		spanConfigTablePrefix, keys.SpanConfigurationsTableID)

	testKey, err := s.ScratchRange()
	require.NoError(t, err)

	testutils.SucceedsSoon(t, func() error {
		repl := store.LookupReplica(roachpb.RKey(testKey))
		if got := repl.GetMaxBytes(ctx); got != defaultMaxBytes {
			return errors.Errorf(
				"range max bytes values did not start at %d; got %d",
				defaultMaxBytes, got)
		}
		return nil
	})

	tableSpan := roachpb.Span{
		Key:    spanConfigTablePrefix,
		EndKey: spanConfigTablePrefix.PrefixEnd(),
	}

	target := spanconfig.MakeTargetFromSpan(tableSpan)

	// System spanconfig to set the range max bytes to 64 MiB.
	systemSpanConfig := roachpb.SpanConfig{
		RangeMaxBytes: overloadMaxRangeBytes, // 64 MiB.
		RangeMinBytes: overloadMinRangeBytes, // 16 MiB.
	}
	record, err := spanconfig.MakeRecord(target, systemSpanConfig)
	require.NoError(t, err)

	kvaccessor := s.SpanConfigKVAccessor().(spanconfig.KVAccessor)

	err = kvaccessor.UpdateSpanConfigRecords(
		ctx, []spanconfig.Target{target},
		[]spanconfig.Record{record}, hlc.MinTimestamp, hlc.MaxTimestamp)
	require.NoError(t, err)

	waitForSpanConfig(t, s, spanConfigTablePrefix, overloadMaxRangeBytes)

	t.Logf("Zone configuration successfully applied!\n")

	// Check if the range is using our custom config.
	repl := store.LookupReplica(keys.MustAddr(spanConfigTablePrefix))
	if repl != nil {
		conf, err := repl.LoadSpanConfig(ctx)
		if err != nil {
			t.Logf("Error loading span config: %v\n", err)
		} else {
			t.Logf(`Current range config - RangeMaxBytes: %d bytes 
			(%d MiB), RangeMinBytes: %d bytes (%d MiB)\n`,
				conf.RangeMaxBytes, conf.RangeMaxBytes>>20,
				conf.RangeMinBytes, conf.RangeMinBytes>>20)
		}

		stats := repl.GetMVCCStats()
		log.Dev.Infof(ctx, "Current range size: %d bytes (%d MiB)\n",
			stats.Total(), stats.Total()>>20)
	}

	t.Logf("Targeting span_configurations table at key: %s (table ID %d)\n",
		spanConfigTablePrefix, keys.SpanConfigurationsTableID)
	t.Logf(`Direct KV writes to span_configurations table 
		range %d times...\n`, numWrites)

	// Create a single target for the scratch range (this will be stored in system.span_configurations)
	testTargetKey := testKey // Use the scratch range key we got earlier.
	testTarget := spanconfig.MakeTargetFromSpan(roachpb.Span{
		Key:    testTargetKey,
		EndKey: testTargetKey.PrefixEnd(),
	})

	// This is a large spanconfig with all fields set to maximum int64 and int32 values.
	// This is done to have a spanconfig that is large enough to trigger backpressure
	// without having to write a million records.
	// Having this be 2 KiB gives us (64 << 20 / 2 << 10) * 2 = 65,536 writes.
	// See func exceedsMultipleOfSplitSize in /pkg/kv/kvserver/replica_metrics.go for the logic.
	spanConfig2KiB := roachpb.SpanConfig{ // 2078 bytes ~ 2 KiB.
		RangeMaxBytes: math.MaxInt64, // maximum int64 value
		RangeMinBytes: math.MaxInt64, // maximum int64 value
		GCPolicy: roachpb.GCPolicy{
			TTLSeconds: math.MaxInt32, // maximum int32 value
			ProtectionPolicies: []roachpb.ProtectionPolicy{
				{
					ProtectedTimestamp: hlc.MaxTimestamp,
				},
				{
					ProtectedTimestamp: hlc.MaxTimestamp,
				},
			},
		},
		NumReplicas: math.MaxInt32, // maximum int32 value
		GlobalReads: true,
		NumVoters:   math.MaxInt32,
		VoterConstraints: []roachpb.ConstraintsConjunction{
			{
				Constraints: []roachpb.Constraint{
					{Key: "max_key", Value: strings.Repeat("x", 1024)}, // very long constraint value
				},
			},
		},
		LeasePreferences: []roachpb.LeasePreference{
			{
				Constraints: []roachpb.Constraint{
					{Key: "max_key", Value: strings.Repeat("y", 1024)}, // very long constraint value
				},
			},
		},
	}

	configBytes, err := protoutil.Marshal(&spanConfig2KiB)
	require.NoError(t, err)

	require.GreaterOrEqual(t, len(configBytes), 2048,
		"spanConfig2KiB should be at least 2 KiB in size")

	// Create a record with the span configuration.
	testRecord, err := spanconfig.MakeRecord(testTarget, spanConfig2KiB)
	require.NoError(t, err)

	// Write span configurations using KVAccessor.
	// We expect this to fail due to backpressure.
	var i int
	for i = 0; i < numWrites; i++ {
		// Use KVAccessor to update span configurations.
		err = kvaccessor.UpdateSpanConfigRecords(ctx, nil,
			[]spanconfig.Record{testRecord}, hlc.MinTimestamp, hlc.MaxTimestamp)
		t.Logf("KVAccessor write %d/%d: target=%q\n", i+1, numWrites, testTargetKey)
		if err != nil {
			break
		}
	}

	// Assert that the operation failed due to backpressure.
	require.NoError(t, err, `Expected span config writes to succeed due to 
		allowlist; they should bypass backpressure`)

	t.Logf("Completed %d direct KV writes\n", i)

	repl = store.LookupReplica(keys.MustAddr(spanConfigTablePrefix))
	if repl != nil {
		stats := repl.GetMVCCStats()
		t.Logf(`Range size after all writes: %d bytes (KeyCount: %d, 
		LiveBytes: %d)\n`, stats.Total(), stats.KeyCount, stats.LiveBytes)
	}

	smallSpanConfig := roachpb.SpanConfig{
		GCPolicy: roachpb.GCPolicy{
			TTLSeconds: 0,
		},
	}

	smallSpancofnRecord, err := spanconfig.MakeRecord(testTarget, smallSpanConfig)
	require.NoError(t, err)

	t.Logf("Testing one more write with a small span config...\n")

	smallSpancofnRecordWriteErr := kvaccessor.UpdateSpanConfigRecords(ctx,
		[]spanconfig.Target{testTarget}, []spanconfig.Record{smallSpancofnRecord},
		hlc.MinTimestamp, hlc.MaxTimestamp)

	require.NoError(t, smallSpancofnRecordWriteErr, `Expected smallSpancofnRecord 
		write to succeed`)
	t.Logf(`SUCCESS: smallSpancofnRecord write succeeded; 
		spanconfigs should bypass backpressure\n`)

}
