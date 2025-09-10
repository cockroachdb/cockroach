// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver_test

import (
	"context"
	"fmt"
	"math"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
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
// verifies that spanconfig updates bypass backpressure when the
// `system.span_configurations` table range is full.
//
// Test strategy:
//  1. Configure `system.span_configurations` table range to be a small size (8 KiB).
//  2. Write many large spanconfig records (2 KiB each) to fill up the range.
//  3. Verify spanconfig updates bypass backpressure (due to allowlist).
//  4. This test verifies that spanconfig updates bypass backpressure.
func TestSpanConfigUpdatesBlockedByRangeSizeBackpressureOnDefaultRanges(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	const (
		overloadMaxRangeBytes = 8 << 10   // 8 KiB, a saner value than default 512 MiB for testing
		overloadMinRangeBytes = 2 << 10   // 2 KiB
		numWrites             = 16        // enough to hit backpressure for 8 KiB range & 2 KiB spanconfig
		defaultMaxBytes       = 512 << 20 // default max bytes for a range
	)
	s := serverutils.StartServerOnly(t, base.TestServerArgs{})
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

	spanConfigTablePrefix := keys.SystemSQLCodec.TablePrefix(
		keys.SpanConfigurationsTableID)

	t.Logf("targeting span_configurations table at key: %s (table ID %d)\n",
		spanConfigTablePrefix, keys.SpanConfigurationsTableID)

	scratchKey, err := s.ScratchRange()
	require.NoError(t, err)

	testutils.SucceedsSoon(t, func() error {
		repl := store.LookupReplica(roachpb.RKey(scratchKey))
		if got := repl.GetMaxBytes(ctx); got != defaultMaxBytes {
			return errors.Errorf(
				"range max bytes values did not start at %d; got %d",
				defaultMaxBytes, got)
		}
		return nil
	})

	systemSpanConfigurationsTableSpan := roachpb.Span{
		Key:    spanConfigTablePrefix,
		EndKey: spanConfigTablePrefix.PrefixEnd(),
	}

	target := spanconfig.MakeTargetFromSpan(systemSpanConfigurationsTableSpan)

	systemSpanConfig := roachpb.SpanConfig{
		RangeMaxBytes: overloadMaxRangeBytes,
		RangeMinBytes: overloadMinRangeBytes,
	}

	configBytessdfsdf, err := protoutil.Marshal(&systemSpanConfig)
	require.NoError(t, err)
	t.Logf("marshalled systemSpanConfig size: %d bytes", len(configBytessdfsdf))

	record, err := spanconfig.MakeRecord(target, systemSpanConfig)
	require.NoError(t, err)

	kvaccessor := s.SpanConfigKVAccessor().(spanconfig.KVAccessor)

	err = kvaccessor.UpdateSpanConfigRecords(
		ctx, []spanconfig.Target{target},
		[]spanconfig.Record{record}, hlc.MinTimestamp, hlc.MaxTimestamp)
	require.NoError(t, err)

	waitForSpanConfig(t, s, spanConfigTablePrefix, overloadMaxRangeBytes)

	// Check if the range is using our custom config.
	repl := store.LookupReplica(keys.MustAddr(spanConfigTablePrefix))
	if repl != nil {
		conf, err := repl.LoadSpanConfig(ctx)
		require.NoError(t, err)
		t.Logf("current range config - RangeMaxBytes: %d bytes (%d MiB), "+
			"RangeMinBytes: %d bytes (%d MiB)",
			conf.RangeMaxBytes, conf.RangeMaxBytes>>20,
			conf.RangeMinBytes, conf.RangeMinBytes>>20)

	}

	t.Logf("targeting span_configurations table at key: %s (table ID %d)\n",
		spanConfigTablePrefix, keys.SpanConfigurationsTableID)

	// Create a single target for the scratch range (this will be stored in system.span_configurations)
	scratchTarget := spanconfig.MakeTargetFromSpan(roachpb.Span{
		Key:    scratchKey,
		EndKey: scratchKey.PrefixEnd(),
	})

	// This is a large spanconfig for a scratch range with relevant fields set
	// to maximum int64 and int32 values. This is done to have a spanconfig that
	// is large enough to trigger backpressure without having to write a million
	// records.
	// We want this config to be relatively large - this is done via setting
	// values to have max values and multiple fields as this config gets
	// marshalled into a protobuf and protobuf uses variant encoding, which
	// means larger values take more bytes to encode.
	spanConfig2KiB := roachpb.SpanConfig{ // 2078 bytes ~ 2 KiB.
		RangeMaxBytes: math.MaxInt64,
		RangeMinBytes: math.MaxInt64,
		GCPolicy: roachpb.GCPolicy{
			TTLSeconds: math.MaxInt32,
			ProtectionPolicies: []roachpb.ProtectionPolicy{
				{
					ProtectedTimestamp: hlc.MaxTimestamp,
				},
				{
					ProtectedTimestamp: hlc.MaxTimestamp,
				},
			},
		},
		NumReplicas: math.MaxInt32,
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
	testRecord, err := spanconfig.MakeRecord(scratchTarget, spanConfig2KiB)
	require.NoError(t, err)

	// Write span configurations using KVAccessor.
	// We expect this to fail due to backpressure.
	var i int
	for i = 0; i < numWrites; i++ {
		// Use KVAccessor to update span configurations.
		err = kvaccessor.UpdateSpanConfigRecords(ctx, nil,
			[]spanconfig.Record{testRecord}, hlc.MinTimestamp, hlc.MaxTimestamp)
		if err != nil {
			break
		}
	}

	// Assert that the operation does not fail due to allowlist for spanconfig updates.
	require.NoError(t, err,
		"expected spanconfig updates to succeed due to allowlist, but they failed")

	systemSpanConfigurationsTableSpanMVCCStats := roachpb.Span{
		Key:    keys.SystemSQLCodec.TablePrefix(keys.SpanConfigurationsTableID),
		EndKey: keys.SystemSQLCodec.TablePrefix(keys.SpanConfigurationsTableID + 1),
	}

	distSender := s.DistSenderI().(*kvcoord.DistSender)

	// Track aggregate MVCC stats across all discovered ranges
	var aggregateStats enginepb.MVCCStats
	var rangeCount int

	for key := systemSpanConfigurationsTableSpanMVCCStats.Key; key.Compare(systemSpanConfigurationsTableSpanMVCCStats.EndKey) < 0; {
		desc, err := distSender.RangeDescriptorCache().Lookup(ctx, keys.MustAddr(key))
		require.NoError(t, err)
		d := desc.Desc

		rangeRepl := store.LookupReplica(d.StartKey)
		if rangeRepl != nil {
			stats := rangeRepl.GetMVCCStats()
			aggregateStats.Add(stats)
			rangeCount++
		}

		// Move to next range.
		key = d.EndKey.AsRawKey()
		if key.Equal(roachpb.KeyMax) {
			break
		}
	}

	t.Logf("aggregate table size after all writes: %d bytes (%d MiB), "+
		"key count: %d, live count: %d",
		aggregateStats.Total(), aggregateStats.Total()>>20,
		aggregateStats.KeyCount, aggregateStats.LiveCount)

	smallSpanConfig := roachpb.SpanConfig{
		GCPolicy: roachpb.GCPolicy{
			TTLSeconds: 0,
		},
	}

	smallSpanconfigRecord, err := spanconfig.MakeRecord(scratchTarget, smallSpanConfig)
	require.NoError(t, err)

	t.Logf("testing one more write with a small span config...")

	smallSpanconfigRecordWriteErr := kvaccessor.UpdateSpanConfigRecords(ctx,
		[]spanconfig.Target{scratchTarget}, []spanconfig.Record{smallSpanconfigRecord},
		hlc.MinTimestamp, hlc.MaxTimestamp)

	require.NoError(t, smallSpanconfigRecordWriteErr,
		"expected smallSpanconfigRecord write to succeed")

}
