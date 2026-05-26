// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gcjob

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestIsProtectedTimestampsPreventTableIndexGC ensures the presence of
// protected timestamps prevents GC. It's a unit test for the isProtected
// function.
func TestProtectedTimestampsPreventGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	ts := func(nanos int) hlc.Timestamp {
		return hlc.Timestamp{
			WallTime: int64(nanos),
		}
	}

	makeSpanConfig := func(nanos int, excludeFromBackup, ignoreIfExcludedFromBackup bool) roachpb.SpanConfig {
		return roachpb.SpanConfig{
			ExcludeDataFromBackup: excludeFromBackup,
			GCPolicy: roachpb.GCPolicy{
				ProtectionPolicies: []roachpb.ProtectionPolicy{{
					IgnoreIfExcludedFromBackup: ignoreIfExcludedFromBackup,
					ProtectedTimestamp:         ts(nanos),
				}},
			},
		}
	}

	for _, tc := range []struct {
		name                          string
		setup                         func(t *testing.T, kvAccessor *manualKVAccessor)
		droppedAtTime                 int64
		expectedProtectedForDataGC    bool
		expectedProtectedForDescClean bool
	}{
		{
			name: "span-config-pts-applies",
			setup: func(t *testing.T, kvAccessor *manualKVAccessor) {
				kvAccessor.spanConfigs = []roachpb.SpanConfig{
					makeSpanConfig(10, false /* excludeFromBackup */, false /* ignoreIfExcludedFromBackup */),
				}
			},
			droppedAtTime:                 13, // The table/index was dropped after the PTS was laid
			expectedProtectedForDataGC:    true,
			expectedProtectedForDescClean: true,
		},
		{
			name: "span-config-pts-exists-but-does-not-apply",
			setup: func(t *testing.T, kvAccessor *manualKVAccessor) {
				kvAccessor.spanConfigs = []roachpb.SpanConfig{
					makeSpanConfig(10, false /* excludeFromBackup */, false /* ignoreIfExcludedFromBackup */),
				}
			},
			droppedAtTime:                 8, // The table/index was dropped before the PTS was laid
			expectedProtectedForDataGC:    false,
			expectedProtectedForDescClean: false,
		},
		{
			name: "system-span-config-pts-applies",
			setup: func(t *testing.T, kvAccessor *manualKVAccessor) {
				// PTS records on span configs exist, but they were laid after the drop
				// time, so they don't apply.
				kvAccessor.spanConfigs = []roachpb.SpanConfig{
					makeSpanConfig(10, false /* excludeFromBackup */, false /* ignoreIfExcludedFromBackup */),
					makeSpanConfig(12, false /* excludeFromBackup */, false /* ignoreIfExcludedFromBackup */),
				}
				// PTS record exists on system span configs and was laid before the
				// drop time.
				kvAccessor.systemSpanConfigs = []roachpb.SpanConfig{
					makeSpanConfig(7, false /* excludeFromBackup */, false /* ignoreIfExcludedFromBackup */),
				}
			},
			droppedAtTime:                 8,
			expectedProtectedForDataGC:    true,
			expectedProtectedForDescClean: true,
		},
		{
			// In the data-GC scope, the backup PTS is filtered out (the
			// existing exclude_data_from_backup contract). In the
			// descriptor-cleanup scope, the same PTS is honored: tearing
			// down the descriptor would remove the span config record
			// carrying ExcludeDataFromBackup, breaking the backup
			// processor's ability to recover from the resulting GC error.
			name: "system-span-config-pts-is-active-but-excluded-because-of-backup",
			setup: func(t *testing.T, kvAccessor *manualKVAccessor) {
				// PTS records on span configs exist, but they were laid after the drop
				// time, so they don't apply.
				kvAccessor.spanConfigs = []roachpb.SpanConfig{
					makeSpanConfig(10, false /* excludeFromBackup */, false /* ignoreIfExcludedFromBackup */),
					makeSpanConfig(12, false /* excludeFromBackup */, false /* ignoreIfExcludedFromBackup */),
				}
				// PTS record exists on system span configs and was laid before the
				// drop time. However, it's on a span to be excluded from backup and
				// the PTS record indicates it should be ignored if the span is excluded
				// from backup.
				kvAccessor.systemSpanConfigs = []roachpb.SpanConfig{
					makeSpanConfig(7, true /* excludeFromBackup */, true /* ignoreIfExcludedFromBackup */),
				}
			},
			droppedAtTime:                 8,
			expectedProtectedForDataGC:    false,
			expectedProtectedForDescClean: true,
		},
		{
			name: "system-span-config-pts-is-active-excluded-from-backup-but-not-ignored",
			setup: func(t *testing.T, kvAccessor *manualKVAccessor) {
				// PTS records on span configs exist, but they were laid after the drop
				// time, so they don't apply.
				kvAccessor.spanConfigs = []roachpb.SpanConfig{
					makeSpanConfig(10, false /* excludeFromBackup */, false /* ignoreIfExcludedFromBackup */),
					makeSpanConfig(12, false /* excludeFromBackup */, false /* ignoreIfExcludedFromBackup */),
				}
				// PTS record exists on system span configs and was laid before the
				// drop time. The span is marked as excluded from backup, however, the
				// PTS record should not be ignored despite it. Thus, we expect the PTS
				// record to apply.
				kvAccessor.systemSpanConfigs = []roachpb.SpanConfig{
					makeSpanConfig(7, true /* excludeFromBackup */, false /* ignoreIfExcludedFromBackup */),
				}
			},
			droppedAtTime:                 8,
			expectedProtectedForDataGC:    true,
			expectedProtectedForDescClean: true,
		},
		{
			name: "pts-records-exist-but-none-apply",
			setup: func(t *testing.T, kvAccessor *manualKVAccessor) {
				// PTS records on span configs exist, but they were laid after the drop
				// time, so they don't apply.
				kvAccessor.spanConfigs = []roachpb.SpanConfig{
					makeSpanConfig(10, false /* excludeFromBackup */, false /* ignoreIfExcludedFromBackup */),
					makeSpanConfig(12, false /* excludeFromBackup */, false /* ignoreIfExcludedFromBackup */),
				}
				// PTS record on system span configs exist, but they were laid after the
				// drop time, so they don't apply.
				kvAccessor.systemSpanConfigs = []roachpb.SpanConfig{
					makeSpanConfig(7, false /* excludeFromBackup */, false /* ignoreIfExcludedFromBackup */),
					makeSpanConfig(8, false /* excludeFromBackup */, false /* ignoreIfExcludedFromBackup */),
				}
			},
			droppedAtTime:                 2,
			expectedProtectedForDataGC:    false,
			expectedProtectedForDescClean: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			scratchRange := roachpb.Span{
				Key:    keys.ScratchRangeMin,
				EndKey: keys.SystemSpanConfigKeyMax,
			}
			for _, sc := range []struct {
				name     string
				scope    protectionScope
				expected bool
			}{
				{name: "data-GC", scope: protectionScopeForDataGC, expected: tc.expectedProtectedForDataGC},
				{name: "descriptor-cleanup", scope: protectionScopeForDescriptorCleanup, expected: tc.expectedProtectedForDescClean},
			} {
				t.Run(sc.name, func(t *testing.T) {
					kvAccessor := &manualKVAccessor{}
					tc.setup(t, kvAccessor)
					isProtected, err := isProtected(
						ctx, jobspb.InvalidJobID, tc.droppedAtTime,
						keys.SystemSQLCodec, nil /* knobs */, kvAccessor, scratchRange, sc.scope,
					)
					require.NoError(t, err)
					require.Equal(t, sc.expected, isProtected)
				})
			}
		})
	}
}

type manualKVAccessor struct {
	systemSpanConfigs []roachpb.SpanConfig
	spanConfigs       []roachpb.SpanConfig
}

var _ spanconfig.KVAccessor = &manualKVAccessor{}

func (m *manualKVAccessor) GetSpanConfigRecords(
	context.Context, []spanconfig.Target,
) (records []spanconfig.Record, _ error) {
	for _, config := range m.spanConfigs {
		rec, err := spanconfig.MakeRecord(
			spanconfig.MakeTargetFromSpan(roachpb.Span{
				Key:    keys.ScratchRangeMin,
				EndKey: keys.SystemSpanConfigKeyMax,
			}),
			config,
		)
		if err != nil {
			return nil, err
		}
		records = append(records, rec)
	}
	return records, nil
}

func (m *manualKVAccessor) GetAllSystemSpanConfigsThatApply(
	context.Context, roachpb.TenantID,
) ([]roachpb.SpanConfig, error) {
	return m.systemSpanConfigs, nil
}

func (m *manualKVAccessor) UpdateSpanConfigRecords(
	context.Context, []spanconfig.Target, []spanconfig.Record, hlc.Timestamp, hlc.Timestamp,
) error {
	panic("unimplemented")
}

func (m *manualKVAccessor) WithTxn(context.Context, *kv.Txn) spanconfig.KVAccessor {
	panic("unimplemented")
}

// WithISQLTxn is part of the KVAccessor interface.
func (k *manualKVAccessor) WithISQLTxn(context.Context, isql.Txn) spanconfig.KVAccessor {
	panic("unimplemented")
}
