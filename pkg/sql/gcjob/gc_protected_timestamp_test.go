// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package gcjob

import (
	"context"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/stretchr/testify/require"
)

// TestIsProtectedTimestampsPreventTableIndexGC ensures the presence of
// protected timestamps prevents GC. It's a unit test for the isProtected
// function.
func TestProtectedTimestampsPreventGC(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()
	srv := serverutils.StartServerOnly(t, base.TestServerArgs{
		Knobs: base.TestingKnobs{
			JobsTestingKnobs: jobs.NewTestingKnobsWithShortIntervals(),
		},
	})
	defer srv.Stopper().Stop(ctx)
	execCfg := srv.ExecutorConfig().(sql.ExecutorConfig)

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
		name                string
		setup               func(t *testing.T, kvAccessor *manualKVAccessor, cache *manualCache)
		droppedAtTime       int64
		expectedIsProtected bool

		failsWithSecondaryTenant bool
	}{
		{
			name: "span-config-pts-applies",
			setup: func(t *testing.T, kvAccessor *manualKVAccessor, cache *manualCache) {
				kvAccessor.spanConfigs = []roachpb.SpanConfig{
					makeSpanConfig(10, false /* excludeFromBackup */, false /* ignoreIfExcludedFromBackup */),
				}
			},
			droppedAtTime:       13, // The table/index was dropped after the PTS was laid
			expectedIsProtected: true,
		},
		{
			name: "span-config-pts-exists-but-does-not-apply",
			setup: func(t *testing.T, kvAccessor *manualKVAccessor, cache *manualCache) {
				kvAccessor.spanConfigs = []roachpb.SpanConfig{
					makeSpanConfig(10, false /* excludeFromBackup */, false /* ignoreIfExcludedFromBackup */),
				}
			},
			droppedAtTime:       8, // The table/index was dropped before the PTS was laid
			expectedIsProtected: false,
		},
		{
			name: "system-span-config-pts-applies",
			setup: func(t *testing.T, kvAccessor *manualKVAccessor, cache *manualCache) {
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
			droppedAtTime:       8,
			expectedIsProtected: true,
		},
		{
			name: "system-span-config-pts-is-active-but-excluded-because-of-backup",
			setup: func(t *testing.T, kvAccessor *manualKVAccessor, cache *manualCache) {
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
			droppedAtTime:       8,
			expectedIsProtected: false,
		},
		{
			name: "system-span-config-pts-is-active-excluded-from-backup-but-not-ignored",
			setup: func(t *testing.T, kvAccessor *manualKVAccessor, cache *manualCache) {
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
			droppedAtTime:       8,
			expectedIsProtected: true,
		},
		{
			name: "deprecated-pts-applies",
			setup: func(t *testing.T, kvAccessor *manualKVAccessor, cache *manualCache) {
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
				cache.protectedTimestamps = []hlc.Timestamp{
					ts(5),
					ts(3), // applies as it's before the drop time
				}
			},
			droppedAtTime:       4,
			expectedIsProtected: true,

			// TODO(sql-foundations): investigate this.
			failsWithSecondaryTenant: true,
		},
		{
			name: "pts-records-exist-but-none-apply",
			setup: func(t *testing.T, kvAccessor *manualKVAccessor, cache *manualCache) {
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
				// PTS records in the old PTS subsystem exist, but they were laid after
				// the drop time, so they don't apply.
				cache.protectedTimestamps = []hlc.Timestamp{
					ts(5),
					ts(3),
				}
			},
			droppedAtTime:       2,
			expectedIsProtected: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.failsWithSecondaryTenant && srv.TenantController().StartedDefaultTestTenant() {
				skip.WithIssue(t, 110014)
			}

			kvAccessor := &manualKVAccessor{}
			cache := &manualCache{}
			tc.setup(t, kvAccessor, cache)
			scratchRange := roachpb.Span{
				Key:    keys.ScratchRangeMin,
				EndKey: keys.SystemSpanConfigKeyMax,
			}
			isProtected, err := isProtected(
				ctx, jobspb.InvalidJobID, tc.droppedAtTime, &execCfg, kvAccessor, cache, scratchRange,
			)
			require.NoError(t, err)
			require.Equal(t, tc.expectedIsProtected, isProtected)
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

type manualCache struct {
	protectedTimestamps []hlc.Timestamp
}

var _ protectedts.Cache = &manualCache{}

func (c *manualCache) Iterate(
	_ context.Context, _, _ roachpb.Key, it protectedts.Iterator,
) hlc.Timestamp {
	for _, protectedTimestamp := range c.protectedTimestamps {
		it(&ptpb.Record{
			Timestamp: protectedTimestamp,
		})
	}
	return hlc.Timestamp{} // asOf
}

func (c *manualCache) GetProtectionTimestamps(
	context.Context, roachpb.Span,
) (protectionTimestamps []hlc.Timestamp, asOf hlc.Timestamp, err error) {
	panic("unimplemented")
}

func (c *manualCache) Refresh(ctx context.Context, asOf hlc.Timestamp) error {
	panic("unimplemented")
}

func (c *manualCache) QueryRecord(context.Context, uuid.UUID) (exists bool, asOf hlc.Timestamp) {
	panic("unimplemented")
}
