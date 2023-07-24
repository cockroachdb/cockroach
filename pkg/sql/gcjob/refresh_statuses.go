// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package gcjob

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/spanconfig"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

var maxDeadline = timeutil.Unix(0, math.MaxInt64)

// Helpers.

// isProtected returns true if the supplied span is considered protected, and
// thus exempt from GC-ing, given the wall time at which it was dropped.
//
// This function is intended for table/index spans -- for spans that cover a
// secondary tenant's keyspace, checkout `isTenantProtected` instead.
func isProtected(
	ctx context.Context,
	jobID jobspb.JobID,
	droppedAtTime int64,
	execCfg *sql.ExecutorConfig,
	kvAccessor spanconfig.KVAccessor,
	ptsCache protectedts.Cache,
	sp roachpb.Span,
) (bool, error) {
	// Wrap this in a closure sp we can pass the protection status to the testing
	// knob.
	isProtected, err := func() (bool, error) {
		// We check the old protected timestamp subsystem for protected timestamps
		// if this is the GC job of the system tenant.
		if execCfg.Codec.ForSystemTenant() &&
			deprecatedIsProtected(ctx, ptsCache, droppedAtTime, sp) {
			return true, nil
		}

		spanConfigRecords, err := kvAccessor.GetSpanConfigRecords(ctx, spanconfig.Targets{
			spanconfig.MakeTargetFromSpan(sp),
		})
		if err != nil {
			return false, err
		}

		_, tenID, err := keys.DecodeTenantPrefix(execCfg.Codec.TenantPrefix())
		if err != nil {
			return false, err
		}
		systemSpanConfigs, err := kvAccessor.GetAllSystemSpanConfigsThatApply(ctx, tenID)
		if err != nil {
			return false, err
		}

		// Collect all protected timestamps that apply to the given span; both by
		// virtue of span configs and system span configs.
		var protectedTimestamps []hlc.Timestamp
		collectProtectedTimestamps := func(configs ...roachpb.SpanConfig) {
			for _, config := range configs {
				for _, protectionPolicy := range config.GCPolicy.ProtectionPolicies {
					// We don't consider protected timestamps written by backups if the span
					// is indicated as "excluded from backup". Checkout the field
					// descriptions for more details about this coupling.
					if config.ExcludeDataFromBackup && protectionPolicy.IgnoreIfExcludedFromBackup {
						continue
					}
					protectedTimestamps = append(protectedTimestamps, protectionPolicy.ProtectedTimestamp)
				}
			}
		}
		for _, record := range spanConfigRecords {
			collectProtectedTimestamps(record.GetConfig())
		}
		collectProtectedTimestamps(systemSpanConfigs...)

		for _, protectedTimestamp := range protectedTimestamps {
			if protectedTimestamp.WallTime < droppedAtTime {
				return true, nil
			}
		}

		return false, nil
	}()
	if err != nil {
		return false, err
	}

	if fn := execCfg.GCJobTestingKnobs.RunAfterIsProtectedCheck; fn != nil {
		fn(jobID, isProtected)
	}

	return isProtected, nil
}

// Returns whether or not a key in the given spans is protected.
// TODO(pbardea): If the TTL for this index/table expired and we're only blocked
// on a protected timestamp, this may be useful information to surface to the
// user.
func deprecatedIsProtected(
	ctx context.Context, protectedtsCache protectedts.Cache, atTime int64, sp roachpb.Span,
) bool {
	protected := false
	protectedtsCache.Iterate(ctx,
		sp.Key, sp.EndKey,
		func(r *ptpb.Record) (wantMore bool) {
			// If we encounter any protected timestamp records in this span, we
			// can't GC.
			if r.Timestamp.WallTime < atTime {
				protected = true
				return false
			}
			return true
		})
	return protected
}
