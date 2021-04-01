// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// RevertTableDefaultBatchSize is the default batch size for reverting tables.
// This only needs to be small enough to keep raft/rocks happy -- there is no
// reply size to worry about.
// TODO(dt): tune this via experimentation.
const RevertTableDefaultBatchSize = 500000

// useTBIForRevertRange is a cluster setting that controls if the time-bound
// iterator optimization is used when processing a revert range request.
var useTBIForRevertRange = settings.RegisterBoolSetting(
	"kv.bulk_io_write.revert_range_time_bound_iterator.enabled",
	"use the time-bound iterator optimization when processing a revert range request",
	true,
)

// RevertTables reverts the passed table to the target time, which much be above
// the GC threshold for every range (unless the flag ignoreGCThreshold is passed
// which should be done with care -- see RevertRangeRequest.IgnoreGCThreshold).
func RevertTables(
	ctx context.Context,
	db *kv.DB,
	execCfg *ExecutorConfig,
	tables []catalog.TableDescriptor,
	targetTime hlc.Timestamp,
	ignoreGCThreshold bool,
	batchSize int64,
) error {
	reverting := make(map[descpb.ID]bool, len(tables))
	for i := range tables {
		reverting[tables[i].GetID()] = true
	}

	spans := make([]roachpb.Span, 0, len(tables))

	// Check that all the tables are revertable -- i.e. offline and that their
	// full interleave hierarchy is being reverted.
	for i := range tables {
		if tables[i].GetState() != descpb.DescriptorState_OFFLINE {
			return errors.New("only offline tables can be reverted")
		}

		if !tables[i].IsPhysicalTable() {
			return errors.Errorf("cannot revert virtual table %s", tables[i].GetName())
		}
		for _, idx := range tables[i].NonDropIndexes() {
			for j := 0; j < idx.NumInterleaveAncestors(); j++ {
				parent := idx.GetInterleaveAncestor(j)
				if !reverting[parent.TableID] {
					return errors.New("cannot revert table without reverting all interleaved tables and indexes")
				}
			}
			for j := 0; j < idx.NumInterleavedBy(); j++ {
				child := idx.GetInterleavedBy(j)
				if !reverting[child.Table] {
					return errors.New("cannot revert table without reverting all interleaved tables and indexes")
				}
			}
		}
		spans = append(spans, tables[i].TableSpan(execCfg.Codec))
	}

	for i := range tables {
		// This is a) rare and b) probably relevant if we are looking at logs so it
		// probably makes sense to log it without a verbosity filter.
		log.Infof(ctx, "reverting table %s (%d) to time %v", tables[i].GetName(), tables[i].GetID(), targetTime)
	}

	// TODO(dt): pre-split requests up using a rangedesc cache and run batches in
	// parallel (since we're passing a key limit, distsender won't do its usual
	// splitting/parallel sending to separate ranges).
	for len(spans) != 0 {
		var b kv.Batch
		for _, span := range spans {
			b.AddRawRequest(&roachpb.RevertRangeRequest{
				RequestHeader: roachpb.RequestHeader{
					Key:    span.Key,
					EndKey: span.EndKey,
				},
				TargetTime:                          targetTime,
				IgnoreGcThreshold:                   ignoreGCThreshold,
				EnableTimeBoundIteratorOptimization: useTBIForRevertRange.Get(&execCfg.Settings.SV),
			})
		}
		b.Header.MaxSpanRequestKeys = batchSize

		if err := db.Run(ctx, &b); err != nil {
			return err
		}

		spans = spans[:0]
		for _, raw := range b.RawResponse().Responses {
			r := raw.GetRevertRange()
			if r.ResumeSpan != nil {
				if !r.ResumeSpan.Valid() {
					return errors.Errorf("invalid resume span: %s", r.ResumeSpan)
				}
				spans = append(spans, *r.ResumeSpan)
			}
		}
	}

	return nil
}
