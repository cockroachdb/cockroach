// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package scjob

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/schemachanger/scexec"
)

func newBackfillTrackerFactory(
	ctx context.Context,
	codec keys.SQLCodec,
	db *kv.DB,
	distSQLPlanner *sql.DistSQLPlanner,
	job *jobs.Job,
) func() scexec.BackfillTracker {
	return func() scexec.BackfillTracker {
		cfg := backfillTrackerConfig{
			numRangesInSpans: func(
				ctx context.Context, span roachpb.Span, containedBy []roachpb.Span,
			) (total, inContainedBy int, _ error) {
				return sql.NumRangesInSpanContainedBy(ctx, db, distSQLPlanner, span, containedBy)
			},
			writeProgressFraction: func(_ context.Context, fractionProgressed float32) error {
				if err := job.FractionProgressed(
					ctx, nil /* txn */, jobs.FractionUpdater(fractionProgressed),
				); err != nil {
					return jobs.SimplifyInvalidStatusError(err)
				}
				return nil
			},
			writeCheckpoint: func(ctx context.Context, progresses []scexec.BackfillProgress) error {
				return job.Update(ctx, nil /* txn */, func(
					txn *kv.Txn, md jobs.JobMetadata, ju *jobs.JobUpdater,
				) error {
					pl := md.Payload
					jobProgress, err := convertToJobBackfillProgress(codec, progresses)
					if err != nil {
						return err
					}
					sc := pl.GetNewSchemaChange()
					sc.BackfillProgress = jobProgress
					ju.UpdatePayload(pl)
					return nil
				})
			},
		}
		payload := job.Payload()
		return newBackfillTracker(
			codec,
			cfg,
			convertFromJobBackfillProgress(
				codec, payload.GetNewSchemaChange().BackfillProgress),
		)
	}
}

func convertToJobBackfillProgress(
	codec keys.SQLCodec, progresses []scexec.BackfillProgress,
) ([]jobspb.BackfillProgress, error) {
	ret := make([]jobspb.BackfillProgress, 0, len(progresses))
	for _, bp := range progresses {
		strippedSpans, err := removeTenantPrefixFromSpans(codec, bp.CompletedSpans)
		if err != nil {
			return nil, err
		}
		ret = append(ret, jobspb.BackfillProgress{
			TableID:        bp.TableID,
			SourceIndexID:  bp.SourceIndexID,
			DestIndexIDs:   bp.DestIndexIDs,
			WriteTimestamp: bp.MinimumWriteTimestamp,
			CompletedSpans: strippedSpans,
		})
	}
	return ret, nil
}

func convertFromJobBackfillProgress(
	codec keys.SQLCodec, progresses []jobspb.BackfillProgress,
) []scexec.BackfillProgress {
	ret := make([]scexec.BackfillProgress, 0, len(progresses))
	for _, bp := range progresses {
		ret = append(ret, scexec.BackfillProgress{
			Backfill: scexec.Backfill{
				TableID:       bp.TableID,
				SourceIndexID: bp.SourceIndexID,
				DestIndexIDs:  bp.DestIndexIDs,
			},
			MinimumWriteTimestamp: bp.WriteTimestamp,
			CompletedSpans:        addTenantPrefixToSpans(codec, bp.CompletedSpans),
		})
	}
	return ret
}

func addTenantPrefixToSpans(codec keys.SQLCodec, spans []roachpb.Span) []roachpb.Span {
	prefix := codec.TenantPrefix()
	prefix = prefix[:len(prefix):len(prefix)] // force realloc on append
	ret := make([]roachpb.Span, 0, len(spans))
	for _, sp := range spans {
		ret = append(ret, roachpb.Span{
			Key:    append(prefix, sp.Key...),
			EndKey: append(prefix, sp.EndKey...),
		})
	}
	return ret
}

func removeTenantPrefixFromSpans(
	codec keys.SQLCodec, spans []roachpb.Span,
) (ret []roachpb.Span, err error) {
	ret = make([]roachpb.Span, 0, len(spans))
	for _, sp := range spans {
		var stripped roachpb.Span
		if stripped.Key, err = codec.StripTenantPrefix(sp.Key); err != nil {
			return nil, err
		}
		if stripped.EndKey, err = codec.StripTenantPrefix(sp.EndKey); err != nil {
			return nil, err
		}
		ret = append(ret, stripped)
	}
	return ret, nil
}
