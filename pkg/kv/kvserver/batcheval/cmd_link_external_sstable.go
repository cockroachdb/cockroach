// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

func init() {
	// Taking out latches/locks across the entire SST span is very coarse, and we
	// could instead iterate over the SST and take out point latches/locks, but
	// the cost is likely not worth it since LinkExternalSSTable is often used with
	// unpopulated spans.
	RegisterReadWriteCommand(kvpb.LinkExternalSSTable, declareKeysAddSSTable, EvalLinkExternalSSTable)
}

// EvalLinkExternalSSTable evaluates an LinkExternalSSTable command. For details, see doc comment
// on LinkExternalSSTableRequest.
func EvalLinkExternalSSTable(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.LinkExternalSSTableRequest)
	ms := cArgs.Stats
	start, end := storage.MVCCKey{Key: args.Key}, storage.MVCCKey{Key: args.EndKey}
	sstToReqTS := args.SSTTimestampToRequestTimestamp

	var span *tracing.Span
	ctx, span = tracing.ChildSpan(ctx, "LinkExternalSSTable")
	defer span.Finish()
	log.Eventf(ctx, "evaluating External SSTable [%s,%s)", start.Key, end.Key)

	path := args.ExternalFile.Path
	log.VEventf(ctx, 1, "Link External SSTable file %s in %s", path, args.ExternalFile.Locator)

	// We have no idea if the SST being ingested contains keys that will shadow
	// existing keys or not, so we need to force its mvcc stats to be estimates.
	s := *args.MVCCStats
	s.ContainsEstimates++
	ms.Add(s)

	return result.Result{
		Replicated: kvserverpb.ReplicatedEvalResult{
			LinkExternalSSTable: &kvserverpb.ReplicatedEvalResult_LinkExternalSSTable{
				RemoteFileLoc:           args.ExternalFile.Locator,
				RemoteFilePath:          path,
				ApproximatePhysicalSize: args.ExternalFile.ApproximatePhysicalSize,
				BackingFileSize:         args.ExternalFile.BackingFileSize,
				Span:                    roachpb.Span{Key: start.Key, EndKey: end.Key},
				RemoteRewriteTimestamp:  sstToReqTS,
				RemoteSyntheticPrefix:   args.ExternalFile.SyntheticPrefix,
			},
			// Since the remote SST could contain keys at any timestamp, consider it
			// a history mutation.
			MVCCHistoryMutation: &kvserverpb.ReplicatedEvalResult_MVCCHistoryMutation{
				Spans: []roachpb.Span{{Key: start.Key, EndKey: end.Key}},
			},
		},
	}, nil
}
