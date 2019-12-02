// Copyright 2014 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

func init() {
	RegisterCommand(roachpb.Migrate, declareKeysMigrate, Migrate)
}

func declareKeysMigrate(
	_ *roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	spans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.RaftTruncatedStateLegacyKey(header.RangeID)})
}

// Migrate ensures that the range proactively carries out any outstanding
// below-Raft migrations. When this command returns, all of the below-Raft
// features known to be available are enabled.
func Migrate(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	newV := cArgs.Args.(*roachpb.MigrateRequest).NewVersion
	if newV.Less(cluster.VersionByKey(cluster.VersionNoLegacyTruncatedAndAppliedState)) {
		// This is only hit during testing or when upgrading an alpha to an unstable below
		// the above version. In production, newV will be a major release and more precisely,
		// it will equal the BinaryServerVersion of any binary in the cluster (they are all
		// equal when the cluster version is bumped, mod pathological races due to user error).
		return result.Result{}, nil
	}

	var legacyTruncatedState roachpb.RaftTruncatedState
	legacyKeyFound, err := engine.MVCCGetProto(
		ctx, batch, keys.RaftTruncatedStateLegacyKey(cArgs.EvalCtx.GetRangeID()),
		hlc.Timestamp{}, &legacyTruncatedState, engine.MVCCGetOptions{},
	)
	if err != nil {
		return result.Result{}, err
	}

	var pd result.Result
	if legacyKeyFound {
		// Time to migrate by deleting the legacy key. The downstream-of-Raft
		// code will atomically rewrite the truncated state (supplied via the
		// side effect) into the new unreplicated key.
		if err := engine.MVCCDelete(
			ctx, batch, cArgs.Stats, keys.RaftTruncatedStateLegacyKey(cArgs.EvalCtx.GetRangeID()),
			hlc.Timestamp{}, nil, /* txn */
		); err != nil {
			return result.Result{}, err
		}
		pd.Replicated.State = &storagepb.ReplicaState{
			// We need to pass in a truncated state to enable the migration.
			// Passing the same one is the easiest thing to do.
			TruncatedState: &legacyTruncatedState,
		}
	}
	return pd, nil
}
