// Copyright 2020 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadWriteCommand(roachpb.Migrate, declareKeysMigrate, Migrate)
}

func declareKeysMigrate(
	_ *roachpb.RangeDescriptor,
	header roachpb.Header,
	_ roachpb.Request,
	latchSpans, lockSpans *spanset.SpanSet,
) {
	// TODO(irfansharif): This will eventually grow to capture the super set of
	// all keys accessed by all migrations defined here. That could get
	// cumbersome. We could spruce up the migration type and allow authors to
	// define the allow authors for specific set of keys each migration needs to
	// grab latches and locks over.
	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.RaftTruncatedStateLegacyKey(header.RangeID)})
	lockSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.RaftTruncatedStateLegacyKey(header.RangeID)})
}

// migrationRegistry is a global registry of all KV-level migrations. See
// pkg/migration for details around how the migrations defined here are
// wired up.
var migrationRegistry = make(map[roachpb.Version]migration)

type migration func(context.Context, storage.ReadWriter, CommandArgs) (result.Result, error)

func init() {
	registerMigration(clusterversion.TruncatedAndRangeAppliedStateMigration, truncatedAndAppliedStateMigration)
}

func registerMigration(key clusterversion.Key, migration migration) {
	migrationRegistry[clusterversion.ByKey(key)] = migration
}

// Migrate executes the below-raft migration corresponding to the given version.
func Migrate(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, _ roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.MigrateRequest)

	fn, ok := migrationRegistry[args.Version]
	if !ok {
		return result.Result{}, errors.Newf("migration for %s not found", args.Version)
	}
	return fn(ctx, readWriter, cArgs)
}

// truncatedAndRangeAppliedStateMigration lets us stop using the legacy
// replicated truncated state and start using the new RangeAppliedState for this
// specific range.
func truncatedAndAppliedStateMigration(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs,
) (result.Result, error) {
	var legacyTruncatedState roachpb.RaftTruncatedState
	legacyKeyFound, err := storage.MVCCGetProto(
		ctx, readWriter, keys.RaftTruncatedStateLegacyKey(cArgs.EvalCtx.GetRangeID()),
		hlc.Timestamp{}, &legacyTruncatedState, storage.MVCCGetOptions{},
	)
	if err != nil {
		return result.Result{}, err
	}

	var pd result.Result
	if legacyKeyFound {
		// Time to migrate by deleting the legacy key. The downstream-of-Raft
		// code will atomically rewrite the truncated state (supplied via the
		// side effect) into the new unreplicated key.
		if err := storage.MVCCDelete(
			ctx, readWriter, cArgs.Stats, keys.RaftTruncatedStateLegacyKey(cArgs.EvalCtx.GetRangeID()),
			hlc.Timestamp{}, nil, /* txn */
		); err != nil {
			return result.Result{}, err
		}
		pd.Replicated.State = &kvserverpb.ReplicaState{
			// We need to pass in a truncated state to enable the migration.
			// Passing the same one is the easiest thing to do.
			TruncatedState: &legacyTruncatedState,
		}
	}
	return pd, nil
}
