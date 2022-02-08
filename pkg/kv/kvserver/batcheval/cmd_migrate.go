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
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/stateloader"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadWriteCommand(roachpb.Migrate, declareKeysMigrate, Migrate)
}

func declareKeysMigrate(
	rs ImmutableRangeState,
	_ *roachpb.Header,
	_ roachpb.Request,
	latchSpans, _ *spanset.SpanSet,
	_ time.Duration,
) {
	// TODO(irfansharif): This will eventually grow to capture the super set of
	// all keys accessed by all migrations defined here. That could get
	// cumbersome. We could spruce up the migration type and allow authors to
	// define the allow authors for specific set of keys each migration needs to
	// grab latches and locks over.

	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.RangeVersionKey(rs.GetRangeID())})
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(rs.GetStartKey())})
}

// migrationRegistry is a global registry of all KV-level migrations. See
// pkg/migration for details around how the migrations defined here are
// wired up.
var migrationRegistry = make(map[roachpb.Version]migration)

type migration func(context.Context, storage.ReadWriter, CommandArgs) (result.Result, error)

func init() {
	_ = registerMigration // prevent unused warning.
	registerMigration(
		clusterversion.AddRaftAppliedIndexTermMigration, addRaftAppliedIndexTermMigration)
}

func registerMigration(key clusterversion.Key, migration migration) {
	migrationRegistry[clusterversion.ByKey(key)] = migration
}

// Migrate executes the below-raft migration corresponding to the given version.
// See roachpb.MigrateRequest for more details.
func Migrate(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, _ roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.MigrateRequest)
	migrationVersion := args.Version

	fn, ok := migrationRegistry[migrationVersion]
	if !ok {
		return result.Result{}, errors.AssertionFailedf("migration for %s not found", migrationVersion)
	}
	pd, err := fn(ctx, readWriter, cArgs)
	if err != nil {
		return result.Result{}, err
	}

	// Since we're a below raft migration, we'll need update our replica state
	// version.
	if err := MakeStateLoader(cArgs.EvalCtx).SetVersion(
		ctx, readWriter, cArgs.Stats, &migrationVersion,
	); err != nil {
		return result.Result{}, err
	}
	if pd.Replicated.State == nil {
		pd.Replicated.State = &kvserverpb.ReplicaState{}
	}
	// NB: We don't check for clusterversion.ReplicaVersions being active here
	// as all below-raft migrations (the only users of Migrate) were introduced
	// after it.
	pd.Replicated.State.Version = &migrationVersion
	return pd, nil
}

// addRaftAppliedIndexTermMigration migrates the system to start populating
// the RangeAppliedState.RaftAppliedIndexTerm field.
func addRaftAppliedIndexTermMigration(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs,
) (result.Result, error) {
	return result.Result{
		Replicated: kvserverpb.ReplicatedEvalResult{
			State: &kvserverpb.ReplicaState{
				// Signal the migration by sending a term on the new field that we
				// want to migrate into. This term is chosen as one that would never
				// be used in practice (since raftInitialLogTerm is 10), so we can
				// special-case it below raft and start writing the (real) term to the
				// AppliedState.
				RaftAppliedIndexTerm: stateloader.RaftLogTermSignalForAddRaftAppliedIndexTermMigration,
			},
		},
	}, nil
}

// TestingRegisterMigrationInterceptor is used in tests to register an
// interceptor for a below-raft migration.
//
// TODO(irfansharif): This is a gross anti-pattern, we're letting tests mutate
// global state. This should instead be accessed EvalKnobs() instead.
func TestingRegisterMigrationInterceptor(version roachpb.Version, fn func()) (unregister func()) {
	if _, ok := migrationRegistry[version]; ok {
		panic("doubly registering migration")
	}
	migrationRegistry[version] = func(context.Context, storage.ReadWriter, CommandArgs) (result.Result, error) {
		fn()
		return result.Result{}, nil
	}
	return func() { delete(migrationRegistry, version) }
}
