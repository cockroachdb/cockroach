// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/lockspanset"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadWriteCommand(kvpb.Migrate, declareKeysMigrate, Migrate)
}

func declareKeysMigrate(
	rs ImmutableRangeState,
	_ *kvpb.Header,
	_ kvpb.Request,
	latchSpans *spanset.SpanSet,
	_ *lockspanset.LockSpanSet,
	_ time.Duration,
) error {
	// TODO(irfansharif): This will eventually grow to capture the super set of
	// all keys accessed by all migrations defined here. That could get
	// cumbersome. We could spruce up the migration type and allow authors to
	// define the allow authors for specific set of keys each migration needs to
	// grab latches and locks over.

	latchSpans.AddNonMVCC(spanset.SpanReadWrite, roachpb.Span{Key: keys.RangeVersionKey(rs.GetRangeID())})
	latchSpans.AddNonMVCC(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(rs.GetStartKey())})
	return nil
}

// migrationRegistry is a global registry of all KV-level migrations. See
// pkg/migration for details around how the migrations defined here are
// wired up.
var migrationRegistry = make(map[roachpb.Version]migration)

type migration func(context.Context, storage.ReadWriter, CommandArgs) (result.Result, error)

// Migrate executes the below-raft migration corresponding to the given version.
// See kvpb.MigrateRequest for more details.
func Migrate(
	ctx context.Context, readWriter storage.ReadWriter, cArgs CommandArgs, _ kvpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*kvpb.MigrateRequest)
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
	// Set DoTimelyApplicationToAllReplicas so that migrates are applied on all
	// replicas. This is done since MigrateRequests trigger a call to
	// waitForApplication (see Replica.executeWriteBatch).
	if cArgs.EvalCtx.ClusterSettings().Version.IsActive(ctx, clusterversion.V25_1_AddRangeForceFlushKey) ||
		cArgs.EvalCtx.EvalKnobs().OverrideDoTimelyApplicationToAllReplicas {
		pd.Replicated.DoTimelyApplicationToAllReplicas = true
	}
	return pd, nil
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
