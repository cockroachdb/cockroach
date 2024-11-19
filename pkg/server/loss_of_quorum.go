// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package server

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/liveness/livenesspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/iterutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/strutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/pebble/vfs"
)

// newPlanStore creates a plan store inside the first store's directory or
// inside a vfs for the first store if in memory configuration is used.
// This function will respect sticky in-memory configuration of test clusters.
func newPlanStore(cfg Config) (loqrecovery.PlanStore, error) {
	spec := cfg.Stores.Specs[0]
	fs := vfs.Default
	path := spec.Path
	if spec.InMemory {
		path = ""
		if spec.StickyVFSID != "" {
			if cfg.TestingKnobs.Server == nil {
				return loqrecovery.PlanStore{}, errors.AssertionFailedf("Could not create a sticky " +
					"engine no server knobs available to get a registry. " +
					"Please use Knobs.Server.StickyVFSRegistry to provide one.")
			}
			knobs := cfg.TestingKnobs.Server.(*TestingKnobs)
			if knobs.StickyVFSRegistry == nil {
				return loqrecovery.PlanStore{}, errors.Errorf("Could not create a sticky " +
					"engine no registry available. Please use " +
					"Knobs.Server.StickyVFSRegistry to provide one.")
			}
			fs = knobs.StickyVFSRegistry.Get(spec.StickyVFSID)
		} else {
			fs = vfs.NewMem()
		}
	}
	return loqrecovery.NewPlanStore(path, fs), nil
}

func logPendingLossOfQuorumRecoveryEvents(ctx context.Context, stores *kvserver.Stores) {
	if err := stores.VisitStores(func(s *kvserver.Store) error {
		// We are not requesting entry deletion here because we need those entries
		// at the end of startup to populate rangelog and possibly other durable
		// cluster-replicated destinations.
		eventCount, err := loqrecovery.RegisterOfflineRecoveryEvents(
			ctx,
			s.TODOEngine(),
			func(ctx context.Context, record loqrecoverypb.ReplicaRecoveryRecord) (bool, error) {
				event := record.AsStructuredLog()
				log.StructuredEvent(ctx, severity.INFO, &event)
				return false, nil
			})
		if eventCount > 0 {
			log.Infof(
				ctx, "registered %d loss of quorum replica recovery events for s%d",
				eventCount, s.Ident.StoreID)
		}
		return err
	}); err != nil {
		// We don't want to abort server if we can't record recovery events
		// as it is the last thing we need if cluster is already unhealthy.
		log.Errorf(ctx, "failed to record loss of quorum recovery events: %v", err)
	}
}

func maybeRunLossOfQuorumRecoveryCleanup(
	ctx context.Context,
	ie isql.Executor,
	stores *kvserver.Stores,
	server *topLevelServer,
	stopper *stop.Stopper,
) {
	publishCtx, publishCancel := stopper.WithCancelOnQuiesce(ctx)
	_ = stopper.RunAsyncTask(publishCtx, "publish-loss-of-quorum-events", func(ctx context.Context) {
		defer publishCancel()
		if err := stores.VisitStores(func(s *kvserver.Store) error {
			_, err := loqrecovery.RegisterOfflineRecoveryEvents(
				ctx,
				s.TODOEngine(),
				func(ctx context.Context, record loqrecoverypb.ReplicaRecoveryRecord) (bool, error) {
					sqlExec := func(ctx context.Context, stmt string, args ...interface{}) (int, error) {
						return ie.ExecEx(ctx, "", nil,
							sessiondata.NodeUserSessionDataOverride, stmt, args...)
					}
					if err := loqrecovery.UpdateRangeLogWithRecovery(ctx, sqlExec, record); err != nil {
						return false, errors.Wrap(err,
							"loss of quorum recovery failed to write RangeLog entry")
					}
					// We only bump metrics as the last step when all processing of events
					// is finished. This is done to ensure that we don't increase metrics
					// more than once.
					// Note that if actual deletion of event fails, it is possible to
					// duplicate rangelog and metrics, but that is very unlikely event
					// and user should be able to identify those events.
					s.Metrics().RangeLossOfQuorumRecoveries.Inc(1)
					return true, nil
				})
			return err
		}); err != nil {
			// We don't want to abort server if we can't record recovery events
			// as it is the last thing we need if cluster is already unhealthy.
			log.Errorf(ctx, "failed to update range log with loss of quorum recovery events: %v", err)
		}
	})

	var cleanup loqrecoverypb.DeferredRecoveryActions
	var actionsSource storage.ReadWriter
	err := stores.VisitStores(func(s *kvserver.Store) error {
		c, found, err := loqrecovery.ReadCleanupActionsInfo(ctx, s.TODOEngine())
		if err != nil {
			log.Errorf(ctx, "failed to read loss of quorum recovery cleanup actions info from store: %s", err)
			return nil
		}
		if found {
			cleanup = c
			actionsSource = s.TODOEngine()
			return iterutil.StopIteration()
		}
		return nil
	})
	if err := iterutil.Map(err); err != nil {
		log.Infof(ctx, "failed to iterate node stores while searching for loq recovery cleanup info: %s", err)
		return
	}
	if len(cleanup.DecommissionedNodeIDs) == 0 {
		return
	}
	decomCtx, decomCancel := stopper.WithCancelOnQuiesce(ctx)
	_ = stopper.RunAsyncTask(decomCtx, "maybe-mark-nodes-as-decommissioned", func(ctx context.Context) {
		defer decomCancel()
		log.Infof(ctx, "loss of quorum recovery decommissioning removed nodes %s",
			strutil.JoinIDs("n", cleanup.DecommissionedNodeIDs))
		retryOpts := retry.Options{
			InitialBackoff: 10 * time.Second,
			MaxBackoff:     time.Hour,
			Multiplier:     2,
		}
		for r := retry.StartWithCtx(ctx, retryOpts); r.Next(); {
			// Nodes are already dead, but are in active state. Internal checks doesn't
			// allow us throwing nodes away, and they need to go through legal state
			// transitions within liveness to succeed. To achieve that we mark nodes as
			// decommissioning first, followed by decommissioned.
			// Those operations may fail because other nodes could be restarted
			// concurrently and also trying this cleanup. We rely on retry and change
			// being idempotent for operation to complete.
			// Mind that it is valid to mark decommissioned nodes as decommissioned and
			// that would result in a noop, so it is safe to always go through this
			// cycle without prior checks for current state.
			err := server.Decommission(ctx, livenesspb.MembershipStatus_DECOMMISSIONING,
				cleanup.DecommissionedNodeIDs)
			if err != nil {
				log.Infof(ctx,
					"loss of quorum recovery cleanup failed to decommissioning dead nodes, this is ok as cluster might not be healed yet: %s", err)
				continue
			}
			err = server.Decommission(ctx, livenesspb.MembershipStatus_DECOMMISSIONED,
				cleanup.DecommissionedNodeIDs)
			if err != nil {
				log.Infof(ctx,
					"loss of quorum recovery cleanup failed to decommissioning dead nodes, this is ok as cluster might not be healed yet: %s", err)
				continue
			}
			if err = loqrecovery.RemoveCleanupActionsInfo(ctx, actionsSource); err != nil {
				log.Infof(ctx, "failed to remove ")
			}
			break
		}
		log.Infof(ctx, "loss of quorum recovery cleanup finished decommissioning removed nodes")
	})
}
