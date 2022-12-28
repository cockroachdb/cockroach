// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/loqrecovery/loqrecoverypb"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondata"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
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
		path = "."
		if spec.StickyInMemoryEngineID != "" {
			if cfg.TestingKnobs.Server == nil {
				return loqrecovery.PlanStore{}, errors.AssertionFailedf("Could not create a sticky " +
					"engine no server knobs available to get a registry. " +
					"Please use Knobs.Server.StickyEngineRegistry to provide one.")
			}
			knobs := cfg.TestingKnobs.Server.(*TestingKnobs)
			if knobs.StickyEngineRegistry == nil {
				return loqrecovery.PlanStore{}, errors.Errorf("Could not create a sticky " +
					"engine no registry available. Please use " +
					"Knobs.Server.StickyEngineRegistry to provide one.")
			}
			var err error
			fs, err = knobs.StickyEngineRegistry.GetUnderlyingFS(spec)
			if err != nil {
				return loqrecovery.PlanStore{}, err
			}
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
			s.Engine(),
			func(ctx context.Context, record loqrecoverypb.ReplicaRecoveryRecord) (bool, error) {
				event := record.AsStructuredLog()
				log.StructuredEvent(ctx, &event)
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

func publishPendingLossOfQuorumRecoveryEvents(
	ctx context.Context, ie sqlutil.InternalExecutor, stores *kvserver.Stores, stopper *stop.Stopper,
) {
	_ = stopper.RunAsyncTask(ctx, "publish-loss-of-quorum-events", func(ctx context.Context) {
		if err := stores.VisitStores(func(s *kvserver.Store) error {
			_, err := loqrecovery.RegisterOfflineRecoveryEvents(
				ctx,
				s.Engine(),
				func(ctx context.Context, record loqrecoverypb.ReplicaRecoveryRecord) (bool, error) {
					sqlExec := func(ctx context.Context, stmt string, args ...interface{}) (int, error) {
						return ie.ExecEx(ctx, "", nil,
							sessiondata.RootUserSessionDataOverride, stmt, args...)
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
}
