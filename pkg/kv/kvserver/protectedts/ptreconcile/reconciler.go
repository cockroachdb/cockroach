// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package ptreconcile provides logic to reconcile protected timestamp records
// with state associated with their metadata.
package ptreconcile

import (
	"context"
	"math/rand"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// ReconcileInterval is the interval between two generations of the reports.
// When set to zero - disables the report generation.
var ReconcileInterval = settings.RegisterPublicNonNegativeDurationSetting(
	"kv.protectedts.reconciliation.interval",
	"the frequency for reconciling jobs with protected timestamp records",
	5*time.Minute,
)

// StatusFunc is used to check on the status of a Record based on its Meta
// field.
type StatusFunc func(
	ctx context.Context, txn *client.Txn, meta []byte,
) (shouldRemove bool, _ error)

// StatusFuncs maps from MetaType to a StatusFunc.
type StatusFuncs map[string]StatusFunc

// Config configures a Reconciler.
type Config struct {
	Settings *cluster.Settings
	// Stores is used to ensure that we only run the reconciliation loop on
	Stores  *kvserver.Stores
	DB      *client.DB
	Storage protectedts.Storage
	Cache   protectedts.Cache

	// We want a map from metaType to a function which determines whether we
	// should clean it up.
	StatusFuncs StatusFuncs
}

// Reconciler runs an a loop to reconcile the protected timestamps with external
// state. Each record's status is determined using the record's meta type and
// meta in conjunction with the configured StatusFunc.
type Reconciler struct {
	settings    *cluster.Settings
	localStores *kvserver.Stores
	db          *client.DB
	cache       protectedts.Cache
	pts         protectedts.Storage
	metrics     Metrics
	statusFuncs StatusFuncs
}

// NewReconciler constructs a Reconciler.
func NewReconciler(cfg Config) *Reconciler {
	return &Reconciler{
		settings:    cfg.Settings,
		localStores: cfg.Stores,
		db:          cfg.DB,
		cache:       cfg.Cache,
		pts:         cfg.Storage,
		metrics:     makeMetrics(),
		statusFuncs: cfg.StatusFuncs,
	}
}

// Metrics returns the Reconciler's metrics.
func (r *Reconciler) Metrics() *Metrics {
	return &r.metrics
}

// Start will start the Reconciler.
func (r *Reconciler) Start(ctx context.Context, stopper *stop.Stopper) error {
	return stopper.RunAsyncTask(ctx, "protectedts-reconciliation", func(ctx context.Context) {
		r.run(ctx, stopper)
	})
}

func (r *Reconciler) run(ctx context.Context, stopper *stop.Stopper) {
	reconcileIntervalChanged := make(chan struct{}, 1)
	ReconcileInterval.SetOnChange(&r.settings.SV, func() {
		select {
		case reconcileIntervalChanged <- struct{}{}:
		default:
		}
	})
	lastReconciled := time.Time{}
	getInterval := func() time.Duration {
		interval := ReconcileInterval.Get(&r.settings.SV)
		const jitterFrac = .1
		return time.Duration(float64(interval) * (1 + (rand.Float64()-.5)*jitterFrac))
	}
	timer := timeutil.NewTimer()
	for {
		timer.Reset(timeutil.Until(lastReconciled.Add(getInterval())))
		select {
		case <-timer.C:
			timer.Read = true
			r.reconcile(ctx)
			lastReconciled = timeutil.Now()
		case <-reconcileIntervalChanged:
			// Go back around again.
		case <-stopper.ShouldQuiesce():
			return
		case <-ctx.Done():
			return
		}
	}
}

func (r *Reconciler) isMeta1Leaseholder(ctx context.Context, now hlc.Timestamp) (bool, error) {
	repl, _, err := r.localStores.GetReplicaForRangeID(1)
	if roachpb.IsRangeNotFoundError(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return repl.OwnsValidLease(now), nil
}

func (r *Reconciler) reconcile(ctx context.Context) {
	now := r.db.Clock().Now()
	isLeaseholder, err := r.isMeta1Leaseholder(ctx, now)
	if err != nil {
		log.Errorf(ctx, "failed to determine whether the local store contains the meta1 lease: %v", err)
		return
	}
	if !isLeaseholder {
		return
	}
	if err := r.cache.Refresh(ctx, now); err != nil {
		log.Errorf(ctx, "failed to refresh the protected timestamp cache to %v: %v", now, err)
		return
	}
	r.cache.Iterate(ctx, keys.MinKey, keys.MaxKey, func(rec *ptpb.Record) (wantMore bool) {
		task, ok := r.statusFuncs[rec.MetaType]
		if !ok {
			// NB: We don't expect to ever hit this case outside of testing.
			log.Infof(ctx, "found protected timestamp record with unknown meta type %q, skipping", rec.MetaType)
			return true
		}
		var didRemove bool
		if err := r.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) (err error) {
			didRemove = false // reset for retries
			shouldRemove, err := task(ctx, txn, rec.Meta)
			if err != nil {
				return err
			}
			if !shouldRemove {
				return nil
			}
			err = r.pts.Release(ctx, txn, rec.ID)
			if err != nil && err != protectedts.ErrNotExists {
				return err
			}
			didRemove = true
			return nil
		}); err != nil {
			r.metrics.ReconciliationErrors.Inc(1)
			log.Errorf(ctx, "failed to reconcile protected timestamp with id %s: %v",
				rec.ID.String(), err)
		} else {
			r.metrics.RecordsProcessed.Inc(1)
			if didRemove {
				r.metrics.RecordsRemoved.Inc(1)
			}
		}
		return true
	})
	r.metrics.ReconcilationRuns.Inc(1)
}
