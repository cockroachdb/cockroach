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

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// ReconcileInterval is the interval between two reconciliations of protected
// timestamp records.
var ReconcileInterval = settings.RegisterDurationSetting(
	settings.TenantReadOnly,
	"kv.protectedts.reconciliation.interval",
	"the frequency for reconciling jobs with protected timestamp records",
	5*time.Minute,
	settings.NonNegativeDuration,
).WithPublic()

// StatusFunc is used to check on the status of a Record based on its Meta
// field.
type StatusFunc func(
	ctx context.Context, txn *kv.Txn, meta []byte,
) (shouldRemove bool, _ error)

// StatusFuncs maps from MetaType to a StatusFunc.
type StatusFuncs map[string]StatusFunc

// Reconciler runs a loop to reconcile the protected timestamps with external
// state. Each record's status is determined using the record's meta type and
// meta in conjunction with the configured StatusFunc.
type Reconciler struct {
	settings    *cluster.Settings
	db          *kv.DB
	pts         protectedts.Storage
	metrics     Metrics
	statusFuncs StatusFuncs
}

// New constructs a Reconciler.
func New(
	st *cluster.Settings, db *kv.DB, storage protectedts.Storage, statusFuncs StatusFuncs,
) *Reconciler {
	return &Reconciler{
		settings:    st,
		db:          db,
		pts:         storage,
		metrics:     makeMetrics(),
		statusFuncs: statusFuncs,
	}
}

// StartReconciler implements the protectedts.Reconciler interface.
func (r *Reconciler) StartReconciler(ctx context.Context, stopper *stop.Stopper) error {
	return stopper.RunAsyncTask(ctx, "protectedts-reconciliation", func(ctx context.Context) {
		r.run(ctx, stopper)
	})
}

// Metrics returns the reconciler's metrics.
func (r *Reconciler) Metrics() *Metrics {
	return &r.metrics
}

func (r *Reconciler) run(ctx context.Context, stopper *stop.Stopper) {
	reconcileIntervalChanged := make(chan struct{}, 1)
	ReconcileInterval.SetOnChange(&r.settings.SV, func(ctx context.Context) {
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

func (r *Reconciler) reconcile(ctx context.Context) {
	// Load protected timestamp records.
	var state ptpb.State
	if err := r.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		var err error
		state, err = r.pts.GetState(ctx, txn)
		return err
	}); err != nil {
		r.metrics.ReconciliationErrors.Inc(1)
		log.Errorf(ctx, "failed to load protected timestamp records: %+v", err)
		return
	}
	for _, rec := range state.Records {
		task, ok := r.statusFuncs[rec.MetaType]
		if !ok {
			// NB: We don't expect to ever hit this case outside of testing.
			continue
		}
		var didRemove bool
		if err := r.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
			didRemove = false // reset for retries
			shouldRemove, err := task(ctx, txn, rec.Meta)
			if err != nil {
				return err
			}
			if !shouldRemove {
				return nil
			}
			err = r.pts.Release(ctx, txn, rec.ID.GetUUID())
			if err != nil && !errors.Is(err, protectedts.ErrNotExists) {
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
	}
	r.metrics.ReconcilationRuns.Inc(1)
}
