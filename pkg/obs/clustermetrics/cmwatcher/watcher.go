// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cmwatcher

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/startup"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
)

// Handler contains callbacks invoked by the Watcher as rangefeed events arrive.
type Handler struct {
	// OnUpsert is called for each row that is inserted or updated
	// (incremental changes only, not during the initial scan).
	OnUpsert func(ctx context.Context, row ClusterMetricRow)
	// OnDelete is called for each row that is deleted. Only the ID field
	// of the row is populated.
	OnDelete func(ctx context.Context, row ClusterMetricRow)
	// OnRefresh is called with the complete set of rows after the initial
	// table scan and after any rangefeed restart. Ownership of the map is
	// transferred to the callback.
	OnRefresh func(ctx context.Context, rows map[int64]ClusterMetricRow)
}

// Watcher monitors the system.cluster_metrics table via a rangefeed and
// delivers events to the caller through Handler callbacks.
type Watcher struct {
	codec   keys.SQLCodec
	clock   *hlc.Clock
	f       *rangefeed.Factory
	stopper *stop.Stopper
	dec     RowDecoder
	handler Handler
}

// NewWatcher constructs a new Watcher. The provided handler callbacks are
// invoked as events arrive from the rangefeed. The codec determines the
// tenant keyspace that the rangefeed watches.
func NewWatcher(
	codec keys.SQLCodec,
	clock *hlc.Clock,
	f *rangefeed.Factory,
	stopper *stop.Stopper,
	handler Handler,
) *Watcher {
	return &Watcher{
		codec:   codec,
		clock:   clock,
		f:       f,
		stopper: stopper,
		dec:     MakeRowDecoder(codec),
		handler: handler,
	}
}

// Start sets up the rangefeed and waits for the initial scan. An error is
// returned if the initial table scan fails, the context is canceled, or the
// stopper is stopped before the initial data is retrieved.
func (w *Watcher) Start(ctx context.Context, sysTableResolver catalog.SystemTableIDResolver) error {
	return w.startRangeFeed(ctx, sysTableResolver)
}

func (w *Watcher) startRangeFeed(
	ctx context.Context, sysTableResolver catalog.SystemTableIDResolver,
) error {
	tableID, err := startup.RunIdempotentWithRetryEx(ctx,
		w.stopper.ShouldQuiesce(),
		"cluster-metrics-watcher start",
		func(ctx context.Context) (descpb.ID, error) {
			return sysTableResolver.LookupSystemTableID(
				ctx, systemschema.ClusterMetricsTable.GetName(),
			)
		})
	if err != nil {
		return err
	}

	tablePrefix := w.codec.TablePrefix(uint32(tableID))
	tableSpan := roachpb.Span{
		Key:    tablePrefix,
		EndKey: tablePrefix.PrefixEnd(),
	}

	var initialScan = struct {
		ch   chan struct{}
		done bool
		err  error
	}{
		ch: make(chan struct{}),
	}

	// allRows accumulates rows during a full table scan (initial or rescan).
	allRows := make(map[int64]ClusterMetricRow)

	translateEvent := func(
		ctx context.Context, kv *kvpb.RangeFeedValue,
	) (rangefeedbuffer.Event, bool) {
		row, tombstone, err := w.dec.DecodeRow(roachpb.KeyValue{
			Key:   kv.Key,
			Value: kv.Value,
		})
		if err != nil {
			log.Dev.Warningf(ctx, "failed to decode cluster_metrics row %v: %v", kv.Key, err)
			return nil, false
		}
		if allRows != nil {
			// We are in the process of doing a full table scan.
			if tombstone {
				log.Dev.Warning(ctx, "unexpected empty value during rangefeed scan")
				return nil, false
			}
			allRows[row.ID] = row
		} else {
			// Processing incremental changes.
			if tombstone {
				if w.handler.OnDelete != nil {
					w.handler.OnDelete(ctx, row)
				}
			} else {
				if w.handler.OnUpsert != nil {
					w.handler.OnUpsert(ctx, row)
				}
			}
		}
		return nil, false
	}

	onUpdate := func(ctx context.Context, update rangefeedcache.Update[rangefeedbuffer.Event]) {
		if update.Type == rangefeedcache.CompleteUpdate {
			if w.handler.OnRefresh != nil {
				w.handler.OnRefresh(ctx, allRows)
			}
			allRows = nil

			if !initialScan.done {
				initialScan.done = true
				close(initialScan.ch)
			}
		}
	}

	onError := func(err error) {
		if !initialScan.done {
			initialScan.err = err
			initialScan.done = true
			close(initialScan.ch)
		} else {
			// The rangefeed will restart and rescan the table.
			allRows = make(map[int64]ClusterMetricRow)
		}
	}

	c := rangefeedcache.NewWatcher(
		"cluster-metrics-watcher",
		w.clock, w.f,
		0, /* bufferSize */
		[]roachpb.Span{tableSpan},
		false, /* withPrevValue */
		true,  /* withRowTSInInitialScan */
		translateEvent,
		onUpdate,
		nil, /* knobs */
	)

	if err := rangefeedcache.Start(ctx, w.stopper, c, onError); err != nil {
		return err
	}

	// Wait for the initial scan before returning.
	select {
	case <-initialScan.ch:
		return initialScan.err

	case <-w.stopper.ShouldQuiesce():
		return errors.Wrap(stop.ErrUnavailable, "failed to retrieve initial cluster metrics")

	case <-ctx.Done():
		return errors.Wrap(ctx.Err(), "failed to retrieve initial cluster metrics")
	}
}
