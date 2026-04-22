// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cloud"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// Driver is a thin glass over Producer + TickManager: it constructs
// both, wires the producer's TickSink to the manager, and forwards
// OnValue / OnCheckpoint to the producer. It exists to give tests
// (and a future "single-process job" entry point) one object with
// rangefeed-shaped methods and nothing else.
//
// In production with a DistSQL split, Driver is replaced by the
// distsql wiring: producer processors hold a Producer with a
// metadata-emitting TickSink, and the coordinator processor holds
// a TickManager that consumes that metadata. Producer and
// TickManager keep their interfaces; only the wiring around them
// changes.
type Driver struct {
	Producer *Producer
	Manager  *TickManager
}

// NewDriver constructs a Driver against the given external storage
// for a single producer covering the given spans. resume is the
// per-producer resume slice (zero value = first run).
//
// Driver does not subscribe a descriptor rangefeed (it's the
// in-process / test seam), so it disables the TickManager's
// descriptor-frontier gating — the close loop fires purely off
// the data frontier, matching how Driver-based tests have
// always behaved.
//
// forwardThreshold is the producer's coalesce-forward threshold; see
// NewProducer. Tests that don't care about coalescing typically pass
// 0 to keep every flush as its own SST file.
func NewDriver(
	es cloud.ExternalStorage,
	spans []roachpb.Span,
	startHLC hlc.Timestamp,
	tickWidth time.Duration,
	fileIDs FileIDSource,
	resume ResumeState,
	forwardThreshold int64,
) (*Driver, error) {
	manager, err := NewTickManager(es, spans, startHLC, tickWidth, fileIDs)
	if err != nil {
		return nil, err
	}
	manager.DisableDescFrontier()
	producer, err := NewProducer(
		es, spans, startHLC, tickWidth, fileIDs, manager, resume, forwardThreshold,
	)
	if err != nil {
		return nil, err
	}
	return &Driver{Producer: producer, Manager: manager}, nil
}

// OnValue forwards to the underlying Producer. Drop-in for a
// rangefeed.OnValue callback (via an adapter that unpacks
// *kvpb.RangeFeedValue into these args).
func (d *Driver) OnValue(
	ctx context.Context, key roachpb.Key, ts hlc.Timestamp, value, prevValue []byte,
) {
	d.Producer.OnValue(ctx, key, ts, value, prevValue)
}

// OnCheckpoint forwards to the underlying Producer. Drop-in for a
// rangefeed.OnCheckpoint callback (via an adapter that unpacks
// *kvpb.RangeFeedCheckpoint).
func (d *Driver) OnCheckpoint(ctx context.Context, sp roachpb.Span, ts hlc.Timestamp) error {
	return d.Producer.OnCheckpoint(ctx, sp, ts)
}
