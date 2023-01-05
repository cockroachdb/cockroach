// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tenantcapabilitieswatcher

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities"
	"github.com/cockroachdb/cockroach/pkg/multitenant/tenantcapabilities/tenantcapabilitiesauthorizer"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// Watcher is a concrete implementation of the tenantcapabilities.Watcher
// interface.
type Watcher struct {
	clock            *hlc.Clock
	rangeFeedFactory *rangefeed.Factory
	stopper          *stop.Stopper
	decoder          *decoder
	bufferMemLimit   int64

	tenantsTableID uint32 // overriden for tests
	knobs          tenantcapabilities.TestingKnobs

	authorizer *tenantcapabilitiesauthorizer.Authorizer
}

var _ tenantcapabilities.CapabilitiesWatcher = &Watcher{}

// New constructs a new Watcher.
func New(
	clock *hlc.Clock,
	rangeFeedFactory *rangefeed.Factory,
	tenantsTableID uint32,
	stopper *stop.Stopper,
	bufferMemLimit int64,
	knobs *tenantcapabilities.TestingKnobs,
) *Watcher {
	if knobs == nil {
		knobs = &tenantcapabilities.TestingKnobs{}
	}
	return &Watcher{
		clock:            clock,
		rangeFeedFactory: rangeFeedFactory,
		stopper:          stopper,
		decoder:          newDecoder(),
		tenantsTableID:   tenantsTableID,
		bufferMemLimit:   bufferMemLimit,
		knobs:            *knobs,
		authorizer:       tenantcapabilitiesauthorizer.New(),
	}
}

// HasCapabilityForBatch implements the tenantcapabilities.Watcher interface.
func (w *Watcher) HasCapabilityForBatch(
	ctx context.Context, tenID roachpb.TenantID, ba *roachpb.BatchRequest,
) bool {
	return w.authorizer.HasCapabilityForBatch(ctx, tenID, ba)
}

// capabilityEntrySize is an estimate for a (tenantID, capability) pair that the
// rangefeed buffer tracks. This is extremely conservative for now, given we
// don't have too many capabilities in the system. We should re-evaluate this
// constant as that changes.
const capabilityEntrySize = 50 // bytes

// Start implements the tenantcapabilities.CapabilitiesWatcher interface.
// Start asycnhronously establishes a rangefeed over the global tenant
// capability state.
func (w *Watcher) Start(ctx context.Context) error {
	tenantsTableStart := keys.SystemSQLCodec.IndexPrefix(
		w.tenantsTableID, keys.TenantsTablePrimaryKeyIndexID,
	)
	tenantsTableSpan := roachpb.Span{
		Key:    tenantsTableStart,
		EndKey: tenantsTableStart.PrefixEnd(),
	}

	rfc := rangefeedcache.NewWatcher(
		"tenant-capability-watcher",
		w.clock,
		w.rangeFeedFactory,
		int(w.bufferMemLimit/capabilityEntrySize), /* bufferSize */
		[]roachpb.Span{tenantsTableSpan},
		true, /* withPrevValue */
		w.decoder.translateEvent,
		w.handleUpdate,
		w.knobs.WatcherRangeFeedKnobs.(*rangefeedcache.TestingKnobs),
	)
	return rangefeedcache.Start(ctx, w.stopper, rfc, nil /* onError */)
}

func (w *Watcher) handleUpdate(ctx context.Context, u rangefeedcache.Update) {
	if u.Type == rangefeedcache.CompleteUpdate {
		// A CompleteUpdate event indicates that the initial table scan is complete.
		// This happens when the rangefeed is first established, or if it's restarted
		// for some reason. Either way, we want to throw away any accumulated state
		// so far, and reconstruct it using the result of the scan.
		log.Info(ctx, "received results of a full table scan for tenant capabilities")
		w.authorizer = tenantcapabilitiesauthorizer.New()
	}

	var updates []tenantcapabilities.Update
	for _, ev := range u.Events {
		update := ev.(*bufferEvent).update
		updates = append(updates, update)
	}
	err := w.authorizer.Apply(updates)
	if err != nil {
		log.Fatalf(ctx, "fatal error applying update: %v", err)
	}

	if fn := w.knobs.WatcherUpdatesInterceptor; fn != nil {
		fn(u.Type, updates)
	}
}

// testFlushCapabilitiesState flushes the underlying global tenant capability
// state for testing purposes.
func (w *Watcher) testingFlushCapabilitiesState() (entries []tenantcapabilities.Entry) {
	return w.authorizer.TestingFlushCapabilitiesState()
}

type bufferEvent struct {
	update tenantcapabilities.Update
	ts     hlc.Timestamp
}

func (e *bufferEvent) Timestamp() hlc.Timestamp {
	return e.ts
}

var _ rangefeedbuffer.Event = &bufferEvent{}
