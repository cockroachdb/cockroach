// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package keyvissubscriber

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/keyvisualizer/keyvispb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedbuffer"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed/rangefeedcache"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/valueside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// Subscriber manages the rangefeed on the SpanStatsTenantBoundariesTable.
type Subscriber struct {
	clock *hlc.Clock
	rff   *rangefeed.Factory
}

// New constructs a new keyvissubscriber.Subscriber.
func New(clock *hlc.Clock, rff *rangefeed.Factory) *Subscriber {
	return &Subscriber{
		clock: clock,
		rff:   rff,
	}
}

// Start starts the rangefeed on the SpanStatsTenantBoundariesTable.
// When a new event is received, the event is decoded and delegated to handleBoundaryUpdate.
func (s *Subscriber) Start(
	ctx context.Context,
	stopper *stop.Stopper,
	sysTableResolver catalog.SystemTableIDResolver,
	handleBoundaryUpdate func(ten roachpb.TenantID, boundaries []*roachpb.Span) error,
) error {

	tableID, err := sysTableResolver.LookupSystemTableID(ctx, systemschema.SpanStatsTenantBoundariesTable.GetName())

	if err != nil {
		return err
	}

	tablePrefix := keys.SystemSQLCodec.TablePrefix(uint32(tableID))
	tableSpan := roachpb.Span{
		Key:    tablePrefix,
		EndKey: tablePrefix.PrefixEnd(),
	}

	columns := systemschema.SpanStatsTenantBoundariesTable.PublicColumns()
	decoder := valueside.MakeDecoder(columns)
	alloc := tree.DatumAlloc{}

	translateEvent := func(
		ctx context.Context,
		kv *roachpb.RangeFeedValue,
	) rangefeedbuffer.Event {

		bytes, err := kv.Value.GetTuple()
		if err != nil {
			log.Warningf(ctx, err.Error())
			return nil
		}

		// try to decode the row
		datums, err := decoder.Decode(&alloc, bytes)
		if err != nil {
			log.Warningf(ctx, err.Error())
			return nil
		}

		// decode the stashed boundaries from the binary encoded proto
		decoded := keyvispb.SaveBoundariesRequest{}
		boundariesEncoded := []byte(tree.MustBeDBytes(datums[1]))

		if err := protoutil.Unmarshal(boundariesEncoded, &decoded); err != nil {
			log.Warningf(ctx, err.Error())
			return nil
		}

		err = handleBoundaryUpdate(*decoded.Tenant, decoded.Boundaries)
		if err != nil {
			return nil
		}
		return nil
	}

	w := rangefeedcache.NewWatcher(
		"tenant-boundaries-watcher",
		s.clock,
		s.rff,
		10,
		[]roachpb.Span{tableSpan},
		false,
		translateEvent,
		func(ctx context.Context, update rangefeedcache.Update) {
			// noop
		},
		nil,
	)

	// Kick off the rangefeedcache which will retry until the stopper stops.
	if err := rangefeedcache.Start(ctx, stopper, w, func(err error) {
		log.Errorf(ctx, "span stats collector rangefeed error: %s", err.Error())
	}); err != nil {
		return err // we're shutting down
	}

	return nil
}
