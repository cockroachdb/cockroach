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
	"fmt"

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
	handleBoundaryUpdate func(update *keyvispb.UpdateBoundariesRequest),
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

	processBoundaryUpdate := func(updateDatum tree.Datum) {
		// decode the stashed boundaries from the binary encoded proto
		decoded := keyvispb.UpdateBoundariesRequest{}
		updateEncoded := []byte(tree.MustBeDBytes(updateDatum))

		if err := protoutil.Unmarshal(updateEncoded, &decoded); err != nil {
			log.Warningf(ctx, fmt.Sprintf("boundary decoding failed with error %v", err))
			return
		}
		handleBoundaryUpdate(&decoded)
	}

	w := rangefeedcache.NewWatcher(
		"tenant-boundaries-watcher", // name
		s.clock,
		s.rff,
		0,                         // bufferSize
		[]roachpb.Span{tableSpan}, // spans
		false,                     //withPrevValue
		func(
			ctx context.Context,
			kv *roachpb.RangeFeedValue,
		) rangefeedbuffer.Event {

			bytes, err := kv.Value.GetTuple()
			if err != nil {
				log.Warningf(ctx, err.Error())
				return nil
			}

			// try to decode the row
			datums, err := decoder.Decode(&tree.DatumAlloc{}, bytes)
			if err != nil {
				log.Warningf(ctx, err.Error())
				return nil
			}

			// We're not concerned about update events or consisted snapshots.
			// We are only concerned with a single row.
			processBoundaryUpdate(datums[1])
			return nil
		},
		func(ctx context.Context, update rangefeedcache.Update) {
			// onUpdate
		},
		nil,
	)

	// Kick off the rangefeedcache which will retry until the stopper stops.
	if err := rangefeedcache.Start(ctx, stopper, w, func(err error) {
		log.Warningf(ctx, "span stats collector rangefeed error: %v", err)
	}); err != nil {
		return err
	}

	return nil
}
