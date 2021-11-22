// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package svstorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// Event corresponds to a write to the singleversion state table.
type Event struct {
	Row
	IsDeletion bool
	Timestamp  hlc.Timestamp
}

func runRangefeed(
	ctx context.Context,
	rff *rangefeed.Factory,
	tc tableCodec,
	a Action,
	startTS hlc.Timestamp,
	callback func(event Event),
) (err error) {

	k, err := tc.encodeActionPrefix(a)
	if err != nil {
		return err
	}
	sp := roachpb.Span{Key: k, EndKey: k.PrefixEnd()}
	errCh := make(chan error, 1)
	reportErr := func(err error) {
		select {
		case errCh <- err:
		default:
		}
	}
	rf, err := rff.RangeFeed(ctx, "singleversion", []roachpb.Span{sp}, startTS, func(
		ctx context.Context, value *roachpb.RangeFeedValue,
	) {
		var r Row
		if err := tc.decode(value.Key, &r); err != nil {
			reportErr(err)
			return
		}
		callback(Event{
			Row:        r,
			IsDeletion: !value.Value.IsPresent(),
			Timestamp:  value.Value.Timestamp,
		})
	})
	if err != nil {
		return err
	}
	defer rf.Close()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		return err
	}
}
