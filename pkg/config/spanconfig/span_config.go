// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package spanconfig

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

type Listener struct {
	DB *kv.DB
}

func (s *Listener) RegisterSpanConfigChannel(ctx context.Context, stopper *stop.Stopper) (<-chan struct{}, error) {
	updateCh := make(chan struct{})
	spanConfigTableStart := keys.SystemSQLCodec.TablePrefix(keys.SpanConfigurationsTableID)
	spanConfigTableSpan := roachpb.Span{
		Key:    spanConfigTableStart,
		EndKey: spanConfigTableStart.PrefixEnd(),
	}

	handleEvent := func(
		ctx context.Context, ev *roachpb.RangeFeedValue,
	) {
		// XXX: Parse out value into span config entry. How does it work with a
		// table with >2 columns?
		select {
		case <-ctx.Done():
		case updateCh <- struct{}{}:
		}
	}

	rangeFeedFactory, err := rangefeed.NewFactory(stopper, s.DB, nil)
	if err != nil {
		return nil, err
	}
	_, _ = rangeFeedFactory.RangeFeed(
		ctx, "span-cfg-rangefeed", spanConfigTableSpan, hlc.Timestamp{}, handleEvent,
	)
	return updateCh, nil
}
