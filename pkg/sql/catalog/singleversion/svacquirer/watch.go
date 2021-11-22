// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package svacquirer

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/singleversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/singleversion/svstorage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// WatchForNotifications is part of singleversion.Acquirer.
func (a *Acquirer) WatchForNotifications(
	ctx context.Context,
	startTS hlc.Timestamp,
	ids catalog.DescriptorIDSet,
	ch chan<- singleversion.Notification,
) (err error) {
	if log.ExpensiveLogEnabled(ctx, 1) {
		defer func() {
			log.Infof(ctx,
				"exiting notifications rangefeed for %s starting from %v: %v",
				ids, startTS, err,
			)
		}()
		log.Infof(ctx,
			"starting notifications rangefeed for %s starting from %v",
			ids, startTS,
		)
	}
	return a.s.RangeFeed(ctx, a.rf, svstorage.Notify, startTS, func(event svstorage.Event) {
		if !ids.Contains(event.Descriptor) {
			return
		}
		if !event.IsDeletion {
			// TODO(ajwerner): This is a weird case which should never happen.
			if a.warnEvery.ShouldLog() {
				log.Warningf(ctx,
					"unexpected write to singleversion notification table for %d",
					event.Descriptor,
				)
			}
			return
		}
		select {
		case ch <- singleversion.Notification{
			ID:        event.Descriptor,
			Timestamp: event.Timestamp,
		}:
		case <-ctx.Done():
		}
	})
}
