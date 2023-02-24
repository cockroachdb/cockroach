package instancestorage

// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// instanceFeed represents a cache over the contents of sql_instances table.
type instanceFeed interface {
	// getInstance returns a single instance by ID. If the instance is not
	// found, ok is set to false.
	getInstance(instanceID base.SQLInstanceID) (i instancerow, ok bool)

	// listInstances returns a list containing all of the cached instance rows.
	// listInstances returns all cached rows, including rows that are unclaimed
	// or owned by an inactive session.
	listInstances() []instancerow

	// Close stops updates to the cache. getInstance and listInstances continue
	// to work after Close, but will return stale results.
	Close()
}

// emptyInstanceFeed is used during initialization. It implements an instance
// feed with no instances.
type emptyInstanceFeed struct {
}

// Close implements instanceFeed
func (*emptyInstanceFeed) Close() {
	// no-op
}

// getInstance implements instanceFeed
func (*emptyInstanceFeed) getInstance(instanceID base.SQLInstanceID) (instancerow, bool) {
	return instancerow{}, false
}

// listInstances implements instanceFeed
func (*emptyInstanceFeed) listInstances() []instancerow {
	return nil
}

var _ instanceFeed = &emptyInstanceFeed{}

// singletonInstanceFeed is used during system start up. It only contains
// server's own sql instance.
type singletonInstanceFeed struct {
	instance instancerow
}

var _ instanceFeed = &singletonInstanceFeed{}

func (s *singletonInstanceFeed) getInstance(instanceID base.SQLInstanceID) (instancerow, bool) {
	if instanceID == s.instance.instanceID {
		return s.instance, true
	}
	return instancerow{}, false
}

func (s *singletonInstanceFeed) listInstances() []instancerow {
	return []instancerow{s.instance}
}

func (s *singletonInstanceFeed) Close() {}

type rangeFeed struct {
	feed *rangefeed.RangeFeed
	mu   struct {
		syncutil.Mutex
		instances map[base.SQLInstanceID]instancerow
	}
}

var _ instanceFeed = &rangeFeed{}

// newRangeFeed constructs an instanceFeed backed by a range feed over the
// sql_instances table. newRangeFeed will block until the initial scan is
// complete.
func newRangeFeed(
	ctx context.Context, rowCodec rowCodec, clock *hlc.Clock, f *rangefeed.Factory,
) (resultFeed instanceFeed, err error) {
	done := make(chan error, 1)

	feed := &rangeFeed{}
	feed.mu.instances = map[base.SQLInstanceID]instancerow{}

	updateCacheFn := func(
		ctx context.Context, keyVal *kvpb.RangeFeedValue,
	) {
		instance, err := rowCodec.decodeRow(keyVal.Key, &keyVal.Value)
		if err != nil {
			log.Ops.Warningf(ctx, "failed to decode settings row %v: %v", keyVal.Key, err)
			return
		}
		feed.updateInstanceMap(instance, !keyVal.Value.IsPresent())
	}
	initialScanDoneFn := func(_ context.Context) {
		select {
		case done <- nil:
			// success reported to the caller
		default:
			// something is already in the done channel
		}
	}
	initialScanErrFn := func(_ context.Context, err error) (shouldFail bool) {
		if grpcutil.IsAuthError(err) ||
			// This is a hack around the fact that we do not get properly structured
			// errors out of gRPC. See #56208.
			strings.Contains(err.Error(), "rpc error: code = Unauthenticated") {
			shouldFail = true
			select {
			case done <- err:
				// err reported to the caller
			default:
				// something is already in the done channel
			}
		}
		return shouldFail
	}

	instancesTablePrefix := rowCodec.makeIndexPrefix()
	instancesTableSpan := roachpb.Span{
		Key:    instancesTablePrefix,
		EndKey: instancesTablePrefix.PrefixEnd(),
	}
	feed.feed, err = f.RangeFeed(ctx,
		"sql_instances",
		[]roachpb.Span{instancesTableSpan},
		clock.Now(),
		updateCacheFn,
		rangefeed.WithSystemTablePriority(),
		rangefeed.WithInitialScan(initialScanDoneFn),
		rangefeed.WithOnInitialScanError(initialScanErrFn),
		rangefeed.WithRowTimestampInInitialScan(true),
	)
	if err != nil {
		return nil, err
	}
	defer func() {
		// Ensure the feed is cleaned up if there is an error
		if resultFeed == nil {
			feed.Close()
		}
	}()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-done:
		if err != nil {
			return nil, err
		}
		return feed, nil
	}
}

func (s *rangeFeed) getInstance(instanceID base.SQLInstanceID) (instancerow, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.mu.instances[instanceID]
	return row, ok
}

func (s *rangeFeed) listInstances() []instancerow {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]instancerow, 0, len(s.mu.instances))
	for _, row := range s.mu.instances {
		result = append(result, row)
	}
	return result
}

func (r *rangeFeed) updateInstanceMap(instance instancerow, deletionEvent bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if deletionEvent {
		delete(r.mu.instances, instance.instanceID)
		return
	}
	r.mu.instances[instance.instanceID] = instance
}

func (s *rangeFeed) Close() {
	s.feed.Close()
}
