// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package instancestorage

import (
	"context"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/grpcutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// instanceCache represents a cache over the contents of sql_instances table.
type instanceCache interface {
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

// emptyInstanceCache is used during initialization. It implements an instance
// feed with no instances.
type emptyInstanceCache struct {
}

// Close implements instanceCache
func (*emptyInstanceCache) Close() {
	// no-op
}

// getInstance implements instanceCache
func (*emptyInstanceCache) getInstance(instanceID base.SQLInstanceID) (instancerow, bool) {
	return instancerow{}, false
}

// listInstances implements instanceCache
func (*emptyInstanceCache) listInstances() []instancerow {
	return nil
}

var _ instanceCache = &emptyInstanceCache{}

// singletonInstanceFeed is used during system start up. It only contains
// server's own sql instance.
type singletonInstanceFeed struct {
	instance instancerow
}

var _ instanceCache = &singletonInstanceFeed{}

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

type rangeFeedCache struct {
	feed *rangefeed.RangeFeed
	mu   struct {
		syncutil.Mutex
		instances map[base.SQLInstanceID]instancerow
	}
}

var _ instanceCache = &rangeFeedCache{}

// newRangeFeedCache constructs an instanceCache backed by a range feed over the
// sql_instances table. newRangeFeedCache will block until the initial scan is
// complete.
func newRangeFeedCache(
	ctx context.Context, rowCodec rowCodec, clock *hlc.Clock, f *rangefeed.Factory,
) (resultFeed instanceCache, err error) {
	done := make(chan error, 1)

	feed := &rangeFeedCache{}
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

func (s *rangeFeedCache) getInstance(instanceID base.SQLInstanceID) (instancerow, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	row, ok := s.mu.instances[instanceID]
	return row, ok
}

func (s *rangeFeedCache) listInstances() []instancerow {
	s.mu.Lock()
	defer s.mu.Unlock()
	result := make([]instancerow, 0, len(s.mu.instances))
	for _, row := range s.mu.instances {
		result = append(result, row)
	}
	return result
}

func (r *rangeFeedCache) updateInstanceMap(instance instancerow, deletionEvent bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if deletionEvent {
		delete(r.mu.instances, instance.instanceID)
		return
	}
	r.mu.instances[instance.instanceID] = instance
}

func (s *rangeFeedCache) Close() {
	s.feed.Close()
}

type migrationCache struct {
	mu struct {
		syncutil.Mutex
		versionChanged bool
		cache          instanceCache
	}
}

// newMigrationCache uses the oldCache and newCache functions to construct
// instanceCaches. The cache registers a hook with the setting and switches
// from the old implementation to the new implementation when the version
// changes to V23_1_SystemRbrReadNew.
func newMigrationCache(
	ctx context.Context,
	stopper *stop.Stopper,
	setting *cluster.Settings,
	oldCache, newCache func(ctx context.Context) (instanceCache, error),
) (instanceCache, error) {
	c := &migrationCache{}
	oldReady := make(chan error, 1)
	newReady := make(chan error, 1)

	onVersionChange := func(ctx context.Context, version clusterversion.ClusterVersion) {
		if !version.IsActive(clusterversion.V23_1_SystemRbrReadNew) {
			return
		}

		c.mu.Lock()
		defer c.mu.Unlock()

		if c.mu.versionChanged {
			return
		}
		c.mu.versionChanged = true

		if c.mu.cache != nil {
			log.Ops.Info(ctx, "closing old system.sql_instance cache")
			c.mu.cache.Close()
		}

		// Use the background context because we don't want RPC cancellation to
		// prevent starting the new range feed.
		err := stopper.RunAsyncTask(context.Background(), "start-new-cache-implementation", func(ctx context.Context) {
			log.Ops.Info(ctx, "starting new system.sql_instance cache")

			cache, err := newCache(ctx)
			if err != nil {
				log.Ops.Errorf(ctx, "error starting the new system.sql_instance cache: %s", err)
				newReady <- err
				return
			}

			c.mu.Lock()
			defer c.mu.Unlock()

			log.Ops.Info(ctx, "new system.sql_instance cache is ready")

			if c.mu.cache != nil {
				c.mu.cache.Close()
			}
			c.mu.cache = cache
			newReady <- nil
		})
		if err != nil {
			log.Ops.Errorf(ctx, "unable to start new system.sql_instance cache: %s", err)
		}
	}

	// Register the hook, then run it once in case the version is already
	// active.
	setting.Version.SetOnChange(onVersionChange)
	onVersionChange(ctx, setting.Version.ActiveVersion(ctx))

	err := stopper.RunAsyncTask(ctx, "start-old-cache-implementation", func(ctx context.Context) {
		cache, err := oldCache(ctx)
		if err != nil {
			oldReady <- err
		}

		c.mu.Lock()
		defer c.mu.Unlock()

		if c.mu.versionChanged {
			cache.Close()
			return
		}

		c.mu.cache = cache
		oldReady <- nil
	})
	if err != nil {
		oldReady <- err
	}

	select {
	case err = <-newReady:
	case err = <-oldReady:
	}
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *migrationCache) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.cache.Close()
}

func (c *migrationCache) getInstance(instanceID base.SQLInstanceID) (i instancerow, ok bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.cache.getInstance(instanceID)
}

func (c *migrationCache) listInstances() []instancerow {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mu.cache.listInstances()
}

var _ instanceCache = &migrationCache{}
