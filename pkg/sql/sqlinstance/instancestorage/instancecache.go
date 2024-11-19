// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package instancestorage

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/regionliveness"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc/keyside"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
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
	ctx context.Context, rowCodec rowCodec, clock *hlc.Clock, f *rangefeed.Factory, storage *Storage,
) (resultFeed instanceCache, err error) {

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
	// Instead of relying on the change feed to do the initial scan, which would
	// be across all regions, we are going to do it ourselves in a region aware
	// manner. Any regions labeled as unavailable will be skipped in the process.
	initialScan := func() (hlc.Timestamp, error) {
		ts := hlc.Timestamp{}
		livenessProber := regionliveness.NewLivenessProber(storage.db, storage.codec, nil, storage.settings)
		return ts, storage.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			// Determine regions known by the system database and figure out if
			// any of them are unavailable.
			regions, err := storage.readRegionsFromSystemDatabase(ctx, txn)
			if err != nil {
				return err
			}
			deadRegions, err := livenessProber.QueryUnavailablePhysicalRegions(ctx, txn, true /*filterAvailable*/)
			if err != nil {
				return err
			}
			for _, region := range regions {
				// Region is toast, no need for an initial scan.
				if deadRegions.ContainsPhysicalRepresentation(string(region)) {
					continue
				}
				instanceKey := rowCodec.makeIndexPrefix()
				instanceKeyWithRegionBytes, err := keyside.Encode(instanceKey, tree.NewDBytes(tree.DBytes(region)), encoding.Ascending)
				if err != nil {
					return err
				}
				instanceKeyWithRegion := roachpb.Key(instanceKeyWithRegionBytes)
				var rows []kv.KeyValue
				scanFunc := func(ctx context.Context) error {
					var err error
					rows, err = txn.Scan(ctx, instanceKeyWithRegion, instanceKeyWithRegion.PrefixEnd(), 0)
					return err
				}
				if hasTimeout, timeout := livenessProber.GetProbeTimeout(); hasTimeout {
					err = timeutil.RunWithTimeout(ctx, "get-instance-cache-scan", timeout, scanFunc)
				} else {
					err = scanFunc(ctx)
				}
				if err != nil {
					if regionliveness.IsQueryTimeoutErr(err) {
						// Probe and mark the region potentially.
						probeErr := livenessProber.ProbeLivenessWithPhysicalRegion(ctx, region)
						if probeErr != nil {
							err = errors.WithSecondaryError(err, probeErr)
							return err
						}
						return errors.Wrapf(err, "get-instance-rows timed out reading from a region")
					}
					return err
				}
				for _, row := range rows {
					keyVal := kvpb.RangeFeedValue{
						Key:   row.Key,
						Value: *row.Value,
					}
					updateCacheFn(ctx, &keyVal)
				}
			}
			ts, err = txn.CommitTimestamp()
			return err
		})
	}
	instancesTablePrefix := rowCodec.makeIndexPrefix()
	instancesTableSpan := roachpb.Span{
		Key:    instancesTablePrefix,
		EndKey: instancesTablePrefix.PrefixEnd(),
	}
	initialTS, err := initialScan()
	if err != nil {
		return nil, err
	}

	feed.feed, err = f.RangeFeed(ctx,
		"sql_instances",
		[]roachpb.Span{instancesTableSpan},
		// Start collecting updates to the table after our initial scan.
		initialTS,
		updateCacheFn,
		rangefeed.WithSystemTablePriority(),
	)
	return feed, err
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
		cache instanceCache
	}
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
