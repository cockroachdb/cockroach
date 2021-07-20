// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package ptcache

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil/singleflight"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// Cache implements protectedts.Cache.
type Cache struct {
	db       *kv.DB
	storage  protectedts.Storage
	stopper  *stop.Stopper
	settings *cluster.Settings
	sf       singleflight.Group
	mu       struct {
		syncutil.RWMutex

		started bool

		// Updated in doUpdate().
		lastUpdate hlc.Timestamp
		state      ptpb.State

		// Updated in doUpdate but mutable. The records in the map are not mutated
		// and should not be by any client.
		recordsByID map[uuid.UUID]*ptpb.Record

		// TODO(ajwerner): add a more efficient lookup structure such as an
		// interval.Tree for Iterate.
	}
}

// Config configures a Cache.
type Config struct {
	DB       *kv.DB
	Storage  protectedts.Storage
	Settings *cluster.Settings
}

// New returns a new cache.
func New(config Config) *Cache {
	c := &Cache{
		db:       config.DB,
		storage:  config.Storage,
		settings: config.Settings,
	}
	c.mu.recordsByID = make(map[uuid.UUID]*ptpb.Record)
	return c
}

var _ protectedts.Cache = (*Cache)(nil)

// Iterate is part of the protectedts.Cache interface.
func (c *Cache) Iterate(
	_ context.Context, from, to roachpb.Key, it protectedts.Iterator,
) (asOf hlc.Timestamp) {
	c.mu.RLock()
	state, lastUpdate := c.mu.state, c.mu.lastUpdate
	c.mu.RUnlock()

	sp := roachpb.Span{
		Key:    from,
		EndKey: to,
	}
	for i := range state.Records {
		r := &state.Records[i]
		if !overlaps(r, sp) {
			continue
		}
		if wantMore := it(r); !wantMore {
			break
		}
	}
	return lastUpdate
}

// QueryRecord is part of the protectedts.Cache interface.
func (c *Cache) QueryRecord(_ context.Context, id uuid.UUID) (exists bool, asOf hlc.Timestamp) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	_, exists = c.mu.recordsByID[id]
	return exists, c.mu.lastUpdate
}

// refreshKey is used for the singleflight.
const refreshKey = ""

// Refresh is part of the protectedts.Cache interface.
func (c *Cache) Refresh(ctx context.Context, asOf hlc.Timestamp) error {
	for !c.upToDate(asOf) {
		ch, _ := c.sf.DoChan(refreshKey, c.doSingleFlightUpdate)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case res := <-ch:
			if res.Err != nil {
				return res.Err
			}
		}
	}
	return nil
}

// Start starts the periodic fetching of the Cache. A Cache must not be used
// until after it has been started. An error will be returned if it has
// already been started.
func (c *Cache) Start(ctx context.Context, stopper *stop.Stopper) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.mu.started {
		return errors.New("cannot start a Cache more than once")
	}
	c.mu.started = true
	c.stopper = stopper
	return c.stopper.RunAsyncTask(ctx, "periodically-refresh-protectedts-cache",
		c.periodicallyRefreshProtectedtsCache)
}

func (c *Cache) periodicallyRefreshProtectedtsCache(ctx context.Context) {
	settingChanged := make(chan struct{}, 1)
	protectedts.PollInterval.SetOnChange(&c.settings.SV, func(ctx context.Context) {
		select {
		case settingChanged <- struct{}{}:
		default:
		}
	})
	timer := timeutil.NewTimer()
	defer timer.Stop()
	timer.Reset(0) // Read immediately upon startup
	var lastReset time.Time
	var doneCh <-chan singleflight.Result
	// TODO(ajwerner): consider resetting the timer when the state is updated
	// due to a call to Refresh.
	for {
		select {
		case <-timer.C:
			// Let's not reset the timer until we get our response.
			timer.Read = true
			doneCh, _ = c.sf.DoChan(refreshKey, c.doSingleFlightUpdate)
		case <-settingChanged:
			if timer.Read { // we're currently fetching
				continue
			}
			interval := protectedts.PollInterval.Get(&c.settings.SV)
			// NB: It's okay if nextUpdate is a negative duration; timer.Reset will
			// treat a negative duration as zero and send a notification immediately.
			nextUpdate := interval - timeutil.Since(lastReset)
			timer.Reset(nextUpdate)
			lastReset = timeutil.Now()
		case res := <-doneCh:
			if res.Err != nil {
				if ctx.Err() == nil {
					log.Errorf(ctx, "failed to refresh protected timestamps: %v", res.Err)
				}
			}
			timer.Reset(protectedts.PollInterval.Get(&c.settings.SV))
			lastReset = timeutil.Now()
		case <-c.stopper.ShouldQuiesce():
			return
		}
	}
}

func (c *Cache) doSingleFlightUpdate() (interface{}, error) {
	// TODO(ajwerner): add log tags to the context.
	ctx, cancel := c.stopper.WithCancelOnQuiesce(context.Background())
	defer cancel()
	return nil, c.stopper.RunTaskWithErr(ctx,
		"refresh-protectedts-cache", c.doUpdate)
}

func (c *Cache) getMetadata() ptpb.Metadata {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.mu.state.Metadata
}

func (c *Cache) doUpdate(ctx context.Context) error {
	// NB: doUpdate is only ever called underneath c.singleFlight and thus is
	// never called concurrently. Due to the lack of concurrency there are no
	// concerns about races as this is the only method which writes to the Cache's
	// state.
	prev := c.getMetadata()
	var (
		versionChanged bool
		state          ptpb.State
		ts             hlc.Timestamp
	)
	err := c.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		// NB: because this is a read-only transaction, the commit will be a no-op;
		// returning nil here means the transaction will commit and will never need
		// to change its read timestamp.
		defer func() {
			if err == nil {
				ts = txn.ReadTimestamp()
			}
		}()
		md, err := c.storage.GetMetadata(ctx, txn)
		if err != nil {
			return errors.Wrap(err, "failed to fetch protectedts metadata")
		}
		if versionChanged = md.Version != prev.Version; !versionChanged {
			return nil
		}
		if state, err = c.storage.GetState(ctx, txn); err != nil {
			return errors.Wrap(err, "failed to fetch protectedts state")
		}
		return nil
	})
	if err != nil {
		return err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.mu.lastUpdate = ts
	if versionChanged {
		c.mu.state = state
		for id := range c.mu.recordsByID {
			delete(c.mu.recordsByID, id)
		}
		for i := range state.Records {
			r := &state.Records[i]
			c.mu.recordsByID[r.ID] = r
		}
	}
	return nil
}

// upToDate returns true if the lastUpdate for the cache is at least asOf.
func (c *Cache) upToDate(asOf hlc.Timestamp) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return asOf.LessEq(c.mu.lastUpdate)
}

func overlaps(r *ptpb.Record, sp roachpb.Span) bool {
	for i := range r.Spans {
		if r.Spans[i].Overlaps(sp) {
			return true
		}
	}
	return false
}
