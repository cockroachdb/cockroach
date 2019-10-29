// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package pttracker

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts"
	"github.com/cockroachdb/cockroach/pkg/storage/protectedts/ptpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/pkg/errors"
)

// TODO(ajwerner): consider hookup up periodic fetching to gossip so that just
// one node in the cluster performs the periodic fetch rather than every node.
// It's not that each node fetching is expensive, I would be somewhat surprised
// if the single-row read is more expensive than the gossip but the single row
// read does risk contending with attempts to write new protectedts records.
// Read locks will help with this contention problem.

// Tracker tracks and caches the state of the protectedts subsystem.
type Tracker struct {
	db       *client.DB
	storage  protectedts.Storage
	stopper  *stop.Stopper
	settings *cluster.Settings

	fetchChan chan struct{}
	mu        struct {
		syncutil.RWMutex

		started bool

		// updated in fetch()
		lastUpdate hlc.Timestamp
		state      ptpb.State

		// TODO(ajwerner): optimize a lookup structure for the state.
		// An interval tree would do but is probably more complicated than it's
		// worth.
	}
}

// ProtectedBy implements protectedts.Tracker.
func (t *Tracker) ProtectedBy(
	ctx context.Context, s roachpb.Span, f func(*ptpb.Record),
) (asOf hlc.Timestamp) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.protectedByRLocked(ctx, s, f)
}

// New returns a new Tracker. It needs to be started separately.
func New(settings *cluster.Settings, db *client.DB, storage protectedts.Storage) *Tracker {
	// TODO(ajwerner): sanity check that the arguments are non-nil.
	return &Tracker{
		db:        db,
		storage:   storage,
		settings:  settings,
		fetchChan: make(chan struct{}),
	}
}

// Start starts the Tracker.
func (t *Tracker) Start(ctx context.Context, stopper *stop.Stopper) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.mu.started {
		return fmt.Errorf("cannot Start a Tracker more than once")
	}
	t.stopper = stopper
	t.mu.started = true
	return t.stopper.RunAsyncTask(ctx, "protectedts-tracker", t.run)
}

// run will fetch the state of the protectedts subsystem periodically based
// on the period defined by the cluster setting.
func (t *Tracker) run(ctx context.Context) {
	settingChanged := make(chan struct{}, 1)
	protectedts.PollInterval.SetOnChange(&t.settings.SV, func() {
		select {
		case settingChanged <- struct{}{}:
		default:
		}
	})
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	timer := timeutil.NewTimer()
	timer.Reset(0) // Read immediately upon startup
	var lastReset time.Time
	for {
		select {
		case <-timer.C:
			// Let's not reset the timer until we get our response.
			timer.Read = true
			if err := t.stopper.RunAsyncTask(ctx, "protectedts-tracker-fetch", func(ctx context.Context) {
				t.fetch(ctx)
			}); err != nil {
				return
			}
		case <-settingChanged:
			if timer.Read { // we're currently fetching
				continue
			}
			timer.Reset(protectedts.PollInterval.Get(&t.settings.SV) - timeutil.Since(lastReset))
			lastReset = timeutil.Now()
		case <-t.fetchChan:
			timer.Reset(protectedts.PollInterval.Get(&t.settings.SV))
			lastReset = timeutil.Now()
		case <-t.stopper.ShouldQuiesce():
			return
		}
	}
}

func (t *Tracker) fetch(ctx context.Context) {
	// TODO(ajwerner): consider retrying at a higher frequency in the case
	// of failure.
	defer func() {
		select {
		case t.fetchChan <- struct{}{}:
		case <-t.stopper.ShouldQuiesce():
		}
	}()
	prev := t.getMetadata()
	var versionChanged bool
	var state ptpb.State
	var ts hlc.Timestamp
	err := t.db.Txn(ctx, func(ctx context.Context, txn *client.Txn) (err error) {
		defer func() {
			if err == nil {
				ts = txn.OrigTimestamp()
			}
		}()
		md, err := t.storage.GetMetadata(ctx, txn)
		if err != nil {
			return errors.Wrap(err, "failed to fetch protectedts metadata")
		}
		if versionChanged = md.Version != prev.Version; !versionChanged {
			return nil
		}
		if state, err = t.storage.GetState(ctx, txn); err != nil {
			return errors.Wrap(err, "failed to fetch protectedts state")
		}
		return nil
	})
	if err != nil {
		if ctx.Err() == nil {
			log.Errorf(ctx, "failed to update protectedts state: %v", err)
		}
		return
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	t.mu.lastUpdate = ts
	if versionChanged {
		t.mu.state = state
	}
}

func (t *Tracker) getMetadata() ptpb.Metadata {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.mu.state.Metadata
}

func (t *Tracker) protectedByRLocked(
	ctx context.Context, s roachpb.Span, f func(*ptpb.Record),
) (asOf hlc.Timestamp) {
	for i := range t.mu.state.Records {
		r := &t.mu.state.Records[i]
		for _, span := range r.Spans {
			if s.Overlaps(span) {
				f(r)
			}
		}
	}
	return t.mu.lastUpdate
}
