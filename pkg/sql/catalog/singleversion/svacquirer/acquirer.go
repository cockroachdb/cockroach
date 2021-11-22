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
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/singleversion"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/singleversion/svstorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/logtags"
)

// TODO(ajwerner): Deal with draining.
// TODO(ajwerner): Deal with session expiration.

// Acquirer is used to acquire singleversion leases.
//
// It's a node-level construct which delegates to sqlliveness for session
// heartbeating. Leases are reference counted.
type Acquirer struct {
	ambientCtx log.AmbientContext
	db         *kv.DB
	s          svstorage.Storage
	stopper    *stop.Stopper
	instance   sqlliveness.Instance
	rf         *rangefeed.Factory
	warnEvery  log.EveryN

	mu struct {
		syncutil.Mutex
		leases map[descpb.ID]*lease
	}
}

// NewAcquirer is used to construct a new Acquirer.
func NewAcquirer(
	ac log.AmbientContext,
	stopper *stop.Stopper,
	kvDB *kv.DB,
	rf *rangefeed.Factory,
	s svstorage.Storage,
	instance sqlliveness.Instance,
) *Acquirer {
	a := &Acquirer{
		ambientCtx: ac,
		db:         kvDB,
		s:          s,
		stopper:    stopper,
		instance:   instance,
		rf:         rf,
		warnEvery:  log.Every(time.Second),
	}
	a.mu.leases = make(map[descpb.ID]*lease)
	return a
}

func (w waiter) wait(ctx context.Context, stopper *stop.Stopper) (err error) {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-stopper.ShouldQuiesce():
		return stop.ErrUnavailable
	case <-w.done:
		return w.err
	}
}

// Acquire acquires a new lease.
func (a *Acquirer) Acquire(ctx context.Context, id descpb.ID) (singleversion.Lease, error) {
	// We want to get a live Lease and increment the refCount.
	// If there is not a live Lease, then we need to create one.
	for {

		// This dance is mostly about making sure that when a lease is actively
		// being released, that we wait for that to finish and then coordinate
		// the re-acquisition.
		l, err := a.getOrCreateLease(ctx, id)
		if err != nil {
			return nil, err
		}
		retry, err := l.waitForValid(ctx)
		switch {
		case retry:
			continue
		case err != nil:
			return nil, err
		default:
			return l, nil
		}
	}
}

// getOrCreateLease will either retrieve an existing lease and increment its
// reference count, or it will start the process of acquiring a new lease, also
// incrementing its reference count. The returned lease needs to be validated
// with a call to waitForValid.
func (a *Acquirer) getOrCreateLease(ctx context.Context, id descpb.ID) (*lease, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	l, ok := a.mu.leases[id]
	if ok {
		l.incRefCount()
		return l, nil
	}
	l = &lease{id: id}
	l.a.Acquirer = a
	l.acquisition.done = make(chan struct{})
	l.incRefCount()
	a.mu.leases[id] = l
	if err := a.stopper.RunAsyncTask(ctx, "acquire-Lease", func(
		ctx context.Context,
	) {
		if err := a.doAcquire(ctx, l); err != nil {
			a.mu.Lock()
			defer a.mu.Unlock()
			// We know that the only possible value is the one that we have here.
			delete(a.mu.leases, id)
			l.acquisition.err = err
		}
		close(l.acquisition.done)
	}); err != nil {
		return nil, err
	}
	return l, nil
}

func (a *Acquirer) doAcquire(ctx context.Context, l *lease) error {
	ctx, cancel := a.stopper.WithCancelOnQuiesce(a.newCtx(ctx))
	defer cancel()

	session, err := a.instance.Session(ctx)
	if err != nil {
		return err
	}
	var commitTimestamp func() hlc.Timestamp
	if err := a.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		commitTimestamp = txn.CommitTimestamp
		// TODO(ajwerner): This is going to return a supremely opaque error if
		// the session expires. I suppose that internally we'd like to get a new
		// session, assuming this is a node with an instance which will do that.
		if err := txn.UpdateDeadline(ctx, session.Expiration()); err != nil {
			return err
		}
		if err := a.s.Scan(ctx, txn, []svstorage.Row{
			{
				Action:     svstorage.Lock,
				Descriptor: l.id,
			},
		}, func(row svstorage.Row) {}); err != nil {
			return err
		}
		return a.s.Put(ctx, txn, svstorage.Row{
			Action:     svstorage.Lease,
			Descriptor: l.id,
			Session:    session.ID(),
		})
	}); err != nil {
		return err
	}

	l.session = session
	l.startTime = commitTimestamp()
	return nil
}

// TODO(ajwerner): Decide if for release we really do want to stop with the stopper.
func (a *Acquirer) newCtx(ctx context.Context) context.Context {
	// Note that we use a new `context` here to avoid a situation where a cancellation
	// of the first context cancels other callers to the `acquireNodeLease()` method,
	// because of its use of `singleflight.Group`. See issue #41780 for how this has
	// happened.
	baseCtx := a.ambientCtx.AnnotateCtx(context.Background())
	// AddTags and not WithTags, so that we combine the tags with those
	// filled by AnnotateCtx.
	return logtags.AddTags(baseCtx, logtags.FromContext(ctx))
}

func (a *Acquirer) release(ctx context.Context, maxUsedTimestamp hlc.Timestamp, l *lease) {
	a.mu.Lock()
	defer a.mu.Unlock()
	l.a.refCount--
	if !maxUsedTimestamp.IsEmpty() {
		l.a.highestUsedTimestamp.Forward(maxUsedTimestamp)
	}
	if l.a.refCount > 0 {
		return
	}
	l.release.done = make(chan struct{}) // indicates we're going to release this

	if err := a.stopper.RunAsyncTask(ctx, "acquire-Lease", func(
		ctx context.Context,
	) {
		a.doRelease(ctx, l)
	}); err != nil {
		l.release.err = err
	}
}

func (a *Acquirer) doRelease(ctx context.Context, l *lease) {

	// Note that we crucially use a new context which is not tied to any client
	// goroutine.
	ctx = a.newCtx(ctx)

	defer close(l.release.done)
	defer a.removeLease(l)

	// Make sure there is not an acquisition in flight. If there is, we need
	// to wait for it.
	if err := l.waitForAcquisition(ctx); err != nil {
		// The assumption here is that if waitForAcquisition returned an error,
		// then the lease was not acquired, so there's no need to release.
		return
	}

	// Wait for the clock, and thus the transaction which has a timestamp pulled
	// from the clock is after the latest timestamp used by the lease. No
	// transaction which modifies the descriptor will commit with a timestamp
	// below this timestamp.
	waitForClockAfter(a.db.Clock(), l.a.highestUsedTimestamp)

	var err error
	for r := retry.Start(retry.Options{
		Closer: a.stopper.ShouldQuiesce(),
	}); r.Next(); {
		if err = a.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			return a.s.Delete(ctx, txn, svstorage.Row{
				Action:     svstorage.Lease,
				Descriptor: l.id,
				Session:    l.session.ID(),
			})
		}); err != nil {
			// TODO(ajwerner): What should we do here? It could be a transient
			// error, in which case we definitely want to keep trying. To return
			// when the row hasn't been deleted would be problematic. Schema
			// changes would not be able to make progress until the node disappears.
			log.Infof(ctx, "failed to delete rows: %v", err)
		} else {
			break
		}
	}
	// TODO(ajwerner): Think about the early return implications
}

// TODO(ajwerner): Adopt a time source or something for this sleeping to
// make it more testable.
func waitForClockAfter(clock *hlc.Clock, timestamp hlc.Timestamp) {
	for clock.Now().Less(timestamp) {
		time.Sleep(timestamp.GoTime().Sub(clock.Now().GoTime()))
	}
}

func (a *Acquirer) removeLease(l *lease) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.mu.leases, l.id)
}
