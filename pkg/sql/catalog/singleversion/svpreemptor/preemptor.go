// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package svpreemptor

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/singleversion/svstorage"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlliveness"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// TODO(ajwerner): Metrics.
// TODO(ajwerner): Figure out what to do about cleaning up old leases
// corresponding to expired sessions.
// TODO(ajwerner): Decide if we need to paginate or anything like that.
// TODO(ajwerner): Figure out what to do if there's a boatload of leases to
// wait for. There probably won't be and we can just say there's a limit on
// the number to wait for at a time.

// Preemptor is used to preempt single-version leases on a descriptor.
type Preemptor struct {
	stopper *stop.Stopper
	s       svstorage.Storage
	db      *kv.DB
	rf      *rangefeed.Factory
	slr     sqlliveness.Reader // assumed to be non-blocking
}

// NewPreemptor constructs a new Preemptor.
func NewPreemptor(
	stopper *stop.Stopper,
	db *kv.DB,
	rf *rangefeed.Factory,
	s svstorage.Storage,
	slr sqlliveness.Reader,
) *Preemptor {
	return &Preemptor{
		stopper: stopper,
		s:       s,
		db:      db,
		rf:      rf,
		slr:     slr,
	}
}

// EnsureNoSingleVersionLeases runs a protocol that ensures that once this
// call returns with no error, there are no outstanding leases for any of
// the descriptors in the ID set of concern.
func (p *Preemptor) EnsureNoSingleVersionLeases(
	ctx context.Context, txn *kv.Txn, descriptors catalog.DescriptorIDSet,
) error {

	// The protocol proceeds as follows:
	//
	// 1. Write an intent to a key which prevents new leases from being
	//    established.
	// 2. (using a separate transaction, at the provisional commit timestamp)
	//    check to  see if there are any live. If none, protocol concludes.
	//    a. We can run this phase in parallel with 1, but if 1 gets pushed, we
	//       may need to retry this phase in order to ensure that no leases were
	//       missed before notifying and watching. This parallelism is warranted
	//       considering how rarely we expect there to be locks.
	//    b. If there are no outstanding locks and the transaction was not pushed
	//       laying down the intents, then we're done.
	// 3. (using a separate transaction) runBatch a notification to pre-empt any
	//    leases.
	// 4. (in parallel with 3) Launch a rangefeed and wait for all existing
	//    leases to drop.
	//    a. Note that this does not need to wait for a resolved timestamp as
	//       the acquisition protocol.
	//    b. However, if any new leases which were not originally tracked get
	//       written and appear via the rangefeed, we'll know the preempting
	//       transaction has been aborted and we'll need to return an error.
	//       If we don't, then we may block forever, as that new Lease won't
	//       see a notification.
	// 5. Force the writing transaction above which invoked the preemptor above
	//    the timestamp of the last Lease drop (see open question regarding
	//    causality).

	// Run phases 1 and 2 in parallel.
	var outstandingLeases rowSet
	var checkTS hlc.Timestamp
	{
		g := ctxgroup.WithContext(ctx)
		g.GoCtx(func(ctx context.Context) error {
			return p.writeLockingIntents(ctx, txn, descriptors)
		})
		checkTS = txn.ProvisionalCommitTimestamp()
		g.GoCtx(func(ctx context.Context) (err error) {
			outstandingLeases, err = p.checkForLeases(ctx, checkTS, descriptors)
			return err
		})
		if err := g.Wait(); err != nil {
			return err
		}
		log.Infof(ctx, "done with phases 1 and 2 %v: %d, %v", descriptors, len(outstandingLeases), outstandingLeases)

		// Case 2b, there were no leases, and we got our locks.
		if len(outstandingLeases) == 0 {
			return nil
		}

		// Case 2a, our read of the leases was invalid because we got pushed writing
		// our intents.
		if txnTS := txn.ProvisionalCommitTimestamp(); !checkTS.Equal(txnTS) {
			var err error
			checkTS = txnTS
			if outstandingLeases, err = p.checkForLeases(
				ctx, checkTS, descriptors,
			); err != nil {
				return errors.Wrap(err, "refreshing the set of outstanding leases after push")
			}
		}
	}

	// Run phases 3 and 4 in parallel.
	var timestampOfRemovals hlc.Timestamp
	{
		g := ctxgroup.WithContext(ctx)
		toNotify := outstandingLeases.toIDSet()
		g.GoCtx(func(ctx context.Context) error {
			return p.notifyOutstandingLeases(ctx, toNotify)
		})
		g.GoCtx(func(ctx context.Context) (err error) {
			timestampOfRemovals, err = p.waitForOutstandingLeases(
				ctx, checkTS, outstandingLeases,
			)
			return err
		})
		// TODO(ajwerner): Detect error indicating that the transaction must be
		// aborted and, well, abort the transaction or figure out if indeed it
		// has been aborted or something.
		if err := g.Wait(); err != nil {
			return err
		}
	}

	// Phase 5.
	if log.ExpensiveLogEnabled(ctx, 1) {
		log.Infof(
			ctx, "leases %v removed as of %v",
			descriptors, timestampOfRemovals,
		)
	}

	return txn.ForwardWriteTimestamp(timestampOfRemovals)
}

func (p *Preemptor) writeLockingIntents(
	ctx context.Context, txn *kv.Txn, descriptors catalog.DescriptorIDSet,
) (err error) {
	return p.s.Delete(
		ctx, txn,
		svstorage.ActionIDsToRows(svstorage.Lock, descriptors)...,
	)
}

func (p *Preemptor) checkForLeases(
	ctx context.Context, ts hlc.Timestamp, descriptors catalog.DescriptorIDSet,
) (rowSet, error) {
	ls := make(rowSet)
	if err := p.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) (err error) {
		ls.reset() // deal with restarts
		if err := txn.SetFixedTimestamp(ctx, ts); err != nil {
			return err
		}
		return p.s.Scan(ctx, txn, svstorage.ActionIDsToRows(svstorage.Lease, descriptors), ls.add)
	}); err != nil {
		return nil, err
	}
	return ls, nil
}

func (p *Preemptor) notifyOutstandingLeases(
	ctx context.Context, toNotify catalog.DescriptorIDSet,
) error {
	return p.db.Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
		return p.s.Delete(ctx, txn, svstorage.ActionIDsToRows(svstorage.Notify, toNotify)...)
	})
}

func (p *Preemptor) waitForOutstandingLeases(
	ctx context.Context, startTS hlc.Timestamp, leases rowSet,
) (leasesRemovedTimestamp hlc.Timestamp, _ error) {
	idsOfInterest := leases.toIDSet()
	removeExpired := func() error {
		lenBefore := len(leases)
		if err := removeExpiredSessions(ctx, p.slr, leases); err != nil {
			return err
		}
		if len(leases) < lenBefore {
			leasesRemovedTimestamp.Forward(p.db.Clock().Now())
		}
		return nil
	}

	if err := removeExpired(); err != nil {
		return hlc.Timestamp{}, err
	}
	if len(leases) == 0 {
		return leasesRemovedTimestamp, nil
	}

	rfCtx, cancel := context.WithCancel(ctx)
	var wg sync.WaitGroup
	wg.Add(1)
	rfErrCh := make(chan error, 1)
	rfEventCh := make(chan svstorage.Event)
	go func() {
		defer wg.Done()
		rfErrCh <- p.s.RangeFeed(rfCtx, p.rf, svstorage.Lease, startTS, func(re svstorage.Event) {
			select {
			case rfEventCh <- re:
			case <-rfCtx.Done():
			}
		})
	}()
	defer wg.Wait()
	defer cancel()
	timer := timeutil.NewTimer()
	defer timer.Stop()
	const pollLivenessInterval = time.Second
	timer.Reset(pollLivenessInterval)
	for len(leases) > 0 {
		select {
		case <-ctx.Done():
			return hlc.Timestamp{}, ctx.Err()
		case <-timer.C:
			timer.Read = true
			if err := removeExpired(); err != nil {
				return hlc.Timestamp{}, err
			}
			timer.Reset(pollLivenessInterval)
		case err := <-rfErrCh:
			return leasesRemovedTimestamp, err
		case ev := <-rfEventCh:
			if !idsOfInterest.Contains(ev.Descriptor) {
				continue
			}
			if !ev.IsDeletion {
				// If we see a new write to an ID we're draining, we know that
				// we must be aborted. Acquisition reads from the locks portion
				// of the table and preemption lays down an intent there before
				// waiting here.
				return hlc.Timestamp{}, errTransactionMustBeAborted
			}
			if _, exists := leases[ev.Row]; exists {
				leasesRemovedTimestamp.Forward(ev.Timestamp)
				delete(leases, ev.Row)
			}
		}
	}
	return leasesRemovedTimestamp, nil
}

var errTransactionMustBeAborted = errors.New(
	"new write to the leases span indicates that the transaction is aborted",
)

func removeExpiredSessions(ctx context.Context, slr sqlliveness.Reader, leases rowSet) error {
	for r := range leases {
		isAlive, err := slr.IsAlive(ctx, r.Session)
		if err != nil {
			return err
		}
		if !isAlive {
			delete(leases, r)
		}
	}
	return nil
}
