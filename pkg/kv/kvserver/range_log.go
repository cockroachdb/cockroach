// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvserver

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/cockroach/pkg/util/retry"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

// DBOrTxn is used to provide flexibility for logging rangelog events either
// transactionally (using a Txn), or non-transactionally (using a DB).
type DBOrTxn interface {
	Run(ctx context.Context, b *kv.Batch) error
	NewBatch() *kv.Batch
}

// RangeLogWriter is used to write range log events to the rangelog
// table.
type RangeLogWriter interface {
	WriteRangeLogEvent(context.Context, DBOrTxn, kvserverpb.RangeLogEvent) error
}

// wrappedRangeLogWriter implements RangeLogWriter, performing logging and
// metric incrementing, and consulting an oracle to decide whether it should
// delegate to the underlying implementation.
type wrappedRangeLogWriter struct {
	getCounter  rangeLogEventTypeCounterFunc
	shouldWrite func() bool
	underlying  RangeLogWriter
}

type rangeLogEventTypeCounterFunc = func(
	eventType kvserverpb.RangeLogEventType,
) *metric.Counter

func newWrappedRangeLogWriter(
	getCounter rangeLogEventTypeCounterFunc, shouldWrite func() bool, underlying RangeLogWriter,
) RangeLogWriter {
	return &wrappedRangeLogWriter{
		getCounter:  getCounter,
		shouldWrite: shouldWrite,
		underlying:  underlying,
	}
}

var _ RangeLogWriter = (*wrappedRangeLogWriter)(nil)

func (w *wrappedRangeLogWriter) WriteRangeLogEvent(
	ctx context.Context, runner DBOrTxn, event kvserverpb.RangeLogEvent,
) error {
	maybeLogRangeLogEvent(ctx, event)
	if c := w.getCounter(event.EventType); c != nil {
		c.Inc(1)
	}
	if w.shouldWrite() && w.underlying != nil {
		return w.underlying.WriteRangeLogEvent(ctx, runner, event)
	}
	return nil
}

func maybeLogRangeLogEvent(ctx context.Context, event kvserverpb.RangeLogEvent) {
	if !log.V(1) {
		return
	}
	// Record range log event to console log.
	var info string
	if event.Info != nil {
		info = event.Info.String()
	}
	log.Infof(ctx, "Range Event: %q, range: %d, info: %s",
		event.EventType, event.RangeID, info)
}

// logSplit logs a range split event into the event table. The affected range is
// the range which previously existed and is being split in half; the "other"
// range is the new range which is being created.
func (s *Store) logSplit(
	ctx context.Context,
	txn *kv.Txn,
	updatedDesc, newDesc roachpb.RangeDescriptor,
	reason string,
	logAsync bool,
) error {
	logEvent := kvserverpb.RangeLogEvent{
		Timestamp:    selectEventTimestamp(s, txn.ReadTimestamp()),
		RangeID:      updatedDesc.RangeID,
		EventType:    kvserverpb.RangeLogEventType_split,
		StoreID:      s.StoreID(),
		OtherRangeID: newDesc.RangeID,
		Info: &kvserverpb.RangeLogEvent_Info{
			UpdatedDesc: &updatedDesc,
			NewDesc:     &newDesc,
			Details:     reason,
		},
	}

	return writeToRangeLogTable(ctx, s, txn, logEvent, logAsync)
}

// logMerge logs a range split event into the event table. The affected range is
// the subsuming range; the "other" range is the subsumed range.
//
// TODO(benesch): There are several different reasons that a range merge
// could occur, and that information should be logged.
func (s *Store) logMerge(
	ctx context.Context, txn *kv.Txn, updatedLHSDesc, rhsDesc roachpb.RangeDescriptor, logAsync bool,
) error {
	logEvent := kvserverpb.RangeLogEvent{
		Timestamp:    selectEventTimestamp(s, txn.ReadTimestamp()),
		RangeID:      updatedLHSDesc.RangeID,
		EventType:    kvserverpb.RangeLogEventType_merge,
		StoreID:      s.StoreID(),
		OtherRangeID: rhsDesc.RangeID,
		Info: &kvserverpb.RangeLogEvent_Info{
			UpdatedDesc: &updatedLHSDesc,
			RemovedDesc: &rhsDesc,
		},
	}

	return writeToRangeLogTable(ctx, s, txn, logEvent, logAsync)
}

// logChange logs a replica change event, which represents a replica being added
// to or removed from a range.
// TODO(mrtracy): There are several different reasons that a replica change
// could occur, and that information should be logged.
func (s *Store) logChange(
	ctx context.Context,
	txn *kv.Txn,
	changeType roachpb.ReplicaChangeType,
	replica roachpb.ReplicaDescriptor,
	desc roachpb.RangeDescriptor,
	reason kvserverpb.RangeLogEventReason,
	details string,
	logAsync bool,
) error {
	var logType kvserverpb.RangeLogEventType
	var info kvserverpb.RangeLogEvent_Info
	switch changeType {
	case roachpb.ADD_VOTER:
		logType = kvserverpb.RangeLogEventType_add_voter
		info = kvserverpb.RangeLogEvent_Info{
			AddedReplica: &replica,
			UpdatedDesc:  &desc,
			Reason:       reason,
			Details:      details,
		}
	case roachpb.REMOVE_VOTER:
		logType = kvserverpb.RangeLogEventType_remove_voter
		info = kvserverpb.RangeLogEvent_Info{
			RemovedReplica: &replica,
			UpdatedDesc:    &desc,
			Reason:         reason,
			Details:        details,
		}
	case roachpb.ADD_NON_VOTER:
		logType = kvserverpb.RangeLogEventType_add_non_voter
		info = kvserverpb.RangeLogEvent_Info{
			AddedReplica: &replica,
			UpdatedDesc:  &desc,
			Reason:       reason,
			Details:      details,
		}
	case roachpb.REMOVE_NON_VOTER:
		logType = kvserverpb.RangeLogEventType_remove_non_voter
		info = kvserverpb.RangeLogEvent_Info{
			RemovedReplica: &replica,
			UpdatedDesc:    &desc,
			Reason:         reason,
			Details:        details,
		}
	default:
		return errors.Errorf("unknown replica change type %s", changeType)
	}

	logEvent := kvserverpb.RangeLogEvent{
		Timestamp: selectEventTimestamp(s, txn.ReadTimestamp()),
		RangeID:   desc.RangeID,
		EventType: logType,
		StoreID:   s.StoreID(),
		Info:      &info,
	}

	return writeToRangeLogTable(ctx, s, txn, logEvent, logAsync)
}

// selectEventTimestamp selects a timestamp for this log message. If the
// transaction this event is being written in has a non-zero timestamp, then that
// timestamp should be used; otherwise, the store's physical clock is used.
// This helps with testing; in normal usage, the logging of an event will never
// be the first action in the transaction, and thus the transaction will have an
// assigned database timestamp. However, in the case of our tests log events
// *are* the first action in a transaction, and we must elect to use the store's
// physical time instead.
func selectEventTimestamp(s *Store, input hlc.Timestamp) time.Time {
	if input.IsEmpty() {
		return s.Clock().PhysicalTime()
	}
	return input.GoTime()
}

// writeToRangeLogTable writes a range-change log event to system.rangelog by
// invoking RangeLogWriter.WriteRangeLogEvent. If logAsync is false, the logging
// is done directly as part of the given transaction. If logAsync is true, the
// logging is done in an async task (with retries and timeouts), and that task is
// added as a commit trigger to the given transaction.
func writeToRangeLogTable(
	ctx context.Context, s *Store, txn *kv.Txn, logEvent kvserverpb.RangeLogEvent, logAsync bool,
) error {
	if !logAsync {
		return s.cfg.RangeLogWriter.WriteRangeLogEvent(ctx, txn, logEvent)
	}

	asyncLogFn := func(ctx context.Context) {
		stopper := txn.DB().Context().Stopper
		// Copy the tags from the original context
		asyncCtx := logtags.AddTags(context.Background(), logtags.FromContext(ctx))
		// Stop writing when the server shuts down.
		asyncCtx, stopCancel := stopper.WithCancelOnQuiesce(asyncCtx)

		const perAttemptTimeout = 20 * time.Second
		const maxAttempts = 3
		retryOpts := base.DefaultRetryOptions()
		retryOpts.Closer = asyncCtx.Done()
		retryOpts.MaxRetries = maxAttempts

		if err := stopper.RunAsyncTask(
			asyncCtx, "rangelog-async", func(ctx context.Context) {
				defer stopCancel()
				for r := retry.Start(retryOpts); r.Next(); {
					if err := timeutil.RunWithTimeout(ctx, "rangelog-timeout", perAttemptTimeout, func(ctx context.Context) error {
						return s.cfg.RangeLogWriter.WriteRangeLogEvent(ctx, txn.DB(), logEvent)
					}); err != nil {
						log.Warningf(ctx, "error logging to system.rangelog: %v", err)
						continue
					}
					break
				}
			}); err != nil {
			log.Warningf(asyncCtx, "async task error while logging to system.rangelog: %v", err)
			stopCancel()
		}
	}
	txn.AddCommitTrigger(asyncLogFn)
	return nil
}
