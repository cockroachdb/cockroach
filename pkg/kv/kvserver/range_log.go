// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/metric"
	"github.com/cockroachdb/errors"
)

// RangeLogWriter is used to write range log events to the rangelog
// table.
type RangeLogWriter interface {
	WriteRangeLogEvent(context.Context, *kv.Txn, kvserverpb.RangeLogEvent) error
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
	ctx context.Context, txn *kv.Txn, event kvserverpb.RangeLogEvent,
) error {
	maybeLogRangeLogEvent(ctx, event)
	if c := w.getCounter(event.EventType); c != nil {
		c.Inc(1)
	}
	if w.shouldWrite() && w.underlying != nil {
		return w.underlying.WriteRangeLogEvent(ctx, txn, event)
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
	ctx context.Context, txn *kv.Txn, updatedDesc, newDesc roachpb.RangeDescriptor, reason string,
) error {
	return s.cfg.RangeLogWriter.WriteRangeLogEvent(ctx, txn, kvserverpb.RangeLogEvent{
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
	})
}

// logMerge logs a range split event into the event table. The affected range is
// the subsuming range; the "other" range is the subsumed range.
//
// TODO(benesch): There are several different reasons that a range merge
// could occur, and that information should be logged.
func (s *Store) logMerge(
	ctx context.Context, txn *kv.Txn, updatedLHSDesc, rhsDesc roachpb.RangeDescriptor,
) error {
	return s.cfg.RangeLogWriter.WriteRangeLogEvent(ctx, txn, kvserverpb.RangeLogEvent{
		Timestamp:    selectEventTimestamp(s, txn.ReadTimestamp()),
		RangeID:      updatedLHSDesc.RangeID,
		EventType:    kvserverpb.RangeLogEventType_merge,
		StoreID:      s.StoreID(),
		OtherRangeID: rhsDesc.RangeID,
		Info: &kvserverpb.RangeLogEvent_Info{
			UpdatedDesc: &updatedLHSDesc,
			RemovedDesc: &rhsDesc,
		},
	})
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

	return s.cfg.RangeLogWriter.WriteRangeLogEvent(ctx, txn, kvserverpb.RangeLogEvent{
		Timestamp: selectEventTimestamp(s, txn.ReadTimestamp()),
		RangeID:   desc.RangeID,
		EventType: logType,
		StoreID:   s.StoreID(),
		Info:      &info,
	})
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
