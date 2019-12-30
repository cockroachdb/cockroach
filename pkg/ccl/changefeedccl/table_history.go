// Copyright 2018 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package changefeedccl

import (
	"context"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

type tableHistoryWaiter struct {
	ts    hlc.Timestamp
	errCh chan error
}

// tableHistory tracks that a some invariants hold over a set of tables as time
// advances.
//
// Internally, two timestamps are tracked. The high-water is the highest
// timestamp such that every version of a TableDescriptor has met a provided
// invariant (via `validateFn`). An error timestamp is also kept, which is the
// lowest timestamp where at least one table doesn't meet the invariant.
//
// The `WaitForTS` method allows a user to block until some given timestamp is
// less (or equal) to either the high-water or the error timestamp. In the
// latter case, it returns the error.
type tableHistory struct {
	validateFn func(context.Context, *sqlbase.TableDescriptor) error

	mu struct {
		syncutil.Mutex

		// the highest known valid timestamp
		highWater hlc.Timestamp

		// the lowest known invalid timestamp
		errTS hlc.Timestamp

		// the error associated with errTS
		err error

		// callers waiting on a timestamp to be resolved as valid or invalid
		waiters []tableHistoryWaiter
	}
}

// makeTableHistory creates tableHistory with the given initial high-water and
// invariant check function. It is expected that `validateFn` is deterministic.
func makeTableHistory(
	validateFn func(context.Context, *sqlbase.TableDescriptor) error, initialHighWater hlc.Timestamp,
) *tableHistory {
	m := &tableHistory{validateFn: validateFn}
	m.mu.highWater = initialHighWater
	return m
}

// HighWater returns the current high-water timestamp.
func (m *tableHistory) HighWater() hlc.Timestamp {
	m.mu.Lock()
	highWater := m.mu.highWater
	m.mu.Unlock()
	return highWater
}

// WaitForTS blocks until the given timestamp is less than or equal to the
// high-water or error timestamp. In the latter case, the error is returned.
//
// If called twice with the same timestamp, two different errors may be returned
// (since the error timestamp can recede). However, the return for a given
// timestamp will never switch from nil to an error or vice-versa (assuming that
// `validateFn` is deterministic and the ingested descriptors are read
// transactionally).
func (m *tableHistory) WaitForTS(ctx context.Context, ts hlc.Timestamp) error {
	var errCh chan error

	m.mu.Lock()
	highWater := m.mu.highWater
	var err error
	if m.mu.errTS != (hlc.Timestamp{}) && m.mu.errTS.LessEq(ts) {
		err = m.mu.err
	}
	fastPath := err != nil || ts.LessEq(highWater)
	if !fastPath {
		errCh = make(chan error, 1)
		m.mu.waiters = append(m.mu.waiters, tableHistoryWaiter{ts: ts, errCh: errCh})
	}
	m.mu.Unlock()
	if fastPath {
		if log.V(1) {
			log.Infof(ctx, "fastpath for %s: %v", ts, err)
		}
		return err
	}

	if log.V(1) {
		log.Infof(ctx, "waiting for %s highwater", ts)
	}
	start := timeutil.Now()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-errCh:
		if log.V(1) {
			log.Infof(ctx, "waited %s for %s highwater: %v", timeutil.Since(start), ts, err)
		}
		return err
	}
}

// IngestDescriptors checks the given descriptors against the invariant check
// function and adjusts the high-water or error timestamp appropriately. It is
// required that the descriptors represent a transactional kv read between the
// two given timestamps.
func (m *tableHistory) IngestDescriptors(
	ctx context.Context, startTS, endTS hlc.Timestamp, descs []*sqlbase.TableDescriptor,
) error {
	sort.Slice(descs, func(i, j int) bool {
		return descs[i].ModificationTime.Less(descs[j].ModificationTime)
	})
	var validateErr error
	for _, desc := range descs {
		if err := m.validateFn(ctx, desc); validateErr == nil {
			validateErr = err
		}
	}
	return m.adjustTimestamps(startTS, endTS, validateErr)
}

// adjustTimestamps adjusts the high-water or error timestamp appropriately.
func (m *tableHistory) adjustTimestamps(startTS, endTS hlc.Timestamp, validateErr error) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if validateErr != nil {
		// don't care about startTS in the invalid case
		if m.mu.errTS == (hlc.Timestamp{}) || endTS.Less(m.mu.errTS) {
			m.mu.errTS = endTS
			m.mu.err = validateErr
			newWaiters := make([]tableHistoryWaiter, 0, len(m.mu.waiters))
			for _, w := range m.mu.waiters {
				if w.ts.Less(m.mu.errTS) {
					newWaiters = append(newWaiters, w)
					continue
				}
				w.errCh <- validateErr
			}
			m.mu.waiters = newWaiters
		}
		return validateErr
	}

	if m.mu.highWater.Less(startTS) {
		return errors.Errorf(`gap between %s and %s`, m.mu.highWater, startTS)
	}
	if m.mu.highWater.Less(endTS) {
		m.mu.highWater = endTS
		newWaiters := make([]tableHistoryWaiter, 0, len(m.mu.waiters))
		for _, w := range m.mu.waiters {
			if m.mu.highWater.Less(w.ts) {
				newWaiters = append(newWaiters, w)
				continue
			}
			w.errCh <- nil
		}
		m.mu.waiters = newWaiters
	}
	return nil
}

type tableHistoryUpdater struct {
	settings *cluster.Settings
	db       *client.DB
	targets  jobspb.ChangefeedTargets
	m        *tableHistory
}

func (u *tableHistoryUpdater) PollTableDescs(ctx context.Context) error {
	// TODO(dan): Replace this with a RangeFeed once it stabilizes.
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(changefeedPollInterval.Get(&u.settings.SV)):
		}

		startTS, endTS := u.m.HighWater(), u.db.Clock().Now()
		if endTS.LessEq(startTS) {
			continue
		}
		descs, err := fetchTableDescriptorVersions(ctx, u.db, startTS, endTS, u.targets)
		if err != nil {
			return err
		}
		if err := u.m.IngestDescriptors(ctx, startTS, endTS, descs); err != nil {
			return err
		}
	}
}

func fetchTableDescriptorVersions(
	ctx context.Context,
	db *client.DB,
	startTS, endTS hlc.Timestamp,
	targets jobspb.ChangefeedTargets,
) ([]*sqlbase.TableDescriptor, error) {
	if log.V(2) {
		log.Infof(ctx, `fetching table descs (%s,%s]`, startTS, endTS)
	}
	start := timeutil.Now()
	span := roachpb.Span{Key: keys.MakeTablePrefix(keys.DescriptorTableID)}
	span.EndKey = span.Key.PrefixEnd()
	header := roachpb.Header{Timestamp: endTS}
	req := &roachpb.ExportRequest{
		RequestHeader: roachpb.RequestHeaderFromSpan(span),
		StartTime:     startTS,
		MVCCFilter:    roachpb.MVCCFilter_All,
		ReturnSST:     true,
		OmitChecksum:  true,
	}
	res, pErr := client.SendWrappedWith(ctx, db.NonTransactionalSender(), header, req)
	if log.V(2) {
		log.Infof(ctx, `fetched table descs (%s,%s] took %s`, startTS, endTS, timeutil.Since(start))
	}
	if pErr != nil {
		err := pErr.GoError()
		return nil, errors.Wrapf(err, `fetching changes for %s`, span)
	}

	var tableDescs []*sqlbase.TableDescriptor
	for _, file := range res.(*roachpb.ExportResponse).Files {
		if err := func() error {
			it, err := engine.NewMemSSTIterator(file.SST, false /* verify */)
			if err != nil {
				return err
			}
			defer it.Close()
			for it.SeekGE(engine.NilKey); ; it.Next() {
				if ok, err := it.Valid(); err != nil {
					return err
				} else if !ok {
					return nil
				}
				k := it.UnsafeKey()
				remaining, _, _, err := sqlbase.DecodeTableIDIndexID(k.Key)
				if err != nil {
					return err
				}
				_, tableID, err := encoding.DecodeUvarintAscending(remaining)
				if err != nil {
					return err
				}
				origName, ok := targets[sqlbase.ID(tableID)]
				if !ok {
					// Uninteresting table.
					continue
				}
				unsafeValue := it.UnsafeValue()
				if unsafeValue == nil {
					return errors.Errorf(`"%v" was dropped or truncated`, origName)
				}
				value := roachpb.Value{RawBytes: unsafeValue}
				var desc sqlbase.Descriptor
				if err := value.GetProto(&desc); err != nil {
					return err
				}
				if tableDesc := desc.Table(k.Timestamp); tableDesc != nil {
					tableDescs = append(tableDescs, tableDesc)
				}
			}
		}(); err != nil {
			return nil, err
		}
	}
	return tableDescs, nil
}
