// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// Package tableset implements a tableset watcher.
package tableset

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catalogkeys"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/systemschema"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/mon"
	"github.com/cockroachdb/cockroach/pkg/util/span"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
	"golang.org/x/sync/errgroup"
)

// Filter is a filter for a tableset.
type Filter struct {
	// DatabaseID is the ID of the database to watch. It is mandatory.
	DatabaseID descpb.ID
	// IncludeTables and ExcludeTables are mutually exclusive.
	// TODO(#147420): replace with the protobuf introduced in the pr for this issue.
	IncludeTables map[string]struct{}
	ExcludeTables map[string]struct{}
}

func (f Filter) String() string {
	return fmt.Sprintf("Filter{database_id=%d, include_tables=%v, exclude_tables=%v}",
		f.DatabaseID, slices.Collect(maps.Keys(f.IncludeTables)),
		slices.Collect(maps.Keys(f.ExcludeTables)))
}

func (f Filter) includes(tableInfo Table) bool {
	if tableInfo.ParentID != f.DatabaseID {
		return false
	}
	if len(f.ExcludeTables) > 0 {
		_, ok := f.ExcludeTables[tableInfo.Name]
		return !ok
	}
	if len(f.IncludeTables) > 0 {
		_, ok := f.IncludeTables[tableInfo.Name]
		return ok
	}
	return true
}

type Table struct {
	descpb.NameInfo
	ID descpb.ID
}

func (t Table) size() int64 {
	return int64(t.NameInfo.Size()) + 8
}

func (t Table) String() string {
	return fmt.Sprintf("Table{parent_id=%d, parent_schema_id=%d, name=%s, id=%d}",
		t.NameInfo.ParentID, t.NameInfo.ParentSchemaID, t.NameInfo.Name, t.ID)
}

type TableDiff struct {
	Added   Table
	Dropped Table
	AsOf    hlc.Timestamp
}

func (d TableDiff) size() int64 {
	return d.Added.size() + d.Dropped.size() + int64(d.AsOf.Size())
}

func compareTableDiffsByTS(a, b TableDiff) int {
	return a.AsOf.Compare(b.AsOf)
}

type TableSet struct {
	Tables []Table
	AsOf   hlc.Timestamp
}

// Watcher watches a tableset and buffers table diffs. It will notify waiters
// when the resolved timestamp is advanced.
type Watcher struct {
	filter Filter

	id      int64
	execCfg *sql.ExecutorConfig
	mon     *mon.BytesMonitor
	acc     mon.BoundAccount // set on Start
	started bool

	mu struct {
		syncutil.Mutex
		tableDiffs []TableDiff
		resolved   hlc.Timestamp
		// NOTE: only one waiter per timestamp is supported.
		// TODO: in reality we only need to support one waiter period.
		resolvedWaiters map[hlc.Timestamp]chan struct{}
	}
}

// NewWatcher creates a new watcher.
func NewWatcher(
	filter Filter, execCfg *sql.ExecutorConfig, mon *mon.BytesMonitor, id int64,
) *Watcher {
	w := &Watcher{
		filter:  filter,
		execCfg: execCfg,
		mon:     mon,
		id:      id,
	}
	w.mu.resolvedWaiters = make(map[hlc.Timestamp]chan struct{})
	return w
}

// Start starts the watcher. It will not return until the watcher is shut down
// due to an error or context cancellation. It may only be run once.
func (w *Watcher) Start(ctx context.Context, initialTS hlc.Timestamp) (retErr error) {
	if w.started {
		return errors.AssertionFailedf("watcher already started")
	}
	w.started = true

	ctx = logtags.AddTag(ctx, "tableset.watcher.id", fmt.Sprintf("%d", w.id))
	ctx, sp := tracing.ChildSpan(ctx, "changefeed.tableset.watcher.start")
	defer sp.Finish()
	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	log.Dev.Infof(ctx, "starting watcher with filter=%s, initialTS=%s", w.filter, initialTS)

	w.acc = w.mon.MakeBoundAccount()
	defer w.acc.Close(ctx)

	errCh := make(chan error, 1)
	setErr := func(err error) {
		if err == nil {
			return
		}
		select {
		case errCh <- err:
		default:
		}
	}

	var cfTargets changefeedbase.Targets
	cfTargets.Add(changefeedbase.Target{
		DescID:            systemschema.NamespaceTable.GetID(),
		StatementTimeName: changefeedbase.StatementTimeName(systemschema.NamespaceTable.GetName()),
		Type:              jobspb.ChangefeedTargetSpecification_EACH_FAMILY,
	})
	dec, err := cdcevent.NewEventDecoder(ctx, w.execCfg, cfTargets, false, false)
	if err != nil {
		return err
	}

	// Buffering goroutine.
	incomingTableDiffs := make(chan TableDiff)
	incomingResolveds := make(chan hlc.Timestamp)
	eg, egCtx := errgroup.WithContext(ctx)
	eg.Go(func() error {
		ctx, sp := tracing.ChildSpan(egCtx, "tableset.watcher.buffer-loop")
		defer sp.Finish()

		// Don't set this to initialTS because we're likely to do a catchup scan which may emit older resolveds.
		var curResolved hlc.Timestamp

		bufferedTableDiffs := make(map[hlc.Timestamp]map[TableDiff]struct{})
		for {
			select {
			// Buffer incoming tables between resolved messages.
			case diff := <-incomingTableDiffs:
				if diff.AsOf.Less(curResolved) {
					// TODO: it's unclear to me if this is possible/expected. If it is, allow it.
					return errors.AssertionFailedf("diff %s is before current resolved %s", diff, curResolved)
				}
				if diff.AsOf.Less(initialTS) {
					if log.V(2) {
						log.Dev.Infof(ctx, "diff %s is before initialTS %s; skipping", diff, initialTS)
					}
					continue
				}

				if _, ok := bufferedTableDiffs[curResolved]; !ok {
					bufferedTableDiffs[curResolved] = make(map[TableDiff]struct{})
				}

				// mem accounting
				if err := w.acc.Grow(ctx, diff.size()); err != nil {
					return errors.Wrapf(err, "failed to allocated %d bytes from monitor", diff.size())
				}

				bufferedTableDiffs[curResolved][diff] = struct{}{}
			// Flush buffered tables when we receive a resolved message.
			case resolved := <-incomingResolveds:
				if log.V(2) {
					log.Dev.Infof(ctx, "resolved: %s; %d tables buffered", resolved, len(bufferedTableDiffs[curResolved]))
				}

				if resolved.Less(curResolved) {
					return errors.AssertionFailedf("resolved %s is less than current resolved %s", resolved, curResolved)
				}
				// The diffs/tables are already deduped, so we just need to sort them by timestamp.
				// Using a btree could be more efficient for some use cases, but this is probably faster for the low volume we expect, and simpler.
				diffs := slices.SortedFunc(maps.Keys(bufferedTableDiffs[curResolved]), compareTableDiffsByTS)

				if err := func() error {
					w.mu.Lock()
					defer w.mu.Unlock()
					w.mu.tableDiffs = append(w.mu.tableDiffs, diffs...)
					// TODO: may be worth using a btree here also to avoid the sort-dedupe
					w.mu.tableDiffs = dedupeAndSortDiffs(w.mu.tableDiffs)

					// Update resolved and notify waiters.
					return w.updateResolvedLocked(ctx, resolved)
				}(); err != nil {
					return err
				}

				delete(bufferedTableDiffs, curResolved)

				curResolved = resolved
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	})

	// Rangefeed setup.
	onValue := func(ctx context.Context, kv *kvpb.RangeFeedValue) {
		setErr(func() error {
			var table, prevTable Table
			var err error

			if kv.Value.IsPresent() {
				table, err = w.kvToTable(ctx, roachpb.KeyValue{Key: kv.Key, Value: kv.Value}, dec)
				if err != nil {
					return err
				}
			}

			if kv.PrevValue.IsPresent() {
				kvpb := roachpb.KeyValue{Key: kv.Key, Value: kv.PrevValue}
				kvpb.Value.Timestamp = kv.Value.Timestamp
				prevTable, err = w.kvToTable(ctx, kvpb, dec)
				if err != nil {
					return err
				}
			}

			if log.V(2) {
				log.Dev.Infof(ctx, "onValue: %s; prev: %s", table, prevTable)
			}

			tableIncluded := w.filter.includes(table)
			prevTableIncluded := kv.PrevValue.IsPresent() && w.filter.includes(prevTable)
			if !tableIncluded && !prevTableIncluded {
				return nil
			}

			added := !kv.PrevValue.IsPresent()
			dropped := kv.PrevValue.IsPresent() && !kv.Value.IsPresent()
			// NOTE: a rename looks like a drop then an add, because the name is part of the key.
			// They will be in the same txn.

			var diff TableDiff
			if added {
				diff = TableDiff{Added: table, AsOf: kv.Value.Timestamp}
			} else if dropped {
				diff = TableDiff{Dropped: prevTable, AsOf: kv.Value.Timestamp}
			} else {
				log.Warningf(ctx, "onValue: unexpected table state: table=%s, prev=%s", table, prevTable)
				return nil
			}
			select {
			case incomingTableDiffs <- diff:
			case <-egCtx.Done():
				return egCtx.Err()
			}
			return nil
		}())
	}

	opts := []rangefeed.Option{
		rangefeed.WithPProfLabel("tableset.watcher", fmt.Sprintf("id=%d", w.id)),
		rangefeed.WithMemoryMonitor(w.mon),
		rangefeed.WithOnCheckpoint(func(ctx context.Context, checkpoint *kvpb.RangeFeedCheckpoint) {
			if log.V(2) {
				log.Dev.Infof(ctx, "onCheckpoint: chk=%s", checkpoint)
			}
			// This can happen when done catching up; ignore it.
			if checkpoint.ResolvedTS.IsEmpty() {
				return
			}
			select {
			case incomingResolveds <- checkpoint.ResolvedTS:
			case <-egCtx.Done():
			}
		}),
		rangefeed.WithOnInternalError(func(ctx context.Context, err error) { setErr(err) }),
		rangefeed.WithDiff(true),
		// We make the id negative so that it's not a valid job id.
		// TODO: a more elegant solution
		rangefeed.WithConsumerID(mkConsumerID(w.id)),
		rangefeed.WithInvoker(func(fn func() error) error { return fn() }),
		rangefeed.WithFiltering(false),
	}

	// Start rangefeed.
	rf := w.execCfg.RangeFeedFactory.New(
		fmt.Sprintf("tableset.watcher.id=%v", w.id), initialTS, onValue, opts...,
	)
	defer rf.Close()

	watchSpans := roachpb.Spans{systemschema.NamespaceTable.TableSpan(w.execCfg.Codec)}
	frontier, err := span.MakeFrontier(watchSpans...)
	if err != nil {
		return err
	}
	for _, span := range watchSpans {
		if _, err := frontier.Forward(span, initialTS); err != nil {
			return err
		}
	}
	frontier = span.MakeConcurrentFrontier(frontier)
	defer frontier.Release()

	if err := rf.StartFromFrontier(ctx, frontier); err != nil {
		return err
	}

	log.Dev.Infof(ctx, "rangefeed started")

	// Wait for shutdown due to error or context cancellation.
	select {
	case err := <-errCh:
		cancel(err)
		err = errors.Join(err, eg.Wait())
		log.Warningf(ctx, "watcher %d shutting down due to error: %v (%s)", w.id, err, context.Cause(ctx))
		return err
	case <-ctx.Done():
		err = errors.Join(ctx.Err(), eg.Wait())
		log.Warningf(ctx, "watcher %d shutting down due to context cancellation: %v (%s)", w.id, err, context.Cause(ctx))
		return err
	}
}

// PopUnchangedUpTo returns true if the tableset is unchanged between when it
// (or popDiffsUpTo()) was last called (or the initialTS) and the given timestamp
// [inclusive, exclusive). It discards data older than the last call to it
func (w *Watcher) PopUnchangedUpTo(
	ctx context.Context, upTo hlc.Timestamp,
) (unchanged bool, diffs []TableDiff, err error) {
	// TODO: we technically dont need to wait for resolved here if there already
	// are diffs buffered > upTo. But this seems tricky to implement rn.

	diffs, err = w.popDiffsUpTo(ctx, upTo)
	if err != nil {
		return false, nil, err
	}
	return len(diffs) == 0, diffs, nil
}

// popDiffsUpTo returns the table diffs between the last call to popDiffsUpTo()
// (or the initial timestamp) and the given timestamp [inclusive, exclusive). It
// discards data older than the last call to popDiffsUpTo(). Users of this
// package will usually call `PopUnchangedUpTo` instead, but this is exposed for
// testing.
func (w *Watcher) popDiffsUpTo(ctx context.Context, upTo hlc.Timestamp) ([]TableDiff, error) {
	if err := w.maybeWaitForResolved(ctx, upTo); err != nil {
		return nil, err
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	if buildutil.CrdbTestBuild {
		sorted := slices.IsSortedFunc(w.mu.tableDiffs, func(a, b TableDiff) int {
			return a.AsOf.Compare(b.AsOf)
		})
		if !sorted {
			return nil, errors.AssertionFailedf("diffs are not sorted")
		}
	}

	upToIdx, _ := slices.BinarySearchFunc(w.mu.tableDiffs, upTo, func(diff TableDiff, ts hlc.Timestamp) int {
		return diff.AsOf.Compare(ts)
	})

	diffs := w.mu.tableDiffs[:upToIdx]
	w.mu.tableDiffs = w.mu.tableDiffs[upToIdx:]

	// Mem accounting.
	sz := int64(0)
	for _, diff := range diffs {
		sz += diff.size()
	}
	w.acc.Shrink(ctx, sz)

	return slices.Clone(diffs), nil
}

// maybeWaitForResolved waits for the resolved timestamp to be greater than or
// equal to the given timestamp. If the resolved timestamp is already greater
// than or equal to the given timestamp, it returns immediately.
func (w *Watcher) maybeWaitForResolved(ctx context.Context, ts hlc.Timestamp) error {
	start := timeutil.Now()
	if log.V(2) {
		log.Dev.Infof(ctx, "maybeWaitForResolved(%s) start", ts)
	}

	maybeWaiter := func() chan struct{} {
		w.mu.Lock()
		defer w.mu.Unlock()

		if w.mu.resolved.Compare(ts) >= 0 {
			if log.V(2) {
				log.Dev.Infof(ctx, "maybeWaitForResolved(%s) already resolved", ts)
			}
			return nil
		}
		return w.addWaiterLocked(ts)
	}()

	if maybeWaiter == nil {
		return nil
	}

	select {
	case <-maybeWaiter:
		if log.V(2) {
			log.Dev.Infof(ctx, "maybeWaitForResolved(%s) done waiting in %s", ts, timeutil.Since(start))
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *Watcher) updateResolvedLocked(ctx context.Context, ts hlc.Timestamp) error {
	if ts.Compare(w.mu.resolved) < 0 {
		return errors.AssertionFailedf("resolved %s is less than current resolved %s", ts, w.mu.resolved)
	}

	// the lag seems to be about 3s, coinciding with the default value of kv.closed_timestamp.target_duration.
	// this is probably fine since the changefeed data will have the same lag anyway.
	if log.V(2) {
		log.Dev.Infof(ctx, "updateResolved@%d %s -> %s", timeutil.Now().Unix(), w.mu.resolved, ts)
	}
	w.mu.resolved = ts
	for wts, waiter := range w.mu.resolvedWaiters {
		if wts.Compare(ts) <= 0 {
			delete(w.mu.resolvedWaiters, wts)
			close(waiter)
		}
	}
	return nil
}

func (w *Watcher) addWaiterLocked(ts hlc.Timestamp) chan struct{} {
	waiter := make(chan struct{})
	w.mu.resolvedWaiters[ts] = waiter
	return waiter
}

func (w *Watcher) kvToTable(
	ctx context.Context, kv roachpb.KeyValue, dec cdcevent.Decoder,
) (Table, error) {
	if log.V(2) {
		log.Dev.Infof(ctx, "kvToTable: %s, %s", kv.Key, kv.Value.Timestamp)
	}

	row, err := dec.DecodeKV(ctx, kv, cdcevent.CurrentRow, kv.Value.Timestamp, false)
	if err != nil {
		return Table{}, err
	}

	var tableID descpb.ID
	err = row.ForAllColumns().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		switch col.Name {
		case "id":
			tableID = descpb.ID(tree.MustBeDInt(d))
		}
		return nil
	})
	if err != nil {
		return Table{}, err
	}

	nameInfo, err := catalogkeys.DecodeNameMetadataKey(w.execCfg.Codec, kv.Key)
	if err != nil {
		return Table{}, err
	}

	table := Table{
		NameInfo: nameInfo,
		ID:       tableID,
	}

	return table, nil
}

func mkConsumerID(id int64) int64 {
	if id > 0 {
		return -id
	}
	return id
}

func dedupeAndSortDiffs(diffs []TableDiff) []TableDiff {
	seen := make(map[TableDiff]struct{})
	for _, diff := range diffs {
		seen[diff] = struct{}{}
	}
	return slices.SortedFunc(maps.Keys(seen), compareTableDiffsByTS)
}
