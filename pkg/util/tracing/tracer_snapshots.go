// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tracing

import (
	"bufio"
	"context"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/petermattis/goid"

	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/caller"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
)

// SpansSnapshot represents a snapshot of all the open spans at a certain point
// in time.
type SpansSnapshot struct {
	// CapturedAt is the time when the snapshot was collected.
	CapturedAt time.Time
	// Traces contains the collected traces. Each "local route" corresponds to one
	// trace. "Detached recording" spans are included in the parent's trace.
	Traces []tracingpb.Recording
	// Stacks is a map from groutine ID to the goroutine's stack trace. All
	// goroutines running at the time when this snapshot is produced are
	// represented here. A goroutine referenced by a span's GoroutineID field will
	// not be present if it has finished since the respective span was started.
	Stacks map[int]string
	// Err is set if an error was encountered when producing the snapshot. The
	// snapshot will be incomplete in this case.
	Err error
}

// SnapshotID identifies a spans snapshot. The ID can be used to retrieve a
// specific snapshot from the Tracer.
type SnapshotID int
type snapshotWithID struct {
	ID SnapshotID
	SpansSnapshot
}

// SaveSnapshot collects a snapshot of the currently open spans (i.e. the
// contents of the Tracer's active spans registry) saves it in-memory in the
// Tracer. The snapshot's ID is returned; the snapshot can be retrieved
// subsequently through t.GetSnapshot(id).
//
// Snapshots also include a dump of all the goroutine stack traces.
func (t *Tracer) SaveSnapshot() SnapshotInfo {
	return t.saveSnapshot(false)
}

// SaveAutomaticSnapshot is like SaveSnapshot but saves the snapshot created in
// the "automatic" snapshots buffer instead of the manual snapshot buffer.
func (t *Tracer) SaveAutomaticSnapshot() SnapshotInfo {
	return t.saveSnapshot(true)
}

func (t *Tracer) saveSnapshot(automatic bool) SnapshotInfo {
	snap := t.generateSnapshot()
	t.snapshotsMu.Lock()
	defer t.snapshotsMu.Unlock()

	snapshots := &t.snapshotsMu.snapshots
	limit := maxSnapshots
	if automatic {
		snapshots = &t.snapshotsMu.autoSnapshots
		limit = maxAutomaticSnapshots
	}

	if snapshots.Len() >= limit {
		snapshots.RemoveFirst()
	}
	var id SnapshotID
	if snapshots.Len() == 0 {
		id = 1
	} else {
		id = snapshots.GetLast().ID + 1
	}
	snapshots.AddLast(snapshotWithID{
		ID:            id,
		SpansSnapshot: snap,
	})
	if automatic {
		id = id * -1
	}
	return SnapshotInfo{
		ID:         id,
		CapturedAt: snap.CapturedAt,
	}
}

var errSnapshotTooOld = errors.Newf(
	"the requested snapshot is too old and has been deleted. "+
		"Only the last %d snapshots are stored.", maxSnapshots)

var errSnapshotDoesntExist = errors.New("the requested snapshot doesn't exist")

// GetSnapshot returns the snapshot with the given ID. If the ID is below the
// minimum stored snapshot, then the requested snapshot must have been
// garbage-collected and errSnapshotTooOld is returned. If the snapshot id is
// beyond the maximum stored ID, errSnapshotDoesntExist is returned.
//
// Note that SpansSpanshot has an Err field through which errors are returned.
// In these error cases, the snapshot will be incomplete.
func (t *Tracer) GetSnapshot(id SnapshotID) (SpansSnapshot, error) {
	t.snapshotsMu.Lock()
	defer t.snapshotsMu.Unlock()
	snapshots := &t.snapshotsMu.snapshots
	if id < 0 {
		snapshots = &t.snapshotsMu.autoSnapshots
		id = id * -1
	}

	if snapshots.Len() == 0 {
		return SpansSnapshot{}, errSnapshotDoesntExist
	}
	minID := snapshots.GetFirst().ID
	if id < minID {
		return SpansSnapshot{}, errSnapshotTooOld
	}
	maxID := snapshots.GetLast().ID
	if id > maxID {
		return SpansSnapshot{}, errSnapshotDoesntExist
	}

	return snapshots.Get(int(id - minID)).SpansSnapshot, nil
}

// SnapshotInfo represents minimal info about a stored snapshot, as returned by
// Tracer.GetSnapshots().
type SnapshotInfo struct {
	ID         SnapshotID
	CapturedAt time.Time
}

// GetSnapshots returns info on all stored span snapshots.
func (t *Tracer) GetSnapshots() []SnapshotInfo {
	t.snapshotsMu.Lock()
	defer t.snapshotsMu.Unlock()
	snapshots := &t.snapshotsMu.snapshots
	autoSnapshots := &t.snapshotsMu.autoSnapshots

	res := make([]SnapshotInfo, snapshots.Len()+autoSnapshots.Len())
	for i := 0; i < snapshots.Len(); i++ {
		s := snapshots.Get(i)
		res[i] = SnapshotInfo{
			ID:         s.ID,
			CapturedAt: s.CapturedAt,
		}
	}
	for i := 0; i < autoSnapshots.Len(); i++ {
		s := autoSnapshots.Get(i)
		res[i] = SnapshotInfo{
			ID:         s.ID * -1,
			CapturedAt: s.CapturedAt,
		}
	}

	return res
}

// generateSnapshot produces a snapshot of all the currently open spans and
// all the current goroutine stacktraces.
//
// Note that SpansSpanshot has an Err field through which errors are returned.
// In these error cases, the snapshot will be incomplete.
func (t *Tracer) generateSnapshot() SpansSnapshot {
	capturedAt := timeutil.Now()
	// Collect the traces.
	traces := make([]tracingpb.Recording, 0, 1000)
	_ = t.SpanRegistry().VisitRoots(func(sp RegistrySpan) error {
		rec := sp.GetFullRecording(tracingpb.RecordingVerbose)
		traces = append(traces, rec.Flatten())
		return nil
	})

	// Collect and parse the goroutine stack traces.

	// We don't know how big the traces are, so grow a few times if they don't
	// fit. Start large, though.
	var stacks []byte
	for n := 1 << 20; /* 1mb */ n <= (1 << 29); /* 512mb */ n *= 2 {
		stacks = make([]byte, n)
		nbytes := runtime.Stack(stacks, true /* all */)
		if nbytes < len(stacks) {
			stacks = stacks[:nbytes]
			break
		}
	}

	splits := strings.Split(string(stacks), "\n\n")
	stackMap := make(map[int]string, len(splits))
	var parseErr error
	for _, s := range splits {
		// Parse the goroutine ID. The first line of each stack is expected to look like:
		// goroutine 115 [chan receive]:
		scanner := bufio.NewScanner(strings.NewReader(s))
		scanner.Split(bufio.ScanWords)
		// Skip the word "goroutine".
		if !scanner.Scan() {
			parseErr = errors.Errorf("unexpected end of string")
			break
		}
		// Scan the goroutine ID.
		if !scanner.Scan() {
			parseErr = errors.Errorf("unexpected end of string")
			break
		}
		goroutineID, err := strconv.Atoi(scanner.Text())
		if err != nil {
			panic(err)
		}
		stackMap[goroutineID] = s
	}

	return SpansSnapshot{
		CapturedAt: capturedAt,
		Traces:     traces,
		Stacks:     stackMap,
		Err:        parseErr,
	}
}

// PeriodicSnapshotsLoop runs until done closes, calling SaveAutomaticSnapshot
// if enabled at the interval set by the trace.snapshot.rate setting.
func (t *Tracer) PeriodicSnapshotsLoop(sv *settings.Values, done <-chan struct{}) {
	if t.autoSnapRateChange == nil {
		t.autoSnapRateChange = make(chan struct{}, 1)
	} else if !buildutil.CrdbTestBuild {
		panic("this tracer already started periodic snapshots")
	}

	periodicSnapshotInterval.SetOnChange(sv, func(_ context.Context) {
		select {
		case t.autoSnapRateChange <- struct{}{}:
		default:
		}
	})

	loop := func(rate time.Duration) (exiting bool) {
		ticker := time.NewTicker(rate)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return true
			case <-ticker.C:
				t.SaveAutomaticSnapshot()
			case <-t.autoSnapRateChange:
				if periodicSnapshotInterval.Get(sv) != rate {
					return false
				}
			}
		}
	}

	for {
		if rate := periodicSnapshotInterval.Get(sv); rate != 0 {
			if loop(rate) {
				return // loop hit done
			}
		} else {
			select {
			case <-done:
				return
			case <-t.autoSnapRateChange:
			}
		}
	}
}

// MaybeRecordStackHistory records in the span found in the passed context, if
// there is one and it is verbose or has a sink, any stacks found for the
// current goroutine in the currently stored tracer automatic snapshots, since
// the passed time (generally when this goroutine started processing this
// request/op). See the "trace.snapshot.rate" setting for controlling whether
// such automatic snapshots are available to be searched and if so at what
// granularity.
func (t *Tracer) MaybeRecordStackHistory(ctx context.Context, op string, since time.Time) {
	sp := SpanFromContext(ctx)
	if sp == nil {
		return
	}
	if !sp.IsVerbose() && !sp.Tracer().HasExternalSink() {
		return
	}

	id := int(goid.Get())
	callerFile, callerLine, _ := caller.Lookup(1)

	t.snapshotsMu.Lock()
	defer t.snapshotsMu.Unlock()

	for i := t.snapshotsMu.autoSnapshots.Len() - 1; i >= 0; i-- {
		s := t.snapshotsMu.autoSnapshots.Get(i)
		if s.CapturedAt.Before(since) {
			return
		}
		if stack, ok := s.Stacks[id]; ok {
			// TODO(dt): make this structured?
			sp.Recordf("%s:%d: %s had following stack as of %.1fs ago: %s",
				callerFile, callerLine, op, timeutil.Since(s.CapturedAt).Seconds(), stack)
		}
	}
}
