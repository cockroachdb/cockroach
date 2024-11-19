// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tracing

import (
	"bufio"
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/util/allstacks"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing/tracingpb"
	"github.com/cockroachdb/errors"
	"github.com/petermattis/goid"
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
	return t.getSnapshot(id, false)
}

// GetAutomaticSnapshot is a variant of GetSnapshot but for retrieving automatic
// snapshots.
func (t *Tracer) GetAutomaticSnapshot(id SnapshotID) (SpansSnapshot, error) {
	return t.getSnapshot(id, true)
}

func (t *Tracer) getSnapshot(id SnapshotID, auto bool) (SpansSnapshot, error) {
	t.snapshotsMu.Lock()
	defer t.snapshotsMu.Unlock()
	snapshots := &t.snapshotsMu.snapshots
	if auto {
		snapshots = &t.snapshotsMu.autoSnapshots
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

	res := make([]SnapshotInfo, snapshots.Len())
	for i := 0; i < snapshots.Len(); i++ {
		s := snapshots.Get(i)
		res[i] = SnapshotInfo{
			ID:         s.ID,
			CapturedAt: s.CapturedAt,
		}
	}
	return res
}

// GetAutomaticSnapshots returns info on all stored automatic span snapshots.
func (t *Tracer) GetAutomaticSnapshots() []SnapshotInfo {
	t.snapshotsMu.Lock()
	defer t.snapshotsMu.Unlock()
	autoSnapshots := &t.snapshotsMu.autoSnapshots

	res := make([]SnapshotInfo, autoSnapshots.Len())
	for i := 0; i < autoSnapshots.Len(); i++ {
		s := autoSnapshots.Get(i)
		res[i] = SnapshotInfo{
			ID:         s.ID,
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
	stacks := allstacks.Get()

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
	ch := make(chan struct{}, 1)
	periodicSnapshotInterval.SetOnChange(sv, func(_ context.Context) {
		select {
		case ch <- struct{}{}:
		default:
		}
	})
	t.runPeriodicSnapshotsLoop(sv, ch, done, nil /* testingKnob */)
}

func (t *Tracer) runPeriodicSnapshotsLoop(
	sv *settings.Values, settingChange, done, testingKnob <-chan struct{},
) {
	loop := func(rate time.Duration) (exiting bool) {
		ticker := time.NewTicker(rate)
		defer ticker.Stop()
		for {
			select {
			case <-done:
				return true
			case <-ticker.C:
				t.SaveAutomaticSnapshot()
				if testingKnob != nil {
					if _, ok := <-testingKnob; !ok {
						return true
					}
				}
			case <-settingChange:
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
			case <-settingChange:
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
func (sp *Span) MaybeRecordStackHistory(since time.Time) {
	if sp == nil || sp.RecordingType() == tracingpb.RecordingOff {
		return
	}

	t := sp.Tracer()
	id := int(goid.Get())

	var prevStack string

	t.snapshotsMu.Lock()
	defer t.snapshotsMu.Unlock()
	for i := 0; i < t.snapshotsMu.autoSnapshots.Len(); i++ {
		s := t.snapshotsMu.autoSnapshots.Get(i)
		if s.CapturedAt.Before(since) {
			continue
		}
		stack, ok := s.Stacks[id]
		if ok {
			sp.RecordStructured(stackDelta(prevStack, stack, timeutil.Since(s.CapturedAt)))
			prevStack = stack
		}
	}
}

func stackDelta(base, change string, age time.Duration) Structured {
	if base == "" {
		return &tracingpb.CapturedStack{Stack: change, Age: age}
	}

	var i, lines int
	for i = range base {
		c := base[len(base)-1-i]
		if i > len(change) || change[len(change)-1-i] != c {
			break
		}
		if c == '\n' {
			lines++
		}
	}
	return &tracingpb.CapturedStack{
		Stack: change[:len(change)-i], SharedSuffix: int32(i), SharedLines: int32(lines),
	}
}
