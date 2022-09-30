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
	"runtime"
	"strconv"
	"strings"
	"time"

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
	snap := t.generateSnapshot()
	t.snapshotsMu.Lock()
	defer t.snapshotsMu.Unlock()

	snapshots := &t.snapshotsMu.snapshots

	if snapshots.Len() == maxSnapshots {
		snapshots.RemoveFirst()
	}
	var id SnapshotID
	if snapshots.Len() == 0 {
		id = 1
	} else {
		id = snapshots.GetLast().(snapshotWithID).ID + 1
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
	t.snapshotsMu.Lock()
	defer t.snapshotsMu.Unlock()
	snapshots := &t.snapshotsMu.snapshots

	if snapshots.Len() == 0 {
		return SpansSnapshot{}, errSnapshotDoesntExist
	}
	minID := snapshots.GetFirst().(snapshotWithID).ID
	if id < minID {
		return SpansSnapshot{}, errSnapshotTooOld
	}
	maxID := snapshots.GetLast().(snapshotWithID).ID
	if id > maxID {
		return SpansSnapshot{}, errSnapshotDoesntExist
	}

	return snapshots.Get(int(id - minID)).(snapshotWithID).SpansSnapshot, nil
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
		s := snapshots.Get(i).(snapshotWithID)
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

	// We don't know how big the traces are, so grow a few times if they don't
	// fit. Start large, though.
	var stacks []byte
	for n := 1 << 20; /* 1mb */ n <= (1 << 29); /* 512mb */ n *= 2 {
		stacks = make([]byte, n)
		nbytes := runtime.Stack(stacks, true /* all */)
		if nbytes < len(stacks) {
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
