// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/span"
)

// State is the snapshot of TickManager state that survives a job
// restart. It is what the coordinator persists periodically and
// what it loads on resume to pick up where the prior incarnation
// left off.
//
// Three pieces, none of which can be derived from the other two:
//
//   - HighWater is the end time of the most-recently-closed tick.
//     User-visible (system.job_progress.resolved) and used by the
//     coordinator on resume to skip already-closed ticks.
//   - Frontier is the multi-span resolved frontier — the per-span
//     timestamp through which every contributing producer has
//     reported. Producers re-subscribe their rangefeeds at this
//     point on resume so events with ts > Frontier are
//     (re-)delivered and events with ts <= Frontier are not.
//   - OpenTicks lists the data files PUT for ticks whose manifest
//     hasn't been written yet. On resume, these get folded into
//     the new incarnation's pending state so the eventual close
//     marker still references files written before the crash —
//     even if the producers that wrote them no longer exist.
type State struct {
	HighWater hlc.Timestamp
	Frontier  span.Frontier
	OpenTicks map[hlc.Timestamp][]revlogpb.File
}

// Persister loads and stores TickManager checkpoint state. The
// orchestration code (TickManager, the checkpoint loop in flow.go)
// uses this interface; concrete implementations live elsewhere
// (progress_persist.go for the production jobs-backed impl;
// in-memory variants for tests).
//
// All persistence is expected to be atomic — a partial Store that
// e.g. writes the frontier but not the open-tick file lists would
// leave a resumed coordinator in a state where the file lists
// disagree with what the producers think the frontier was at, so
// the implementation should compose every write into one
// transaction.
//
// Load returns (state, true, nil) when a prior checkpoint exists,
// (zero, false, nil) when the job has no prior checkpoint (first
// run), or (zero, false, err) on a hard error.
type Persister interface {
	Load(ctx context.Context) (State, bool, error)
	Store(ctx context.Context, state State) error
}
