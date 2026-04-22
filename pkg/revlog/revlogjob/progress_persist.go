// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package revlogjob

import (
	"context"
	"math"

	"github.com/cockroachdb/cockroach/pkg/jobs"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobfrontier"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/revlog/revlogpb"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// jobInfoKeys are the InfoStorage / jobfrontier key names this
// package owns under a job ID. Using one place prevents drift
// between Load and Store paths.
const (
	openTicksInfoKey   = "revlog/open-ticks"
	flushedFrontierKey = "revlog/flushed"
)

// jobPersister composes the three job-side storage APIs into one
// atomic checkpoint:
//
//   - jobs.ProgressStorage(id).Set publishes the high-water as the
//     job's user-visible resolved timestamp (the value SHOW JOBS
//     and operator dashboards surface) and clears any stale
//     fraction.
//   - jobs.InfoStorageForJob.WriteProto persists the open-tick file
//     lists as a small proto blob.
//   - jobfrontier.Store persists the multi-span flushed frontier
//     using the job_info-backed frontier wrapper.
//
// All three writes happen in one InternalDB.Txn so a partial
// checkpoint never leaves the resumed coordinator with state
// pieces that disagree with each other.
//
// jobPersister is the only place in revlogjob that imports the
// jobs/jobspb/isql trio — orchestration code (TickManager,
// flow.go's checkpoint loop) holds it via the Persister interface.
type jobPersister struct {
	jobID jobspb.JobID
	db    isql.DB
}

var _ Persister = (*jobPersister)(nil)

// newJobPersister returns a Persister that bridges TickManager
// state to the job's progress / info / frontier storage rows under
// jobID.
func newJobPersister(jobID jobspb.JobID, db isql.DB) *jobPersister {
	return &jobPersister{jobID: jobID, db: db}
}

// Load reads the persisted checkpoint state for the job. Returns
// (zero, false, nil) when the job has no prior checkpoint (first
// run or never-checkpointed).
func (p *jobPersister) Load(ctx context.Context) (State, bool, error) {
	var (
		state State
		found bool
	)
	err := p.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		state = State{}
		found = false

		var openTicks revlogpb.OpenTicks
		ok, err := jobs.InfoStorageForJob(txn, p.jobID).GetProto(ctx, openTicksInfoKey, &openTicks)
		if err != nil {
			return errors.Wrap(err, "loading open-ticks blob")
		}
		if !ok {
			// No prior checkpoint at all. Frontier and high-water
			// reads are skipped — the caller treats !found as "first
			// run" and initializes state from scratch.
			return nil
		}
		found = true
		state.OpenTicks = make(map[hlc.Timestamp][]revlogpb.File, len(openTicks.Ticks))
		for _, t := range openTicks.Ticks {
			state.OpenTicks[t.TickEnd] = t.Files
		}

		frontier, ok, err := jobfrontier.Get(ctx, txn, p.jobID, flushedFrontierKey)
		if err != nil {
			return errors.Wrap(err, "loading flushed frontier")
		}
		if ok {
			state.Frontier = frontier
		}

		// The user-visible high-water lives in
		// system.job_progress.resolved — read it back via
		// ProgressStorage rather than maintaining a redundant copy
		// in InfoStorage.
		_, hw, _, err := jobs.ProgressStorage(p.jobID).Get(ctx, txn)
		if err != nil {
			return errors.Wrap(err, "loading job progress")
		}
		state.HighWater = hw
		return nil
	})
	if err != nil {
		return State{}, false, err
	}
	return state, found, nil
}

// Store atomically writes the three pieces of checkpoint state
// under one transaction. Order of writes within the txn doesn't
// matter — they all commit or none do — but the Set is last so
// that a hypothetical caller observing partial commits would never
// see the user-visible HighWater jump ahead of the actual
// underlying state. (Today we always commit the whole txn or none,
// so this is belt-and-suspenders.)
func (p *jobPersister) Store(ctx context.Context, state State) error {
	return p.db.Txn(ctx, func(ctx context.Context, txn isql.Txn) error {
		ticks := make([]revlogpb.OpenTick, 0, len(state.OpenTicks))
		for tickEnd, files := range state.OpenTicks {
			ticks = append(ticks, revlogpb.OpenTick{
				TickEnd: tickEnd,
				Files:   files,
			})
		}
		blob := &revlogpb.OpenTicks{Ticks: ticks}
		if err := jobs.InfoStorageForJob(txn, p.jobID).WriteProto(
			ctx, openTicksInfoKey, blob,
		); err != nil {
			return errors.Wrap(err, "writing open-ticks blob")
		}
		if state.Frontier != nil {
			if err := jobfrontier.Store(
				ctx, txn, p.jobID, flushedFrontierKey, state.Frontier,
			); err != nil {
				return errors.Wrap(err, "writing flushed frontier")
			}
		}
		// Continuous backup is unbounded, so there's no meaningful
		// completion fraction to report — pass NaN to leave it null.
		if err := jobs.ProgressStorage(p.jobID).Set(
			ctx, txn, math.NaN(), state.HighWater,
		); err != nil {
			return errors.Wrap(err, "writing job progress")
		}
		return nil
	})
}
