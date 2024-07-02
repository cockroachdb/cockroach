// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package logical

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/repstream/streampb"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

// purgatory is an ordered list of purgatory levels, each consisting of some
// number of events that need to be durably processed to finish that level and
// an optional checkpoint that can be applied when it is fully processed. If
// purgatory is non-empty, incoming checkpoints must go to purgatory instead
// of being emitted, and will be emitted when the purgatory level is processed
// instead.
type purgatory struct {
	levels   []purgatoryLevel
	delay    time.Duration
	deadline time.Duration
}

type purgatoryLevel struct {
	events                  []streampb.StreamEvent_KV
	willResolve             []jobspb.ResolvedSpan
	closedAt, lastAttempted time.Time
}

func (p *purgatory) Checkpoint(ctx context.Context, checkpoint []jobspb.ResolvedSpan) {
	if len(p.levels) == 0 || p.levels[len(p.levels)-1].willResolve != nil {
		// If the current purgatory level is already closed, make a new one.
		p.levels = append(p.levels, purgatoryLevel{})
	}
	// Close the current layer and mark it as resolving this checkpoint.
	p.levels[len(p.levels)-1].willResolve = checkpoint
	p.levels[len(p.levels)-1].closedAt = timeutil.Now()
}

func (p *purgatory) Store(events []streampb.StreamEvent_KV) {
	// Open a new level if there is no level or the current level is closed.
	if len(p.levels) == 0 || p.levels[len(p.levels)-1].willResolve != nil {
		p.levels = append(p.levels, purgatoryLevel{})
	}
	p.levels[len(p.levels)-1].events = events
}

func (p *purgatory) Drain(ctx context.Context,
	flush func(context.Context, []streampb.StreamEvent_KV, bool) ([]streampb.StreamEvent_KV, error),
	checkpoint func(context.Context, []jobspb.ResolvedSpan) error,
) error {
	var resolved int
	for i := range p.levels {
		// If we need to make space, or if the events have been in purgatory for a
		// over a minute, then any events that still fail to apply must just DLQ.
		mustProcess := (i == 0 && p.Full()) || timeutil.Since(p.levels[i].closedAt) > p.deadline

		// If tried to flush this purgatory recently and it isn't required to flush
		// now, wait a until next time to try again.
		if timeutil.Since(p.levels[i].lastAttempted) < p.delay && !mustProcess {
			break
		}

		p.levels[i].lastAttempted = timeutil.Now()
		remaining, err := flush(ctx, p.levels[i].events, mustProcess)
		if err != nil {
			return err
		}
		p.levels[i].events = remaining
		if len(remaining) > 0 || p.levels[i].willResolve == nil {
			break
		}
		if err := checkpoint(ctx, p.levels[i].willResolve); err != nil {
			return err
		}
		resolved++
	}
	// Remove all levels that were resolved.
	p.levels = p.levels[resolved:]
	return nil
}

func (p purgatory) Empty() bool {
	return len(p.levels) == 0
}

func (p *purgatory) Full() bool {
	// TODO(dt): make this smarter.
	return len(p.levels) >= 10
}
