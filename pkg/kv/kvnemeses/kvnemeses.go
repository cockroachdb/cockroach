// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvnemeses

import (
	"context"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// RunNemeses generates and applies a series of Operations to exercise the KV
// api. It returns the resulting log of inputs and outputs.
func RunNemeses(
	ctx context.Context, rng *rand.Rand, db *client.DB, config StepperConfig,
) ([]Step, *Engine, error) {
	const concurrency, numSteps = 5, 30

	var mu struct {
		syncutil.Mutex
		s *Stepper
	}
	mu.s = MakeStepper(config)
	a := MakeApplier(db)
	r, err := MakeResulter(ctx, db, mu.s.DataSpan())
	if err != nil {
		return nil, nil, err
	}
	defer r.Close()

	var stepsStartedAtomic int64
	stepCh := make(chan *Step, numSteps)

	workerFn := func(ctx context.Context, workerIdx int) error {
		workerName := fmt.Sprintf(`%d`, workerIdx)
		var step *Step
		var buf strings.Builder
		for atomic.AddInt64(&stepsStartedAtomic, 1) <= numSteps {
			func() {
				mu.Lock()
				defer mu.Unlock()
				next := mu.s.RandStep(rng, step)
				step = &next
			}()
			if err := a.Apply(ctx, step); err != nil {
				buf.Reset()
				step.format(&buf, `  `+workerName+` ERRORED `)
				log.Infof(ctx, "error: %+v\n\n%s", err, buf.String())
				return err
			}
			buf.Reset()
			step.format(&buf, `  `+workerName+` OPERATION `)
			log.Info(ctx, buf.String())
			stepCh <- step
		}
		return nil
	}
	if err := ctxgroup.GroupWorkers(ctx, concurrency, workerFn); err != nil {
		return nil, nil, err
	}
	close(stepCh)

	var steps []Step
	for step := range stepCh {
		steps = append(steps, *step)
	}
	kvs, err := r.KVs(ctx, steps)
	if err != nil {
		return nil, nil, err
	}

	return steps, kvs, nil
}

// Resulter tracks the kv changes that happen over a span.
type Resulter struct {
	db          *client.DB
	dataWatcher *Watcher
}

// MakeResulter returns a Resulter for the given span. All changes that happen
// between when this method returns and when Finish is first called are
// guaranteed to be captured.
func MakeResulter(ctx context.Context, db *client.DB, dataSpan roachpb.Span) (*Resulter, error) {
	r := &Resulter{db: db}
	var err error
	if r.dataWatcher, err = Watch(ctx, db, dataSpan); err != nil {
		return nil, err
	}
	return r, nil
}

// Close ensures that the Resulter is torn down. It may be called multiple times.
func (r *Resulter) Close() {
	_ = r.dataWatcher.Finish()
}

// KVs waits for the kvs written by the given steps to be captured, then returns
// these kvs.
func (r *Resulter) KVs(ctx context.Context, steps []Step) (*Engine, error) {
	if len(steps) == 0 {
		return nil, nil
	}
	lastStep := steps[len(steps)-1]

	// TODO(dan): Also slurp the splits. The meta ranges use expiration based
	// leases, so we can't use RangeFeed to do it. Maybe ExportRequest?

	if err := r.dataWatcher.WaitForFrontier(ctx, lastStep.After); err != nil {
		return nil, errors.Wrap(err, `waiting for data watcher`)
	}
	kvs := r.dataWatcher.Finish()

	return kvs, nil
}
