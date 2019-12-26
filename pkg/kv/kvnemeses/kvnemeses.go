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
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

// RunNemeses generates and applies a series of Operations to exercise the KV
// api. It returns the resulting log of inputs and outputs.
func RunNemeses(
	ctx context.Context,
	rng *rand.Rand,
	db *client.DB,
	ct ClosedTimestampTargetInterval,
	config StepperConfig,
) ([]error, error) {
	const concurrency, numSteps = 5, 30

	var mu struct {
		syncutil.Mutex
		s *Stepper
	}
	mu.s = MakeStepper(config)
	a := MakeApplier(db)
	r, err := MakeResulter(ctx, db, ct, StepperDataSpan())
	if err != nil {
		return nil, err
	}
	defer r.Close()

	var stepsStartedAtomic int64
	stepsByWorker := make([][]Step, concurrency)

	workerFn := func(ctx context.Context, workerIdx int) error {
		workerName := fmt.Sprintf(`%d`, workerIdx)
		var buf strings.Builder
		for atomic.AddInt64(&stepsStartedAtomic, 1) <= numSteps {
			var step Step
			func() {
				mu.Lock()
				defer mu.Unlock()
				step = mu.s.RandStep(rng)
			}()
			if err := a.Apply(ctx, &step); err != nil {
				buf.Reset()
				step.format(&buf, formatCtx{receiver: `db`, indent: `  ` + workerName + ` ERRORED `})
				log.Infof(ctx, "error: %+v\n\n%s", err, buf.String())
				return err
			}
			buf.Reset()
			step.format(&buf, formatCtx{receiver: `db`, indent: `  ` + workerName + ` OPERATION `})
			log.Info(ctx, buf.String())
			stepsByWorker[workerIdx] = append(stepsByWorker[workerIdx], step)
		}
		return nil
	}
	if err := ctxgroup.GroupWorkers(ctx, concurrency, workerFn); err != nil {
		return nil, err
	}

	allSteps := make([]Step, 0, numSteps)
	for _, steps := range stepsByWorker {
		allSteps = append(allSteps, steps...)
	}

	kvs, err := r.KVs(ctx, allSteps)
	if err != nil {
		return nil, err
	}
	failures := Validate(allSteps, kvs)

	if len(failures) > 0 {
		log.Infof(ctx, "reproduction steps:\n%s", printRepro(stepsByWorker))
		log.Infof(ctx, "kvs (recorded from rangefeed):\n%s", kvs.DebugPrint("  "))

		span := StepperDataSpan()
		scanKVs, err := db.Scan(ctx, span.Key, span.EndKey, -1)
		if err != nil {
			log.Infof(ctx, "could not scan actual latest values: %+v", err)
		} else {
			var kvsBuf strings.Builder
			for _, kv := range scanKVs {
				fmt.Fprintf(&kvsBuf, "  %s %s -> %s\n", kv.Key, kv.Value.Timestamp, kv.Value.PrettyPrint())
			}
			log.Infof(ctx, "kvs (scan of latest values according to crdb):\n%s", kvsBuf.String())
		}
	}

	return failures, nil
}

// Resulter tracks the kv changes that happen over a span.
type Resulter struct {
	db          *client.DB
	dataWatcher *Watcher
}

// MakeResulter returns a Resulter for the given span. All changes that happen
// between when this method returns and when Finish is first called are
// guaranteed to be captured.
func MakeResulter(
	ctx context.Context, db *client.DB, ct ClosedTimestampTargetInterval, dataSpan roachpb.Span,
) (*Resulter, error) {
	r := &Resulter{db: db}
	var err error
	if r.dataWatcher, err = Watch(ctx, db, ct, dataSpan); err != nil {
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
	var latestAfter hlc.Timestamp
	for _, step := range steps {
		latestAfter.Forward(step.After)
	}

	// TODO(dan): Also slurp the splits. The meta ranges use expiration based
	// leases, so we can't use RangeFeed to do it. Maybe ExportRequest?

	if err := r.dataWatcher.WaitForFrontier(ctx, latestAfter); err != nil {
		return nil, errors.Wrap(err, `waiting for data watcher`)
	}
	kvs := r.dataWatcher.Finish()

	return kvs, nil
}

func printRepro(stepsByWorker [][]Step) string {
	// TODO(dan): Make this more copy and paste, especially the error handling.
	var buf strings.Builder
	buf.WriteString("g := ctxgroup.WithContext(ctx)\n")
	for _, steps := range stepsByWorker {
		buf.WriteString("g.GoCtx(func(ctx context.Context) error {")
		for _, step := range steps {
			step.format(&buf, formatCtx{receiver: `db`, indent: "  "})
		}
		buf.WriteString("\n  return nil\n")
		buf.WriteString("})\n")
	}
	buf.WriteString("g.Wait()\n")
	return buf.String()
}
