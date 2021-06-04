// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package layered

import (
	"context"
	gosql "database/sql"
	"fmt"
	"math/rand"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// Env is a handle to a running CockroachDB cluster
// that Stepper can run against.
//
// This is a placeholder - *cluster is to be polished and
// exposed through an interface which will take the place
// of this struct. Initially we will try
// to port it more or less verbatim but over time we should
// also aim to improve how tests orchestrate CRDB and events
// against it. It should be straightforward to select nodes
// by version or location, steps shouldn't get to upload
// binaries or to otherwise poke around, at least not by
// default.
type Env interface {
	// InvokeWorkload invokes ./cockroach workload <args> on a suitable workload
	// node and points it at the nodes in the cluster.
	InvokeWorkload(ctx context.Context, t Fataler, args ...string)
	// Conn returns a SQL connection to a healthy node in the cluster. Absent
	// unforeseen problems, the Conn will be healthy for the duration of the Step,
	// i.e. the test harness will not shut down a node that has active Conn()s.
	Conn() *gosql.DB
}

type SeqEnv interface {
	Env
	// Unique suffixes the string such that it becomes unique across
	// all prior calls to Unique in the surrounding Schedule. It's a convenient
	// way to pick table names, etc, that will never conflict with that chosen
	// by prior or concurrent Steps. The result is often stored in State().
	Unique(string) string
	// State is a map that is passed from one Step in a Sequence to the next, to
	// allow for transfer of information (such as Unique() table names) between
	// steps.
	State() map[string]interface{}
}

type Fataler interface {
	Errorf(string, ...interface{})
	Fatalf(string, ...interface{})
	FailNow()
}

type workerState struct {
	// Current work, if any.
	// May be nil, but only jointly.
	seq  *WeightedSequence
	step Step
}

type WeightedSequence struct {
	Seq    Sequence
	Weight float64
}

type SchedOptions struct {
	Sequences    []WeightedSequence
	Concurrency  int
	EventHandler func(context.Context, Event)
	Rand         *rand.Rand
}

type Scheduler struct {
	SchedOptions

	ws []*workerState

	available []*WeightedSequence
}

func (s *Scheduler) Get(ctx context.Context) (_ Step, seq fmt.Stringer, _ error) {
	if s.ws == nil {
		// Initial setup on first call.
		s.ws = make([]*workerState, s.Concurrency)
		for i := range s.ws {
			s.ws[i] = &workerState{}
		}
		for _, seq := range s.Sequences {
			seq := seq
			s.available = append(s.available, &seq)
		}
	}

	for {
		wIdx, seq, err := s.takeSeq(ctx)
		if err != nil {
			return nil, nil, err
		}
		step, ok := seq.Seq.Next(s.Rand)
		if !ok {
			s.EventHandler(ctx, &EvSeqExhausted{Seq: seq.Seq.String()})
			log.Eventf(ctx, "seq %s is exhausted", seq.Seq.String())
			continue
		}
		s.ws[wIdx] = &workerState{
			seq:  seq,
			step: step,
		}
		s.EventHandler(ctx, &EvStepStart{
			Worker: wIdx,
			Seq:    seq.Seq.String(),
			S:      step.String(),
		})
		return step, seq.Seq, nil
	}
}

func (s *Scheduler) takeSeq(ctx context.Context) (workedIdx int, _ *WeightedSequence, _ error) {
	// Find an idle concurrency slot.
	wIdx := func() int {
		for idx, w := range s.ws {
			if w.seq == nil {
				return idx
			}
		}
		return -1
	}()
	n := len(s.available)
	if wIdx < 0 {
		return 0, nil, ErrWorkersBusy
	}
	if n == 0 {
		for _, w := range s.ws {
			if w.seq != nil {
				// There's a running worker, so we have to
				// wait for it to terminate and only then
				// will we either be able to start new work
				// or realize there is no more work to be
				// done.
				return 0, nil, ErrSequencesBusy
			}
		}
		// All workers are idle and there are no runnable sequences.
		return 0, nil, ErrDone
	}

	sumWeights := make([]float64, n)
	names := make([]string, n)
	for i, seq := range s.available {
		sumWeights[i] = seq.Weight
		if i > 0 {
			sumWeights[i] += sumWeights[i-1]
		}
		names[i] = seq.Seq.String()
	}

	s.EventHandler(ctx, &EvUnstructured{Worker: wIdx, Message: fmt.Sprintf("picking a sequence from %v", names)})
	dart := s.Rand.Float64() * sumWeights[n-1] // throw a dart at [0, cumulativeWeight)
	for i := range sumWeights {
		var lo float64
		if i > 0 {
			lo = sumWeights[i-1]
		}
		hi := sumWeights[i]
		if lo <= dart && dart < hi {
			// Swap chosen sequence to the end of the slice.
			s.available[i], s.available[n-1] = s.available[n-1], s.available[i]
			seq := s.available[n-1]
			// Shorten slice to remove chosen seq.
			s.available = s.available[:n-1]
			return wIdx, seq, nil
		}
	}
	return 0, nil, errors.New("did not pick a sequence") // unreachable assuming no bugs
}

func (s *Scheduler) StepDone(ctx context.Context, step Step, result interface{}) {
	for wIdx, w := range s.ws {
		if w.step == step {
			seq := w.seq
			w.seq, w.step = nil, nil
			if result == nil {
				s.available = append(s.available, seq)
			}
			s.EventHandler(ctx, &EvStepDone{
				Worker: wIdx,
				Seq:    seq.Seq.String(), S: step.String(),
				// TODO(tbg): when we get more refined about resilient sequences, then we
				// won't always have to fail the sequence.
				SequenceFailed: result != nil,
				Result:         result,
			})
		}
	}
}

type stepFataler struct {
	ctx context.Context
}

func (t *stepFataler) Errorf(format string, args ...interface{}) {
	log.VEventfDepth(t.ctx, 2, 1, format, args...)
}

func (t *stepFataler) Fatalf(format string, args ...interface{}) {
	log.VEventfDepth(t.ctx, 2, 1, format, args...)
	panic(fmt.Sprintf(format, args...))
}

func (t *stepFataler) FailNow() {
	panic("failed")
}
