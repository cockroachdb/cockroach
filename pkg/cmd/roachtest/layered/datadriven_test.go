// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package layered_test

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/layered"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

type TestStep struct {
	layered.AtLeastV21Dot2MixedSupporter
	started bool

	Name   string
	Sleep  int // ticks, has to be > 0
	Output string
	Result interface{}
}

func (ts *TestStep) String() string {
	return ts.Name
}

func (ts *TestStep) Owner() registry.Owner {
	return registry.OwnerTestEng
}

func (ts *TestStep) Idempotent() bool {
	return false
}

func (ts *TestStep) Run(_ context.Context, _ *rand.Rand, t layered.Fataler, _ layered.SeqEnv) {
	if ts.started {
		t.Fatalf("already running")
	}
	ts.started = true
}

func makeTestSeq() (_ []layered.WeightedSequence, totalNumSteps int) {
	const stepsPerSeq = 5
	seqNames := []string{"import", "kv0", "tpcc", "ycsb", "AOST"}
	var seqs []layered.WeightedSequence
	for _, name := range seqNames {
		seq := &layered.SliceSequence{Name: name}
		for i := 0; i < stepsPerSeq; i++ {
			sleep := 1 + ((2*i + 3) % 5)
			stepName := fmt.Sprintf("%s.s%d", name, i+1)
			seq.Items = append(seq.Items, &TestStep{
				Name:   stepName,
				Sleep:  sleep,
				Output: fmt.Sprintf("%s: done after %d ticks", stepName, sleep),
			})
		}
		seqs = append(seqs, layered.WeightedSequence{Weight: 1, Seq: seq})
	}
	return seqs, len(seqNames) * stepsPerSeq
}

func makeTestSeqsWithFailure() (_ []layered.WeightedSequence, totalNumSteps int) {
	seqs, numSteps := makeTestSeq()
	seq := layered.SliceSequence{
		Name: "explody",
	}
	seq.Items = append(seq.Items, &TestStep{
		Name:   "explody.passing",
		Output: "hello",
		Sleep:  3,
	}, &TestStep{
		Name:   "explody.failing",
		Output: "",
		Sleep:  1,
		Result: errors.New("boom!"),
	}, &TestStep{
		Name:   "explody.unreachable",
		Output: "never seen",
		Sleep:  4,
	},
	)
	return append(seqs, layered.WeightedSequence{Seq: &seq, Weight: 1}), numSteps + 2
}

type testEnv struct {
	tick      *int
	rec       *layered.UMLRecorder
	seqs      []layered.WeightedSequence
	sch       *layered.Scheduler
	stepsDone *int
}

func makeTestScheduler(t *testing.T, seqs ...layered.WeightedSequence) *testEnv {
	var tick int
	var rec layered.UMLRecorder
	var stepsDone int
	sch := layered.Scheduler{SchedOptions: layered.SchedOptions{
		Sequences:   seqs,
		Concurrency: 3,
		EventHandler: func(ctx context.Context, ev layered.Event) {
			t.Helper()
			t.Logf("t=%d: %#v", tick, ev)
			switch sev := ev.(type) {
			case *layered.EvStepStart:
				rec.Record(time.Unix(int64(tick), 0), strconv.Itoa(sev.Worker+1), sev.S)
			case *layered.EvStepDone:
				// NB: the recorder methods would be less buggy if they were "last write wins"
				// at conflicting (worker,timestamp) pairs.
				stepsDone++
				rec.Idle(time.Unix(int64(tick), 0), strconv.Itoa(sev.Worker+1))
			}
		},
		Rand: rand.New(rand.NewSource(0)),
	}}
	return &testEnv{seqs: seqs, tick: &tick, rec: &rec, sch: &sch, stepsDone: &stepsDone}
}

func (e *testEnv) run(ctx context.Context, t *testing.T) {
	for {
		for {
			step, _, err := e.sch.Get(ctx)
			if errors.Is(err, layered.ErrDone) {
				// Ran to completion.
				return
			}
			if errors.Is(err, layered.ErrWorkersBusy) || errors.Is(err, layered.ErrSequencesBusy) {
				// Can't schedule new work right now, so break out of the loop
				// and finish some work below.
				break
			}
			require.NoError(t, err)
			step.Run(ctx, nil, nil, nil)
		}

		var ticked bool
		for _, ws := range e.seqs {
			for _, step := range ws.Seq.(*layered.SliceSequence).Items {
				s := step.(*TestStep)
				if !s.started {
					continue
				}
				if s.Sleep == 0 {
					// Step is finished.
					continue
				}
				s.Sleep--
				if !ticked {
					ticked = true
					// NB: we tick before event handling so that the timestamps
					// match up.
					*e.tick++
					t.Logf("tick=%d", *e.tick)
				}
				if s.Sleep == 0 {
					e.sch.EventHandler(ctx, &layered.EvUnstructured{Message: s.Output})
					e.sch.StepDone(ctx, s, s.Result)
				}
			}
		}
	}
}

func TestSchedulerBasic(t *testing.T) {
	seqs, numSteps := makeTestSeq()
	e := makeTestScheduler(t, seqs...)

	defer func() {
		t.Log(e.rec.String())
		if t.Failed() {
			return
		}
		echotest.Require(t, e.rec.String(), testutils.TestDataPath(t, t.Name()+".txt"))
	}()

	ctx := context.Background()
	e.run(ctx, t)
	require.Equal(t, numSteps, *e.stepsDone)
}

func TestSchedulerFailingStep(t *testing.T) {
	seqs, numSteps := makeTestSeqsWithFailure()
	e := makeTestScheduler(t, seqs...)

	defer func() {
		t.Log(e.rec.String())
		if t.Failed() {
			return
		}
		echotest.Require(t, e.rec.String(), testutils.TestDataPath(t, t.Name()+".txt"))
	}()

	e.run(context.Background(), t)
	require.Equal(t, numSteps, *e.stepsDone)
}

func TestRun(t *testing.T) {
	ctx := context.Background()
	seqs, numSteps := makeTestSeq()
	e := makeTestScheduler(t, seqs...)
	origEvHandler := e.sch.EventHandler
	e.sch.EventHandler = func(ctx context.Context, ev layered.Event) {
		*e.tick++
		origEvHandler(ctx, ev)
	}
	require.Nil(t, layered.Run(ctx, e.sch, nil))
	{
		_, _, err := e.sch.Get(ctx)
		require.True(t, errors.Is(err, layered.ErrDone), "%+v", err)
	}
	require.Equal(t, numSteps, *e.stepsDone)
	t.Log(e.rec.String())
}
