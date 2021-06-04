package layered

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/logtags"
)

func Run(ctx context.Context, sch *Scheduler, env Env) error {
	// Make a sub-rand so that we're deterministically picking only a single
	// number off the generator, to preserve as much determinism as possible in
	// the scheduler itself. Of course there is still nondeterminism because the
	// order in which we signal steps as completed to the scheduler depends on
	// which of multiple concurrent steps finishes first, and this influences the
	// next emitted step.
	r := rand.New(rand.NewSource(sch.Rand.Int63()))

	closedCh := make(chan struct{})
	close(closedCh)
	finishedSteps := make(chan finishedStep)

	state := map[fmt.Stringer]map[string]interface{}{} // seq -> perSeqState
	var uniq uniquer

	maybeBlockCh := closedCh // nil if we have to wait for a step to finish, closed otherwise
	for {
		select {
		case sr := <-finishedSteps:
			sch.StepDone(ctx, sr.step, sr.result)
		case <-ctx.Done():
			return ctx.Err()
		case <-maybeBlockCh:
		}
		maybeBlockCh = closedCh

		step, seq, err := sch.Get(ctx)
		if errors.Is(err, ErrDone) {
			// Ran to completion.
			return nil
		}
		if errors.Is(err, ErrWorkersBusy) || errors.Is(err, ErrSequencesBusy) {
			// Wait for one of our inflight steps to finish, then try again.
			maybeBlockCh = nil
			continue
		}
		if err != nil {
			return err
		}
		go func(ctx context.Context, step Step) {
			defer func() {
				result := recover()
				select {
				case finishedSteps <- finishedStep{step: step, result: result}:
				case <-ctx.Done():
				}
			}()
			t := &stepFataler{ctx: logtags.AddTag(ctx, step.String(), nil)} // TODO what about seq name?

			if state[seq] == nil {
				state[seq] = map[string]interface{}{}
			}
			seqEnv := &seqScopedEnv{
				uniquer: &uniq,
				Env:     env,
				state:   state[seq],
			}

			step.Run(ctx, r, t, seqEnv)
		}(ctx, step)
	}
}

type uniquer int

func (u *uniquer) Unique(prefix string) string {
	*u++
	return fmt.Sprintf("%s_%d", prefix, *u)
}

type seqScopedEnv struct {
	*uniquer
	Env
	state map[string]interface{}
}

func (l *seqScopedEnv) State() map[string]interface{} {
	return l.state
}

type finishedStep struct {
	step   Step
	result interface{}
}
