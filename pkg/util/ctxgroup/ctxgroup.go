// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

/*
Package ctxgroup wraps golang.org/x/sync/errgroup with a context func.

This package extends and modifies the errgroup API slightly to
make context variables more explicit. WithContext no longer returns
a context. Instead, the GoCtx method explicitly passes one to the
invoked func. The goal is to make misuse of context vars with errgroups
more difficult. Example usage:

	ctx := context.Background()
	g := ctxgroup.WithContext(ctx)
	ch := make(chan bool)
	g.GoCtx(func(ctx context.Context) error {
		defer close(ch)
		for _, val := range []bool{true, false} {
			select {
			case ch <- val:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	g.GoCtx(func(ctx context.Context) error {
		for val := range ch {
			if err := api.Call(ctx, val); err != nil {
				return err
			}
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return err
	}
	api.Call(ctx, "done")

# Problems with errgroup

The bugs this package attempts to prevent are: misuse of shadowed
ctx variables after errgroup closure and confusion in the face of
multiple ctx variables when trying to prevent shadowing. The following
are all example bugs that Cockroach has had during its use of errgroup:

	ctx := context.Background()
	g, ctx := errgroup.WithContext(ctx)
	ch := make(chan bool)
	g.Go(func() error {
		defer close(ch)
		for _, val := range []bool{true, false} {
			select {
			case ch <- val:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	g.Go(func() error {
		for val := range ch {
			if err := api.Call(ctx, val); err != nil {
				return err
			}
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return err
	}
	api.Call(ctx, "done")

The ctx used by the final api.Call is already closed because the
errgroup has returned. This happened because of the desire to not
create another ctx variable, and so we shadowed the original ctx var,
but then incorrectly continued to use it after the errgroup had closed
its context. So we make a modification and create new gCtx variable
that doesn't shadow the original ctx:

	ctx := context.Background()
	g, gCtx := errgroup.WithContext(ctx)
	ch := make(chan bool)
	g.Go(func() error {
		defer close(ch)
		for _, val := range []bool{true, false} {
			select {
			case ch <- val:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	})
	g.Go(func() error {
		for val := range ch {
			if err := api.Call(ctx, val); err != nil {
				return err
			}
		}
		return nil
	})
	if err := g.Wait(); err != nil {
		return err
	}
	api.Call(ctx, "done")

Now the final api.Call is correct. But the other api.Call is incorrect
and the ctx.Done receive is incorrect because they are using the wrong
context and thus won't correctly exit early if the errgroup needs to
exit early.
*/
package ctxgroup

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"golang.org/x/sync/errgroup"
)

// Group wraps errgroup.
type Group struct {
	wrapped *errgroup.Group
	ctx     context.Context
	panicMu *recovered
}

type recovered struct {
	syncutil.Mutex
	payload error
}

// Wait blocks until all function calls from the Go method have returned, then
// returns the first non-nil error (if any) from them. If Wait() is invoked
// after the context (originally supplied to WithContext) is canceled, Wait
// returns an error, even if no Go invocation did. In particular, calling
// Wait() after Done has been closed is guaranteed to return an error.
func (g Group) Wait() error {
	if g.ctx == nil {
		return nil
	}
	ctxErr := g.ctx.Err()
	err := g.wrapped.Wait()

	if g.panicMu.payload != nil {
		panic(g.panicMu.payload)
	}

	if err != nil {
		return err
	}
	return ctxErr
}

// WithContext returns a new Group and an associated Context derived from ctx.
func WithContext(ctx context.Context) Group {
	grp, ctx := errgroup.WithContext(ctx)
	return Group{
		wrapped: grp,
		ctx:     ctx,
		panicMu: &recovered{},
	}
}

// SetLimit limits the number of active goroutines in this group to at most n. A
// negative value indicates no limit. A limit of zero will prevent any new
// goroutines from being added. Any subsequent call to the Go method will block
// until it can add an active goroutine without exceeding the configured limit.
// The limit must not be modified while any goroutines in the group are active.
// This delegates to errgroup.Group.SetLimit.
func (g Group) SetLimit(n int) {
	g.wrapped.SetLimit(n)
}

// Go calls the given function in a new goroutine.
func (g Group) Go(f func() error) {
	g.wrapped.Go(f)
}

// GoCtx calls the given function in a new goroutine. If the function passed
// panics, the shared context is cancelled and then the subsequent call to Wait
// will panic with the original panic payload wrapped in a Panic error.
// If multiple tasks in the group panic, their panic errors are combined and
// thrown as a single panic in Wait().
func (g Group) GoCtx(f func(ctx context.Context) error) {
	g.wrapped.Go(func() (err error) {
		defer func() {
			if r := recover(); r != nil {
				err = wrapPanic(1, r)
				g.panicMu.Lock()
				defer g.panicMu.Unlock()
				g.panicMu.payload = errors.CombineErrors(g.panicMu.payload, err)
			}
		}()
		return f(g.ctx)
	})
}

// GroupWorkers runs num worker go routines in an errgroup.
func GroupWorkers(ctx context.Context, num int, f func(context.Context, int) error) error {
	group := WithContext(ctx)
	for i := 0; i < num; i++ {
		workerID := i
		group.GoCtx(func(ctx context.Context) error { return f(ctx, workerID) })
	}
	return group.Wait()
}

// GoAndWait calls the given functions each in a new goroutine. It then Waits
// for them to finish. This is intended to help prevent bugs caused by returning
// early after running some goroutines but before Waiting for them to complete.
func GoAndWait(ctx context.Context, fs ...func(ctx context.Context) error) error {
	group := WithContext(ctx)
	for _, f := range fs {
		if f != nil {
			group.GoCtx(f)
		}
	}
	return group.Wait()
}

// wrapPanic turns r into an error if it is not one already.
//
// TODO(dt): consider raw recovered payload as well.
//
// TODO(dt): replace this with logcrash.PanicAsError after moving that to a new
// standalone pkg (since we cannot depend on `util/log` here) and teaching it to
// preserve the original payload.
func wrapPanic(depth int, r interface{}) error {
	if err, ok := r.(error); ok {
		return errors.WithStackDepth(err, depth+1)
	}
	return errors.NewWithDepthf(depth+1, "panic: %v", r)
}
