// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// Package ctxgroup wraps golang.org/x/sync/errgroup with a context func.
//
// This package extends and modifies the errgroup API slightly to make context
// variables more explicit. WithContext no longer returns a context. Instead,
// the GoCtx method explicitly passes one to the invoked func. The goal is
// to make misuse of context vars with errgroups more difficult.
package ctxgroup

import (
	"context"

	"golang.org/x/sync/errgroup"
)

// Group wraps errgroup.
type Group struct {
	*errgroup.Group
	Ctx context.Context
	// Done is set to Ctx.Done().
	Done <-chan struct{}
}

// WithContext returns a new Group and an associated Context derived from ctx.
func WithContext(ctx context.Context) Group {
	grp, ctx := errgroup.WithContext(ctx)
	return Group{
		Group: grp,
		Ctx:   ctx,
		Done:  ctx.Done(),
	}
}

// GoCtx calls the given function in a new goroutine.
func (g Group) GoCtx(f func(ctx context.Context) error) {
	g.Group.Go(func() error {
		return f(g.Ctx)
	})
}

// GroupWorkers runs num worker go routines in an errgroup.
func GroupWorkers(ctx context.Context, num int, f func(context.Context) error) error {
	group := WithContext(ctx)
	for i := 0; i < num; i++ {
		group.GoCtx(f)
	}
	return group.Wait()
}
