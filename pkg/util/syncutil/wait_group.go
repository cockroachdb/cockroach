// Copyright 2017 The Cockroach Authors.
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
// implied.  See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Nikhil Benesch (nikhil.benesch@gmail.com)

package syncutil

import (
	"time"

	"golang.org/x/net/context"
	"golang.org/x/sync/errgroup"
)

// NewErrGroupWithProgress creates a new ErrGroupWithProgress and context
// derived from ctx.
func NewErrGroupWithProgress(ctx context.Context) (*ErrGroupWithProgress, context.Context) {
	g, ctx := errgroup.WithContext(ctx)
	return &ErrGroupWithProgress{
		Group: g,
		stop:  make(chan bool),
		done:  make(chan bool),
	}, ctx
}

// ErrGroupWithProgress is an errgroup.Group that will periodically run
// callbacks while the wait group is running.
type ErrGroupWithProgress struct {
	*errgroup.Group
	stop chan bool
	done chan bool
	n    int
}

// Every runs callback fn while the wait group is running at the specified
// interval. fn will be run at least twice: once soon after Every is called, and
// once after the wait group is complete but before Wait returns.
func (g *ErrGroupWithProgress) Every(interval time.Duration, fn func()) {
	g.n++
	go func() {
		for {
			fn()
			select {
			case <-time.After(interval):
			case <-g.stop:
				fn()
				g.done <- true
				return
			}
		}
	}()
}

// Wait blocks until all function calls from the Go method have returned,
// guarantees all callbacks installed with Every run at least once more, then
// returns the first non-nil error (if any).
func (g *ErrGroupWithProgress) Wait() error {
	err := g.Group.Wait()
	for i := 0; i < g.n; i++ {
		g.stop <- true
	}
	for i := 0; i < g.n; i++ {
		<-g.done
	}
	return err
}
