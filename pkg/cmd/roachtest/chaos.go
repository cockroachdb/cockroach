// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// ChaosTimer configures a chaos schedule.
type ChaosTimer interface {
	Timing() (time.Duration, time.Duration)
}

// Periodic is a chaos timing using fixed durations.
type Periodic struct {
	Period, DownTime time.Duration
}

// Timing implements ChaosTimer.
func (p Periodic) Timing() (time.Duration, time.Duration) {
	return p.Period, p.DownTime
}

// Chaos stops and restarts nodes in a cluster.
type Chaos struct {
	// Timing is consulted before each chaos event. It provides the duration of
	// the downtime and the subsequent chaos-free duration.
	Timer ChaosTimer
	// Target is consulted before each chaos event to determine the node(s) which
	// should be killed.
	Target func() nodeListOption
	// Stopper is a channel that the chaos agent listens on. The agent will
	// terminate cleanly once it receives on the channel.
	Stopper <-chan time.Time
	// DrainAndQuit is used to determine if want to kill the node vs draining it
	// first and shutting down gracefully.
	DrainAndQuit bool
}

// Runner returns a closure that runs chaos against the given cluster without
// setting off the monitor. The process returns without an error after the chaos
// duration.
func (ch *Chaos) Runner(c *cluster, m *monitor) func(context.Context) error {
	return func(ctx context.Context) (err error) {
		l, err := c.l.ChildLogger("CHAOS")
		if err != nil {
			return err
		}
		defer func() {
			l.Printf("chaos stopping: %v", err)
		}()
		t := timeutil.Timer{}
		{
			p, _ := ch.Timer.Timing()
			t.Reset(p)
		}
		for {
			select {
			case <-ch.Stopper:
				return nil
			case <-ctx.Done():
				return ctx.Err()
			case <-t.C:
				t.Read = true
			}

			period, downTime := ch.Timer.Timing()

			target := ch.Target()
			m.ExpectDeath()

			if ch.DrainAndQuit {
				l.Printf("stopping and draining %v\n", target)
				if err := c.StopE(ctx, target, stopArgs("--sig=15")); err != nil {
					return errors.Wrapf(err, "could not stop node %s", target)
				}
			} else {
				l.Printf("killing %v\n", target)
				if err := c.StopE(ctx, target); err != nil {
					return errors.Wrapf(err, "could not stop node %s", target)
				}
			}

			select {
			case <-ch.Stopper:
				// NB: the roachtest harness checks that at the end of the test,
				// all nodes that have data also have a running process.
				l.Printf("restarting %v (chaos is done)\n", target)
				if err := c.StartE(ctx, target); err != nil {
					return errors.Wrapf(err, "could not restart node %s", target)
				}
				return nil
			case <-ctx.Done():
				// NB: the roachtest harness checks that at the end of the test,
				// all nodes that have data also have a running process.
				l.Printf("restarting %v (chaos is done)\n", target)
				// Use a one-off context to restart the node because ours is
				// already canceled.
				tCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := c.StartE(tCtx, target); err != nil {
					return errors.Wrapf(err, "could not restart node %s", target)
				}
				return ctx.Err()
			case <-time.After(downTime):
			}
			l.Printf("restarting %v after %s of downtime\n", target, downTime)
			t.Reset(period)
			if err := c.StartE(ctx, target); err != nil {
				return errors.Wrapf(err, "could not restart node %s", target)
			}
		}
	}
}
