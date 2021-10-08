// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
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
	Target func() option.NodeListOption
	// Stopper is a channel that the chaos agent listens on. The agent will
	// terminate cleanly once it receives on the channel.
	Stopper <-chan time.Time
	// DrainAndQuit is used to determine if want to kill the node vs draining it
	// first and shutting down gracefully.
	DrainAndQuit bool
	// ChaosEventCh is a channel that the chaos runner will send events on when
	// the runner performs an action.
	// Chaos is responsible for closing the channel when the test is over.
	// This is optional.
	ChaosEventCh chan ChaosEvent
}

// ChaosEventType signifies an event that occurs during chaos.
type ChaosEventType uint64

const (
	// ChaosEventTypePreShutdown signifies a shutdown on target(s) is about to happen.
	ChaosEventTypePreShutdown ChaosEventType = iota
	// ChaosEventTypeShutdownComplete signifies that the target(s) have shutdown.
	ChaosEventTypeShutdownComplete
	// ChaosEventTypePreStartup signifies the target(s) is about to be restarted.
	ChaosEventTypePreStartup
	// ChaosEventTypeStartupComplete signifies the target(s) have restarted.
	ChaosEventTypeStartupComplete

	// ChaosEventTypeStart signifies the chaos runner has started.
	ChaosEventTypeStart
	// ChaosEventTypeEnd signifies the chaos runner has ended.
	ChaosEventTypeEnd
)

// ChaosEvent is an event which happens during chaos running.
type ChaosEvent struct {
	Type   ChaosEventType
	Target option.NodeListOption
	Time   time.Time
}

func (ch *Chaos) sendEvent(t ChaosEventType, target option.NodeListOption) {
	if ch.ChaosEventCh != nil {
		ch.ChaosEventCh <- ChaosEvent{
			Type:   t,
			Target: target,
			Time:   timeutil.Now(),
		}
	}
}

// Runner returns a closure that runs chaos against the given cluster without
// setting off the monitor. The process returns without an error after the chaos
// duration.
func (ch *Chaos) Runner(
	c cluster.Cluster, t test.Test, m cluster.Monitor,
) func(context.Context) error {
	return func(ctx context.Context) (err error) {
		l, err := t.L().ChildLogger("CHAOS")
		if err != nil {
			return err
		}
		defer func() {
			ch.sendEvent(ChaosEventTypeEnd, nil)
			if ch.ChaosEventCh != nil {
				close(ch.ChaosEventCh)
			}
			l.Printf("chaos stopping: %v", err)
		}()
		t := timeutil.Timer{}
		{
			p, _ := ch.Timer.Timing()
			t.Reset(p)
		}
		ch.sendEvent(ChaosEventTypeStart, nil)
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
			m.ExpectDeaths(int32(len(target)))

			ch.sendEvent(ChaosEventTypePreShutdown, target)
			if ch.DrainAndQuit {
				l.Printf("stopping and draining %v\n", target)
				if err := c.StopE(ctx, target, option.StopArgs("--sig=15"), option.WithWorkerAction()); err != nil {
					return errors.Wrapf(err, "could not stop node %s", target)
				}
			} else {
				l.Printf("killing %v\n", target)
				if err := c.StopE(ctx, target, option.WithWorkerAction()); err != nil {
					return errors.Wrapf(err, "could not stop node %s", target)
				}
			}
			ch.sendEvent(ChaosEventTypeShutdownComplete, target)

			select {
			case <-ch.Stopper:
				// NB: the roachtest harness checks that at the end of the test,
				// all nodes that have data also have a running process.
				l.Printf("restarting %v (chaos is done)\n", target)
				if err := c.StartE(ctx, target, option.WithWorkerAction()); err != nil {
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
				if err := c.StartE(tCtx, target, option.WithWorkerAction()); err != nil {
					return errors.Wrapf(err, "could not restart node %s", target)
				}
				return ctx.Err()
			case <-time.After(downTime):
			}
			l.Printf("restarting %v after %s of downtime\n", target, downTime)
			t.Reset(period)
			ch.sendEvent(ChaosEventTypePreStartup, target)
			if err := c.StartE(ctx, target, option.WithWorkerAction()); err != nil {
				return errors.Wrapf(err, "could not restart node %s", target)
			}
			ch.sendEvent(ChaosEventTypeStartupComplete, target)
		}
	}
}
