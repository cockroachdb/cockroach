// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rpc

import (
	"context"
	"time"

	"github.com/cenkalti/backoff"
	circuit "github.com/cockroachdb/circuitbreaker"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/facebookgo/clock"
)

const maxBackoff = time.Second

// breakerClock is an implementation of clock.Clock that internally uses an
// hlc.Clock. It is used to bridge the hlc clock to the circuit breaker
// clocks. Note that it only implements the After() and Now() methods needed by
// circuit breakers and backoffs.
type breakerClock struct {
	clock *hlc.Clock
}

func (c *breakerClock) After(d time.Duration) <-chan time.Time {
	return time.After(d)
}

func (c *breakerClock) AfterFunc(d time.Duration, f func()) *clock.Timer {
	panic("unimplemented")
}

func (c *breakerClock) Now() time.Time {
	return c.clock.PhysicalTime()
}

func (c *breakerClock) Sleep(d time.Duration) {
	panic("unimplemented")
}

func (c *breakerClock) Tick(d time.Duration) <-chan time.Time {
	panic("unimplemented")
}

func (c *breakerClock) Ticker(d time.Duration) *clock.Ticker {
	panic("unimplemented")
}

func (c *breakerClock) Timer(d time.Duration) *clock.Timer {
	panic("unimplemented")
}

var _ clock.Clock = &breakerClock{}

// newBackOff creates a new exponential backoff properly configured for RPC
// connection backoff.
func newBackOff(clock backoff.Clock) backoff.BackOff {
	// This exponential backoff limits the circuit breaker to 1 second
	// intervals between successive attempts to resolve a node address
	// and connect via GRPC.
	//
	// NB (nota Ben): MaxInterval should be less than the Raft election timeout
	// (1.5s) to avoid disruptions. A newly restarted node will be in follower
	// mode with no knowledge of the Raft leader. If it doesn't hear from a
	// leader before the election timeout expires, it will start to campaign,
	// which can be disruptive. Therefore the leader needs to get in touch (via
	// Raft heartbeats) with such nodes within one election timeout of their
	// restart, which won't happen if their backoff is too high.
	b := &backoff.ExponentialBackOff{
		InitialInterval:     500 * time.Millisecond,
		RandomizationFactor: 0.5,
		Multiplier:          1.5,
		MaxInterval:         maxBackoff,
		MaxElapsedTime:      0,
		Clock:               clock,
	}
	b.Reset()
	return b
}

func newBreaker(ctx context.Context, name string, clock clock.Clock) *circuit.Breaker {
	return circuit.NewBreakerWithOptions(&circuit.Options{
		Name:       name,
		BackOff:    newBackOff(clock),
		Clock:      clock,
		ShouldTrip: circuit.ThresholdTripFunc(1),
		Logger:     breakerLogger{ctx},
	})
}

// breakerLogger implements circuit.Logger to expose logging from the
// circuitbreaker package. Debugf is logged with a vmodule level of 2 so to see
// the circuitbreaker debug messages set --vmodule=breaker=2
type breakerLogger struct {
	ctx context.Context
}

func (r breakerLogger) Debugf(format string, v ...interface{}) {
	if log.V(2) {
		log.Dev.InfofDepth(r.ctx, 1, format, v...)
	}
}

func (r breakerLogger) Infof(format string, v ...interface{}) {
	log.Ops.InfofDepth(r.ctx, 1, format, v...)
}
