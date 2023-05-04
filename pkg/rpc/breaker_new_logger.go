// Copyright 2023 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/util/circuit"
	"github.com/cockroachdb/errors"
)

type circuitBreakerLogger struct {
	wrapped circuit.EventHandler
}

func (b *circuitBreakerLogger) OnTrip(breaker *circuit.Breaker, prev, cur error) {
	if prev == nil && errors.Is(cur, ErrNotHeartbeated) {
		// The breaker is created in this tripped state, don't log this.
		return
	} else if errors.HasType(prev, cur) {
		// Last heartbeat failed similar to current one, don't repeatedly log.
		return
	}
	b.wrapped.OnTrip(breaker, prev, cur)
}

func (b *circuitBreakerLogger) OnProbeLaunched(breaker *circuit.Breaker) {
	// No-op. If we want logging, better do it directly from heartbeat loop.
}

func (b *circuitBreakerLogger) OnProbeDone(breaker *circuit.Breaker) {
	// No-op. If we want logging, better do it directly from heartbeat loop.
}

func (b *circuitBreakerLogger) OnReset(breaker *circuit.Breaker) {
	// No-op because we already log from the heartbeat loop (which triggers the
	// reset) and have better information there.
}
