// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package circuit

import (
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// An EventHandler is reported to by circuit breakers.
type EventHandler interface {
	OnTrip(_ *Breaker, prev, cur error)
	OnProbeLaunched(*Breaker)
	OnProbeDone(*Breaker)
	OnReset(*Breaker)
}

// EventLogBridge is an EventHandler that relays to a logging function.
type EventLogBridge struct {
	Logf func(format redact.SafeString, args ...interface{})
}

// OnTrip implements EventHandler. If the previous error is nil, it logs the
// error. If the previous error is not nil and has a different cause than the
// current error, logs a message indicating the old and new error.
func (d EventLogBridge) OnTrip(b *Breaker, prev, cur error) {
	if prev != nil && !errors.Is(errors.Cause(prev), errors.Cause(cur)) {
		d.Logf("%s: now tripped with error: %s (previously: %s)", b.Opts().Name, cur, prev)
	} else {
		d.Logf("%s: tripped with error: %s", b.Opts().Name, cur)
	}
}

// OnProbeLaunched implements EventHandler. It is a no-op.
func (d EventLogBridge) OnProbeLaunched(*Breaker) {}

// OnProbeDone implements EventHandler. It is a no-op.
func (d EventLogBridge) OnProbeDone(*Breaker) {}

// OnReset implements EventHandler. It logs a message.
func (d EventLogBridge) OnReset(b *Breaker) {
	d.Logf("%s: breaker reset", b.Opts().Name)
}
