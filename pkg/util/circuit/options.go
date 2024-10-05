// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package circuit

import "github.com/cockroachdb/redact"

// Options are the arguments to NewBreaker. All fields are required.
type Options struct {
	// Name is the name of a Breaker and will be mentioned in the errors
	// that a particular Breaker generates.
	Name redact.RedactableString

	// AsyncProbe is invoked when the Breaker is in a tripped state. The method
	// should not block but instead delegate any work that needs to be done to a
	// goroutine (the "probe") that can then invoke the methods supplied to it.
	// Whenever the probe calls `report`, the error passed to it replaces the
	// latest error tripping the probe. `report(nil)` untrips the Breaker.
	// `done()` must be invoked when the probe winds down (regardless of whether
	// the breaker is still tripped); this lets the Breaker know that if necessary
	// AsyncProbe can be invoked again.
	//
	// It is legitimate for the work triggered by AsyncProbe to be long-running
	// (i.e. it could repeatedly check if the condition triggering the breaker has
	// resolved, returning only once it has or a timeout has elapsed) or one-shot
	// (i.e. making a single attempt, reporting the result, and invoking
	// `done()`).
	//
	// The Breaker will only ever permit one active invocation of the probe (i.e.
	// once a probe is launched, no second probe is launched until the first probe
	// returns via done()). In addition to this, it will trigger probes only when
	// the Breaker first trips, and after that when Breaker.Signal returns a
	// non-nil error. In other words, the probe is not spawned when the Breaker is
	// not seeing any usage.
	AsyncProbe func(report func(error), done func())

	// EventHandler receives events from the Breaker. For an implementation that
	// performs unstructured logging, see EventLogger. Can be nil if no event
	// handler is needed.
	EventHandler EventHandler

	// signalInterceptor gets to see and change the return value of the Signal
	// method. This is useful for testing, as it allows "pretending" that the
	// breaker is tripped without having to replace the probe method and
	// coordinating with a running probe.
	//
	// Accessible only via TestingSetTripped and TestingReset.
	signalInterceptor func(Signal) Signal
}
