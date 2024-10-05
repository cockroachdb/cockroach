// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

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
	OnReset(_ *Breaker, prev error)
}

// EventLogger is an implementation of EventHandler that relays to a logging
// function. For each event, Log is invoked with a StringBuilder containing
// a log message about the event.
type EventLogger struct {
	Log func(redact.StringBuilder)
}

func (d *EventLogger) maybeLog(buf redact.StringBuilder) {
	if buf.Len() == 0 {
		return
	}
	d.Log(buf)
}

// OnTrip implements EventHandler. If the previous error is nil, it logs the
// error. If the previous error is not nil and has a different cause than the
// current error, logs a message indicating the old and new error.
func (d *EventLogger) OnTrip(b *Breaker, prev, cur error) {
	var buf redact.StringBuilder
	EventFormatter{}.OnTrip(b, prev, cur, &buf)
	d.maybeLog(buf)
}

// OnProbeLaunched implements EventHandler. It is a no-op.
func (d *EventLogger) OnProbeLaunched(b *Breaker) {
	var buf redact.StringBuilder
	EventFormatter{}.OnProbeLaunched(b, &buf)
	d.maybeLog(buf)
}

// OnProbeDone implements EventHandler. It is a no-op.
func (d *EventLogger) OnProbeDone(b *Breaker) {
	var buf redact.StringBuilder
	EventFormatter{}.OnProbeDone(b, &buf)
	d.maybeLog(buf)
}

// OnReset implements EventHandler. It logs a message.
func (d *EventLogger) OnReset(b *Breaker, prev error) {
	var buf redact.StringBuilder
	EventFormatter{}.OnReset(b, &buf)
	d.maybeLog(buf)
}

// EventFormatter allows direct access to the translation of
// EventHandler inputs to log line suitable for formatted output.
// Its methods mirror the `EventHandler` interface, but with a
// StringBuilder as the last argument, which will be written to.
// Not all methods produce output. The caller should check if
// anything was written to the buffer before logging it.
type EventFormatter struct{}

// OnTrip mirrors EventHandler.OnTrip.
func (EventFormatter) OnTrip(b *Breaker, prev, cur error, buf *redact.StringBuilder) {
	opts := b.Opts()
	if prev != nil && !errors.Is(errors.Cause(prev), errors.Cause(cur)) {
		buf.Printf("%s: now tripped with error: %s (previously: %s)", opts.Name, cur, prev)
	} else {
		buf.Printf("%s: tripped with error: %s", opts.Name, cur)
	}

}

// OnProbeLaunched mirrors EventHandler.OnProbeLaunched.
func (EventFormatter) OnProbeLaunched(*Breaker, *redact.StringBuilder) {
	// No-op.
}

// OnProbeDone mirrors EventHandler.OnProbeDone.
func (EventFormatter) OnProbeDone(*Breaker, *redact.StringBuilder) {
	// No-op.
}

// OnReset mirrors EventHandler.OnReset.
func (EventFormatter) OnReset(b *Breaker, buf *redact.StringBuilder) {
	buf.Printf("%s: breaker reset", b.Opts().Name)
}
