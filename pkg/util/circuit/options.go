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

import "github.com/cockroachdb/redact"

// Options are the arguments to NewBreaker. All fields are required.
type Options struct {
	// Name is the name of a Breaker and will be mentioned in the errors
	// that particular Breaker generates.
	Name redact.RedactableString

	// ShouldTrip intercepts and may annotate or replace the errors passed to
	// `Breaker.Report`. In particular, it can return nil to ignore certain
	// errors.
	ShouldTrip func(err error) error

	// AsyncProbe is invoked when the Breaker is in a tripped state. The method
	// should not block but instead delegate any work that needs to be done to
	// a goroutine that can then invoke the methods supplied to it. Whenever the
	// probe calls `report`, the error passed to it replaces the latest error
	// tripping the probe (without being intercepted by ShouldTrip). `report(nil)`
	// untrips the Breaker. `done()` must be invoked when the probe winds down
	// (regardless of whether the breaker is still tripped); this lets the Breaker
	// know that if necessary AsyncProbe can be invoked again.
	//
	// It is legitimate for the work triggered by AsyncProbe to be long-running
	// (i.e. it could repeatedly check if the condition triggering the breaker has
	// resolved, returning only once it has) or one-shot (i.e. making a single
	// attempt, reporting the result, and invoking `done()`).
	AsyncProbe func(report func(error), done func())

	// EventHandler receives events from the Breaker. For an implementation that
	// performs unstructured logging, see EventLogger.
	EventHandler EventHandler
}
