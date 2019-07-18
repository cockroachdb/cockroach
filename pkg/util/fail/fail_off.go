// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build !race

package fail

// Enabled is true if CockroachDB was built with the race build tag.
const Enabled = false

// Activate actives the fail point with the specified identifier. While
// activated, calls to Point with that identifier will trigger the provided
// callback, allowing callers to return errors, inject panics, and sleep. If a
// callback is not provided (f = nil), the default fail point callback will be
// used, which returns a *fail.Error when triggered.
//
// Activate returns a function to deactivate the fail point, which should be
// called by the end of the current test to avoid leaving the fail point
// activated for other tests.
func Activate(e Event, f Callback) func() {
	panic(`fail.Activate should not be called when fail.Enabled is false. Consider adding 

    if !fail.Enabled {
        t.Skip("uses fail points")
    }

to your test.
`)
}

// Point triggers the fail point with the specified identifier. If the fail
// point is activated then its callback will be called. If not then the fail
// point will be a no-op and will return nil.
func Point(e Event, args ...interface{}) error {
	// No-op. Should allow the function call to be optimized away, which in turn
	// should prevent any values passed through the empty interface vararg from
	// escaping.
	return nil
}
