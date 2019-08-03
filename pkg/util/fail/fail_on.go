// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// +build race

package fail

import (
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

// Enabled is true if CockroachDB was built with the race build tag.
const Enabled = true

// reg holds a registry of fail point events and their corresponding callbacks.
var reg = make(map[Event]Callback)
var regMu syncutil.Mutex

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
	if _, ok := reg[e]; ok {
		panic(fmt.Sprintf("duplicate fail point activation for %s", e))
	}
	if f == nil {
		f = defaultFailPoint(e)
	}
	regMu.Lock()
	reg[e] = f
	regMu.Unlock()
	return func() {
		regMu.Lock()
		delete(reg, e)
		regMu.Unlock()
	}
}

func defaultFailPoint(e Event) Callback {
	return func(_ ...interface{}) error {
		return &Error{e}
	}
}

// Point triggers the fail point with the specified identifier. If the fail
// point is activated then its callback will be called. If not then the fail
// point will be a no-op and will return nil.
func Point(e Event, args ...interface{}) error {
	regMu.Lock()
	fn, ok := reg[e]
	regMu.Unlock()
	if ok {
		return fn(args...)
	}
	return nil
}
