// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package fail provides a fail point implementation for testing-only failure
// injection.
//
// Fail points are code instrumentations that allow errors and other behavior to
// be injected dynamically at runtime, primarily for testing purposes. Fail
// points are flexible and can be configured to exhibit a variety of behavior,
// including returning errors, panicing, and sleeping.
//
// Fail points have a few unique benefits when compared to the standard testing
// hook pattern. To start, adding a new fail point requires significantly less
// code than adding a new testing hook. To add a new fail point, simply create
// a new Event in event.go and trigger the fail point where desired by calling
// fail.Point. Unit tests can then activate this new failure point by calling
// fail.Activate. This combines to make fail points lightweight and easy to use.
// This compares favorably to the effort needed to install a new testing hook,
// which often requires defining new types, threading testing knob dependencies
// through production code, and avoiding package dependency cycles.
//
// More importantly, fail points have no associated runtime cost in production
// builds of the code. Fail points use conditional compilation through the use
// of build tags to optimize away to nothing when not enabled. Only when enabled
// do the calls to fail.Point have any effect. This means that users of the
// library are free to scatter fail points in performance critical code paths
// without fear of having an effect on performance.
//
// This package is inspired by FreeBSD's failpoints:
// https://freebsd.org/cgi/man.cgi?query=fail
package fail

import "fmt"

// Callback is a function called when a failpoint is triggered.
type Callback func(...interface{}) error

// Error is an error returned by the detault fail point callback.
type Error struct{ e Event }

// Error implements the error interface.
func (e *Error) Error() string { return fmt.Sprintf("fail point %s triggered", e.e) }

// Event returns the identifier of the fail point that created the receiver.
func (e *Error) Event() Event { return e.e }
