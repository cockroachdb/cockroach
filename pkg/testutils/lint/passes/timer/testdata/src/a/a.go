// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package a

import "github.com/cockroachdb/cockroach/pkg/util/timeutil"

// Timer value.
func init() {
	var timer timeutil.Timer
	for {
		timer.Reset(0)
		select {
		case <-timer.C:
			timer.Read = true
		}
	}
	for {
		timer.Reset(0)
		select {
		case <-timer.C: // want `must set timer.Read = true after reading from timer.C \(see timeutil/timer.go\)`
		}
	}
}

// Timer pointer.
func init() {
	timer := &timeutil.Timer{}
	for {
		timer.Reset(0)
		select {
		case <-timer.C:
			timer.Read = true
		}
	}
	for {
		timer.Reset(0)
		select {
		case <-timer.C: // want `must set timer.Read = true after reading from timer.C \(see timeutil/timer.go\)`
		}
	}
}
