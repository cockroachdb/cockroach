// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cluster

import "context"

// A Monitor watches the cluster for unexpected node deaths.
//
// NB: In a roachtest, it's best practice to spawn a new go routine via a
// monitor, instead of directly in the testSpec.Run() closure because the
// monitor's go routine has builtin recovery from panics. In other words, a
// t.Fatal() call within monitor.Go() will lead to a graceful roachtest to
// failure, just like when t.Fatal is called directly in testSpec.Run(). This
// ensures proper roachprod cluster shutdown, for example.
type Monitor interface {
	ExpectDeath()
	ExpectDeaths(count int32)
	ResetDeaths()

	// Go spawns a goroutine whose fatal errors will be handled gracefully leading to a
	// clean roachtest shutdown. To prevent leaky goroutines, the caller must call
	// Wait() or WaitE() before returning.
	Go(fn func(context.Context) error)
	GoWithCancel(fn func(context.Context) error) func()
	WaitForNodeDeath() error
	WaitE() error
	Wait()
}
