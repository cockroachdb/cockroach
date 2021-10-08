// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package option

import "fmt"

// StopArgs specifies extra arguments that are passed to `roachprod` during `c.Stop`.
func StopArgs(extraArgs ...string) Option {
	return RoachprodArgOption(extraArgs)
}

// RoachprodArgOption is an Option that will result in additional
// arguments passed to a `roachprod` invocation.
type RoachprodArgOption []string

// Option implements Option.
func (o RoachprodArgOption) Option() {}

// StartArgs specifies extra arguments that are passed to `roachprod` during `c.Start`.
func StartArgs(extraArgs ...string) Option {
	return RoachprodArgOption(extraArgs)
}

// StartArgsDontEncrypt will pass '--encrypt=false' to roachprod regardless of the
// --encrypt flag on roachtest. This is useful for tests that cannot pass with
// encryption enabled.
var StartArgsDontEncrypt = StartArgs("--encrypt=false")

// Racks is an option which specifies the number of racks to partition the nodes
// into.
func Racks(n int) Option {
	return StartArgs(fmt.Sprintf("--racks=%d", n))
}

// WorkerAction informs a cluster operation that the callee is a "worker" rather
// than the test's main goroutine.
type WorkerAction struct{}

var _ Option = WorkerAction{}

// Option implements Option.
func (o WorkerAction) Option() {}

// WithWorkerAction is an option informing a cluster operation that the callee
// is a "worker" rather than the test's main goroutine.
func WithWorkerAction() Option {
	return WorkerAction{}
}
