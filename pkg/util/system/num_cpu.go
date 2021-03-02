// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package system

import "runtime"

// NumCPU returns the number of logical CPUs usable by the current process.
//
// !!Note!! If you are considering using this to scale parallelism with the
// machine size, use runtime.GOMAXPROCS(0) instead. The latter is better because
// GOMAXPROCS is reduced with certain test runs (with the race detector); it can
// also be reduced in containerized environments.
//
// The set of available CPUs is checked by querying the operating system
// at process startup. Changes to operating system CPU allocation after
// process startup are not reflected.
func NumCPU() int {
	return runtime.NumCPU()
}
