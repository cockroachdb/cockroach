// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

// +build go1.8

package cli

import (
	"runtime"

	"github.com/cockroachdb/cockroach/pkg/util/envutil"
)

func init() {
	// Enable the mutex profile for a fraction of mutex contention events.
	// Smaller values provide more accurate profiles but are more expensive. 0
	// and 1 are special: 0 disables the mutex profile and 1 captures 100% of
	// mutex contention events. For other values, the profiler will sample on
	// average 1/X events.
	//
	// The mutex profile can be viewed with `pprof http://HOST:PORT/debug/pprof/mutex`
	runtime.SetMutexProfileFraction(envutil.EnvOrDefaultInt("COCKROACH_MUTEX_PROFILE_RATE", 0))
}
