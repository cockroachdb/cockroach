// Copyright 2014 The Cockroach Authors.
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
//
// Author: Tobias Schottdorf

package util

import "time"

// TruncateDuration returns a new duration obtained from the first argument
// by discarding the portions at finer resolution than that given by the
// second argument.
// Example: TruncateDuration(time.Second+1, time.Second) == time.Second.
func TruncateDuration(d time.Duration, r time.Duration) time.Duration {
	if r == 0 {
		panic("zero passed as resolution")
	}
	return d - (d % r)
}
