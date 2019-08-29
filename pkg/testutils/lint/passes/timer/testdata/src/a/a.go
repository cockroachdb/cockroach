// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package a

import "github.com/cockroachdb/cockroach/pkg/util/timeutil"

func init() {
	timer := timeutil.NewTimer()
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
