// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package syncutil

import (
	"testing"
	"time"
)

func TestStuckMutex(t *testing.T) {
	t.Skip("manual")
	var rwm RWMutex
	go func() {
		func() {
			func() {
				rwm.Lock() // oops
			}()
		}()
	}()
	for i := 0; i < 9999; i++ {
		time.Sleep(time.Second)
	}
}
