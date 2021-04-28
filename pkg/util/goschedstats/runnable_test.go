// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package goschedstats

import (
	"fmt"
	"runtime"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/testutils"
)

func TestNumRunnableGoroutines(t *testing.T) {
	// Start 500 goroutines that never finish.
	const n = 400
	for i := 0; i < n; i++ {
		go func(i int) {
			a := 1
			for x := 0; x >= 0; x++ {
				a = a*13 + x
			}
		}(i)
	}
	// When we run, we expect at most GOMAXPROCS-1 of the n goroutines to be
	// running, with the rest waiting.
	expected := n - runtime.GOMAXPROCS(0) + 1
	testutils.SucceedsSoon(t, func() error {
		if n, _ := numRunnableGoroutines(); n < expected {
			return fmt.Errorf("only %d runnable goroutines, expected %d", n, expected)
		}
		return nil
	})
}
