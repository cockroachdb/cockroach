// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package a

import (
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type TestUnlockLint struct {
	mu struct {
		syncutil.Mutex
	}
}

func init() {
	var mut syncutil.Mutex
	var rwmut syncutil.RWMutex
	testUnlock := &TestUnlockLint{}

	// Test the main use case.
	// Should only capture Unlock()
	testUnlock.mu.Lock()
	testUnlock.mu.Unlock() // want `Mutex Unlock not deferred`
	// This should pass.
	defer testUnlock.mu.Unlock()

	// Test within a function.
	okFn := func() {
		testUnlock.mu.Lock()
		defer testUnlock.mu.Unlock()
	}
	failFn := func() {
		testUnlock.mu.Lock()
		testUnlock.mu.Unlock() // want `Mutex Unlock not deferred`
	}
	okFn()
	failFn()

	// Test mut variation.
	defer mut.Unlock()
	mut.Unlock() // want `Mutex Unlock not deferred`

	// Test RUnlock
	defer rwmut.RUnlock()
	rwmut.RUnlock() // want `Mutex RUnlock not deferred`

	// Test the no lint rule.
	testUnlock.mu.Unlock()
}
