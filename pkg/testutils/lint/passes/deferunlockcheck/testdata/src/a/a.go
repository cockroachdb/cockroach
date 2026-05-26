// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package a

import (
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
)

type TestUnlockLint struct {
	mu struct {
		syncutil.Mutex
		foo string
		too bool
	}
	amended bool
}

func testFnCall() bool {
	return true
}

func basicCases() {
	t := &TestUnlockLint{}

	// Less than or equal to 5 lines.
	t.mu.Lock()
	t.mu.foo = "a"
	t.mu.Unlock()

	// Unlock is too far away.
	t.mu.Lock()
	// Lines
	// Lines
	// Lines
	// Lines
	// Lines
	t.mu.Unlock() // want `Unlock is >5 lines away from matching Lock, move it to a defer statement after Lock`

	// nolint exception inline.
	t.mu.Lock()
	// Lines
	// Lines
	// Lines
	// Lines
	// Lines
	t.mu.Unlock() // nolint:deferunlock

	// nolint exception above.
	t.mu.Lock()
	// Lines
	// Lines
	// Lines
	// Lines
	// Lines
	// nolint:deferunlock
	t.mu.Unlock()

	// If statement between lock/unlock pair.
	someVar := "foo"
	t.mu.Lock()
	if someVar != "" { // want `if statement between Lock and Unlock may be unsafe, move Unlock to a defer statement after Lock`
		t.mu.foo = "bar"
	}
	t.mu.Unlock()

	// For statement between lock/unlock pair.
	someArr := []string{
		"a",
		"b",
		"c",
	}
	t.mu.Lock()
	for i := len(someArr); i < len(someArr); i++ { // want `for loop between Lock and Unlock may be unsafe, move Unlock to a defer statement after Lock`
		t.mu.foo = someArr[i]
	}
	t.mu.Unlock()

	// Range statement between lock/unlock pair.
	t.mu.Lock()
	for _, v := range someArr { // want `for loop between Lock and Unlock may be unsafe, move Unlock to a defer statement after Lock`
		t.mu.foo = v
	}
	t.mu.Unlock()

	// Function calls between lock/unlock pair.
	t.mu.Lock()
	testFnCall() // want `function call between Lock and Unlock may be unsafe, move Unlock to a defer statement after Lock`
	t.mu.Unlock()

	t.mu.Lock()
	t.mu.too = testFnCall() // want `function call between Lock and Unlock may be unsafe, move Unlock to a defer statement after Lock`
	t.mu.Unlock()

	// Allow non linear control flow if the nolint comment is present.
	t.mu.Lock()
	// nolint:deferunlock
	if someVar != "" {
		t.mu.foo = "bar"
	}
	t.mu.Unlock()

	t.mu.Lock()
	for _, v := range someArr { // nolint:deferunlock
		t.mu.foo = v
	}
	t.mu.Unlock()

	t.mu.Lock()
	testFnCall() // nolint:deferunlock
	t.mu.Unlock()

	// Allow deferring unlocks.
	t.mu.Lock()
	defer t.mu.Unlock()
}

func deferFunc() {
	t := &TestUnlockLint{}
	// Having an unlock in a defer function w/out non linear control flow is ok.
	t.mu.Lock()
	defer func() {
		t.mu.Unlock()
	}()

	// Still catches if Unlock is too far.
	t.mu.Lock()
	defer func() {
		// Lines
		// Lines
		// Lines
		// Lines
		// Lines
		t.mu.Unlock() // want `Unlock is >5 lines away from matching Lock, move it to a defer statement after Lock`
	}()
}

func inlineFunc() {
	t := &TestUnlockLint{}
	// Inline function with deferred unlock is ok.
	func() {
		t.mu.Lock()
		defer t.mu.Unlock()
		t.mu.foo = "a"
		t.mu.too = true
	}()
	// Same with inline function and no non linear control flow in between.
	func() {
		t.mu.Lock()
		t.mu.foo = "a"
		t.mu.Unlock()
	}()
	// This will produce a non linear control flow error.
	func() {
		t.mu.Lock()
		testFnCall() // want `function call between Lock and Unlock may be unsafe, move Unlock to a defer statement after Lock`
		t.mu.Unlock()
	}()
}
