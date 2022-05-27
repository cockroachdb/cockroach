// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package p

import (
	"fmt"
	"net"
	"sync"
	"testing"
)

var (
	intID   = func(n int) int { return n }
	doWork  = func() {}
	runFunc = func(f func()) { f() }

	collection = []int{1, 2, 3}
)

func OutOfScope() {
	var i int
	for range collection {
		i++
		go func() {
			intID(i) // valid data race, but out of scope for this linter
		}()
	}
}

// TableDriven ensures that we are able to flag a common pattern in
// table-driven tests. If a Go routine is spawned while iterating over
// the test cases, it's easy to accidentally reference the test case
// variable, leading to flaky tests if the test cases run in parallel.
func TableDriven(t *testing.T) {
	values := [][]byte{{0x08, 0x00, 0x00, 0xff, 0xff}}

	for _, tc := range values {
		t.Run("", func(t *testing.T) {
			w, _ := net.Pipe()
			errChan := make(chan error, 1)

			go func() {
				if _, err := w.Write(tc); err != nil { // want `loop variable 'tc' captured by reference`
					errChan <- err
					return
				}
			}()
		})
	}
}

// Conditional ensures that even when a Go routine is created in more
// syntactically complex subtrees, it's still flagged if it captures a
// loop variable. In this case, the code is technically safe since the
// Go routine is only created in the last iteration of the loop, but
// it is believed that the variable should not be captured either way
// to avoid the chance of introducing bugs when this code is changed
// (it's also not possible to statically determine when using a loop
// variable inside a Go routine is safe, so we err on the side of
// caution).
func Conditional() {
	for i, n := range collection {
		intID(n)
		if i == len(collection)-1 {
			go func() {
				fmt.Printf("i = %d\n", i) // want `loop variable 'i' captured by reference`
			}()
		}
	}

	for j := 0; j < 10; j++ {
		go func() {
			doWork()
			fmt.Printf("done: %d\n", j) // want `loop variable 'j' captured by reference`
		}()
	}
}

// FuncLitArg ensures that function literals (closures) passed as
// argument to a function call in a 'go' statement should also be
// flagged if they capture a loop variable.
func FuncLitArg() {
	for _, n := range collection {
		doWork()
		go runFunc(func() {
			intID(n) // want `loop variable 'n' captured by reference`
		})

		go intID(n) // this is OK
		doWork()
	}

	for j := 0; j < len(collection); j++ {
		doWork()
		go runFunc(func() {
			intID(collection[j]) // want `loop variable 'j' captured by reference`
		})

		go intID(collection[j]) // this is OK
	}
}

// Synchronization is another example of a technically safe use of a
// loop variable in a Go routine that we decide to flag anyway.
func Synchronization() {
	for _, n := range collection {
		var wg sync.WaitGroup
		go func() {
			defer wg.Done()
			intID(n) // want `loop variable 'n' captured by reference`
		}()

		wg.Wait()
	}
}

// IndirectClosure makes sure that closures that capture loop
// variables cannot be called in a Go routine.
func IndirectClosure() {
	for i := range collection {
		badClosure := func() { fmt.Printf("finished iteration %d\n", i+1) }
		goodClosure := func(i int) { fmt.Printf("finished iteration %d\n", i+1) }

		wrapper1 := func() { badClosure() }
		wrapper2 := func() { wrapper1() }
		wrapper3 := func() { goodClosure(i) }

		iCopy := i
		go func() {
			defer badClosure() // want `'badClosure' function captures loop variable 'i' by reference`
			doWork()

			// referencing a closure without invoking it is fine
			if badClosure != nil {
				wrapper1() // want `'wrapper1' function captures loop variable 'i' \(via 'badClosure'\)`
				doWork()
				wrapper2() // want `'wrapper2' function captures loop variable 'i' \(via 'wrapper1' -> 'badClosure'\)`

				wrapper3() // want `'wrapper3' function captures loop variable 'i' by reference`

				// copying here does not solve the problem
				k := i         // want `loop variable 'i' captured by reference`
				goodClosure(k) // still problematic

				goodClosure(iCopy) // this is OK
			}
		}()

		go badClosure() // want `'badClosure' function captures loop variable 'i' by reference`
		go wrapper2()   // want `'wrapper2' function captures loop variable 'i' \(via 'wrapper1' -> 'badClosure'\)`
	}

	for j := 0; j < len(collection); j++ {
		showProgress := func() {
			fmt.Printf("finished iteration %d\n", j+1)
		}

		go func() {
			doWork()
			showProgress() // want `'showProgress' function captures loop variable 'j' by reference`
		}()
	}
}

// FixedFunction tests that common patterns to fix loop variable
// capture by reference in Go routines work: namely, passing the loop
// variable as an argument to the function called asynchronously; or
// creating a scoped copy of the loop variable within the loop.
func FixedFunction() {
	for _, n := range collection {
		doWork()
		go func(n int) {
			intID(n) // this is OK
		}(n)
	}

	for j := 0; j < len(collection); j++ {
		j := j
		go func() {
			intID(j) // this is OK
		}()
	}
}

// RespectsNolintComments makes sure that developers are able to
// silence the linter using their own judgement.
func RespectsNolintComments() {
	for _, n := range collection {
		var wg sync.WaitGroup
		wg.Add(1)

		badClosure := func() { fmt.Printf("n = %d\n", n) }

		go func() {
			defer wg.Done()
			//nolint:goroutinerefcapture
			intID(n)

			//nolint:goroutinerefcapture
			badClosure()
		}()

		//nolint:goroutinerefcapture
		go badClosure()

		wg.Wait()
	}
}
