// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package p

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"sync"
	"testing"

	"example.org/concurrency"
	"golang.org/x/sync/errgroup"
)

var (
	intID         = func(n int) int { return n }
	doWork        = func() {}
	runFunc       = func(f func()) { f() }
	someCondition = func() bool { return true }

	collection = []int{1, 2, 3}
)

type MyStruct struct {
	closure func()
}

func OutOfScope() {
	var i int
	var s MyStruct
	for j := range collection {
		s.closure = func() {
			fmt.Printf("captured: %d\n", j)
		}

		i++
		go func() {
			intID(i) // valid data race, but out of scope for this linter

			// valid data race, but we don't track assignments to struct
			// fields right now: it would add complexity to the linter and
			// it's a much less common pattern.
			s.closure()
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

// NestedLoops makes sure nested loops are supported, and all
// references to loop variables in inner or outer loops are detected.
func NestedLoops() {
	for i, n := range collection {
		go func() {
			defer func() {
				fmt.Printf("iter = %d\n", n) // want `loop variable 'n' captured by reference`
			}()
			doWork()
		}()

		for j := range collection {
			go func() {
				doWork()
				intID(j) // want `loop variable 'j' captured by reference`
				doWork()
				intID(n) // want `loop variable 'n' captured by reference`
				doWork()
			}()

			for k := j; k < len(collection); k++ {
				go func(idx int) {
					intID(k)   // want `loop variable 'k' captured by reference`
					intID(idx) // this is OK
					intID(n)   // want `loop variable 'n' captured by reference`

					if k > 0 { // want `loop variable 'k' captured by reference`
						intID(j) // want `loop variable 'j' captured by reference`
					}
				}(i)
			}
		}
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

		defer func(n int) {
			intID(n) // this i OK
		}(n)
	}

	for j := 0; j < len(collection); j++ {
		j := j
		go func() {
			intID(j) // this is OK
		}()

		defer func() {
			intID(j) // this is OK
		}()
	}
}

// CapturingDefers makes sure that `defer` statements that are passed
// closures that capture loop variables by reference are also detected.
func CapturingDefers() {
	for i, n := range collection {
		showProgress := func() {
			fmt.Printf("finished iteration: %d\n", i)
		}

		if n > 0 {
			defer func() {
				fmt.Printf("cleaning up: %d\n", n) // want `loop variable 'n' captured by reference`
			}()

			defer showProgress() // want `'showProgress' function captures loop variable 'i' by reference`

			defer func(callback func()) {
				fmt.Printf("finished loop, nothing to see here")
				callback()
			}(func() { intID(n) }) // want `loop variable 'n' captured by reference`
		}

		for j := 0; i < len(collection); j++ {
			defer func(idx int) {
				intID(n)              // want `loop variable 'n' captured by reference`
				fmt.Printf("%d\n", j) // want `loop variable 'j' captured by reference`
				intID(idx)            // this is OK
			}(i)
		}
	}
}

// SafeDefers makes sure that `defer` statements contained in a
// closure defined withing a loop do not lead to an issue being
// reported, as they are always safe.
func SafeDefers() {
	for i, n := range collection {
		// function called immediately; defer is always safe
		func() {
			fmt.Printf("doing work: %d", n)
			defer func() {
				intID(n) // this is OK
			}()

			// this continues to be problematic
			go func() {
				intID(n) // want `loop variable 'n' captured by reference`
			}()
		}()

		// defer within locally-scoped closure. Only unsafe if called in a
		// Go routine
		customErr := func() (retErr error) {
			doWork()
			defer func() {
				if r := recover(); r != nil {
					retErr = fmt.Errorf("panicked at %d: %v", i, r) // this is OK
				}
			}()
			return nil
		}

		if someCondition() {
			customErr() // this is OK -- not a Go routine
		}

		// this is not safe as it is called in a Go routine
		go customErr() // want `'customErr' function captures loop variable 'i' by reference`

		go func() {
			fmt.Printf("async\n")
			customErr() // want `'customErr' function captures loop variable 'i' by reference`
		}()

		// make sure we still keep the `closures` map updated even when a
		// closure is assigned in a safe `defer`
		var badClosure func()
		func() {
			doWork()
			defer func() {
				badClosure = func() { intID(n) }
			}()
		}()

		go badClosure() // want `'badClosure' function captures loop variable 'n' by reference`
		go func() {
			badClosure() // want `'badClosure' function captures loop variable 'n' by reference`
			doWork()
		}()

		var anotherBadClosure func() error
		func() {
			doWork()
			func() {
				anotherBadClosure = func() error {
					intID(n)
					doWork()
					return nil
				}

				defer func() { intID(n) }() // this is OK
			}()
			doWork()
		}()

		go anotherBadClosure() // want `'anotherBadClosure' function captures loop variable 'n' by reference`
		go func() {
			doWork()
			anotherBadClosure() // want `'anotherBadClosure' function captures loop variable 'n' by reference`
		}()
	}
}

// CapturingGoRoutineFunctions tests that captures of loop variables
// in functions that are known to create Go routines are also detected
// and reported.
func CapturingGoRoutineFunctions() {
	var eg errgroup.Group
	var cg concurrency.Group

	for _, n := range collection {
		eg.Go(func() error {
			fmt.Printf("working on n = %d\n", n) // want `loop variable 'n' captured by reference`

			if rand.Float64() < 0.5 {
				return fmt.Errorf("random error: %d", n) // want `loop variable 'n' captured by reference`
			}

			return nil
		})

		cg.Go(func() { intID(n) }) // want `loop variable 'n' captured by reference`

		concurrency.Go(func() { intID(n) })           // want `loop variable 'n' captured by reference`
		concurrency.SafeFunction(func() { intID(n) }) // this is OK

		err := concurrency.GoWithError(func() { intID(n) }) // want `loop variable 'n' captured by reference`
		if err != nil {
			panic(err)
		}
	}
}

// RespectsNolintComments makes sure that developers are able to
// silence the linter using their own judgement.
func RespectsNolintComments() {
	for _, n := range collection {
		var eg errgroup.Group
		var wg sync.WaitGroup
		wg.Add(1)

		badClosure := func() { fmt.Printf("n = %d\n", n) }

		go func() {
			defer wg.Done()
			//nolint:loopvarcapture
			intID(n)

			//nolint:loopvarcapture
			badClosure()
		}()

		//nolint:loopvarcapture
		go badClosure()

		eg.Go(func() error {
			//nolint:loopvarcapture
			intID(n)
			return nil
		})

		go func() {
			//nolint:loopvarcapture
			intID(n)
		}()

		wg.Wait()
	}
}

// regression test for https://github.com/cockroachdb/cockroach/issues/102678
func registerKVScalability() {
	runScalability := func(ctx context.Context, percent int) {
		nodes := 3

		const maxPerNodeConcurrency = 64
		for i := nodes; i <= nodes*maxPerNodeConcurrency; i += nodes {
			fmt.Println("running workload")

			m := NewMonitor(ctx)
			m.Go(func(ctx context.Context) error {
				cmd := fmt.Sprintf("./workload run kv --init --read-percent=%d "+
					"--splits=1000 --duration=1m "+fmt.Sprintf("--concurrency=%d", i), // want `loop variable 'i' captured by reference`
					percent, nodes)

				return fmt.Errorf("failed to run workload: %s", cmd)
			})
		}
	}
	runScalability(nil, 0)
}

type Monitor interface {
	Go(fn func(context.Context) error)
}

type monitorImpl struct {
	ctx context.Context
}

func (m monitorImpl) Go(fn func(context.Context) error) {
	panic("implement me")
}

func NewMonitor(ctx context.Context) Monitor {
	return newMonitor(ctx)
}

func newMonitor(ctx context.Context) *monitorImpl {
	return &monitorImpl{ctx: ctx}
}
