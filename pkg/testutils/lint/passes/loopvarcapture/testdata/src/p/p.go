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
	"math/rand"
	"net"
	"sync"
	"testing"

	"example.org/concurrency"
	"golang.org/x/sync/errgroup"
)

var (
	intID         = func(n int) int { return n }
	intRef        = func(n *int) int { return *n }
	boolID        = func(b bool) bool { return b }
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

// Synchronization exercises the synchronization detection mechanisms
// in the linter; when synchronization is detected, references to loop
// variables in Go routines are not flagged.
func Synchronization() {
	outerChan := make(chan error)

	for _, n := range collection {
		var wg1 sync.WaitGroup
		wg1.Add(1)
		go func() {
			defer wg1.Done()
			intID(n) // this is OK
		}()

		go func() {
			intID(n) // this is OK
			defer wg1.Done()
		}()

		go func() {
			wg1.Done()
			intID(n) // want `loop variable 'n' captured by reference`
		}()

		chan1 := make(chan error)
		chan2 := make(chan error)
		go func() {
			if len(collection) == 0 {
				intID(n) // this is OK
			}

			chan1 <- nil // channel is waited on at the end of the loop
		}()

		go func() {
			chan1 <- nil

			// this is not OK -- channel send happens before the loop
			// variable reference
			if len(collection) == 0 {
				intID(n) // want `loop variable 'n' captured by reference`
			}
		}()

		go func() {
			defer close(chan2)
			intID(n) // this is OK
		}()

		go func() {
			close(chan2)
			// this is not OK -- synchronization call happens before the
			// loop variable reference
			intID(n) // want `loop variable 'n' captured by reference`
		}()

		go func() {
			defer close(outerChan)
			intID(n)         // want `loop variable 'n' captured by reference`
			outerChan <- nil // outerChan is *not* read in the loop
		}()

		var wg2 sync.WaitGroup
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			// this is *not* safe because wg2 is not Waited in the loop
			intID(n) // want `loop variable 'n' captured by reference`
		}()

		goodClosure := func() {
			defer wg1.Done()
			fmt.Printf("working on %d\n", n)
		}
		badClosure := func() {
			defer wg2.Done() // not waited on in the loop
			fmt.Printf("working on %d\n", n)
		}

		wrapper1 := func() { goodClosure() }
		wrapper2 := func() { badClosure() }

		go func() {
			doWork()
			goodClosure() // this is OK, we wait on wg1 in the loop
		}()

		go func() {
			badClosure() // want `'badClosure' function captures loop variable 'n' by reference`
			doWork()
		}()

		go func() {
			doWork()
			wrapper1() // this is OK, we wait on wg1 in the loop
		}()

		go func() {
			doWork()
			wrapper2() // want `'wrapper2' function captures loop variable 'n' \(via 'badClosure'\)`
		}()

		go goodClosure()
		go badClosure() // want `'badClosure' function captures loop variable 'n' by reference`

		go wrapper1()
		go wrapper2() // want `'wrapper2' function captures loop variable 'n' \(via 'badClosure'\)`

		var wg3 sync.WaitGroup
		wg3.Add(1)
		wg3.Wait()

		go func() {
			defer wg3.Done()
			// not OK: wg3 is waited on *before* the Go routine is spawned
			intID(n) // want `loop variable 'n' captured by reference`
		}()

		// overwrite `close` builtin
		close := func(c chan error) {
			fmt.Print("not actually closing channel %v\n", c)
		}
		go func() {
			// while it's not good practice to overwrite Go builtins, we
			// make sure that if you do so, we still flag an unsafe use
			// of the loop variable `n`
			defer close(chan1)
			intID(n) // want `loop variable 'n' captured by reference`
		}()

		defer func() {
			wg1.Done()
			// this is still invalid, as this function will be called when
			// the function returns, no matter what the loop does.
			intID(n) // want `loop variable 'n' captured by reference`
		}()

		synchronized := func() {
			defer wg1.Done()
			intID(n)
			fmt.Printf("done")
		}
		notSynchronized := func() {
			defer close(outerChan)
			intID(n)
		}
		intProducer := func(msg string, f func()) int {
			fmt.Printf("msg: %s\n", msg)
			f()
			return 42
		}

		go runFunc(synchronized)    // this is OK
		go runFunc(notSynchronized) // want `'notSynchronized' function captures loop variable 'n' by reference`

		go intID(intProducer("bad", notSynchronized)) // want `'notSynchronized' function captures loop variable 'n' by reference`
		go intID(intProducer("ok", synchronized))     // this is OK

		// inline functions
		func() {
			doWork()
			go func() {
				intID(n) // want `loop variable 'n' captured by reference`
			}()

			go func() {
				defer wg1.Done()
				intID(n) // this is OK
			}()
		}()

		// reading from a channel in a `range` loop is also a way to
		// provide synchronization
		rangeChan := make(chan error)
		go func() {
			doWork()
			rangeChan <- nil
			intID(n) // this is OK -- synchronized below
			rangeChan <- nil
		}()

		for range rangeChan {
			doWork()
		}

		wg1.Wait()
		<-chan1
		<-chan2
	}

	<-outerChan
}

// GoWrapperWait exercises functionality provided by wrapper objects
// that manage multiple Go routines internally and provide some
// mechanism to wait for them to finish (see `GoRoutineFunctions` in
// the linter). Using them to synchronize access to loop variables
// (making sure the loop variable doesn't change until the go routine
// is finished) should be safe.
func GoWrapperWait() {
	var eg1 errgroup.Group    // properly used errgroup
	var eg2 errgroup.Group    // never waited for
	var cg1 concurrency.Group // waited for outside the loop
	var cg2 concurrency.Group // waited for before the call to Go()

	for _, n := range collection {
		eg1.Go(func() error {
			intID(n) // this is OK, as we call Wait() within the loop
			return nil
		})

		f := func() {
			intID(n)
			fmt.Printf("done!\n")
		}
		eg2.Go(func() error {
			if rand.Float64() < 0.5 {
				// eg2 is not waited from within the loop
				f() // want `'f' function captures loop variable 'n' by reference`
			}

			return nil
		})

		cg1.Go(func() {
			// cg1.Wait() is called outside the loop
			intID(n) // want `loop variable 'n' captured by reference`
		})

		cg2.Wait()
		cg2.Go(func() {
			// cg2.Wait() is called before this Go() call
			intID(n) // want `loop variable 'n' captured by reference`
		})

		eg1.Wait()

		// this is safe as we call the corresponding wait function below
		concurrency.Go(func() { intID(n) })
		concurrency.Wait1()

		if err := concurrency.GoWithError(func() {
			// not safe: corresponding wait call called outside the loop
			intID(n) // want `loop variable 'n' captured by reference`
		}); err != nil {
			panic(err)
		}
	}

	cg1.Wait()
	concurrency.Await()
}

// TestRunner exercises common patterns in Go tests, including the
// 'table-driven' style.
func TestRunner(t *testing.T) {
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

	testCases := []int{1, 2, 3}
	ch := make(chan error)
	for _, tc := range testCases {
		t.Run("sequential", func(t *testing.T) {
			doWork()
			go func() {
				fmt.Printf("working...\n")
				intID(tc) // this is OK due to synchronization via `ch`
				ch <- nil
			}()

			<-ch
		})
	}

	for _, tc := range testCases {
		t.Run("parallel", func(t *testing.T) {
			t.Parallel()
			doWork()
			go func() {
				fmt.Printf("working...\n")
				intID(tc) // want `loop variable 'tc' captured by reference`
				ch <- nil
			}()

			<-ch
		})

		// `Parallel` setting is reset in a new Run() call
		t.Run("sequential 2", func(t *testing.T) {
			doWork()
			go func() {
				intID(tc) // this is OK
				ch <- nil
			}()

			<-ch
		})
	}

	// `Parallel` setting is reset in a new loop
	for _, tc := range testCases {
		t.Run("sequential 3", func(t *testing.T) {
			doWork()
			go func() {
				intID(tc) // this is OK
				ch <- nil
			}()

			<-ch
		})
	}

	for _, tc := range testCases {
		t.Run("sequential defer", func(t *testing.T) {
			doWork()
			defer func() {
				intID(tc) // this is OK
			}()

			defer intRef(&tc) // this is OK
		})
	}

	for _, tc := range testCases {
		t.Run("parallel defer", func(t *testing.T) {
			t.Parallel()
			doWork()
			defer func() {
				intID(tc) // want `loop variable 'tc' captured by reference`
			}()
		})
	}

	for _, tc := range testCases {
		t.Run("parallel group", func(t *testing.T) {
			t.Parallel()
			t.Run("p1", func(t *testing.T) {
				go func() {
					defer close(ch)
					intID(tc) // want `loop variable 'tc' captured by reference`
				}()

				<-ch
			})

			t.Run("p2", func(t *testing.T) {
				go func() {
					defer close(ch)
					intID(tc) // want `loop variable 'tc' captured by reference`
				}()

				<-ch
			})
		})
	}

	// when the test is parallel, any references to loop variables are
	// invalid
	for _, tc := range testCases {
		t.Run(fmt.Sprintf("%s", tc), func(t *testing.T) {
			t.Parallel()

			n := intID(tc) // want `loop variable 'tc' captured by reference`
			fmt.Printf("n = %d\n", n)

			// having synchronization code does not change the fact that
			// references are not safe
			var wg sync.WaitGroup
			defer wg.Done()
			intID(tc) // want `loop variable 'tc' captured by reference`
			wg.Wait()
		})
	}
}

// AddressOfLoopVar tests scenarios where the address of a loop
// variable is taken and either used in a Go routine, or passed as
// parameter to a go routine spawn
func AddressOfLoopVar() {
	for _, n := range collection {
		go intRef(&n) // want `loop variable 'n' captured by reference`
		cp := n
		go intRef(&cp) // this is OK

		var x *int
		go func() {
			doWork()
			x = &n // want `loop variable 'n' captured by reference`
			if x == nil {
				return
			}
		}()

		var wg sync.WaitGroup
		go func() {
			defer wg.Done()
			doWork()
			x = &n // this is OK -- synchronized
		}()

		var eg errgroup.Group
		eg.Go(func() error {
			x = &n // this is OK -- synchronized
			return nil
		})
		eg.Wait()

		wg.Wait()
	}
}

type T struct {
	N int
}

func (t T) Copy() {}
func (t *T) Ref() {}

// PointerReceiverLoopVar tests calls to `go` that invoke a function
// defined on a pointer to a loop variable.
func PointerReceiverLoopVar() {
	ts := []T{{1}, {2}, {3}}

	var cg concurrency.Group
	for _, t := range ts {
		go t.Copy() // this is OK
		go t.Ref()  // want `loop variable 't' captured by reference`

		go func() {
			t.Copy()   // want `loop variable 't' captured by reference`
			intID(t.N) // want `loop variable 't' captured by reference`
			t.Ref()    // want `loop variable 't' captured by reference`
		}()

		cg.Go(func() {
			doWork()
			t.Copy() // this is OK -- synchronized
			t.Ref()  // this is OK -- synchronized
		})
		cg.Wait()

		cg.Go(func() {
			doWork()
			t.Copy() // want `loop variable 't' captured by reference`
			doWork()
			t.Ref() // want `loop variable 't' captured by reference`
		})
	}

	// when iterating on pointer types, `go t.Method()` is fine
	// (but not using it in an asynchronous closure)
	tPointers := []*T{{1}, {2}, {3}}
	for _, t := range tPointers {
		go t.Copy() // this is OK
		go t.Ref()  // this is also OK

		go func() {
			t.Copy()   // want `loop variable 't' captured by reference`
			intID(t.N) // want `loop variable 't' captured by reference`
			t.Ref()    // want `loop variable 't' captured by reference`
		}()

		cg.Go(func() {
			doWork()
			t.Copy() // this is OK -- synchronized
			t.Ref()  // this is OK -- synchronized
		})
		cg.Wait()

		cg.Go(func() {
			doWork()
			t.Copy() // want `loop variable 't' captured by reference`
			doWork()
			t.Ref() // want `loop variable 't' captured by reference`
		})
	}

}

// Select tests for common patterns using `select`, inspired by real
// examples in the CockroachDB codebase
func Select(t *testing.T) {
	for _, b := range []bool{true, false} {
		cmd1Done := make(chan error)
		cmd2Done := make(chan error)

		go func() {
			args := boolID(b) // this is OK due to the synchronization with cmd2Done
			fmt.Printf("args: %v\n", args)
			cmd2Done <- nil
		}()

		select {
		case pErr := <-cmd2Done:
			if pErr != nil {
				t.Fatal(pErr)
			}

		case pErr := <-cmd1Done:
			t.Fatal("cmd1 should have been blocked, got: %v", pErr)
		}
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

		nonCapturing := func() {
			fmt.Printf("doing some work...\n")
			doWork()
		}
		intProducer := func(msg string, f func()) int {
			fmt.Printf("msg: %s\n", msg)
			f()
			return 42
		}

		go runFunc(nonCapturing)                  // this is OK
		go runFunc(badClosure)                    // want `'badClosure' function captures loop variable 'i' by reference`
		go intID(intProducer("bad", badClosure))  // want `'badClosure' function captures loop variable 'i' by reference`
		go intID(intProducer("ok", nonCapturing)) // this is OK
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
		badClosure := func() { fmt.Printf("n = %d\n", n) }

		go func() {
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
	}
}
