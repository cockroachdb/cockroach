// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package testvalue

import (
	"context"
	"errors"
	"fmt"
	"github.com/cockroachdb/cockroach/pkg/util/ctxgroup"
	"github.com/stretchr/testify/assert"
	"reflect"
	"sync/atomic"
	"testing"
)

func init() {
	Enable()
}

func expectPanic(t *testing.T) {
	if r := recover(); r == nil {
		t.Errorf("The code did not panic")
	}
}

func forceDisable() func() {
	atomic.StoreInt32(&enabled, 0)
	return Enable
}

func TestCannotSetWhenDisabled(t *testing.T) {
	defer expectPanic(t)
	defer forceDisable()()
	Set("foo", 1)
}

func TestCanCallAdjustWhenDisabled(t *testing.T) {
	defer forceDisable()()
	Adjust("foo", nil)
}

// Adjust requires a pointer type argument.
func TestAdjustRequiresPointerArg(t *testing.T) {
	// Attempt to call Adjust for various non-pointer types, and verify that these calls panic.
	for _, v := range []interface{}{
		1, 2.0, true, "str",
		[]int{3, 2, 1}, map[int]bool{0: true, 1: false},
		struct{}{}, []struct{}{{}, {}},
		nil, func() {},
	} {
		t.Run(fmt.Sprintf("check-non-pointer-%T", v), func(t *testing.T) {
			defer Reset()

			group := ctxgroup.WithContext(context.Background())
			defer func() {
				if err := group.Wait(); err != nil {
					t.Error(err)
				}
			}()

			group.Go(func() (err error) {
				err = nil
				defer func() {
					if r := recover(); r == nil {
						err = errors.New(fmt.Sprintf("Expected to panic for value %v of type %T", v, v))
					}
				}()
				Set("foo", v)
				Adjust("foo", v)
				return
			})
		})
	}
}

// Calling Adjust() when the override was not set in test is a no-op.
func TestAdjustWhenNotSetIsNoop(t *testing.T) {
	defer Reset()

	for _, v := range []interface{}{
		1, 2.0, true, "str",
		[]int{3, 2, 1}, map[int]bool{0: true, 1: false},
		struct{}{}, []struct{}{{}, {}},
	} {
		t.Run(fmt.Sprintf("check-adjust-when-not-set-%T", v), func(t *testing.T) {
			old := v
			Adjust("not-there", &v)
			assert.True(t, reflect.DeepEqual(old, v))
		})
	}
}

func TestCanAdjustValues(t *testing.T) {
	for _, testCase := range []struct {
		val      interface{}
		override interface{}
	}{
		{
			val:      1,
			override: 42,
		},
		{
			val:      true,
			override: false,
		},
		{
			val:      "hello world",
			override: "hello moon",
		},
		{
			val:      []int{3, 2, 1},
			override: []int{42},
		},
		{
			val:      map[string]int{"0": 0, "1": 1},
			override: map[string]int{"1": 1, "2": 2, "3": 3},
		},
	} {
		t.Run(fmt.Sprintf("adjust-%T", testCase.val), func(t *testing.T) {
			defer Reset()
			Set("adjust", testCase.override)
			Adjust("adjust", &testCase.val)
			assert.True(t, reflect.DeepEqual(testCase.val, testCase.override))
		})
	}
}

func TestCanAdjustValuesViaSetter(t *testing.T) {
	for _, testCase := range []struct {
		val      interface{}
		expected interface{}
		setter   CallbackSetter
	}{
		{
			val:      1,
			expected: 42,
			setter:   func() interface{} { return 42 },
		},
		{
			val:      true,
			expected: false,
			setter:   func() interface{} { return false },
		},
		{
			val:      "hello world",
			expected: "bonjour monde",
			setter:   func() interface{} { return "bonjour monde" },
		},
		{
			val:      []int{3, 2, 1},
			expected: []int{1, 2, 3, 42},
			setter:   func() interface{} { return []int{1, 2, 3, 42} },
		},
		{
			val:      map[string]int{"0": 0, "1": 1},
			expected: map[string]int{},
			setter: func() interface{} {
				return map[string]int{}
			},
		},
	} {
		t.Run(fmt.Sprintf("adjust-with-setter-%T", testCase.val), func(t *testing.T) {
			defer Reset()

			SetCallback("var", testCase.setter)
			Adjust("var", &testCase.val)
			assert.True(t, reflect.DeepEqual(testCase.expected, testCase.val))
		})
	}
}

func TestAdjustTypesMustMatch(t *testing.T) {
	makeBadResults := func(b ...interface{}) []interface{} { return b }

	for _, testCase := range []struct {
		val interface{}
		bad []interface{}
	}{
		{
			val: 1,
			bad: makeBadResults(true, []int{}, "bad", 2.0, map[string]int{}),
		},
		{
			val: true,
			bad: makeBadResults(1, []int{}, "bad", 2.0, map[string]int{}),
		},
		{
			val: "hello world",
			bad: makeBadResults(true, []int{}, 2.0, map[string]int{}),
		},
		{
			val: []int{3, 2, 1},
			bad: makeBadResults(true, "bad", []string{}, 2.0, map[string]int{}),
		},
		{
			val: map[string]int{"0": 0, "1": 1},
			bad: makeBadResults(true, "bad", []string{}, 2.0, map[string]string{}),
		},
	} {
		for _, badVal := range testCase.bad {
			for _, useSetter := range []bool{false, true} {
				t.Run(fmt.Sprintf("bad-adjust-%T->%T-use_setter=%v", badVal, testCase.val, useSetter), func(t *testing.T) {
					defer Reset()

					if useSetter {
						SetCallback("bad", func() interface{} { return badVal })
					} else {
						Set("bad", badVal)
					}

					// Arrange for panic propagation back to t.
					group := ctxgroup.WithContext(context.Background())
					defer func() {
						if err := group.Wait(); err != nil {
							t.Error(err)
						}
					}()

					group.Go(func() (err error) {
						err = nil
						defer func() {
							if r := recover(); r == nil {
								err = errors.New("expected panic, but alas")
							}
						}()
						Adjust("bad", testCase.val)
						return
					})
				})
			}
		}
	}
}

func TestCanAdjustFunc(t *testing.T) {
	defer Reset()

	val := 1
	handler := func() {
		val = 0
	}

	Set("fun", func() {
		val = 42
	})

	Adjust("fun", &handler)

	handler()
	assert.Equal(t, 42, val)
}

// Test that adjusting values of function type, the signature of the function
// must match exactly.
func TestFuncSignatureMustMatchExactly(t *testing.T) {
	for i, testCase := range []struct {
		f1 interface{}
		f2 interface{}
	}{
		{
			f1: func() {},
			f2: func() error { return nil },
		},
		{
			f1: func() {},
			f2: func(int) {},
		},
		{
			f1: func(int) {},
			f2: func() {},
		},
		{
			f1: func(int) (int, int, int) { return 0, 0, 0 },
			f2: func(int) (int, int) { return 0, 0 },
		},
	} {
		t.Run(fmt.Sprintf("sig-match-%T-%T", testCase.f1, testCase.f2), func(t *testing.T) {
			defer Reset()

			group := ctxgroup.WithContext(context.Background())
			defer func() {
				if err := group.Wait(); err != nil {
					t.Error(err)
				}
			}()

			group.Go(func() (err error) {
				err = nil
				defer func() {
					if r := recover(); r == nil {
						err = errors.New(fmt.Sprintf("Expected to panic for test case %d", i))
					}
				}()
				key := fmt.Sprintf("fun-%d", i)
				Set(key, testCase.f1)
				Adjust(key, &testCase.f2)
				return
			})
		})
	}
}

type greeter interface {
	getGreeting() string
}

type dummyGreeter struct{}

var _ greeter = &dummyGreeter{}

func (*dummyGreeter) getGreeting() string {
	return "dummy"
}

type cannedResponseGreeter struct {
	response string
}

var _ greeter = &cannedResponseGreeter{}

func (g *cannedResponseGreeter) getGreeting() string {
	return g.response
}

// Tests adjustment through interface pointer.
func TestCanAdjustInterface(t *testing.T) {
	defer Reset()

	var greeting greeter = &dummyGreeter{}

	Set("impl", &cannedResponseGreeter{"hello world"})
	Adjust("impl", &greeting)

	assert.Equal(t, "hello world", greeting.getGreeting())
}

func TestCanAdjustChannel(t *testing.T) {
	defer Reset()

	ch := make(chan string)
	origCh := ch
	assert.True(t, ch == origCh)

	Set("ch", make(chan string))
	Adjust("ch", &ch)

	assert.True(t, ch != origCh)
}

func TestCannotAdjustChannelsOfIncompatibleTypes(t *testing.T) {
	defer Reset()
	defer expectPanic(t)

	ch := make(chan string)
	Set("ch", make(chan int))
	Adjust("ch", &ch)
}

type stoppable struct {
	stopped bool
}

func (s *stoppable) stop() {
	s.stopped = true
}

// Ensure the cleanup code scheduled w/ defer on the original object runs even if we
// adjust the value.
func TestAdjustDoesNotLeakDeferredCleanup(t *testing.T) {
	defer Reset()

	st := &stoppable{}
	orig := st

	defer func() {
		// We schedule the deferred cleanup work below (st.stop), before we override 'st' with a new
		// value.  The cleanup against original st should have run anyway.
		assert.True(t, orig.stopped)
		assert.False(t, st.stopped)
	}()

	defer st.stop()

	Set("st", &stoppable{})
	Adjust("st", &st)
}

// A call to testvalue.Adjust doesn't need to actually change anything.
// It can be used to synchronise the test with asynchronous execution of production code.
// This test verifies this behavior.
func TestCanUseAdjustToSynchronise(t *testing.T) {
	defer Reset()

	ready := make(chan interface{})
	proceed := make(chan interface{})

	go func() {
		// This is the asynchronous code ("server") we want to synchronise with.
		var z bool
		Adjust("sync_point", &z)
	}()

	SetCallback("sync_point", func() interface{} {
		// This function is called when the "server" runs.
		// So, notify the test that we're ready (i.e. we're blocked in the server code).
		ready <- struct{}{}

		// Wait for the test to tell us to continue execution.
		<-proceed
		return false
	})

	// In the test, we wait until we're blocked (ready)...
	<-ready
	// We could modify some external state here, while the "server" is paused.
	// Then we let the server continue.
	proceed <- struct{}{}
}
