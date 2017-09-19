// Copyright 2013 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package singleflight

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestDo(t *testing.T) {
	var g Group
	v, _, err := g.Do("key", func() (interface{}, error) {
		return "bar", nil
	})
	res := Result{Val: v, Err: err}
	assertRes(t, res, false)
}

func TestDoChan(t *testing.T) {
	var g Group
	resC, leader := g.DoChan("key", func() (interface{}, error) {
		return "bar", nil
	})
	if !leader {
		t.Errorf("DoChan returned not leader, expected leader")
	}
	res := <-resC
	assertRes(t, res, false)
}

func TestDoErr(t *testing.T) {
	var g Group
	someErr := errors.New("Some error")
	v, _, err := g.Do("key", func() (interface{}, error) {
		return nil, someErr
	})
	if err != someErr {
		t.Errorf("Do error = %v; want someErr %v", err, someErr)
	}
	if v != nil {
		t.Errorf("unexpected non-nil value %#v", v)
	}
}

func TestDoDupSuppress(t *testing.T) {
	var g Group
	var wg1, wg2 sync.WaitGroup
	c := make(chan string, 1)
	var calls int32
	fn := func() (interface{}, error) {
		if atomic.AddInt32(&calls, 1) == 1 {
			// First invocation.
			wg1.Done()
		}
		v := <-c
		c <- v // pump; make available for any future calls

		time.Sleep(10 * time.Millisecond) // let more goroutines enter Do

		return v, nil
	}

	const n = 10
	wg1.Add(1)
	for i := 0; i < n; i++ {
		wg1.Add(1)
		wg2.Add(1)
		go func() {
			defer wg2.Done()
			wg1.Done()
			v, _, err := g.Do("key", fn)
			if err != nil {
				t.Errorf("Do error: %v", err)
				return
			}
			if s, _ := v.(string); s != "bar" {
				t.Errorf("Do = %T %v; want %q", v, v, "bar")
			}
		}()
	}
	wg1.Wait()
	// At least one goroutine is in fn now and all of them have at
	// least reached the line before the Do.
	c <- "bar"
	wg2.Wait()
	if got := atomic.LoadInt32(&calls); got <= 0 || got >= n {
		t.Errorf("number of calls = %d; want over 0 and less than %d", got, n)
	}
}

func TestDoChanDupSuppress(t *testing.T) {
	c := make(chan struct{})
	fn := func() (interface{}, error) {
		<-c
		return "bar", nil
	}

	var g Group
	resC1, leader1 := g.DoChan("key", fn)
	if !leader1 {
		t.Errorf("DoChan returned not leader, expected leader")
	}

	resC2, leader2 := g.DoChan("key", fn)
	if leader2 {
		t.Errorf("DoChan returned leader, expected not leader")
	}

	close(c)
	for _, res := range []Result{<-resC1, <-resC2} {
		assertRes(t, res, true)
	}
}

func assertRes(t *testing.T, res Result, expectShared bool) {
	if got, want := fmt.Sprintf("%v (%T)", res.Val, res.Val), "bar (string)"; got != want {
		t.Errorf("Res.Val = %v; want %v", got, want)
	}
	if res.Err != nil {
		t.Errorf("Res.Err = %v", res.Err)
	}
	if res.Shared != expectShared {
		t.Errorf("Res.Shared = %t; want %t", res.Shared, expectShared)
	}
}
