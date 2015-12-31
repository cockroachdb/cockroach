// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package util

import (
	"net"
	"testing"
	"time"
)

// verifyAddr starts a server listener at the specified addr and
// then dials a client to verify a connection is established.
func verifyAddr(addr net.Addr, t *testing.T) {
	ln, err := net.Listen(addr.Network(), addr.String())
	if err != nil {
		t.Error(err)
		return
	}

	acceptChan := make(chan struct{})
	go func() {
		_, err := ln.Accept()
		if err != nil {
			t.Error(err)
		}
		close(acceptChan)
	}()

	addr = ln.Addr()
	conn, err := net.Dial(addr.Network(), addr.String())
	if err != nil {
		t.Errorf("could not connect to %s", addr)
		return
	}
	select {
	case <-acceptChan:
		// success.
	case <-time.After(500 * time.Millisecond):
		t.Error("timeout waiting for client connection after 500ms")
	}
	conn.Close()
}

func TestCreateTestAddr(t *testing.T) {
	verifyAddr(CreateTestAddr("unix"), t)
	verifyAddr(CreateTestAddr("tcp"), t)
}

func TestTrueWithin(t *testing.T) {
	// Start with a function that never returns true.
	if err := IsTrueWithin(func() bool { return false }, 1*time.Millisecond); err == nil {
		t.Error("expected true within to fail on method which always returns false")
	}
	// Try a method which always returns true.
	if err := IsTrueWithin(func() bool { return true }, 1*time.Millisecond); err != nil {
		t.Errorf("unexpected error on method which always returns true: %v", err)
	}
	// Try a method which returns true on 5th invocation.
	count := 0
	if err := IsTrueWithin(func() bool { count++; return count >= 5 }, 1*time.Millisecond); err != nil {
		t.Errorf("unexpected error on method which returns true after 5 invocations: %v", err)
	}
}

func TestSucceedsWithin(t *testing.T) {
	// Try a method which always succeeds.
	SucceedsWithin(t, 1*time.Millisecond, func() error { return nil })

	// Try a method which succeeds after a known duration.
	start := time.Now()
	duration := time.Millisecond
	SucceedsWithin(t, 100*duration, func() error {
		elapsed := time.Since(start)
		if elapsed > duration {
			return nil
		}
		return Errorf("%s elapsed, waiting until %s elapses", elapsed, duration)
	})
}
