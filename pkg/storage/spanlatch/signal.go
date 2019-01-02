// Copyright 2018 The Cockroach Authors.
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

package spanlatch

import (
	"sync/atomic"
	"unsafe"
)

const (
	// not yet signaled.
	noSig int32 = iota
	// signaled and the channel was not closed.
	sig
	// signaled and the channel was closed.
	sigClosed
)

// signal is a type that can signal the completion of an operation.
//
// The type has three benefits over using a channel directly and
// closing the channel when the operation completes:
// 1. signaled() uses atomics to provide a fast-path for checking
//    whether the operation has completed. It is ~75x faster than
//    using a channel for this purpose.
// 2. the receiver's channel is lazily initialized when signalChan()
//    is called, avoiding the allocation when one is not needed.
// 3. because of 2, the type's zero value can be used directly.
//
type signal struct {
	a int32
	c unsafe.Pointer // chan struct{}, lazily initialized
}

func (s *signal) signal() {
	if !atomic.CompareAndSwapInt32(&s.a, noSig, sig) {
		panic("signaled twice")
	}
	// Close the channel if it was ever initialized.
	if cPtr := atomic.LoadPointer(&s.c); cPtr != nil {
		// Coordinate with signalChan to avoid double-closing.
		if atomic.CompareAndSwapInt32(&s.a, sig, sigClosed) {
			close(ptrToChan(cPtr))
		}
	}
}

func (s *signal) signaled() bool {
	return atomic.LoadInt32(&s.a) > noSig
}

func (s *signal) signalChan() <-chan struct{} {
	// If the signal has already been signaled, return a closed channel.
	if s.signaled() {
		return closedC
	}

	// If the signal's channel has already been lazily initialized, return it.
	if cPtr := atomic.LoadPointer(&s.c); cPtr != nil {
		return ptrToChan(cPtr)
	}

	// Lazily initialize the channel.
	c := make(chan struct{})
	if !atomic.CompareAndSwapPointer(&s.c, nil, chanToPtr(c)) {
		// We raced with another initialization.
		return ptrToChan(atomic.LoadPointer(&s.c))
	}

	// Coordinate with signal to close the new channel, if necessary.
	if atomic.CompareAndSwapInt32(&s.a, sig, sigClosed) {
		close(c)
	}
	return c
}

func chanToPtr(c chan struct{}) unsafe.Pointer {
	return *(*unsafe.Pointer)(unsafe.Pointer(&c))
}

func ptrToChan(p unsafe.Pointer) chan struct{} {
	return *(*chan struct{})(unsafe.Pointer(&p))
}

var closedC chan struct{}

func init() {
	closedC = make(chan struct{})
	close(closedC)
}
