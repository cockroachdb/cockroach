// Copyright 2016 The Cockroach Authors.
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
// Author: Nathan VanBenschoten (nvanbenschoten@gmail.com)

package util

import (
	"testing"
	"time"
)

func TestTimerTimeout(t *testing.T) {
	var timer Timer
	defer timer.Stop()
	timer = timer.Reset(1 * time.Millisecond)

	<-timer.C
	timer.Read = true

	select {
	case <-timer.C:
		t.Errorf("expected timer to only timeout once after Reset; got two timeouts")
	case <-time.After(5 * time.Millisecond):
	}
}

func TestTimerStop(t *testing.T) {
	var timer Timer
	timer = timer.Reset(1 * time.Millisecond)
	timer.Stop()

	select {
	case <-timer.C:
		t.Errorf("expected timer to stop after call to Stop; got timer that was not stopped")
	case <-time.After(5 * time.Millisecond):
	}
}

func TestTimerUninitializedStopNoop(t *testing.T) {
	var timer Timer
	timer.Stop()
}

func TestTimerResetBeforeTimeout(t *testing.T) {
	var timer Timer
	defer timer.Stop()
	timer = timer.Reset(1 * time.Millisecond)

	timer = timer.Reset(1 * time.Millisecond)
	<-timer.C
	timer.Read = true

	select {
	case <-timer.C:
		t.Errorf("expected timer to only timeout once after Reset; got two timeouts")
	case <-time.After(5 * time.Millisecond):
	}
}

func TestTimerResetAfterTimeoutAndNoRead(t *testing.T) {
	var timer Timer
	defer timer.Stop()
	timer = timer.Reset(1 * time.Millisecond)

	time.Sleep(2 * time.Millisecond)

	timer = timer.Reset(1 * time.Millisecond)
	<-timer.C
	timer.Read = true

	select {
	case <-timer.C:
		t.Errorf("expected timer to only timeout once after Reset; got two timeouts")
	case <-time.After(5 * time.Millisecond):
	}
}

func TestTimerResetAfterTimeoutAndRead(t *testing.T) {
	var timer Timer
	defer timer.Stop()
	timer = timer.Reset(1 * time.Millisecond)

	time.Sleep(2 * time.Millisecond)
	<-timer.C
	timer.Read = true

	timer = timer.Reset(1 * time.Millisecond)
	<-timer.C
	timer.Read = true

	select {
	case <-timer.C:
		t.Errorf("expected timer to only timeout once after Reset; got two timeouts")
	case <-time.After(5 * time.Millisecond):
	}
}

func TestTimerMakesProgressInLoop(t *testing.T) {
	var timer Timer
	defer timer.Stop()
	for i := 0; i < 5; i++ {
		timer = timer.Reset(1 * time.Millisecond)
		<-timer.C
		timer.Read = true
	}
}
