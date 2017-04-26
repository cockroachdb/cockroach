// Copyright 2017 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.

package grpcutil

import (
	"errors"
	"regexp"
	"testing"
	"time"
)

func TestShouldPrint(t *testing.T) {
	const duration = 10 * time.Millisecond

	formatRe, err := regexp.Compile("^foo")
	if err != nil {
		t.Fatal(err)
	}
	argRe, err := regexp.Compile("[a-z][0-9]")
	if err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		format      string
		args        []interface{}
		alwaysPrint bool
	}{
		// format: no match, args: no match
		{format: "bar=%s", args: []interface{}{errors.New("baz")}, alwaysPrint: true},
		// format: match, args: no match
		{format: "foobar=%s", args: []interface{}{errors.New("baz")}, alwaysPrint: true},
		// format: no match, args: match
		{format: "bar=%s", args: []interface{}{errors.New("a1")}, alwaysPrint: true},
		// format: match, args: match
		{format: "foobar=%s", args: []interface{}{errors.New("a1")}, alwaysPrint: false},
	} {
		curriedShouldPrint := func() bool {
			return shouldPrint(formatRe, argRe, duration, tc.format, tc.args...)
		}

		// First call should always print.
		if !curriedShouldPrint() {
			t.Errorf("expected first call to print: %v", tc)
		}

		// Call from another goroutine should always print.
		done := make(chan bool)
		go func() {
			done <- curriedShouldPrint()
		}()
		if !<-done {
			t.Errorf("expected other-goroutine call to print: %v", tc)
		}

		// Should print if non-matching.
		if didPrint := curriedShouldPrint(); didPrint != tc.alwaysPrint {
			t.Errorf("expected second call print=%t, got print=%t: %v", tc.alwaysPrint, didPrint, tc)
		}

		// Should print after sleep.
		if !tc.alwaysPrint {
			time.Sleep(duration)
		}
		if !curriedShouldPrint() {
			t.Errorf("expected third call to print: %v", tc)
		}
	}
}
