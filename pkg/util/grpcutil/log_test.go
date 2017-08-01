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
	"fmt"
	"regexp"
	"testing"
	"time"
)

func TestShouldPrint(t *testing.T) {
	const duration = 100 * time.Millisecond

	formatRe, err := regexp.Compile("^foo")
	if err != nil {
		t.Fatal(err)
	}
	argRe, err := regexp.Compile("[a-z][0-9]")
	if err != nil {
		t.Fatal(err)
	}

	for _, formatMatch := range []bool{false, true} {
		t.Run(fmt.Sprintf("formatMatch=%t", formatMatch), func(t *testing.T) {
			for _, argsMatch := range []bool{false, true} {
				t.Run(fmt.Sprintf("argsMatch=%t", argsMatch), func(t *testing.T) {
					format := "bar=%s"
					if formatMatch {
						format = "foobar=%s"
					}
					args := []interface{}{errors.New("baz")}
					if argsMatch {
						args = []interface{}{errors.New("a1")}
					}
					curriedShouldPrint := func() bool {
						return shouldPrint(formatRe, argRe, duration, format, args...)
					}

					// First call should always print.
					if !curriedShouldPrint() {
						t.Error("expected first call to print")
					}

					// Call from another goroutine should always print.
					done := make(chan bool)
					go func() {
						done <- curriedShouldPrint()
					}()
					if !<-done {
						t.Error("expected other-goroutine call to print")
					}

					// Should print if non-matching.
					alwaysPrint := !(formatMatch && argsMatch)

					if alwaysPrint {
						if !curriedShouldPrint() {
							t.Error("expected second call to print")
						}
					} else {
						if curriedShouldPrint() {
							t.Error("unexpected second call to print")
						}
					}

					// Should print after sleep.
					if !alwaysPrint {
						time.Sleep(duration)
					}
					if !curriedShouldPrint() {
						t.Error("expected third call to print")
					}
				})
			}
		})
	}
}
