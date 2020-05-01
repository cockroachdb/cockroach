// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package grpcutil

import (
	"regexp"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
	"github.com/petermattis/goid"
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

	testutils.RunTrueAndFalse(t, "formatMatch", func(t *testing.T, formatMatch bool) {
		testutils.RunTrueAndFalse(t, "argsMatch", func(t *testing.T, argsMatch bool) {
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

			if !alwaysPrint {
				// Force printing by pretending the previous output was well in the
				// past.
				spamMu.Lock()
				spamMu.gids[goid.Get()] = timeutil.Now().Add(-time.Hour)
				spamMu.Unlock()
			}
			if !curriedShouldPrint() {
				t.Error("expected third call to print")
			}
		})
	})
}
