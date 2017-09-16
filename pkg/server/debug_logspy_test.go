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
// permissions and limitations under the License.

package server

import (
	"reflect"
	"regexp"
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

func TestDebugLogSpyRun(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	send := make(chan log.InterceptorFn, 1)
	spy := logSpy{
		setIntercept: func(ctx context.Context, f log.InterceptorFn) error {
			send <- f
			return nil
		},
	}

	msgC := make(chan logSpyMsg, 10)
	entries := []log.Entry{
		{
			File:    "first.go",
			Line:    1,
			Message: "#1",
		},
		{
			File:    "nonmatching.go",
			Line:    12345,
			Message: "ignored because neither message nor file match",
		},
		{
			File:    "second.go",
			Line:    2,
			Message: "#2",
		},
	}

	// Launch a goroutine to run the logSpy task.
	go func() {
		spy.run(ctx, msgC, logSpyOptions{
			Duration: logSpyMaxDuration,
			Count:    3, // we expect 2 results but log spy sends a header event
			Grep:     regexp.MustCompile(`first\.go|#2`),
		})
	}()

	// Launch a goroutine to act as a log interceptor.
	done := make(chan struct{})
	go func() {
		f := <-send
		for _, entry := range entries {
			f(entry)
		}
		if undoF := <-send; undoF != nil {
			msgC <- logSpyMsg{err: errors.New("interceptor closed with non-nil function")}
			return
		}

		for i := 0; i < 10000; i++ {
			// f could be invoked arbitrarily after the operation finishes (though
			// in reality the duration would be limited to the blink of an eye). It
			// must not fill up a channel and block, or panic.
			f(log.Entry{})
		}

		close(done)
	}()

	// Read all logSpy entries from the msgC channel.
	expEntries := []log.Entry{entries[0], entries[2]}
	foundEntries := []log.Entry{}
	for msg := range msgC {
		if err := msg.err; err != nil {
			t.Fatal(err)
		}
		foundEntries = append(foundEntries, msg.entry)
	}

	// foundEntries will have an extra header entry, which we ignore when
	// performing the DeepEqual.
	if len(expEntries) != len(foundEntries)-1 || !reflect.DeepEqual(expEntries, foundEntries[1:]) {
		t.Fatalf("expected entries %+v, found entries %+v", expEntries, foundEntries)
	}

	// The extra calls to f should not block.
	<-done
}
