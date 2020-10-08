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
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
)

func TestShouldPrint(t *testing.T) {
	testutils.RunTrueAndFalse(t, "argsMatch", func(t *testing.T, argsMatch bool) {
		msg := "blablabla"
		if argsMatch {
			msg = "grpc: addrConn.createTransport failed to connect to 127.0.0.1:1234 (connection refused)"
		}

		args := []interface{}{msg}
		curriedShouldPrint := func() bool {
			return shouldPrintWarning(args...)
		}

		// First call should always print.
		if !curriedShouldPrint() {
			t.Error("1st call: should print expected true, got false")
		}

		// Should print if non-matching.
		alwaysPrint := !argsMatch
		if alwaysPrint {
			if !curriedShouldPrint() {
				t.Error("2nd call: should print expected true, got false")
			}
		} else {
			if curriedShouldPrint() {
				t.Error("2nd call: should print expected false, got true")
			}
		}

		if !alwaysPrint {
			// Force printing by pretending the previous output was well in the
			// past.
			spamMu.Lock()
			spamMu.strs[msg] = timeutil.Now().Add(-time.Hour)
			spamMu.Unlock()
		}
		if !curriedShouldPrint() {
			t.Error("3rd call (after reset): should print expected true, got false")
		}
	})
}
