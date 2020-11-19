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
	"math"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/util/log/severity"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/stretchr/testify/require"
)

const filteredWarningExampleMsg = "grpc: addrConn.createTransport failed to " +
	"connect to 127.0.0.1:1234 (connection refused)"

func TestShouldPrint(t *testing.T) {
	testutils.RunTrueAndFalse(t, "argsMatch", func(t *testing.T, argsMatch bool) {
		msg := "blablabla"
		if argsMatch {
			msg = filteredWarningExampleMsg
		}

		l := getGRPCLogger(severity.WARNING, "", "")

		args := []interface{}{msg}
		curriedShouldPrint := func() bool {
			return l.shouldPrintWarning(0 /* depth */, args...)
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

func TestGRPCLogger(t *testing.T) {
	{
		// If input < env var severity, use input.
		l := getGRPCLogger(severity.INFO, "warning", "")
		require.Equal(t, l.sev, severity.INFO)
	}

	{
		// If env var smaller than info, use env var.
		l := getGRPCLogger(severity.ERROR, "info", "")
		require.Equal(t, l.sev, severity.INFO)
	}

	{
		// When threshold is ERROR, should not log WARNING or below.
		l := getGRPCLogger(severity.ERROR, "", "")
		require.False(t, l.shouldLog(severity.WARNING, 1 /* depth */))
		require.False(t, l.shouldLog(severity.INFO, 1 /* depth */))
		require.True(t, l.shouldLog(severity.ERROR, 1 /* depth */))
	}

	{
		// When threshold is warning, some warnings are stripped because
		// we consider them too noisy.
		l := getGRPCLogger(severity.ERROR, "warning", "")
		require.True(t, l.shouldPrintWarning(
			0 /* depth */, "i", "am", "harmless"),
		)
		deniedAtLeastOnce := false
		for i := 0; i < 1000; i++ {
			// The throttling here is stateful (rate limit based)
			// so we hit it a few times.
			shouldPrint := l.shouldPrintWarning(0 /* depth */, filteredWarningExampleMsg)
			deniedAtLeastOnce = deniedAtLeastOnce || !shouldPrint
		}
		require.True(t, deniedAtLeastOnce)
	}

	{
		// When verbosity is nonzero, l.V() should reflect this.
		//
		// Also, we log at all severities when verbosity is
		// not zero.
		//
		// Also, we don't filter any warnings.
		l := getGRPCLogger(severity.ERROR, "", "1000")
		require.Equal(t, l.grpcVerbosityLevel, 1000)
		require.True(t, l.V(1000))
		require.True(t, l.shouldLog(severity.INFO, 1 /* depth */))
		require.True(t, l.shouldLog(severity.WARNING, 1 /* depth */))
		require.True(t, l.shouldLog(severity.ERROR, 1 /* depth */))
		for i := 0; i < 1000; i++ {
			// The throttling here is stateful (rate limit based)
			// so we hit it a few times. It should want to log every
			// single time.
			require.True(t, l.shouldPrintWarning(0 /* depth */, filteredWarningExampleMsg))
		}
		// NB: this could fail if we run with `--vmodule=*=<maxint32>`
		// but the hope is that we don't do that.
		require.False(t, l.V(math.MaxInt32))
	}
}
