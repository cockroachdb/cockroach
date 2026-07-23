// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlshell

import (
	"bufio"
	"context"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/clicfg"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlexec"
	"github.com/cockroachdb/cockroach/pkg/sql/scanner"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIsEndOfStatement(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	tests := []struct {
		in         string
		isEnd      bool
		isNotEmpty bool
	}{
		{
			in:         ";",
			isEnd:      true,
			isNotEmpty: true,
		},
		{
			in:         "; /* comment */",
			isEnd:      true,
			isNotEmpty: true,
		},
		{
			in:         "; SELECT",
			isNotEmpty: true,
		},
		{
			in:         "SELECT",
			isNotEmpty: true,
		},
		{
			in:         "SET; SELECT 1;",
			isEnd:      true,
			isNotEmpty: true,
		},
		{
			in:         "SELECT ''''; SET;",
			isEnd:      true,
			isNotEmpty: true,
		},
		{
			in: "  -- hello",
		},
		{
			in:         "select 'abc", // invalid token
			isNotEmpty: true,
		},
		{
			in:         "'abc", // invalid token
			isNotEmpty: true,
		},
		{
			in:         `SELECT e'\xaa';`, // invalid token but last token is semicolon
			isEnd:      true,
			isNotEmpty: true,
		},
	}

	for _, test := range tests {
		lastTok, isNotEmpty := scanner.LastLexicalToken(test.in)
		if isNotEmpty != test.isNotEmpty {
			t.Errorf("%q: isNotEmpty expected %v, got %v", test.in, test.isNotEmpty, isNotEmpty)
		}
		isEnd := isEndOfStatement(lastTok)
		if isEnd != test.isEnd {
			t.Errorf("%q: isEnd expected %v, got %v", test.in, test.isEnd, isEnd)
		}
	}
}

func TestHandleCliCmdSlashDInvalidSyntax(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	clientSideCommandTests := []string{`\d goodarg badarg`}

	for _, tt := range clientSideCommandTests {
		c := setupTestCliState()
		c.lastInputLine = tt
		gotState := c.doHandleCliCmd(cliStateEnum(0), cliStateEnum(1))

		assert.Equal(t, cliStateEnum(0), gotState)
	}
}

func TestHandleDemoNodeCommandsInvalidNodeName(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	demoNodeCommandTests := []string{"shutdown", "*"}

	c := setupTestCliState()
	c.handleDemoNodeCommands(demoNodeCommandTests, cliStateEnum(0), cliStateEnum(1))
	assert.ErrorContains(t, c.exitErr, "invalid syntax")
}

func setupTestCliState() *cliState {
	cliCtx := &clicfg.Context{}
	sqlConnCtx := &clisqlclient.Context{CliCtx: cliCtx}
	sqlExecCtx := &clisqlexec.Context{
		CliCtx:             cliCtx,
		TableDisplayFormat: clisqlexec.TableDisplayTable,
	}
	sqlCtx := &Context{}
	c := NewShell(cliCtx, sqlConnCtx, sqlExecCtx, sqlCtx, nil).(*cliState)
	c.ins = &bufioReader{wout: os.Stdout, buf: bufio.NewReader(os.Stdin)}
	return c
}

func TestGetSetArgs(t *testing.T) {
	defer leaktest.AfterTest(t)()

	td := []struct {
		input    string
		ok       bool
		option   string
		hasValue bool
		value    string
	}{
		// Missing option.
		{``, false, ``, false, ``},
		// Missing option.
		{`    `, false, ``, false, ``},
		// Standalone option, also supporting various characters in the option.
		{`a`, true, `a`, false, ``},
		{`a.b`, true, `a.b`, false, ``},
		{`a_b`, true, `a_b`, false, ``},
		{`a-b`, true, `a-b`, false, ``},
		{`a123`, true, `a123`, false, ``},
		{`a/b`, true, `a/b`, false, ``},
		// Optional spaces.
		{`a   `, true, `a`, false, ``},
		{`  a   `, true, `a`, false, ``},
		// Simple values surrounded by spaces.
		{`a b`, true, `a`, true, `b`},
		{`a b    `, true, `a`, true, `b`},
		{`   a b    `, true, `a`, true, `b`},
		{`a    b`, true, `a`, true, `b`},
		{`a    b     `, true, `a`, true, `b`},
		// Quoted value.
		{`a "b c"`, true, `a`, true, `"b c"`},
		{`a   "b c"  `, true, `a`, true, `"b c"`},
		{`a   'b\"c'  `, true, `a`, true, `b"c`},
		{`a "" `, true, `a`, true, `""`},
		// Non-quoted value.
		{`a   b.c  `, true, `a`, true, `b.c`},
		// Equal sign with optional spaces.
		{` a=    b`, true, `a`, true, `b`},
		{` a=    b`, true, `a`, true, `b`},
		{` a    =    b`, true, `a`, true, `b`},
		{` a    =    b   `, true, `a`, true, `b`},
		{` a     =b`, true, `a`, true, `b`},
		{` a     =b  `, true, `a`, true, `b`},
		{` a     ="b c"  `, true, `a`, true, `"b c"`},
		{` a "=b"`, true, `a`, true, `"=b"`},
	}

	for _, tc := range td {
		args, err := scanLocalCmdArgs(tc.input)
		if err != nil {
			t.Errorf("%s: %v", tc.input, err)
			continue
		}
		ok, option, hasValue, value := getSetArgs(args)
		if ok != tc.ok || option != tc.option || hasValue != tc.hasValue || value != tc.value {
			t.Errorf("%s: expected (%v,%v,%v,%v), got (%v,%v,%v,%v)", tc.input,
				tc.ok, tc.option, tc.hasValue, tc.value,
				ok, option, hasValue, value)
		}
	}
}

func TestDisableHistory(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	c := setupTestCliState()
	c.cliCtx.IsInteractive = true
	c.sqlCtx.DisableHistory = true

	// configurePreShellDefaults needs *os.File for in/out/err but does
	// not read from them in this code path; a fresh pipe is sufficient.
	rIn, wIn, err := os.Pipe()
	require.NoError(t, err)
	defer func() { _ = rIn.Close(); _ = wIn.Close() }()

	cleanup, err := c.configurePreShellDefaults(rIn, os.Stdout, os.Stderr)
	require.NoError(t, err)
	defer cleanup()

	assert.Empty(t, c.iCtx.histFile,
		"DisableHistory=true should leave histFile unset")
}

func TestHandleEmbedderInterrupt(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	c := setupTestCliState()
	c.cliCtx.IsInteractive = true
	c.iCtx.stderr = os.Stderr // handleEmbedderInterrupt writes status here

	interruptCh := make(chan struct{}, 1)
	c.sqlCtx.InterruptCh = interruptCh

	// Simulate a query in flight: cancelFn signals it was called;
	// doneCh releases the wait loop afterwards.
	cancelCalled := make(chan struct{}, 1)
	doneCh := make(chan struct{})
	c.iCtx.mu.cancelFn = func(context.Context) error {
		cancelCalled <- struct{}{}
		return nil
	}
	c.iCtx.mu.doneCh = doneCh

	finalFn := c.maybeHandleInterrupt()
	defer finalFn()

	interruptCh <- struct{}{}
	select {
	case <-cancelCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("cancelFn was not invoked after InterruptCh write")
	}
	// Release the inner wait loop so the goroutine returns to its outer
	// select; finalFn() in the defer then exits the goroutine cleanly.
	close(doneCh)
}

func TestHandleEmbedderInterruptExitsDuringWait(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	c := setupTestCliState()
	c.cliCtx.IsInteractive = true
	c.iCtx.stderr = os.Stderr
	interruptCh := make(chan struct{}, 1)
	c.sqlCtx.InterruptCh = interruptCh

	// Simulate a query whose cancellation never resolves: cancelFn
	// runs but doneCh stays open. Without ctx.Done() in the inner
	// wait loop the goroutine would block for the full 3-second
	// timeout before observing the embedder's teardown.
	cancelCalled := make(chan struct{}, 1)
	doneCh := make(chan struct{})
	c.iCtx.mu.cancelFn = func(context.Context) error {
		cancelCalled <- struct{}{}
		return nil
	}
	c.iCtx.mu.doneCh = doneCh

	finalFn := c.maybeHandleInterrupt()

	interruptCh <- struct{}{}
	select {
	case <-cancelCalled:
	case <-time.After(2 * time.Second):
		t.Fatal("cancelFn was not invoked after InterruptCh write")
	}

	// finalFn blocks on goroutine exit; it must return well under the
	// 3-second wait-loop timeout.
	returned := make(chan struct{})
	go func() {
		finalFn()
		close(returned)
	}()
	select {
	case <-returned:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("finalFn did not return promptly after embedder teardown")
	}
}

func TestHandleEmbedderInterruptIgnoresIdle(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	c := setupTestCliState()
	c.cliCtx.IsInteractive = true
	c.iCtx.stderr = os.Stderr
	interruptCh := make(chan struct{}, 1)
	c.sqlCtx.InterruptCh = interruptCh
	// No cancelFn set: simulates "no query running".

	finalFn := c.maybeHandleInterrupt()
	defer finalFn()

	// A write must be silently dropped and not block subsequent writes.
	interruptCh <- struct{}{}
	time.Sleep(50 * time.Millisecond)
	interruptCh <- struct{}{}
	time.Sleep(50 * time.Millisecond)
}

func TestDisableUnsafeCmds(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	blocked := []string{
		`\!`,
		`\| sort`,
		`\i file.sql`,
		`\ir file.sql`,
		`\o output`,
		// \statement-diag download writes a zip to the local FS via a
		// gate inside handleStatementDiag; \statement-diag list stays
		// enabled because it only issues a server-side query.
		`\statement-diag download 42`,
		// Unknown commands are rejected by the allow-list rather than
		// falling through to the default "invalid syntax" path. This
		// is the fail-closed property: a future metacommand that
		// touches the local FS is blocked until it is added to
		// embedderSafeCmds.
		`\unknown-future-command`,
	}
	for _, line := range blocked {
		c := setupTestCliState()
		c.sqlCtx.DisableUnsafeCmds = true
		c.lastInputLine = line
		c.doHandleCliCmd(cliStateEnum(0), cliStateEnum(1))
		require.Errorf(t, c.exitErr, "input %q", line)
		assert.Containsf(t, c.exitErr.Error(), "disabled by embedder", "input %q", line)
	}

	// Positive control: \echo is in embedderSafeCmds and does not
	// require a conn, so it must run cleanly under DisableUnsafeCmds.
	c := setupTestCliState()
	c.sqlCtx.DisableUnsafeCmds = true
	c.lastInputLine = `\echo hi`
	c.doHandleCliCmd(cliStateEnum(0), cliStateEnum(1))
	require.NoError(t, c.exitErr, "\\echo must be allowed under DisableUnsafeCmds")

	// Describe-family commands route to handleDescribe above the
	// allow-list gate. They are not in embedderSafeCmds because the
	// gate doesn't see them; putting them in the set would suggest
	// duplicated logic and could mask future routing bugs.
	for _, line := range []string{`\d`, `\dt`, `\dT+`, `\du`, `\dv`, `\sf`, `\sv`, `\l`} {
		_, present := embedderSafeCmds[line]
		assert.False(t, present,
			"%s is dispatched to handleDescribe; it should not appear in embedderSafeCmds", line)
	}
}

func TestDisablePasswordCmd(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	c := setupTestCliState()
	c.sqlCtx.DisablePasswordCmd = true
	c.lastInputLine = `\password`
	c.doHandleCliCmd(cliStateEnum(0), cliStateEnum(1))
	require.Error(t, c.exitErr)
	assert.Contains(t, c.exitErr.Error(), "disabled by embedder")
}
