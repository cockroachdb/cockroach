// Copyright 2016 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlshell

import (
	"bufio"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/cli/clicfg"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlclient"
	"github.com/cockroachdb/cockroach/pkg/cli/clisqlexec"
	"github.com/cockroachdb/cockroach/pkg/sql/scanner"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/assert"
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
