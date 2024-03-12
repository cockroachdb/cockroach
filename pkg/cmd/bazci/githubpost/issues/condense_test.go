// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package issues

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type condenseTestCase struct {
	input     string
	digest    string
	lastLines int
}

const fiveLines = `info1
info2
info3
info4
info5
`

const messagePanic = `panic: boom [recovered]
panic: boom
`

const messageFatal = `F191210 10:35:27.740990 196365 foo/bar.go:276  [some,tags] something broke, badly:

everything is broken!
`

const firstStack = `goroutine 1 [running]:
main.main.func2()
	/Users/tschottdorf/go/src/github.com/tbg/goplay/panic/main.go:15 +0x43
panic(0x1061be0, 0x1087db0)
	/usr/local/Cellar/go/1.13.4/libexec/src/runtime/panic.go:679 +0x1b2
main.main()
	/Users/tschottdorf/go/src/github.com/tbg/goplay/panic/main.go:17 +0x92
`
const restStack = `
goroutine 5 [sleep]:
runtime.goparkunlock(...)
	/usr/local/Cellar/go/1.13.4/libexec/src/runtime/proc.go:310
time.Sleep(0x3b9aca00)
	/usr/local/Cellar/go/1.13.4/libexec/src/runtime/time.go:105 +0x157
main.main.func1()
	/Users/tschottdorf/go/src/github.com/tbg/goplay/panic/main.go:10 +0x2a
created by main.main
	/Users/tschottdorf/go/src/github.com/tbg/goplay/panic/main.go:9 +0x42

goroutine 6 [sleep]:
runtime.goparkunlock(...)
	/usr/local/Cellar/go/1.13.4/libexec/src/runtime/proc.go:310
time.Sleep(0x3b9aca00)
	/usr/local/Cellar/go/1.13.4/libexec/src/runtime/time.go:105 +0x157
main.main.func1()
	/Users/tschottdorf/go/src/github.com/tbg/goplay/panic/main.go:10 +0x2a
created by main.main
	/Users/tschottdorf/go/src/github.com/tbg/goplay/panic/main.go:9 +0x42

goroutine 7 [runnable]:
time.Sleep(0x3b9aca00)
	/usr/local/Cellar/go/1.13.4/libexec/src/runtime/time.go:100 +0x109
main.main.func1()
	/Users/tschottdorf/go/src/github.com/tbg/goplay/panic/main.go:10 +0x2a
created by main.main
	/Users/tschottdorf/go/src/github.com/tbg/goplay/panic/main.go:9 +0x42
exit status 2
`

const rsgCrash = `    rsg_test.go:755: Crash detected: server panic: pq: internal error: something bad
		SELECT
			foo
		FROM
			bar
		LIMIT
			33:::INT8;
`

const rsgRepro = `    rsg_test.go:575: To reproduce, use schema:
    rsg_test.go:577: 
        	CREATE TABLE table1 (col1_0 BOOL);
        ;
    rsg_test.go:577: 
        CREATE TYPE greeting AS ENUM ('hello', 'hi');
        ;
    rsg_test.go:579: 
`

const panic5Lines = fiveLines + messagePanic + firstStack + restStack
const fatal5Lines = fiveLines + messageFatal + firstStack + restStack
const crashAndRepro = fiveLines + rsgCrash + fiveLines + rsgRepro

var errorCases = []condenseTestCase{
	{

		panic5Lines,
		fiveLines + messagePanic + firstStack,
		100,
	},
	{

		panic5Lines,
		"info5\n" + messagePanic + firstStack,
		1,
	},
	{

		panic5Lines,
		messagePanic + firstStack,
		0,
	},
	{
		fatal5Lines,
		fiveLines + messageFatal + firstStack,
		100,
	},
	{
		fatal5Lines,
		messageFatal + firstStack,
		0,
	},
	{
		crashAndRepro,
		rsgCrash + rsgRepro,
		100,
	},
	{
		crashAndRepro,
		"    rsg_test.go:755: Crash detected: server panic: pq: internal error: something bad\n",
		0,
	},
}

var lineCases = []condenseTestCase{
	{
		``,
		``,
		5,
	},
	{
		`foo
bar
baz
`,
		`baz
`,
		1,
	},
	{
		`foo
bar
baz
`,
		`bar
baz
`,
		2,
	},
	{
		`foo
bar
baz
`,
		`foo
bar
baz
`,
		3,
	},
	{
		`foo
bar
baz
`,
		`foo
bar
baz
`,
		4,
	},
	{
		`foo
bar
baz
`,
		`foo
bar
baz
`,
		100,
	},
}

func TestCondense(t *testing.T) {
	run := func(t *testing.T, tc condenseTestCase) {
		require.Equal(t, tc.digest, CondensedMessage(tc.input).Digest(tc.lastLines))
	}

	for _, tc := range lineCases {
		t.Run("line", func(t *testing.T) {
			run(t, tc)
		})
	}
	for _, tc := range errorCases {
		t.Run("panic", func(t *testing.T) {
			run(t, tc)
		})
	}
}
