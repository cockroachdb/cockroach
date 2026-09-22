// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package issues

import (
	"fmt"
	"strings"
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

const raceDelim = "==================\n"

const dataRace = `WARNING: DATA RACE
Read at 0x00c0004e2a48 by goroutine 189:
  github.com/cockroachdb/cockroach/pkg/kv/kvserver.(*Replica).mu()
      pkg/kv/kvserver/replica.go:411 +0x64

Previous write at 0x00c0004e2a48 by goroutine 214:
  github.com/cockroachdb/cockroach/pkg/kv/kvserver.(*Store).Send()
      pkg/kv/kvserver/store_send.go:142 +0x1a4

Goroutine 189 (running) created at:
  github.com/cockroachdb/cockroach/pkg/util/stop.(*Stopper).RunAsyncTask()
      pkg/util/stop/stopper.go:494 +0x2c8
`

// trailingLog stands in for whatever the test binary printed after the race
// report. Digest used to return this and drop the report entirely.
const trailingLog = `    --- FAIL: TestFoo (4.20s)
    	testing.go:1490: race detected during execution of test
FAIL
`

const panic5Lines = fiveLines + messagePanic + firstStack + restStack
const fatal5Lines = fiveLines + messageFatal + firstStack + restStack
const crashAndRepro = fiveLines + rsgCrash + fiveLines + rsgRepro

var longTail = strings.Repeat("trailing log line\n", 60)

var errorCases = []condenseTestCase{
	{
		// The report survives more trailing output than the line window, which
		// used to crowd it out completely.
		raceDelim + dataRace + raceDelim + longTail,
		dataRace,
		50,
	},
	{
		fiveLines + raceDelim + dataRace + raceDelim + trailingLog,
		fiveLines + dataRace,
		100,
	},
	{
		fiveLines + raceDelim + dataRace + raceDelim + trailingLog,
		dataRace,
		0,
	},
	{
		// A report truncated by halt_on_error=1 killing the process, or by the
		// log being cut off, still yields the report.
		fiveLines + raceDelim + dataRace,
		fiveLines + dataRace,
		100,
	},
	{
		// A race outranks a panic that followed it.
		raceDelim + dataRace + raceDelim + messagePanic + firstStack,
		dataRace,
		0,
	},
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

// TestElideRepeatedStackFrames checks that the runaway stacks the race
// detector reconstructs for long-lived goroutines are cut down, that the
// frames nearest the racing access survive, that an ellipsis marks each gap,
// and that the line count is reported once per stack.
func TestElideRepeatedStackFrames(t *testing.T) {
	frame := func(name string, line int) string {
		return fmt.Sprintf("  pkg.%s()\n      pkg/f.go:%d +0x1\n", name, line)
	}
	top := frame("racingWrite", 1) + frame("caller", 2)
	// Two call sites in one loop body, stacked on themselves up to the frame cap.
	cycle := frame("loop", 3) + frame("loop", 4)
	kept := strings.Repeat(cycle, dataRaceStackMaxFrameCopies)
	const gap = "  [...]\n"
	omitted := func(n int) string {
		return fmt.Sprintf("  [%d repeated lines omitted; see full output]\n", n)
	}
	head := "WARNING: DATA RACE\nWrite at 0x0 by goroutine 1:\n"
	created := "\nGoroutine 1 (running) created at:\n"

	// The second stack resumes with a real frame after its gap, which the first
	// stack does not, so the two put the count in different places.
	report := head + top + strings.Repeat(cycle, 127) +
		created + strings.Repeat(cycle, 4) + frame("start", 5)
	require.Greater(t, len(strings.Split(report, "\n")), dataRaceReportLineLimit)

	require.Equal(t,
		head+top+kept+gap+omitted(124*4)+
			created+kept+gap+frame("start", 5)+omitted(1*4),
		elideRepeatedStackFrames(report))
}
