// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package ring

import (
	"fmt"
	"math"
	"math/rand"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestBuffer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cb := Buffer[int]{}
	cbString := func() string {
		var b strings.Builder
		// Clone cb and trash the clone's state. It should not affect cb.
		cbClone := cb.Clone()
		require.Equal(t, cbClone, cb)
		for i := 0; i < len(cbClone.buf); i++ {
			cbClone.buf[i] = -1
		}
		cbClone = Buffer[int]{}
		printStats := func() {
			fmt.Fprintf(&b, "first: %d len: %d cap: %d pushes: %d, max-len: %d\n",
				cb.first, cb.len, len(cb.buf), cb.pushesSinceCheck, cb.maxObservedLen)
		}
		fmt.Fprintf(&b, "buf:")
		if cb.Length() == 0 {
			fmt.Fprintf(&b, " empty\n")
			printStats()
			return b.String()
		}
		// We collapse the entries into intervals that represent a dense ascending
		// sequence. The interval is [first, first+count).
		var first, count int
		printIntervalAndReset := func() {
			if count == 0 {
				return
			}
			if count == 1 {
				// Don't use interval notation.
				fmt.Fprintf(&b, " %d", first)
			} else {
				fmt.Fprintf(&b, " [%d, %d]", first, first+count-1)
			}
			first = 0
			count = 0
		}
		for i := 0; i < cb.Length(); i++ {
			if count > 0 && cb.At(i) != (first+count) {
				// Cannot extend the current interval since the next entry is not
				// equal to first + count which is the exclusive end of the current
				// interval. So print the current interval and reset it to empty.
				printIntervalAndReset()
			}
			if count == 0 {
				// Start a new interval.
				first = cb.At(i)
			}
			// Add to the current interval.
			count++
		}
		// If there is an accumulated interval, print it.
		printIntervalAndReset()
		fmt.Fprintf(&b, "\n")
		printStats()
		return b.String()
	}
	datadriven.RunTest(t, "testdata/buffer",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				cb = Buffer[int]{}
				return ""

			case "push":
				var entry int
				d.ScanArgs(t, "entry", &entry)
				count := 1
				if d.HasArg("count") {
					d.ScanArgs(t, "count", &count)
				}
				pop := false
				if d.HasArg("pop-after-each-push-except-last") {
					pop = true
				}
				for i := 0; i < count; i++ {
					cb.Push(entry + i)
					if pop && i != count-1 {
						cb.Pop(1)
					}
				}
				return cbString()

			case "pop":
				var num int
				d.ScanArgs(t, "num", &num)
				cb.Pop(num)
				return cbString()

			case "shrink-to-prefix":
				var num int
				d.ScanArgs(t, "num", &num)
				cb.ShrinkToPrefix(num)
				return cbString()

			case "set-first":
				var entry int
				d.ScanArgs(t, "entry", &entry)
				cb.SetFirst(entry)
				return cbString()

			case "set-last":
				var entry int
				d.ScanArgs(t, "entry", &entry)
				cb.SetLast(entry)
				return cbString()

			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}

func TestBufferRandom(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Reference implementation of a queue.
	var queue []int
	push := func(x int) {
		queue = append(queue, x)
	}
	pop := func(count int) {
		require.LessOrEqual(t, count, len(queue))
		queue = queue[count:]
	}

	// Validate at each step that the reference queue matches the circular buffer.
	var b Buffer[int]
	check := func() {
		require.Equal(t, b.Length(), len(queue))
		for i, x := range queue {
			require.Equal(t, x, b.At(i))
		}
	}

	seed := timeutil.Now().UnixNano()
	t.Logf("Seed: %d", seed)
	rng := rand.New(rand.NewSource(seed))

	operate := func(pushSkew float64, minSize, maxSize int, popEach int) {
		// The number of planned items to pop.
		pops := 0
		// The countdown until the next pop.
		nextPop := 0

		schedule := func() {
			// mean == stddev == popEach
			nextPop = max(int(rng.NormFloat64()*float64(popEach)+float64(popEach)), 1)
		}
		schedule()

		for len(queue) > minSize && len(queue) < maxSize {
			// Push with probability == pushSkew, or otherwise "plan" a pop.
			if rng.Float64() < pushSkew { // push
				x := rng.Int()
				push(x)
				b.Push(x)
			} else if ln := len(queue) - pops; ln > 0 { // pop (if can)
				pops++
			}
			// Run a pop according to pop frequency preference.
			if nextPop--; nextPop <= 0 {
				pop(pops)
				b.Pop(pops)
				pops = 0
				schedule()
			}
			check()
		}
	}

	test := func(maxSize, popEach int) {
		b = Buffer[int]{}
		require.Zero(t, b.Length())
		check()
		// Operate the buffer with skew towards pushes, until it reaches the max size.
		operate(0.8 /* pushSkew */, math.MinInt, maxSize, popEach)
		require.Equal(t, b.Length(), maxSize)
		// Operate the buffer with skew towards pops, until it is emptied.
		operate(0.3 /* pushSkew */, 0, math.MaxInt, popEach)
		require.Zero(t, b.Length())
	}

	const maxSize = 1024 + 123
	t.Run("pop-single", func(t *testing.T) { test(maxSize, 1) })
	t.Run("pop-small", func(t *testing.T) { test(maxSize, 5) })
	t.Run("pop-med", func(t *testing.T) { test(maxSize, 25) })
	t.Run("pop-big", func(t *testing.T) { test(maxSize, 100) })
	t.Run("pop-most", func(t *testing.T) { test(3451, 2000) })
}
