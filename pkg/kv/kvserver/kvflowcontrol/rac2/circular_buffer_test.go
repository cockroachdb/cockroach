// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rac2

import (
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
)

func TestCircularBuffer(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	cb := CircularBuffer[int]{}
	cbString := func() string {
		var b strings.Builder
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
		var first, count int
		printInterval := func() {
			if count == 0 {
				return
			}
			if count == 1 {
				fmt.Fprintf(&b, " %d", first)
			} else {
				fmt.Fprintf(&b, " [%d, %d]", first, first+count-1)
			}
			first = 0
			count = 0
		}
		for i := 0; i < cb.Length(); i++ {
			if count > 0 && cb.At(i) != (first+count) {
				// Cannot continue a run.
				printInterval()
			}
			if count == 0 {
				first = cb.At(i)
			}
			count++
		}
		printInterval()
		fmt.Fprintf(&b, "\n")
		printStats()
		return b.String()
	}
	datadriven.RunTest(t, "testdata/circular_buffer",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				cb = CircularBuffer[int]{}
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
