// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangemvcc

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestDataDriven(t *testing.T) {
	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		var h mvccHistory
		var breakpoint bool
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			if breakpoint {
				breakpoint = false // set your breakpoint on this line
			}
			switch d.Cmd {
			case "breakpoint":
				breakpoint = true
			case "nop":
				h.allocTS()
				return fmt.Sprintf("ts=%d", h.seq)
			case "put":
				seq := h.seq + 1
				v := fmt.Sprintf("v%d", seq)
				require.Equal(t, seq, h.Put(v))
				return fmt.Sprintf("ts=%d", seq)
			case "get":
				lo := 1
				hi := h.seq
				if len(d.CmdArgs) > 0 {
					d.CmdArgs[0].Scan(t, 0, &hi)
					lo = hi
				}
				var buf bytes.Buffer
				for i := lo; i <= hi; i++ {
					if lo != hi {
						fmt.Fprintf(&buf, "get(%d): ", i)
					}
					s := h.Get(i).v
					if s == "" {
						s = "-"
					}
					buf.WriteString(s)
					if lo != hi {
						fmt.Fprint(&buf, "\n")
					}
				}
				return buf.String()
			case "revert_to":
				var ts int
				d.CmdArgs[0].Scan(t, 0, &ts)
				seq := h.Revert(ts)
				return fmt.Sprintf("ts=%d", seq)
			default:
				return "unknown command"
			}
			return ""
		})
	})
}
