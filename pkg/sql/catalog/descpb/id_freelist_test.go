// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package descpb

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
)

func TestIDFreelist(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.Walk(t, "testdata/idfreelist", func(t *testing.T, path string) {
		var fl IDFreeList
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "new-free-list":
				fl = IDFreeList{}
				return ""
			case "insert":
				var elemStr string
				d.ScanArgs(t, "elems", &elemStr)
				for _, elem := range strings.Split(elemStr, ",") {
					i, err := strconv.Atoi(elem)
					if err != nil {
						return err.Error()
					}
					fl.AddElement(uint32(i))
				}
				return fl.DebugString()
			case "dump":
				var b strings.Builder
				count := 0
				for {
					next, ok := fl.GetNextFree()
					if !ok {
						break
					}
					if count > 0 {
						b.WriteString(" ")
					}
					b.WriteString(fmt.Sprintf("%d", next))
					count++
				}
				return b.String()
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}
