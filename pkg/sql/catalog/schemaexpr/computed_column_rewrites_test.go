// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package schemaexpr

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
)

func TestParseComputedColumnRewrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	path := "testdata/computed_column_rewrites"
	datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
		switch d.Cmd {
		case "parse":
			rewrites, err := ParseComputedColumnRewrites(d.Input)
			if err != nil {
				return fmt.Sprintf("error: %v", err)
			}
			var keys []string
			for k := range rewrites {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			var buf bytes.Buffer
			for _, k := range keys {
				fmt.Fprintf(&buf, "(%v) -> (%v)\n", k, tree.Serialize(rewrites[k]))
			}
			return buf.String()

		default:
			t.Fatalf("unsupported command %s", d.Cmd)
			return ""
		}
	})
}
