// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package schemaexpr

import (
	"bytes"
	"fmt"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
)

func TestParseComputedColumnRewrites(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	path := datapathutils.TestDataPath(t, "computed_column_rewrites")
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
