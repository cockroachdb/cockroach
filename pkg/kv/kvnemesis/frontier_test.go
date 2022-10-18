// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
//

package kvnemesis

import (
	"fmt"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/echotest"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestFrontier(t *testing.T) {
	defer leaktest.AfterTest(t)()

	w := echotest.Walk(t, testutils.TestDataPath(t, t.Name()))
	defer w.Check(t)

	for _, tc := range []struct {
		name string
		adds []lastWrite
	}{
		{
			name: "single span",
			adds: []lastWrite{
				{
					Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					idx:  1,
					ts:   hlc.Timestamp{WallTime: 123},
				},
			},
		},
	} {
		w.Do(t, tc.name, func(t *testing.T, path string) {
			// Flatten all start and end keys in a slice, sort them, and
			// assign them indexes (used for printing pretty pictures).
			var ks []string
			for _, w := range tc.adds {
				ks = append(ks, string(w.Key))
				if len(w.EndKey) > 0 {
					ks = append(ks, string(w.EndKey))
				}
			}
			sort.Strings(ks)

			k2indent := map[string]int{} // key to indent
			var indent int
			for _, k := range ks {
				if _, ok := k2indent[k]; !ok {
					k2indent[k] = indent
					indent += len(k)
				}
			}

			var buf strings.Builder
			for _, w := range tc.adds {
				k := string(w.Key)
				ek := string(w.EndKey)
				if ek == "" {
					fmt.Fprintf(&buf, "%s%s", strings.Repeat(" ", k2indent[k]), k)
				} else {
					fmt.Fprintf(&buf, "%s%s%s%s", strings.Repeat(" ", k2indent[k]), k, strings.Repeat("-", k2indent[ek]-k2indent[k]), ek)
				}
			}
			echotest.Require(t, buf.String(), path)
		})
	}

}
