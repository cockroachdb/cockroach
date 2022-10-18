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
	"strconv"
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

	mkW := func(k, ek string, ts int64) lastWrite {
		return lastWrite{
			Span: roachpb.Span{Key: roachpb.Key(k), EndKey: roachpb.Key(ek)},
			idx:  0, // for now
			ts:   hlc.Timestamp{WallTime: ts},
		}

	}
	c := func(ws ...lastWrite) []lastWrite {
		return ws
	}

	w := echotest.Walk(t, testutils.TestDataPath(t, t.Name()))
	defer w.Check(t)

	const (
		ts1 = 1 + iota
		ts2
		ts3
	)

	for _, tc := range []struct {
		name string
		adds []lastWrite
	}{
		{
			name: "single point",
			adds: c(mkW("a", "", ts1)),
		},
		{
			name: "single span",
			adds: c(mkW("a", "b", ts1)),
		},
		{
			name: "non-overlapping spans",
			adds: c(
				mkW("a", "b", ts1),
				mkW("b", "d", ts2),
				mkW("f", "z", ts3),
			),
		},
		{
			name: "identical points",
			adds: c(
				mkW("a", "", ts1),
				mkW("a", "", ts2),
			),
		},
		{
			name: "identical spans",
			adds: c(
				mkW("b", "c", ts1),
				mkW("b", "c", ts2),
			),
		},
		{
			name: "second covering, extending left",
			adds: c(
				mkW("b", "c", ts1),
				mkW("a", "c", ts2),
			),
		},
		{
			name: "second covering, extending right",
			adds: c(
				mkW("b", "c", ts1),
				mkW("b", "d", ts2),
			),
		},
	} {
		t.Run(tc.name, w.Do(t, tc.name, func(t *testing.T, path string) {
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
					indent += len(k)
					k2indent[k] = indent
					indent += 1
				}
			}
			t.Log(k2indent)
			indent += 3

			var buf strings.Builder
			for _, w := range tc.adds {
				k := string(w.Key)
				ek := string(w.EndKey)
				pk := k2indent[k]
				var pek int
				if ek != "" {
					pek = k2indent[ek] - pk
				}
				pr := indent - pk - pek
				fmt.Fprintf(&buf, "%"+strconv.Itoa(pk)+"s", k)
				if ek != "" {
					fmt.Fprintf(&buf, "%s%s", strings.Repeat("-", pek-len(ek)), ek)
				}
				fmt.Fprintf(&buf, "%"+strconv.Itoa(pr)+"s", "") // just pad
				fmt.Fprintf(&buf, "<-- ts=%d\n", w.ts.WallTime)
			}
			echotest.Require(t, buf.String(), path)
		}))
	}
}
