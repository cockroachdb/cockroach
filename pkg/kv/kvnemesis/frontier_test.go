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
			var buf strings.Builder
			fmt.Fprintln(&buf, "input:")
			fmt.Fprint(&buf, frontier(tc.adds))

			var f frontier
			for _, w := range tc.adds {
				f = f.Add(w)
			}
			fmt.Fprintln(&buf, "result:")
			fmt.Fprint(&buf, f)

			echotest.Require(t, buf.String(), path)
		}))
	}
}
