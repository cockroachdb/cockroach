// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package clisqlshell

import (
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestRuneOffset(t *testing.T) {
	defer leaktest.AfterTest(t)()

	td := []struct {
		t    string
		l, c int
		exp  int
	}{
		{"", 0, 0, 0},
		{"\n", 1, 0, 1},
		{"", 0, 1, 0},
		{"", 0, 2, 0},
		{"ab", 0, 4, 2},
		{"abc\nde\nfgh", 1, 1, 5},
		{"⏩⏩\n⏩", 1, 1, 4},
	}

	for _, tc := range td {
		lines := strings.Split(tc.t, "\n")
		text := make([][]rune, len(lines))
		for i, l := range lines {
			text[i] = []rune(l)
		}

		offset := runeOffset(text, tc.l, tc.c)
		if offset != tc.exp {
			t.Errorf("%q (%d,%d): expected %d, got %d\n%+v", tc.t, tc.l, tc.c, tc.exp, offset, text)
		}
	}
}
