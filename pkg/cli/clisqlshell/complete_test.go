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

	"github.com/knz/bubbline/computil"
)

func TestRuneOffset(t *testing.T) {
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

func TestByteOffsetToRuneOffset(t *testing.T) {
	td := []struct {
		t string
		// cursor position.
		l, c int
		// test input byte offset.
		bo int
		// expected output rune offset.
		exp int
	}{
		{``, 0, 0, 0, 0},
		{``, 0, 0, 10, 0},
		{`abc`, 0, 1, 0, 0},
		{`abc`, 0, 1, 3, 3},
		{`⏩⏩⏩`, 0, 1, 3, 1}, // 3 UTF-8 bytes needed to encode "⏩".
		{`⏩⏩⏩`, 0, 1, 6, 2},
		{`⏩⏩⏩`, 0, 1, 9, 3},
		{`⏩⏩⏩`, 0, 0, 3, 1},
		{`⏩⏩⏩`, 0, 0, 6, 2},
		{`⏩⏩⏩`, 0, 0, 9, 3},
		{`⏩⏩⏩`, 0, 2, 3, 1},
		{`⏩⏩⏩`, 0, 2, 6, 2},
		{`⏩⏩⏩`, 0, 2, 9, 3},
	}

	for _, tc := range td {
		lines := strings.Split(tc.t, "\n")
		text := make([][]rune, len(lines))
		for i, l := range lines {
			text[i] = []rune(l)
		}
		cursorRuneOffset := runeOffset(text, tc.l, tc.c)
		sql, cursorByteOffset := computil.Flatten(text, tc.l, tc.c)

		runeOffset := byteOffsetToRuneOffset(sql, cursorRuneOffset, cursorByteOffset, tc.bo)
		if runeOffset != tc.exp {
			t.Errorf("%q rune cursor at 2D (%d, %d) 1D %d -> bytes %+v cursor at byte offset %d\n"+
				"converting byte offset %d: expected rune offset %d, got %d",
				tc.t, tc.l, tc.c, cursorRuneOffset, []byte(sql), cursorByteOffset,
				tc.bo, tc.exp, runeOffset)
		}
	}
}
