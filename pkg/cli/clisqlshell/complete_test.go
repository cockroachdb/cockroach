// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlshell

import (
	"context"
	gosql "database/sql"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/lexbase"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/knz/bubbline/computil"
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

func TestByteOffsetToRuneOffset(t *testing.T) {
	defer leaktest.AfterTest(t)()

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

// TestCompletions exercises the translation from the server-side
// completion engine and the bubbline completions metadata.
func TestCompletions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	datadriven.Walk(t, "testdata/complete", func(t *testing.T, path string) {
		ctx := context.Background()
		s, db, _ := serverutils.StartServer(t, base.TestServerArgs{})
		defer s.Stopper().Stop(ctx)

		datadriven.RunTest(t, path, func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "sql":
				_, err := db.Exec(td.Input)
				if err != nil {
					t.Fatalf("%s: sql error: %v", td.Pos, err)
				}
				return "ok"

			case "complete":
				// Translate runes to input. We use a '@' sign as cursor position.
				var input [][]rune
				var cursorLine, cursorColumn int
				for thisline, line := range strings.Split(td.Input, "\n") {
					thiscol := 0
					var rline []rune
					for _, r := range line {
						if r == '@' {
							cursorLine = thisline
							cursorColumn = thiscol
						} else {
							rline = append(rline, r)
						}
						thiscol++
					}
					input = append(input, rline)
				}
				// Generate the completions.
				b := bubblineReader{sql: mockShell{db: db}}
				msg, comps := b.getCompletions(input, cursorLine, cursorColumn)

				// Report what we're seeing.
				var out strings.Builder
				fmt.Fprintf(&out, "complete %d %d\n", cursorLine, cursorColumn)
				fmt.Fprintf(&out, "msg: %q\n", msg)
				if comps == nil {
					fmt.Fprintf(&out, "(no completions generated)\n")
				} else {
					fmt.Fprintf(&out, "completions:\n")
					for i := 0; i < comps.NumCategories(); i++ {
						fmt.Fprintf(&out, "- %q:\n", comps.CategoryTitle(i))
						for j := 0; j < comps.NumEntries(i); j++ {
							if j >= 10 {
								fmt.Fprintf(&out, "  ... entries omitted ...\n")
								break
							}
							entry := comps.Entry(i, j)
							candidate := comps.Candidate(entry)
							fmt.Fprintf(&out, "  %q (%s) -> %q (%d, %d)\n",
								entry.Title(), entry.Description(),
								candidate.Replacement(),
								candidate.MoveRight(), candidate.DeleteLeft())
						}
					}
				}

				return out.String()

			default:
				t.Fatalf("%s: unrecognized command: %q", td.Pos, td.Cmd)
			}
			return ""
		})
	})
}

// mockShell is a mock of the sqlShell interface for the benefit
// of the completion test.
type mockShell struct {
	db *gosql.DB
}

var _ sqlShell = mockShell{}

func (mockShell) inCopy() bool      { return false }
func (mockShell) enableDebug() bool { return false }
func (mockShell) serverSideParse(sql string) (string, error) {
	panic("not implemented")
}
func (mockShell) reflow(_ bool, _ string, _ int) (bool, string, string) {
	panic("not implemented")
}

func (s mockShell) runShowCompletions(sql string, offset int) (rows [][]string, err error) {
	query := fmt.Sprintf(`SHOW COMPLETIONS AT OFFSET %d FOR %s`, offset, lexbase.EscapeSQLString(sql))
	srows, err := s.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer srows.Close()

	for srows.Next() {
		var completion, category, description string
		var a, b int
		if err := srows.Scan(&completion, &category, &description, &a, &b); err != nil {
			return nil, err
		}
		rows = append(rows, []string{completion, category, description, fmt.Sprint(a), fmt.Sprint(b)})
	}
	return rows, srows.Err()
}
