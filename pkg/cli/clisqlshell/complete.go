// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlshell

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/cli/clierror"
	"github.com/knz/bubbline"
	"github.com/knz/bubbline/computil"
)

// completions is the interface between the shell and the bubbline
// completion infra.
type completions struct {
	categories  []string
	compEntries map[string][]compCandidate
}

var _ bubbline.Completions = (*completions)(nil)

// NumCategories is part of the bubbline.Completions interface.
func (c *completions) NumCategories() int { return len(c.categories) }

// CategoryTitle is part of the bubbline.Completions interface.
func (c *completions) CategoryTitle(cIdx int) string { return c.categories[cIdx] }

// NumEntries is part of the bubbline.Completions interface.
func (c *completions) NumEntries(cIdx int) int { return len(c.compEntries[c.categories[cIdx]]) }

// Entry is part of the bubbline.Completions interface.
func (c *completions) Entry(cIdx, eIdx int) bubbline.Entry {
	return &c.compEntries[c.categories[cIdx]][eIdx]
}

// Candidate is part of the bubbline.Completions interface.
func (c *completions) Candidate(e bubbline.Entry) bubbline.Candidate { return e.(*compCandidate) }

// compCandidate represents one completion candidate.
type compCandidate struct {
	completion string
	desc       string
	moveRight  int
	deleteLeft int
}

var _ bubbline.Entry = (*compCandidate)(nil)

// Title is part of the bubbline.Entry interface.
func (c *compCandidate) Title() string { return c.completion }

// Description is part of the bubbline.Entry interface.
func (c *compCandidate) Description() string { return c.desc }

// Replacement is part of the bubbline.Candidate interface.
func (c *compCandidate) Replacement() string { return c.completion }

// MoveRight is part of the bubbline.Candidate interface.
func (c *compCandidate) MoveRight() int { return c.moveRight }

// DeleteLeft is part of the bubbline.Candidate interface.
func (c *compCandidate) DeleteLeft() int { return c.deleteLeft }

// getCompletions implements the editline AutoComplete interface.
func (b *bubblineReader) getCompletions(
	v [][]rune, line, col int,
) (msg string, comps bubbline.Completions) {
	// In COPY mode, we can't complete anything.
	if b.sql.inCopy() {
		return "", comps
	}

	sql, boffset := computil.Flatten(v, line, col)

	if col > 1 && v[line][col-1] == '?' && v[line][col-2] == '?' {
		// This is a syntax check.
		sql = strings.TrimSuffix(sql[:boffset], "\n")
		helpText, err := b.sql.serverSideParse(sql)
		if helpText != "" {
			// We have a completion suggestion. Use that.
			msg = fmt.Sprintf("\nSuggestion:\n%s\n", helpText)
		} else if err != nil {
			// Some other error. Display it.
			var buf strings.Builder
			clierror.OutputError(&buf, err, true /*showSeverity*/, false /*verbose*/)
			msg = buf.String()
		}
		return msg, comps
	}

	// TODO(knz): do not read all the rows - stop after a maximum.
	rows, err := b.sql.runShowCompletions(sql, boffset)
	if err != nil {
		var buf strings.Builder
		clierror.OutputError(&buf, err, true /*showSeverity*/, false /*verbose*/)
		msg = buf.String()
		return msg, comps
	}

	compByCategory := make(map[string][]compCandidate)
	roffset := runeOffset(v, line, col)
	for _, row := range rows {
		c := compCandidate{completion: row[0]}
		category := "completions"
		if len(row) >= 5 {
			// New-gen server-side completion engine.
			category = row[1]
			c.desc = row[2]
			var err error
			i, err := strconv.Atoi(row[3])
			if err != nil {
				var buf strings.Builder
				clierror.OutputError(&buf, err, true /*showSeverity*/, false /*verbose*/)
				msg = buf.String()
				return msg, comps
			}
			j, err := strconv.Atoi(row[4])
			if err != nil {
				var buf strings.Builder
				clierror.OutputError(&buf, err, true /*showSeverity*/, false /*verbose*/)
				msg = buf.String()
				return msg, comps
			}
			start := byteOffsetToRuneOffset(sql, roffset, boffset /* cursor */, i)
			end := byteOffsetToRuneOffset(sql, roffset, boffset /* cursor */, j)
			c.moveRight = end - roffset
			c.deleteLeft = end - start
		} else {
			// Previous CockroachDB versions with only keyword completion.
			// It does not return the start/end markers so we need to
			// provide our own.
			//
			// TODO(knz): Delete this code when the previous completion code
			// is not available any more.
			_, start, end := computil.FindWord(v, line, col)
			c.moveRight = end - roffset
			c.deleteLeft = end - start
		}
		compByCategory[category] = append(compByCategory[category], c)
	}

	if len(compByCategory) == 0 {
		return msg, comps
	}

	// TODO(knz): select an "interesting" category order,
	// as recommended by andrei.
	categories := make([]string, 0, len(compByCategory))
	for k := range compByCategory {
		categories = append(categories, k)
	}
	sort.Strings(categories)
	comps = &completions{
		categories:  categories,
		compEntries: compByCategory,
	}
	return msg, comps
}

// runeOffset converts the 2D rune cursor to a 1D offset from the
// start of the text. The result can be used by the byteOffsetToRuneOffset
// conversion function.
func runeOffset(v [][]rune, line, col int) int {
	roffset := 0
	for i, l := range v {
		if i > 0 {
			// Increment for newline from previous line.
			roffset++
		}
		if line != i {
			roffset += len(l)
			continue
		}
		if col > len(l) {
			col = len(l)
		}
		roffset += col
		break
	}
	return roffset
}

// byteOffsetToRuneOffset converts a byte offset into the SQL string,
// as produced by the SHOW COMPLETIONS statement, into a rune offset
// suitable for the bubble rune-based addressing. We use the cursor as
// a starting point to avoid scanning the SQL string from the beginning.
func byteOffsetToRuneOffset(sql string, runeCursor, byteCursor int, byteOffset int) int {
	byteDistance := byteOffset - byteCursor
	switch {
	case byteDistance == 0:
		return runeCursor

	case byteDistance > 0:
		// offset to the right of the cursor. Search forward.
		result := runeCursor
		s := sql[byteCursor:]
		for i := range s {
			if i >= byteDistance {
				break
			}
			result++
		}
		return result

	default:
		// offset to the left of the cursor. Search backward.
		result := runeCursor
		s := sql[:byteCursor]
		for {
			if len(s) == 0 || len(s)-byteCursor <= byteDistance {
				break
			}
			_, sz := utf8.DecodeLastRuneInString(s)
			s = s[:len(s)-sz]
			result--
		}
		return result
	}
}
