// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

import (
	"regexp"
	"strings"
)

func getLine(s *StateBlock, line int) string {
	pos := s.BMarks[line] + s.BlkIndent
	max := s.EMarks[line]
	if pos >= max {
		return ""
	}
	return s.Src[pos:max]
}

func escapedSplit(s string) (result []string) {
	pos := 0
	escapes := 0
	lastPos := 0
	backTicked := false
	lastBackTick := 0

	for pos < len(s) {
		ch := s[pos]
		if ch == '`' {
			if backTicked {
				backTicked = false
				lastBackTick = pos
			} else if escapes%2 == 0 {
				backTicked = true
				lastBackTick = pos
			}
		} else if ch == '|' && (escapes%2 == 0) && !backTicked {
			result = append(result, s[lastPos:pos])
			lastPos = pos + 1
		}

		if ch == '\\' {
			escapes++
		} else {
			escapes = 0
		}

		pos++

		if pos == len(s) && backTicked {
			backTicked = false
			pos = lastBackTick + 1
		}
	}

	return append(result, s[lastPos:])
}

var rColumn = regexp.MustCompile("^:?-+:?$")

func ruleTable(s *StateBlock, startLine, endLine int, silent bool) bool {
	if !s.Md.Tables {
		return false
	}

	if startLine+2 > endLine {
		return false
	}

	nextLine := startLine + 1

	if s.SCount[nextLine] < s.BlkIndent {
		return false
	}

	if s.SCount[nextLine]-s.BlkIndent >= 4 {
		return false
	}

	pos := s.BMarks[nextLine] + s.TShift[nextLine]
	if pos >= s.EMarks[nextLine] {
		return false
	}

	src := s.Src
	ch := src[pos]
	pos++

	if ch != '|' && ch != '-' && ch != ':' {
		return false
	}

	for pos < s.EMarks[nextLine] {
		ch = src[pos]
		if ch != '|' && ch != '-' && ch != ':' && !byteIsSpace(ch) {
			return false
		}
		pos++
	}

	//

	lineText := getLine(s, startLine+1)

	columns := strings.Split(lineText, "|")
	var aligns []Align
	for i := 0; i < len(columns); i++ {
		t := strings.TrimSpace(columns[i])
		if t == "" {
			if i == 0 || i == len(columns)-1 {
				continue
			}
			return false
		}

		if !rColumn.MatchString(t) {
			return false
		}

		if t[len(t)-1] == ':' {
			if t[0] == ':' {
				aligns = append(aligns, AlignCenter)
			} else {
				aligns = append(aligns, AlignRight)
			}
		} else if t[0] == ':' {
			aligns = append(aligns, AlignLeft)
		} else {
			aligns = append(aligns, AlignNone)
		}
	}

	lineText = strings.TrimSpace(getLine(s, startLine))
	if strings.IndexByte(lineText, '|') == -1 {
		return false
	}
	if s.SCount[startLine]-s.BlkIndent >= 4 {
		return false
	}
	columns = escapedSplit(strings.TrimSuffix(strings.TrimPrefix(lineText, "|"), "|"))
	columnCount := len(columns)
	if columnCount > len(aligns) {
		return false
	}

	if silent {
		return true
	}

	tableTok := &TableOpen{
		Map: [2]int{startLine, 0},
	}
	s.PushOpeningToken(tableTok)
	s.PushOpeningToken(&TheadOpen{
		Map: [2]int{startLine, startLine + 1},
	})
	s.PushOpeningToken(&TrOpen{
		Map: [2]int{startLine, startLine + 1},
	})

	for i := 0; i < len(columns); i++ {
		s.PushOpeningToken(&ThOpen{
			Align: aligns[i],
			Map:   [2]int{startLine, startLine + 1},
		})
		s.PushToken(&Inline{
			Content: strings.TrimSpace(columns[i]),
			Map:     [2]int{startLine, startLine + 1},
		})
		s.PushClosingToken(&ThClose{})
	}

	s.PushClosingToken(&TrClose{})
	s.PushClosingToken(&TheadClose{})

	tbodyTok := &TbodyOpen{
		Map: [2]int{startLine + 2, 0},
	}
	s.PushOpeningToken(tbodyTok)

	for nextLine = startLine + 2; nextLine < endLine; nextLine++ {
		if s.SCount[nextLine] < s.BlkIndent {
			break
		}

		lineText = strings.TrimSpace(getLine(s, nextLine))
		if strings.IndexByte(lineText, '|') == -1 {
			break
		}
		if s.SCount[nextLine]-s.BlkIndent >= 4 {
			break
		}
		columns = escapedSplit(strings.TrimPrefix(strings.TrimSuffix(lineText, "|"), "|"))

		if len(columns) < len(aligns) {
			columns = append(columns, make([]string, len(aligns)-len(columns))...)
		} else if len(columns) > len(aligns) {
			columns = columns[:len(aligns)]
		}

		s.PushOpeningToken(&TrOpen{})
		for i := 0; i < columnCount; i++ {
			tdOpen := TdOpen{}
			if i < len(aligns) {
				tdOpen.Align = aligns[i]
			}
			s.PushOpeningToken(&tdOpen)

			inline := Inline{}
			if i < len(columns) {
				inline.Content = strings.TrimSpace(columns[i])
			}
			s.PushToken(&inline)

			s.PushClosingToken(&TdClose{})
		}
		s.PushClosingToken(&TrClose{})
	}

	s.PushClosingToken(&TbodyClose{})
	s.PushClosingToken(&TableClose{})

	tableTok.Map[1] = nextLine
	tbodyTok.Map[1] = nextLine
	s.Line = nextLine

	return true
}
