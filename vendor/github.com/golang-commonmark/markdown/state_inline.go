// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

import (
	"bytes"
	"unicode"
	"unicode/utf8"
)

type StateInline struct {
	StateCore

	Pos          int
	PosMax       int
	Level        int
	Pending      bytes.Buffer
	PendingLevel int
	Delimiters   []Delimiter

	Cache map[int]int
}

func (s *StateInline) PushToken(tok Token) {
	if s.Pending.Len() > 0 {
		s.PushPending()
	}
	tok.SetLevel(s.Level)
	s.PendingLevel = s.Level
	s.Tokens = append(s.Tokens, tok)
}

func (s *StateInline) PushOpeningToken(tok Token) {
	if s.Pending.Len() > 0 {
		s.PushPending()
	}
	tok.SetLevel(s.Level)
	s.Level++
	s.PendingLevel = s.Level
	s.Tokens = append(s.Tokens, tok)
}

func (s *StateInline) PushClosingToken(tok Token) {
	if s.Pending.Len() > 0 {
		s.PushPending()
	}
	s.Level--
	tok.SetLevel(s.Level)
	s.PendingLevel = s.Level
	s.Tokens = append(s.Tokens, tok)
}

func (s *StateInline) PushPending() {
	s.Tokens = append(s.Tokens, &Text{
		Content: s.Pending.String(),
		Lvl:     s.PendingLevel,
	})
	s.Pending.Reset()
}

func (s *StateInline) scanDelims(start int, canSplitWord bool) (canOpen bool, canClose bool, length int) {
	pos := start
	max := s.PosMax
	src := s.Src
	marker := src[start]
	leftFlanking, rightFlanking := true, true

	lastChar := ' '
	if start > 0 {
		lastChar, _ = utf8.DecodeLastRuneInString(src[:start])
	}

	for pos < max && src[pos] == marker {
		pos++
	}
	length = pos - start

	nextChar := ' '
	if pos < max {
		nextChar, _ = utf8.DecodeRuneInString(src[pos:])
	}

	isLastPunct := isMdAsciiPunct(lastChar) || unicode.IsPunct(lastChar)
	isNextPunct := isMdAsciiPunct(nextChar) || unicode.IsPunct(nextChar)

	isLastWhiteSpace := unicode.IsSpace(lastChar)
	isNextWhiteSpace := unicode.IsSpace(nextChar)

	if isNextWhiteSpace {
		leftFlanking = false
	} else if isNextPunct {
		if !(isLastWhiteSpace || isLastPunct) {
			leftFlanking = false
		}
	}

	if isLastWhiteSpace {
		rightFlanking = false
	} else if isLastPunct {
		if !(isNextWhiteSpace || isNextPunct) {
			rightFlanking = false
		}
	}

	if !canSplitWord {
		canOpen = leftFlanking && (!rightFlanking || isLastPunct)
		canClose = rightFlanking && (!leftFlanking || isNextPunct)
	} else {
		canOpen = leftFlanking
		canClose = rightFlanking
	}

	return
}
