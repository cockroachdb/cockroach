// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

type Delimiter struct {
	Length int
	Jump   int
	Token  int
	Level  int
	End    int
	Open   bool
	Close  bool
	Marker byte
}

func ruleEmphasis(s *StateInline, silent bool) bool {
	src := s.Src
	start := s.Pos
	marker := src[start]

	if silent {
		return false
	}

	if marker != '_' && marker != '*' {
		return false
	}

	canOpen, canClose, length := s.scanDelims(s.Pos, marker == '*')
	for i := 0; i < length; i++ {
		s.PushToken(&Text{Content: string(marker)})

		s.Delimiters = append(s.Delimiters, Delimiter{
			Marker: marker,
			Length: length,
			Jump:   i,
			Token:  len(s.Tokens) - 1,
			Level:  s.Level,
			End:    -1,
			Open:   canOpen,
			Close:  canClose,
		})
	}

	s.Pos += length

	return true
}

func ruleEmphasisPostprocess(s *StateInline) {
	delimiters := s.Delimiters
	max := len(delimiters)
	for i := max - 1; i >= 0; i-- {
		startDelim := delimiters[i]
		if startDelim.Marker != '_' && startDelim.Marker != '*' {
			continue
		}

		if startDelim.End == -1 {
			continue
		}

		endDelim := delimiters[startDelim.End]

		isStrong := i > 0 &&
			delimiters[i-1].End == startDelim.End+1 &&
			delimiters[i-1].Token == startDelim.Token-1 &&
			delimiters[startDelim.End+1].Token == endDelim.Token+1 &&
			delimiters[i-1].Marker == startDelim.Marker

		if isStrong {
			s.Tokens[startDelim.Token] = &StrongOpen{}
			s.Tokens[endDelim.Token] = &StrongClose{}

			if text, ok := s.Tokens[delimiters[i-1].Token].(*Text); ok {
				text.Content = ""
			}
			if text, ok := s.Tokens[delimiters[startDelim.End+1].Token].(*Text); ok {
				text.Content = ""
			}
			i--
		} else {
			s.Tokens[startDelim.Token] = &EmphasisOpen{}
			s.Tokens[endDelim.Token] = &EmphasisClose{}
		}
	}
}
