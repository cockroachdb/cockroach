// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

func ruleStrikeThrough(s *StateInline, silent bool) bool {
	src := s.Src
	start := s.Pos
	marker := src[start]

	if silent {
		return false
	}

	if src[start] != '~' {
		return false
	}

	canOpen, canClose, length := s.scanDelims(start, true)
	origLength := length
	ch := string(marker)
	if length < 2 {
		return false
	}

	if length%2 != 0 {
		s.PushToken(&Text{
			Content: ch,
		})
		length--
	}

	for i := 0; i < length; i += 2 {
		s.PushToken(&Text{
			Content: ch + ch,
		})

		s.Delimiters = append(s.Delimiters, Delimiter{
			Marker: marker,
			Length: -1,
			Jump:   i,
			Token:  len(s.Tokens) - 1,
			Level:  s.Level,
			End:    -1,
			Open:   canOpen,
			Close:  canClose,
		})
	}

	s.Pos += origLength

	return true

}

func ruleStrikethroughPostprocess(s *StateInline) {
	var loneMarkers []int
	delimiters := s.Delimiters
	max := len(delimiters)

	for i := 0; i < max; i++ {
		startDelim := delimiters[i]

		if startDelim.Marker != '~' {
			continue
		}

		if startDelim.End == -1 {
			continue
		}

		endDelim := delimiters[startDelim.End]

		s.Tokens[startDelim.Token] = &StrikethroughOpen{}
		s.Tokens[endDelim.Token] = &StrikethroughClose{}

		if text, ok := s.Tokens[endDelim.Token-1].(*Text); ok && text.Content == "~" {
			loneMarkers = append(loneMarkers, endDelim.Token-1)
		}
	}

	for len(loneMarkers) > 0 {
		i := loneMarkers[len(loneMarkers)-1]
		loneMarkers = loneMarkers[:len(loneMarkers)-1]
		j := i + 1

		for j < len(s.Tokens) {
			if _, ok := s.Tokens[j].(*StrikethroughClose); !ok {
				break
			}
			j++
		}

		j--

		if i != j {
			s.Tokens[i], s.Tokens[j] = s.Tokens[j], s.Tokens[i]
		}
	}
}
