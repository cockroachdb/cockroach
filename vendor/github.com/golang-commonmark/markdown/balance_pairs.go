// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

func ruleBalancePairs(s *StateInline) {
	delimiters := s.Delimiters
	max := len(delimiters)

	for i := 0; i < max; i++ {
		lastDelim := delimiters[i]

		if !lastDelim.Close {
			continue
		}

		j := i - lastDelim.Jump - 1

		for j >= 0 {
			currDelim := delimiters[j]

			if currDelim.Open &&
				currDelim.Marker == lastDelim.Marker &&
				currDelim.End < 0 &&
				currDelim.Level == lastDelim.Level {
				oddMatch := (currDelim.Close || lastDelim.Open) &&
					currDelim.Length != -1 &&
					lastDelim.Length != -1 &&
					(currDelim.Length+lastDelim.Length)%3 == 0
				if !oddMatch {
					delimiters[i].Jump = i - j
					delimiters[i].Open = false
					delimiters[j].End = i
					delimiters[j].Jump = 0
					break
				}
			}

			j -= currDelim.Jump + 1
		}
	}
}
