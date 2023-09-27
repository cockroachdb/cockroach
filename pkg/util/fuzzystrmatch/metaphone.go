// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package fuzzystrmatch

import (
	"strings"
	"unicode"
	"fmt"
)

const maxMetaphoneLen = 4

// special encodings
const SH = 'X'
const TH = '0'

// ports from Postgres
var _codes = [26]int{1, 16, 4, 16, 9, 2, 4, 16, 9, 2, 0, 2, 2, 2, 1, 4, 0, 2, 4, 4, 1, 0, 0, 0, 8, 0}
//					  a  b   c  d   e  f  g  h   i  j  k  l  m  n  o  p  q  r  s  t  u  v  w  x  y  z

func getcode(c rune) int {
	if (IsAlpha(c)) {
		c = unicode.ToUpper(c)
		return _codes[int(c - 'A')]
	}
	return 0
}

func isVowel(c rune) bool {
	return (getcode(c) & 1) != 0
}

func isFJMNR(c rune) bool {
	return (getcode(c) & 2) != 0
}

func isCGPST(c rune) bool {
	return (getcode(c) & 4) != 0
}

func isEIY(c rune) bool {
	return (getcode(c) & 8) != 0
}

func isBDH(c rune) bool {
	return (getcode(c) & 16) != 0
}

// ???
type Iterator struct {
	src []rune
	idx int
}

func (itr *Iterator) len() int {
	return len(itr.src)
}

func (itr *Iterator) next() bool {
	if itr.idx >= len(itr.src) - 1 {
		return false
	}
	itr.idx++
	return true
}

func (itr *Iterator) nextAhead(n int) bool {
	if itr.idx + n >= len(itr.src) - 1 {
		return false
	}
	itr.idx += n
	return true
}

func (itr *Iterator) lookAhead(n int) rune {
	return itr.src[min(itr.idx + n, n)]
}

func (itr *Iterator) lookBack(n int) rune {
	return itr.src[max(itr.idx - n, 0)]
}

func (itr *Iterator) lookNextLetter() (rune, bool) {
	if itr.idx + 1 >= itr.len() {
		return rune(0), false
	}
	return itr.src[itr.idx + 1], true
}

func (itr *Iterator) lookCurrLetter() (rune, bool) {
	if itr.idx >= itr.len() {
		return rune(0), false
	}
	return itr.src[itr.idx], true
}

func (itr *Iterator) lookPrevLetter() (rune, bool) {
	if itr.idx - 1 < 0 {
		return rune(0), false
	}
	return itr.src[itr.idx - 1], true
}

func (itr *Iterator) phonize(c rune) {
	itr.src[itr.idx] = c
	itr.idx++
}

func Metaphone(source string) string {
	code := metaphone(source)
	fmt.Printf("\tDEBUGGING %s -> %s\n", source, code)
	return code
}

func metaphone(source string) string {
	source = strings.TrimLeftFunc(source, func (c rune) bool {
		return !IsAlpha(c)
	})
	if len(source) == 0 {
		return source
	}
	source = strings.ToUpper(source)

	itrSrc := Iterator{
		src: []rune(source),
		idx: 0,
	}
	phoned := make([]rune, maxMetaphoneLen)
	itrPhoned := Iterator{
		src: phoned,
		idx: 0,
	}
	var _ = itrSrc
	var _ = itrPhoned

	// Handle the first letter
	nextLetter, _ := itrSrc.lookNextLetter()
	switch currLetter, _ := itrSrc.lookCurrLetter(); currLetter {
		// AE becomes E
		case 'A':
			if nextLetter == 'E' {
				itrPhoned.phonize('E')
				itrSrc.next()
				itrSrc.next()
			} else {
				// Preserve vowel at the beginning!
				itrPhoned.phonize('A')
				itrSrc.next()
			}
		// [GKP]N becomes N
		case 'G':
		case 'K':
		case 'P':
			if nextLetter == 'N' {
				itrPhoned.phonize('N')
				itrSrc.next()
				itrSrc.next()
			}
		// WH becomes H, WR becomes R, W if followed by a vowel
		case 'W':
			if nextLetter == 'H' ||
			   nextLetter == 'R' {
				itrPhoned.phonize(nextLetter)
			} else if isVowel(nextLetter) {
				itrPhoned.phonize('W')
			}
			itrSrc.next()
			itrSrc.next()
		// X becomes S
		case 'X':
			itrPhoned.phonize('S')
			itrSrc.next()
		// Vowels
		// We did A already
		case 'E':
		case 'I':
		case 'O':
		case 'U':
			itrPhoned.phonize(currLetter)
			itrSrc.next()
		default:
	}

	// On to the metaphoning
	numSkipLetters := 0
	nextLetter, _ = itrSrc.lookNextLetter()
	for currLetter, success := itrSrc.lookCurrLetter(); success && itrPhoned.len() < maxMetaphoneLen; itrSrc.next() {
		// Ignore non-alphas
		if !IsAlpha(currLetter) {
			continue
		}

		prevLetter, _ := itrSrc.lookPrevLetter()
		// Drop duplicates, except CC
		if prevLetter != currLetter && currLetter == 'C' {
			continue
		}

		switch currLetter {
			// B becomes B unless in MB
			case 'B':
				if prevLetter != 'M' {
					itrPhoned.phonize('B')
				}

			case 'C':
				// C[EIY]
				if isEIY(nextLetter) {
					// CIA
					if nextLetter == 'I' && itrSrc.lookAhead(2) == 'A' {
						itrPhoned.phonize(SH)
					} else if prevLetter == 'S' {
						// SC[IEY]
						// Dropped
					} else {
						itrPhoned.phonize('S')
					}
				} else if nextLetter == 'H' {
					if prevLetter == 'S' || itrSrc.lookAhead(2) == 'R' {
						itrPhoned.phonize('K')
					} else {
						itrPhoned.phonize(SH)
					}
				}
				numSkipLetters++
				
			// becomes J if in -DGE-, -DGI- or -DGY-; else T
			case 'D':
				if nextLetter == 'G' && isEIY(itrSrc.lookAhead(2)) {
					itrPhoned.phonize('J')
					numSkipLetters++
				} else {
					itrPhoned.phonize('T')
				}

			//
			case 'G':
				if nextLetter == 'H' {
					if !isBDH(itrSrc.lookBack(3)) || itrSrc.lookBack(4) == 'H' {
						itrPhoned.phonize('F')
						numSkipLetters++
					} 
					// else { Silent }
				} else if nextLetter == 'N' {
					if !IsAlpha(itrSrc.lookAhead(2)) || 
					   (itrSrc.lookAhead(2) == 'E' && itrSrc.lookAhead(3) == 'D') {
						// Dropped
					} else {
						itrPhoned.phonize('F')
					}
				} else if isEIY(nextLetter) && prevLetter != 'G' {
					itrPhoned.phonize('J')
				} else {
					itrPhoned.phonize('K')
				}

			// H if before a vowel and not after C,G,P,S,T
			case 'H':
				if (isVowel(nextLetter) && !isCGPST(prevLetter)) {
					itrPhoned.phonize('H')
				}

			//
			case 'K':
				if prevLetter != 'C' {
					itrPhoned.phonize('K')
				}

			//
			case 'P':
				if nextLetter == 'H' {
					itrPhoned.phonize('F')
				} else {
					itrPhoned.phonize('P')
				}

			//
			case 'Q':
				itrPhoned.phonize('K')

			// 
			case 'S':
				if nextLetter == 'I' && 
				   (itrSrc.lookAhead(2) == 'O' || itrSrc.lookAhead(2) == 'A') {
					itrPhoned.phonize(SH)
				} else if nextLetter == 'H' {
					itrPhoned.phonize(SH)
					numSkipLetters++
				} else if nextLetter == 'C'  &&
						  itrSrc.lookAhead(2) == 'H' &&
						  itrSrc.lookAhead(3) == 'W' {
					itrPhoned.phonize(SH)
					numSkipLetters += 2
				} else {
					itrPhoned.phonize('S')
				}

			// 
			case 'T':
				if nextLetter == 'I' && 
				   (itrSrc.lookAhead(2) == 'O' || itrSrc.lookAhead(2) == 'A') {
					itrPhoned.phonize(SH)
				} else if nextLetter == 'H' {
					itrPhoned.phonize(TH)
					numSkipLetters++
				} else {
					itrPhoned.phonize('T')
				}

			case 'V':
				itrPhoned.phonize('F')
		
			case 'W':
				if (isVowel(nextLetter)) {
					itrPhoned.phonize('W')
				}

			case 'X':
				itrPhoned.phonize('K')
				if itrPhoned.len() < maxMetaphoneLen {
					itrPhoned.phonize('S')
				}

			case 'Y':
				if (isVowel(nextLetter)) {
					itrPhoned.phonize('Y')
				}

			case 'Z':
				itrPhoned.phonize('S')

			// No transformation
			case 'F':
			case 'J':
			case 'L':
			case 'M':
			case 'N':
			case 'R':
				itrPhoned.phonize(currLetter);
			default:

			itrSrc.nextAhead(numSkipLetters) // ???
		}
	
	}

	return string(phoned)
}

// other utilities
func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}