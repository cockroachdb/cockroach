// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fuzzystrmatch

import (
	"strings"
	"unicode"
)

// Binstock, Andrew & Rex, John. "Metaphone: A Modern Soundex." Practical Algorithms For Programmers.
// Reading, Mass: Addion-Wesley, 1995 pp160-169

// Special encodings
const SH = 'X'
const TH = '0'

/* Port from Postgres */
var _codes = [26]int{1, 16, 4, 16, 9, 2, 4, 16, 9, 2, 0, 2, 2, 2, 1, 4, 0, 2, 4, 4, 1, 0, 0, 0, 8, 0}

//                   a  b   c  d   e  f  g  h   i  j  k  l  m  n  o  p  q  r  s  t  u  v  w  x  y  z

func getcode(c rune) int {
	if IsAlpha(c) {
		c = unicode.ToUpper(c)
		return _codes[int(c-'A')]
	}
	return 0
}

func isVowel(c rune) bool {
	return (getcode(c) & 1) != 0
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

// Iterator on a string
type Iterator struct {
	src []rune
	idx int
}

func (itr *Iterator) len() int {
	return itr.idx + 1
}

func (itr *Iterator) next(count int) bool {
	if itr.idx+count >= len(itr.src) {
		return false
	}
	itr.idx += count
	return true
}

func (itr *Iterator) letterAt(offset int, pred func(rune) bool) bool {
	if itr.idx+offset < 0 || itr.idx+offset >= len(itr.src) {
		return false
	}
	return pred(itr.src[itr.idx+offset])
}

func (itr *Iterator) compareLetterAt(offset int, other rune) bool {
	return itr.letterAt(offset, func(this rune) bool {
		return this == other
	})
}

func (itr *Iterator) lookNextLetter() rune {
	if itr.idx+1 >= len(itr.src) {
		return rune(0)
	}
	return itr.src[itr.idx+1]
}

func (itr *Iterator) lookCurrLetter() rune {
	if itr.idx >= len(itr.src) {
		return rune(0)
	}
	return itr.src[itr.idx]
}

func (itr *Iterator) lookPrevLetter() rune {
	if itr.idx-1 < 0 {
		return rune(0)
	}
	return itr.src[itr.idx-1]
}

func (itr *Iterator) phonize(c rune) {
	itr.src[itr.idx] = c
	itr.idx++
}

func Metaphone(source string, outlen int) string {
	source = strings.TrimLeftFunc(source, func(c rune) bool {
		return !IsAlpha(c)
	})
	if len(source) == 0 || outlen == 0 {
		return ""
	}

	source = strings.ToUpper(source)
	itrSrc := Iterator{
		src: []rune(source),
		idx: 0,
	}
	phoned := make([]rune, outlen)
	itrPhoned := Iterator{
		src: phoned,
		idx: 0,
	}

	// Handle the first letter
	ok := true
	nextLetter := itrSrc.lookNextLetter()
	currLetter := itrSrc.lookCurrLetter()
	switch currLetter {
	case 'A':
		// AE becomes E
		if nextLetter == 'E' {
			itrPhoned.phonize('E')
			ok = itrSrc.next(2)
		} else {
			// Preserve vowel at the beginning
			itrPhoned.phonize('A')
			ok = itrSrc.next(1)
		}

	// [GKP]N becomes N
	case 'G', 'K', 'P':
		if nextLetter == 'N' {
			itrPhoned.phonize('N')
			ok = itrSrc.next(2)
		}

	// WH becomes H, WR becomes R, W becomes W if followed by a vowel
	case 'W':
		if nextLetter == 'H' ||
			nextLetter == 'R' {
			itrPhoned.phonize(nextLetter)
		} else if isVowel(nextLetter) {
			itrPhoned.phonize('W')
		}
		ok = itrSrc.next(2)

	// X becomes S
	case 'X':
		itrPhoned.phonize('S')
		ok = itrSrc.next(1)

	// Vowels
	// Note that we handle case 'A' already
	case 'E', 'I', 'O', 'U':
		itrPhoned.phonize(currLetter)
		ok = itrSrc.next(1)

	default:

	}

	// On to the metaphoning
	for ; ok && itrPhoned.len() <= outlen; ok = itrSrc.next(1) {
		numSkipLetters := 0 // How many letters to skip because an earlier encoding handled multiple letters
		currLetter = itrSrc.lookCurrLetter()
		nextLetter = itrSrc.lookNextLetter()
		prevLetter := itrSrc.lookPrevLetter()

		// Ignore non-alphas
		if !IsAlpha(currLetter) {
			continue
		}
		// Drop duplicates, except CC
		if currLetter == prevLetter && currLetter != 'C' {
			continue
		}

		switch currLetter {
		// B becomes B unless in MB
		case 'B':
			if prevLetter != 'M' {
				itrPhoned.phonize('B')
			}

		// C becomes SH if -CIA- or -CH, but not SCH
		// else S if -CE-, -CI- or -CY-
		// else dropped if SCE-, -SCI-, -SCY- (handled in case 'S')
		// else K
		case 'C':
			// C[EIY]
			if isEIY(nextLetter) {
				// CIA
				if nextLetter == 'I' && itrSrc.compareLetterAt(2, 'A') {
					itrPhoned.phonize(SH)
				} else if prevLetter == 'S' {
					// SC[EIY]
					// Dropped
				} else {
					itrPhoned.phonize('S')
				}
			} else if nextLetter == 'H' {
				itrPhoned.phonize(SH)
				numSkipLetters++
			} else {
				itrPhoned.phonize('K')
			}

		// D becomes J if in -DGE-, -DGI- or -DGY-
		// else T
		case 'D':
			if nextLetter == 'G' && itrSrc.letterAt(2, isEIY) {
				itrPhoned.phonize('J')
				numSkipLetters++
			} else {
				itrPhoned.phonize('T')
			}

		// G becomes F if in -GH but not B--GH, D--GH, -H--GH, -H---GH
		// else dropped if -GNED, -GN
		// else dropped if -DGE-, -DGI- or -DGY- (handled in case 'D' above)
		// else J if in -GE-, -GI, -GY and not GG
		// else K
		case 'G':
			if nextLetter == 'H' {
				if !(itrSrc.letterAt(-3, isBDH) || itrSrc.compareLetterAt(-4, 'H')) {
					itrPhoned.phonize('F')
					numSkipLetters++
				}
			} else if nextLetter == 'N' && itrSrc.compareLetterAt(2, 'E') && itrSrc.compareLetterAt(3, 'D') {
				// Dropped
			} else if isEIY(nextLetter) && prevLetter != 'G' {
				itrPhoned.phonize('J')
			} else {
				itrPhoned.phonize('K')
			}

		// H becomes H if before a vowel and not after C,G,P,S,T
		case 'H':
			if isVowel(nextLetter) && !isCGPST(prevLetter) {
				itrPhoned.phonize('H')
			}

		// K is dropped if after C, else K
		case 'K':
			if prevLetter != 'C' {
				itrPhoned.phonize('K')
			}

		// P becomes F if before H, else P
		case 'P':
			if nextLetter == 'H' {
				itrPhoned.phonize('F')
			} else {
				itrPhoned.phonize('P')
			}

		// Q becomes K
		case 'Q':
			itrPhoned.phonize('K')

		// S becomes SH if in -SH-, -SIA- or -SIO-
		// else S
		case 'S':
			if nextLetter == 'I' &&
				(itrSrc.compareLetterAt(2, 'A') || itrSrc.compareLetterAt(2, 'O')) {
				itrPhoned.phonize(SH)
			} else if nextLetter == 'H' {
				itrPhoned.phonize(SH)
				numSkipLetters++
			} else {
				itrPhoned.phonize('S')
			}

		// T becomes SH if in -TIA- or -TIO-
		// else TH before H
		// else T
		case 'T':
			if nextLetter == 'I' &&
				(itrSrc.compareLetterAt(2, 'A') || itrSrc.compareLetterAt(2, 'O')) {
				itrPhoned.phonize(SH)
			} else if nextLetter == 'H' {
				itrPhoned.phonize(TH)
				numSkipLetters++
			} else {
				itrPhoned.phonize('T')
			}

		// V becomes F
		case 'V':
			itrPhoned.phonize('F')

		// W becomes W if before a vowel, else dropped
		case 'W':
			if isVowel(nextLetter) {
				itrPhoned.phonize('W')
			}

		// X becomes KS
		case 'X':
			itrPhoned.phonize('K')
			if itrPhoned.len() < outlen {
				itrPhoned.phonize('S')
			}

		// Y becomes Y if before a vowel
		case 'Y':
			if isVowel(nextLetter) {
				itrPhoned.phonize('Y')
			}

		// Z becomes S
		case 'Z':
			itrPhoned.phonize('S')

		// No transformation
		case 'F', 'J', 'L', 'M', 'N', 'R':
			itrPhoned.phonize(currLetter)

		default:

		}

		_ = itrSrc.next(numSkipLetters)
	}

	result := string(phoned)
	result = strings.TrimRightFunc(result, func(c rune) bool {
		return !IsAlpha(c)
	})
	return result
}
