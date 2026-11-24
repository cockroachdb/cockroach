// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fuzzystrmatch

import (
	"strings"
	"unicode/utf8"
)

// DMetaphone returns the primary Double Metaphone code for the input string.
// Ported from PostgreSQL's contrib/fuzzystrmatch/dmetaphone.c.
func DMetaphone(source string) string {
	primary, _ := doubleMetaphone(source)
	return primary
}

// DMetaphoneAlt returns the alternate Double Metaphone code for the input
// string.
func DMetaphoneAlt(source string) string {
	_, secondary := doubleMetaphone(source)
	return secondary
}

// doubleMetaphone computes both the primary and alternate Double Metaphone
// codes simultaneously.
func doubleMetaphone(source string) (string, string) {
	if utf8.RuneCountInString(source) == 0 {
		return "", ""
	}

	// Work with uppercase ASCII. Pad with spaces so we can look ahead safely.
	upper := strings.ToUpper(source)
	original := upper + "     "
	length := len(upper)
	last := length - 1

	var primary, secondary strings.Builder
	current := 0

	// Check for Slavo-Germanic indicators.
	slavoGermanic := dmSlavoGermanic(original)

	// Skip initial silent letters.
	if dmStringAt(original, 0, 2, "GN", "KN", "PN", "WR", "PS") {
		current++
	}

	// Initial 'X' is pronounced 'Z' e.g. 'Xavier'.
	if dmCharAt(original, 0) == 'X' {
		primary.WriteString("S")
		secondary.WriteString("S")
		current++
	}

	for primary.Len() < 4 || secondary.Len() < 4 {
		if current >= length {
			break
		}

		switch dmCharAt(original, current) {
		case 'A', 'E', 'I', 'O', 'U', 'Y':
			if current == 0 {
				primary.WriteString("A")
				secondary.WriteString("A")
			}
			current++

		case 'B':
			primary.WriteString("P")
			secondary.WriteString("P")
			if dmCharAt(original, current+1) == 'B' {
				current += 2
			} else {
				current++
			}

		case '\xc7': // Ç
			primary.WriteString("S")
			secondary.WriteString("S")
			current++

		case 'C':
			current = dmHandleC(original, current, length, last,
				slavoGermanic, &primary, &secondary)

		case 'D':
			current = dmHandleD(original, current, last, &primary, &secondary)

		case 'F':
			primary.WriteString("F")
			secondary.WriteString("F")
			if dmCharAt(original, current+1) == 'F' {
				current += 2
			} else {
				current++
			}

		case 'G':
			current = dmHandleG(original, current, length, last,
				slavoGermanic, &primary, &secondary)

		case 'H':
			// Only keep if first & before vowel or between 2 vowels.
			if (current == 0 || dmIsVowel(original, current-1)) &&
				dmIsVowel(original, current+1) {
				primary.WriteString("H")
				secondary.WriteString("H")
				current += 2
			} else {
				current++
			}

		case 'J':
			current = dmHandleJ(original, current, length, last,
				slavoGermanic, &primary, &secondary)

		case 'K':
			primary.WriteString("K")
			secondary.WriteString("K")
			if dmCharAt(original, current+1) == 'K' {
				current += 2
			} else {
				current++
			}

		case 'L':
			current = dmHandleL(original, current, length, last,
				&primary, &secondary)

		case 'M':
			primary.WriteString("M")
			secondary.WriteString("M")
			if (dmStringAt(original, current-1, 3, "UMB") &&
				(current+1 == last ||
					dmStringAt(original, current+2, 2, "ER"))) ||
				dmCharAt(original, current+1) == 'M' {
				current += 2
			} else {
				current++
			}

		case 'N':
			primary.WriteString("N")
			secondary.WriteString("N")
			if dmCharAt(original, current+1) == 'N' {
				current += 2
			} else {
				current++
			}

		case '\xd1': // Ñ
			primary.WriteString("N")
			secondary.WriteString("N")
			current++

		case 'P':
			if dmCharAt(original, current+1) == 'H' {
				primary.WriteString("F")
				secondary.WriteString("F")
				current += 2
			} else {
				primary.WriteString("P")
				secondary.WriteString("P")
				if dmStringAt(original, current+1, 1, "P", "B") {
					current += 2
				} else {
					current++
				}
			}

		case 'Q':
			primary.WriteString("K")
			secondary.WriteString("K")
			if dmCharAt(original, current+1) == 'Q' {
				current += 2
			} else {
				current++
			}

		case 'R':
			// French e.g. 'rogier', but exclude 'hochmeier'.
			if current == last &&
				!slavoGermanic &&
				dmStringAt(original, current-2, 2, "IE") &&
				!dmStringAt(original, current-4, 2, "ME", "MA") {
				secondary.WriteString("R")
			} else {
				primary.WriteString("R")
				secondary.WriteString("R")
			}
			if dmCharAt(original, current+1) == 'R' {
				current += 2
			} else {
				current++
			}

		case 'S':
			current = dmHandleS(original, current, length, last,
				slavoGermanic, &primary, &secondary)

		case 'T':
			current = dmHandleT(original, current, length, last,
				&primary, &secondary)

		case 'V':
			primary.WriteString("F")
			secondary.WriteString("F")
			if dmCharAt(original, current+1) == 'V' {
				current += 2
			} else {
				current++
			}

		case 'W':
			current = dmHandleW(original, current, length, last,
				slavoGermanic, &primary, &secondary)

		case 'X':
			// French e.g. breaux.
			if !(current == last &&
				(dmStringAt(original, current-3, 3, "IAU", "EAU") ||
					dmStringAt(original, current-2, 2, "AU", "OU"))) {
				primary.WriteString("KS")
				secondary.WriteString("KS")
			}
			if dmStringAt(original, current+1, 1, "C", "X") {
				current += 2
			} else {
				current++
			}

		case 'Z':
			current = dmHandleZ(original, current, last,
				slavoGermanic, &primary, &secondary)

		default:
			current++
		}
	}

	p := primary.String()
	s := secondary.String()
	if len(p) > 4 {
		p = p[:4]
	}
	if len(s) > 4 {
		s = s[:4]
	}
	return p, s
}

// Helper functions.

func dmCharAt(s string, pos int) byte {
	if pos < 0 || pos >= len(s) {
		return 0
	}
	return s[pos]
}

func dmStringAt(s string, start, length int, targets ...string) bool {
	if start < 0 || start >= len(s) {
		return false
	}
	for _, t := range targets {
		if len(t) != length {
			continue
		}
		if start+length <= len(s) && s[start:start+length] == t {
			return true
		}
	}
	return false
}

func dmIsVowel(s string, pos int) bool {
	if pos < 0 || pos >= len(s) {
		return false
	}
	c := s[pos]
	return c == 'A' || c == 'E' || c == 'I' || c == 'O' ||
		c == 'U' || c == 'Y'
}

func dmSlavoGermanic(s string) bool {
	return strings.Contains(s, "W") || strings.Contains(s, "K") ||
		strings.Contains(s, "CZ") || strings.Contains(s, "WITZ")
}

func dmHandleC(
	original string,
	current, length, last int,
	slavoGermanic bool,
	primary, secondary *strings.Builder,
) int {
	// Various Germanic.
	if current > 1 &&
		!dmIsVowel(original, current-2) &&
		dmStringAt(original, current-1, 3, "ACH") &&
		dmCharAt(original, current+2) != 'I' &&
		(dmCharAt(original, current+2) != 'E' ||
			dmStringAt(original, current-2, 6, "BACHER", "MACHER")) {
		primary.WriteString("K")
		secondary.WriteString("K")
		return current + 2
	}

	// Special case 'caesar'.
	if current == 0 && dmStringAt(original, current, 6, "CAESAR") {
		primary.WriteString("S")
		secondary.WriteString("S")
		return current + 2
	}

	// Italian 'chianti'.
	if dmStringAt(original, current, 4, "CHIA") {
		primary.WriteString("K")
		secondary.WriteString("K")
		return current + 2
	}

	if dmStringAt(original, current, 2, "CH") {
		// Find 'Michael'.
		if current > 0 && dmStringAt(original, current, 4, "CHAE") {
			primary.WriteString("K")
			secondary.WriteString("X")
			return current + 2
		}

		// Greek roots e.g. 'chemistry', 'chorus'.
		if current == 0 &&
			(dmStringAt(original, current+1, 5, "HARAC", "HARIS") ||
				dmStringAt(original, current+1, 3, "HOR", "HYM", "HIA", "HEM")) &&
			!dmStringAt(original, 0, 5, "CHORE") {
			primary.WriteString("K")
			secondary.WriteString("K")
			return current + 2
		}

		// Germanic, Greek, or otherwise 'ch' for 'kh' sound.
		if (dmStringAt(original, 0, 4, "VAN ", "VON ") ||
			dmStringAt(original, 0, 3, "SCH")) ||
			dmStringAt(original, current-2, 6,
				"ORCHES", "ARCHIT", "ORCHID") ||
			dmStringAt(original, current+2, 1, "T", "S") ||
			((dmStringAt(original, current-1, 1,
				"A", "O", "U", "E") || current == 0) &&
				dmStringAt(original, current+2, 1,
					"L", "R", "N", "M", "B", "H", "F", "V", "W", " ")) {
			primary.WriteString("K")
			secondary.WriteString("K")
		} else {
			if current > 0 {
				if dmStringAt(original, 0, 2, "MC") {
					primary.WriteString("K")
					secondary.WriteString("K")
				} else {
					primary.WriteString("X")
					secondary.WriteString("K")
				}
			} else {
				primary.WriteString("X")
				secondary.WriteString("X")
			}
		}
		return current + 2
	}

	// e.g., 'czerny'.
	if dmStringAt(original, current, 2, "CZ") &&
		!dmStringAt(original, current-2, 4, "WICZ") {
		primary.WriteString("S")
		secondary.WriteString("X")
		return current + 2
	}

	// e.g., 'focaccia'.
	if dmStringAt(original, current+1, 3, "CIA") {
		primary.WriteString("X")
		secondary.WriteString("X")
		return current + 3
	}

	// Double 'C', but not if e.g. 'McClellan'.
	if dmStringAt(original, current, 2, "CC") &&
		!(current == 1 && dmCharAt(original, 0) == 'M') {
		// 'bellocchio' but not 'bacchus'.
		if dmStringAt(original, current+2, 1, "I", "E", "H") &&
			!dmStringAt(original, current+2, 2, "HU") {
			// 'accident', 'accede', 'succeed'.
			if (current == 1 && dmCharAt(original, current-1) == 'A') ||
				dmStringAt(original, current-1, 5, "UCCEE", "UCCES") {
				primary.WriteString("KS")
				secondary.WriteString("KS")
			} else {
				primary.WriteString("X")
				secondary.WriteString("X")
			}
			return current + 3
		}
		// Pierce's rule.
		primary.WriteString("K")
		secondary.WriteString("K")
		return current + 2
	}

	if dmStringAt(original, current, 2, "CK", "CG", "CQ") {
		primary.WriteString("K")
		secondary.WriteString("K")
		return current + 2
	}

	if dmStringAt(original, current, 2, "CI", "CE", "CY") {
		// Italian vs. English.
		if dmStringAt(original, current, 3, "CIO", "CIE", "CIA") {
			primary.WriteString("S")
			secondary.WriteString("X")
		} else {
			primary.WriteString("S")
			secondary.WriteString("S")
		}
		return current + 2
	}

	// Else.
	primary.WriteString("K")
	secondary.WriteString("K")

	// Name sent in 'mac caffrey', 'mac gregor'.
	if dmStringAt(original, current+1, 2, " C", " Q", " G") {
		return current + 3
	}
	if dmStringAt(original, current+1, 1, "C", "K", "Q") &&
		!dmStringAt(original, current+1, 2, "CE", "CI") {
		return current + 2
	}
	return current + 1
}

func dmHandleD(original string, current, last int, primary, secondary *strings.Builder) int {
	if dmStringAt(original, current, 2, "DG") {
		if dmStringAt(original, current+2, 1, "I", "E", "Y") {
			// e.g. 'edge'.
			primary.WriteString("J")
			secondary.WriteString("J")
			return current + 3
		}
		// e.g. 'edgar'.
		primary.WriteString("TK")
		secondary.WriteString("TK")
		return current + 2
	}

	if dmStringAt(original, current, 2, "DT", "DD") {
		primary.WriteString("T")
		secondary.WriteString("T")
		return current + 2
	}

	primary.WriteString("T")
	secondary.WriteString("T")
	return current + 1
}

func dmHandleG(
	original string,
	current, length, last int,
	slavoGermanic bool,
	primary, secondary *strings.Builder,
) int {
	if dmCharAt(original, current+1) == 'H' {
		return dmHandleGH(original, current, length, last, slavoGermanic,
			primary, secondary)
	}

	if dmCharAt(original, current+1) == 'N' {
		if current == 1 && dmIsVowel(original, 0) && !slavoGermanic {
			primary.WriteString("KN")
			secondary.WriteString("N")
		} else if !dmStringAt(original, current+2, 2, "EY") &&
			dmCharAt(original, current+1) != 'Y' &&
			!slavoGermanic {
			primary.WriteString("N")
			secondary.WriteString("KN")
		} else {
			primary.WriteString("KN")
			secondary.WriteString("KN")
		}
		return current + 2
	}

	// 'tagliaro'.
	if dmStringAt(original, current+1, 2, "LI") && !slavoGermanic {
		primary.WriteString("KL")
		secondary.WriteString("L")
		return current + 2
	}

	// -ges-,-gep-,-gel-, -gie- at beginning.
	if current == 0 &&
		(dmCharAt(original, current+1) == 'Y' ||
			dmStringAt(original, current+1, 2,
				"ES", "EP", "EB", "EL", "EY", "IB", "IL", "IN", "IE",
				"EI", "ER")) {
		primary.WriteString("K")
		secondary.WriteString("J")
		return current + 2
	}

	// -ger-, -gy-.
	if (dmStringAt(original, current+1, 2, "ER") ||
		dmCharAt(original, current+1) == 'Y') &&
		!dmStringAt(original, 0, 6, "DANGER", "RANGER", "MANGER") &&
		!dmStringAt(original, current-1, 1, "E", "I") &&
		!dmStringAt(original, current-1, 3, "RGY", "OGY") {
		primary.WriteString("K")
		secondary.WriteString("J")
		return current + 2
	}

	// Italian e.g, 'biaggi'.
	if dmStringAt(original, current+1, 1, "E", "I", "Y") ||
		dmStringAt(original, current-1, 4, "AGGI", "OGGI") {
		// Obvious Germanic.
		if (dmStringAt(original, 0, 4, "VAN ", "VON ") ||
			dmStringAt(original, 0, 3, "SCH")) ||
			dmStringAt(original, current+1, 2, "ET") {
			primary.WriteString("K")
			secondary.WriteString("K")
		} else {
			// Always soft if French ending.
			if dmStringAt(original, current+1, 4, "IER ") {
				primary.WriteString("J")
				secondary.WriteString("J")
			} else {
				primary.WriteString("J")
				secondary.WriteString("K")
			}
		}
		return current + 2
	}

	primary.WriteString("K")
	secondary.WriteString("K")
	if dmCharAt(original, current+1) == 'G' {
		return current + 2
	}
	return current + 1
}

func dmHandleGH(
	original string,
	current, length, last int,
	slavoGermanic bool,
	primary, secondary *strings.Builder,
) int {
	if current > 0 && !dmIsVowel(original, current-1) {
		primary.WriteString("K")
		secondary.WriteString("K")
		return current + 2
	}

	if current < 3 {
		// 'ghislane', 'ghiradelli'.
		if current == 0 {
			if dmCharAt(original, current+2) == 'I' {
				primary.WriteString("J")
				secondary.WriteString("J")
			} else {
				primary.WriteString("K")
				secondary.WriteString("K")
			}
			return current + 2
		}
	}

	// Parker's rule (with some further refinements) - e.g. 'hugh'.
	if (current > 1 && dmStringAt(original, current-2, 1,
		"B", "H", "D")) ||
		(current > 2 && dmStringAt(original, current-3, 1,
			"B", "H", "D")) ||
		(current > 3 && dmStringAt(original, current-4, 1,
			"B", "H")) {
		return current + 2
	}

	// e.g., 'laugh', 'McLaughlin', 'cough', 'gough', 'rough', 'tough'.
	if current > 2 &&
		dmCharAt(original, current-1) == 'U' &&
		dmStringAt(original, current-3, 1,
			"C", "G", "L", "R", "T") {
		primary.WriteString("F")
		secondary.WriteString("F")
	} else if current > 0 && dmCharAt(original, current-1) != 'I' {
		primary.WriteString("K")
		secondary.WriteString("K")
	}

	return current + 2
}

func dmHandleJ(
	original string,
	current, length, last int,
	slavoGermanic bool,
	primary, secondary *strings.Builder,
) int {
	// Obvious Spanish, 'jose', 'san jacinto'.
	if dmStringAt(original, current, 4, "JOSE") ||
		dmStringAt(original, 0, 4, "SAN ") {
		if (current == 0 && dmCharAt(original, current+4) == ' ') ||
			dmStringAt(original, 0, 4, "SAN ") {
			primary.WriteString("H")
			secondary.WriteString("H")
		} else {
			primary.WriteString("J")
			secondary.WriteString("H")
		}
		return current + 1
	}

	if current == 0 && !dmStringAt(original, current, 4, "JOSE") {
		primary.WriteString("J")
		secondary.WriteString("A")
	} else {
		// Spanish pron. of e.g. 'bajador'.
		if dmIsVowel(original, current-1) &&
			!slavoGermanic &&
			(dmCharAt(original, current+1) == 'A' ||
				dmCharAt(original, current+1) == 'O') {
			primary.WriteString("J")
			secondary.WriteString("H")
		} else {
			if current == last {
				primary.WriteString("J")
			} else {
				if !dmStringAt(original, current+1, 1,
					"L", "T", "K", "S", "N", "M", "B", "Z") &&
					!dmStringAt(original, current-1, 1,
						"S", "K", "L") {
					primary.WriteString("J")
					secondary.WriteString("J")
				}
			}
		}
	}

	if dmCharAt(original, current+1) == 'J' {
		return current + 2
	}
	return current + 1
}

func dmHandleL(
	original string, current, length, last int, primary, secondary *strings.Builder,
) int {
	if dmCharAt(original, current+1) == 'L' {
		// Spanish e.g. 'cabrillo', 'gallegos'.
		if (current == length-3 &&
			dmStringAt(original, current-1, 4,
				"ILLO", "ILLA", "ALLE")) ||
			((dmStringAt(original, last-1, 2, "AS", "OS") ||
				dmStringAt(original, last, 1, "A", "O")) &&
				dmStringAt(original, current-1, 4, "ALLE")) {
			primary.WriteString("L")
			return current + 2
		}
		current += 2
	} else {
		current++
	}
	primary.WriteString("L")
	secondary.WriteString("L")
	return current
}

func dmHandleS(
	original string,
	current, length, last int,
	slavoGermanic bool,
	primary, secondary *strings.Builder,
) int {
	// Special cases 'island', 'isle', 'carlisle', 'carlysle'.
	if dmStringAt(original, current-1, 3, "ISL", "YSL") {
		return current + 1
	}

	// Special case 'sugar-'.
	if current == 0 && dmStringAt(original, current, 5, "SUGAR") {
		primary.WriteString("X")
		secondary.WriteString("S")
		return current + 1
	}

	if dmStringAt(original, current, 2, "SH") {
		// Germanic.
		if dmStringAt(original, current+1, 4,
			"HEIM", "HOEK", "HOLM", "HOLZ") {
			primary.WriteString("S")
			secondary.WriteString("S")
		} else {
			primary.WriteString("X")
			secondary.WriteString("X")
		}
		return current + 2
	}

	// Italian & Armenian.
	if dmStringAt(original, current, 3, "SIO", "SIA") ||
		dmStringAt(original, current, 4, "SIAN") {
		if !slavoGermanic {
			primary.WriteString("S")
			secondary.WriteString("X")
		} else {
			primary.WriteString("S")
			secondary.WriteString("S")
		}
		return current + 3
	}

	// German & anglicisations, e.g. 'smith' match 'schmidt',
	// 'snider' match 'schneider'. Also, -sz- in Slavic language.
	if (current == 0 &&
		dmStringAt(original, current+1, 1, "M", "N", "L", "W")) ||
		dmStringAt(original, current+1, 1, "Z") {
		primary.WriteString("S")
		secondary.WriteString("X")
		if dmStringAt(original, current+1, 1, "Z") {
			return current + 2
		}
		return current + 1
	}

	if dmStringAt(original, current, 2, "SC") {
		return dmHandleSC(original, current, length, last,
			primary, secondary)
	}

	// French e.g. 'resnais', 'artois'.
	if current == last &&
		dmStringAt(original, current-2, 2, "AI", "OI") {
		secondary.WriteString("S")
	} else {
		primary.WriteString("S")
		secondary.WriteString("S")
	}

	if dmStringAt(original, current+1, 1, "S", "Z") {
		return current + 2
	}
	return current + 1
}

func dmHandleSC(
	original string, current, length, last int, primary, secondary *strings.Builder,
) int {
	// Schlesinger's rule.
	if dmCharAt(original, current+2) == 'H' {
		// Dutch origin, e.g. 'school', 'schooner'.
		if dmStringAt(original, current+3, 2,
			"OO", "ER", "EN", "UY", "ED", "EM") {
			// 'schermerhorn', 'schenker'.
			if dmStringAt(original, current+3, 2, "ER", "EN") {
				primary.WriteString("X")
				secondary.WriteString("SK")
			} else {
				primary.WriteString("SK")
				secondary.WriteString("SK")
			}
			return current + 3
		}
		if current == 0 && !dmIsVowel(original, 3) &&
			dmCharAt(original, 3) != 'W' {
			primary.WriteString("X")
			secondary.WriteString("S")
		} else {
			primary.WriteString("X")
			secondary.WriteString("X")
		}
		return current + 3
	}

	if dmStringAt(original, current+2, 1, "I", "E", "Y") {
		primary.WriteString("S")
		secondary.WriteString("S")
		return current + 3
	}

	primary.WriteString("SK")
	secondary.WriteString("SK")
	return current + 3
}

func dmHandleT(
	original string, current, length, last int, primary, secondary *strings.Builder,
) int {
	if dmStringAt(original, current, 4, "TION") {
		primary.WriteString("X")
		secondary.WriteString("X")
		return current + 3
	}

	if dmStringAt(original, current, 3, "TIA", "TCH") {
		primary.WriteString("X")
		secondary.WriteString("X")
		return current + 3
	}

	if dmStringAt(original, current, 2, "TH") ||
		dmStringAt(original, current, 3, "TTH") {
		// Special case 'thomas', 'thames' or Germanic.
		if dmStringAt(original, current+2, 2, "OM", "AM") ||
			dmStringAt(original, 0, 4, "VAN ", "VON ") ||
			dmStringAt(original, 0, 3, "SCH") {
			primary.WriteString("T")
			secondary.WriteString("T")
		} else {
			primary.WriteString("0")
			secondary.WriteString("T")
		}
		return current + 2
	}

	primary.WriteString("T")
	secondary.WriteString("T")
	if dmStringAt(original, current+1, 1, "T", "D") {
		return current + 2
	}
	return current + 1
}

func dmHandleW(
	original string,
	current, length, last int,
	slavoGermanic bool,
	primary, secondary *strings.Builder,
) int {
	// Can also be in middle of word.
	if dmStringAt(original, current, 2, "WR") {
		primary.WriteString("R")
		secondary.WriteString("R")
		return current + 2
	}

	if current == 0 &&
		(dmIsVowel(original, current+1) ||
			dmStringAt(original, current, 2, "WH")) {
		if dmIsVowel(original, current+1) {
			primary.WriteString("A")
			secondary.WriteString("F")
		} else {
			primary.WriteString("A")
			secondary.WriteString("A")
		}
	}

	// Arnow should match Arnoff.
	if (current == last && dmIsVowel(original, current-1)) ||
		dmStringAt(original, current-1, 5,
			"EWSKI", "EWSKY", "OWSKI", "OWSKY") ||
		dmStringAt(original, 0, 3, "SCH") {
		secondary.WriteString("F")
		return current + 1
	}

	// Polish e.g. 'filipowicz'.
	if dmStringAt(original, current, 4, "WICZ", "WITZ") {
		primary.WriteString("TS")
		secondary.WriteString("FX")
		return current + 4
	}

	return current + 1
}

func dmHandleZ(
	original string, current, last int, slavoGermanic bool, primary, secondary *strings.Builder,
) int {
	// Chinese pinyin e.g. 'zhao'.
	if dmCharAt(original, current+1) == 'H' {
		primary.WriteString("J")
		secondary.WriteString("J")
		return current + 2
	}

	if dmStringAt(original, current+1, 2, "ZO", "ZI", "ZA") ||
		(slavoGermanic && current > 0 &&
			dmCharAt(original, current-1) != 'T') {
		primary.WriteString("S")
		secondary.WriteString("TS")
	} else {
		primary.WriteString("S")
		secondary.WriteString("S")
	}

	if dmCharAt(original, current+1) == 'Z' {
		return current + 2
	}
	return current + 1
}
