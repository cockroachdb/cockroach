// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package fuzzystrmatch

import (
	"sort"
	"strings"
)

// dmCodeLen is the number of digits in a Daitch-Mokotoff soundex code.
const dmCodeLen = 6

// dmCodes represents the three positional codes for a letter sequence:
// [0] = at the start of name, [1] = before a vowel, [2] = any other position.
// "X" means "not coded" (letter is ignored in that position).
type dmCodes [3]string

// dmRule represents a single letter-to-code mapping in the coding table.
// A letter sequence may have multiple alternative code sets (branching).
type dmRule struct {
	pattern string
	codes   []dmCodes
}

// dmTrie is a trie node for efficient longest-match lookup.
type dmTrieNode struct {
	children map[byte]*dmTrieNode
	codes    []dmCodes // non-nil if this node represents a complete match
}

// dmBranch tracks the state of one code branch being built.
type dmBranch struct {
	code      [dmCodeLen]byte
	length    int
	lastDigit byte
}

// dmISO8859_1ToASCII maps Latin-1 codepoints U+00C0–U+00FF to uppercase
// ASCII base letters. Entries of 0 indicate non-letter codepoints (× and ÷)
// which are skipped. Matches PostgreSQL's iso8859_1_to_ascii_upper table.
var dmISO8859_1ToASCII = [64]byte{
	'A', 'A', 'A', 'A', 'A', 'A', 'E', 'C', // 0xC0-0xC7
	'E', 'E', 'E', 'E', 'I', 'I', 'I', 'I', // 0xC8-0xCF
	'D', 'N', 'O', 'O', 'O', 'O', 'O', 0, //   0xD0-0xD7 (× = skip)
	'O', 'U', 'U', 'U', 'U', 'Y', 'D', 'S', // 0xD8-0xDF
	'A', 'A', 'A', 'A', 'A', 'A', 'E', 'C', // 0xE0-0xE7
	'E', 'E', 'E', 'E', 'I', 'I', 'I', 'I', // 0xE8-0xEF
	'D', 'N', 'O', 'O', 'O', 'O', 'O', 0, //   0xF0-0xF7 (÷ = skip)
	'O', 'U', 'U', 'U', 'U', 'Y', 'D', 'Y', // 0xF8-0xFF
}

// The coding table, ported from PostgreSQL's daitch_mokotoff_coding.h.
// Each entry maps a letter sequence to one or more code alternatives.
var dmRules = []dmRule{
	// Vowel combinations.
	{"AI", []dmCodes{{"0", "1", "X"}}},
	{"AJ", []dmCodes{{"0", "1", "X"}}},
	{"AU", []dmCodes{{"0", "7", "X"}}},
	{"AY", []dmCodes{{"0", "1", "X"}}},
	{"A", []dmCodes{{"0", "X", "X"}}},

	{"EI", []dmCodes{{"0", "1", "X"}}},
	{"EJ", []dmCodes{{"0", "1", "X"}}},
	{"EU", []dmCodes{{"1", "1", "X"}}},
	{"EY", []dmCodes{{"0", "1", "X"}}},
	{"E", []dmCodes{{"0", "X", "X"}}},

	{"IA", []dmCodes{{"1", "X", "X"}}},
	{"IE", []dmCodes{{"1", "X", "X"}}},
	{"IO", []dmCodes{{"1", "X", "X"}}},
	{"IU", []dmCodes{{"1", "X", "X"}}},
	{"I", []dmCodes{{"0", "X", "X"}}},

	{"OI", []dmCodes{{"0", "1", "X"}}},
	{"OJ", []dmCodes{{"0", "1", "X"}}},
	{"OY", []dmCodes{{"0", "1", "X"}}},
	{"O", []dmCodes{{"0", "X", "X"}}},

	{"UI", []dmCodes{{"0", "1", "X"}}},
	{"UE", []dmCodes{{"0", "1", "X"}}},
	{"UJ", []dmCodes{{"0", "1", "X"}}},
	{"UY", []dmCodes{{"0", "1", "X"}}},
	{"U", []dmCodes{{"0", "X", "X"}}},

	// Consonant combinations.
	{"B", []dmCodes{{"7", "7", "7"}}},

	{"CHS", []dmCodes{{"5", "54", "54"}}},
	{"CH", []dmCodes{{"5", "5", "5"}, {"4", "4", "4"}}},
	{"CK", []dmCodes{{"5", "5", "5"}, {"45", "45", "45"}}},
	{"CSZ", []dmCodes{{"4", "4", "4"}}},
	{"CS", []dmCodes{{"4", "4", "4"}}},
	{"CZS", []dmCodes{{"4", "4", "4"}}},
	{"CZ", []dmCodes{{"4", "4", "4"}}},
	{"C", []dmCodes{{"5", "5", "5"}, {"4", "4", "4"}}},

	{"DRS", []dmCodes{{"4", "4", "4"}}},
	{"DRZ", []dmCodes{{"4", "4", "4"}}},
	{"DSH", []dmCodes{{"4", "4", "4"}}},
	{"DSZ", []dmCodes{{"4", "4", "4"}}},
	{"DS", []dmCodes{{"4", "4", "4"}}},
	{"DT", []dmCodes{{"3", "3", "3"}}},
	{"DZH", []dmCodes{{"4", "4", "4"}}},
	{"DZS", []dmCodes{{"4", "4", "4"}}},
	{"DZ", []dmCodes{{"4", "4", "4"}}},
	{"D", []dmCodes{{"3", "3", "3"}}},

	{"FB", []dmCodes{{"7", "7", "7"}}},
	{"F", []dmCodes{{"7", "7", "7"}}},

	{"G", []dmCodes{{"5", "5", "5"}}},

	{"H", []dmCodes{{"5", "5", "X"}}},

	{"J", []dmCodes{{"1", "X", "X"}, {"4", "4", "4"}}},

	{"KH", []dmCodes{{"5", "5", "5"}}},
	{"KS", []dmCodes{{"5", "54", "54"}}},
	{"K", []dmCodes{{"5", "5", "5"}}},

	{"L", []dmCodes{{"8", "8", "8"}}},

	{"MN", []dmCodes{{"66", "66", "66"}}},
	{"M", []dmCodes{{"6", "6", "6"}}},

	{"NM", []dmCodes{{"66", "66", "66"}}},
	{"N", []dmCodes{{"6", "6", "6"}}},

	{"PF", []dmCodes{{"7", "7", "7"}}},
	{"PH", []dmCodes{{"7", "7", "7"}}},
	{"P", []dmCodes{{"7", "7", "7"}}},

	{"Q", []dmCodes{{"5", "5", "5"}}},

	{"RS", []dmCodes{{"94", "94", "94"}, {"4", "4", "4"}}},
	{"RZ", []dmCodes{{"94", "94", "94"}, {"4", "4", "4"}}},
	{"R", []dmCodes{{"9", "9", "9"}}},

	{"SCHTSCH", []dmCodes{{"2", "4", "4"}}},
	{"SCHTSH", []dmCodes{{"2", "4", "4"}}},
	{"SCHTCH", []dmCodes{{"2", "4", "4"}}},
	{"SCHD", []dmCodes{{"2", "43", "43"}}},
	{"SCHT", []dmCodes{{"2", "43", "43"}}},
	{"SCH", []dmCodes{{"4", "4", "4"}}},
	{"SC", []dmCodes{{"2", "4", "4"}}},
	{"SHTCH", []dmCodes{{"2", "4", "4"}}},
	{"SHTSH", []dmCodes{{"2", "4", "4"}}},
	{"SHCH", []dmCodes{{"2", "4", "4"}}},
	{"SHD", []dmCodes{{"2", "43", "43"}}},
	{"SHT", []dmCodes{{"2", "43", "43"}}},
	{"SH", []dmCodes{{"4", "4", "4"}}},
	{"STCH", []dmCodes{{"2", "4", "4"}}},
	{"STSCH", []dmCodes{{"2", "4", "4"}}},
	{"STRZ", []dmCodes{{"2", "4", "4"}}},
	{"STRS", []dmCodes{{"2", "4", "4"}}},
	{"STSH", []dmCodes{{"2", "4", "4"}}},
	{"SD", []dmCodes{{"2", "43", "43"}}},
	{"ST", []dmCodes{{"2", "43", "43"}}},
	{"SZCZ", []dmCodes{{"2", "4", "4"}}},
	{"SZCS", []dmCodes{{"2", "4", "4"}}},
	{"SZD", []dmCodes{{"2", "43", "43"}}},
	{"SZT", []dmCodes{{"2", "43", "43"}}},
	{"SZ", []dmCodes{{"4", "4", "4"}}},
	{"S", []dmCodes{{"4", "4", "4"}}},

	{"TTSCH", []dmCodes{{"4", "4", "4"}}},
	{"TTCH", []dmCodes{{"4", "4", "4"}}},
	{"TTSZ", []dmCodes{{"4", "4", "4"}}},
	{"TTZ", []dmCodes{{"4", "4", "4"}}},
	{"TTS", []dmCodes{{"4", "4", "4"}}},
	{"TSCH", []dmCodes{{"4", "4", "4"}}},
	{"TSH", []dmCodes{{"4", "4", "4"}}},
	{"TSZ", []dmCodes{{"4", "4", "4"}}},
	{"TCH", []dmCodes{{"4", "4", "4"}}},
	{"TC", []dmCodes{{"4", "4", "4"}}},
	{"TH", []dmCodes{{"3", "3", "3"}}},
	{"TRS", []dmCodes{{"4", "4", "4"}}},
	{"TRZ", []dmCodes{{"4", "4", "4"}}},
	{"TS", []dmCodes{{"4", "4", "4"}}},
	{"TZ", []dmCodes{{"4", "4", "4"}}},
	{"TZS", []dmCodes{{"4", "4", "4"}}},
	{"T", []dmCodes{{"3", "3", "3"}}},

	{"V", []dmCodes{{"7", "7", "7"}}},
	{"W", []dmCodes{{"7", "7", "7"}}},
	{"X", []dmCodes{{"5", "54", "54"}}},
	{"Y", []dmCodes{{"1", "X", "X"}}},

	{"ZHDZH", []dmCodes{{"2", "4", "4"}}},
	{"ZHDZ", []dmCodes{{"2", "4", "4"}}},
	{"ZHD", []dmCodes{{"2", "43", "43"}}},
	{"ZH", []dmCodes{{"4", "4", "4"}}},
	{"ZDZH", []dmCodes{{"2", "4", "4"}}},
	{"ZDZ", []dmCodes{{"2", "4", "4"}}},
	{"ZD", []dmCodes{{"2", "43", "43"}}},
	{"ZSCH", []dmCodes{{"4", "4", "4"}}},
	{"ZSH", []dmCodes{{"4", "4", "4"}}},
	{"ZS", []dmCodes{{"4", "4", "4"}}},
	{"Z", []dmCodes{{"4", "4", "4"}}},

	// Polish/Romanian characters, represented by placeholder bytes.
	// These bytes are reserved in dmNormalize and never appear from
	// regular ASCII input.
	{"[", []dmCodes{{"X", "X", "6"}, {"X", "X", "X"}}},  // Ą/ą
	{"\\", []dmCodes{{"X", "X", "6"}, {"X", "X", "X"}}}, // Ę/ę
	{"]", []dmCodes{{"3", "3", "3"}, {"4", "4", "4"}}},  // Ţ/ţ/Ț/ț
}

var dmTrie *dmTrieNode

func init() {
	dmTrie = buildDMTrie()
}

func buildDMTrie() *dmTrieNode {
	root := &dmTrieNode{children: make(map[byte]*dmTrieNode)}
	for _, rule := range dmRules {
		node := root
		for i := 0; i < len(rule.pattern); i++ {
			ch := rule.pattern[i]
			if node.children[ch] == nil {
				node.children[ch] = &dmTrieNode{
					children: make(map[byte]*dmTrieNode),
				}
			}
			node = node.children[ch]
		}
		node.codes = rule.codes
	}
	return root
}

// dmLookup finds the longest matching letter sequence in the trie starting
// at position pos in the input string. Returns the codes for that sequence
// and the number of characters consumed.
func dmLookup(input string, pos int) ([]dmCodes, int) {
	node := dmTrie
	var bestCodes []dmCodes
	bestLen := 0

	for i := pos; i < len(input); i++ {
		child := node.children[input[i]]
		if child == nil {
			break
		}
		node = child
		if node.codes != nil {
			bestCodes = node.codes
			bestLen = i - pos + 1
		}
	}

	return bestCodes, bestLen
}

// dmIsVowelCode returns true if the code set represents a vowel-like sound
// (starts with '0' or '1'). Used to determine the "before a vowel" context.
func dmIsVowelCode(codes []dmCodes) bool {
	if len(codes) == 0 {
		return false
	}
	c := codes[0][0]
	return len(c) > 0 && (c[0] == '0' || c[0] == '1')
}

// DaitchMokotoff computes Daitch-Mokotoff soundex codes for the input string.
// Returns a sorted slice of unique 6-digit code strings.
func DaitchMokotoff(source string) []string {
	// Transliterate to uppercase ASCII (with placeholder bytes for
	// Polish/Romanian characters), stripping unrecognized characters.
	input := dmNormalize(source)
	if len(input) == 0 {
		return nil
	}

	// Start with a single branch.
	branches := []dmBranch{{}}

	pos := 0
	letterNo := 0
	for pos < len(input) {
		// Find the longest matching sequence.
		codes, advance := dmLookup(input, pos)
		if advance == 0 {
			// No match (non-alphabetic character); skip.
			pos++
			continue
		}

		// Look ahead to determine if the next letter is a vowel.
		nextPos := pos + advance
		var nextCodes []dmCodes
		if nextPos < len(input) {
			nextCodes, _ = dmLookup(input, nextPos)
		}

		// Determine code column index.
		var codeIndex int
		if letterNo == 0 {
			codeIndex = 0 // start of name
		} else if dmIsVowelCode(nextCodes) {
			codeIndex = 1 // before a vowel
		} else {
			codeIndex = 2 // any other
		}

		// Apply codes to all branches, possibly creating new branches.
		branches = dmApplyCodes(branches, codes, codeIndex)

		pos = nextPos
		letterNo++
	}

	// Pad all codes to 6 digits and collect unique results.
	seen := make(map[string]struct{})
	var result []string
	for _, b := range branches {
		code := dmPadCode(b)
		if _, ok := seen[code]; !ok {
			seen[code] = struct{}{}
			result = append(result, code)
		}
	}

	sort.Strings(result)
	return result
}

// dmApplyCodes processes one letter's codes across all branches.
// Equivalent branches are deduplicated immediately to avoid multiplicative
// growth from alternate paths that lead to the same state.
func dmApplyCodes(branches []dmBranch, codes []dmCodes, codeIndex int) []dmBranch {
	result := make([]dmBranch, 0, len(branches))
	seen := make(map[dmBranch]struct{}, len(branches))
	for _, b := range branches {
		if b.length >= dmCodeLen {
			if _, ok := seen[b]; !ok {
				seen[b] = struct{}{}
				result = append(result, b)
			}
			continue
		}
		for _, alt := range codes {
			codeStr := alt[codeIndex]
			newBranches := dmApplyCodeStr(b, codeStr)
			for _, newBranch := range newBranches {
				if _, ok := seen[newBranch]; ok {
					continue
				}
				seen[newBranch] = struct{}{}
				result = append(result, newBranch)
			}
		}
	}
	return result
}

// dmApplyCodeStr applies a single code string (e.g. "54", "X") to a branch.
func dmApplyCodeStr(b dmBranch, codeStr string) []dmBranch {
	if codeStr == "X" {
		// Not coded. Reset lastDigit so the next consonant won't be
		// deduped against the consonant before this vowel.
		b.lastDigit = 0
		return []dmBranch{b}
	}

	return dmApplyDigits(b, codeStr, 0)
}

// dmApplyDigits recursively applies code digits to a branch.
func dmApplyDigits(b dmBranch, codeStr string, digitIdx int) []dmBranch {
	if digitIdx >= len(codeStr) || b.length >= dmCodeLen {
		return []dmBranch{b}
	}

	digit := codeStr[digitIdx]
	if digit == 'X' {
		return dmApplyDigits(b, codeStr, digitIdx+1)
	}

	var results []dmBranch

	// Dedup: first digit might match previous code digit.
	if digitIdx == 0 && digit == b.lastDigit {
		// Same as previous - skip path (dedup).
		skipBranch := b
		results = append(results, dmApplyDigits(skipBranch, codeStr, digitIdx+1)...)
	}

	if digitIdx > 0 || digit != b.lastDigit {
		// Different from previous - add path.
		addBranch := b
		if addBranch.length < dmCodeLen {
			addBranch.code[addBranch.length] = digit
			addBranch.length++
			addBranch.lastDigit = digit
		}
		results = append(results, dmApplyDigits(addBranch, codeStr, digitIdx+1)...)
	}

	return results
}

// dmPadCode pads a branch's code to 6 digits with zeros.
func dmPadCode(b dmBranch) string {
	var buf [dmCodeLen]byte
	copy(buf[:], b.code[:b.length])
	for i := b.length; i < dmCodeLen; i++ {
		buf[i] = '0'
	}
	return string(buf[:])
}

// dmNormalize transliterates the input to uppercase ASCII letters and
// placeholder bytes for Polish/Romanian characters ([, \, ] for Ą, Ę, Ţ/Ț).
// ISO-8859-1 accented letters (U+00C0–U+00FF) are mapped to their ASCII
// base letter. All other characters are dropped.
func dmNormalize(s string) string {
	var buf strings.Builder
	buf.Grow(len(s))
	for _, r := range s {
		switch {
		case r >= 'A' && r <= 'Z':
			buf.WriteByte(byte(r))
		case r >= 'a' && r <= 'z':
			buf.WriteByte(byte(r - 'a' + 'A'))
		case r >= 0xC0 && r <= 0xFF:
			if c := dmISO8859_1ToASCII[r-0xC0]; c != 0 {
				buf.WriteByte(c)
			}
		case r == 0x0104 || r == 0x0105: // Ą/ą
			buf.WriteByte('[')
		case r == 0x0118 || r == 0x0119: // Ę/ę
			buf.WriteByte('\\')
		case r == 0x0162 || r == 0x0163 || r == 0x021A || r == 0x021B: // Ţ/ţ/Ț/ț
			buf.WriteByte(']')
		}
	}
	return buf.String()
}
