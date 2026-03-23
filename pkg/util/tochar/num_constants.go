// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tochar

// numPos enumerates the numeric format keywords.
// Taken from https://github.com/postgres/postgres/blob/b0b72c64a0ce7bf5dd78a80b33d85c89c943ad0d/src/backend/utils/adt/formatting.c#L717.
type numPos int

const (
	NUM_COMMA numPos = iota
	NUM_DEC
	NUM_0
	NUM_9
	NUM_B
	NUM_C
	NUM_D
	NUM_E
	NUM_FM
	NUM_G
	NUM_L
	NUM_MI
	NUM_PL
	NUM_PR
	NUM_RN
	NUM_SG
	NUM_SP
	NUM_S
	NUM_TH
	NUM_V
	NUM_b
	NUM_c
	NUM_d
	NUM_e
	NUM_fm
	NUM_g
	NUM_l
	NUM_mi
	NUM_pl
	NUM_pr
	NUM_rn
	NUM_sg
	NUM_sp
	NUM_s
	NUM_th
	NUM_v

	NUM_last
)

// numKeyWord describes a numeric format keyword.
type numKeyWord struct {
	name string
	id   numPos
}

// numKeywords is the sorted array of numeric format keywords, ordered in
// ASCII order. Matches PostgreSQL's NUM_keywords.
// Taken from https://github.com/postgres/postgres/blob/b0b72c64a0ce7bf5dd78a80b33d85c89c943ad0d/src/backend/utils/adt/formatting.c#L889.
var numKeywords = []numKeyWord{
	{",", NUM_COMMA},
	{".", NUM_DEC},
	{"0", NUM_0},
	{"9", NUM_9},
	{"B", NUM_B},
	{"C", NUM_C},
	{"D", NUM_D},
	{"EEEE", NUM_E},
	{"FM", NUM_FM},
	{"G", NUM_G},
	{"L", NUM_L},
	{"MI", NUM_MI},
	{"PL", NUM_PL},
	{"PR", NUM_PR},
	{"RN", NUM_RN},
	{"SG", NUM_SG},
	{"SP", NUM_SP},
	{"S", NUM_S},
	{"TH", NUM_TH},
	{"V", NUM_V},
	{"b", NUM_b},
	{"c", NUM_c},
	{"d", NUM_d},
	{"eeee", NUM_e},
	{"fm", NUM_fm},
	{"g", NUM_g},
	{"l", NUM_l},
	{"mi", NUM_mi},
	{"pl", NUM_pl},
	{"pr", NUM_pr},
	{"rn", NUM_rn},
	{"sg", NUM_sg},
	{"sp", NUM_sp},
	{"s", NUM_s},
	{"th", NUM_th},
	{"v", NUM_v},
}

// numIndex maps ASCII characters (starting from space, position 32) to the
// start index in numKeywords of possible matches.
// Taken from https://github.com/postgres/postgres/blob/b0b72c64a0ce7bf5dd78a80b33d85c89c943ad0d/src/backend/utils/adt/formatting.c#L961.
var numIndex = []numPos{
	/*---- first 0..31 chars are skipped ----*/

	-1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, NUM_COMMA, -1, NUM_DEC, -1, NUM_0, -1,
	-1, -1, -1, -1, -1, -1, -1, NUM_9, -1, -1,
	-1, -1, -1, -1, -1, -1, NUM_B, NUM_C, NUM_D, NUM_E,
	NUM_FM, NUM_G, -1, -1, -1, -1, NUM_L, NUM_MI, -1, -1,
	NUM_PL, -1, NUM_RN, NUM_SG, NUM_TH, -1, NUM_V, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, NUM_b, NUM_c,
	NUM_d, NUM_e, NUM_fm, NUM_g, -1, -1, -1, -1, NUM_l, NUM_mi,
	-1, -1, NUM_pl, -1, NUM_rn, NUM_sg, NUM_th, -1, NUM_v, -1,
	-1, -1, -1, -1, -1, -1,

	/*---- chars over 126 are skipped ----*/
}

// numFormatNode represents a parsed node in a numeric format string.
type numFormatNode struct {
	typ       formatNodeType
	character string
	key       *numKeyWord
}

// Roman numeral tables for int_to_roman conversion.
var (
	romanOnes     = []string{"I", "II", "III", "IV", "V", "VI", "VII", "VIII", "IX"}
	romanTens     = []string{"X", "XX", "XXX", "XL", "L", "LX", "LXX", "LXXX", "XC"}
	romanHundreds = []string{"C", "CC", "CCC", "CD", "D", "DC", "DCC", "DCCC", "CM"}
)
