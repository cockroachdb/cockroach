// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tochar

// fromCharDateMode is used to denote date conventions.
// Taken from https://github.com/postgres/postgres/blob/b0b72c64a0ce7bf5dd78a80b33d85c89c943ad0d/src/backend/utils/adt/formatting.c#L170-L175.
type fromCharDateMode int

const (
	fromCharDateNone = 0
	fromCharDateGregorian
	fromCharDateISOWeek
)

// Taken from https://github.com/postgres/postgres/blob/b0b72c64a0ce7bf5dd78a80b33d85c89c943ad0d/src/backend/utils/adt/formatting.c#L632.
// Denotes a "keyword" used for to_char.
type dchPos int

// This block is word-for-word out of the enum for dch_pos in PG.
const (
	DCH_A_D dchPos = iota
	DCH_A_M
	DCH_AD
	DCH_AM
	DCH_B_C
	DCH_BC
	DCH_CC
	DCH_DAY
	DCH_DDD
	DCH_DD
	DCH_DY
	DCH_Day
	DCH_Dy
	DCH_D
	DCH_FF1
	DCH_FF2
	DCH_FF3
	DCH_FF4
	DCH_FF5
	DCH_FF6
	DCH_FX
	DCH_HH24
	DCH_HH12
	DCH_HH
	DCH_IDDD
	DCH_ID
	DCH_IW
	DCH_IYYY
	DCH_IYY
	DCH_IY
	DCH_I
	DCH_J
	DCH_MI
	DCH_MM
	DCH_MONTH
	DCH_MON
	DCH_MS
	DCH_Month
	DCH_Mon
	DCH_OF
	DCH_P_M
	DCH_PM
	DCH_Q
	DCH_RM
	DCH_SSSSS
	DCH_SSSS
	DCH_SS
	DCH_TZH
	DCH_TZM
	DCH_TZ
	DCH_US
	DCH_WW
	DCH_W
	DCH_Y_YYY
	DCH_YYYY
	DCH_YYY
	DCH_YY
	DCH_Y
	DCH_a_d
	DCH_a_m
	DCH_ad
	DCH_am
	DCH_b_c
	DCH_bc
	DCH_cc
	DCH_day
	DCH_ddd
	DCH_dd
	DCH_dy
	DCH_d
	DCH_ff1
	DCH_ff2
	DCH_ff3
	DCH_ff4
	DCH_ff5
	DCH_ff6
	DCH_fx
	DCH_hh24
	DCH_hh12
	DCH_hh
	DCH_iddd
	DCH_id
	DCH_iw
	DCH_iyyy
	DCH_iyy
	DCH_iy
	DCH_i
	DCH_j
	DCH_mi
	DCH_mm
	DCH_month
	DCH_mon
	DCH_ms
	DCH_of
	DCH_p_m
	DCH_pm
	DCH_q
	DCH_rm
	DCH_sssss
	DCH_ssss
	DCH_ss
	DCH_tzh
	DCH_tzm
	DCH_tz
	DCH_us
	DCH_ww
	DCH_w
	DCH_y_yyy
	DCH_yyyy
	DCH_yyy
	DCH_yy
	DCH_y

	DCH_last
)

// KeyWord is used to denote properties from the dchPos.
// Taken from https://github.com/postgres/postgres/blob/b0b72c64a0ce7bf5dd78a80b33d85c89c943ad0d/src/backend/utils/adt/formatting.c#L177-L184
type keyWord struct {
	name     string
	id       dchPos
	isDigit  bool
	dateMode fromCharDateMode
}

// dchKeywords is an array of keyWords, ordered in ASCII order.
// Taken from https://github.com/postgres/postgres/blob/b0b72c64a0ce7bf5dd78a80b33d85c89c943ad0d/src/backend/utils/adt/formatting.c#L798.
var dchKeywords = []keyWord{
	{"A.D.", DCH_A_D, false, fromCharDateNone}, /* A */
	{"A.M.", DCH_A_M, false, fromCharDateNone},
	{"AD", DCH_AD, false, fromCharDateNone},
	{"AM", DCH_AM, false, fromCharDateNone},
	{"B.C.", DCH_B_C, false, fromCharDateNone}, /* B */
	{"BC", DCH_BC, false, fromCharDateNone},
	{"CC", DCH_CC, true, fromCharDateNone},    /* C */
	{"DAY", DCH_DAY, false, fromCharDateNone}, /* D */
	{"DDD", DCH_DDD, true, fromCharDateGregorian},
	{"DD", DCH_DD, true, fromCharDateGregorian},
	{"DY", DCH_DY, false, fromCharDateNone},
	{"Day", DCH_Day, false, fromCharDateNone},
	{"Dy", DCH_Dy, false, fromCharDateNone},
	{"D", DCH_D, true, fromCharDateGregorian},
	{"FF1", DCH_FF1, false, fromCharDateNone}, /* F */
	{"FF2", DCH_FF2, false, fromCharDateNone},
	{"FF3", DCH_FF3, false, fromCharDateNone},
	{"FF4", DCH_FF4, false, fromCharDateNone},
	{"FF5", DCH_FF5, false, fromCharDateNone},
	{"FF6", DCH_FF6, false, fromCharDateNone},
	{"FX", DCH_FX, false, fromCharDateNone},
	{"HH24", DCH_HH24, true, fromCharDateNone}, /* H */
	{"HH12", DCH_HH12, true, fromCharDateNone},
	{"HH", DCH_HH, true, fromCharDateNone},
	{"IDDD", DCH_IDDD, true, fromCharDateISOWeek}, /* I */
	{"ID", DCH_ID, true, fromCharDateISOWeek},
	{"IW", DCH_IW, true, fromCharDateISOWeek},
	{"IYYY", DCH_IYYY, true, fromCharDateISOWeek},
	{"IYY", DCH_IYY, true, fromCharDateISOWeek},
	{"IY", DCH_IY, true, fromCharDateISOWeek},
	{"I", DCH_I, true, fromCharDateISOWeek},
	{"J", DCH_J, true, fromCharDateNone},   /* J */
	{"MI", DCH_MI, true, fromCharDateNone}, /* M */
	{"MM", DCH_MM, true, fromCharDateGregorian},
	{"MONTH", DCH_MONTH, false, fromCharDateGregorian},
	{"MON", DCH_MON, false, fromCharDateGregorian},
	{"MS", DCH_MS, true, fromCharDateNone},
	{"Month", DCH_Month, false, fromCharDateGregorian},
	{"Mon", DCH_Mon, false, fromCharDateGregorian},
	{"OF", DCH_OF, false, fromCharDateNone},    /* O */
	{"P.M.", DCH_P_M, false, fromCharDateNone}, /* P */
	{"PM", DCH_PM, false, fromCharDateNone},
	{"Q", DCH_Q, true, fromCharDateNone},         /* Q */
	{"RM", DCH_RM, false, fromCharDateGregorian}, /* R */
	{"SSSSS", DCH_SSSS, true, fromCharDateNone},  /* S */
	{"SSSS", DCH_SSSS, true, fromCharDateNone},
	{"SS", DCH_SS, true, fromCharDateNone},
	{"TZH", DCH_TZH, false, fromCharDateNone}, /* T */
	{"TZM", DCH_TZM, true, fromCharDateNone},
	{"TZ", DCH_TZ, false, fromCharDateNone},
	{"US", DCH_US, true, fromCharDateNone},      /* U */
	{"WW", DCH_WW, true, fromCharDateGregorian}, /* W */
	{"W", DCH_W, true, fromCharDateGregorian},
	{"Y,YYY", DCH_Y_YYY, true, fromCharDateGregorian}, /* Y */
	{"YYYY", DCH_YYYY, true, fromCharDateGregorian},
	{"YYY", DCH_YYY, true, fromCharDateGregorian},
	{"YY", DCH_YY, true, fromCharDateGregorian},
	{"Y", DCH_Y, true, fromCharDateGregorian},
	{"a.d.", DCH_a_d, false, fromCharDateNone}, /* a */
	{"a.m.", DCH_a_m, false, fromCharDateNone},
	{"ad", DCH_ad, false, fromCharDateNone},
	{"am", DCH_am, false, fromCharDateNone},
	{"b.c.", DCH_b_c, false, fromCharDateNone}, /* b */
	{"bc", DCH_bc, false, fromCharDateNone},
	{"cc", DCH_CC, true, fromCharDateNone},    /* c */
	{"day", DCH_day, false, fromCharDateNone}, /* d */
	{"ddd", DCH_DDD, true, fromCharDateGregorian},
	{"dd", DCH_DD, true, fromCharDateGregorian},
	{"dy", DCH_dy, false, fromCharDateNone},
	{"d", DCH_D, true, fromCharDateGregorian},
	{"ff1", DCH_FF1, false, fromCharDateNone}, /* f */
	{"ff2", DCH_FF2, false, fromCharDateNone},
	{"ff3", DCH_FF3, false, fromCharDateNone},
	{"ff4", DCH_FF4, false, fromCharDateNone},
	{"ff5", DCH_FF5, false, fromCharDateNone},
	{"ff6", DCH_FF6, false, fromCharDateNone},
	{"fx", DCH_FX, false, fromCharDateNone},
	{"hh24", DCH_HH24, true, fromCharDateNone}, /* h */
	{"hh12", DCH_HH12, true, fromCharDateNone},
	{"hh", DCH_HH, true, fromCharDateNone},
	{"iddd", DCH_IDDD, true, fromCharDateISOWeek}, /* i */
	{"id", DCH_ID, true, fromCharDateISOWeek},
	{"iw", DCH_IW, true, fromCharDateISOWeek},
	{"iyyy", DCH_IYYY, true, fromCharDateISOWeek},
	{"iyy", DCH_IYY, true, fromCharDateISOWeek},
	{"iy", DCH_IY, true, fromCharDateISOWeek},
	{"i", DCH_I, true, fromCharDateISOWeek},
	{"j", DCH_J, true, fromCharDateNone},   /* j */
	{"mi", DCH_MI, true, fromCharDateNone}, /* m */
	{"mm", DCH_MM, true, fromCharDateGregorian},
	{"month", DCH_month, false, fromCharDateGregorian},
	{"mon", DCH_mon, false, fromCharDateGregorian},
	{"ms", DCH_MS, true, fromCharDateNone},
	{"of", DCH_OF, false, fromCharDateNone},    /* o */
	{"p.m.", DCH_p_m, false, fromCharDateNone}, /* p */
	{"pm", DCH_pm, false, fromCharDateNone},
	{"q", DCH_Q, true, fromCharDateNone},         /* q */
	{"rm", DCH_rm, false, fromCharDateGregorian}, /* r */
	{"sssss", DCH_SSSS, true, fromCharDateNone},  /* s */
	{"ssss", DCH_SSSS, true, fromCharDateNone},
	{"ss", DCH_SS, true, fromCharDateNone},
	{"tzh", DCH_TZH, false, fromCharDateNone}, /* t */
	{"tzm", DCH_TZM, true, fromCharDateNone},
	{"tz", DCH_tz, false, fromCharDateNone},
	{"us", DCH_US, true, fromCharDateNone},      /* u */
	{"ww", DCH_WW, true, fromCharDateGregorian}, /* w */
	{"w", DCH_W, true, fromCharDateGregorian},
	{"y,yyy", DCH_Y_YYY, true, fromCharDateGregorian}, /* y */
	{"yyyy", DCH_YYYY, true, fromCharDateGregorian},
	{"yyy", DCH_YYY, true, fromCharDateGregorian},
	{"yy", DCH_YY, true, fromCharDateGregorian},
	{"y", DCH_Y, true, fromCharDateGregorian},

	/* last */
	//{"", 0, false, 0},
}

// Months in roman-numeral
// (Must be in reverse order for seq_search (in FROM_CHAR), because
// 'VIII' must have higher precedence than 'V')
var (
	ucMonthRomanNumerals = []string{"XII", "XI", "X", "IX", "VIII", "VII", "VI", "V", "IV", "III", "II", "I"}
	lcMonthRomanNumerals = []string{"xii", "xi", "x", "ix", "viii", "vii", "vi", "v", "iv", "iii", "ii", "i"}
)

// From https://github.com/postgres/postgres/blob/b0b72c64a0ce7bf5dd78a80b33d85c89c943ad0d/src/backend/utils/adt/formatting.c#L194-L198.
type formatNodeType int

const (
	formatNodeEnd formatNodeType = iota
	formatNodeAction
	formatNodeChar
	formatNodeSeparator
	formatNodeSpace
)

type formatNode struct {
	typ       formatNodeType
	character string
	suffix    dchSuffix
	key       *keyWord
}

// dchIndex maps ASCII characters to the start index in dchKeywords of possible
// matches.
var dchIndex = []dchPos{
	/*---- first 0..31 chars are skipped ----*/

	-1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, -1, -1, -1, -1, -1,
	-1, -1, -1, -1, -1, DCH_A_D, DCH_B_C, DCH_CC, DCH_DAY, -1,
	DCH_FF1, -1, DCH_HH24, DCH_IDDD, DCH_J, -1, -1, DCH_MI, -1, DCH_OF,
	DCH_P_M, DCH_Q, DCH_RM, DCH_SSSSS, DCH_TZH, DCH_US, -1, DCH_WW, -1, DCH_Y_YYY,
	-1, -1, -1, -1, -1, -1, -1, DCH_a_d, DCH_b_c, DCH_cc,
	DCH_day, -1, DCH_ff1, -1, DCH_hh24, DCH_iddd, DCH_j, -1, -1, DCH_mi,
	-1, DCH_of, DCH_p_m, DCH_q, DCH_rm, DCH_sssss, DCH_tzh, DCH_us, -1, DCH_ww,
	-1, DCH_y_yyy, -1, -1, -1, -1,

	/*---- chars over 126 are skipped ----*/
}

// From https://github.com/postgres/postgres/blob/master/src/backend/utils/adt/formatting.c#L155.
type keySuffixType int

const (
	keySuffixPrefix keySuffixType = iota
	keySuffixPostfix
)

type dchSuffix int

// This blocks contains all dch_suffix types used in PG.
const (
	DCH_S_FM dchSuffix = 0x01
	DCH_S_TH dchSuffix = 0x02
	DCH_S_th dchSuffix = 0x04
	DCH_S_SP dchSuffix = 0x08
	DCH_S_TM dchSuffix = 0x10
)

func (d dchSuffix) FM() bool {
	return d&DCH_S_FM > 0
}

func (d dchSuffix) SP() bool {
	return d&DCH_S_SP > 0
}

func (d dchSuffix) TM() bool {
	return d&DCH_S_TM > 0
}

func (d dchSuffix) THth() bool {
	return d.TH() || d.th()
}

func (d dchSuffix) TH() bool {
	return d&DCH_S_TH > 0
}

func (d dchSuffix) th() bool {
	return d&DCH_S_th > 0
}

type keySuffix struct {
	name string
	id   dchSuffix
	typ  keySuffixType
}

var dchSuffixes = []keySuffix{
	{"FM", DCH_S_FM, keySuffixPrefix},
	{"fm", DCH_S_FM, keySuffixPrefix},
	{"TM", DCH_S_TM, keySuffixPrefix},
	{"tm", DCH_S_TM, keySuffixPrefix},
	{"TH", DCH_S_TH, keySuffixPostfix},
	{"th", DCH_S_th, keySuffixPostfix},
	{"SP", DCH_S_SP, keySuffixPostfix},
	/* last */
	//{"", 0, -1},
}
