// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tochar

// numDesc describes the parsed structure of a numeric format string. It
// tracks digit counts, sign positioning, and various formatting flags.
// Matches PostgreSQL's NUMDesc struct.
// See https://github.com/postgres/postgres/blob/b0b72c64a0ce7bf5dd78a80b33d85c89c943ad0d/src/backend/utils/adt/formatting.c#L283.
type numDesc struct {
	pre         int // count of digits before decimal point
	post        int // count of digits after decimal point
	lsign       int // locale sign position: NUM_LSIGN_PRE, NUM_LSIGN_POST, or NUM_LSIGN_NONE
	flag        int // bitfield of NUM_F_* flags
	preLsignNum int // number of pre-decimal digits when lsign was set
	multi       int // multiplier count for 'V' pattern
	zeroStart   int // position of first '0' in format
	zeroEnd     int // position of last '0' in format
}

// Flags for numDesc.flag.
const (
	numFlagDecimal  = 1 << 1
	numFlagLDecimal = 1 << 2
	numFlagZero     = 1 << 3
	numFlagBlank    = 1 << 4
	numFlagFillMode = 1 << 5
	numFlagLSign    = 1 << 6
	numFlagBracket  = 1 << 7
	numFlagMinus    = 1 << 8
	numFlagPlus     = 1 << 9
	numFlagRoman    = 1 << 10
	numFlagMulti    = 1 << 11
	numFlagPlusPost = 1 << 12
	numFlagMinPost  = 1 << 13
	numFlagEEEE     = 1 << 14
)

// Locale sign position constants.
const (
	numLSignPre  = -1
	numLSignPost = 1
	numLSignNone = 0
)

func (n *numDesc) isDecimal() bool  { return n.flag&numFlagDecimal != 0 }
func (n *numDesc) isZero() bool     { return n.flag&numFlagZero != 0 }
func (n *numDesc) isBlank() bool    { return n.flag&numFlagBlank != 0 }
func (n *numDesc) isFillMode() bool { return n.flag&numFlagFillMode != 0 }
func (n *numDesc) isLSign() bool    { return n.flag&numFlagLSign != 0 }
func (n *numDesc) isBracket() bool  { return n.flag&numFlagBracket != 0 }
func (n *numDesc) isMinus() bool    { return n.flag&numFlagMinus != 0 }
func (n *numDesc) isPlus() bool     { return n.flag&numFlagPlus != 0 }
func (n *numDesc) isRoman() bool    { return n.flag&numFlagRoman != 0 }
func (n *numDesc) isMulti() bool    { return n.flag&numFlagMulti != 0 }
func (n *numDesc) isEEEE() bool     { return n.flag&numFlagEEEE != 0 }
