// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tochar

import (
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// numIndexSeqSearch looks up a keyword in numKeywords using the numIndex
// for fast lookup. Matches PostgreSQL's index_seq_search for NUM keywords.
func numIndexSeqSearch(s string) *numKeyWord {
	if len(s) == 0 || s[0] <= ' ' || s[0] >= '~' {
		return nil
	}
	idxOffset := int(s[0] - ' ')
	if idxOffset >= 0 && idxOffset < len(numIndex) {
		pos := int(numIndex[idxOffset])
		for pos >= 0 && pos < len(numKeywords) {
			k := numKeywords[pos]
			if strings.HasPrefix(s, k.name) {
				return &numKeywords[pos]
			}
			pos++
			if pos >= len(numKeywords) || numKeywords[pos].name[0] != s[0] {
				break
			}
		}
	}
	return nil
}

// numDescPrepare validates and populates a numDesc based on a format node.
// Matches PostgreSQL's NUMDesc_prepare.
// See https://github.com/postgres/postgres/blob/b0b72c64a0ce7bf5dd78a80b33d85c89c943ad0d/src/backend/utils/adt/formatting.c#L1142.
func numDescPrepare(num *numDesc, n *numFormatNode) error {
	if n.typ != formatNodeAction {
		return nil
	}

	if num.isEEEE() && n.key.id != NUM_E {
		return pgerror.New(pgcode.Syntax, `"EEEE" must be the last pattern used`)
	}

	switch n.key.id {
	case NUM_9:
		if num.isBracket() {
			return pgerror.New(pgcode.Syntax, `"9" must be ahead of "PR"`)
		}
		if num.isMulti() {
			num.multi++
			return nil
		}
		if num.isDecimal() {
			num.post++
		} else {
			num.pre++
		}

	case NUM_0:
		if num.isBracket() {
			return pgerror.New(pgcode.Syntax, `"0" must be ahead of "PR"`)
		}
		if !num.isZero() && !num.isDecimal() {
			num.flag |= numFlagZero
			num.zeroStart = num.pre + 1
		}
		if !num.isDecimal() {
			num.pre++
		} else {
			num.post++
		}
		num.zeroEnd = num.pre + num.post

	case NUM_B, NUM_b:
		if num.pre == 0 && num.post == 0 && !num.isZero() {
			num.flag |= numFlagBlank
		}

	case NUM_D, NUM_d:
		num.flag |= numFlagLDecimal
		fallthrough
	case NUM_DEC:
		if num.isDecimal() {
			return pgerror.New(pgcode.Syntax, "multiple decimal points")
		}
		if num.isMulti() {
			return pgerror.New(
				pgcode.Syntax, `cannot use "V" and decimal point together`,
			)
		}
		num.flag |= numFlagDecimal

	case NUM_FM, NUM_fm:
		num.flag |= numFlagFillMode

	case NUM_S, NUM_s:
		if num.isLSign() {
			return pgerror.New(pgcode.Syntax, `cannot use "S" twice`)
		}
		if num.isPlus() || num.isMinus() || num.isBracket() {
			return pgerror.New(
				pgcode.Syntax,
				`cannot use "S" and "PL"/"MI"/"SG"/"PR" together`,
			)
		}
		if !num.isDecimal() {
			num.lsign = numLSignPre
			num.preLsignNum = num.pre
			num.flag |= numFlagLSign
		} else if num.lsign == numLSignNone {
			num.lsign = numLSignPost
			num.flag |= numFlagLSign
		}

	case NUM_MI, NUM_mi:
		if num.isLSign() {
			return pgerror.New(
				pgcode.Syntax, `cannot use "S" and "MI" together`,
			)
		}
		num.flag |= numFlagMinus
		if num.isDecimal() {
			num.flag |= numFlagMinPost
		}

	case NUM_PL, NUM_pl:
		if num.isLSign() {
			return pgerror.New(
				pgcode.Syntax, `cannot use "S" and "PL" together`,
			)
		}
		num.flag |= numFlagPlus
		if num.isDecimal() {
			num.flag |= numFlagPlusPost
		}

	case NUM_SG, NUM_sg:
		if num.isLSign() {
			return pgerror.New(
				pgcode.Syntax, `cannot use "S" and "SG" together`,
			)
		}
		num.flag |= numFlagMinus
		num.flag |= numFlagPlus

	case NUM_PR, NUM_pr:
		if num.isLSign() || num.isPlus() || num.isMinus() {
			return pgerror.New(
				pgcode.Syntax,
				`cannot use "PR" and "S"/"PL"/"MI"/"SG" together`,
			)
		}
		num.flag |= numFlagBracket

	case NUM_RN, NUM_rn:
		num.flag |= numFlagRoman

	case NUM_L, NUM_l, NUM_G, NUM_g:
		// Need locale (we use defaults).

	case NUM_V, NUM_v:
		if num.isDecimal() {
			return pgerror.New(
				pgcode.Syntax, `cannot use "V" and decimal point together`,
			)
		}
		num.flag |= numFlagMulti

	case NUM_E, NUM_e:
		if num.isEEEE() {
			return pgerror.New(pgcode.Syntax, `cannot use "EEEE" twice`)
		}
		if num.isBlank() || num.isFillMode() || num.isLSign() ||
			num.isBracket() || num.isMinus() || num.isPlus() ||
			num.isRoman() || num.isMulti() {
			return pgerror.Newf(
				pgcode.Syntax,
				`"EEEE" is incompatible with other formats`,
			)
		}
		num.flag |= numFlagEEEE
	}
	return nil
}

// parseNumFormat parses a numeric format string into a slice of
// numFormatNodes and a numDesc. Matches PostgreSQL's parse_format
// called with NUM_keywords.
func parseNumFormat(f string) ([]numFormatNode, numDesc, error) {
	var nodes []numFormatNode
	var desc numDesc

	for len(f) > 0 {
		kw := numIndexSeqSearch(f)
		if kw != nil {
			node := numFormatNode{
				typ: formatNodeAction,
				key: kw,
			}
			if err := numDescPrepare(&desc, &node); err != nil {
				return nil, numDesc{}, err
			}
			nodes = append(nodes, node)
			f = f[len(kw.name):]
		} else {
			if f[0] == '"' {
				// Process quoted text.
				f = f[1:]
				for len(f) > 0 {
					if f[0] == '"' {
						f = f[1:]
						break
					}
					if f[0] == '\\' {
						f = f[1:]
					}
					if len(f) == 0 {
						break
					}
					r, size := utf8.DecodeRuneInString(f)
					nodes = append(nodes, numFormatNode{
						typ:       formatNodeChar,
						character: string(r),
					})
					f = f[size:]
				}
			} else {
				// Outside double-quoted strings, backslash is only special
				// if it immediately precedes a double quote.
				if f[0] == '\\' && len(f) > 1 && f[1] == '"' {
					f = f[1:]
				}

				r, size := utf8.DecodeRuneInString(f)
				nodes = append(nodes, numFormatNode{
					typ:       formatNodeChar,
					character: string(r),
				})
				f = f[size:]
			}
		}
	}
	nodes = append(nodes, numFormatNode{typ: formatNodeEnd})
	return nodes, desc, nil
}
