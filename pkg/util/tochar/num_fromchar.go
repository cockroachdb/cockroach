// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tochar

import (
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/apd/v3"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgcode"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
)

// CharToDecimal parses a formatted string into a decimal using the given
// format string. This is the main entry point for to_number(text, text).
func CharToDecimal(s string, c *FormatCache, format string) (*apd.Decimal, error) {
	nodes, desc, err := c.lookupNum(format)
	if err != nil {
		return nil, err
	}

	if len(format) == 0 {
		return nil, nil
	}

	return numFromChar(s, nodes, &desc)
}

// numFromChar implements the core parsing logic.
// Matches PostgreSQL's NUM_processor in the from_char direction.
func numFromChar(input string, nodes []numFormatNode, desc *numDesc) (*apd.Decimal, error) {
	if desc.isEEEE() {
		return nil, pgerror.New(
			pgcode.FeatureNotSupported,
			`"EEEE" not supported for input`,
		)
	}
	if desc.isRoman() {
		return nil, pgerror.New(
			pgcode.FeatureNotSupported,
			`"RN" not supported for input`,
		)
	}

	// Make a copy of desc since we modify it during processing.
	descCopy := *desc
	desc = &descCopy

	if desc.zeroStart > 0 {
		desc.zeroStart--
	}

	// Build the number string by walking format nodes and input in parallel.
	// The sign is tracked separately to avoid rewriting the builder.
	inIdx := 0
	sign := byte(' ') // ' ' means no sign seen yet
	var numBuilder strings.Builder

	readDec := false
	readPost := 0
	readPre := 0

	for _, n := range nodes {
		if n.typ == formatNodeEnd {
			break
		}

		if inIdx >= len(input) {
			break
		}

		if n.typ != formatNodeAction {
			// Non-pattern character: skip one input character.
			if inIdx < len(input) {
				_, size := utf8.DecodeRuneInString(input[inIdx:])
				inIdx += size
			}
			continue
		}

		switch n.key.id {
		case NUM_9, NUM_0, NUM_DEC, NUM_D, NUM_d:
			if inIdx >= len(input) {
				break
			}

			// Skip leading spaces.
			if inIdx < len(input) && input[inIdx] == ' ' {
				inIdx++
			}
			if inIdx >= len(input) {
				break
			}

			// Read sign before first digit.
			if sign == ' ' &&
				(n.key.id == NUM_0 || n.key.id == NUM_9) &&
				readPre+readPost == 0 {
				if desc.isLSign() && desc.lsign == numLSignPre {
					if inIdx < len(input) && input[inIdx] == '-' {
						sign = '-'
						inIdx++
					} else if inIdx < len(input) && input[inIdx] == '+' {
						sign = '+'
						inIdx++
					}
				} else {
					if inIdx < len(input) && (input[inIdx] == '-' ||
						(desc.isBracket() && input[inIdx] == '<')) {
						sign = '-'
						inIdx++
					} else if inIdx < len(input) && input[inIdx] == '+' {
						sign = '+'
						inIdx++
					}
				}
			}

			if inIdx >= len(input) {
				break
			}

			// Read digit or decimal point. PostgreSQL's
			// NUM_numpart_from_char does not advance inout_p here;
			// the caller's trailing Np->inout_p++ handles that.
			// We mirror this: read without advancing, then always
			// advance by 1 at the end of the case.
			digitConsumed := false
			if input[inIdx] >= '0' && input[inIdx] <= '9' {
				if readDec && readPost == desc.post {
					break
				}
				numBuilder.WriteByte(input[inIdx])
				digitConsumed = true
				if readDec {
					readPost++
				} else {
					readPre++
				}
			} else if desc.isDecimal() && !readDec {
				// Try to match decimal separator.
				if input[inIdx] == '.' {
					numBuilder.WriteByte('.')
					digitConsumed = true
					readDec = true
				}
			}

			// Read trailing sign. PostgreSQL's NUM_numpart_from_char reads
			// the sign after the last digit under two conditions:
			// 1. LSIGN (S format): when a digit was just read and the next
			//    char is not a digit. PG peeks ahead (inout_p+1), advances
			//    past the digit, and reads the sign.
			// 2. MI/PL/SG: when no digit was read in this call (the sign
			//    appears at the exhausted digit position). PG reads the sign
			//    at inout_p without advancing.
			// Both checks run BEFORE the caller's trailing inout_p++.
			if sign == ' ' && readPre+readPost > 0 {
				if desc.isLSign() && digitConsumed &&
					inIdx+1 < len(input) &&
					!(input[inIdx+1] >= '0' && input[inIdx+1] <= '9') {
					if input[inIdx+1] == '-' {
						sign = '-'
						inIdx++ // advance past digit to sign position
					} else if input[inIdx+1] == '+' {
						sign = '+'
						inIdx++ // advance past digit to sign position
					}
				} else if !digitConsumed && !desc.isLSign() &&
					(desc.isPlus() || desc.isMinus()) {
					if inIdx < len(input) &&
						(input[inIdx] == '-' || input[inIdx] == '+') {
						sign = input[inIdx]
						// Don't advance; the caller's ++ below handles it.
					}
				}
			}

			// Advance past the digit position character, matching
			// PostgreSQL's trailing Np->inout_p++ in NUM_processor.
			inIdx++

		case NUM_COMMA:
			// In PG's from_char direction, num_in is always false, so
			// the FM check is always entered.
			if desc.isFillMode() {
				continue
			}
			if inIdx < len(input) && input[inIdx] == ',' {
				inIdx++
			}

		case NUM_G, NUM_g:
			// Same as NUM_COMMA: num_in is always false in from_char.
			if desc.isFillMode() {
				continue
			}
			if inIdx < len(input) && input[inIdx] == ',' {
				inIdx++
			}

		case NUM_L, NUM_l:
			// Skip currency symbol character(s).
			if inIdx < len(input) && input[inIdx] == '$' {
				inIdx++
			} else {
				eatNonDataChars(input, &inIdx, 1)
			}

		case NUM_MI, NUM_mi:
			if inIdx < len(input) && input[inIdx] == '-' {
				sign = '-'
				inIdx++
			} else {
				eatNonDataChars(input, &inIdx, 1)
			}

		case NUM_PL, NUM_pl:
			if inIdx < len(input) && input[inIdx] == '+' {
				sign = '+'
				inIdx++
			} else {
				eatNonDataChars(input, &inIdx, 1)
			}

		case NUM_SG, NUM_sg:
			if inIdx < len(input) {
				if input[inIdx] == '-' {
					sign = '-'
					inIdx++
				} else if input[inIdx] == '+' {
					sign = '+'
					inIdx++
				} else {
					eatNonDataChars(input, &inIdx, 1)
				}
			}

		case NUM_PR, NUM_pr:
			// Skip closing '>' bracket.
			if inIdx < len(input) && input[inIdx] == '>' {
				inIdx++
			}

		case NUM_TH, NUM_th:
			// Skip ordinal suffix (2 chars).
			eatNonDataChars(input, &inIdx, 2)

		case NUM_RN, NUM_rn:
			// Not supported for input.
			continue

		default:
			continue
		}
	}

	// Build the final number string with sign prepended.
	result := numBuilder.String()
	if len(result) > 0 && result[len(result)-1] == '.' {
		result = result[:len(result)-1]
	}

	var numStr string
	if sign != ' ' {
		numStr = strings.TrimSpace(string(sign) + result)
	} else {
		numStr = strings.TrimSpace(result)
	}
	if numStr == "" || numStr == "-" || numStr == "+" {
		if readPre+readPost == 0 {
			// No actual digits were read from the input. This can happen when
			// the format has no digit positions, or when all digit positions
			// encountered non-digit characters. PostgreSQL's numeric_in errors
			// on the resulting empty/sign-only string.
			var pgNumStr string
			if sign != ' ' {
				pgNumStr = string(sign)
			} else {
				pgNumStr = " "
			}
			return nil, pgerror.Newf(
				pgcode.InvalidTextRepresentation,
				"invalid input syntax for type numeric: %q", pgNumStr,
			)
		}
		numStr = "0"
	}

	// Update post for precision.
	desc.post = readPost

	d, _, err := new(apd.Decimal).SetString(numStr)
	if err != nil {
		return nil, pgerror.Newf(
			pgcode.InvalidTextRepresentation,
			"invalid input syntax for type numeric: %q", numStr,
		)
	}

	// Apply V multiplier: divide by 10^multi. PostgreSQL computes
	// numeric_power(10, -multi) with NUMERIC_MIN_SIG_DIGITS (16) significant
	// digits, then multiplies; the result has 16+multi decimal places.
	if desc.isMulti() {
		divisor := new(apd.Decimal)
		divisor.SetFinite(1, int32(desc.multi))
		scale := int32(16 + desc.multi)
		ctx := apd.BaseContext.WithPrecision(uint32(scale) + 4)
		result := new(apd.Decimal)
		if _, err := ctx.Quo(result, d, divisor); err != nil {
			return nil, err
		}
		// Set exactly 16+multi decimal places to match PostgreSQL.
		if _, err := ctx.Quantize(result, result, -scale); err != nil {
			return nil, err
		}
		d = result
	}

	return d, nil
}

// eatNonDataChars skips n input characters, but only if they aren't
// numeric data characters (digits, '.', ',', '+', '-').
// Matches PostgreSQL's NUM_eat_non_data_chars.
func eatNonDataChars(input string, idx *int, n int) {
	for n > 0 && *idx < len(input) {
		ch := input[*idx]
		if ch >= '0' && ch <= '9' {
			break
		}
		if ch == '.' || ch == ',' || ch == '+' || ch == '-' {
			break
		}
		_, size := utf8.DecodeRuneInString(input[*idx:])
		*idx += size
		n--
	}
}
