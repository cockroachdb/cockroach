// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tochar

import (
	"strings"

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
	inIdx := 0
	var numBuilder strings.Builder
	numBuilder.WriteByte(' ') // first char is reserved for sign

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
				inIdx += charLen(input[inIdx:])
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
			if numBuilder.Len() > 0 && numBuilder.String()[0] == ' ' &&
				(n.key.id == NUM_0 || n.key.id == NUM_9) &&
				readPre+readPost == 0 {
				if desc.isLSign() && desc.lsign == numLSignPre {
					if inIdx < len(input) && input[inIdx] == '-' {
						setBuilderSign(&numBuilder, '-')
						inIdx++
					} else if inIdx < len(input) && input[inIdx] == '+' {
						setBuilderSign(&numBuilder, '+')
						inIdx++
					}
				} else {
					if inIdx < len(input) && (input[inIdx] == '-' ||
						(desc.isBracket() && input[inIdx] == '<')) {
						setBuilderSign(&numBuilder, '-')
						inIdx++
					} else if inIdx < len(input) && input[inIdx] == '+' {
						setBuilderSign(&numBuilder, '+')
						inIdx++
					}
				}
			}

			if inIdx >= len(input) {
				break
			}

			// Read digit or decimal point.
			if input[inIdx] >= '0' && input[inIdx] <= '9' {
				if readDec && readPost == desc.post {
					break
				}
				numBuilder.WriteByte(input[inIdx])
				inIdx++
				if readDec {
					readPost++
				} else {
					readPre++
				}
			} else if desc.isDecimal() && !readDec {
				// Try to match decimal separator.
				if input[inIdx] == '.' {
					numBuilder.WriteByte('.')
					inIdx++
					readDec = true
				}
			}

			// Read trailing sign.
			if numBuilder.Len() > 0 && numBuilder.String()[0] == ' ' &&
				readPre+readPost > 0 {
				if !desc.isLSign() && (desc.isPlus() || desc.isMinus()) {
					if inIdx < len(input) && (input[inIdx] == '-' || input[inIdx] == '+') {
						setBuilderSign(&numBuilder, input[inIdx])
						inIdx++
					}
				} else if desc.isLSign() && desc.lsign == numLSignPost {
					if inIdx < len(input) && input[inIdx] == '-' {
						setBuilderSign(&numBuilder, '-')
						inIdx++
					} else if inIdx < len(input) && input[inIdx] == '+' {
						setBuilderSign(&numBuilder, '+')
						inIdx++
					}
				}
			}

		case NUM_COMMA:
			if !numIn_isActive(readPre, readPost) {
				if desc.isFillMode() {
					continue
				}
			}
			if inIdx < len(input) && input[inIdx] == ',' {
				inIdx++
			}

		case NUM_G, NUM_g:
			if !numIn_isActive(readPre, readPost) {
				if desc.isFillMode() {
					continue
				}
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
				setBuilderSign(&numBuilder, '-')
				inIdx++
			} else {
				eatNonDataChars(input, &inIdx, 1)
			}

		case NUM_PL, NUM_pl:
			if inIdx < len(input) && input[inIdx] == '+' {
				setBuilderSign(&numBuilder, '+')
				inIdx++
			} else {
				eatNonDataChars(input, &inIdx, 1)
			}

		case NUM_SG, NUM_sg:
			if inIdx < len(input) {
				if input[inIdx] == '-' {
					setBuilderSign(&numBuilder, '-')
					inIdx++
				} else if input[inIdx] == '+' {
					setBuilderSign(&numBuilder, '+')
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

	// Fix up the result: remove trailing '.' if present.
	result := numBuilder.String()
	if len(result) > 1 && result[len(result)-1] == '.' {
		result = result[:len(result)-1]
	}

	// The actual number starts after the sign byte at position 0.
	numStr := strings.TrimSpace(result)
	if numStr == "" {
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

	// Apply V multiplier: divide by 10^multi.
	if desc.isMulti() {
		divisor := new(apd.Decimal)
		divisor.SetFinite(1, int32(-desc.multi))
		ctx := apd.BaseContext.WithPrecision(50)
		result := new(apd.Decimal)
		if _, err := ctx.Quo(result, d, divisor); err != nil {
			return nil, err
		}
		d = result
	}

	return d, nil
}

// setBuilderSign sets the first byte of the builder (the sign position) to the
// given character. The builder must already have at least one byte written.
func setBuilderSign(sb *strings.Builder, sign byte) {
	b := []byte(sb.String())
	b[0] = sign
	sb.Reset()
	sb.Write(b)
}

// numIn_isActive returns true if we've already read some digits.
func numIn_isActive(readPre, readPost int) bool {
	return readPre+readPost > 0
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
		*idx += charLen(input[*idx:])
		n--
	}
}

// charLen returns the byte length of the first UTF-8 character in s.
func charLen(s string) int {
	if len(s) == 0 {
		return 0
	}
	if s[0] < 0x80 {
		return 1
	}
	// Multi-byte UTF-8.
	for i := 1; i < len(s) && i < 4; i++ {
		if s[i]&0xC0 != 0x80 {
			return i
		}
	}
	if len(s) < 4 {
		return len(s)
	}
	return 4
}
