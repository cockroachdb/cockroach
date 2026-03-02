// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package tochar

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/apd/v3"
)

// DecimalToChar formats a decimal number using the given format string.
// This is the main entry point for to_char(numeric, text),
// to_char(int, text), and to_char(float8, text).
func DecimalToChar(d *apd.Decimal, c *FormatCache, format string) (string, error) {
	nodes, desc, err := c.lookupNum(format)
	if err != nil {
		return "", err
	}

	if len(format) == 0 {
		return "", nil
	}

	return numToChar(d, nodes, &desc)
}

// numToChar implements the core formatting logic.
// Matches PostgreSQL's numeric_to_char flow.
func numToChar(d *apd.Decimal, nodes []numFormatNode, desc *numDesc) (string, error) {
	// Handle Roman numerals.
	if desc.isRoman() {
		return numToCharRoman(d, desc)
	}

	// Handle scientific notation (EEEE).
	if desc.isEEEE() {
		return numToCharEEEE(d, desc)
	}

	// Make a copy of desc since we modify it during processing.
	descCopy := *desc

	return numToCharNormal(d, nodes, &descCopy)
}

// numToCharRoman converts a decimal to a Roman numeral string.
func numToCharRoman(d *apd.Decimal, desc *numDesc) (string, error) {
	// Round to integer.
	var rounded apd.Decimal
	ctx := apd.BaseContext.WithPrecision(0)
	if _, err := ctx.RoundToIntegralValue(&rounded, d); err != nil {
		return "", err
	}
	val, err := rounded.Int64()
	if err != nil || val < 1 || val > 3999 {
		// Out-of-range or non-integer values produce a fill string, not an error.
		return strings.Repeat("#", 15), nil //nolint:returnerrcheck
	}
	roman := intToRoman(int(val))
	if desc.isFillMode() {
		return roman, nil
	}
	return fmt.Sprintf("%15s", roman), nil
}

// intToRoman converts an integer (1-3999) to a Roman numeral string.
// Matches PostgreSQL's int_to_roman.
func intToRoman(number int) string {
	if number < 1 || number > 3999 {
		return strings.Repeat("#", 15)
	}
	digits := strconv.Itoa(number)
	var sb strings.Builder
	for i, ch := range digits {
		num := int(ch - '1')
		if num < 0 {
			continue
		}
		digitsLeft := len(digits) - i
		if digitsLeft > 3 {
			for j := 0; j <= num; j++ {
				sb.WriteByte('M')
			}
		} else if digitsLeft == 3 {
			sb.WriteString(romanHundreds[num])
		} else if digitsLeft == 2 {
			sb.WriteString(romanTens[num])
		} else {
			sb.WriteString(romanOnes[num])
		}
	}
	return sb.String()
}

// numToCharEEEE formats a decimal in scientific notation.
func numToCharEEEE(d *apd.Decimal, desc *numDesc) (string, error) {
	// Handle special values. Matches PostgreSQL (formatting.c lines 6210-6222):
	// fill with '#', leading space, dot at position pre+1.
	switch d.Form {
	case apd.NaN, apd.NaNSignaling, apd.Infinite:
		numStr := fillString('#', desc.pre+desc.post+6)
		b := []byte(numStr)
		b[0] = ' '
		if desc.pre > 0 {
			b[desc.pre+1] = '.'
		}
		return string(b), nil
	}

	// Format using decimal arithmetic to avoid float64 precision loss.
	// This mirrors PostgreSQL's numeric_out_sci: compute the exponent, divide
	// by 10^exponent to get a significand in [1, 10), and round to desc.post
	// decimal places.
	sign := d.Sign()
	if sign == 0 {
		var sb strings.Builder
		sb.WriteByte(' ')
		sb.WriteByte('0')
		if desc.post > 0 {
			sb.WriteByte('.')
			sb.WriteString(strings.Repeat("0", desc.post))
		}
		sb.WriteString("e+00")
		return sb.String(), nil
	}

	abs := new(apd.Decimal).Abs(d)

	// Compute the exponent: floor(log10(abs)).
	coeffStr := abs.Coeff.String()
	exponent := len(coeffStr) - 1 + int(abs.Exponent)

	// Divide by 10^exponent to get the significand in [1, 10).
	divisor := new(apd.Decimal)
	divisor.SetFinite(1, int32(exponent))
	ctx := apd.BaseContext.WithPrecision(uint32(desc.post + 16))
	significand := new(apd.Decimal)
	if _, err := ctx.Quo(significand, abs, divisor); err != nil {
		return "", err
	}

	// Round the significand to desc.post decimal places.
	rounded := new(apd.Decimal)
	if _, err := ctx.Quantize(rounded, significand, -int32(desc.post)); err != nil {
		return "", err
	}

	sigText := rounded.Text('f')

	var sb strings.Builder
	if sign < 0 {
		sb.WriteByte('-')
	} else {
		sb.WriteByte(' ')
	}
	sb.WriteString(sigText)
	sb.WriteString(fmt.Sprintf("e%+03d", exponent))
	return sb.String(), nil
}

// numToCharNormal formats a decimal using the standard digit-by-digit processor.
// Matches PostgreSQL's NUM_processor in to_char direction.
func numToCharNormal(d *apd.Decimal, nodes []numFormatNode, desc *numDesc) (string, error) {
	// Handle NaN and Infinity before arithmetic, since operations like
	// Quantize return errors on infinite values. PostgreSQL lets these flow
	// through numeric_out → "NaN"/"Infinity" and then relies on the normal
	// numstr_pre_len > Num.pre overflow path.
	if d.Form == apd.NaN || d.Form == apd.NaNSignaling || d.Form == apd.Infinite {
		sign := byte('+')
		if d.Negative {
			sign = '-'
		}
		numStr := d.Text('f')
		if numStr[0] == '-' {
			numStr = numStr[1:]
		}
		numStrPreLen := len(numStr)
		if numStrPreLen <= desc.pre {
			// Enough room to display the literal text.
			outPreSpaces := desc.pre - numStrPreLen
			return numProcessorToChar(nodes, desc, numStr, sign, outPreSpaces)
		}
		// Overflow: fill with '#'.
		overflow := fillString('#', desc.pre+desc.post+1)
		if desc.post > 0 {
			overflow = overflow[:desc.pre] + "." + overflow[desc.pre:desc.pre+desc.post]
		}
		return numProcessorToChar(nodes, desc, overflow, sign, 0)
	}

	// Apply V multiplier if needed.
	val := new(apd.Decimal).Set(d)
	if desc.isMulti() {
		multiplier := new(apd.Decimal)
		multiplier.SetFinite(1, int32(desc.multi))
		ctx := apd.BaseContext.WithPrecision(50)
		if _, err := ctx.Mul(val, val, multiplier); err != nil {
			return "", err
		}
		desc.pre += desc.multi
	}

	// Round to desc.post decimal places.
	{
		ctx := apd.BaseContext.WithPrecision(50)
		rounded := new(apd.Decimal)
		if _, err := ctx.Quantize(rounded, val, -int32(desc.post)); err != nil {
			return "", err
		}
		val = rounded
	}

	// Convert to string, extract sign and digits.
	numStr := val.Text('f')
	sign := byte('+')
	if numStr[0] == '-' {
		sign = '-'
		numStr = numStr[1:]
	}

	// Split at decimal point.
	var intPart, fracPart string
	if dotIdx := strings.IndexByte(numStr, '.'); dotIdx >= 0 {
		intPart = numStr[:dotIdx]
		fracPart = numStr[dotIdx+1:]
	} else {
		intPart = numStr
	}

	// Pad or truncate fractional part to match desc.post.
	if desc.post > 0 {
		if len(fracPart) < desc.post {
			fracPart += strings.Repeat("0", desc.post-len(fracPart))
		} else if len(fracPart) > desc.post {
			fracPart = fracPart[:desc.post]
		}
		numStr = intPart + "." + fracPart
	} else {
		numStr = intPart
	}

	// Calculate leading spaces.
	numStrPreLen := len(intPart)
	outPreSpaces := 0
	if numStrPreLen < desc.pre {
		outPreSpaces = desc.pre - numStrPreLen
	} else if numStrPreLen > desc.pre {
		// Overflow: fill with '#'.
		overflow := fillString('#', desc.pre+desc.post+1)
		if desc.post > 0 {
			overflow = overflow[:desc.pre] + "." + overflow[desc.pre:desc.pre+desc.post]
		}
		numStr = overflow
	}

	return numProcessorToChar(nodes, desc, numStr, sign, outPreSpaces)
}

// numProcessorToChar walks format nodes and produces the output string.
// Matches PostgreSQL's NUM_processor in the to_char direction.
func numProcessorToChar(
	nodes []numFormatNode, desc *numDesc, number string, sign byte, outPreSpaces int,
) (string, error) {
	var sb strings.Builder

	// Determine sign-writing behavior.
	signWrote := false
	if desc.isPlus() || desc.isMinus() {
		// MI/PL/SG - write sign in the pattern position, not before number.
		if desc.isPlus() && !desc.isMinus() {
			signWrote = false
		} else {
			signWrote = true
		}
	} else {
		if sign != '-' {
			if desc.isFillMode() {
				desc.flag &^= numFlagBracket
			}
		}
		if sign == '+' && desc.isFillMode() && !desc.isLSign() {
			signWrote = true
		}

		if desc.lsign == numLSignPre && desc.pre == desc.preLsignNum {
			desc.lsign = numLSignPost
		}
	}

	// Determine last relevant decimal digit for FM mode.
	var lastRelevant int = -1 // index into number string, -1 means not set
	if desc.isFillMode() && desc.isDecimal() {
		lastRelevant = getLastRelevantDecNum(number)
		// If there are '0' specifiers, don't strip those digits.
		if lastRelevant >= 0 && desc.zeroEnd > outPreSpaces {
			lastZeroPos := len(number) - 1
			zeroEndPos := desc.zeroEnd - outPreSpaces
			if zeroEndPos < lastZeroPos {
				lastZeroPos = zeroEndPos
			}
			if lastRelevant < lastZeroPos {
				lastRelevant = lastZeroPos
			}
		}
	}

	// Number of digit positions (0/9) including decimal point.
	numCount := desc.pre + desc.post - 1
	if !signWrote && outPreSpaces == 0 {
		numCount++
	}

	// Walk through digits.
	numIn := false
	numCurr := 0
	numberIdx := 0

	if desc.zeroStart > 0 {
		desc.zeroStart--
	}

	for _, n := range nodes {
		if n.typ == formatNodeEnd {
			break
		}

		if n.typ != formatNodeAction {
			// Non-pattern character: copy to output.
			sb.WriteString(n.character)
			continue
		}

		switch n.key.id {
		case NUM_9, NUM_0, NUM_DEC, NUM_D, NUM_d:
			// Write sign before first digit when appropriate.
			if !signWrote &&
				(numCurr >= outPreSpaces || (desc.isZero() && desc.zeroStart == numCurr)) &&
				!(isPredecSpace(desc, number, numberIdx) && !(lastRelevant >= 0 && numberIdx < len(number) && number[numberIdx] == '.')) {
				sb.WriteString(writeSign(desc, sign))
				signWrote = true
			}

			// Write digit, blank, or zero.
			if numCurr < outPreSpaces && (desc.zeroStart > numCurr || !desc.isZero()) {
				// Before actual digits: write blank (or skip in FM mode).
				if !desc.isFillMode() {
					sb.WriteByte(' ')
				}
			} else if desc.isZero() && numCurr < outPreSpaces && desc.zeroStart <= numCurr {
				// Zero-filled position before actual digits.
				sb.WriteByte('0')
				numIn = true
			} else {
				// Write actual digit or decimal point.
				if numberIdx < len(number) && number[numberIdx] == '.' {
					// Decimal point.
					if lastRelevant < 0 || number[lastRelevant] != '.' {
						if n.key.id == NUM_D || n.key.id == NUM_d {
							sb.WriteByte('.')
						} else {
							sb.WriteByte('.')
						}
					} else if desc.isFillMode() && lastRelevant >= 0 && number[lastRelevant] == '.' {
						sb.WriteByte('.')
					}
				} else if lastRelevant >= 0 && numberIdx > lastRelevant && n.key.id != NUM_0 {
					// After last relevant digit in FM mode: skip.
				} else if isPredecSpace(desc, number, numberIdx) {
					// "0.1" formatted as "9.9" -> " .1"
					if !desc.isFillMode() {
						sb.WriteByte(' ')
					} else if lastRelevant >= 0 && numberIdx < len(number) && number[lastRelevant] == '.' {
						sb.WriteByte('0')
					}
				} else {
					// Write the actual digit.
					if numberIdx < len(number) {
						sb.WriteByte(number[numberIdx])
						numIn = true
					}
				}
				if numberIdx < len(number) {
					numberIdx++
				}
			}

			// After last digit, write closing bracket or post-sign.
			end := numCount + boolToInt(outPreSpaces > 0) + boolToInt(desc.isDecimal())
			if lastRelevant >= 0 && numberIdx-1 == lastRelevant {
				end = numCurr
			}
			if numCurr+1 == end {
				if signWrote && desc.isBracket() {
					if sign == '+' {
						sb.WriteByte(' ')
					} else {
						sb.WriteByte('>')
					}
				} else if desc.isLSign() && desc.lsign == numLSignPost {
					if sign == '-' {
						sb.WriteByte('-')
					} else {
						sb.WriteByte('+')
					}
				}
			}
			numCurr++

		case NUM_COMMA:
			if !numIn {
				if desc.isFillMode() {
					continue
				}
				sb.WriteByte(' ')
			} else {
				sb.WriteByte(',')
			}

		case NUM_G, NUM_g:
			if !numIn {
				if desc.isFillMode() {
					continue
				}
				sb.WriteByte(' ')
			} else {
				sb.WriteByte(',')
			}

		case NUM_L, NUM_l:
			sb.WriteByte('$')

		case NUM_RN:
			// Already handled at top level; this shouldn't be reached
			// for normal flow, but write the roman string if present.

		case NUM_rn:
			// Same as RN but lowercase.

		case NUM_TH:
			if desc.isRoman() || (len(number) > 0 && number[0] == '#') ||
				sign == '-' || desc.isDecimal() {
				continue
			}
			sb.WriteString(numGetTH(number, true))

		case NUM_th:
			if desc.isRoman() || (len(number) > 0 && number[0] == '#') ||
				sign == '-' || desc.isDecimal() {
				continue
			}
			sb.WriteString(numGetTH(number, false))

		case NUM_MI, NUM_mi:
			if sign == '-' {
				sb.WriteByte('-')
			} else if desc.isFillMode() {
				continue
			} else {
				sb.WriteByte(' ')
			}

		case NUM_PL, NUM_pl:
			if sign == '+' {
				sb.WriteByte('+')
			} else if desc.isFillMode() {
				continue
			} else {
				sb.WriteByte(' ')
			}

		case NUM_SG, NUM_sg:
			sb.WriteByte(sign)

		case NUM_SP, NUM_sp, NUM_C, NUM_c:
			// Not supported, skip.
			continue

		case NUM_S, NUM_s, NUM_B, NUM_b, NUM_FM, NUM_fm,
			NUM_V, NUM_v, NUM_E, NUM_e:
			// These are handled via flags, not during output walk.
			continue

		default:
			continue
		}
	}

	return sb.String(), nil
}

// writeSign returns the sign string to write before the first digit.
func writeSign(desc *numDesc, sign byte) string {
	if desc.isLSign() {
		if desc.lsign == numLSignPre {
			if sign == '-' {
				return "-"
			}
			return "+"
		}
		return ""
	} else if desc.isBracket() {
		if sign == '+' {
			return " "
		}
		return "<"
	} else if sign == '+' {
		if !desc.isFillMode() {
			return " "
		}
		return ""
	}
	return "-"
}

// isPredecSpace checks if the current position is a leading zero that should
// be shown as a space (for patterns like "9.9" formatting "0.1").
func isPredecSpace(desc *numDesc, number string, idx int) bool {
	return !desc.isZero() &&
		idx == 0 &&
		len(number) > 0 && number[0] == '0' &&
		desc.post > 0
}

// getLastRelevantDecNum returns the index of the last relevant digit after
// the decimal point (i.e., the last non-zero digit, or the decimal point
// itself if all trailing digits are zero). Returns -1 if no decimal point.
// Matches PostgreSQL's get_last_relevant_decnum.
func getLastRelevantDecNum(num string) int {
	dotIdx := strings.IndexByte(num, '.')
	if dotIdx < 0 {
		return -1
	}
	result := dotIdx
	for i := dotIdx + 1; i < len(num); i++ {
		if num[i] != '0' {
			result = i
		}
	}
	return result
}

// numGetTH returns the ordinal suffix (st, nd, rd, th) for a number string.
func numGetTH(number string, upper bool) string {
	if len(number) == 0 {
		if upper {
			return "TH"
		}
		return "th"
	}
	// Parse the integer part of the number.
	numStr := number
	if dotIdx := strings.IndexByte(numStr, '.'); dotIdx >= 0 {
		numStr = numStr[:dotIdx]
	}
	// Remove leading zeros/spaces.
	numStr = strings.TrimLeft(numStr, " 0")
	if len(numStr) == 0 {
		if upper {
			return "TH"
		}
		return "th"
	}
	v := int(numStr[len(numStr)-1] - '0')
	if v < 0 || v > 9 {
		if upper {
			return "TH"
		}
		return "th"
	}
	// Check the last two digits for 11, 12, 13.
	var lastTwo int
	if len(numStr) >= 2 {
		lastTwo = int(numStr[len(numStr)-2]-'0')*10 + v
	} else {
		lastTwo = v
	}
	return getTH(lastTwo, upper)
}

// fillString creates a string of length n filled with character c.
func fillString(c byte, n int) string {
	if n <= 0 {
		return ""
	}
	return strings.Repeat(string(c), n)
}

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}
