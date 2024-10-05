// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package clisqlshell

import (
	"strings"

	"github.com/cockroachdb/errors"
)

// scanLocalCmdArgs scans the line for a postgres-like meta-command.
// It supports single-quoting and double-quoting in a way similar
// to psql.
func scanLocalCmdArgs(line string) (res []string, err error) {
	var arg strings.Builder
	// quote is the last observed quote character.
	var quote byte
	for i := 0; i < len(line); i++ {
		if quote == '\'' {
			// Inside a single-quoted string, we perform certain standard SQL substitutions:
			// - double single quote -> single quote
			// - \n, \t, etc -> special characters
			switch line[i] {
			case '\'':
				if i+1 < len(line) && line[i+1] == '\'' {
					// Double single quote.
					i++
					arg.WriteByte('\'')
					continue
				}
				// End of single-quoted string.
				quote = 0
				continue

			case '\\':
				// Escape character.
				if i+1 >= len(line) {
					return nil, errors.New("unterminated escape sequence")
				}
				i++
				switch line[i] {
				case 'n':
					arg.WriteByte('\n')
				case 't':
					arg.WriteByte('\t')
				case 'b':
					arg.WriteByte('\b')
				case 'r':
					arg.WriteByte('\r')
				case 'f':
					arg.WriteByte('\f')
				case '0', '1', '2', '3', '4', '5', '6', '7':
					// Octal escape. Like in psql, we support escapes of up to 3 octal digits.
					oct := (line[i] - '0')
					i++
					if i < len(line) && line[i] >= '0' && line[i] <= '7' {
						oct = oct*8 + (line[i] - '0')
						i++
						if i < len(line) && line[i] >= '0' && line[i] <= '7' {
							oct = oct*8 + (line[i] - '0')
							i++
						}
					}
					arg.WriteByte(oct)
					i--

				case 'x':
					// Hex escape.
					i++
					if i < len(line) && isHex(line[i]) {
						hex := fromHex(line[i])
						i++
						if i < len(line) && isHex(line[i]) {
							hex = hex*16 + fromHex(line[i])
							i++
						}
						arg.WriteByte(hex)
						i--
					} else {
						// Invalid hex escape. Just treat it as a literal 'x'.
						arg.WriteByte('x')
					}

				default:
					// Any other character is preserved as-is.
					arg.WriteByte(line[i])
				}
				continue

			default:
				// Any other character is preserved as-is.
				arg.WriteByte(line[i])
				continue
			}
		} else if quote == '"' {
			switch line[i] {
			case '"':
				// Double double quote: insert a double quote in the output.
				// Otherwise, finish the quoted string.
				if i+1 < len(line) && line[i+1] == '"' {
					i++
					arg.WriteByte('"')
					continue
				}
				// If a string is double-quoted, the final quote is also
				// included in the result.
				arg.WriteByte('"')
				quote = 0
				continue

			default:
				// Any other character is preserved as-is.
				arg.WriteByte(line[i])
				continue
			}
		} else {
			// Unquoted.
			switch line[i] {
			case '\'':
				quote = '\''
				continue

			case '"':
				// If a string is double-quoted, the quote is also included in the result.
				arg.WriteByte('"')
				quote = '"'
				continue

			case ' ', '\t':
				// Whitespace. If we're not in a quoted string, finish the argument.
				if arg.Len() > 0 {
					res = append(res, arg.String())
					arg.Reset()
				}

			case '\\':
				if arg.Len() == 0 && len(res) == 0 {
					// Special case: a backslash at the beginning of the line
					// (optionally after whitespace). This is allowed.
					arg.WriteByte('\\')
					continue
				}
				// In psql, a naked '\' in the input indicates the end of the
				// previous client-side command and the start of a new one. We
				// do not yet support this behavior.
				return nil, errors.WithHint(errors.New("invalid command delimiter in input"),
					"To include a literal backslash, use '\\\\' inside a quoted string.")

			default:
				// Any other character is preserved as-is.
				arg.WriteByte(line[i])
			}
		}
	}

	if quote != 0 {
		return nil, errors.New("unterminated quoted string")
	}
	if arg.Len() > 0 {
		res = append(res, arg.String())
	}
	return res, nil
}

// isHex returns true iff d is a valid hex digit.
func isHex(d byte) bool {
	return (d >= '0' && d <= '9') || (d >= 'a' && d <= 'f') || (d >= 'A' && d <= 'F')
}

// fromHex returns the value of a hex digit.
func fromHex(d byte) byte {
	switch {
	case d >= '0' && d <= '9':
		return d - '0'
	case d >= 'a' && d <= 'f':
		return d - 'a' + 10
	case d >= 'A' && d <= 'F':
		return d - 'A' + 10
	default:
		// Unreachable.
		panic("invalid hex digit")
	}
}
