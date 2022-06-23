// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package linkify provides a way to find links in plain text.
package linkify

import (
	"strings"
	"unicode/utf8"
)

// Link represents a link found in a string with a schema and a position in the string.
type Link struct {
	Scheme     string
	Start, End int
}

func max(a, b int) int {
	if a >= b {
		return a
	}
	return b
}

// Links returns links found in s.
func Links(s string) (links []Link) {
	for i := 0; i < len(s)-2; i++ {
		switch s[i] {
		case '.': // IP address or domain name
			if i == 0 {
				continue // . at the start of a line
			}
			if length := match(s[i+1:]); length > 0 {
				pos := i + 1 + length
				switch s[pos-1] {
				case '.': // IP address
					if pos >= len(s) {
						continue // . at the end of line
					}
					if !digit(s[i-1]) {
						i = pos
						continue // . should be preceded by a digit
					}
					if !digit(s[pos]) {
						i = pos
						continue // . should be followed by a digit
					}

					// find the start of the IP address
					j := i - 2
					m := max(0, j-3)
					for j >= m && digit(s[j]) {
						j--
					}
					if i-2-j > 2 {
						i = pos + 1
						continue // at most 3 digits
					}
					start := 0
					if j >= 0 {
						r, rlen := utf8.DecodeLastRuneInString(s[:j+1])
						if !isPunctOrSpaceOrControl(r) {
							i = pos + 1
							continue
						}
						switch r {
						case '.', ':', '/', '\\', '-', '_':
							i = pos + 1
							continue
						}
						start = j + 2 - rlen
					}

					length, ok := skipIPv4(s[start:])
					if !ok {
						i = pos + 1
						continue
					}
					end := start + length
					if end == len(s) {
						links = append(links, Link{
							Scheme: "",
							Start:  start,
							End:    end,
						})
						return
					}

					r, _ := utf8.DecodeRuneInString(s[end:])
					if !isPunctOrSpaceOrControl(r) {
						continue
					}

					end = skipPort(s, end)
					end = skipPath(s, end)
					end = skipQuery(s, end)
					end = skipFragment(s, end)
					end = unskipPunct(s, end)

					if end < len(s) {
						r, _ = utf8.DecodeRuneInString(s[end:])
						if !isPunctOrSpaceOrControl(r) || r == '%' {
							continue
						}
					}

					links = append(links, Link{
						Scheme: "",
						Start:  start,
						End:    end,
					})
					i = end

				default: // domain name
					r, _ := utf8.DecodeLastRuneInString(s[:i])
					if !isLetterOrDigit(r) {
						continue // should be preceded by a letter or a digit
					}

					if pos == len(s) {
						start, ok := findHostnameStart(s, i)
						if !ok {
							continue
						}
						links = append(links, Link{
							Scheme: "",
							Start:  start,
							End:    pos,
						})
						return
					}

					if s[i+1:pos] != "xn--" {
						r, _ = utf8.DecodeRuneInString(s[pos:])
						if isLetterOrDigit(r) {
							continue // should not be followed by a letter or a digit
						}
					}

					end, dot, ok := findHostnameEnd(s, pos)
					if !ok {
						continue
					}
					dot = max(dot, i)

					if !(dot+5 <= len(s) && s[dot+1:dot+5] == "xn--") {
						if length := match(s[dot+1:]); dot+length+1 != end {
							continue
						}
					}

					start, ok := findHostnameStart(s, i)
					if !ok {
						continue
					}

					end = skipPort(s, end)
					end = skipPath(s, end)
					end = skipQuery(s, end)
					end = skipFragment(s, end)
					end = unskipPunct(s, end)

					if end < len(s) {
						r, _ = utf8.DecodeRuneInString(s[end:])
						if !isPunctOrSpaceOrControl(r) || r == '%' {
							continue // should be followed by punctuation or space
						}
					}

					links = append(links, Link{
						Scheme: "",
						Start:  start,
						End:    end,
					})
					i = end
				}
			}

		case '/': // schema-less link
			if s[i+1] != '/' {
				continue
			}

			if i > 0 {
				if s[i-1] == ':' {
					i++
					continue // should not be preceded by a colon
				}
				r, _ := utf8.DecodeLastRuneInString(s[:i])
				if !isPunctOrSpaceOrControl(r) {
					i++
					continue // should be preceded by punctuation or space
				}
			}

			r, _ := utf8.DecodeRuneInString(s[i+2:])
			if !isLetterOrDigit(r) {
				i++
				continue // should be followed by a letter or a digit
			}

			start := i
			end, dot, ok := findHostnameEnd(s, i+2)
			if !ok {
				continue
			}
			if s[i+2:end] != "localhost" {
				if dot == -1 {
					continue // no dot
				}
				if length, ok := skipIPv4(s[i+2:]); !ok || i+2+length != end {
					if length := match(s[dot+1:]); dot+length+1 != end {
						continue
					}
				}
			}

			end = skipPort(s, end)
			end = skipPath(s, end)
			end = skipQuery(s, end)
			end = skipFragment(s, end)
			end = unskipPunct(s, end)

			if end < len(s) {
				r, _ = utf8.DecodeRuneInString(s[end:])
				if !isPunctOrSpaceOrControl(r) || r == '%' {
					continue // should be followed by punctuation or space
				}
			}

			links = append(links, Link{
				Scheme: "//",
				Start:  start,
				End:    end,
			})
			i = end

		case ':': // http, https, ftp, mailto or localhost
			if i < 3 { // at least ftp:
				continue
			}

			if i >= 9 && s[i-1] == 't' && s[i-9:i] == "localhost" {
				j := i - 9
				if !digit(s[j+10]) {
					continue
				}
				if j > 0 {
					r, _ := utf8.DecodeLastRuneInString(s[:j])
					if !isPunctOrSpaceOrControl(r) {
						i++
						continue // should be preceded by punctuation or space
					}
				}

				start := j
				pos := j + 9
				end := skipPort(s, pos)
				if end == pos {
					continue // invalid port
				}
				end = skipPath(s, end)
				end = skipQuery(s, end)
				end = skipFragment(s, end)
				end = unskipPunct(s, end)

				if end < len(s) {
					r, _ := utf8.DecodeRuneInString(s[end:])
					if !isPunctOrSpaceOrControl(r) || r == '%' {
						i++
						continue // should be followed by punctuation or space
					}
				}

				links = append(links, Link{
					Scheme: "",
					Start:  start,
					End:    end,
				})
				i = end

				break
			}

			j := i - 1
			var start int
			var schema string

			switch byteToLower(s[j]) {
			case 'o': // mailto
				if j < 5 {
					continue // too short for mailto
				}
				if len(s)-j < 8 {
					continue // insufficient length after
				}
				if strings.ToLower(s[j-5:j+2]) != "mailto:" {
					continue
				}
				r, _ := utf8.DecodeLastRuneInString(s[:j-5])
				if isLetterOrDigit(r) {
					continue // should not be preceded by a letter or a digit
				}
				r, _ = utf8.DecodeRuneInString(s[j+2:])
				if !isAllowedInEmail(r) {
					continue // should be followed by a valid e-mail character
				}

				start = j - 5
				end, ok := findEmailEnd(s, j+2)
				if !ok {
					continue
				}

				links = append(links, Link{
					Scheme: "mailto:",
					Start:  start,
					End:    end,
				})
				i = end
				continue // continue processing

			case 'p': // http or ftp
				if len(s)-j < 8 {
					continue // insufficient length after
				}
				switch byteToLower(s[j-2]) {
				case 'f':
					if strings.ToLower(s[j-2:j+4]) != "ftp://" {
						continue
					}
					start = j - 2
					schema = "ftp:"
				case 't':
					if j < 3 {
						continue
					}
					if strings.ToLower(s[j-3:j+4]) != "http://" {
						continue
					}
					start = j - 3
					schema = "http:"
				default:
					continue
				}

			case 's': // https
				if j < 4 {
					continue // too short for https
				}
				if len(s)-j < 8 {
					continue // insufficient length after
				}
				start = j - 4
				if strings.ToLower(s[start:j+4]) != "https://" {
					continue
				}
				schema = "https:"

			default:
				continue
			}

			// http, https or ftp

			if start > 0 {
				r, _ := utf8.DecodeLastRuneInString(s[:start])
				if !isPunctOrSpaceOrControl(r) {
					continue // should be preceded by punctuation or space
				}
			}

			r, _ := utf8.DecodeRuneInString(s[j+4:])
			if !isLetterOrDigit(r) {
				continue // should be followed by a letter or a digit
			}

			end, dot, ok := findHostnameEnd(s, j+4)
			if !ok {
				continue
			}
			if s[j+4:end] != "localhost" {
				if dot == -1 {
					continue // no dot
				}
				if length, ok := skipIPv4(s[j+4:]); !ok || j+4+length != end {
					if !(dot+5 <= len(s) && s[dot+1:dot+5] == "xn--") {
						if length := match(s[dot+1:]); dot+length+1 != end {
							continue
						}
					}
				}
			}

			end = skipPort(s, end)
			end = skipPath(s, end)
			end = skipQuery(s, end)
			end = skipFragment(s, end)
			end = unskipPunct(s, end)

			if end < len(s) {
				r, _ = utf8.DecodeRuneInString(s[end:])
				if !isPunctOrSpaceOrControl(r) || r == '%' {
					continue // should be followed by punctuation or space
				}
			}

			links = append(links, Link{
				Scheme: schema,
				Start:  start,
				End:    end,
			})
			i = end

		case '@': // schema-less e-mail
			if i == 0 {
				continue // @ at the start of a line
			}

			if len(s)-i < 5 {
				continue // insufficient length after
			}

			r, _ := utf8.DecodeLastRuneInString(s[:i])
			if !isAllowedInEmail(r) {
				continue // should be preceded by a valid e-mail character
			}

			r, _ = utf8.DecodeRuneInString(s[i+1:])
			if !isLetterOrDigit(r) {
				continue // should be followed by a letter or a digit
			}

			start, ok := findEmailStart(s, i-1)
			if !ok {
				continue
			}

			end, dot, ok := findHostnameEnd(s, i+1)
			if !ok {
				continue
			}
			if dot == -1 {
				continue // no dot
			}
			if !(dot+5 <= len(s) && s[dot+1:dot+5] == "xn--") {
				if length := match(s[dot+1:]); dot+length+1 != end {
					continue
				}
			}

			links = append(links, Link{
				Scheme: "mailto:",
				Start:  start,
				End:    end,
			})
			i = end
		}
	}
	return
}
