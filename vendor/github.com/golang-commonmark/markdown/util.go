// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

import (
	"bytes"
	"regexp"
	"strings"
	"unicode"

	"github.com/golang-commonmark/html"
	"github.com/golang-commonmark/mdurl"
	"github.com/golang-commonmark/puny"
)

func runeIsSpace(r rune) bool {
	return r == ' ' || r == '\t'
}

func byteIsSpace(b byte) bool {
	return b == ' ' || b == '\t'
}

func isLetter(b byte) bool {
	return b >= 'a' && b <= 'z' || b >= 'A' && b <= 'Z'
}

func isUppercaseLetter(b byte) bool {
	return b >= 'A' && b <= 'Z'
}

func mdpunct(b byte) bool {
	return strings.IndexByte("!\"#$%&'()*+,-./:;<=>?@[\\]^_`{|}~", b) != -1
}

func isMdAsciiPunct(r rune) bool {
	if r > 0x7e {
		return false
	}
	return mdpunct(byte(r))
}

func recodeHostnameFor(proto string) bool {
	switch proto {
	case "http", "https", "mailto":
		return true
	}
	return false
}

func normalizeLink(url string) string {
	parsed, err := mdurl.Parse(url)
	if err != nil {
		return ""
	}

	if parsed.Host != "" && (parsed.Scheme == "" || recodeHostnameFor(parsed.Scheme)) {
		parsed.Host = puny.ToASCII(parsed.Host)
	}

	parsed.Scheme = parsed.RawScheme

	return mdurl.Encode(parsed.String())
}

func normalizeLinkText(url string) string {
	parsed, err := mdurl.Parse(url)
	if err != nil {
		return ""
	}

	if parsed.Host != "" && (parsed.Scheme == "" || recodeHostnameFor(parsed.Scheme)) {
		parsed.Host = puny.ToUnicode(parsed.Host)
	}

	parsed.Scheme = parsed.RawScheme

	return mdurl.Decode(parsed.String())
}

var badProtos = []string{"file", "javascript", "vbscript"}

var rGoodData = regexp.MustCompile(`^data:image/(gif|png|jpeg|webp);`)

func removeSpecial(s string) string {
	i := 0
	for i < len(s) && !(s[i] <= 0x20 || s[i] == 0x7f) {
		i++
	}
	if i >= len(s) {
		return s
	}
	buf := make([]byte, len(s))
	j := 0
	for i := 0; i < len(s); i++ {
		if !(s[i] <= 0x20 || s[i] == 0x7f) {
			buf[j] = s[i]
			j++
		}
	}
	return string(buf[:j])
}

func validateLink(url string) bool {
	str := strings.TrimSpace(url)
	str = strings.ToLower(str)

	if strings.IndexByte(str, ':') >= 0 {
		proto := strings.SplitN(str, ":", 2)[0]
		proto = removeSpecial(proto)
		for _, p := range badProtos {
			if proto == p {
				return false
			}
		}
		if proto == "data" && !rGoodData.MatchString(str) {
			return false
		}
	}

	return true
}

func unescapeAll(s string) string {
	anyChanges := false
	i := 0
	for i < len(s)-1 {
		b := s[i]
		if b == '\\' {
			if mdpunct(s[i+1]) {
				anyChanges = true
				break
			}
		} else if b == '&' {
			if e, n := html.ParseEntity(s[i:]); n > 0 && e != html.BadEntity {
				anyChanges = true
				break
			}
		}
		i++
	}

	if !anyChanges {
		return s
	}

	buf := make([]byte, len(s))
	copy(buf[:i], s)
	j := i
	for i < len(s) {
		b := s[i]
		if b == '\\' {
			if i+1 < len(s) {
				b = s[i+1]
				if mdpunct(b) {
					buf[j] = b
					j++
				} else {
					buf[j] = '\\'
					j++
					buf[j] = b
					j++
				}
				i += 2
				continue
			}
		} else if b == '&' {
			if e, n := html.ParseEntity(s[i:]); n > 0 && e != html.BadEntity {
				if len(e) > n && len(buf) == len(s) {
					newBuf := make([]byte, cap(buf)*2)
					copy(newBuf[:j], buf)
					buf = newBuf
				}
				j += copy(buf[j:], e)
				i += n
				continue
			}
		}
		buf[j] = b
		j++
		i++
	}

	return string(buf[:j])
}

func normalizeInlineCode(s string) string {
	if s == "" {
		return ""
	}

	byteFuckery := false
	i := 0
	for i < len(s)-1 {
		b := s[i]
		if b == '\n' {
			byteFuckery = true
			break
		}
		if b == ' ' {
			i++
			b = s[i]
			if b == ' ' || b == '\n' {
				i--
				byteFuckery = true
				break
			}
		}
		i++
	}

	if !byteFuckery {
		return strings.TrimSpace(s)
	}

	buf := make([]byte, len(s))
	copy(buf[:i], s)
	buf[i] = ' '
	i++
	j := i
	lastSpace := true
	for i < len(s) {
		b := s[i]
		switch b {
		case ' ', '\n':
			if lastSpace {
				break
			}

			buf[j] = ' '
			lastSpace = true
			j++
		default:
			buf[j] = b
			lastSpace = false
			j++
		}

		i++
	}

	return string(bytes.TrimSpace(buf[:j]))
}

func normalizeReference(s string) string {
	var buf bytes.Buffer
	lastSpace := false
	for _, r := range s {
		if unicode.IsSpace(r) {
			if !lastSpace {
				buf.WriteByte(' ')
				lastSpace = true
			}
			continue
		}

		buf.WriteRune(unicode.To(unicode.LowerCase, r))
		lastSpace = false
	}

	return string(bytes.TrimSpace(buf.Bytes()))
}
