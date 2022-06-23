// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

import (
	"regexp"
	"strings"
)

var (
	rAutolink = regexp.MustCompile(`^<([a-zA-Z][a-zA-Z0-9+.\-]{1,31}):([^<>\x00-\x20]*)>`)
	rEmail    = regexp.MustCompile(`^<([a-zA-Z0-9.!#$%&'*+/=?^_{|}~-]+@[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?(?:\.[a-zA-Z0-9](?:[a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?)*)>`)
)

func ruleAutolink(s *StateInline, silent bool) bool {
	pos := s.Pos
	src := s.Src

	if src[pos] != '<' {
		return false
	}

	tail := src[pos:]

	if strings.IndexByte(tail, '>') < 0 {
		return false
	}

	link := rAutolink.FindString(tail)
	if link != "" {
		link = link[1 : len(link)-1]
		href := normalizeLink(link)
		if !validateLink(href) {
			return false
		}

		if !silent {
			s.PushOpeningToken(&LinkOpen{Href: href})
			s.PushToken(&Text{Content: normalizeLinkText(link)})
			s.PushClosingToken(&LinkClose{})
		}

		s.Pos += len(link) + 2

		return true
	}

	email := rEmail.FindString(tail)
	if email != "" {
		email = email[1 : len(email)-1]
		href := normalizeLink("mailto:" + email)
		if !validateLink(href) {
			return false
		}

		if !silent {
			s.PushOpeningToken(&LinkOpen{Href: href})
			s.PushToken(&Text{Content: normalizeLinkText(email)})
			s.PushClosingToken(&LinkClose{})
		}

		s.Pos += len(email) + 2

		return true
	}

	return false
}
