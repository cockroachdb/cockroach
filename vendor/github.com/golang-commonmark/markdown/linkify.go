// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

import (
	"strings"

	"github.com/golang-commonmark/linkify"
)

func isLinkOpen(s string) bool { return isLetter(s[1]) }

func isLinkClose(s string) bool { return s[1] == '/' }

func ruleLinkify(s *StateCore) {
	blockTokens := s.Tokens

	if !s.Md.Linkify {
		return
	}

	for _, tok := range blockTokens {
		if tok, ok := tok.(*Inline); ok {
			tokens := tok.Children

			htmlLinkLevel := 0

			for i := len(tokens) - 1; i >= 0; i-- {
				currentTok := tokens[i]

				if _, ok := currentTok.(*LinkClose); ok {
					i--
					for tokens[i].Level() != currentTok.Level() {
						if _, ok := tokens[i].(*LinkOpen); ok {
							break
						}
						i--
					}
					continue
				}

				if currentTok, ok := currentTok.(*HTMLInline); ok {
					if isLinkOpen(currentTok.Content) && htmlLinkLevel > 0 {
						htmlLinkLevel--
					}
					if isLinkClose(currentTok.Content) {
						htmlLinkLevel++
					}
				}
				if htmlLinkLevel > 0 {
					continue
				}

				if currentTok, ok := currentTok.(*Text); ok {
					text := currentTok.Content
					links := linkify.Links(text)
					if len(links) == 0 {
						continue
					}

					var nodes []Token
					level := currentTok.Lvl
					lastPos := 0

					for _, ln := range links {
						urlText := text[ln.Start:ln.End]
						url := urlText
						if ln.Scheme == "" {
							url = "http://" + url
						} else if ln.Scheme == "mailto:" && !strings.HasPrefix(url, "mailto:") {
							url = "mailto:" + url
						}
						url = normalizeLink(url)
						if !validateLink(url) {
							continue
						}

						if ln.Scheme == "" {
							urlText = strings.TrimPrefix(normalizeLinkText("http://"+urlText), "http://")
						} else if ln.Scheme == "mailto:" && !strings.HasPrefix(urlText, "mailto:") {
							urlText = strings.TrimPrefix(normalizeLinkText("mailto:"+urlText), "mailto:")
						} else {
							urlText = normalizeLinkText(urlText)
						}

						pos := ln.Start

						if pos > lastPos {
							tok := Text{
								Content: text[lastPos:pos],
								Lvl:     level,
							}
							nodes = append(nodes, &tok)
						}

						nodes = append(nodes, &LinkOpen{
							Href: url,
							Lvl:  level,
						})
						nodes = append(nodes, &Text{
							Content: urlText,
							Lvl:     level + 1,
						})
						nodes = append(nodes, &LinkClose{
							Lvl: level,
						})

						lastPos = ln.End
					}

					if lastPos < len(text) {
						tok := Text{
							Content: text[lastPos:],
							Lvl:     level,
						}
						nodes = append(nodes, &tok)
					}

					children := make([]Token, len(tokens)+len(nodes)-1)
					copy(children, tokens[:i])
					copy(children[i:], nodes)
					copy(children[i+len(nodes):], tokens[i+1:])
					tok.Children = children
					tokens = children
				}
			}
		}
	}
}
