// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

import (
	"strings"
	"unicode"
	"unicode/utf8"
)

type stackItem struct {
	token  int
	text   []rune
	pos    int
	single bool
	level  int
}

func nextQuoteIndex(s []rune, from int) int {
	for i := from; i < len(s); i++ {
		r := s[i]
		if r == '\'' || r == '"' {
			return i
		}
	}
	return -1
}

func firstRune(s string) rune {
	for _, r := range s {
		return r
	}
	return utf8.RuneError
}

func replaceQuotes(tokens []Token, s *StateCore) {
	var stack []stackItem
	var changed map[int][]rune

	for i, tok := range tokens {
		thisLevel := tok.Level()

		j := len(stack) - 1
		for j >= 0 {
			if stack[j].level <= thisLevel {
				break
			}
			j--
		}
		stack = stack[:j+1]

		tok, ok := tok.(*Text)
		if !ok || !strings.ContainsAny(tok.Content, `"'`) {
			continue
		}

		text := []rune(tok.Content)
		pos := 0
		max := len(text)

	loop:
		for pos < max {
			index := nextQuoteIndex(text, pos)
			if index < 0 {
				break
			}

			canOpen := true
			canClose := true
			pos = index + 1
			isSingle := text[index] == '\''

			lastChar := ' '
			if index-1 > 0 {
				lastChar = text[index-1]
			} else {
			loop1:
				for j := i - 1; j >= 0; j-- {
					switch tok := tokens[j].(type) {
					case *Softbreak:
						break loop1
					case *Hardbreak:
						break loop1
					case *Text:
						lastChar, _ = utf8.DecodeLastRuneInString(tok.Content)
						break loop1
					default:
						continue
					}
				}
			}

			nextChar := ' '
			if pos < max {
				nextChar = text[pos]
			} else {
			loop2:
				for j := i + 1; j < len(tokens); j++ {
					switch tok := tokens[j].(type) {
					case *Softbreak:
						break loop2
					case *Hardbreak:
						break loop2
					case *Text:
						nextChar, _ = utf8.DecodeRuneInString(tok.Content)
						break loop2
					default:
						continue
					}
				}
			}

			isLastPunct := isMdAsciiPunct(lastChar) || unicode.IsPunct(lastChar)
			isNextPunct := isMdAsciiPunct(nextChar) || unicode.IsPunct(nextChar)
			isLastWhiteSpace := unicode.IsSpace(lastChar)
			isNextWhiteSpace := unicode.IsSpace(nextChar)

			if isNextWhiteSpace {
				canOpen = false
			} else if isNextPunct {
				if !(isLastWhiteSpace || isLastPunct) {
					canOpen = false
				}
			}

			if isLastWhiteSpace {
				canClose = false
			} else if isLastPunct {
				if !(isNextWhiteSpace || isNextPunct) {
					canClose = false
				}
			}

			if nextChar == '"' && text[index] == '"' {
				if lastChar >= '0' && lastChar <= '9' {
					canClose = false
					canOpen = false
				}
			}

			if canOpen && canClose {
				canOpen = false
				canClose = isNextPunct
			}

			if !canOpen && !canClose {
				if isSingle {
					text[index] = '’'
					if changed == nil {
						changed = make(map[int][]rune)
					}
					if _, ok := changed[i]; !ok {
						changed[i] = text
					}
				}
				continue
			}

			if canClose {
				for j := len(stack) - 1; j >= 0; j-- {
					item := stack[j]
					if item.level < thisLevel {
						break
					}
					if item.single == isSingle && item.level == thisLevel {
						if changed == nil {
							changed = make(map[int][]rune)
						}

						var q1, q2 string
						if isSingle {
							q1 = s.Md.options.Quotes[2]
							q2 = s.Md.options.Quotes[3]
						} else {
							q1 = s.Md.options.Quotes[0]
							q2 = s.Md.options.Quotes[1]
						}

						if utf8.RuneCountInString(q1) == 1 && utf8.RuneCountInString(q2) == 1 {
							item.text[item.pos] = firstRune(q1)
							text[index] = firstRune(q2)
						} else if tok == tokens[item.token] {
							newText := make([]rune, 0, len(text)-2+len(q1)+len(q2))
							newText = append(newText, text[:item.pos]...)
							newText = append(newText, []rune(q1)...)
							newText = append(newText, text[item.pos+1:index]...)
							newText = append(newText, []rune(q2)...)
							newText = append(newText, text[index+1:]...)

							text = newText
							item.text = newText
						} else {
							newText := make([]rune, 0, len(item.text)-1+len(q1))
							newText = append(newText, item.text[:item.pos]...)
							newText = append(newText, []rune(q1)...)
							newText = append(newText, item.text[item.pos+1:]...)
							item.text = newText

							newText = make([]rune, 0, len(text)-1+len(q2))
							newText = append(newText, text[:index]...)
							newText = append(newText, []rune(q2)...)
							newText = append(newText, text[index+1:]...)

							text = newText
						}

						max = len(text)

						if _, ok := changed[i]; !ok {
							changed[i] = text
						}
						if ii := item.token; ii != i {
							if _, ok := changed[ii]; !ok {
								changed[ii] = item.text
							}
						}
						stack = stack[:j]
						continue loop
					}
				}
			}

			if canOpen {
				stack = append(stack, stackItem{
					token:  i,
					text:   text,
					pos:    index,
					single: isSingle,
					level:  thisLevel,
				})
			} else if canClose && isSingle {
				text[index] = '’'
				if changed == nil {
					changed = make(map[int][]rune)
				}
				if _, ok := changed[i]; !ok {
					changed[i] = text
				}
			}
		}
	}

	if changed != nil {
		for i, text := range changed {
			tokens[i].(*Text).Content = string(text)
		}
	}
}

func ruleSmartQuotes(s *StateCore) {
	if !s.Md.Typographer {
		return
	}

	tokens := s.Tokens
	for i := len(tokens) - 1; i >= 0; i-- {
		tok := tokens[i]
		if tok, ok := tok.(*Inline); ok {
			replaceQuotes(tok.Children, s)
		}
	}
}
