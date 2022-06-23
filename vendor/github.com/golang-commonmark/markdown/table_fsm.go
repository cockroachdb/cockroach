// Copyright 2015 The Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package markdown

func isHeaderLine(s string) bool {
	if s == "" {
		return false
	}

	st := 0
	n := 0
	for i := 0; i < len(s); i++ {
		b := s[i]
		switch st {
		case 0: // initial state
			switch b {
			case '|':
				st = 1
			case ':':
				st = 2
			case '-':
				st = 3
				n++
			case ' ':
				break
			default:
				return false
			}

		case 1: // |
			switch b {
			case ' ':
				break
			case ':':
				st = 2
			case '-':
				st = 3
				n++
			default:
				return false
			}

		case 2: // |:
			switch b {
			case ' ':
				break
			case '-':
				st = 3
				n++
			default:
				return false
			}

		case 3: // |:-
			switch b {
			case '-':
				break
			case ':':
				st = 4
			case '|':
				st = 5
			case ' ':
				st = 6
			default:
				return false
			}

		case 4: // |:---:
			switch b {
			case ' ':
				break
			case '|':
				st = 5
			default:
				return false
			}

		case 5: // |:---:|
			switch b {
			case ' ':
				break
			case ':':
				st = 2
			case '-':
				st = 3
				n++
			default:
				return false
			}

		case 6: // |:--- SPACE
			switch b {
			case ' ':
				break
			case ':':
				st = 4
			case '|':
				st = 5
			default:
				return false
			}
		}
	}

	return n >= 1
}
