// Copyright 2018 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package pretty

// This file contains utility and other non-standard functions not listed
// in the paper (see the package documentation). Utility functions are those
// that are generally useful or need to create certain structures (like union)
// in a correct way.

// Join joins Docs d with string s and Line. There is no space between each
// item in d and the subsequent instance of s.
func Join(s string, d ...Doc) Doc {
	return JoinDoc(Concat(Text(s), Line), d...)
}

// JoinDoc joins Docs d with Doc s.
func JoinDoc(s Doc, d ...Doc) Doc {
	switch len(d) {
	case 0:
		return Nil
	case 1:
		return d[0]
	default:
		return Fold(Concat, d[0], s, JoinDoc(s, d[1:]...))
	}
}

// JoinNestedRight nests nested with string s.
// Every item after the first is indented.
// For example:
// aaaa
// <sep> bbb
//       bbb
// <sep> ccc
//       ccc
func JoinNestedRight(sep Doc, nested ...Doc) Doc {
	switch len(nested) {
	case 0:
		return Nil
	case 1:
		return nested[0]
	default:
		return Concat(
			nested[0],
			FoldMap(Concat,
				func(a Doc) Doc { return Concat(Line, ConcatSpace(sep, NestT(Group(a)))) },
				nested[1:]...))
	}
}

// ConcatLine concatenates two Docs with a Line.
func ConcatLine(a, b Doc) Doc {
	return simplifyNil(a, b, func(a, b Doc) Doc {
		return Concat(
			a,
			Concat(
				Line,
				b,
			),
		)
	})
}

// ConcatSpace concatenates two Docs with a space.
func ConcatSpace(a, b Doc) Doc {
	return simplifyNil(a, b, func(a, b Doc) Doc {
		return Concat(
			a,
			Concat(
				textSpace,
				b,
			),
		)
	})
}

// Stack concats Docs with a Line between each.
func Stack(d ...Doc) Doc {
	return Fold(ConcatLine, d...)
}

// JoinGroupAligned nests joined d with divider under head.
func JoinGroupAligned(head, divider string, d ...Doc) Doc {
	return AlignUnder(Text(head), Join(divider, d...))
}

// NestUnder nests nested under head.
func NestUnder(head, nested Doc) Doc {
	return Group(Concat(
		head,
		NestT(Concat(
			Line,
			Group(nested),
		)),
	))
}

// AlignUnder aligns nested to the right of head, and, if
// this does not fit on the line, nests nested under head.
func AlignUnder(head, nested Doc) Doc {
	g := Group(nested)
	a := ConcatSpace(head, Align(g))
	b := Concat(head, NestT(Concat(Line, g)))
	return Group(union{a, b})
}

// Fold applies f recursively to all Docs in d.
func Fold(f func(a, b Doc) Doc, d ...Doc) Doc {
	switch len(d) {
	case 0:
		return Nil
	case 1:
		return d[0]
	default:
		return f(d[0], Fold(f, d[1:]...))
	}
}

// FoldMap applies f recursively to all Docs in d processed through g.
func FoldMap(f func(a, b Doc) Doc, g func(Doc) Doc, d ...Doc) Doc {
	switch len(d) {
	case 0:
		return Nil
	case 1:
		return g(d[0])
	default:
		return f(g(d[0]), FoldMap(f, g, d[1:]...))
	}
}

// Bracket brackets x with l and r and given Nest arguments.
// We use the "soft break" special document here so that
// the flattened version (when grouped) does not insert
// spaces between the parentheses and their content.
func Bracket(l string, x Doc, r string) Doc {
	return BracketDoc(Text(l), x, Text(r))
}

// BracketDoc is like Bracket except it accepts Docs instead of strings.
func BracketDoc(l, x, r Doc) Doc {
	return Group(Fold(Concat,
		l,
		NestT(Concat(SoftBreak, x)),
		SoftBreak,
		r,
	))
}

// simplifyNil returns fn(a, b). If either Doc is Nil, the other Doc
// is returned without invoking fn.
func simplifyNil(a, b Doc, fn func(Doc, Doc) Doc) Doc {
	if a == Nil {
		return b
	}
	if b == Nil {
		return a
	}
	return fn(a, b)
}

// JoinNestedOuter attempts to de-indent the items after the first so
// that the operator appears in the right margin. This replacement is
// only done if there are enough "simple spaces" (as per previous uses
// of Align) to de-indent. No attempt is made to de-indent hard tabs,
// otherwise alignment may break on output devices with a different
// physical tab width.
func JoinNestedOuter(lbl string, d ...Doc) Doc {
	sep := Text(lbl)
	return &snesting{
		f: func(k int16) Doc {
			if k < int16(len(lbl)+1) {
				// If there is not enough space, don't even try. Just use a
				// regular nested section.
				return JoinNestedRight(sep, d...)
			}
			// If there is enough space, on the other hand, we can de-indent
			// every line after the first.
			return Concat(d[0],
				NestS(int16(-len(lbl)-1),
					FoldMap(
						Concat,
						func(x Doc) Doc {
							return Concat(
								Line,
								ConcatSpace(sep, Align(Group(x))))
						},
						d[1:]...)))
		},
	}
}

// RLTableRow is the data for one row of a RLTable (see below).
type RLTableRow struct {
	Label string
	Doc   Doc
}

// RLTable defines a document that formats a list of pairs of items either:
//  - as a 2-column table, with the left column right-aligned and the right
//    column left-aligned, for example:
//       SELECT aaa
//              bbb
//         FROM ccc
//  - as sections, for example:
//       SELECT
//           aaa
//           bbb
//       FROM
//           ccc
//
// We restrict the left value in each list item to be a one-line string
// to make the width computation efficient.
//
// For convenience, the function also skips over rows with a nil
// pointer as doc.
//
// For convenience, the function also takes a boolean "align" which,
// if false, skips the alignment and only produces the section-based
// output.
func RLTable(align bool, rows ...RLTableRow) Doc {
	items := make([]Doc, 0, len(rows))

	// Compute the nested formatting in "sections". It's simple.
	// Note that we do not use NestUnder() because we are not grouping
	// at this level (the group is done for the final form below).
	for _, r := range rows {
		if r.Doc == nil || (r.Label == "" && r.Doc == Nil) {
			continue
		}
		if r.Label != "" {
			d := simplifyNil(Text(r.Label), r.Doc,
				func(a, b Doc) Doc {
					return Concat(a, NestT(Concat(Line, Group(b))))
				})
			items = append(items, d)
		} else {
			items = append(items, Group(r.Doc))
		}
	}
	nestedSections := Stack(items...)

	finalDoc := nestedSections

	if align {
		// First, we need the left column width.
		leftwidth := 0
		for _, r := range rows {
			if r.Doc == nil {
				continue
			}
			if leftwidth < len(r.Label) {
				leftwidth = len(r.Label)
			}
		}
		// Now convert the rows.
		items = items[:0]
		for _, r := range rows {
			if r.Doc == nil || (r.Label == "" && r.Doc == Nil) {
				continue
			}
			if r.Label != "" {
				d := simplifyNil(Text(r.Label), r.Doc,
					func(a, b Doc) Doc {
						return ConcatSpace(a, Align(Group(b)))
					})
				items = append(items, Concat(pad{int16(leftwidth - len(r.Label))}, d))
			} else {
				items = append(items, Concat(pad{int16(leftwidth + 1)}, Align(Group(r.Doc))))
			}
		}
		alignedTable := Stack(items...)

		finalDoc = union{alignedTable, nestedSections}
	}

	return Group(finalDoc)
}
