// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

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

// ConcatDoc concatenates two Docs with between.
func ConcatDoc(a, b, between Doc) Doc {
	return simplifyNil(a, b, func(a, b Doc) Doc {
		return Concat(
			a,
			Concat(
				between,
				b,
			),
		)
	})
}

// ConcatLine concatenates two Docs with a Line.
func ConcatLine(a, b Doc) Doc {
	return ConcatDoc(a, b, Line)
}

// ConcatSpace concatenates two Docs with a space.
func ConcatSpace(a, b Doc) Doc {
	return ConcatDoc(a, b, textSpace)
}

// Stack concats Docs with a Line between each.
func Stack(d ...Doc) Doc {
	return Fold(ConcatLine, d...)
}

// Fillwords fills lines with as many docs as will fit joined with a space or
// line.
func Fillwords(d ...Doc) Doc {
	u := &union{textSpace, line{}}
	fill := func(a, b Doc) Doc {
		return ConcatDoc(a, b, u)
	}
	return Fold(fill, d...)
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
	return Group(&union{a, b})
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
// physical tab width. docFn should be set to Text or Keyword and will be
// used when converting lbl to a Doc.
func JoinNestedOuter(lbl string, docFn func(string) Doc, d ...Doc) Doc {
	sep := docFn(lbl)
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

// TableRow is the data for one row of a RLTable (see below).
type TableRow struct {
	Label string
	Doc   Doc
}

// TableAlignment should be used as first argument to Table().
type TableAlignment int

const (
	// TableNoAlign does not use alignment and instead uses NestUnder.
	TableNoAlign TableAlignment = iota
	// TableRightAlignFirstColumn right-aligns (left-pads) the first column.
	TableRightAlignFirstColumn
	// TableLeftAlignFirstColumn left-aligns (right-pads) the first column.
	TableLeftAlignFirstColumn
)

// Table defines a document that formats a list of pairs of items either:
//  - as a 2-column table, with the two columns aligned for example:
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
// docFn should be set to Text or Keyword and will be used when converting
// TableRow label's to Docs.
func Table(alignment TableAlignment, docFn func(string) Doc, rows ...TableRow) Doc {
	// Compute the nested formatting in "sections". It's simple.
	// Note that we do not use NestUnder() because we are not grouping
	// at this level (the group is done for the final form below).
	items := makeTableNestedSections(docFn, rows)
	nestedSections := Stack(items...)

	finalDoc := nestedSections

	if alignment != TableNoAlign {
		items = makeAlignedTableItems(alignment == TableRightAlignFirstColumn /* leftPad */, docFn, rows, items)
		alignedTable := Stack(items...)
		finalDoc = &union{alignedTable, nestedSections}
	}

	return Group(finalDoc)
}

func computeLeftColumnWidth(rows []TableRow) int {
	leftwidth := 0
	for _, r := range rows {
		if r.Doc == nil {
			continue
		}
		if leftwidth < len(r.Label) {
			leftwidth = len(r.Label)
		}
	}
	return leftwidth
}

func makeAlignedTableItems(
	leftPad bool, docFn func(string) Doc, rows []TableRow, items []Doc,
) []Doc {
	// We'll need the left column width below.
	leftWidth := computeLeftColumnWidth(rows)
	// Now convert the rows.
	items = items[:0]
	for _, r := range rows {
		if r.Doc == nil || (r.Label == "" && r.Doc == Nil) {
			continue
		}
		if r.Label != "" {
			lbl := docFn(r.Label)
			var pleft Doc
			if leftPad {
				pleft = Concat(pad{int16(leftWidth - len(r.Label))}, lbl)
			} else {
				pleft = Concat(lbl, pad{int16(leftWidth - len(r.Label))})
			}
			d := simplifyNil(pleft, r.Doc,
				func(a, b Doc) Doc {
					return ConcatSpace(a, Align(Group(b)))
				})
			items = append(items, d)
		} else {
			items = append(items, Concat(pad{int16(leftWidth + 1)}, Align(Group(r.Doc))))
		}
	}
	return items
}

func makeTableNestedSections(docFn func(string) Doc, rows []TableRow) []Doc {
	items := make([]Doc, 0, len(rows))
	for _, r := range rows {
		if r.Doc == nil || (r.Label == "" && r.Doc == Nil) {
			continue
		}
		if r.Label != "" {
			d := simplifyNil(docFn(r.Label), r.Doc,
				func(a, b Doc) Doc {
					return Concat(a, NestT(Concat(Line, Group(b))))
				})
			items = append(items, d)
		} else {
			items = append(items, Group(r.Doc))
		}
	}
	return items
}
