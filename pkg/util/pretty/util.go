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
func JoinNestedRight(n int, sep Doc, nested ...Doc) Doc {
	switch len(nested) {
	case 0:
		return Nil
	case 1:
		return nested[0]
	default:
		return Concat(
			nested[0],
			FoldMap(Concat,
				func(a Doc) Doc { return Concat(Line, ConcatSpace(sep, Nest(n, Group(a)))) },
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

// JoinGroup nests joined d with divider under name.
func JoinGroup(n int, name, divider string, d ...Doc) Doc {
	return AlignUnder(n, Text(name), Join(divider, d...))
}

// NestUnder nests nested under head.
func NestUnder(n int, head, nested Doc) Doc {
	return Group(Concat(
		head,
		Nest(n, Concat(
			Line,
			Group(nested),
		)),
	))
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
func Bracket(n int, l string, x Doc, r string) Doc {
	return Group(Fold(Concat,
		Text(l),
		Nest(n, Concat(SoftBreak, x)),
		SoftBreak,
		Text(r),
	))
}

// AlignUnder aligns nested to the right of name, and, if
// this does not fit on the line, nests nested under name
// with nesting level n.
func AlignUnder(n int, name, nested Doc) Doc {
	g := Group(nested)
	a := ConcatSpace(name, Align(g))
	b := Concat(name, Nest(n, Concat(Line, g)))
	return Group(union{a, b})
}

// simplifyNil returns fn(a, b). nil (the Go value) is converted to Nil (the
// Doc). If either Doc is Nil, the other Doc is returned without invoking fn.
func simplifyNil(a, b Doc, fn func(Doc, Doc) Doc) Doc {
	if a == nil {
		a = Nil
	}
	if b == nil {
		b = Nil
	}
	if a == Nil {
		return b
	}
	if b == Nil {
		return a
	}
	return fn(a, b)
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
func RLTable(n int, rows ...RLTableRow) Doc {
	items := make([]Doc, len(rows))

	// We'll compute the aligned formatting first. For this we first
	// need the left column width.
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
	for i, r := range rows {
		if r.Doc == nil {
			continue
		}
		items[i] = Concat(pad{leftwidth - len(r.Label)},
			ConcatSpace(Text(r.Label), Align(Group(r.Doc))))
	}
	alignedTable := Stack(items...)

	// The compute the nested formatting. It's simpler.
	// Note that we do not use NestUnder() because we are not
	// grouping at this level (the group is done for the final
	// form below).
	for i, r := range rows {
		if r.Doc == nil {
			continue
		}
		items[i] = Concat(Text(r.Label), Nest(n, Concat(Line, Group(r.Doc))))
	}
	nestedSections := Stack(items...)

	return Group(union{alignedTable, nestedSections})
}
