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
func JoinNestedRight(n int, s string, sep Doc, nested ...Doc) Doc {
	switch len(nested) {
	case 0:
		return Nil
	case 1:
		return nested[0]
	default:
		return Concat(
			nested[0],
			FoldMap(Concat,
				func(a Doc) Doc { return Concat(Line, ConcatSpace(sep, Nest(n, s, Group(a)))) },
				nested[1:]...))
	}
}

// ConcatLine concatenates two Docs with a Line.
func ConcatLine(a, b Doc) Doc {
	return concatFn(a, b, func(a, b Doc) Doc {
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
	return concatFn(a, b, func(a, b Doc) Doc {
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
func JoinGroup(n int, s string, name, divider string, d ...Doc) Doc {
	return NestName(n, s, Text(name), Join(divider, d...))
}

// NestName nests nested under name with string s.
func NestName(n int, s string, name, nested Doc) Doc {
	return Group(Concat(
		name,
		Nest(n, s, Concat(
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
func Bracket(n int, s string, l string, x Doc, r string) Doc {
	a := Fold(Concat,
		Text(l),
		x,
		Text(r),
	)
	b := Fold(Concat,
		Text(l),
		Nest(n, s, Concat(Line, x)),
		Line,
		Text(r),
	)
	return union{flatten(a), b}
}
