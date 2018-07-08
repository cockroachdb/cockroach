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

import (
	"fmt"
	"strings"
)

// See the referenced paper in the package documentation for explanations
// of the below code. Methods, variables, and implementation details were
// made to resemble it as close as possible.

// docBest represents a selected document as described by the type
// "Doc" in the referenced paper (not "DOC"). This is the
// less-abstract representation constructed during "best layout"
// selection.
type docBest struct {
	tag docBestType
	i   int
	s   string
	d   *docBest
}

type docBestType int

const (
	textB docBestType = iota
	lineB
)

// Pretty returns a pretty-printed string for the Doc d at line length
// n and tab width t.
func Pretty(d Doc, n int, useTabs bool, tabWidth int) string {
	var sb strings.Builder
	b := best(n, d)
	layout(&sb, useTabs, tabWidth, b)
	return sb.String()
}

// w is the max line width.
func best(w int, x Doc) *docBest {
	b := beExec{
		w:        w,
		memoBe:   make(map[beArgs]*docBest),
		memoiDoc: make(map[iDoc]*iDoc),
	}
	return b.be(0, &iDoc{0, x, nil})
}

// iDoc represents the type [(Int,DOC)] in the paper,
// extended with arbitrary string prefixes (not just int).
// We'll use linked lists because this makes the
// recursion more efficient than slices.
type iDoc struct {
	i    int
	d    Doc
	next *iDoc
}

type beExec struct {
	// w is the available line width.
	w int

	// memoBe internalizes the results of the be function, so that the
	// same value is not computed multiple times.
	memoBe map[beArgs]*docBest

	// memo internalizes iDoc objects to ensure they are unique in memory,
	// and we can use pointer-pointer comparisons.
	memoiDoc map[iDoc]*iDoc

	// docAlloc speeds up the allocations of be()'s return values
	// by (*beExec).newDocBest() defined below.
	docAlloc []docBest

	// idocAlloc speeds up the allocations by (*beExec).iDoc() defined
	// below.
	idocAlloc []iDoc
}

func (b *beExec) be(k int, xlist *iDoc) *docBest {
	// Shortcut: be k [] = Nil
	if xlist == nil {
		return nil
	}

	// If we've computed this result before, short cut here too.
	memoKey := beArgs{k: k, d: xlist}
	if cached, ok := b.memoBe[memoKey]; ok {
		return cached
	}

	// General case.

	d := *xlist
	z := xlist.next

	// Note: we'll need to memoize the result below.
	var res *docBest

	switch t := d.d.(type) {
	case nilDoc:
		res = b.be(k, z)
	case concat:
		res = b.be(k, b.iDoc(d.i, t.a, b.iDoc(d.i, t.b, z)))
	case nest:
		res = b.be(k, b.iDoc(d.i+t.n, t.d, z))
	case text:
		res = b.newDocBest(docBest{
			tag: textB,
			s:   string(t),
			d:   b.be(k+len(t), z),
		})
	case line, softbreak:
		res = b.newDocBest(docBest{
			tag: lineB,
			i:   d.i,
			d:   b.be(d.i, z),
		})
	case union:
		res = better(b.w, k,
			b.be(k, b.iDoc(d.i, t.x, z)),
			// We eta-lift the second argument to avoid eager evaluation.
			func() *docBest {
				return b.be(k, b.iDoc(d.i, t.y, z))
			},
		)
	default:
		panic(fmt.Errorf("unknown type: %T", d.d))
	}

	// Memoize so we don't compute the same result twice.
	b.memoBe[memoKey] = res

	return res
}

// newDocBest makes a new docBest on the heap. Allocations
// are batched for more efficiency.
func (b *beExec) newDocBest(d docBest) *docBest {
	buf := &b.docAlloc
	if len(*buf) == 0 {
		*buf = make([]docBest, 100)
	}
	r := &(*buf)[0]
	*r = d
	*buf = (*buf)[1:]
	return r
}

// iDoc retrieves the unique instance of iDoc in memory for the given
// values of i, s, d and z. The object is constructed if it does not
// exist yet.
//
// The results of this function guarantee that the pointer addresses
// are equal if the arguments used to construct the value were equal.
func (b *beExec) iDoc(i int, d Doc, z *iDoc) *iDoc {
	idoc := iDoc{i, d, z}
	if m, ok := b.memoiDoc[idoc]; ok {
		return m
	}
	r := b.newiDoc(idoc)
	b.memoiDoc[idoc] = r
	return r
}

// newiDoc makes a new iDoc on the heap. Allocations are batched
// for more efficiency. Do not use this directly! Instead
// use the iDoc() method defined above.
func (b *beExec) newiDoc(d iDoc) *iDoc {
	buf := &b.idocAlloc
	if len(*buf) == 0 {
		*buf = make([]iDoc, 100)
	}
	r := &(*buf)[0]
	*r = d
	*buf = (*buf)[1:]
	return r
}

type beArgs struct {
	k int
	d *iDoc
}

func better(w, k int, x *docBest, y func() *docBest) *docBest {
	if fits(w-k, x) {
		return x
	}
	return y()
}

func fits(w int, x *docBest) bool {
	if w < 0 {
		return false
	}
	if x == nil {
		// Nil doc.
		return true
	}
	switch x.tag {
	case textB:
		return fits(w-len(x.s), x.d)
	case lineB:
		return true
	default:
		panic(fmt.Errorf("unknown type: %d", x.tag))
	}
}

func layout(sb *strings.Builder, useTabs bool, tabWidth int, d *docBest) {
	for ; d != nil; d = d.d {
		switch d.tag {
		case textB:
			sb.WriteString(d.s)
		case lineB:
			sb.WriteByte('\n')
			// Fill as many blanks with tabs, then the rest with spaces.
			c := 0
			if useTabs {
				for ; c+tabWidth <= d.i; c += tabWidth {
					sb.WriteByte('\t')
				}
			}
			for ; c < d.i; c++ {
				sb.WriteByte(' ')
			}
		default:
			panic(fmt.Errorf("unknown type: %d", d.tag))
		}
	}
}
