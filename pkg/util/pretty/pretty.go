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
	i   docPos
	s   string
	d   *docBest
}

type docBestType int

const (
	textB docBestType = iota
	lineB
	hardlineB
	spacesB
	keywordB
)

// Pretty returns a pretty-printed string for the Doc d at line length
// n and tab width t. Keyword Docs are filtered through keywordTransform
// if not nil. keywordTransform must not change the visible length of its
// argument. It can, for example, add invisible characters like control codes
// (colors, etc.).
func Pretty(d Doc, n int, useTabs bool, tabWidth int, keywordTransform func(string) string) string {
	var sb strings.Builder
	b := beExec{
		w:                int16(n),
		tabWidth:         int16(tabWidth),
		memoBe:           make(map[beArgs]*docBest),
		memoiDoc:         make(map[iDoc]*iDoc),
		keywordTransform: keywordTransform,
	}
	ldoc := b.best(d)
	b.layout(&sb, useTabs, ldoc)
	return sb.String()
}

// w is the max line width.
func (b *beExec) best(x Doc) *docBest {
	return b.be(docPos{0, 0}, b.iDoc(docPos{0, 0}, x, nil))
}

// iDoc represents the type [(Int,DOC)] in the paper,
// extended with arbitrary string prefixes (not just int).
// We'll use linked lists because this makes the
// recursion more efficient than slices.
type iDoc struct {
	d    Doc
	next *iDoc
	i    docPos
}

type docPos struct {
	tabs   int16
	spaces int16
}

type beExec struct {
	// w is the available line width.
	w int16
	// tabWidth is the virtual tab width.
	tabWidth int16

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

	// keywordTransform filters keywords if not nil.
	keywordTransform func(string) string
}

func (b *beExec) be(k docPos, xlist *iDoc) *docBest {
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
	case *concat:
		res = b.be(k, b.iDoc(d.i, t.a, b.iDoc(d.i, t.b, z)))
	case nests:
		res = b.be(k, b.iDoc(docPos{d.i.tabs, d.i.spaces + t.n}, t.d, z))
	case nestt:
		res = b.be(k, b.iDoc(docPos{d.i.tabs + 1 + d.i.spaces/b.tabWidth, 0}, t.d, z))
	case text:
		res = b.newDocBest(docBest{
			tag: textB,
			s:   string(t),
			d:   b.be(docPos{k.tabs, k.spaces + int16(len(t))}, z),
		})
	case keyword:
		res = b.newDocBest(docBest{
			tag: keywordB,
			s:   string(t),
			d:   b.be(docPos{k.tabs, k.spaces + int16(len(t))}, z),
		})
	case line, softbreak:
		res = b.newDocBest(docBest{
			tag: lineB,
			i:   d.i,
			d:   b.be(d.i, z),
		})
	case hardline:
		res = b.newDocBest(docBest{
			tag: hardlineB,
			i:   d.i,
			d:   b.be(d.i, z),
		})
	case *union:
		res = b.better(k,
			b.be(k, b.iDoc(d.i, t.x, z)),
			// We eta-lift the second argument to avoid eager evaluation.
			func() *docBest {
				return b.be(k, b.iDoc(d.i, t.y, z))
			},
		)
	case *scolumn:
		res = b.be(k, b.iDoc(d.i, t.f(k.spaces), z))
	case *snesting:
		res = b.be(k, b.iDoc(d.i, t.f(d.i.spaces), z))
	case pad:
		res = b.newDocBest(docBest{
			tag: spacesB,
			i:   docPos{spaces: t.n},
			d:   b.be(docPos{k.tabs, k.spaces + t.n}, z),
		})
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
func (b *beExec) iDoc(i docPos, d Doc, z *iDoc) *iDoc {
	idoc := iDoc{i: i, d: d, next: z}
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
	d *iDoc
	k docPos
}

func (b *beExec) better(k docPos, x *docBest, y func() *docBest) *docBest {
	remainder := b.w - k.spaces - k.tabs*b.tabWidth
	if fits(remainder, x) {
		return x
	}
	return y()
}

func fits(w int16, x *docBest) bool {
	if w < 0 {
		return false
	}
	if x == nil {
		// Nil doc.
		return true
	}
	switch x.tag {
	case textB, keywordB:
		return fits(w-int16(len(x.s)), x.d)
	case lineB:
		return true
	case hardlineB:
		return false
	case spacesB:
		return fits(w-x.i.spaces, x.d)
	default:
		panic(fmt.Errorf("unknown type: %d", x.tag))
	}
}

func (b *beExec) layout(sb *strings.Builder, useTabs bool, d *docBest) {
	for ; d != nil; d = d.d {
		switch d.tag {
		case textB:
			sb.WriteString(d.s)
		case keywordB:
			if b.keywordTransform != nil {
				sb.WriteString(b.keywordTransform(d.s))
			} else {
				sb.WriteString(d.s)
			}
		case lineB, hardlineB:
			sb.WriteByte('\n')
			// Fill the tabs first.
			padTabs := d.i.tabs * b.tabWidth
			if useTabs {
				for i := int16(0); i < d.i.tabs; i++ {
					sb.WriteByte('\t')
				}
				padTabs = 0
			}

			// Fill the remaining spaces.
			for i := int16(0); i < padTabs+d.i.spaces; i++ {
				sb.WriteByte(' ')
			}
		case spacesB:
			for i := int16(0); i < d.i.spaces; i++ {
				sb.WriteByte(' ')
			}
		default:
			panic(fmt.Errorf("unknown type: %d", d.tag))
		}
	}
}
