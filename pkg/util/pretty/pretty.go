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
	"bytes"
	"fmt"
	"strings"
)

// See the referenced paper in the package documentation for explanations
// of the below code. Methods, variables, and implementation details were
// made to resemble it as close as possible.

// Pretty returns a pretty-printed string for the Doc d at line length n.
func Pretty(d Doc, n int) string {
	var sb strings.Builder
	b := best(n, d)
	layout(&sb, b)
	return sb.String()
}

// w is the max line width.
func best(w int, x Doc) Doc {
	b := beExec{
		w:     w,
		cache: make(map[string]Doc),
	}
	return b.be(0, iDoc{0, "", x})
}

type iDoc struct {
	i int
	s string
	d Doc
}

func (i iDoc) String() string {
	return fmt.Sprintf("{%d: %s}", i.i, i.d)
}

type beExec struct {
	w int
	// cache is a memoized cache used during better calculation.
	cache map[string]Doc
	buf   bytes.Buffer
}

func (b beExec) be(k int, x ...iDoc) Doc {
	if len(x) == 0 {
		return Nil
	}
	d := x[0]
	z := x[1:]
	switch t := d.d.(type) {
	case nilDoc:
		return b.be(k, z...)
	case concat:
		return b.be(k, append([]iDoc{{d.i, d.s, t.a}, {d.i, d.s, t.b}}, z...)...)
	case nest:
		x[0] = iDoc{
			d: t.d,
			s: d.s + t.s,
			i: d.i + t.n,
		}
		return b.be(k, x...)
	case text:
		return textX{
			s: string(t),
			d: b.be(k+len(t), z...),
		}
	case line:
		return lineX{
			s: d.s,
			d: b.be(d.i, z...),
		}
	case union:
		// Use a memoized version of the Doc and check if it's been through this
		// function before. There may be a faster implementation that converts this
		// function to an iterative style, but this current implementation is almost
		// identical to the paper (as this in done automatically in Haskell) and is
		// fast enough.
		for _, xd := range x {
			b.buf.WriteString(xd.String())
		}
		s := b.buf.String()
		b.buf.Reset()
		cached, ok := b.cache[s]
		if ok {
			return cached
		}

		n := append([]iDoc{{d.i, d.s, t.x}}, z...)
		res := better(b.w, k,
			b.be(k, n...),
			func() Doc {
				n[0].d = t.y
				return b.be(k, n...)
			},
		)
		b.cache[s] = res
		return res
	default:
		panic(fmt.Errorf("unknown type: %T", d.d))
	}
}

func better(w, k int, x Doc, y func() Doc) Doc {
	if fits(w-k, x) {
		return x
	}
	return y()
}

func fits(w int, x Doc) bool {
	if w < 0 {
		return false
	}
	if x == Nil {
		return true
	}
	if t, ok := x.(textX); ok {
		return fits(w-len(t.s), t.d)
	}
	if _, ok := x.(lineX); ok {
		return true
	}
	panic(fmt.Errorf("unknown type: %T", x))
}

func layout(sb *strings.Builder, d Doc) {
	switch d := d.(type) {
	case nilDoc:
		// ignore
	case textX:
		sb.WriteString(d.s)
		layout(sb, d.d)
	case lineX:
		sb.WriteString("\n")
		sb.WriteString(d.s)
		layout(sb, d.d)
	default:
		panic(fmt.Errorf("unknown type: %T", d))
	}
}
