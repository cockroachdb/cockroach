// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package pretty

import (
	"strings"

	"github.com/cockroachdb/cockroach/pkg/util/errorutil"
	"github.com/cockroachdb/errors"
)

// See the referenced paper in the package documentation for the
// original algorithm. This implementation uses the same document
// model (union, flatten, group) but processes it greedily and
// iteratively: each union is resolved by checking if the flat
// layout fits on the current line, then committing to one branch
// without rendering both. Output is written directly to a
// strings.Builder with no intermediate representation; this is
// possible because fitness is checked on the Doc tree (via
// fits) before committing, so there is no need to build the
// layout first and inspect it afterward.

// iDoc represents the type [(Int,DOC)] in the paper,
// extended with arbitrary string prefixes (not just int).
// We store them in linked lists to linearize the Doc tree
// for iterative processing.
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
	// useTabs controls whether indentation uses tab characters.
	useTabs bool

	// idocAlloc speeds up the allocations by (*beExec).newiDoc()
	// defined below.
	idocAlloc []iDoc

	// keywordTransform filters keywords if not nil.
	keywordTransform func(string) string
}

// Pretty returns a pretty-printed string for the Doc d at line length
// n and tab width t. Keyword Docs are filtered through keywordTransform
// if not nil. keywordTransform must not change the visible length of its
// argument. It can, for example, add invisible characters like control codes
// (colors, etc.).
func Pretty(
	d Doc, n int, useTabs bool, tabWidth int, keywordTransform func(string) string,
) (_ string, retErr error) {
	defer errorutil.MaybeCatchPanic(&retErr, nil /* errCallback */)

	var sb strings.Builder
	b := beExec{
		w:                int16(n),
		tabWidth:         int16(tabWidth),
		useTabs:          useTabs,
		keywordTransform: keywordTransform,
	}
	b.be(&sb, docPos{0, 0}, b.newiDoc(docPos{0, 0}, d, nil))
	return sb.String(), nil
}

// be iteratively selects the best layout for the document represented
// by xlist and writes it directly to sb. Structural cases (nil,
// concat, nest, union, etc.) transform the iDoc list. Content cases
// (text, keyword, line, pad) write to sb and advance.
func (b *beExec) be(sb *strings.Builder, k docPos, xlist *iDoc) {
	for {
		if xlist == nil {
			return
		}

		d := *xlist
		z := xlist.next

		switch t := d.d.(type) {
		// Structural cases: transform the iDoc list.
		case nilDoc:
			xlist = z
		case *concat:
			xlist = b.newiDoc(d.i, t.a, b.newiDoc(d.i, t.b, z))
		case nests:
			xlist = b.newiDoc(
				docPos{d.i.tabs, d.i.spaces + t.n}, t.d, z,
			)
		case nestt:
			xlist = b.newiDoc(
				docPos{d.i.tabs + 1 + d.i.spaces/b.tabWidth, 0},
				t.d, z,
			)
		case *scolumn:
			xlist = b.newiDoc(d.i, t.f(k.spaces), z)
		case *snesting:
			xlist = b.newiDoc(d.i, t.f(d.i.spaces), z)
		case *union:
			// Greedy: check if the flat (x) branch fits on the
			// current line, then commit without rendering both.
			flat := b.newiDoc(d.i, t.x, z)
			if b.fits(k, flat) {
				xlist = flat
			} else {
				xlist = b.newiDoc(d.i, t.y, z)
			}
		// Content cases: write directly to sb and advance.
		case text:
			sb.WriteString(string(t))
			k = docPos{k.tabs, k.spaces + int16(len(t))}
			xlist = z
		case keyword:
			s := string(t)
			if b.keywordTransform != nil {
				sb.WriteString(b.keywordTransform(s))
			} else {
				sb.WriteString(s)
			}
			k = docPos{k.tabs, k.spaces + int16(len(t))}
			xlist = z
		case line, softbreak, hardline:
			sb.WriteByte('\n')
			// Fill the tabs first.
			padTabs := d.i.tabs * b.tabWidth
			if b.useTabs {
				for i := int16(0); i < d.i.tabs; i++ {
					sb.WriteByte('\t')
				}
				padTabs = 0
			}
			// Fill the remaining spaces.
			for i := int16(0); i < padTabs+d.i.spaces; i++ {
				sb.WriteByte(' ')
			}
			k = d.i
			xlist = z
		case pad:
			for i := int16(0); i < t.n; i++ {
				sb.WriteByte(' ')
			}
			k = docPos{k.tabs, k.spaces + t.n}
			xlist = z
		// Error cases.
		case failDoc:
			panic(errors.AssertionFailedf("failDoc should never be reached"))
		default:
			panic(errors.AssertionFailedf("unknown type: %T", d.d))
		}
	}
}

// fitsState saves the position and iDoc list at a union so that
// fits can backtrack to the broken branch if the flat branch
// doesn't fit.
type fitsState struct {
	k     docPos
	xlist *iDoc
}

// fits checks whether the first line of the document represented
// by xlist fits within the remaining line width given position k. It
// scans the iDoc list counting character widths without writing any
// output. For unions, it tries both branches since the outer union
// could still fit if inner unions break.
func (b *beExec) fits(k docPos, xlist *iDoc) bool {
	var checkpoints []fitsState
	for {
	scanLoop:
		for {
			if k.spaces+k.tabs*b.tabWidth > b.w {
				break scanLoop
			}
			if xlist == nil {
				return true
			}
			d := *xlist
			z := xlist.next
			switch t := d.d.(type) {
			case nilDoc:
				xlist = z
			case *concat:
				xlist = b.newiDoc(d.i, t.a, b.newiDoc(d.i, t.b, z))
			case nests:
				xlist = b.newiDoc(
					docPos{d.i.tabs, d.i.spaces + t.n}, t.d, z,
				)
			case nestt:
				xlist = b.newiDoc(
					docPos{d.i.tabs + 1 + d.i.spaces/b.tabWidth, 0},
					t.d, z,
				)
			case *scolumn:
				xlist = b.newiDoc(d.i, t.f(k.spaces), z)
			case *snesting:
				xlist = b.newiDoc(d.i, t.f(d.i.spaces), z)
			case text:
				k = docPos{k.tabs, k.spaces + int16(len(t))}
				xlist = z
			case keyword:
				k = docPos{k.tabs, k.spaces + int16(len(t))}
				xlist = z
			case line, softbreak, hardline:
				return true
			case *union:
				// Try flat branch first; save broken branch as checkpoint.
				xlist = b.newiDoc(d.i, t.x, z)
				checkpoints = append(checkpoints, fitsState{
					k: k, xlist: b.newiDoc(d.i, t.y, z),
				})
			case pad:
				k = docPos{k.tabs, k.spaces + t.n}
				xlist = z
			case failDoc:
				break scanLoop
			default:
				panic(errors.AssertionFailedf("unknown type: %T", d.d))
			}
		}
		// Backtrack to the most recent union's broken branch.
		if len(checkpoints) == 0 {
			return false
		}
		cp := checkpoints[len(checkpoints)-1]
		checkpoints = checkpoints[:len(checkpoints)-1]
		k = cp.k
		xlist = cp.xlist
	}
}

// newiDoc makes a new iDoc on the heap. Allocations are batched
// for more efficiency.
func (b *beExec) newiDoc(i docPos, d Doc, z *iDoc) *iDoc {
	buf := &b.idocAlloc
	if len(*buf) == 0 {
		*buf = make([]iDoc, 100)
	}
	r := &(*buf)[0]
	*r = iDoc{i: i, d: d, next: z}
	*buf = (*buf)[1:]
	return r
}
