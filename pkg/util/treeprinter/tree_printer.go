// Copyright 2017 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package treeprinter

import (
	"bytes"
	"fmt"
	"strings"
)

var (
	edgeLinkChr = rune('│')
	edgeMidChr  = rune('├')
	edgeLastChr = rune('└')
	horLineChr  = rune('─')
	bulletChr   = rune('•')
)

// Node is a handle associated with a specific depth in a tree. See below for
// sample usage.
type Node struct {
	tree  *tree
	level int
}

// New creates a tree printer and returns a sentinel node reference which
// should be used to add the root. Sample usage:
//
//   tp := New()
//   root := tp.Child("root")
//   root.Child("child-1")
//   root.Child("child-2").Child("grandchild\ngrandchild-more-info")
//   root.Child("child-3")
//
//   fmt.Print(tp.String())
//
// Output:
//
//   root
//    ├── child-1
//    ├── child-2
//    │    └── grandchild
//    │        grandchild-more-info
//    └── child-3
//
// Note that the Child calls can't be rearranged arbitrarily; they have
// to be in the order they need to be displayed (depth-first pre-order).
func New() Node {
	return NewWithStyle(DefaultStyle)
}

// NewWithStyle creates a tree printer like New, permitting customization of
// the style of the resulting tree.
func NewWithStyle(style Style) Node {
	t := &tree{style: style}

	switch style {
	case CompactStyle:
		t.edgeLink = []rune{edgeLinkChr}
		t.edgeMid = []rune{edgeMidChr, ' '}
		t.edgeLast = []rune{edgeLastChr, ' '}

	case BulletStyle:
		t.edgeLink = []rune{edgeLinkChr}
		t.edgeMid = []rune{edgeMidChr, horLineChr, horLineChr, ' '}
		t.edgeLast = []rune{edgeLastChr, horLineChr, horLineChr, ' '}

	default:
		t.edgeLink = []rune{' ', edgeLinkChr}
		t.edgeMid = []rune{' ', edgeMidChr, horLineChr, horLineChr, ' '}
		t.edgeLast = []rune{' ', edgeLastChr, horLineChr, horLineChr, ' '}
	}

	return Node{
		tree:  t,
		level: 0,
	}
}

// Style is one of the predefined treeprinter styles.
type Style int

const (
	// DefaultStyle is the default style. Example:
	//
	//   foo
	//    ├── bar1
	//    │   bar2
	//    │    └── baz
	//    └── qux
	//
	DefaultStyle Style = iota

	// CompactStyle is a compact style, for deeper trees. Example:
	//
	//   foo
	//   ├ bar1
	//   │ bar2
	//   │ └ baz
	//   └ qux
	//
	CompactStyle

	// BulletStyle is a style that shows a bullet for each node, and groups any
	// other lines under that bullet. Example:
	//
	//   • foo
	//   │
	//   ├── • bar1
	//   │   │ bar2
	//   │   │
	//   │   └── • baz
	//   │
	//   └── • qux
	//
	BulletStyle
)

// tree implements the tree printing machinery.
//
// All Nodes hold a reference to the tree and Node calls result in modification
// of the tree. At any point in time, tree.rows contains the formatted tree that
// was described by the Node calls performed so far.
//
// When new nodes are added, some of the characters of the previous formatted
// tree need to be updated. Here is an example stepping through the state:
//
//   API call                       Rows
//
//
//   tp := New()                    <empty>
//
//
//   root := tp.Child("root")       root
//
//
//   root.Child("child-1")          root
//                                   └── child-1
//
//
//   c2 := root.Child("child-2")    root
//                                   ├── child-1
//                                   └── child-2
//
//     Note: here we had to go back up and change └─ into ├─ for child-1.
//
//
//   c2.Child("grandchild")         root
//                                   ├── child-1
//                                   └── child-2
//                                        └── grandchild
//
//
//   root.Child("child-3"           root
//                                   ├── child-1
//                                   ├── child-2
//                                   │    └── grandchild
//                                   └── child-3
//
//     Note: here we had to go back up and change └─ into ├─ for child-2, and
//     add a │ on the grandchild row. In general, we may need to add an
//     arbitrary number of vertical bars.
//
// In order to perform these character changes, we maintain information about
// the nodes on the bottom-most path.
type tree struct {
	style Style

	// rows maintains the rows accumulated so far, as rune arrays.
	rows [][]rune

	// stack contains information pertaining to the nodes on the bottom-most path
	// of the tree.
	stack []nodeInfo

	edgeLink []rune
	edgeMid  []rune
	edgeLast []rune
}

type nodeInfo struct {
	// firstChildConnectRow is the index (in tree.rows) of the row up to which we
	// have to connect the first child of this node.
	firstChildConnectRow int

	// nextSiblingConnectRow is the index (in tree.rows) of the row up to which we
	// have to connect the next sibling of this node. Typically this is the same
	// with firstChildConnectRow, except when the node has multiple rows. For
	// example:
	//
	//      foo
	//       └── bar1               <---- nextSiblingConnectRow
	//           bar2               <---- firstChildConnectRow
	//
	// firstChildConnectRow is used when adding "baz", nextSiblingConnectRow
	// is used when adding "qux":
	//      foo
	//       ├── bar1
	//       │   bar2
	//       │    └── baz
	//       └── qux
	//
	nextSiblingConnectRow int
}

// set copies the string of runes into a given row, at a specific position. The
// row is extended with spaces if needed.
func (t *tree) set(rowIdx int, colIdx int, what []rune) {
	// Extend the line if necessary.
	for len(t.rows[rowIdx]) < colIdx+len(what) {
		t.rows[rowIdx] = append(t.rows[rowIdx], ' ')
	}
	copy(t.rows[rowIdx][colIdx:], what)
}

// addRow adds a row with a given text, with the proper indentation for the
// given level.
func (t *tree) addRow(level int, text string) (rowIdx int) {
	runes := []rune(text)
	// Each level indents by this much.
	k := len(t.edgeLast)
	indent := level * k
	row := make([]rune, indent+len(runes))
	for i := 0; i < indent; i++ {
		row[i] = ' '
	}
	copy(row[indent:], runes)
	t.rows = append(t.rows, row)
	return len(t.rows) - 1
}

// Childf adds a node as a child of the given node.
func (n Node) Childf(format string, args ...interface{}) Node {
	return n.Child(fmt.Sprintf(format, args...))
}

// Child adds a node as a child of the given node. Multi-line strings are
// supported with appropriate indentation.
func (n Node) Child(text string) Node {
	if strings.ContainsRune(text, '\n') {
		splitLines := strings.Split(text, "\n")
		node := n.childLine(splitLines[0])
		for _, l := range splitLines[1:] {
			node.AddLine(l)
		}
		return node
	}
	return n.childLine(text)
}

// AddLine adds a new line to a node without an edge.
func (n Node) AddLine(text string) {
	t := n.tree
	if t.style == BulletStyle {
		text = "  " + text
	}
	rowIdx := t.addRow(n.level-1, text)
	if t.style != BulletStyle {
		t.stack[n.level-1].firstChildConnectRow = rowIdx
	}
}

// childLine adds a node as a child of the given node.
func (n Node) childLine(text string) Node {
	t := n.tree
	if t.style == BulletStyle {
		text = fmt.Sprintf("%c %s", bulletChr, text)
		if n.level > 0 {
			n.AddEmptyLine()
		}
	}
	rowIdx := t.addRow(n.level, text)
	edgePos := (n.level - 1) * len(t.edgeLast)
	if n.level == 0 {
		// Case 1: root.
		if len(t.stack) != 0 {
			panic("multiple root nodes")
		}
	} else if len(t.stack) <= n.level {
		// Case 2: first child. Connect to parent.
		if len(t.stack) != n.level {
			panic("misuse of node")
		}
		parentRow := t.stack[n.level-1].firstChildConnectRow
		for i := parentRow + 1; i < rowIdx; i++ {
			t.set(i, edgePos, t.edgeLink)
		}
		t.set(rowIdx, edgePos, t.edgeLast)
	} else {
		// Case 3: non-first child. Connect to sibling.
		siblingRow := t.stack[n.level].nextSiblingConnectRow
		t.set(siblingRow, edgePos, t.edgeMid)
		for i := siblingRow + 1; i < rowIdx; i++ {
			t.set(i, edgePos, t.edgeLink)
		}
		t.set(rowIdx, edgePos, t.edgeLast)
		// Update the nextSiblingConnectRow.
		t.stack = t.stack[:n.level]
	}

	t.stack = append(t.stack, nodeInfo{
		firstChildConnectRow:  rowIdx,
		nextSiblingConnectRow: rowIdx,
	})

	// Return a TreePrinter that can be used for children of this node.
	return Node{
		tree:  t,
		level: n.level + 1,
	}
}

// AddEmptyLine adds an empty line to the output; used to introduce vertical
// spacing as needed.
func (n Node) AddEmptyLine() {
	n.tree.rows = append(n.tree.rows, []rune{})
}

// FormattedRows returns the formatted rows. Can only be called on the result of
// treeprinter.New.
func (n Node) FormattedRows() []string {
	if n.level != 0 {
		panic("Only the root can be stringified")
	}
	res := make([]string, len(n.tree.rows))
	for i, r := range n.tree.rows {
		res[i] = string(r)
	}
	return res
}

func (n Node) String() string {
	if n.level != 0 {
		panic("Only the root can be stringified")
	}
	var buf bytes.Buffer
	for _, r := range n.tree.rows {
		buf.WriteString(string(r))
		buf.WriteByte('\n')
	}
	return buf.String()
}
