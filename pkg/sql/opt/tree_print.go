// Copyright 2017 The Cockroach Authors.
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

package opt

import (
	"bytes"
	"fmt"
)

// treePrinter pretty-prints a tree:
//
//   root
//    |- child1
//    |   |- grandchild1
//    |   |- grandchild2
//    |- child2
//
type treePrinter struct {
	level int

	// We maintain the rows accumulated so far.
	// When a new child is adedd (e.g. child2 above), we may have to go back up
	// and replace spaces with "|".
	rows [][]byte

	// The index of the last row for a given level.
	lastEntry []int
}

func makeTreePrinter() treePrinter {
	return treePrinter{
		lastEntry: make([]int, 1, 4),
	}
}

// Enter indicates that entries that follow are children of the last entry.
// Each Enter() call must be paired with a subsequent Exit() call.
func (tp *treePrinter) Enter() {
	tp.level++
	tp.lastEntry = append(tp.lastEntry, -1)
}

// Exit is the reverse of Enter.
func (tp *treePrinter) Exit() {
	if tp.level == 0 {
		panic("Exit without Enter")
	}
	tp.level--
	tp.lastEntry = tp.lastEntry[:len(tp.lastEntry)-1]
}

func (tp *treePrinter) Addf(format string, args ...interface{}) {
	tp.Add(fmt.Sprintf(format, args...))
}

func (tp *treePrinter) Add(entry string) {
	// Each level indents by four spaces (" |- ").
	indent := 4 * tp.level
	row := make([]byte, indent+len(entry))
	for i := 0; i < indent-4; i++ {
		row[i] = ' '
	}
	if indent >= 4 {
		copy(row[indent-4:], " |- ")
	}
	copy(row[indent:], entry)
	// Connect to the previous sibling.
	if tp.level > 0 && tp.lastEntry[tp.level] != -1 {
		for i := tp.lastEntry[tp.level] + 1; i < len(tp.rows); i++ {
			tp.rows[i][indent-3] = '|'
		}
	}
	tp.lastEntry[tp.level] = len(tp.rows)
	tp.rows = append(tp.rows, row)
}

func (tp *treePrinter) String() string {
	if tp.level != 0 {
		panic("Enter without Exit")
	}
	var buf bytes.Buffer
	for _, r := range tp.rows {
		buf.Write(r)
		buf.WriteByte('\n')
	}
	return buf.String()
}
