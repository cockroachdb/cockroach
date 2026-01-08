// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vm

// Program is a sequence of Ops to be executed by the VM.
type Program struct {
	ops []Op
}

// Len return the number of Ops in the Progam.
func (p *Program) Len() int {
	return len(p.ops)
}

// Push appends the given op to the end of the program. It returns the instruction
// pointer of the appended op.
func (p *Program) Push(op Op) int {
	ip := len(p.ops)
	p.ops = append(p.ops, op)
	return ip
}
