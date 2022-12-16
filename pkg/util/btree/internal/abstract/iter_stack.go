// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package abstract

// iterStack represents a stack of (Node, Pos) tuples, which captures
// iteration state as an Iterator descends a AugBTree.
type iterStack[K, V, A any] struct {
	a    iterStackArr[K, V, A]
	aLen int16 // -1 when using s
	s    []iterFrame[K, V, A]
}

const iterStackDepth = 3

// Used to avoid allocations for stacks below a certain size.
type iterStackArr[K, V, A any] [iterStackDepth]iterFrame[K, V, A]

type iterFrame[K, V, A any] struct {
	Node *Node[K, V, A]
	Pos  int16
}

func (is *iterStack[K, V, A]) push(f iterFrame[K, V, A]) {
	if is.aLen == -1 {
		is.s = append(is.s, f)
	} else if int(is.aLen) == len(is.a) {
		is.s = make([](iterFrame[K, V, A]), int(is.aLen)+1, 2*int(is.aLen))
		copy(is.s, is.a[:])
		is.s[int(is.aLen)] = f
		is.aLen = -1
	} else {
		is.a[is.aLen] = f
		is.aLen++
	}
}

func (is *iterStack[K, V, A]) pop() iterFrame[K, V, A] {
	if is.aLen == -1 {
		f := is.s[len(is.s)-1]
		is.s = is.s[:len(is.s)-1]
		return f
	}
	is.aLen--
	return is.a[is.aLen]
}

func (is *iterStack[K, V, A]) len() int {
	if is.aLen == -1 {
		return len(is.s)
	}
	return int(is.aLen)
}

func (is *iterStack[K, V, A]) reset() {
	if is.aLen == -1 {
		is.s = is.s[:0]
	} else {
		is.aLen = 0
	}
}
