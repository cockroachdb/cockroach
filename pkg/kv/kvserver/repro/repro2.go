// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package repro

import (
	"context"
	"reflect"

	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/redact"
)

type Node = NodeT[struct{}]

type NodeT[T any] struct {
	nodeImpl
	data T
}

func (n *NodeT[T]) Data() *T {
	return &n.data
}

type nodeImpl struct {
	step StepID
	name redact.SafeString
	dep  *nodeImpl
	done chan struct{}
}

func (n *nodeImpl) IsNext() bool {
	return (n.dep == nil || n.dep.IsDone()) && !n.IsDone()
}

func (n *nodeImpl) Once() bool {
	if n.Gate() {
		n.Step()
		return true
	}
	return false
}

func (n *nodeImpl) Gate() bool {
	if dep := n.dep; dep != nil {
		<-dep.Done()
	}
	return !n.IsDone()
}

func (n *nodeImpl) Step() {
	log.Dev.Warningf(context.Background(), "REPRO[%d]: %s", n.step, n.name)
	close(n.done)
}

func (n *nodeImpl) Done() <-chan struct{} {
	return n.done
}

func (n *nodeImpl) IsDone() bool {
	select {
	case <-n.done:
		return true
	default:
		return false
	}
}

type impl interface {
	self() *nodeImpl
}

func (n *nodeImpl) self() *nodeImpl {
	return n
}

func Struct(seqPtr any) {
	val := reflect.ValueOf(seqPtr).Elem()
	typ := val.Type()

	var prev *nodeImpl
	for i := range val.NumField() {
		next := val.Field(i).Addr().Interface().(impl).self()
		*next = nodeImpl{
			step: StepID(i),
			name: redact.SafeString(typ.Field(i).Name),
			dep:  prev,
			done: make(chan struct{}),
		}
		prev = next
	}
}

func makeNoOp(seqPtr any) {
	val := reflect.ValueOf(seqPtr).Elem()
	for i := range val.NumField() {
		close(val.Field(i).Addr().Interface().(impl).self().done)
	}
}

var S struct {
	Init            Node
	GCStart         Node
	GCSnapshotTaken Node
	SplitStart      Node
	SplitApplied    Node
	RebalancedRHS   Node
	FirstClearRange Node
	AdmitFirstCR    Node
	PreWriteToRHS   Node
	WriteToRHS      Node
	GCSent          Node
	ReadRHS         Node
	ConsistencyRHS  Node
}

func init() {
	Struct(&S)
	makeNoOp(&S)
}
