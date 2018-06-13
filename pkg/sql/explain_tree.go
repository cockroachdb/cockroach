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

package sql

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// planToTree uses a stack to "parse" the planObserver's sequence of calls
// into a tree which can be easily serialized as JSON or Protobuf.
//
// e.g. for the plan
//
//   join [cond: t1.a = t2.b]
//     scan [table: t1]
//     scan [table: t2]
//
// the observer would call
//
//   enterNode join         // push onto stack
//   enterNode scan         // push onto stack
//   attr table: t1         // add attribute
//   leaveNode              // pop scan node; add it as a child of join node
//   enterNode scan         // push onto stack
//   attr table: t2         // add attribute
//   leaveNode              // pop scan node; add it as a child of join node
//   expr cond: t1.a = t2.b // add attribute
//   leaveNode              // keep root node on stack (base case because it's the root).
//
// and planToTree would return the join node.
func planToTree(ctx context.Context, top planTop) *roachpb.ExplainTreePlanNode {
	nodeStack := &planNodeStack{}
	observer := planObserver{
		enterNode: func(ctx context.Context, nodeName string, plan planNode) (bool, error) {
			nodeStack.push(&roachpb.ExplainTreePlanNode{
				Name: nodeName,
			})
			return true, nil
		},
		expr: func(_ observeVerbosity, nodeName, fieldName string, n int, expr tree.Expr) {
			if expr == nil {
				return
			}
			stackTop := nodeStack.peek()
			stackTop.Attrs = append(stackTop.Attrs, &roachpb.ExplainTreePlanNode_Attr{
				Key:   fieldName,
				Value: expr.String(),
			})
		},
		attr: func(nodeName, fieldName, attr string) {
			stackTop := nodeStack.peek()
			stackTop.Attrs = append(stackTop.Attrs, &roachpb.ExplainTreePlanNode_Attr{
				Key:   fieldName,
				Value: attr,
			})
		},
		leaveNode: func(nodeName string, plan planNode) error {
			if nodeStack.len() == 1 {
				return nil
			}
			poppedNode := nodeStack.pop()
			newStackTop := nodeStack.peek()
			newStackTop.Children = append(newStackTop.Children, poppedNode)
			return nil
		},
	}

	if err := populateEntriesForObserver(ctx, top.plan, top.subqueryPlans, observer, true /* returnError */); err != nil {
		panic(fmt.Sprintf("error while walking plan to save it to statement stats: %s", err.Error()))
	}
	return nodeStack.peek()
}

type planNodeStack struct {
	stack []*roachpb.ExplainTreePlanNode
}

func (ns *planNodeStack) push(node *roachpb.ExplainTreePlanNode) {
	ns.stack = append(ns.stack, node)
}

func (ns *planNodeStack) pop() *roachpb.ExplainTreePlanNode {
	if len(ns.stack) == 0 {
		return nil
	}
	stackTop := ns.stack[len(ns.stack)-1]
	ns.stack = ns.stack[0 : len(ns.stack)-1]
	return stackTop
}

func (ns *planNodeStack) peek() *roachpb.ExplainTreePlanNode {
	if len(ns.stack) == 0 {
		return nil
	}
	return ns.stack[len(ns.stack)-1]
}

func (ns *planNodeStack) len() int {
	return len(ns.stack)
}
