// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package optbuilder

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
)

func TestStatementTree(t *testing.T) {
	type cmd uint16
	const (
		push cmd = 1 << iota
		pop
		mut
		simple
		post
		getInit
		init
		getInitDeferred
		initDeferred
		t1
		t2
		fail
	)
	type testCase struct {
		cmds []cmd
	}
	testCases := []testCase{
		// 0.
		// Push, CanMutateTable(t1, simpleInsert), Pop
		{
			cmds: []cmd{
				push,
				mut | t1 | simple,
				pop,
			},
		},
		// 1.
		// Push, CanMutateTable(t1, default), Pop
		{
			cmds: []cmd{
				push,
				mut | t1,
				pop,
			},
		},
		// 2.
		// Push, CanMutateTable(t1, default), CanMutateTable(t2, default), Pop
		{
			cmds: []cmd{
				push,
				mut | t1,
				mut | t2,
				pop,
			},
		},
		// 3.
		// Push, CanMutateTable(t1, default), CanMutateTable(t1, simpleInsert) FAIL
		{
			cmds: []cmd{
				push,
				mut | t1,
				mut | t1 | simple | fail,
			},
		},
		// 4.
		// Push, CanMutateTable(t1, simpleInsert), CanMutateTable(t1, default) FAIL
		{
			cmds: []cmd{
				push,
				mut | t1 | simple,
				mut | t1 | fail,
			},
		},
		// 5.
		// Push, CanMutateTable(t1, default), CanMutateTable(t2, default), CanMutateTable(t1, default) FAIL
		{
			cmds: []cmd{
				push,
				mut | t1,
				mut | t2,
				mut | t1 | fail,
			},
		},
		// 6.
		// Push
		//     CanMutateTable(t1, default)
		//     Push
		//         CanMutateTable(t1, default) FAIL
		{
			cmds: []cmd{
				push,
				mut | t1,
				push,
				mut | t1 | fail,
			},
		},
		// 7.
		// Push
		//     CanMutateTable(t1, default)
		//     Push
		//         CanMutateTable(t1, simpleInsert) FAIL
		{
			cmds: []cmd{
				push,
				mut | t1,
				push,
				mut | t1 | simple | fail,
			},
		},
		// 8.
		// Push
		//     CanMutateTable(t1, simpleInsert)
		//     Push
		//         CanMutateTable(t1, default) FAIL
		{
			cmds: []cmd{
				push,
				mut | t1 | simple,
				push,
				mut | t1 | fail,
			},
		},
		// 9.
		// Push
		//     CanMutateTable(t1, simpleInsert)
		//     Push
		//         CanMutateTable(t1, simpleInsert)
		//         Push
		//             CanMutateTable(t1, simpleInsert)
		//         Pop
		//     Pop
		// Pop
		{
			cmds: []cmd{
				push,
				mut | t1 | simple,
				push,
				mut | t1 | simple,
				push,
				mut | t1 | simple,
				pop,
				pop,
				pop,
			},
		},
		// 10.
		// Push
		//     CanMutateTable(t1, simpleInsert)
		//     Push
		//         CanMutateTable(t1, simpleInsert)
		//         Push
		//             CanMutateTable(t1, default) FAIL
		{
			cmds: []cmd{
				push,
				mut | t1 | simple,
				push,
				mut | t1 | simple,
				push,
				mut | t1 | fail,
			},
		},
		// 11.
		// Push
		//     CanMutateTable(t1, simpleInsert)
		//     Push
		//         CanMutateTable(t2, simpleInsert)
		//         Push
		//             CanMutateTable(t1, default) FAIL
		{
			cmds: []cmd{
				push,
				mut | t1 | simple,
				push,
				mut | t2 | simple,
				push,
				mut | t1 | fail,
			},
		},
		// 12.
		// Push
		//     CanMutateTable(t1, default)
		//     Push
		//         CanMutateTable(t2, simpleInsert)
		//         Push
		//             CanMutateTable(t1, simpleInsert) FAIL
		{
			cmds: []cmd{
				push,
				mut | t1,
				push,
				mut | t2 | simple,
				push,
				mut | t1 | simple | fail,
			},
		},
		// 13.
		// Push
		//     CanMutateTable(t1, simpleInsert)
		//     Push
		//         CanMutateTable(t1, simpleInsert)
		//     Pop
		//     CanMutateTable(t1, default) FAIL
		{
			cmds: []cmd{
				push,
				mut | t1 | simple,
				push,
				mut | t1 | simple,
				pop,
				mut | t1 | fail,
			},
		},
		// 14.
		// Push
		//     CanMutateTable(t1, simpleInsert)
		//     Push
		//         CanMutateTable(t1, simpleInsert)
		//     Pop
		//     Push
		//         CanMutateTable(t1, simpleInsert)
		//     Pop
		// Pop
		{
			cmds: []cmd{
				push,
				mut | t1 | simple,
				push,
				mut | t1 | simple,
				pop,
				push,
				mut | t1 | simple,
				pop,
				pop,
			},
		},
		// 15.
		// Push
		//     CanMutateTable(t1, simpleInsert)
		//     Push
		//         CanMutateTable(t1, simpleInsert)
		//         Push
		//             CanMutateTable(t1, simpleInsert)
		//         Pop
		//     Pop
		// Pop
		{
			cmds: []cmd{
				push,
				mut | t1 | simple,
				push,
				mut | t1 | simple,
				push,
				mut | t1 | simple,
				pop,
				pop,
				pop,
			},
		},
		// 16.
		// Push
		//     Push
		//         CanMutateTable(t1, default)
		//     Pop
		//     Push
		//         CanMutateTable(t1, default)
		//     Pop
		// Pop
		{
			cmds: []cmd{
				push,
				push,
				mut | t1,
				pop,
				push,
				mut | t1,
				pop,
				pop,
			},
		},
		// 17.
		// Push
		//     Push
		//         CanMutateTable(t1, simpleInsert)
		//     Pop
		//     CanMutateTable(t1, default) FAIL
		{
			cmds: []cmd{
				push,
				push,
				mut | t1 | simple,
				pop,
				mut | t1 | fail,
			},
		},
		// 18.
		// Push
		//     Push
		//         CanMutateTable(t1, default)
		//     Pop
		//     CanMutateTable(t1, simpleInsert) FAIL
		{
			cmds: []cmd{
				push,
				push,
				mut | t1,
				pop,
				mut | t1 | simple | fail,
			},
		},
		// 19.
		// Push
		//     Push
		//         Push
		//             CanMutateTable(t1, default)
		//         Pop
		//         CanMutateTable(t1, simpleInsert) FAIL
		{
			cmds: []cmd{
				push,
				push,
				push,
				mut | t1,
				pop,
				mut | t1 | simple | fail,
			},
		},
		// 20.
		// Push
		//     Push
		//         CanMutateTable(t2, simpleInsert)
		//         Push
		//             CanMutateTable(t2, simpleInsert)
		//         Pop
		//         Push
		//             CanMutateTable(t2, simpleInsert)
		//         Pop
		//         Push
		//             CanMutateTable(t1, simpleInsert)
		//         Pop
		//         Push
		//             CanMutateTable(t2, simpleInsert)
		//         Pop
		//     Pop
		//     CanMutateTable(t2, simpleInsert)
		//     CanMutateTable(t1, simpleInsert)
		//     CanMutateTable(t1, default) FAIL
		{
			cmds: []cmd{
				push,
				push,
				mut | t2 | simple,
				push,
				mut | t2 | simple,
				pop,
				push,
				mut | t2 | simple,
				pop,
				push,
				mut | t1 | simple,
				pop,
				push,
				mut | t2 | simple,
				pop,
				pop,
				mut | t2 | simple,
				mut | t1 | simple,
				mut | t1 | fail,
			},
		},
		// 21.
		// Push
		//     CanMutateTable(t1, simpleInsert)
		//     Push
		//         CanMutateTable(t2, default)
		//         CanMutateTable(t1, default, post) FAIL
		{
			cmds: []cmd{
				push,
				mut | t1 | simple,
				push,
				mut | t2,
				mut | t1 | post | fail,
			},
		},
		// 22.
		// Push
		//     Push
		//         CanMutateTable(t1, default)
		//         CanMutateTable(t2, default, post)
		//     Pop
		//     CanMutateTable(t2, simpleInsert) FAIL
		{
			cmds: []cmd{
				push,
				push,
				mut | t1,
				mut | t2 | post,
				pop,
				mut | t1 | simple | fail,
			},
		},
		// 23.
		// Push
		//     Push
		//         CanMutateTable(t1, default)
		//         CanMutateTable(t2, default, post)
		//     Pop
		// Pop
		{
			cmds: []cmd{
				push,
				push,
				mut | t1,
				mut | t2 | post,
				pop,
				pop,
			},
		},
		// 24.
		// Original:
		// Push
		//     CanMutateTable(t1, default)
		//     GetInitFnForPostQuery()
		// Pop
		//
		// Post-Query:
		// initFn()
		// Push
		//     CanMutateTable(t1, default)
		// Pop
		{
			cmds: []cmd{
				push,
				mut | t1,
				getInit,
				pop,
				init,
				push,
				mut | t1,
				pop,
			},
		},
		// 25.
		// Original:
		// Push
		//     Push
		//         CanMutateTable(t1, default)
		//     Pop
		//     GetInitFnForPostQuery()
		// Pop
		//
		// Post-Query:
		// initFn()
		// Push
		//     CanMutateTable(t1, default)
		// Pop
		{
			cmds: []cmd{
				push,
				push,
				mut | t1,
				pop,
				getInit,
				pop,
				init,
				push,
				mut | t1,
				pop,
			},
		},
		// 26.
		// Original:
		// Push
		//     Push
		//         CanMutateTable(t1, default)
		//     Pop
		//     Push
		//         GetInitFnForPostQuery()
		//     Pop
		// Pop
		//
		// Post-Query:
		// initFn()
		// Push
		//     CanMutateTable(t1, default)
		// Pop
		{
			cmds: []cmd{
				push,
				push,
				mut | t1,
				pop,
				push,
				getInit,
				pop,
				pop,
				init,
				push,
				mut | t1,
				pop,
			},
		},
		// 27.
		// Original:
		// Push
		//     CanMutateTable(t1, default)
		//     Push
		//         GetInitFnForPostQuery()
		//     Pop
		// Pop
		//
		// Post-Query:
		// initFn()
		// Push
		//     CanMutateTable(t1, default) FAIL
		{
			cmds: []cmd{
				push,
				mut | t1,
				push,
				getInit,
				pop,
				pop,
				init,
				push,
				mut | t1 | fail,
			},
		},
		// 28.
		// Original:
		// Push
		//     Push
		//         GetInitFnForPostQuery()
		//     Pop
		//     CanMutateTable(t1, default)
		// Pop
		//
		// Post-Query:
		// initFn()
		// Push
		//     CanMutateTable(t1, default) FAIL
		{
			cmds: []cmd{
				push,
				push,
				getInit,
				pop,
				mut | t1,
				pop,
				init,
				push,
				mut | t1 | fail,
			},
		},
		// 29.
		// Original:
		// Push
		//     CanMutateTable(t1, default)
		//     Push
		//         Push
		//             GetInitFnForPostQuery()
		//         Pop
		//     Pop
		// Pop
		//
		// Post-Query:
		// initFn()
		// Push
		//     CanMutateTable(t1, default) FAIL
		{
			cmds: []cmd{
				push,
				mut | t1,
				push,
				push,
				getInit,
				pop,
				pop,
				pop,
				init,
				push,
				mut | t1 | fail,
			},
		},
		// 30.
		// Pointer-based capture sees late-arriving mutations.
		// Original:
		// Push
		//     CanMutateTable(t1, default)
		//     GetInitFnForDeferredRoutine()
		//     CanMutateTable(t2, default) <-- added after capture
		// Pop
		//
		// Deferred Routine:
		// initFn()
		// Push
		//     CanMutateTable(t2, default) FAIL <-- visible via pointer
		{
			cmds: []cmd{
				push,
				mut | t1,
				getInitDeferred,
				mut | t2,
				pop,
				initDeferred,
				push,
				mut | t2 | fail,
			},
		},
		// 31.
		// Current-level mutations visible in deferred routine.
		// e.g. UPDATE t SET x = my_udf() where my_udf body also mutates t.
		// Original:
		// Push
		//     CanMutateTable(t1, default)
		//     GetInitFnForDeferredRoutine()
		// Pop
		//
		// Deferred Routine:
		// initFn()
		// Push
		//     CanMutateTable(t1, default) FAIL
		{
			cmds: []cmd{
				push,
				mut | t1,
				getInitDeferred,
				pop,
				initDeferred,
				push,
				mut | t1 | fail,
			},
		},
		// 32.
		// Different-table mutations allowed.
		// Original:
		// Push
		//     CanMutateTable(t1, default)
		//     GetInitFnForDeferredRoutine()
		// Pop
		//
		// Deferred Routine:
		// initFn()
		// Push
		//     CanMutateTable(t2, default)
		// Pop
		{
			cmds: []cmd{
				push,
				mut | t1,
				getInitDeferred,
				pop,
				initDeferred,
				push,
				mut | t2,
				pop,
			},
		},
		// 33.
		// Nested ancestor mutations visible.
		// Original:
		// Push
		//     CanMutateTable(t1, default)
		//     Push
		//         GetInitFnForDeferredRoutine()
		//     Pop
		// Pop
		//
		// Deferred Routine:
		// initFn()
		// Push
		//     CanMutateTable(t1, default) FAIL
		{
			cmds: []cmd{
				push,
				mut | t1,
				push,
				getInitDeferred,
				pop,
				pop,
				initDeferred,
				push,
				mut | t1 | fail,
			},
		},
		// 34.
		// Sibling CTE mutations visible via pointer capture.
		// Original:
		// Push
		//     Push
		//         GetInitFnForDeferredRoutine()
		//     Pop
		//     CanMutateTable(t1, default) <-- sibling mutation after capture
		// Pop
		//
		// Deferred Routine:
		// initFn()
		// Push
		//     CanMutateTable(t1, default) FAIL
		{
			cmds: []cmd{
				push,
				push,
				getInitDeferred,
				pop,
				mut | t1,
				pop,
				initDeferred,
				push,
				mut | t1 | fail,
			},
		},
		// 35.
		// Child mutations merged via Pop are NOT visible to deferred routine.
		// The child's mutation is a sibling of the deferred routine (e.g. two
		// CTEs), so it should not conflict.
		// e.g. WITH cte1 AS (SELECT my_udf()), cte2 AS (UPDATE t ...) ...
		// Original:
		// Push
		//     GetInitFnForDeferredRoutine()
		//     Push
		//         CanMutateTable(t1, default)
		//     Pop  <-- t1 merges into parent's children*, not simpleInsert/generalMutation
		// Pop
		//
		// Deferred Routine:
		// initFn()
		// Push
		//     CanMutateTable(t1, default)  <-- OK, sibling mutation
		// Pop
		{
			cmds: []cmd{
				push,
				getInitDeferred,
				push,
				mut | t1,
				pop,
				pop,
				initDeferred,
				push,
				mut | t1,
				pop,
			},
		},
		// 36.
		// simpleInsert+simpleInsert on same table OK.
		// Original:
		// Push
		//     CanMutateTable(t1, simpleInsert)
		//     GetInitFnForDeferredRoutine()
		// Pop
		//
		// Deferred Routine:
		// initFn()
		// Push
		//     CanMutateTable(t1, simpleInsert)
		// Pop
		{
			cmds: []cmd{
				push,
				mut | t1 | simple,
				getInitDeferred,
				pop,
				initDeferred,
				push,
				mut | t1 | simple,
				pop,
			},
		},
		// 37.
		// generalMutation+simpleInsert on same table FAIL.
		// Original:
		// Push
		//     CanMutateTable(t1, default)
		//     GetInitFnForDeferredRoutine()
		// Pop
		//
		// Deferred Routine:
		// initFn()
		// Push
		//     CanMutateTable(t1, simpleInsert) FAIL
		{
			cmds: []cmd{
				push,
				mut | t1,
				getInitDeferred,
				pop,
				initDeferred,
				push,
				mut | t1 | simple | fail,
			},
		},
		// 38.
		// Empty stack returns nil initFn.
		//
		// Deferred Routine:
		// initFn() == nil => empty statementTree
		// Push
		//     CanMutateTable(t1, default)
		// Pop
		{
			cmds: []cmd{
				getInitDeferred,
				initDeferred,
				push,
				mut | t1,
				pop,
			},
		},
		// 39.
		// Sibling body statements within deferred routine don't conflict.
		// Original:
		// Push
		//     CanMutateTable(t1, default)
		//     GetInitFnForDeferredRoutine()
		// Pop
		//
		// Deferred Routine:
		// initFn()
		// Push
		//     Push
		//         CanMutateTable(t2, default)
		//     Pop
		//     Push
		//         CanMutateTable(t2, default)
		//     Pop
		// Pop
		{
			cmds: []cmd{
				push,
				mut | t1,
				getInitDeferred,
				pop,
				initDeferred,
				push,
				push,
				mut | t2,
				pop,
				push,
				mut | t2,
				pop,
				pop,
			},
		},
		// 40.
		// Two separate deferred routine invocations mutating the same table.
		// The deferred routines are siblings, so they should not conflict.
		// Original:
		// Push
		//     GetInitFnForDeferredRoutine()  -- routine 1
		//     GetInitFnForDeferredRoutine()  -- routine 2
		// Pop
		//
		// Deferred Routine 1:
		// initFn()
		// Push
		//     CanMutateTable(t1, default)
		// Pop
		//
		// Deferred Routine 2:
		// initFn()
		// Push
		//     CanMutateTable(t1, default)
		// Pop
		{
			cmds: []cmd{
				push,
				getInitDeferred,
				getInitDeferred,
				pop,
				initDeferred,
				push,
				mut | t1,
				pop,
				initDeferred,
				push,
				mut | t1,
				pop,
			},
		},
	}

	for i, tc := range testCases {
		var mt statementTree
		var pqTreeFn func() statementTree
		var deferredTreeFns []func() statementTree
		for j, c := range tc.cmds {
			switch {
			case c&push == push:
				mt.Push()

			case c&pop == pop:
				mt.Pop()

			case c&getInit == getInit:
				if pqTreeFn != nil {
					t.Fatalf("test case %d: GetInitFnForPostQuery called twice", i)
				}
				pqTreeFn = mt.GetInitFnForPostQuery()

			case c&init == init:
				if pqTreeFn == nil {
					mt = statementTree{}
				} else {
					mt = pqTreeFn()
				}

			case c&getInitDeferred == getInitDeferred:
				deferredTreeFns = append(deferredTreeFns, mt.GetInitFnForDeferredRoutine())

			case c&initDeferred == initDeferred:
				if len(deferredTreeFns) == 0 {
					mt = statementTree{}
				} else {
					fn := deferredTreeFns[0]
					deferredTreeFns = deferredTreeFns[1:]
					if fn == nil {
						mt = statementTree{}
					} else {
						mt = fn()
					}
				}

			case c&mut == mut:
				var tabID cat.StableID
				switch {
				case c&t1 == t1:
					tabID = 1
				case c&t2 == t2:
					tabID = 2
				}

				typ := generalMutation
				if c&simple == simple {
					typ = simpleInsert
				}

				isPost := false
				if c&post == post {
					isPost = true
				}

				res := mt.CanMutateTable(tabID, typ, isPost)

				expected := c&fail != fail
				if res != expected {
					t.Fatalf("test case %d: expected: %v at command %d, got: %v", i, expected, j, res)
				}
			}
		}
	}
}
