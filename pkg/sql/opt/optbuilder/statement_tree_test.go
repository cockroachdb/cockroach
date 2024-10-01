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
	type cmd uint8
	const (
		push cmd = 1 << iota
		pop
		mut
		simple
		post
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
	}

	for i, tc := range testCases {
		var mt statementTree
		for j, c := range tc.cmds {
			switch {
			case c&push == push:
				mt.Push()

			case c&pop == pop:
				mt.Pop()

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
