// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfrapb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/stretchr/testify/require"
)

// TestSetupSpansAndFrontier tests that the setupSpansAndFrontier function
// correctly sets up frontier for the changefeed aggregator frontier.
func TestSetupSpansAndFrontier(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	statementTime := hlc.Timestamp{WallTime: 10}
	for _, tc := range []struct {
		name             string
		expectedFrontier hlc.Timestamp
		watches          []execinfrapb.ChangeAggregatorSpec_Watch
	}{
		{
			name:             "new initial scan",
			expectedFrontier: hlc.Timestamp{},
			watches: []execinfrapb.ChangeAggregatorSpec_Watch{
				{
					Span:            roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					InitialResolved: hlc.Timestamp{},
				},
				{
					Span:            roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
					InitialResolved: hlc.Timestamp{},
				},
				{
					Span:            roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
					InitialResolved: hlc.Timestamp{},
				},
			},
		},
		{
			name:             "incomplete initial scan with non-empty initial resolved in the middle",
			expectedFrontier: hlc.Timestamp{},
			watches: []execinfrapb.ChangeAggregatorSpec_Watch{
				{
					Span:            roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					InitialResolved: hlc.Timestamp{WallTime: 5},
				},
				{
					Span:            roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
					InitialResolved: hlc.Timestamp{},
				},
				{
					Span:            roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
					InitialResolved: hlc.Timestamp{WallTime: 20},
				},
			},
		},
		{
			name:             "incomplete initial scan with non-empty initial resolved in the front",
			expectedFrontier: hlc.Timestamp{},
			watches: []execinfrapb.ChangeAggregatorSpec_Watch{
				{
					Span:            roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					InitialResolved: hlc.Timestamp{},
				},
				{
					Span:            roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
					InitialResolved: hlc.Timestamp{WallTime: 10},
				},
				{
					Span:            roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
					InitialResolved: hlc.Timestamp{WallTime: 20},
				},
			},
		},
		{
			name:             "incomplete initial scan with empty initial resolved in the end",
			expectedFrontier: hlc.Timestamp{},
			watches: []execinfrapb.ChangeAggregatorSpec_Watch{
				{
					Span:            roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					InitialResolved: hlc.Timestamp{WallTime: 10},
				},
				{
					Span:            roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
					InitialResolved: hlc.Timestamp{WallTime: 20},
				},
				{
					Span:            roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
					InitialResolved: hlc.Timestamp{},
				},
			},
		},
		{
			name:             "complete initial scan",
			expectedFrontier: hlc.Timestamp{WallTime: 5},
			watches: []execinfrapb.ChangeAggregatorSpec_Watch{
				{
					Span:            roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")},
					InitialResolved: hlc.Timestamp{WallTime: 10},
				},
				{
					Span:            roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")},
					InitialResolved: hlc.Timestamp{WallTime: 20},
				},
				{
					Span:            roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")},
					InitialResolved: hlc.Timestamp{WallTime: 5},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ca := &changeAggregator{
				spec: execinfrapb.ChangeAggregatorSpec{
					Watches: tc.watches,
				},
			}
			ca.spec.Feed.StatementTime = statementTime
			_, err := ca.setupSpansAndFrontier()
			require.NoError(t, err)
			require.Equal(t, tc.expectedFrontier, ca.frontier.Frontier())
		})
	}
}

type checkpointSpan struct {
	span roachpb.Span
	ts   hlc.Timestamp
}

type checkpointSpans []checkpointSpan

// TestSetupSpansAndFrontierWithNewSpec tests that the setupSpansAndFrontier
// function correctly sets up frontier for the changefeed aggregator frontier
// with the new ChangeAggregatorSpec_Watch where initial resolved is not set but
// initial highwater is passed in.
func TestSetupSpansAndFrontierWithNewSpec(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	statementTime := hlc.Timestamp{WallTime: 10}

	for _, tc := range []struct {
		name             string
		initialHighWater hlc.Timestamp
		watches          []execinfrapb.ChangeAggregatorSpec_Watch
		//lint:ignore SA1019 deprecated usage
		checkpoints          execinfrapb.ChangeAggregatorSpec_Checkpoint
		expectedFrontierTs   hlc.Timestamp
		expectedFrontierSpan checkpointSpans
	}{
		{
			name:             "new initial scan",
			initialHighWater: hlc.Timestamp{},
			watches: []execinfrapb.ChangeAggregatorSpec_Watch{
				{Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}},
				{Span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("e")}},
				{Span: roachpb.Span{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")}},
			},
			expectedFrontierTs: hlc.Timestamp{},
			expectedFrontierSpan: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: hlc.Timestamp{}},
				{span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("e")}, ts: hlc.Timestamp{}},
				{span: roachpb.Span{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")}, ts: hlc.Timestamp{}},
			},
		},
		{
			name:             "complete initial scan with empty span level checkpoints",
			initialHighWater: hlc.Timestamp{WallTime: 5},
			watches: []execinfrapb.ChangeAggregatorSpec_Watch{
				{Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}},
				{Span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("e")}},
				{Span: roachpb.Span{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")}},
			},
			expectedFrontierTs: hlc.Timestamp{WallTime: 5},
			expectedFrontierSpan: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: hlc.Timestamp{WallTime: 5}},
				{span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("e")}, ts: hlc.Timestamp{WallTime: 5}},
				{span: roachpb.Span{Key: roachpb.Key("g"), EndKey: roachpb.Key("h")}, ts: hlc.Timestamp{WallTime: 5}},
			},
		},
		{
			name:             "initial scan in progress with span level checkpoints and checkpoint ts",
			initialHighWater: hlc.Timestamp{},
			watches: []execinfrapb.ChangeAggregatorSpec_Watch{
				{Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}},
				{Span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}},
				{Span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
			},
			//lint:ignore SA1019 deprecated usage
			checkpoints: execinfrapb.ChangeAggregatorSpec_Checkpoint{
				Spans:     []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, {Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
				Timestamp: hlc.Timestamp{WallTime: 5},
			},
			expectedFrontierTs: hlc.Timestamp{},
			expectedFrontierSpan: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: hlc.Timestamp{WallTime: 5}},
				{span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}, ts: hlc.Timestamp{}},
				{span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}, ts: hlc.Timestamp{WallTime: 5}},
			},
		},
		{
			name:             "initial scan with span level checkpoints but no checkpoint ts",
			initialHighWater: hlc.Timestamp{},
			watches: []execinfrapb.ChangeAggregatorSpec_Watch{
				{Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}},
				{Span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}},
				{Span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
			},
			//lint:ignore SA1019 deprecated usage
			checkpoints: execinfrapb.ChangeAggregatorSpec_Checkpoint{
				Spans: []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, {Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
			},
			expectedFrontierTs: hlc.Timestamp{},
			expectedFrontierSpan: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: statementTime},
				{span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}, ts: hlc.Timestamp{}},
				{span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}, ts: statementTime},
			},
		},
		{
			name:             "schema backfill with span level checkpoints and checkpoint ts",
			initialHighWater: hlc.Timestamp{WallTime: 5},
			watches: []execinfrapb.ChangeAggregatorSpec_Watch{
				{Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}},
				{Span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}},
				{Span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
			},
			//lint:ignore SA1019 deprecated usage
			checkpoints: execinfrapb.ChangeAggregatorSpec_Checkpoint{
				Spans:     []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, {Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
				Timestamp: hlc.Timestamp{WallTime: 7},
			},
			expectedFrontierTs: hlc.Timestamp{WallTime: 5},
			expectedFrontierSpan: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: hlc.Timestamp{WallTime: 7}},
				{span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}, ts: hlc.Timestamp{WallTime: 5}},
				{span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}, ts: hlc.Timestamp{WallTime: 7}},
			},
		},
		{
			name:             "schema backfill with span level checkpoints but no checkpoint ts",
			initialHighWater: hlc.Timestamp{WallTime: 5},
			watches: []execinfrapb.ChangeAggregatorSpec_Watch{
				{Span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}},
				{Span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}},
				{Span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
			},
			//lint:ignore SA1019 deprecated usage
			checkpoints: execinfrapb.ChangeAggregatorSpec_Checkpoint{
				Spans: []roachpb.Span{{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, {Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}},
			},
			expectedFrontierSpan: checkpointSpans{
				{span: roachpb.Span{Key: roachpb.Key("a"), EndKey: roachpb.Key("b")}, ts: hlc.Timestamp{WallTime: 5}.Next()},
				{span: roachpb.Span{Key: roachpb.Key("b"), EndKey: roachpb.Key("c")}, ts: hlc.Timestamp{WallTime: 5}},
				{span: roachpb.Span{Key: roachpb.Key("c"), EndKey: roachpb.Key("d")}, ts: hlc.Timestamp{WallTime: 5}.Next()},
			},
			expectedFrontierTs: hlc.Timestamp{WallTime: 5},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ca := &changeAggregator{}
			ca.spec.Feed.StatementTime = statementTime
			ca.spec.Checkpoint = tc.checkpoints
			ca.spec.Watches = tc.watches
			ca.spec.InitialHighWater = &tc.initialHighWater
			_, err := ca.setupSpansAndFrontier()
			require.NoError(t, err)
			actualFrontierSpan := checkpointSpans{}
			for sp, ts := range ca.frontier.Entries() {
				actualFrontierSpan = append(actualFrontierSpan, checkpointSpan{span: sp, ts: ts})
			}

			require.Equal(t, tc.expectedFrontierSpan, actualFrontierSpan)
			require.Equal(t, tc.expectedFrontierTs, ca.frontier.Frontier())
		})
	}
}

// TestMoveToDrainingInStartFollowedByReturn ensures that all MoveToDraining
// calls in the Start methods of changeAggregator and changeFrontier are
// followed by a return statement (optionally preceded by a cancel() call).
// This prevents bugs where error handling forgets to return after calling
// MoveToDraining, allowing execution to continue with potentially nil values.
func TestMoveToDrainingInStartFollowedByReturn(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Get the path to the source file. In Bazel, we need to use Runfile.
	sourceFile := "pkg/ccl/changefeedccl/changefeed_processors.go"
	if bazel.BuiltWithBazel() {
		var err error
		sourceFile, err = bazel.Runfile(sourceFile)
		require.NoError(t, err)
	}

	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, sourceFile, nil, 0 /* mode */)
	require.NoError(t, err)

	var violations []string

	// Only check Start methods for changeAggregator and changeFrontier.
	targetTypes := map[string]struct{}{
		"changeAggregator": {},
		"changeFrontier":   {},
	}
	seenTypes := make(map[string]struct{})

	ast.Inspect(f, func(n ast.Node) bool {
		// Descend into the file to find function declarations.
		if _, ok := n.(*ast.File); ok {
			return true
		}
		funcDecl, ok := n.(*ast.FuncDecl)
		if !ok {
			return false
		}

		// Check if this is a Start method on one of our target types.
		if funcDecl.Name.Name != "Start" {
			return false
		}
		if funcDecl.Recv == nil || len(funcDecl.Recv.List) == 0 {
			return false
		}

		// Get the receiver type name.
		var recvTypeName string
		recvType := funcDecl.Recv.List[0].Type
		if star, ok := recvType.(*ast.StarExpr); ok {
			if ident, ok := star.X.(*ast.Ident); ok {
				recvTypeName = ident.Name
			}
		}
		if _, ok := targetTypes[recvTypeName]; !ok {
			return false
		}
		if _, seen := seenTypes[recvTypeName]; seen {
			t.Fatalf("found duplicate Start method for %s", recvTypeName)
		}
		seenTypes[recvTypeName] = struct{}{}

		// Now check all MoveToDraining calls within this function.
		ast.Inspect(funcDecl.Body, func(n ast.Node) bool {
			block, ok := n.(*ast.BlockStmt)
			if !ok {
				return true
			}

			for i, stmt := range block.List {
				exprStmt, ok := stmt.(*ast.ExprStmt)
				if !ok {
					continue
				}

				call, ok := exprStmt.X.(*ast.CallExpr)
				if !ok {
					continue
				}

				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "MoveToDraining" {
					continue
				}

				// Get receiver name for cancel() checking.
				var recvName string
				if recvIdent, ok := sel.X.(*ast.Ident); ok {
					recvName = recvIdent.Name
				}

				// Found a MoveToDraining call. Check what follows.
				if i+1 >= len(block.List) {
					pos := fset.Position(stmt.Pos())
					violations = append(violations, fmt.Sprintf(
						"%s:%d: MoveToDraining call not followed by return",
						pos.Filename, pos.Line,
					))
					continue
				}

				// If next statement is a return, we're good.
				nextStmt := block.List[i+1]
				if _, ok := nextStmt.(*ast.ReturnStmt); ok {
					continue
				}

				// Check for receiver.cancel() followed by return.
				if recvName != "" && isMethodCallOn(nextStmt, recvName, "cancel") {
					if i+2 < len(block.List) {
						if _, ok := block.List[i+2].(*ast.ReturnStmt); ok {
							continue
						}
					}
					pos := fset.Position(stmt.Pos())
					violations = append(violations, fmt.Sprintf(
						"%s:%d: MoveToDraining followed by cancel() but no return",
						pos.Filename, pos.Line,
					))
					continue
				}

				pos := fset.Position(stmt.Pos())
				violations = append(violations, fmt.Sprintf(
					"%s:%d: MoveToDraining must be followed by return (or cancel() then return)",
					pos.Filename, pos.Line,
				))
			}

			return true
		})

		// We've already inspected the body with the inner Inspect, so skip it.
		return false
	})

	// Verify we found exactly one Start method for each target type.
	for typeName := range targetTypes {
		if _, seen := seenTypes[typeName]; !seen {
			t.Fatalf("did not find Start method for %s", typeName)
		}
	}

	if len(violations) > 0 {
		t.Errorf("MoveToDraining calls in Start methods must be followed by return:\n%s",
			strings.Join(violations, "\n"))
	}
}

// isMethodCallOn checks if stmt is an expression statement calling receiver.methodName().
func isMethodCallOn(stmt ast.Stmt, receiver, methodName string) bool {
	exprStmt, ok := stmt.(*ast.ExprStmt)
	if !ok {
		return false
	}
	call, ok := exprStmt.X.(*ast.CallExpr)
	if !ok {
		return false
	}
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	recvIdent, ok := sel.X.(*ast.Ident)
	if !ok {
		return false
	}
	return recvIdent.Name == receiver && sel.Sel.Name == methodName
}
