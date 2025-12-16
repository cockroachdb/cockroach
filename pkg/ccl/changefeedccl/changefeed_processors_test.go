// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package changefeedccl

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strings"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/build/bazel"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/redact"
	"github.com/stretchr/testify/require"
)

func TestSaveRateLimiter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx := context.Background()

	intervals := map[string]time.Duration{
		"positive interval":   30 * time.Second,
		"negative interval":   -30 * time.Second,
		"zero interval":       0,
		"very small interval": time.Nanosecond,
	}

	jitters := map[string]float64{
		"positive jitter":   0.1,
		"negative jitter":   -0.1,
		"zero jitter":       0,
		"very small jitter": 1e-12,
	}

	for intervalName, interval := range intervals {
		for jitterName, jitter := range jitters {
			t.Run(fmt.Sprintf("%s with %s", intervalName, jitterName), func(t *testing.T) {
				// Set up the mock clock for testing.
				now := timeutil.Now()
				clock := timeutil.NewManualTime(now)

				// Create the save rate limiter.
				l, err := newSaveRateLimiter(saveRateConfig{
					name: "test",
					intervalName: func() redact.SafeValue {
						return redact.SafeString(intervalName)
					},
					interval: func() time.Duration {
						return interval
					},
					jitter: func() float64 {
						return jitter
					},
				}, clock)
				require.NoError(t, err)

				// A non-positive interval indicates that saving is disabled so we only
				// need to test that we can't save at all.
				if interval <= 0 {
					require.False(t, l.canSave(ctx))
					clock.Advance(24 * time.Hour)
					require.False(t, l.canSave(ctx))
					return
				}

				// We can do one save right away if the interval.
				require.True(t, l.canSave(ctx))
				l.doneSave(0 /* saveDuration */)

				// Can't immediately save again.
				require.False(t, l.canSave(ctx))

				// Make sure interval and jitter works correctly.
				var maxJitter time.Duration
				if jitter > 0 {
					maxJitter = time.Duration(jitter * float64(interval))
				}
				clock.Advance(interval + maxJitter)
				require.True(t, l.canSave(ctx))

				// Set the save duration to something high to make sure we can't save
				// due to high average save duration.
				l.doneSave(time.Hour)
				clock.Advance(interval + maxJitter)
				require.False(t, l.canSave(ctx))
				clock.Advance(time.Hour - (interval + maxJitter))
				require.True(t, l.canSave(ctx))

				// Set the save duration to something even higher to make sure the
				// average algorithm works.
				l.doneSave(2 * time.Hour)
				clock.Advance(time.Hour)
				require.False(t, l.canSave(ctx))
				clock.Advance(time.Hour)
				require.True(t, l.canSave(ctx))
				l.doneSave(0 /* saveDuration */)
			})
		}
	}
}

func TestSaveRateLimiterError(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	name := redact.SafeString("test")
	intervalName := func() redact.SafeValue {
		return redact.SafeString("interval")
	}
	interval := func() time.Duration {
		return 30 * time.Second
	}

	for testName, tc := range map[string]struct {
		config      saveRateConfig
		expectedErr string
	}{
		"missing name": {
			config: saveRateConfig{
				intervalName: intervalName,
				interval:     interval,
			},
			expectedErr: "name is required",
		},
		"missing interval name": {
			config: saveRateConfig{
				name:     name,
				interval: interval,
			},
			expectedErr: "interval name is required",
		},
		"missing interval": {
			config: saveRateConfig{
				name:         name,
				intervalName: intervalName,
			},
			expectedErr: "interval is required",
		},
	} {
		t.Run(testName, func(t *testing.T) {
			if tc.expectedErr == "" {
				t.Fatal("missing expected error")
			}
			_, err := newSaveRateLimiter(tc.config, timeutil.DefaultTimeSource{})
			require.ErrorContains(t, err, tc.expectedErr)
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
