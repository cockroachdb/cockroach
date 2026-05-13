// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package eval_test

import (
	"context"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/parserutils"
	_ "github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

// TestDomainCheckConstraintDataRace verifies that concurrent evaluation of
// domain CHECK constraints does not race. The CHECK expression uses a function
// call (abs) that does not reference VALUE, exercising the code path where
// re-parsing is necessary to avoid sharing AST nodes across goroutines.
func TestDomainCheckConstraintDataRace(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Inject the parser so that parserutils.ParseExpr works (we can't import
	// pkg/sql due to import cycles, so do the injection manually).
	origParseExpr := parserutils.ParseExpr
	parserutils.ParseExpr = parser.ParseExpr
	t.Cleanup(func() { parserutils.ParseExpr = origParseExpr })

	ctx := context.Background()

	// Build a domain type with a CHECK constraint whose parsed expression
	// contains a function call (abs) that does NOT reference VALUE. This
	// means SimpleVisit will not copy that subtree, leaving it shared
	// across all concurrent evaluations.
	domainType := types.MakeDomain(types.Int, 100000 /* typeOID */, 100001 /* arrayTypeOID */)
	domainType.TypeMeta = types.UserDefinedTypeMetadata{
		Name: &types.UserDefinedTypeName{Name: "d_racetest"},
		DomainData: &types.DomainMetadata{
			BaseType: types.Int,
			CheckConstraints: []types.DomainCheckConstraint{
				{
					Name: "d_racetest_check",
					Expr: "VALUE > 0 AND abs(1) > 0",
				},
			},
		},
	}

	const goroutines = 8
	const iterations = 500

	var wg sync.WaitGroup
	wg.Add(goroutines)
	errCh := make(chan error, goroutines)

	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			evalCtx := eval.NewTestingEvalContext(
				cluster.MakeTestingClusterSettings(),
			)
			defer evalCtx.Stop(ctx)
			for i := 0; i < iterations; i++ {
				d := tree.NewDInt(tree.DInt(i + 1))
				if err := eval.ValidateDomainConstraints(
					ctx, evalCtx, d, domainType,
				); err != nil {
					errCh <- err
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("unexpected error: %v", err)
	}
}
