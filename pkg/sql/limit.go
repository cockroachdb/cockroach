// Copyright 2015 The Cockroach Authors.
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
//
// Author: Tamir Duberstein (tamird@gmail.com)

package sql

import (
	"fmt"
	"math"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
)

// limitNode represents a node that limits the number of rows
// returned or only return them past a given number (offset).
type limitNode struct {
	p          *planner
	plan       planNode
	countExpr  parser.TypedExpr
	offsetExpr parser.TypedExpr
	evaluated  bool
	count      int64
	offset     int64
	rowIndex   int64
	explain    explainMode
	debugVals  debugValues
}

// limit constructs a limitNode based on the LIMIT and OFFSET clauses.
func (p *planner) Limit(ctx context.Context, n *parser.Limit) (*limitNode, error) {
	if n == nil || (n.Count == nil && n.Offset == nil) {
		// No LIMIT nor OFFSET; there is nothing special to do.
		return nil, nil
	}

	res := limitNode{p: p}

	data := []struct {
		name string
		src  parser.Expr
		dst  *parser.TypedExpr
	}{
		{"LIMIT", n.Count, &res.countExpr},
		{"OFFSET", n.Offset, &res.offsetExpr},
	}

	for _, datum := range data {
		if datum.src != nil {
			if err := p.parser.AssertNoAggregationOrWindowing(
				datum.src, datum.name, p.session.SearchPath,
			); err != nil {
				return nil, err
			}

			normalized, err := p.analyzeExpr(ctx, datum.src, nil, parser.IndexedVarHelper{}, parser.TypeInt, true, datum.name)
			if err != nil {
				return nil, err
			}
			*datum.dst = normalized
		}
	}
	return &res, nil
}

func (n *limitNode) Start(ctx context.Context) error {
	if err := n.plan.Start(ctx); err != nil {
		return err
	}

	return n.evalLimit()
}

// estimateLimit pre-computes the count and offset fields if they are constants,
// otherwise predefines them to be MaxInt64, 0. Used by index selection.
// This must be called after type checking and constant folding.
func (n *limitNode) estimateLimit() {
	n.count = math.MaxInt64
	n.offset = 0

	// Use simple integer datum if available.
	// The limit can be a simple DInt here either because it was
	// entered as such in the query, or as a result of constant
	// folding prior to type checking.

	if n.countExpr != nil {
		if i, ok := parser.AsDInt(n.countExpr); ok {
			n.count = int64(i)
		}
	}
	if n.offsetExpr != nil {
		if i, ok := parser.AsDInt(n.offsetExpr); ok {
			n.offset = int64(i)
		}
	}
}

// evalLimit evaluates the Count and Offset fields. If Count is missing, the
// value is MaxInt64. If Offset is missing, the value is 0
func (n *limitNode) evalLimit() error {
	n.count = math.MaxInt64
	n.offset = 0

	data := []struct {
		name string
		src  parser.TypedExpr
		dst  *int64
	}{
		{"LIMIT", n.countExpr, &n.count},
		{"OFFSET", n.offsetExpr, &n.offset},
	}

	for _, datum := range data {
		if datum.src != nil {
			dstDatum, err := datum.src.Eval(&n.p.evalCtx)
			if err != nil {
				return err
			}

			if dstDatum == parser.DNull {
				// Use the default value.
				continue
			}

			dstDInt := parser.MustBeDInt(dstDatum)
			val := int64(dstDInt)
			if val < 0 {
				return fmt.Errorf("negative value for %s", datum.name)
			}
			*datum.dst = val
		}
	}
	n.evaluated = true
	return nil
}

func (n *limitNode) Columns() sqlbase.ResultColumns { return n.plan.Columns() }
func (n *limitNode) Values() parser.Datums          { return n.plan.Values() }
func (n *limitNode) Ordering() orderingInfo         { return n.plan.Ordering() }

func (n *limitNode) Spans(ctx context.Context) (_, _ roachpb.Spans, _ error) {
	return n.plan.Spans(ctx)
}

func (n *limitNode) MarkDebug(mode explainMode) {
	if mode != explainDebug {
		panic(fmt.Sprintf("unknown debug mode %d", mode))
	}
	n.explain = mode
	n.plan.MarkDebug(mode)
}

func (n *limitNode) DebugValues() debugValues {
	if n.explain != explainDebug {
		panic(fmt.Sprintf("node not in debug mode (mode %d)", n.explain))
	}
	return n.debugVals
}

func (n *limitNode) Next(ctx context.Context) (bool, error) {
	// n.rowIndex is the 0-based index of the next row.
	// We don't do (n.rowIndex >= n.offset + n.count) to avoid overflow (count can be MaxInt64).
	if n.rowIndex-n.offset >= n.count {
		return false, nil
	}

	for {
		if next, err := n.plan.Next(ctx); !next {
			return false, err
		}

		if n.explain == explainDebug {
			n.debugVals = n.plan.DebugValues()
			if n.debugVals.output != debugValueRow {
				// Let the non-row debug values pass through.
				break
			}
		}

		n.rowIndex++
		if n.rowIndex > n.offset {
			// Row within limits, return it.
			break
		}

		if n.explain == explainDebug {
			// Return as a filtered row.
			n.debugVals.output = debugValueFiltered
			break
		}
		// Fetch the next row.
	}
	return true, nil
}

func (n *limitNode) Close(ctx context.Context) {
	n.plan.Close(ctx)
}
