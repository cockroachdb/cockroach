// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/memo"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/vm"
)

// Compiler compiles an opt.Expr into a vm.Program. It currently only supports
// compilation of PlaceholderScans.
//
// As an example, consider the following schema and prepare statements:
//
//	CREATE TABLE t (
//	  k INT PRIMARY KEY,
//	  a INT,
//	  b INT,
//	  INDEX (a, b)
//	)
//
//	PREPARE p AS SELECT * FROM t WHERE a = $1 AND b = $2
//
// The compiled program for the prepared statement above would be:
//
// 00  OpSetTE [$1, $2]
// 01  OpEvalTE
// 02  OpGenSpan
// 03  OpFetcherInit ◀───────┐
// 04  OpFetcherNext         │
// 05  OpHasRow              │
// 06  OpJumpIf 2   ────────┐│
// 07  OpFetcherClose       ││
// 08  OpHasRow     ◀───────┘│
// 09  OpJumpIf 2   ────────┐│
// 10  OpJump 3  ──▶ exit   ││
// 11  OpOutputRow  ◀───────┘│
// 12  OpJump -8    ─────────┘
//
// Ops 0-7 are compiled in compilePlaceholderScan. 0-3 set up a row fetcher over
// the span defined by the placeholders. 4-7 fetch the next row, and if there is
// no row, close the fetcher and exit. If there is a row, it is stored in the
// ROW registerd and execution continues.
//
// Ops 8-12 are added in the top-level compileExpr method. 8-10, check if there is a
// row, and if not, exit. If there is a row, it is output (11), and then execution
// jumps back to 4 to fetch the next row.
//
// The general idea is that each expression compiles a series of Ops that
// produce a single row, leaving it in the ROW register. This works well for a
// simple placeholder scan and would expand easily for filters and projections.
// Something more clever might be needed to support joins, aggregations, etc.
type Compiler struct {
	md      *opt.Metadata
	evalCtx *eval.Context

	p vm.Program
}

// Compile compiles the given expression into a VM program.
func (c *Compiler) Compile(
	evalCtx *eval.Context, md *opt.Metadata, expr opt.Expr,
) (_ vm.Program, ok bool) {
	*c = Compiler{
		md:      md,
		evalCtx: evalCtx,
	}

	// Compile the expression.
	nextRowIP, ok := c.compileExpr(expr)
	if !ok {
		return vm.Program{}, false
	}

	// If there is a row, output it and jump back to fetch the next row. If
	// there is no row, jump to the end of the program.
	c.p.Push(vm.OpHasRow())
	c.p.Push(vm.OpJumpIf(2))
	c.p.Push(vm.OpJump(3))
	ip := c.p.Push(vm.OpOutputRow())
	c.p.Push(vm.OpJump(nextRowIP - ip - 1))

	return c.p, true
}

// compileExpr compiles the given expression into VM instructions. The compiled
// Ops for each expression should leave the next row in the ROW register, or set
// it to nil if there is not another row.
func (c *Compiler) compileExpr(e opt.Expr) (nextRowIP int, ok bool) {
	switch t := e.(type) {
	case *memo.PlaceholderScanExpr:
		return c.compilePlaceholderScan(t)
	default:
		return 0, false
	}
}

func (c *Compiler) compilePlaceholderScan(e *memo.PlaceholderScanExpr) (nextRowIP int, ok bool) {
	tab := c.md.Table(e.Table)
	idx := tab.Index(e.Index)
	tabID := descpb.ID(tab.ID())
	idxID := descpb.IndexID(idx.ID())

	// TODO(mgartner): Ideally the KV fetcher would not need information from
	// the descriptors. This is preventing the VM, compiler, and ops from having
	// their own package. We can currently only access the descriptors through
	// optTable and optIndex. We should have all the information we need in the
	// memo.
	tabDesc := tab.(*optTable).desc
	idxDesc := idx.(*optIndex).idx

	// First, evaluate the typed expressions to replace placeholders with
	// datums.
	exprs, ok := constantsAndPlaceholders(e.Span)
	if !ok {
		return 0, false
	}
	c.p.Push(vm.OpSetTE(exprs))
	c.p.Push(vm.OpEvalTE())

	// Next, generate a span and start the scan.
	c.p.Push(vm.OpGenSpan(c.evalCtx, idx, tabID, idxID))
	c.p.Push(vm.OpFetcherInit(c.evalCtx.Codec, tab, e.Table, e.Cols, tabDesc, idxDesc))

	// Fetch the next row. If a row is fetched, jump over the following
	// instructions to close the fetcher.
	nextRowIP = c.p.Push(vm.OpFetcherNext())
	c.p.Push(vm.OpHasRow())
	c.p.Push(vm.OpJumpIf(2))

	// If no row is fetch, close the fetcher.
	c.p.Push(vm.OpFetcherClose())

	return nextRowIP, true
}

func constantsAndPlaceholders(expr memo.ScalarListExpr) (_ []tree.TypedExpr, ok bool) {
	// This function returns a slice of TypedExprs that are either constants or
	// placeholders. It is used to extract the values from the Span of a
	// PlaceholderScanExpr.
	exprs := make([]tree.TypedExpr, len(expr))
	for i, e := range expr {
		// The expression is either a placeholder or a constant.
		if opt.IsConstValueOp(e) {
			exprs[i] = memo.ExtractConstDatum(e)
			continue
		}
		if p, ok := e.(*memo.PlaceholderExpr); ok {
			exprs[i] = p.Value
			continue
		}
		return nil, false
	}
	return exprs, true
}
