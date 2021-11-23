// Copyright 2018 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Portions of this file are additionally subject to the following
// license and copyright.
//
// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// Package nilness inspects the control-flow graph of an SSA function
// and reports errors such as nil pointer dereferences and degenerate
// nil pointer comparisons.
package nilness

import (
	"fmt"
	"go/token"
	"go/types"

	"golang.org/x/tools/go/analysis"
	"golang.org/x/tools/go/analysis/passes/buildssa"
	"golang.org/x/tools/go/ssa"
)

const doc = `check for redundant or impossible nil comparisons

The nilness checker inspects the control-flow graph of each function in
a package and reports nil pointer dereferences, degenerate nil
pointers, and panics with nil values. A degenerate comparison is of the form
x==nil or x!=nil where x is statically known to be nil or non-nil. These are
often a mistake, especially in control flow related to errors. Panics with nil
values are checked because they are not detectable by

	if r := recover(); r != nil {

This check reports conditions such as:

	if f == nil { // impossible condition (f is a function)
	}

and:

	p := &v
	...
	if p != nil { // tautological condition
	}

and:

	if p == nil {
		print(*p) // nil dereference
	}

and:

	if p == nil {
		panic(p)
	}
`

type argExtractor func(c *ssa.CallCommon) ssa.Value

type analyzerConfig struct {
	// reportDegenerateConditions controls the reporting of "impossible"
	// and "tautological" comparisons. While many of these are in error,
	// it also catches nil checks where the current control flow graph
	// can't produce nil but where we might want to guard against changes
	// in the future. It also catches a number of compound conditionals
	// where the "tautological" check produces more readable code.
	reportDegenerateIfConditions bool
	// argExtractors is a map from the full name of a function to
	// a func that returns the argument from a Call that should
	// not be passed provably nil values.
	argExtractors map[string]argExtractor
}

var crdbConfig = analyzerConfig{
	reportDegenerateIfConditions: false,
	argExtractors: map[string]argExtractor{
		"github.com/cockroachdb/errors.Handled":                          firstArg,
		"github.com/cockroachdb/errors.HandledWithMessage":               firstArg,
		"github.com/cockroachdb/errors.NewAssertionErrorWithWrappedErrf": firstArg,
		"github.com/cockroachdb/errors.Mark":                             firstArg,
		"github.com/cockroachdb/errors.WithContextTags":                  firstArg,
		"github.com/cockroachdb/errors.WithDetail":                       firstArg,
		"github.com/cockroachdb/errors.WithDetailf":                      firstArg,
		"github.com/cockroachdb/errors.WithHint":                         firstArg,
		"github.com/cockroachdb/errors.WithHintf":                        firstArg,
		"github.com/cockroachdb/errors.WithMessage":                      firstArg,
		"github.com/cockroachdb/errors.WithMessagef":                     firstArg,
		"github.com/cockroachdb/errors.WithStack":                        firstArg,
		"github.com/cockroachdb/errors.WithStackDepth":                   firstArg,
		"github.com/cockroachdb/errors.Wrap":                             firstArg,
		"github.com/cockroachdb/errors.Wrapf":                            firstArg,
		"github.com/cockroachdb/errors.WrapWithDepth":                    firstArg,
		"github.com/cockroachdb/errors.WrapWithDepthf":                   firstArg,
	},
}

var defaultConfig = analyzerConfig{
	reportDegenerateIfConditions: true,
}

// Analyzer defines a pass that checks for uses of provably nil
// values that were likely in error with custom configuration
// suitable for the CRDB codebase.
var Analyzer = &analysis.Analyzer{
	Name:     "nilness",
	Doc:      doc,
	Run:      crdbConfig.run,
	Requires: []*analysis.Analyzer{buildssa.Analyzer},
}

// TestAnalyzer defines a pass that checks for uses of provably nil values
// that were likely in error.
var TestAnalyzer = &analysis.Analyzer{
	Name:     "nilness",
	Doc:      doc,
	Run:      defaultConfig.run,
	Requires: []*analysis.Analyzer{buildssa.Analyzer},
}

func (a *analyzerConfig) run(pass *analysis.Pass) (interface{}, error) {
	ssainput := pass.ResultOf[buildssa.Analyzer].(*buildssa.SSA)
	for _, fn := range ssainput.SrcFuncs {
		a.runFunc(pass, fn)
	}
	return nil, nil
}

func (a *analyzerConfig) runFunc(pass *analysis.Pass, fn *ssa.Function) {
	reportf := func(category string, pos token.Pos, format string, args ...interface{}) {
		pass.Report(analysis.Diagnostic{
			Pos:      pos,
			Category: category,
			Message:  fmt.Sprintf(format, args...),
		})
	}

	// notNil reports an error if v is provably nil.
	notNil := func(stack []fact, instr ssa.Instruction, v ssa.Value, descr string) {
		if nilnessOf(stack, v) == isnil {
			reportf("nilderef", instr.Pos(), "nil dereference in "+descr)
		}
	}

	// notNilArg reports an error if v is provably nil.
	notNilArg := func(stack []fact, instr ssa.Instruction, v ssa.Value, descr string) {
		if nilnessOf(stack, v) == isnil {
			reportf("nilarg", instr.Pos(), "nil argument to "+descr)
		}
	}

	// visit visits reachable blocks of the CFG in dominance order,
	// maintaining a stack of dominating nilness facts.
	//
	// By traversing the dom tree, we can pop facts off the stack as
	// soon as we've visited a subtree.  Had we traversed the CFG,
	// we would need to retain the set of facts for each block.
	seen := make([]bool, len(fn.Blocks)) // seen[i] means visit should ignore block i
	var visit func(b *ssa.BasicBlock, stack []fact)
	visit = func(b *ssa.BasicBlock, stack []fact) {
		if seen[b.Index] {
			return
		}
		seen[b.Index] = true

		// Report nil dereferences.
		for _, instr := range b.Instrs {
			switch instr := instr.(type) {
			case ssa.CallInstruction:
				call := instr.Common()
				notNil(stack, instr, call.Value, call.Description())

				switch f := call.Value.(type) {
				case *ssa.Function:
					if tf, ok := f.Object().(*types.Func); ok {
						if extract, ok := a.argExtractors[tf.FullName()]; ok {
							notNilArg(stack, instr, extract(call), tf.FullName())
						}
					}
				}
			case *ssa.FieldAddr:
				notNil(stack, instr, instr.X, "field selection")
			case *ssa.IndexAddr:
				notNil(stack, instr, instr.X, "index operation")
			case *ssa.MapUpdate:
				notNil(stack, instr, instr.Map, "map update")
			case *ssa.Slice:
				// A nilcheck occurs in ptr[:] iff ptr is a pointer to an array.
				if _, ok := instr.X.Type().Underlying().(*types.Pointer); ok {
					notNil(stack, instr, instr.X, "slice operation")
				}
			case *ssa.Store:
				notNil(stack, instr, instr.Addr, "store")
			case *ssa.TypeAssert:
				if !instr.CommaOk {
					notNil(stack, instr, instr.X, "type assertion")
				}
			case *ssa.UnOp:
				if instr.Op == token.MUL { // *X
					notNil(stack, instr, instr.X, "load")
				}
			}
		}

		// Look for panics with nil value
		for _, instr := range b.Instrs {
			switch instr := instr.(type) {
			case *ssa.Panic:
				if nilnessOf(stack, instr.X) == isnil {
					reportf("nilpanic", instr.Pos(), "panic with nil value")
				}
			}
		}

		// For nil comparison blocks, report an error if the condition
		// is degenerate, and push a nilness fact on the stack when
		// visiting its true and false successor blocks.
		if binop, tsucc, fsucc := eq(b); binop != nil {
			xnil := nilnessOf(stack, binop.X)
			ynil := nilnessOf(stack, binop.Y)

			if ynil != unknown && xnil != unknown && (xnil == isnil || ynil == isnil) {
				// Degenerate condition:
				// the nilness of both operands is known,
				// and at least one of them is nil.
				if a.reportDegenerateIfConditions {
					var adj string
					if (xnil == ynil) == (binop.Op == token.EQL) {
						adj = "tautological"
					} else {
						adj = "impossible"
					}
					reportf("cond", binop.Pos(), "%s condition: %s %s %s", adj, xnil, binop.Op, ynil)
				}

				// If tsucc's or fsucc's sole incoming edge is impossible,
				// it is unreachable.  Prune traversal of it and
				// all the blocks it dominates.
				// (We could be more precise with full dataflow
				// analysis of control-flow joins.)
				var skip *ssa.BasicBlock
				if xnil == ynil {
					skip = fsucc
				} else {
					skip = tsucc
				}
				for _, d := range b.Dominees() {
					if d == skip && len(d.Preds) == 1 {
						continue
					}
					visit(d, stack)
				}
				return
			}

			// "if x == nil" or "if nil == y" condition; x, y are unknown.
			if xnil == isnil || ynil == isnil {
				var newFacts facts
				if xnil == isnil {
					// x is nil, y is unknown:
					// t successor learns y is nil.
					newFacts = expandFacts(fact{binop.Y, isnil})
				} else {
					// x is nil, y is unknown:
					// t successor learns x is nil.
					newFacts = expandFacts(fact{binop.X, isnil})
				}

				for _, d := range b.Dominees() {
					// Successor blocks learn a fact
					// only at non-critical edges.
					// (We could do be more precise with full dataflow
					// analysis of control-flow joins.)
					s := stack
					if len(d.Preds) == 1 {
						if d == tsucc {
							s = append(s, newFacts...)
						} else if d == fsucc {
							s = append(s, newFacts.negate()...)
						}
					}
					visit(d, s)
				}
				return
			}
		}

		for _, d := range b.Dominees() {
			visit(d, stack)
		}
	}

	// Visit the entry block.  No need to visit fn.Recover.
	if fn.Blocks != nil {
		visit(fn.Blocks[0], make([]fact, 0, 20)) // 20 is plenty
	}
}

// A fact records that a block is dominated
// by the condition v == nil or v != nil.
type fact struct {
	value   ssa.Value
	nilness nilness
}

func (f fact) negate() fact { return fact{f.value, -f.nilness} }

type nilness int

const (
	isnonnil         = -1
	unknown  nilness = 0
	isnil            = 1
)

var nilnessStrings = []string{"non-nil", "unknown", "nil"}

func (n nilness) String() string { return nilnessStrings[n+1] }

// nilnessOf reports whether v is definitely nil, definitely not nil,
// or unknown given the dominating stack of facts.
func nilnessOf(stack []fact, v ssa.Value) nilness {
	switch v := v.(type) {
	// unwrap ChangeInterface values recursively, to detect if underlying
	// values have any facts recorded or are otherwise known with regard to nilness.
	//
	// This work must be in addition to expanding facts about
	// ChangeInterfaces during inference/fact gathering because this covers
	// cases where the nilness of a value is intrinsic, rather than based
	// on inferred facts, such as a zero value interface variable. That
	// said, this work alone would only inform us when facts are about
	// underlying values, rather than outer values, when the analysis is
	// transitive in both directions.
	case *ssa.ChangeInterface:
		if underlying := nilnessOf(stack, v.X); underlying != unknown {
			return underlying
		}
	}

	// Is value intrinsically nil or non-nil?
	switch v := v.(type) {
	case *ssa.Alloc,
		*ssa.FieldAddr,
		*ssa.FreeVar,
		*ssa.Function,
		*ssa.Global,
		*ssa.IndexAddr,
		*ssa.MakeChan,
		*ssa.MakeClosure,
		*ssa.MakeInterface,
		*ssa.MakeMap,
		*ssa.MakeSlice:
		return isnonnil
	case *ssa.Const:
		if v.IsNil() {
			return isnil
		}
		return isnonnil
	}

	// Search dominating control-flow facts.
	for _, f := range stack {
		if f.value == v {
			return f.nilness
		}
	}
	return unknown
}

// If b ends with an equality comparison, eq returns the operation and
// its true (equal) and false (not equal) successors.
func eq(b *ssa.BasicBlock) (op *ssa.BinOp, tsucc, fsucc *ssa.BasicBlock) {
	if If, ok := b.Instrs[len(b.Instrs)-1].(*ssa.If); ok {
		if binop, ok := If.Cond.(*ssa.BinOp); ok {
			switch binop.Op {
			case token.EQL:
				return binop, b.Succs[0], b.Succs[1]
			case token.NEQ:
				return binop, b.Succs[1], b.Succs[0]
			}
		}
	}
	return nil, nil, nil
}

// expandFacts takes a single fact and returns the set of facts that can be
// known about it or any of its related values. Some operations, like
// ChangeInterface, have transitive nilness, such that if you know the
// underlying value is nil, you also know the value itself is nil, and vice
// versa. This operation allows callers to match on any of the related values
// in analyzes, rather than just the one form of the value that happened to
// appear in a comparison.
//
// This work must be in addition to unwrapping values within nilnessOf because
// while this work helps give facts about transitively known values based on
// inferred facts, the recursive check within nilnessOf covers cases where
// nilness facts are intrinsic to the underlying value, such as a zero value
// interface variables.
//
// ChangeInterface is the only expansion currently supported, but others, like
// Slice, could be added. At this time, this tool does not check slice
// operations in a way this expansion could help. See
// https://play.golang.org/p/mGqXEp7w4fR for an example.
func expandFacts(f fact) []fact {
	ff := []fact{f}

Loop:
	for {
		switch v := f.value.(type) {
		case *ssa.ChangeInterface:
			f = fact{v.X, f.nilness}
			ff = append(ff, f)
		default:
			break Loop
		}
	}

	return ff
}

type facts []fact

func (ff facts) negate() facts {
	nn := make([]fact, len(ff))
	for i, f := range ff {
		nn[i] = f.negate()
	}
	return nn
}

var firstArg = func(c *ssa.CallCommon) ssa.Value { return c.Args[0] }
