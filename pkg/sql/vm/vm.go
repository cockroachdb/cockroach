// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vm

import (
	"context"
	"fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
)

// VM is a virtual machine that executes a "SQL Program". A Program is a slice
// of Ops. Each Op is a closure that takes the VM as an argument and returns a
// delta to apply to the VM's instruction pointer. The VM provides typed
// registers which Ops use for working memory.
//
// The design of this prototype is inspired by:
// https://planetscale.com/blog/faster-interpreters-in-go-catching-up-with-cpp
//
// Advantages of this particular VM design include:
//
//  1. The execution loop is trivial - see Exec.
//  2. There is no bytecode to define and therefore no bytecode interpreter to
//     keep in-sync with bytecode. Each Op "interprets" itself.
//  3. Distinguishing compile-time logic from run-time logic is easy -
//     compile-time logic is outside of Op closures and run-time logic is inside
//     Op closures.
//  4. The working memory of the VM is explicit - see the registers in struct in
//     VM.reg. Reusing working memory across executions of multiple Programs is
//     simple.
//  5. Registers add type safety and avoid the overhead of type assertions
//     during run-time that an untyped stack would require.
//  6. Easy debuggability of Programs (in my experience) - simply set a
//     breakpoint in Exec and step through each Op.
//  7. Ops have the flexibility to be low-level or high-level. For example,
//     OpJump is a simple jump to another Op in the Program, while OpFetcherInit
//     is a high-level operation that initializes a row.Fetcher over spans
//
// One advantage of using a VM for SQL execution is that it forces discipline.
// Ops cannot call arbitrary Go code. They can only interact with the VM's
// facilities. State between Ops must be stored in the VM's registers. This
// makes it easier to reason about run-time logic, memory allocations, and
// performance.
//
// Another advantage is the conditional logic for special cases can be performed
// at compile-time, completely avoiding run-time overhead.
//
// See sql.Compiler for more explanation on how SQL queries are compiled into a
// Program.
type VM struct {
	ctx     context.Context
	evalCtx *eval.Context
	out     Output

	// The fields below are "registers" that represent working memory of the
	// virtual machine. They are used to store and access values across multiple
	// Ops during execution of a Program.
	reg struct {
		B   bool
		ROW []tree.Datum
		TE  []tree.TypedExpr
		SP  []roachpb.Span
		F   row.Fetcher
	}
}

// Output is an interface that receives rows produced by the VM.
type Output interface {
	OutputRow(row tree.Datums)
}

// Init initializes the VM with the given context, evaluation context, and
// output receiver.
func (vm *VM) Init(ctx context.Context, evalCtx *eval.Context, out Output) {
	*vm = VM{
		ctx:     ctx,
		evalCtx: evalCtx,
		out:     out,
	}
	vm.reg.ROW = vm.reg.ROW[:0]
	vm.reg.TE = vm.reg.TE[:0]
	vm.reg.SP = vm.reg.SP[:0]
}

// Exec executes the given Program.
func (vm *VM) Exec(p Program) {
	ip := 0
	for ip < len(p.ops) {
		ip += p.ops[ip](vm)
	}
}

// TODO(mgartner): Handle unexpected errors.
func panicTODO(err any) {
	panic(fmt.Sprintf("unexpected error: %s", err))
}
