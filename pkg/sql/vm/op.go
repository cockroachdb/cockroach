// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package vm

import (
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/catenumpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/opt"
	"github.com/cockroachdb/cockroach/pkg/sql/opt/cat"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/rowinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/span"
)

// Op is an instruction in the virtual machine. It is a closure that takes the
// VM state and returns a delta to apply to the VM's instruction pointer.
type Op func(vm *VM) (ipDelta int)

// OpJump returns an op that jumps by the given instruction pointer delta.
func OpJump(d int) Op {
	return func(vm *VM) int {
		return d
	}
}

// OpJumpIf returns an op that jumps by the given instruction pointer delta if the
// B register is true. If the B register is false, it continues to the next
// instruction.
func OpJumpIf(d int) Op {
	return func(vm *VM) int {
		if vm.reg.B {
			return d
		}
		return 1
	}
}

// OpHasRow returns an op that sets the B register to true if the ROW register
// is not nil, and false otherwise.
func OpHasRow() Op {
	return func(vm *VM) int {
		vm.reg.B = vm.reg.ROW != nil
		return 1
	}
}

// OpOutputRow returns an op that outputs the row in the ROW register.
func OpOutputRow() Op {
	return func(vm *VM) int {
		vm.out.OutputRow(vm.reg.ROW)
		return 1
	}
}

// OpSetTE returns an op that sets the TE register to the given typed
// expression.
func OpSetTE(exprs tree.TypedExprs) Op {
	return func(vm *VM) int {
		vm.reg.TE = exprs
		return 1
	}
}

// OpEvalTE returns an op that evaluates the expressions in the TE register and
// places the results in the ROW register.
func OpEvalTE() Op {
	return func(vm *VM) int {
		// Reuse reg.row if it is large enough, otherwise allocate a new one.
		if cap(vm.reg.ROW) < len(vm.reg.TE) {
			vm.reg.ROW = make([]tree.Datum, len(vm.reg.TE))
		}
		vm.reg.ROW = vm.reg.ROW[:len(vm.reg.TE)]
		for i, expr := range vm.reg.TE {
			// Invariant: The expression is either a placeholder or a constant.
			switch t := expr.(type) {
			case *tree.Placeholder:
				val, err := eval.Expr(vm.ctx, vm.evalCtx, t)
				if err != nil {
					panicTODO(err)
				}
				vm.reg.ROW[i] = val
			case tree.Datum:
				vm.reg.ROW[i] = t
			default:
				panicTODO("unexpected expression type")
			}
		}
		return 1
	}
}

// OpGenSpan returns an op that generates a span from the datums in the ROW
// register and places the span in the SP register.
func OpGenSpan(evalCtx *eval.Context, idx cat.Index, tabID descpb.ID, idxID descpb.IndexID) Op {
	args := new(struct {
		sb     span.Builder
		colMap catalog.TableColMap
	})

	keyCols := make([]fetchpb.IndexFetchSpec_KeyColumn, idx.ColumnCount())
	for i := range keyCols {
		col := idx.Column(i)
		dir := catenumpb.IndexColumn_ASC
		if col.Descending {
			dir = catenumpb.IndexColumn_DESC
		}
		// NOTE: span.Builder.SpanFromDatumRow only uses the ColumnID and
		// direction to build spans, so no other fields need to be set.
		keyCols[i] = fetchpb.IndexFetchSpec_KeyColumn{
			IndexFetchSpec_Column: fetchpb.IndexFetchSpec_Column{
				ColumnID: descpb.ColumnID(col.ColID()),
			},
			Direction: dir,
		}
	}

	for i, n := 0, idx.KeyColumnCount(); i < n; i++ {
		colID := descpb.ColumnID(idx.Column(i).ColID())
		args.colMap.Set(colID, i)
	}

	args.sb.InitAlt(evalCtx, evalCtx.Codec, tabID, idxID, keyCols)
	return func(vm *VM) int {
		// Reuse _SP if it can store a single span.
		if cap(vm.reg.SP) < 1 {
			vm.reg.SP = make([]roachpb.Span, 1)
		}
		vm.reg.SP = vm.reg.SP[:1]
		var err error
		vm.reg.SP[0], _, err = args.sb.SpanFromDatumRow(vm.reg.ROW, len(vm.reg.ROW), args.colMap)
		if err != nil {
			panicTODO(err)
		}
		return 1
	}
}

// OpFetcherInit returns an op that initializes the fetcher in the F register
// based on the span in the SP register.
func OpFetcherInit(
	codec keys.SQLCodec,
	tab cat.Table,
	tabID opt.TableID,
	cols opt.ColSet,
	tabDesc catalog.TableDescriptor,
	idxDesc catalog.Index,
) Op {
	args := new(struct {
		spec fetchpb.IndexFetchSpec
	})

	fetchCols := make([]descpb.ColumnID, 0, cols.Len())
	for col, ok := cols.Next(0); ok; col, ok = cols.Next(col + 1) {
		ord := tabID.ColumnOrdinal(col)
		fetchCols = append(fetchCols, descpb.ColumnID(tab.Column(ord).ColID()))
	}

	// TODO(mgartner): Ideally the fetch spec would not need information from
	// the descriptors to function. We should have all the information we need
	// in the memo.
	err := rowenc.InitIndexFetchSpec(&args.spec, codec, tabDesc, idxDesc, fetchCols)
	if err != nil {
		panicTODO(err)
	}

	return func(vm *VM) int {
		// TODO(mgartner): Initializing the fetcher allocates some temporary
		// memory. This could be allocated in the VM and reused.
		err := vm.reg.F.Init(vm.ctx, row.FetcherInitArgs{
			WillUseKVProvider: false,
			Txn:               vm.evalCtx.Txn,
			Reverse:           false,
			Alloc:             nil,
			Spec:              &args.spec,
			TraceKV:           false,
			SpansCanOverlap:   false,
			// TODO(mgartner): Set the remaining fields, if necessary.
			// MemMonitor:                 nil,
			// TraceKVEvery:               nil,
			// ForceProductionKVBatchSize: flowCtx.EvalCtx.TestingKnobs.ForceProductionValues,
			// LockStrength:               0,
			// LockWaitPolicy:             0,
			// LockDurability:             0,
			// LockTimeout:                flowCtx.EvalCtx.SessionData().LockTimeout,
			// DeadlockTimeout:            flowCtx.EvalCtx.SessionData().DeadlockTimeout,
		})
		if err != nil {
			panicTODO(err)
		}

		// TODO(mgartner): Set the row limit hint.
		var spanIDs []int
		err = vm.reg.F.StartScan(
			vm.ctx, vm.reg.SP, spanIDs, rowinfra.BytesLimit(10000), rowinfra.NoRowLimit,
		)
		if err != nil {
			panicTODO(err)
		}
		return 1
	}
}

// OpFetcherNext returns an op that fetches the next row from the fetcher in the
// F register and places it in the ROW register.
func OpFetcherNext() Op {
	return func(vm *VM) int {
		var err error
		vm.reg.ROW, _, err = vm.reg.F.NextRowDecoded(vm.ctx)
		if err != nil {
			panicTODO(err)
		}
		return 1
	}
}

// OpFetcherClose returns an op that closes the fetcher in the F register.
func OpFetcherClose() Op {
	return func(vm *VM) int {
		vm.reg.F.Close(vm.ctx)
		return 1
	}
}
