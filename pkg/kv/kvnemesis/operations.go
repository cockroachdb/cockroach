// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package kvnemesis

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvnemesis/kvnemesisutil"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// Result returns the Result field of the given Operation.
func (op Operation) Result() *Result {
	switch o := op.GetValue().(type) {
	case *GetOperation:
		return &o.Result
	case *PutOperation:
		return &o.Result
	case *ScanOperation:
		return &o.Result
	case *DeleteOperation:
		return &o.Result
	case *DeleteRangeOperation:
		return &o.Result
	case *DeleteRangeUsingTombstoneOperation:
		return &o.Result
	case *AddSSTableOperation:
		return &o.Result
	case *BarrierOperation:
		return &o.Result
	case *SplitOperation:
		return &o.Result
	case *MergeOperation:
		return &o.Result
	case *ChangeReplicasOperation:
		return &o.Result
	case *TransferLeaseOperation:
		return &o.Result
	case *ChangeSettingOperation:
		return &o.Result
	case *ChangeZoneOperation:
		return &o.Result
	case *BatchOperation:
		return &o.Result
	case *ClosureTxnOperation:
		return &o.Result
	case *SavepointCreateOperation:
		return &o.Result
	case *SavepointReleaseOperation:
		return &o.Result
	case *SavepointRollbackOperation:
		return &o.Result
	default:
		panic(errors.AssertionFailedf(`unknown operation: %T %v`, o, o))
	}
}

type steps []Step

func (s steps) After() hlc.Timestamp {
	var ts hlc.Timestamp
	for _, step := range s {
		ts.Forward(step.After)
	}
	return ts
}

func (s Step) String() string {
	var fctx formatCtx
	var buf strings.Builder
	s.format(&buf, fctx)
	return buf.String()
}

type formatCtx struct {
	receiver string
	indent   string
	// TODO(dan): error handling.
}

func (fctx formatCtx) maybeCtx() string {
	if fctx.receiver == `b` {
		return ""
	}
	return "ctx, "
}

func (s Step) format(w *strings.Builder, fctx formatCtx) {
	if fctx.receiver != `` {
		panic(`cannot specify receiver in Step.format fctx`)
	}
	fctx.receiver = fmt.Sprintf(`db%d`, s.DBID)
	w.WriteString("\n")
	w.WriteString(fctx.indent)
	s.Op.format(w, fctx)
}

func formatOps(w *strings.Builder, fctx formatCtx, ops []Operation) {
	for _, op := range ops {
		w.WriteString("\n")
		w.WriteString(fctx.indent)
		op.format(w, fctx)
	}
}

func (op Operation) String() string {
	fctx := formatCtx{receiver: `x`, indent: ``}
	var buf strings.Builder
	op.format(&buf, fctx)
	return buf.String()
}

func (op Operation) format(w *strings.Builder, fctx formatCtx) {
	switch o := op.GetValue().(type) {
	case *GetOperation:
		o.format(w, fctx)
	case *PutOperation:
		o.format(w, fctx)
	case *ScanOperation:
		o.format(w, fctx)
	case *DeleteOperation:
		o.format(w, fctx)
	case *DeleteRangeOperation:
		o.format(w, fctx)
	case *DeleteRangeUsingTombstoneOperation:
		o.format(w, fctx)
	case *AddSSTableOperation:
		o.format(w, fctx)
	case *BarrierOperation:
		o.format(w, fctx)
	case *SplitOperation:
		o.format(w, fctx)
	case *MergeOperation:
		o.format(w, fctx)
	case *ChangeReplicasOperation:
		o.format(w, fctx)
	case *TransferLeaseOperation:
		o.format(w, fctx)
	case *ChangeSettingOperation:
		o.format(w, fctx)
	case *ChangeZoneOperation:
		o.format(w, fctx)
	case *BatchOperation:
		newFctx := fctx
		newFctx.indent = fctx.indent + `  `
		newFctx.receiver = `b`
		w.WriteString(`{`)
		o.format(w, newFctx)
		w.WriteString("\n")
		w.WriteString(newFctx.indent)
		w.WriteString(fctx.receiver)
		w.WriteString(`.Run(ctx, b)`)
		o.Result.format(w)
		w.WriteString("\n")
		w.WriteString(fctx.indent)
		w.WriteString(`}`)
	case *ClosureTxnOperation:
		txnName := `txn` + o.TxnID
		newFctx := fctx
		newFctx.indent = fctx.indent + `  `
		newFctx.receiver = txnName
		w.WriteString(fctx.receiver)
		fmt.Fprintf(w, `.Txn(ctx, func(ctx context.Context, %s *kv.Txn) error {`, txnName)
		w.WriteString("\n")
		w.WriteString(newFctx.indent)
		w.WriteString(newFctx.receiver)
		fmt.Fprintf(w, `.SetIsoLevel(isolation.%s)`, o.IsoLevel)
		formatOps(w, newFctx, o.Ops)
		if o.CommitInBatch != nil {
			newFctx.receiver = `b`
			o.CommitInBatch.format(w, newFctx)
			newFctx.receiver = txnName
			w.WriteString("\n")
			w.WriteString(newFctx.indent)
			w.WriteString(newFctx.receiver)
			w.WriteString(`.CommitInBatch(ctx, b)`)
			o.CommitInBatch.Result.format(w)
		}
		w.WriteString("\n")
		w.WriteString(newFctx.indent)
		switch o.Type {
		case ClosureTxnType_Commit:
			w.WriteString(`return nil`)
		case ClosureTxnType_Rollback:
			w.WriteString(`return errors.New("rollback")`)
		default:
			panic(errors.AssertionFailedf(`unknown closure txn type: %s`, o.Type))
		}
		w.WriteString("\n")
		w.WriteString(fctx.indent)
		w.WriteString(`})`)
		o.Result.format(w)
		if o.Txn != nil {
			fmt.Fprintf(w, "\n%s// ^-- txnpb:(%s)", fctx.indent, o.Txn)
		}
	case *SavepointCreateOperation:
		o.format(w, fctx)
	case *SavepointReleaseOperation:
		o.format(w, fctx)
	case *SavepointRollbackOperation:
		o.format(w, fctx)
	default:
		fmt.Fprintf(w, "%v", op.GetValue())
	}
}

func fmtKey(k []byte) string {
	return fmt.Sprintf(`tk(%d)`, fk(string(k)))
}

func (op GetOperation) format(w *strings.Builder, fctx formatCtx) {
	methodName := `Get`
	if op.ForUpdate {
		methodName = `GetForUpdate`
	}
	if op.ForShare {
		methodName = `GetForShare`
	}
	if op.SkipLocked {
		// NB: SkipLocked is a property of a batch, not an individual operation. We
		// don't have a way to represent this here, so we pretend it's part of the
		// method name for debugging purposes.
		methodName += "SkipLocked"
	}
	if op.GuaranteedDurability {
		methodName += "GuaranteedDurability"
	}
	fmt.Fprintf(w, `%s.%s(%s%s)`, fctx.receiver, methodName, fctx.maybeCtx(), fmtKey(op.Key))
	op.Result.format(w)
}

func (op PutOperation) format(w *strings.Builder, fctx formatCtx) {
	fmt.Fprintf(w, `%s.Put(%s%s, sv(%d))`, fctx.receiver, fctx.maybeCtx(), fmtKey(op.Key), op.Seq)
	op.Result.format(w)
}

// sv stands for sequence value, i.e. the value dictated by the given sequence number.
func sv(seq kvnemesisutil.Seq) string {
	return `v` + strconv.Itoa(int(seq))
}

// Value returns the value written by this put. This is a function of the
// sequence number.
func (op PutOperation) Value() string {
	return sv(op.Seq)
}

func (op ScanOperation) format(w *strings.Builder, fctx formatCtx) {
	methodName := `Scan`
	if op.ForUpdate {
		methodName = `ScanForUpdate`
	}
	if op.ForShare {
		methodName = `ScanForShare`
	}
	if op.SkipLocked {
		// NB: SkipLocked is a property of a batch, not an individual operation. We
		// don't have a way to represent this here, so we pretend it's part of the
		// method name for debugging purposes.
		methodName += "SkipLocked"
	}
	if op.GuaranteedDurability {
		methodName += "GuaranteedDurability"
	}
	if op.Reverse {
		methodName = `Reverse` + methodName
	}
	// NB: DB.Scan has a maxRows parameter that Batch.Scan does not have.
	maxRowsArg := `, 0`
	if fctx.receiver == `b` {
		maxRowsArg = ``
	}
	fmt.Fprintf(w, `%s.%s(%s%s, %s%s)`, fctx.receiver, methodName, fctx.maybeCtx(), fmtKey(op.Key), fmtKey(op.EndKey), maxRowsArg)
	op.Result.format(w)
}

func (op DeleteOperation) format(w *strings.Builder, fctx formatCtx) {
	fmt.Fprintf(w, `%s.Del(%s%s /* @%s */)`, fctx.receiver, fctx.maybeCtx(), fmtKey(op.Key), op.Seq)
	op.Result.format(w)
}

func (op DeleteRangeOperation) format(w *strings.Builder, fctx formatCtx) {
	fmt.Fprintf(w, `%s.DelRange(%s%s, %s, true /* @%s */)`, fctx.receiver, fctx.maybeCtx(), fmtKey(op.Key), fmtKey(op.EndKey), op.Seq)
	op.Result.format(w)
}

func (op DeleteRangeUsingTombstoneOperation) format(w *strings.Builder, fctx formatCtx) {
	fmt.Fprintf(w, `%s.DelRangeUsingTombstone(ctx, %s, %s /* @%s */)`, fctx.receiver, fmtKey(op.Key), fmtKey(op.EndKey), op.Seq)
	op.Result.format(w)
}

func (op AddSSTableOperation) format(w *strings.Builder, fctx formatCtx) {
	fmt.Fprintf(w, `%s.AddSSTable(%s%s, %s, ... /* @%s */)`,
		fctx.receiver, fctx.maybeCtx(), fmtKey(op.Span.Key), fmtKey(op.Span.EndKey), op.Seq)
	if op.AsWrites {
		fmt.Fprintf(w, ` (as writes)`)
	}

	iter, err := storage.NewMemSSTIterator(op.Data, false /* verify */, storage.IterOptions{
		KeyTypes:   storage.IterKeyTypePointsAndRanges,
		LowerBound: keys.MinKey,
		UpperBound: keys.MaxKey,
	})
	if err != nil {
		panic(err)
	}
	defer iter.Close()
	for iter.SeekGE(storage.MVCCKey{Key: keys.MinKey}); ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			panic(err)
		} else if !ok {
			break
		}
		if iter.RangeKeyChanged() {
			hasPoint, hasRange := iter.HasPointAndRange()
			if hasRange {
				for _, rkv := range iter.RangeKeys().AsRangeKeyValues() {
					mvccValue, err := storage.DecodeMVCCValue(rkv.Value)
					if err != nil {
						panic(err)
					}
					seq := mvccValue.KVNemesisSeq.Get()
					fmt.Fprintf(w, "\n%s// ^-- [%s, %s) -> sv(%s): %s -> %s", fctx.indent,
						fmtKey(rkv.RangeKey.StartKey), fmtKey(rkv.RangeKey.EndKey), seq,
						rkv.RangeKey.Bounds(), mvccValue.Value.PrettyPrint())
				}
			}
			if !hasPoint {
				continue
			}
		}
		rawValue, err := iter.UnsafeValue()
		if err != nil {
			panic(err)
		}
		key := iter.UnsafeKey()
		mvccValue, err := storage.DecodeMVCCValue(rawValue)
		if err != nil {
			panic(err)
		}
		seq := mvccValue.KVNemesisSeq.Get()
		fmt.Fprintf(w, "\n%s// ^-- %s -> sv(%s): %s -> %s", fctx.indent,
			fmtKey(key.Key), seq, key, mvccValue.Value.PrettyPrint())
	}
}

func (op BarrierOperation) format(w *strings.Builder, fctx formatCtx) {
	if op.WithLeaseAppliedIndex {
		fmt.Fprintf(w, `%s.BarrierWithLAI(ctx, %s, %s)`,
			fctx.receiver, fmtKey(op.Key), fmtKey(op.EndKey))
	} else {
		fmt.Fprintf(w, `%s.Barrier(ctx, %s, %s)`, fctx.receiver, fmtKey(op.Key), fmtKey(op.EndKey))
	}
	op.Result.format(w)
}

func (op SplitOperation) format(w *strings.Builder, fctx formatCtx) {
	fmt.Fprintf(w, `%s.AdminSplit(ctx, %s, hlc.MaxTimestamp)`, fctx.receiver, fmtKey(op.Key))
	op.Result.format(w)
}

func (op MergeOperation) format(w *strings.Builder, fctx formatCtx) {
	fmt.Fprintf(w, `%s.AdminMerge(ctx, %s)`, fctx.receiver, fmtKey(op.Key))
	op.Result.format(w)
}

func (op BatchOperation) format(w *strings.Builder, fctx formatCtx) {
	w.WriteString("\n")
	w.WriteString(fctx.indent)
	w.WriteString(`b := &kv.Batch{}`)
	formatOps(w, fctx, op.Ops)
}

func (op ChangeReplicasOperation) format(w *strings.Builder, fctx formatCtx) {
	changes := make([]string, len(op.Changes))
	for i, c := range op.Changes {
		changes[i] = fmt.Sprintf("kvpb.ReplicationChange{ChangeType: roachpb.%s, Target: roachpb.ReplicationTarget{NodeID: %d, StoreID: %d}}",
			c.ChangeType, c.Target.NodeID, c.Target.StoreID)
	}
	fmt.Fprintf(w, `%s.AdminChangeReplicas(ctx, %s, getRangeDesc(ctx, %s, %s), %s)`, fctx.receiver,
		fmtKey(op.Key), fmtKey(op.Key), fctx.receiver, strings.Join(changes, ", "))
	op.Result.format(w)
}

func (op TransferLeaseOperation) format(w *strings.Builder, fctx formatCtx) {
	fmt.Fprintf(w, `%s.AdminTransferLease(ctx, %s, %d)`, fctx.receiver, fmtKey(op.Key), op.Target)
	op.Result.format(w)
}

func (op ChangeSettingOperation) format(w *strings.Builder, fctx formatCtx) {
	switch op.Type {
	case ChangeSettingType_SetLeaseType:
		fmt.Fprintf(w, `env.SetClusterSetting(ctx, %s, %s)`, op.Type, op.LeaseType)
	default:
		panic(errors.AssertionFailedf(`unknown ChangeSettingType: %v`, op.Type))
	}
	op.Result.format(w)
}

func (op ChangeZoneOperation) format(w *strings.Builder, fctx formatCtx) {
	fmt.Fprintf(w, `env.UpdateZoneConfig(ctx, %s)`, op.Type)
	op.Result.format(w)
}

func (op SavepointCreateOperation) format(w *strings.Builder, fctx formatCtx) {
	fmt.Fprintf(w, `%s.CreateSavepoint(ctx, %d)`, fctx.receiver, int(op.ID))
	op.Result.format(w)
}

func (op SavepointReleaseOperation) format(w *strings.Builder, fctx formatCtx) {
	fmt.Fprintf(w, `%s.ReleaseSavepoint(ctx, %d)`, fctx.receiver, int(op.ID))
	op.Result.format(w)
}

func (op SavepointRollbackOperation) format(w *strings.Builder, fctx formatCtx) {
	fmt.Fprintf(w, `%s.RollbackSavepoint(ctx, %d)`, fctx.receiver, int(op.ID))
	op.Result.format(w)
}

func (r Result) format(w *strings.Builder) {
	if r.Type == ResultType_Unknown {
		return
	}
	fmt.Fprintf(w, ` //`)
	if r.OptionalTimestamp.IsSet() {
		fmt.Fprintf(w, ` @%s`, r.OptionalTimestamp)
	}

	var sl []string
	errString := "<nil>"
	switch r.Type {
	case ResultType_NoError:
	case ResultType_Error:
		err := errors.DecodeError(context.TODO(), *r.Err)
		errString = fmt.Sprint(err)
	case ResultType_Keys:
		for _, k := range r.Keys {
			sl = append(sl, roachpb.Key(k).String())
		}
	case ResultType_Value:
		sl = append(sl, mustGetStringValue(r.Value))
	case ResultType_Values:
		for _, kv := range r.Values {
			sl = append(sl, fmt.Sprintf(`%s:%s`, roachpb.Key(kv.Key), mustGetStringValue(kv.Value)))
		}
	default:
		panic("unhandled ResultType")
	}

	w.WriteString(" ")

	sl = append(sl, errString)
	if len(sl) > 1 {
		w.WriteString("(")
	}
	w.WriteString(strings.Join(sl, ", "))
	if len(sl) > 1 {
		w.WriteString(")")
	}

}

// Error decodes and returns the r.Err if it is set.
func (r Result) Error() error {
	if r.Err == nil {
		return nil
	}
	if !r.Err.IsSet() {
		return nil
	}
	return errors.DecodeError(context.Background(), *r.Err)
}
