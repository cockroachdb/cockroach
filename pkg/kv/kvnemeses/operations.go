// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvnemeses

import (
	"context"
	"fmt"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
)

func (s Step) String() string {
	var buf strings.Builder
	s.format(&buf, ``)
	return buf.String()
}

func (s Step) format(w *strings.Builder, indent string) {
	w.WriteString("\n")
	w.WriteString(indent)
	if len(s.Ops) == 1 {
		s.Ops[0].format(w, indent, `db`)
		return
	}
	w.WriteString(`Concurrent`)
	formatOps(w, indent+`  `, `db`, s.Ops)
}

func formatOps(w *strings.Builder, indent, receiver string, ops []Operation) {
	for _, op := range ops {
		w.WriteString("\n")
		w.WriteString(indent)
		op.format(w, indent, receiver)
	}
}

func (op Operation) String() string {
	var buf strings.Builder
	op.format(&buf, ``, `x`)
	return buf.String()
}

func (op Operation) format(w *strings.Builder, indent, receiver string) {
	switch o := op.GetValue().(type) {
	case *GetOperation:
		o.format(w, receiver)
	case *PutOperation:
		o.format(w, receiver)
	case *SplitOperation:
		o.format(w)
	case *MergeOperation:
		o.format(w)
	case *BatchOperation:
		newIndent := indent + `  `
		w.WriteString(`{`)
		w.WriteString("\n")
		w.WriteString(newIndent)
		w.WriteString(`b := &Batch{}`)
		formatOps(w, newIndent, `b`, o.Ops)
		w.WriteString("\n")
		w.WriteString(newIndent)
		w.WriteString(receiver)
		w.WriteString(`.Run(ctx, b)`)
		o.Result.format(w)
		w.WriteString("\n")
		w.WriteString(indent)
		w.WriteString(`}`)
	case *BeginTxnOperation:
		o.format(w)
	case *UseTxnOperation:
		newIndent := indent + `  `
		w.WriteString(`{`)
		formatOps(w, newIndent, `txn`+o.TxnID, o.Ops)
		w.WriteString("\n")
		w.WriteString(indent)
		w.WriteString(`}`)
	case *CommitTxnOperation:
		o.format(w)
	case *RollbackTxnOperation:
		o.format(w)
	default:
		fmt.Fprintf(w, "%v", op.GetValue())
	}
}

func (op GetOperation) format(w *strings.Builder, receiver string) {
	fmt.Fprintf(w, `%s.Get(ctx, %s)`, receiver, roachpb.Key(op.Key))
	switch op.Result.Type {
	case ResultType_Error:
		err := errors.DecodeError(context.TODO(), *op.Result.Err)
		fmt.Fprintf(w, ` -> (nil, %s)`, err.Error())
	case ResultType_Value:
		v := `nil`
		if len(op.Result.Value) > 0 {
			v = `"` + string(op.Result.Value) + `"`
		}
		fmt.Fprintf(w, ` -> (%s, nil)`, v)
	}
}

func (op PutOperation) format(w *strings.Builder, receiver string) {
	fmt.Fprintf(w, `%s.Put(ctx, %s, %s)`, receiver, roachpb.Key(op.Key), op.Value)
	op.Result.format(w)
}

func (op SplitOperation) format(w *strings.Builder) {
	fmt.Fprintf(w, `db.Split(ctx, %s)`, roachpb.Key(op.Key))
	op.Result.format(w)
}

func (op MergeOperation) format(w *strings.Builder) {
	fmt.Fprintf(w, `db.Merge(ctx, %s)`, roachpb.Key(op.Key))
	op.Result.format(w)
}

func (op BeginTxnOperation) format(w *strings.Builder) {
	fmt.Fprintf(w, `txn%s := db.NewTxn(ctx)`, op.TxnID)
}

func (op CommitTxnOperation) format(w *strings.Builder) {
	fmt.Fprintf(w, `txn%s.Commit(ctx)`, op.TxnID)
	op.Result.format(w)
}

func (op RollbackTxnOperation) format(w *strings.Builder) {
	fmt.Fprintf(w, `txn%s.Rollback(ctx)`, op.TxnID)
	op.Result.format(w)
}

func (r Result) format(w *strings.Builder) {
	switch r.Type {
	case ResultType_Error:
		err := errors.DecodeError(context.TODO(), *r.Err)
		fmt.Fprintf(w, ` -> %s`, err.Error())
	case ResultType_NoError:
		fmt.Fprintf(w, ` -> nil`)
	}
}
