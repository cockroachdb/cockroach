// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/errors"
)

// tableDeleter handles writing kvs and forming table rows for deletes.
type tableDeleter struct {
	tableWriterBase

	rd    row.Deleter
	alloc *tree.DatumAlloc
}

var _ tableWriter = &tableDeleter{}

// desc is part of the tableWriter interface.
func (*tableDeleter) desc() string { return "deleter" }

// walkExprs is part of the tableWriter interface.
func (td *tableDeleter) walkExprs(_ func(desc string, index int, expr tree.TypedExpr)) {}

// init is part of the tableWriter interface.
func (td *tableDeleter) init(
	_ context.Context, txn *kv.Txn, evalCtx *eval.Context, sv *settings.Values,
) error {
	return td.tableWriterBase.init(txn, td.tableDesc(), evalCtx, sv)
}

// row is part of the tableWriter interface.
func (td *tableDeleter) row(
	ctx context.Context, values tree.Datums, pm row.PartialIndexUpdateHelper, traceKV bool,
) error {
	td.currentBatchSize++
	return td.rd.DeleteRow(ctx, td.b, values, pm, traceKV)
}

// deleteIndex runs the kv operations necessary to delete all kv entries in the
// given index.
//
// resume is the resume-span which should be used for the index deletion
// when the index deletion is chunked. The first call to this method should
// use a zero resume-span. After a chunk of the index is deleted a new resume-
// span is returned.
//
// limit is a limit on the number of index entries deleted in the operation.
func (td *tableDeleter) deleteIndex(
	ctx context.Context, idx catalog.Index, resume roachpb.Span, limit int64, traceKV bool,
) (roachpb.Span, error) {
	if resume.Key == nil {
		resume = td.tableDesc().IndexSpan(td.rd.Helper.Codec, idx.GetID())
	}

	if traceKV {
		log.VEventf(ctx, 2, "DelRange %s - %s", resume.Key, resume.EndKey)
	}
	td.b.DelRange(resume.Key, resume.EndKey, false /* returnKeys */)
	td.b.Header.MaxSpanRequestKeys = limit
	if err := td.finalize(ctx); err != nil {
		return resume, err
	}
	if l := len(td.b.Results); l != 1 {
		panic(errors.AssertionFailedf("%d results returned, expected 1", l))
	}
	return td.b.Results[0].ResumeSpanAsValue(), nil
}

func (td *tableDeleter) tableDesc() catalog.TableDescriptor {
	return td.rd.Helper.TableDesc
}
