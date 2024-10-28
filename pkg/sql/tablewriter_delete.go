// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
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

// init initializes the tableDeleter with a Txn.
func (td *tableDeleter) init(_ context.Context, txn *kv.Txn, evalCtx *eval.Context) error {
	return td.tableWriterBase.init(txn, td.tableDesc(), evalCtx)
}

// row performs a delete.
//
// The passed Datums is not used after `row` returns.
//
// The PartialIndexUpdateHelper is used to determine which partial indexes
// to avoid updating when performing row modification. This is necessary
// because not all rows are indexed by partial indexes.
//
// The traceKV parameter determines whether the individual K/V operations
// should be logged to the context. We use a separate argument here instead
// of a Value field on the context because Value access in context.Context
// is rather expensive.
func (td *tableDeleter) row(
	ctx context.Context, values tree.Datums, pm row.PartialIndexUpdateHelper, traceKV bool,
) error {
	td.currentBatchSize++
	return td.rd.DeleteRow(ctx, td.b, values, pm, nil, traceKV)
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

// tableDesc returns the TableDescriptor for the table that the tableDeleter
// will modify.
func (td *tableDeleter) tableDesc() catalog.TableDescriptor {
	return td.rd.Helper.TableDesc
}
