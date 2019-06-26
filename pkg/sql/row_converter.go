// Copyright 2017 The Cockroach Authors.
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

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/builtins"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/transform"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/sql/types"
	"github.com/pkg/errors"
)

// Inserter implements the putter interface.
type Inserter func(roachpb.KeyValue)

// CPut is not implmented.
func (i Inserter) CPut(key, value, expValue interface{}) {
	panic("unimplemented")
}

// Del is not implemented.
func (i Inserter) Del(key ...interface{}) {
	panic("unimplemented")
}

// Put method of the putter interface.
func (i Inserter) Put(key, value interface{}) {
	i(roachpb.KeyValue{
		Key:   *key.(*roachpb.Key),
		Value: *value.(*roachpb.Value),
	})
}

// InitPut method of the putter interface.
func (i Inserter) InitPut(key, value interface{}, failOnTombstones bool) {
	i(roachpb.KeyValue{
		Key:   *key.(*roachpb.Key),
		Value: *value.(*roachpb.Value),
	})
}

// RowConverter converts Datums into kvs and streams it to the destination
// channel.
type RowConverter struct {
	// current row buf
	Datums []tree.Datum

	// kv destination and current batch
	KvCh     chan<- []roachpb.KeyValue
	KvBatch  []roachpb.KeyValue
	BatchCap int

	tableDesc *sqlbase.ImmutableTableDescriptor

	// The rest of these are derived from tableDesc, just cached here.
	hidden                int
	ri                    row.Inserter
	EvalCtx               *tree.EvalContext
	cols                  []sqlbase.ColumnDescriptor
	VisibleCols           []sqlbase.ColumnDescriptor
	VisibleColTypes       []*types.T
	defaultExprs          []tree.TypedExpr
	computedIVarContainer sqlbase.RowIndexedVarContainer
}

const kvRowConverterBatchSize = 5000

// NewRowConverter returns an instance of a RowConverter.
func NewRowConverter(
	tableDesc *sqlbase.TableDescriptor, evalCtx *tree.EvalContext, kvCh chan<- []roachpb.KeyValue,
) (*RowConverter, error) {
	immutDesc := sqlbase.NewImmutableTableDescriptor(*tableDesc)
	c := &RowConverter{
		tableDesc: immutDesc,
		KvCh:      kvCh,
		EvalCtx:   evalCtx,
	}

	ri, err := row.MakeInserter(nil /* txn */, immutDesc, nil, /* fkTables */
		immutDesc.Columns, false /* checkFKs */, &sqlbase.DatumAlloc{})
	if err != nil {
		return nil, errors.Wrap(err, "make row inserter")
	}
	c.ri = ri

	var txCtx transform.ExprTransformContext
	// Although we don't yet support DEFAULT expressions on visible columns,
	// we do on hidden columns (which is only the default _rowid one). This
	// allows those expressions to run.
	cols, defaultExprs, err := sqlbase.ProcessDefaultColumns(immutDesc.Columns, immutDesc, &txCtx, c.EvalCtx)
	if err != nil {
		return nil, errors.Wrap(err, "process default columns")
	}
	c.cols = cols
	c.defaultExprs = defaultExprs

	c.VisibleCols = immutDesc.VisibleColumns()
	c.VisibleColTypes = make([]*types.T, len(c.VisibleCols))
	for i := range c.VisibleCols {
		c.VisibleColTypes[i] = c.VisibleCols[i].DatumType()
	}
	c.Datums = make([]tree.Datum, len(c.VisibleCols), len(cols))

	// Check for a hidden column. This should be the unique_rowid PK if present.
	c.hidden = -1
	for i := range cols {
		col := &cols[i]
		if col.Hidden {
			if col.DefaultExpr == nil || *col.DefaultExpr != "unique_rowid()" || c.hidden != -1 {
				return nil, errors.New("unexpected hidden column")
			}
			c.hidden = i
			c.Datums = append(c.Datums, nil)
		}
	}
	if len(c.Datums) != len(cols) {
		return nil, errors.New("unexpected hidden column")
	}

	padding := 2 * (len(immutDesc.Indexes) + len(immutDesc.Families))
	c.BatchCap = kvRowConverterBatchSize + padding
	c.KvBatch = make([]roachpb.KeyValue, 0, c.BatchCap)

	c.computedIVarContainer = sqlbase.RowIndexedVarContainer{
		Mapping: ri.InsertColIDtoRowIndex,
		Cols:    immutDesc.Columns,
	}
	return c, nil
}

// Row inserts kv operations into the current kv batch, and triggers a SendBatch
// if necessary.
func (c *RowConverter) Row(ctx context.Context, fileIndex int32, rowIndex int64) error {
	if c.hidden >= 0 {
		// We don't want to call unique_rowid() for the hidden PK column because
		// it is not idempotent. The sampling from the first stage will be useless
		// during the read phase, producing a single range split with all of the
		// data. Instead, we will call our own function that mimics that function,
		// but more-or-less guarantees that it will not interfere with the numbers
		// that will be produced by it. The lower 15 bits mimic the node id, but as
		// the CSV file number. The upper 48 bits are the line number and mimic the
		// timestamp. It would take a file with many more than 2**32 lines to even
		// begin approaching what unique_rowid would return today, so we assume it
		// to be safe. Since the timestamp is won't overlap, it is safe to use any
		// number in the node id portion. The 15 bits in that portion should account
		// for up to 32k CSV files in a single IMPORT. In the case of > 32k files,
		// the data is xor'd so the final bits are flipped instead of set.
		c.Datums[c.hidden] = tree.NewDInt(builtins.GenerateUniqueID(fileIndex, uint64(rowIndex)))
	}

	// TODO(justin): we currently disallow computed columns in import statements.
	var computeExprs []tree.TypedExpr
	var computedCols []sqlbase.ColumnDescriptor

	insertRow, err := GenerateInsertRow(
		c.defaultExprs, computeExprs, c.cols, computedCols, c.EvalCtx, c.tableDesc, c.Datums, &c.computedIVarContainer)
	if err != nil {
		return errors.Wrap(err, "generate insert row")
	}
	if err := c.ri.InsertRow(
		ctx,
		Inserter(func(kv roachpb.KeyValue) {
			kv.Value.InitChecksum(kv.Key)
			c.KvBatch = append(c.KvBatch, kv)
		}),
		insertRow,
		true, /* ignoreConflicts */
		row.SkipFKs,
		false, /* traceKV */
	); err != nil {
		return errors.Wrap(err, "insert row")
	}
	// If our batch is full, flush it and start a new one.
	if len(c.KvBatch) >= kvRowConverterBatchSize {
		if err := c.SendBatch(ctx); err != nil {
			return err
		}
	}
	return nil
}

// SendBatch streams kv operations from the current KvBatch to the destination
// channel, and resets the KvBatch to empty.
func (c *RowConverter) SendBatch(ctx context.Context) error {
	if len(c.KvBatch) == 0 {
		return nil
	}
	select {
	case c.KvCh <- c.KvBatch:
	case <-ctx.Done():
		return ctx.Err()
	}
	c.KvBatch = make([]roachpb.KeyValue, 0, c.BatchCap)
	return nil
}
