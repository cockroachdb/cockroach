// Copyright 2024 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package logical

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/cdcevent"
	"github.com/cockroachdb/cockroach/pkg/ccl/changefeedccl/changefeedbase"
	"github.com/cockroachdb/cockroach/pkg/jobs/jobspb"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/lease"
	"github.com/cockroachdb/cockroach/pkg/sql/execinfra"
	"github.com/cockroachdb/cockroach/pkg/sql/isql"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/eval"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/errors"
)

// A kvRowProcessor is a RowProcessor that bypasses SQL execution and directly
// writes KV pairs for replicated events.
type kvRowProcessor struct {
	decoder cdcevent.Decoder
	lastRow cdcevent.Row
	alloc   *tree.DatumAlloc
	cfg     *execinfra.ServerConfig
	evalCtx *eval.Context

	dstBySrc map[descpb.ID]descpb.ID
	writers  map[descpb.ID]*kvTableWriter

	fallback *sqlRowProcessor
	failureInjector
}

// newKVRowProcessor returns a RowProcessor that operates by writing KV pairs
// directly to the db, bypassing SQL Query execution.
func newKVRowProcessor(
	ctx context.Context,
	cfg *execinfra.ServerConfig,
	evalCtx *eval.Context,
	srcTablesByDestID map[descpb.ID]sqlProcessorTableConfig,
	fallback *sqlRowProcessor,
) (*kvRowProcessor, error) {
	cdcEventTargets := changefeedbase.Targets{}
	srcTablesBySrcID := make(map[descpb.ID]catalog.TableDescriptor, len(srcTablesByDestID))
	dstBySrc := make(map[descpb.ID]descpb.ID, len(srcTablesByDestID))

	for dstID, s := range srcTablesByDestID {
		dstBySrc[s.srcDesc.GetID()] = dstID
		srcTablesBySrcID[s.srcDesc.GetID()] = s.srcDesc
		cdcEventTargets.Add(changefeedbase.Target{
			Type:              jobspb.ChangefeedTargetSpecification_EACH_FAMILY,
			TableID:           s.srcDesc.GetID(),
			StatementTimeName: changefeedbase.StatementTimeName(s.srcDesc.GetName()),
		})
	}

	prefixlessCodec := keys.SystemSQLCodec
	rfCache, err := cdcevent.NewFixedRowFetcherCache(
		ctx, prefixlessCodec, evalCtx.Settings, cdcEventTargets, srcTablesBySrcID,
	)
	if err != nil {
		return nil, err
	}

	p := &kvRowProcessor{
		cfg:      cfg,
		evalCtx:  evalCtx,
		dstBySrc: dstBySrc,
		writers:  make(map[descpb.ID]*kvTableWriter, len(srcTablesByDestID)),
		decoder:  cdcevent.NewEventDecoderWithCache(ctx, rfCache, false, false),
		alloc:    &tree.DatumAlloc{},
		fallback: fallback,
	}
	return p, nil
}

var originID1Options = &kvpb.WriteOptions{OriginID: 1}

func (p *kvRowProcessor) ProcessRow(
	ctx context.Context, txn isql.Txn, keyValue roachpb.KeyValue, prevValue roachpb.Value,
) (batchStats, error) {
	if err := p.injectFailure(); err != nil {
		return batchStats{}, err
	}

	var err error
	keyValue.Key, err = keys.StripTenantPrefix(keyValue.Key)
	if err != nil {
		return batchStats{}, errors.Wrap(err, "stripping tenant prefix")
	}

	row, err := p.decoder.DecodeKV(ctx, keyValue, cdcevent.CurrentRow, keyValue.Value.Timestamp, false)
	if err != nil {
		p.lastRow = cdcevent.Row{}
		return batchStats{}, errors.Wrap(err, "decoding KeyValue")
	}
	p.lastRow = row

	// TODO(dt, ssd): the rangefeed prev value does not include its mvcc ts, which
	// is a problem for us if we want to use CPut to replace the old row with the
	// new row, because our local version of the old row is likely to have the
	// remote version's mvcc timestamp in its origin ts column, i.e. in the value.
	// Without knowing the remote previous row's ts, we cannot exactly reconstruct
	// the value of our local row to put in the expected value for a CPut.
	// Instead, for now, we just don't use the direct CPut for anything other than
	// inserts. If/when we have a LDR-flavor CPut (or if we move the TS out and
	// decide that equal values negate LWW) we can remove this.
	if prevValue.IsPresent() {
		return p.fallback.processParsedRow(ctx, txn, row, keyValue.Key, prevValue)
	}

	if err := p.processParsedRow(ctx, txn, row, keyValue, prevValue); err != nil {
		stats, err := p.fallback.processParsedRow(ctx, txn, row, keyValue.Key, prevValue)
		if err == nil {
			stats.kvWriteFallbacks += 1
		}
		return stats, err
	}
	return batchStats{}, nil

}

func (p *kvRowProcessor) processParsedRow(
	ctx context.Context, txn isql.Txn, row cdcevent.Row, k roachpb.KeyValue, prevValue roachpb.Value,
) error {
	dstTableID, ok := p.dstBySrc[row.TableID]
	if !ok {
		return errors.AssertionFailedf("replication configuration missing for table %d / %q", row.TableID, row.TableName)
	}

	makeBatch := func(txn *kv.Txn) *kv.Batch {
		b := txn.NewBatch()
		b.Header.WriteOptions = originID1Options
		b.AdmissionHeader.Priority = int32(admissionpb.BulkLowPri)
		b.AdmissionHeader.Source = kvpb.AdmissionHeader_FROM_SQL
		return b
	}

	if txn == nil {
		if err := p.cfg.DB.KV().Txn(ctx, func(ctx context.Context, txn *kv.Txn) error {
			b := makeBatch(txn)

			if err := p.addToBatch(ctx, txn, b, dstTableID, row, k, prevValue); err != nil {
				return err
			}
			return txn.CommitInBatch(ctx, b)
		}); err != nil {
			return err
		}

		return nil
	}

	kvTxn := txn.KV()
	b := makeBatch(kvTxn)

	if err := p.addToBatch(ctx, kvTxn, b, dstTableID, row, k, prevValue); err != nil {
		return err
	}
	return txn.KV().Run(ctx, b)
}

func (p *kvRowProcessor) addToBatch(
	ctx context.Context,
	txn *kv.Txn,
	b *kv.Batch,
	dstTableID descpb.ID,
	row cdcevent.Row,
	keyValue roachpb.KeyValue,
	prevValue roachpb.Value,
) error {
	w, err := p.getWriter(ctx, dstTableID, txn.ProvisionalCommitTimestamp())
	if err != nil {
		return err
	}
	// This batch should only commit if it can do so prior to the expiration of
	// the lease of the descriptor used to encode it.
	if err := txn.UpdateDeadline(ctx, w.leased.Expiration(ctx)); err != nil {
		return err
	}
	if prevValue.IsPresent() {
		prevRow, err := p.decoder.DecodeKV(ctx, roachpb.KeyValue{
			Key:   keyValue.Key,
			Value: prevValue,
		}, cdcevent.PrevRow, prevValue.Timestamp, false)
		if err != nil {
			return err
		}

		if row.IsDeleted() {
			if err := w.deleteRow(ctx, b, prevRow, row); err != nil {
				return err
			}
		} else {
			if err := w.updateRow(ctx, b, prevRow, row); err != nil {
				return err
			}
		}
	} else {
		if err := w.insertRow(ctx, b, row); err != nil {
			return err
		}
	}
	return nil
}

// GetLastRow implements the RowProcessor interface.
func (p *kvRowProcessor) GetLastRow() cdcevent.Row {
	return p.lastRow
}

// SetSyntheticFailurePercent implements the RowProcessor interface.
func (p *kvRowProcessor) SetSyntheticFailurePercent(rate uint32) {
	// TODO(dt): support failure injection.
}

func (p *kvRowProcessor) Close(ctx context.Context) {
	for _, w := range p.writers {
		w.leased.Release(ctx)
	}
}

func (p *kvRowProcessor) getWriter(
	ctx context.Context, id descpb.ID, ts hlc.Timestamp,
) (*kvTableWriter, error) {
	w, ok := p.writers[id]
	if ok {
		// If the lease is still valid, just use the writer.
		if w.leased.Expiration(ctx).After(ts) {
			return w, nil
		}
		// The lease is invalid; we'll be getting a new one so release this one.
		w.leased.Release(ctx)
	}

	l, err := p.cfg.LeaseManager.(*lease.Manager).Acquire(ctx, ts, id)
	if err != nil {
		return nil, err
	}

	// If the new lease just so happened to be the same version, we can just swap
	// the lease in the existing writer.
	if ok && l.Underlying().GetVersion() == w.leased.Underlying().GetVersion() {
		w.leased = l
		return w, nil
	}

	// New lease and desc version; make a new writer.
	w, err = newKVTableWriter(ctx, l, p.alloc, p.evalCtx)
	if err != nil {
		return nil, err
	}

	p.writers[id] = w
	return w, nil
}

// kvTableWriter writes row changes for a specific table using a leased desc for
// that table to a passed batch. An instance of a kvTableWriter should not be
// used past the expiration of the lease used to construct it, and batches that
// it populates should commit no later than the expiration of said lease.
type kvTableWriter struct {
	leased           lease.LeasedDescriptor
	newVals, oldVals []tree.Datum
	ru               row.Updater
	ri               row.Inserter
	rd               row.Deleter
	scratchTS        tree.DDecimal
}

func newKVTableWriter(
	ctx context.Context, leased lease.LeasedDescriptor, a *tree.DatumAlloc, evalCtx *eval.Context,
) (*kvTableWriter, error) {

	tableDesc := leased.Underlying().(catalog.TableDescriptor)

	const internal = true

	// TODO(dt): figure out the right sets of columns here and in fillNew/fillOld.
	readCols, writeCols := tableDesc.PublicColumns(), tableDesc.PublicColumns()

	// TODO(dt): pass these some sort fo flag to have them use versions of CPut
	// or a new LWW KV API. For now they're not detecting/handling conflicts.
	ri, err := row.MakeInserter(ctx, nil, evalCtx.Codec, tableDesc, nil /* uniqueWithTombstoneIndexes */, writeCols, a, &evalCtx.Settings.SV, internal, nil)
	if err != nil {
		return nil, err
	}
	rd := row.MakeDeleter(evalCtx.Codec, tableDesc, readCols, &evalCtx.Settings.SV, internal, nil)
	ru, err := row.MakeUpdater(
		ctx, nil, evalCtx.Codec, tableDesc, nil /* uniqueWithTombstoneIndexes */, readCols, writeCols, row.UpdaterDefault, a, &evalCtx.Settings.SV, internal, nil,
	)
	if err != nil {
		return nil, err
	}

	return &kvTableWriter{
		leased:  leased,
		oldVals: make([]tree.Datum, len(readCols)),
		newVals: make([]tree.Datum, len(writeCols)),
		ri:      ri,
		rd:      rd,
		ru:      ru,
	}, nil
}

func (p *kvTableWriter) insertRow(ctx context.Context, b *kv.Batch, after cdcevent.Row) error {
	if err := p.fillNew(after); err != nil {
		return err
	}

	var ph row.PartialIndexUpdateHelper
	// TODO(dt): support partial indexes.

	return p.ri.InsertRow(ctx, &row.KVBatchAdapter{Batch: b}, p.newVals, ph, false, false)
}

func (p *kvTableWriter) updateRow(
	ctx context.Context, b *kv.Batch, before, after cdcevent.Row,
) error {
	if err := p.fillOld(before); err != nil {
		return err
	}
	if err := p.fillNew(after); err != nil {
		return err
	}

	var ph row.PartialIndexUpdateHelper
	// TODO(dt): support partial indexes.

	_, err := p.ru.UpdateRow(ctx, b, p.oldVals, p.newVals, ph, false)
	return err
}

func (p *kvTableWriter) deleteRow(
	ctx context.Context, b *kv.Batch, before, after cdcevent.Row,
) error {
	if err := p.fillOld(before); err != nil {
		return err
	}

	var ph row.PartialIndexUpdateHelper
	// TODO(dt): support partial indexes.

	return p.rd.DeleteRow(ctx, b, p.oldVals, ph, false)
}

func (p *kvTableWriter) fillOld(vals cdcevent.Row) error {
	p.oldVals = p.oldVals[:0]
	if err := vals.ForAllColumns().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		// TODO(dt): add indirection from col ID to offset.
		p.oldVals = append(p.oldVals, d)
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func (p *kvTableWriter) fillNew(vals cdcevent.Row) error {
	p.newVals = p.newVals[:0]
	if err := vals.ForAllColumns().Datum(func(d tree.Datum, col cdcevent.ResultColumn) error {
		// TODO(dt): add indirection from col ID to offset.
		if col.Name == originTimestampColumnName {
			p.scratchTS.Decimal = eval.TimestampToDecimal(vals.MvccTimestamp)
			d = &p.scratchTS
		}
		p.newVals = append(p.newVals, d)
		return nil
	}); err != nil {
		return err
	}
	return nil
}
