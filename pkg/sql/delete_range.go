// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package sql

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/fetchpb"
	"github.com/cockroachdb/cockroach/pkg/sql/row"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/errors"
)

// deleteRangeNode implements DELETE on a primary index satisfying certain
// conditions that permit the direct use of the DeleteRange kv operation,
// instead of many point deletes.
//
// Note: deleteRangeNode can't autocommit in the general case, because it has to
// delete in batches, and it won't know whether or not there is more work to do
// until after a batch is returned. This property precludes using auto commit.
// However, if the optimizer can prove that only a small number of rows will
// be deleted, it'll enable autoCommit for delete range.
type deleteRangeNode struct {
	zeroInputPlanNode
	rowsAffectedOutputHelper
	// spans are the spans to delete. The slice is owned by the deleteRangeNode.
	spans roachpb.Spans
	// desc is the table descriptor the delete is operating on.
	desc catalog.TableDescriptor
	// singleColumnFamily, if set, indicates that the table has only one column
	// family.
	singleColumnFamily bool
	// fetcher is used to decode the returned keys from the DeleteRange, so that
	// we can count the number of rows deleted. It's only used when the table
	// has multiple column families (i.e. singleColumnFamily is false).
	fetcher row.Fetcher

	// autoCommitEnabled is set to true if the optimizer proved that we can safely
	// use autocommit - so that the number of possible returned keys from this
	// operation is low. If this is true, we won't attempt to run the delete in
	// batches and will just send one big delete with a commit statement attached.
	autoCommitEnabled bool

	// curRowPrefix is the prefix for all KVs (i.e. for all column families) of
	// the SQL row that increased rowCount last. It is maintained across
	// different BatchRequests in order to not double count the same SQL row. It
	// is maintained only when singleColumnFamily is false.
	curRowPrefix []byte

	// kvCPUTimeAccum tracks the cumulative CPU time (in nanoseconds) that KV
	// reported in BatchResponse headers during the execution of this delete range.
	kvCPUTimeAccum int64

	// localKVCPUTimeAccum tracks the cumulative SQL goroutine CPU time (in
	// nanoseconds) spent inside KV calls during delete range execution, as
	// measured by the grunning library. This is the portion of SQL goroutine
	// CPU that overlapped with KV work, not the CPU consumed on KV servers (see
	// kvCPUTimeAccum for that).
	localKVCPUTimeAccum int64
	// cpuStopWatch measures grunning time around KV calls.
	cpuStopWatch timeutil.CPUStopWatch
}

var _ planNode = &deleteRangeNode{}
var _ mutationPlanNode = &deleteRangeNode{}

func (d *deleteRangeNode) rowsWritten() int64 {
	return d.rowsAffected()
}

func (d *deleteRangeNode) indexRowsWritten() int64 {
	// Same as rowsWritten, because deleteRangeNode only applies to primary index
	// rows (it is not used if there's a secondary index on the table).
	return d.rowsAffected()
}

func (d *deleteRangeNode) indexBytesWritten() int64 {
	// No bytes counted as written for a deletion.
	return 0
}

func (d *deleteRangeNode) returnsRowsAffected() bool {
	// DeleteRange always returns the number of rows deleted.
	return true
}

func (d *deleteRangeNode) kvCPUTime() int64 {
	return d.kvCPUTimeAccum
}

func (d *deleteRangeNode) localKVCPUTime() int64 {
	return d.localKVCPUTimeAccum
}

// startExec implements the planNode interface.
func (d *deleteRangeNode) startExec(params runParams) error {
	if err := params.p.cancelChecker.Check(); err != nil {
		return err
	}

	if !d.singleColumnFamily {
		// Configure the fetcher, which is only used to decode the returned keys
		// from the Del and the DelRange operations, and is never used to
		// actually fetch kvs.
		var spec fetchpb.IndexFetchSpec
		if err := rowenc.InitIndexFetchSpec(
			&spec, params.ExecCfg().Codec, d.desc, d.desc.GetPrimaryIndex(), nil, /* fetchColumnIDs */
		); err != nil {
			return err
		}
		if err := d.fetcher.Init(
			params.ctx,
			row.FetcherInitArgs{
				WillUseKVProvider: true,
				Alloc:             &tree.DatumAlloc{},
				Spec:              &spec,
			},
		); err != nil {
			return err
		}
	}

	ctx := params.ctx
	log.VEvent(ctx, 2, "fast delete: skipping scan")
	// We own the slice, so we can modify it as we please.
	spans := d.spans
	if !d.autoCommitEnabled {
		// Without autocommit, we're going to run each batch one by one, respecting
		// a max span request keys size. We use spans as a queue of spans to delete.
		// It'll be edited if there are any resume spans encountered (if any request
		// hits the key limit).
		for len(spans) != 0 {
			b := params.p.txn.NewBatch()
			b.Header.MaxSpanRequestKeys = int64(row.DeleteRangeChunkSize(params.extendedEvalCtx.TestingKnobs.ForceProductionValues))
			b.Header.LockTimeout = params.SessionData().LockTimeout
			b.Header.DeadlockTimeout = params.SessionData().DeadlockTimeout
			d.deleteSpans(params, b, spans)
			log.VEventf(ctx, 2, "fast delete: processing %d spans", len(spans))
			d.cpuStopWatch.Start()
			runErr := params.p.txn.Run(ctx, b)
			if delta := d.cpuStopWatch.Stop(); delta > 0 {
				d.localKVCPUTimeAccum += int64(delta)
			}
			if runErr != nil {
				return row.ConvertBatchError(ctx, d.desc, b, false /* alwaysConvertCondFailed */)
			}

			spans = spans[:0]
			var err error
			if spans, err = d.processResults(b, spans); err != nil {
				return err
			}
		}
	} else {
		log.Event(ctx, "autocommit enabled")
		// With autocommit, we're going to run the deleteRange in a single batch
		// without a limit, since limits and deleteRange aren't compatible with 1pc
		// transactions / autocommit. This isn't inherently safe, because without a
		// limit, this command could technically use up unlimited memory. However,
		// the optimizer only enables autoCommit if the maximum possible number of
		// keys to delete in this command are low, so we're made safe.
		b := params.p.txn.NewBatch()
		b.Header.LockTimeout = params.SessionData().LockTimeout
		b.Header.DeadlockTimeout = params.SessionData().DeadlockTimeout
		d.deleteSpans(params, b, spans)
		log.VEventf(ctx, 2, "fast delete: processing %d spans and committing", len(spans))
		d.cpuStopWatch.Start()
		commitErr := params.p.txn.CommitInBatch(ctx, b)
		if delta := d.cpuStopWatch.Stop(); delta > 0 {
			d.localKVCPUTimeAccum += int64(delta)
		}
		if commitErr != nil {
			return row.ConvertBatchError(ctx, d.desc, b, false /* alwaysConvertCondFailed */)
		}

		if resumeSpans, err := d.processResults(b, nil /* resumeSpans */); err != nil {
			return err
		} else if len(resumeSpans) != 0 {
			// This shouldn't ever happen - we didn't pass a limit into the batch.
			return errors.AssertionFailedf("deleteRange without a limit unexpectedly returned resumeSpans")
		}
	}

	// Possibly initiate a run of CREATE STATISTICS.
	params.ExecCfg().StatsRefresher.NotifyMutation(params.ctx, d.desc, d.rowCount)

	return nil
}

// deleteSpans adds each input span to a Del or a DelRange command in the given
// batch.
func (d *deleteRangeNode) deleteSpans(params runParams, b *kv.Batch, spans roachpb.Spans) {
	ctx := params.ctx
	traceKV := params.p.ExtendedEvalContext().Tracing.KVTracingEnabled()
	for _, span := range spans {
		if span.EndKey == nil {
			if traceKV {
				log.VEventf(ctx, 2, "Del (locking) %s", span.Key)
			}
			// We use the locking Del here unconditionally since:
			// - if buffered writes are enabled, since we haven't performed the
			// read, we need to tell the KV layer to acquire the lock
			// explicitly.
			// - if buffered writes are disabled, then the KV layer will write
			// an intent which acts as a lock.
			b.DelMustAcquireExclusiveLock(span.Key)
		} else {
			if traceKV {
				log.VEventf(ctx, 2, "DelRange %s - %s", span.Key, span.EndKey)
			}
			b.DelRange(span.Key, span.EndKey, true /* returnKeys */)
		}
	}
}

// processResults parses the results of a DelRangeResponse, incrementing the
// rowCount we're going to return for each row. If any resume spans are
// encountered during result processing, they're appended to the resumeSpans
// input parameter.
func (d *deleteRangeNode) processResults(
	b *kv.Batch, resumeSpans []roachpb.Span,
) (roachpb.Spans, error) {
	results := b.Results
	if br := b.RawResponse(); br != nil && br.CPUTime > 0 {
		d.kvCPUTimeAccum += br.CPUTime
	}

	if !d.autoCommitEnabled && !d.singleColumnFamily {
		defer func() {
			// Make a copy of curRowPrefix to avoid referencing the memory from
			// the now-old BatchRequest.
			//
			// When auto-commit is enabled, we expect to not see any resume
			// spans, so we won't need to access d.curRowPrefix later.
			curRowPrefix := make([]byte, len(d.curRowPrefix))
			copy(curRowPrefix, d.curRowPrefix)
			d.curRowPrefix = curRowPrefix
		}()
	}
	for _, r := range results {
		if d.singleColumnFamily {
			d.addAffectedRows(len(r.Keys))
		} else {
			for _, keyBytes := range r.Keys {
				// If prefix is same, don't bother decoding key.
				if len(d.curRowPrefix) > 0 && bytes.HasPrefix(keyBytes, d.curRowPrefix) {
					continue
				}

				after, _, err := d.fetcher.DecodeIndexKey(keyBytes)
				if err != nil {
					return nil, err
				}
				k := keyBytes[:len(keyBytes)-len(after)]
				if !bytes.Equal(k, d.curRowPrefix) {
					d.curRowPrefix = k
					d.incAffectedRows()
				}
			}
		}
		if r.ResumeSpan != nil && r.ResumeSpan.Valid() {
			resumeSpans = append(resumeSpans, *r.ResumeSpan)
		}
	}
	return resumeSpans, nil
}

// Next implements the planNode interface.
func (d *deleteRangeNode) Next(params runParams) (bool, error) {
	return d.next(), nil
}

// Values implements the planNode interface.
func (d *deleteRangeNode) Values() tree.Datums {
	return d.values()
}

// Close implements the planNode interface.
func (*deleteRangeNode) Close(ctx context.Context) {}
