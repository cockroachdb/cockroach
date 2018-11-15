// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package distsqlrun

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/backfill"
	"github.com/cockroachdb/cockroach/pkg/sql/pgwire/pgerror"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
)

// indexBackfiller is a processor that backfills new indexes.
type indexBackfiller struct {
	backfiller

	backfill.IndexBackfiller

	desc *sqlbase.ImmutableTableDescriptor
}

var _ Processor = &indexBackfiller{}
var _ chunkBackfiller = &indexBackfiller{}

func newIndexBackfiller(
	flowCtx *FlowCtx,
	processorID int32,
	spec BackfillerSpec,
	post *PostProcessSpec,
	output RowReceiver,
) (*indexBackfiller, error) {
	ib := &indexBackfiller{
		desc: sqlbase.NewImmutableTableDescriptor(spec.Table),
		backfiller: backfiller{
			name:        "Index",
			filter:      backfill.IndexMutationFilter,
			flowCtx:     flowCtx,
			processorID: processorID,
			output:      output,
			spec:        spec,
		},
	}
	ib.backfiller.chunkBackfiller = ib

	if err := ib.IndexBackfiller.Init(ib.desc); err != nil {
		return nil, err
	}

	return ib, nil
}

func (ib *indexBackfiller) runChunk(
	tctx context.Context,
	mutations []sqlbase.DescriptorMutation,
	sp roachpb.Span,
	chunkSize int64,
	readAsOf hlc.Timestamp,
) (roachpb.Key, error) {
	if ib.flowCtx.testingKnobs.RunBeforeBackfillChunk != nil {
		if err := ib.flowCtx.testingKnobs.RunBeforeBackfillChunk(sp); err != nil {
			return nil, err
		}
	}
	if ib.flowCtx.testingKnobs.RunAfterBackfillChunk != nil {
		defer ib.flowCtx.testingKnobs.RunAfterBackfillChunk()
	}

	ctx, traceSpan := tracing.ChildSpan(tctx, "chunk")
	defer tracing.FinishSpan(traceSpan)

	var key roachpb.Key
	transactionalChunk := func(ctx context.Context) error {
		return ib.flowCtx.ClientDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
			// TODO(knz): do KV tracing in DistSQL processors.
			var err error
			key, err = ib.RunIndexBackfillChunk(
				ctx, txn, ib.desc, sp, chunkSize, true /*alsoCommit*/, false /*traceKV*/)
			return err
		})
	}

	// TODO(jordan): enable this once IsMigrated is a real implementation.
	/*
		if !util.IsMigrated() {
			// If we're running a mixed cluster, some of the nodes will have an old
			// implementation of InitPut that doesn't take into account the expected
			// timetsamp. In that case, we have to run our chunk transactionally at the
			// current time.
			err := transactionalChunk(ctx)
			return ib.fetcher.Key(), err
		}
	*/

	var entries []sqlbase.IndexEntry
	if err := ib.flowCtx.ClientDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		txn.SetFixedTimestamp(ctx, readAsOf)

		// TODO(knz): do KV tracing in DistSQL processors.
		var err error
		entries, key, err = ib.BuildIndexEntriesChunk(ctx, txn, ib.desc, sp, chunkSize, false /*traceKV*/)
		return err
	}); err != nil {
		return nil, err
	}

	retried := false
	// Write the new index values.
	if err := ib.flowCtx.ClientDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		batch := txn.NewBatch()

		for _, entry := range entries {
			// Since we're not regenerating the index entries here, if the
			// transaction restarts the values might already have their checksums
			// set which is invalid - clear them.
			if retried {
				// Reset the value slice. This is necessary because gRPC may still be
				// holding onto the underlying slice here. See #17348 for more details.
				// We only need to reset RawBytes because neither entry nor entry.Value
				// are pointer types.
				rawBytes := entry.Value.RawBytes
				entry.Value.RawBytes = make([]byte, len(rawBytes))
				copy(entry.Value.RawBytes, rawBytes)
				entry.Value.ClearChecksum()
			}
			batch.InitPut(entry.Key, &entry.Value, true /* failOnTombstones */)
		}
		retried = true
		if err := txn.CommitInBatch(ctx, batch); err != nil {
			if _, ok := batch.MustPErr().GetDetail().(*roachpb.ConditionFailedError); ok {
				return pgerror.NewError(pgerror.CodeUniqueViolationError, "")
			}
			return err
		}
		return nil
	}); err != nil {
		if sqlbase.IsUniquenessConstraintViolationError(err) {
			log.VEventf(ctx, 2, "failed write. retrying transactionally: %v", err)
			// Someone wrote a value above one of our new index entries. Since we did
			// a historical read, we didn't have the most up-to-date value for the
			// row we were backfilling so we can't just blindly write it to the
			// index. Instead, we retry the transaction at the present timestamp.
			if err := transactionalChunk(ctx); err != nil {
				log.VEventf(ctx, 2, "failed transactional write: %v", err)
				return nil, err
			}
		} else {
			log.VEventf(ctx, 2, "failed write due to other error, not retrying: %v", err)
			return nil, err
		}
	}

	return key, nil
}
