// Copyright 2017 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// columnBackfiller is a processor for backfilling columns.
type columnBackfiller struct {
	backfiller

	backfill.ColumnBackfiller

	desc        *sqlbase.ImmutableTableDescriptor
	otherTables []*sqlbase.ImmutableTableDescriptor
}

var _ Processor = &columnBackfiller{}
var _ chunkBackfiller = &columnBackfiller{}

func newColumnBackfiller(
	flowCtx *FlowCtx,
	processorID int32,
	spec BackfillerSpec,
	post *PostProcessSpec,
	output RowReceiver,
) (*columnBackfiller, error) {
	otherTables := make([]*sqlbase.ImmutableTableDescriptor, len(spec.OtherTables))
	for i, tbl := range spec.OtherTables {
		otherTables[i] = sqlbase.NewImmutableTableDescriptor(tbl)
	}
	cb := &columnBackfiller{
		desc:        sqlbase.NewImmutableTableDescriptor(spec.Table),
		otherTables: otherTables,
		backfiller: backfiller{
			name:        "Column",
			filter:      backfill.ColumnMutationFilter,
			flowCtx:     flowCtx,
			processorID: processorID,
			output:      output,
			spec:        spec,
		},
	}
	cb.backfiller.chunkBackfiller = cb

	if err := cb.ColumnBackfiller.Init(cb.flowCtx.NewEvalCtx(), cb.desc); err != nil {
		return nil, err
	}

	return cb, nil
}

// runChunk implements the chunkBackfiller interface.
func (cb *columnBackfiller) runChunk(
	ctx context.Context,
	mutations []sqlbase.DescriptorMutation,
	sp roachpb.Span,
	chunkSize int64,
	readAsOf hlc.Timestamp,
) (roachpb.Key, error) {
	var key roachpb.Key
	err := cb.flowCtx.ClientDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		if cb.flowCtx.testingKnobs.RunBeforeBackfillChunk != nil {
			if err := cb.flowCtx.testingKnobs.RunBeforeBackfillChunk(sp); err != nil {
				return err
			}
		}
		if cb.flowCtx.testingKnobs.RunAfterBackfillChunk != nil {
			defer cb.flowCtx.testingKnobs.RunAfterBackfillChunk()
		}

		// TODO(knz): do KV tracing in DistSQL processors.
		var err error
		key, err = cb.RunColumnBackfillChunk(
			ctx,
			txn,
			cb.desc,
			cb.otherTables,
			sp,
			chunkSize,
			true,  /*alsoCommit*/
			false, /*traceKV*/
		)
		return err
	})
	return key, err
}
