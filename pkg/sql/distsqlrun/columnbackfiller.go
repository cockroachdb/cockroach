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
}

var _ Processor = &columnBackfiller{}
var _ chunkBackfiller = &columnBackfiller{}

func newColumnBackfiller(
	flowCtx *FlowCtx, spec BackfillerSpec, post *PostProcessSpec, output RowReceiver,
) (*columnBackfiller, error) {
	cb := &columnBackfiller{
		backfiller: backfiller{
			name:    "Column",
			filter:  backfill.ColumnMutationFilter,
			flowCtx: flowCtx,
			output:  output,
			spec:    spec,
		},
	}
	cb.backfiller.chunkBackfiller = cb

	if err := cb.ColumnBackfiller.Init(cb.flowCtx.NewEvalCtx(), cb.spec.Table); err != nil {
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
	tableDesc := cb.backfiller.spec.Table
	var key roachpb.Key
	err := cb.flowCtx.clientDB.Txn(ctx, func(ctx context.Context, txn *client.Txn) error {
		if cb.flowCtx.testingKnobs.RunBeforeBackfillChunk != nil {
			if err := cb.flowCtx.testingKnobs.RunBeforeBackfillChunk(sp); err != nil {
				return err
			}
		}
		if cb.flowCtx.testingKnobs.RunAfterBackfillChunk != nil {
			defer cb.flowCtx.testingKnobs.RunAfterBackfillChunk()
		}

		var err error
		key, err = cb.RunColumnBackfillChunk(
			ctx,
			txn,
			tableDesc,
			cb.backfiller.spec.OtherTables,
			sp,
			chunkSize,
			true, /*alsoCommit*/
		)
		return err
	})
	return key, err
}
