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
//
// Author: Vivek Menezes (vivek@cockroachlabs.com)

package distsqlrun

import (
	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/tracing"
	"github.com/pkg/errors"
)

// WriteResumeSpan writes a checkpoint for the backfill work on origSpan.
// origSpan is the span of keys that were assigned to be backfilled,
// resume is the left over work from origSpan.
func WriteResumeSpan(
	ctx context.Context,
	db *client.DB,
	id sqlbase.ID,
	origSpan roachpb.Span,
	resume roachpb.Span,
	mutationIdx int,
) error {
	ctx, traceSpan := tracing.ChildSpan(ctx, "checkpoint")
	defer tracing.FinishSpan(traceSpan)
	if resume.Key != nil && !resume.EndKey.Equal(origSpan.EndKey) {
		panic("resume must end on the same key as origSpan")
	}
	cnt := 0
	return db.Txn(ctx, func(txn *client.Txn) error {
		cnt++
		tableDesc, err := sqlbase.GetTableDescFromID(txn, id)
		if err != nil {
			return err
		}
		if cnt > 1 {
			log.Infof(ctx, "retrying adding checkpoint %s to table %s", resume, tableDesc.Name)
		}

		// This loop is finding a span in the checkpoint that fits
		// origSpan. It then carves a spot for origSpan in the
		// checkpoint, and replaces origSpan in the checkpoint with
		// resume.
		mutation := &tableDesc.Mutations[mutationIdx]
		for i, sp := range mutation.ResumeSpans {
			if sp.Key.Compare(origSpan.Key) <= 0 &&
				sp.EndKey.Compare(origSpan.EndKey) >= 0 {
				// origSpan is in sp; split sp if needed to accommodate
				// origSpan and replace origSpan with resume.
				before := mutation.ResumeSpans[:i]
				after := append([]roachpb.Span{}, mutation.ResumeSpans[i+1:]...)

				// add span to before, but merge it with the last span
				// if possible.
				addSpan := func(begin, end roachpb.Key) {
					if begin.Equal(end) {
						return
					}
					if len(before) > 0 && before[len(before)-1].EndKey.Equal(begin) {
						before[len(before)-1].EndKey = end
					} else {
						before = append(before, roachpb.Span{Key: begin, EndKey: end})
					}
				}

				// The work done = [origSpan.Key...resume.Key]
				addSpan(sp.Key, origSpan.Key)
				if resume.Key != nil {
					addSpan(resume.Key, resume.EndKey)
				} else {
					log.VEventf(txn.Context, 2, "completed processing of span: %+v", origSpan)
				}
				addSpan(origSpan.EndKey, sp.EndKey)
				mutation.ResumeSpans = append(before, after...)

				log.VEventf(txn.Context, 2, "ckpt %+v", mutation.ResumeSpans)
				txn.SetSystemConfigTrigger()
				return txn.Put(sqlbase.MakeDescMetadataKey(tableDesc.GetID()), sqlbase.WrapDescriptor(tableDesc))
			}
		}
		// Unable to find a span containing origSpan?
		return errors.Errorf(
			"span %+v not found among %+v", origSpan, mutation.ResumeSpans,
		)
	})
}

// ConvertBackfillError returns a cleaner SQL error for a failed Batch.
func ConvertBackfillError(tableDesc *sqlbase.TableDescriptor, b *client.Batch) error {
	// A backfill on a new schema element has failed and the batch contains
	// information useful in printing a sensible error. However
	// ConvertBatchError() will only work correctly if the schema elements
	// are "live" in the tableDesc.
	desc := protoutil.Clone(tableDesc).(*sqlbase.TableDescriptor)
	mutationID := desc.Mutations[0].MutationID
	for _, mutation := range desc.Mutations {
		if mutation.MutationID != mutationID {
			// Mutations are applied in a FIFO order. Only apply the first set
			// of mutations if they have the mutation ID we're looking for.
			break
		}
		desc.MakeMutationComplete(mutation)
	}
	return sqlbase.ConvertBatchError(desc, b)
}
