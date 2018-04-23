// Copyright 2018 The Cockroach Authors.
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

package batcheval

import (
	"bytes"
	"context"

	"github.com/cockroachdb/cockroach/pkg/storage/rditer"
	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
)

func init() {
	RegisterCommand(roachpb.Subsume, declareKeysSubsume, Subsume)
}

func declareKeysSubsume(
	desc roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	// Subsume must not run concurrently with any other command. It declares that
	// it reads and writes every addressable key in the range; this guarantees
	// that it conflicts with any other command because every command must declare
	// at least one addressable key. It does not, in fact, write any keys.
	spans.Add(spanset.SpanReadWrite, roachpb.Span{
		Key:    desc.StartKey.AsRawKey(),
		EndKey: desc.EndKey.AsRawKey(),
	})
	spans.Add(spanset.SpanReadWrite, roachpb.Span{
		Key:    keys.MakeRangeKeyPrefix(desc.StartKey),
		EndKey: keys.MakeRangeKeyPrefix(desc.EndKey).PrefixEnd(),
	})
}

// Subsume notifies a range that its left-hand neighbor has initiated a merge.
// It is the means by which the merging ranges ensure there is no moment in time
// where they could both process commands for the same keys, which would lead to
// stale reads and lost writes.
//
// Specifically, the receiving replica guarantees that:
//   - it is the leaseholder,
//   - when it responds, there are no commands in flight,
//   - the snapshot in the response has the latest writes,
//   - it, and all future leaseholders for the range, will not process another
//     command until they refresh their range descriptor with a consistent read
//     from meta2, and
//   - if it or any future leaseholder for the range finds that its range
//     descriptor has been deleted, it self destructs.
//
// Provided the merge transaction lays down an intent on the right-hand side's
// meta2 descriptor before sending a SubsumeRequest, these four guarantees
// combine to ensure a correct merge. If the merge transaction commits, the
// right-hand side never serves another request. If the merge transaction
// aborts, the right-hand side resumes processing of requests.
func Subsume(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.SubsumeRequest)
	reply := resp.(*roachpb.SubsumeResponse)
	desc := cArgs.EvalCtx.Desc()

	// Sanity check that the requesting range is our left neighbor. The ordering
	// of operations in the AdminMerge transaction should make it impossible for
	// these ranges to be nonadjacent, but double check.
	if !bytes.Equal(args.LeftRange.EndKey, desc.StartKey) {
		return result.Result{}, errors.Errorf("ranges are not adjacent: %s != %s",
			args.LeftRange.EndKey, desc.StartKey)
	}

	eng := engine.NewInMem(roachpb.Attributes{}, 1<<20)
	defer eng.Close()

	// XXX: This command reads the whole replica into memory. We'll need to be
	// more careful when merging large ranges.
	snapBatch := eng.NewBatch()
	defer snapBatch.Close()

	iter := rditer.NewReplicaDataIterator(desc, batch, true /* replicatedOnly */)
	defer iter.Close()
	for ; ; iter.Next() {
		if ok, err := iter.Valid(); err != nil {
			return result.Result{}, err
		} else if !ok {
			break
		}
		if err := snapBatch.Put(iter.Key(), iter.Value()); err != nil {
			return result.Result{}, err
		}
	}
	reply.Data = snapBatch.Repr()

	return result.Result{
		Local: result.LocalResult{SetMerging: true},
	}, nil
}
