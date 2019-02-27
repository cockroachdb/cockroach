// Copyright 2017 The Cockroach Authors.
//
/// Licensed under the Apache License, Version 2.0 (the "License");
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/pkg/errors"
)

func init() {
	RegisterCommand(roachpb.AddSSTable, DefaultDeclareKeys, EvalAddSSTable)
}

// EvalAddSSTable evaluates an AddSSTable command.
func EvalAddSSTable(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, _ roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.AddSSTableRequest)
	h := cArgs.Header
	ms := cArgs.Stats
	mvccStartKey, mvccEndKey := engine.MVCCKey{Key: args.Key}, engine.MVCCKey{Key: args.EndKey}

	// TODO(tschottdorf): restore the below in some form (gets in the way of testing).
	// _, span := tracing.ChildSpan(ctx, fmt.Sprintf("AddSSTable [%s,%s)", args.Key, args.EndKey))
	// defer tracing.FinishSpan(span)
	log.Eventf(ctx, "evaluating AddSSTable [%s,%s)", mvccStartKey.Key, mvccEndKey.Key)

	// Verify that the keys in the sstable are within the range specified by the
	// request header, verify the key-value checksums, and compute the new
	// MVCCStats.
	stats, err := verifySSTable(
		args.Data, mvccStartKey, mvccEndKey, h.Timestamp.WallTime)
	if err != nil {
		return result.Result{}, errors.Wrap(err, "verifying sstable data")
	}
	stats.ContainsEstimates = true
	ms.Add(stats)

	return result.Result{
		Replicated: storagepb.ReplicatedEvalResult{
			AddSSTable: &storagepb.ReplicatedEvalResult_AddSSTable{
				Data:  args.Data,
				CRC32: util.CRC32(args.Data),
			},
		},
	}, nil
}

func verifySSTable(
	data []byte, start, end engine.MVCCKey, nowNanos int64,
) (enginepb.MVCCStats, error) {
	// To verify every KV is a valid roachpb.KeyValue in the range [start, end)
	// we a) pass a verify flag on the iterator so that as ComputeStatsGo calls
	// Next, we're also verifying each KV pair. We explicitly check the first key
	// is >= start and then that we do not find a key after end.
	dataIter, err := engine.NewMemSSTIterator(data, true)
	if err != nil {
		return enginepb.MVCCStats{}, err
	}
	defer dataIter.Close()

	// Check that the first key is in the expected range.
	dataIter.Seek(engine.MVCCKey{Key: keys.MinKey})
	ok, err := dataIter.Valid()
	if err != nil {
		return enginepb.MVCCStats{}, err
	} else if ok {
		if unsafeKey := dataIter.UnsafeKey(); unsafeKey.Less(start) {
			return enginepb.MVCCStats{}, errors.Errorf("first key %s not in request range [%s,%s)",
				unsafeKey.Key, start.Key, end.Key)
		}
	}

	stats, err := engine.ComputeStatsGo(dataIter, start, end, nowNanos)
	if err != nil {
		return stats, err
	}

	dataIter.Seek(end)
	ok, err = dataIter.Valid()
	if err != nil {
		return enginepb.MVCCStats{}, err
	} else if ok {
		if unsafeKey := dataIter.UnsafeKey(); !unsafeKey.Less(end) {
			return enginepb.MVCCStats{}, errors.Errorf("last key %s not in request range [%s,%s)",
				unsafeKey.Key, start.Key, end.Key)
		}
	}
	return stats, nil
}
