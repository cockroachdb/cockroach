// Copyright 2014 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func init() {
	RegisterCommand(roachpb.Get, DefaultDeclareKeys, Get)
}

// Get returns the value for a specified key.
func Get(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.GetRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.GetResponse)

	val, intent, err := engine.MVCCGet(ctx, batch, args.Key, h.Timestamp, engine.MVCCGetOptions{
		Inconsistent:   h.ReadConsistency != roachpb.CONSISTENT,
		IgnoreSequence: shouldIgnoreSequenceNums(cArgs.EvalCtx),
		Txn:            h.Txn,
	})
	if err != nil {
		return result.Result{}, err
	}
	var intents []roachpb.Intent
	if intent != nil {
		intents = append(intents, *intent)
	}

	reply.Value = val
	if h.ReadConsistency == roachpb.READ_UNCOMMITTED {
		var intentVals []roachpb.KeyValue
		intentVals, err = CollectIntentRows(ctx, batch, cArgs, intents)
		if err == nil {
			switch len(intentVals) {
			case 0:
			case 1:
				reply.IntentValue = &intentVals[0].Value
			default:
				log.Fatalf(ctx, "more than 1 intent on single key: %v", intentVals)
			}
		}
	}
	return result.FromIntents(intents, args), err
}

func shouldIgnoreSequenceNums(rec EvalContext) bool {
	// Versions 2.1 and below did not properly propagate sequence numbers to leaf
	// TxnCoordSenders. This means that we can't rely on sequence numbers being
	// properly assigned by those nodes. Gate the use of sequence numbers while
	// scanning on a cluster setting.
	// NOTE: because we check this during batcheval instead of when sending a
	// get/scan request with an associated flag on the request itself, 2.2 clients
	// can't use a similar IsActive check to determine when they can start relying
	// on the correct sequence number behavior. Instead, they must wait until a
	// new cluster version introduced in 2.3. Alternatively, we can add a flag to
	// batch headers that can be set on a client (who would perform the IsActive
	// check itself) and would override this decision. We haven't added that yet
	// because it's not clear that it will be needed.
	return !rec.ClusterSettings().Version.IsActive(cluster.VersionSequencedReads)
}
