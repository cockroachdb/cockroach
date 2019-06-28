// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package batcheval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
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
		IgnoreSequence: shouldIgnoreSequenceNums(),
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

func shouldIgnoreSequenceNums() bool {
	// NOTE: In version 19.1 and below this checked if a cluster version was
	// active. This was because Versions 2.1 and below did not properly
	// propagate sequence numbers to leaf TxnCoordSenders, which meant that we
	// couldn't rely on sequence numbers being properly assigned by those nodes.
	// Therefore, we gated the use of sequence numbers while scanning on the
	// cluster version.
	//
	// Because we checked this during batcheval instead of when sending a
	// get/scan request with an associated flag on the request itself, 19.2
	// clients can't immediately start relying on the correct sequence number
	// behavior. This is because it's possible that a 19.2 node joins the
	// cluster before all 19.1 nodes realize that the cluster version has been
	// upgraded to the version that instructs them to respect sequence numbers.
	// Instead, they must wait until a new cluster version (19.2.X) is active
	// which proves that all nodes the could be evaluating their request will
	// respect sequence numbers.
	//
	// TODO(nvanbenschoten): Remove in 20.1. This serves only as documentation
	// now.
	return false
}
