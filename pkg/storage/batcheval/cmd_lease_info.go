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

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
)

func init() {
	RegisterCommand(roachpb.LeaseInfo, declareKeysLeaseInfo, LeaseInfo)
}

func declareKeysLeaseInfo(
	_ roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	spans.Add(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeLeaseKey(header.RangeID)})
}

// LeaseInfo returns information about the lease holder for the range.
func LeaseInfo(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	reply := resp.(*roachpb.LeaseInfoResponse)
	lease, nextLease := cArgs.EvalCtx.GetLease()
	if nextLease != nil {
		// If there's a lease request in progress, speculatively return that future
		// lease.
		reply.Lease = *nextLease
	} else {
		reply.Lease = lease
	}
	return result.Result{}, nil
}
