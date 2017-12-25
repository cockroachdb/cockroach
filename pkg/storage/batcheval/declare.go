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
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/spanset"
)

// DefaultDeclareKeys is the default implementation of Command.DeclareKeys
func DefaultDeclareKeys(
	desc roachpb.RangeDescriptor, header roachpb.Header, req roachpb.Request, spans *spanset.SpanSet,
) {
	if roachpb.IsReadOnly(req) {
		spans.Add(spanset.SpanReadOnly, req.Header())
	} else {
		spans.Add(spanset.SpanReadWrite, req.Header())
	}
	if header.Txn != nil {
		header.Txn.AssertInitialized(context.TODO())
		spans.Add(spanset.SpanReadOnly, roachpb.Span{
			Key: keys.AbortSpanKey(header.RangeID, header.Txn.ID),
		})
	}
	if header.ReturnRangeInfo {
		spans.Add(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeLeaseKey(header.RangeID)})
		spans.Add(spanset.SpanReadOnly, roachpb.Span{Key: keys.RangeDescriptorKey(desc.StartKey)})
	}
}

// CommandArgs contains all the arguments to a command.
// TODO(bdarnell): consider merging with storagebase.FilterArgs (which
// would probably require removing the EvalCtx field due to import order
// constraints).
type CommandArgs struct {
	EvalCtx EvalContext
	Header  roachpb.Header
	Args    roachpb.Request

	// If MaxKeys is non-zero, span requests should limit themselves to
	// that many keys. Commands using this feature should also set
	// NumKeys and ResumeSpan in their responses.
	MaxKeys int64

	// *Stats should be mutated to reflect any writes made by the command.
	Stats *enginepb.MVCCStats
}
