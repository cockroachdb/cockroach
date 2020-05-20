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
	"time"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/spanset"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func init() {
	RegisterReadOnlyCommand(roachpb.ComputeChecksum, declareKeysComputeChecksum, ComputeChecksum)
}

func declareKeysComputeChecksum(
	_ *roachpb.RangeDescriptor, _ roachpb.Header, _ roachpb.Request, _, _ *spanset.SpanSet,
) {
	// Intentionally declare no keys, as ComputeChecksum does not need to be
	// serialized with any other commands. It simply needs to be committed into
	// the Raft log.
}

// Version numbers for Replica checksum computation. Requests silently no-op
// unless the versions are compatible.
const (
	ReplicaChecksumVersion    = 4
	ReplicaChecksumGCInterval = time.Hour
)

// ComputeChecksum starts the process of computing a checksum on the replica at
// a particular snapshot. The checksum is later verified through a
// CollectChecksumRequest.
func ComputeChecksum(
	_ context.Context, _ storage.Reader, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.ComputeChecksumRequest)

	reply := resp.(*roachpb.ComputeChecksumResponse)
	reply.ChecksumID = uuid.MakeV4()

	var pd result.Result
	pd.Replicated.ComputeChecksum = &kvserverpb.ComputeChecksum{
		Version:      args.Version,
		ChecksumID:   reply.ChecksumID,
		SaveSnapshot: args.Snapshot,
		Mode:         args.Mode,
		Checkpoint:   args.Checkpoint,
		Terminate:    args.Terminate,
	}
	return pd, nil
}
