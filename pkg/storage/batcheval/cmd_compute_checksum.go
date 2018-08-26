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
	"time"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

func init() {
	RegisterCommand(roachpb.ComputeChecksum, DefaultDeclareKeys, ComputeChecksum)
}

// Version numbers for Replica checksum computation. Requests silently no-op
// unless the versions are compatible.
const (
	ReplicaChecksumVersion    = 3
	ReplicaChecksumGCInterval = time.Hour
)

// ComputeChecksum starts the process of computing a checksum on the replica at
// a particular snapshot. The checksum is later verified through a
// CollectChecksumRequest.
func ComputeChecksum(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.ComputeChecksumRequest)

	if args.Version != ReplicaChecksumVersion {
		log.Infof(ctx, "incompatible ComputeChecksum versions (server: %d, requested: %d)",
			ReplicaChecksumVersion, args.Version)
		return result.Result{}, nil
	}

	reply := resp.(*roachpb.ComputeChecksumResponse)
	reply.ChecksumID = uuid.MakeV4()

	var pd result.Result
	pd.Replicated.ComputeChecksum = &storagebase.ComputeChecksum{
		ChecksumID:   reply.ChecksumID,
		SaveSnapshot: args.Snapshot,
	}
	return pd, nil
}
