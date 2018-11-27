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
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
)

func init() {
	RegisterCommand(roachpb.Merge, DefaultDeclareKeys, Merge)
}

// Merge is used to merge a value into an existing key. Merge is an
// efficient accumulation operation which is exposed by RocksDB, used
// by CockroachDB for the efficient accumulation of certain
// values. Due to the difficulty of making these operations
// transactional, merges are not currently exposed directly to
// clients. Merged values are explicitly not MVCC data.
func Merge(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.MergeRequest)
	h := cArgs.Header

	return result.Result{}, engine.MVCCMerge(ctx, batch, cArgs.Stats, args.Key, h.Timestamp, args.Value)
}
