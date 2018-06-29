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
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
)

func init() {
	RegisterCommand(roachpb.CheckExists, DefaultDeclareKeys, CheckExists)
}

// CheckExists scans the key range specified by start key
// through end key and returns whether the key range contained
// any keys.
func CheckExists(
	ctx context.Context, batch engine.ReadWriter, cArgs CommandArgs, resp roachpb.Response,
) (result.Result, error) {
	args := cArgs.Args.(*roachpb.CheckExistsRequest)
	h := cArgs.Header
	reply := resp.(*roachpb.CheckExistsResponse)

	rows, _, intents, err := engine.MVCCScan(ctx, batch, args.Key, args.EndKey,
		1 /* max */, h.Timestamp, h.ReadConsistency == roachpb.CONSISTENT, h.Txn)
	if err != nil {
		return result.Result{}, err
	}

	reply.Exists = len(rows) > 0
	return result.FromIntents(intents, args), err
}
