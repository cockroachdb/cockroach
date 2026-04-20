// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package batcheval

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/batcheval/result"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/errors"
)

func init() {
	RegisterReadOnlyCommand(kvpb.GetTxnDetails, DefaultDeclareIsolatedKeys, GetTxnDetails)
}

// GetTxnDetails retrieves a transaction's raw writes and dependencies on a
// single range. Transaction A depends on B if A's write set overlaps B's write
// set, or A's read set overlaps B's write set. The server computes dependencies
// by MVCC-scanning the transaction's spans to find overlapping write
// timestamps, then translating those timestamps to transaction IDs via a
// raft-log-derived mapping maintained by the range.
func GetTxnDetails(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	_ = cArgs.Args.(*kvpb.GetTxnDetailsRequest)
	_ = resp.(*kvpb.GetTxnDetailsResponse)
	return result.Result{}, errors.New("GetTxnDetails not yet implemented")
}
