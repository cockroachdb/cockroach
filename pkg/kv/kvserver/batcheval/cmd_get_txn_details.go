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
// single range. See GetTxnDetailsRequest for the full API contract.
func GetTxnDetails(
	ctx context.Context, reader storage.Reader, cArgs CommandArgs, resp kvpb.Response,
) (result.Result, error) {
	_ = cArgs.Args.(*kvpb.GetTxnDetailsRequest)
	_ = resp.(*kvpb.GetTxnDetailsResponse)
	return result.Result{}, errors.New("GetTxnDetails not yet implemented")
}
