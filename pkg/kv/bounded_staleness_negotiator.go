// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kv

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// BoundedStalenessNegotiator provides the local resolved timestamp for a
// collection of key spans.
type BoundedStalenessNegotiator interface {
	LocalResolvedTimestamp(ctx context.Context, ba *kvpb.BatchRequest) (hlc.Timestamp, *kvpb.Error)
}

type BoundedStalenessNegotiatorWithoutCaching struct {
	db *DB
}

func (bsn BoundedStalenessNegotiatorWithoutCaching) LocalResolvedTimestamp(
	ctx context.Context, ba *kvpb.BatchRequest,
) (hlc.Timestamp, *kvpb.Error) {
	sendFunc := func(ctx context.Context, queryResBa *kvpb.BatchRequest) (*kvpb.BatchResponse, *kvpb.Error) {
		queryResBa.RoutingPolicy = ba.RoutingPolicy
		queryResBa.WaitPolicy = ba.WaitPolicy
		return bsn.db.send(ctx, queryResBa)
	}
	resTS, pErr := BoundedStalenessNegotiateResolvedTimestamp(ctx, ba, sendFunc)
	if pErr != nil {
		return hlc.Timestamp{}, pErr
	}
	return resTS, nil
}
