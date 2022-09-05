// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package consistencychecker

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

// ConsistencyChecker implements ConsistencyCheckRunner.
type ConsistencyChecker struct {
	db *kv.DB
}

// NewConsistencyChecker returns a new instance of
// consistencychecker.ConsistencyChecker.
func NewConsistencyChecker(db *kv.DB) *ConsistencyChecker {
	return &ConsistencyChecker{
		db: db,
	}
}

// CheckConsistency implements the eval.ConsistencyChecker interface.
func (s *ConsistencyChecker) CheckConsistency(
	ctx context.Context, from, to roachpb.Key, mode roachpb.ChecksumMode,
) (*roachpb.CheckConsistencyResponse, error) {
	var b kv.Batch
	b.AddRawRequest(&roachpb.CheckConsistencyRequest{
		RequestHeader: roachpb.RequestHeader{
			Key:    from,
			EndKey: to,
		},
		Mode: mode,
	})

	// NB: DistSender has special code to avoid parallelizing the request if
	// we're requesting CHECK_FULL.
	if err := s.db.Run(ctx, &b); err != nil {
		return nil, err
	}
	resp := b.RawResponse().Responses[0].GetInner().(*roachpb.CheckConsistencyResponse)
	return resp, nil
}
