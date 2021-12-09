// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
)

type DRPCNode struct {
	roachpb.DRPCInternalUnimplementedServer
	n *Node
	// struct fields
}

// Batch .
func (s *DRPCNode) Batch(
	ctx context.Context, ba *roachpb.BatchRequest,
) (*roachpb.BatchResponse, error) {
	return s.n.Batch(ctx, ba)
}
