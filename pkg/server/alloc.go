// Copyright 2019 The Cockroach Authors.
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

package server

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/pkg/errors"
)

type allocServer struct {
	db              *client.DB
	gossipConnected <-chan struct{}
	done            <-chan struct{}
}

func newAllocServer(
	db *client.DB, gossipConnected <-chan struct{}, done <-chan struct{},
) *allocServer {
	return &allocServer{db: db, gossipConnected: gossipConnected, done: done}
}

func (s *allocServer) AllocateNodeID(
	ctx context.Context, args *serverpb.AllocateNodeIDRequest,
) (*serverpb.AllocateNodeIDResponse, error) {
	select {
	case <-s.done:
		return nil, errors.New("node is stopping")
	case <-s.gossipConnected:
	}
	nodeID, err := allocateNodeID(ctx, s.db)
	if err != nil {
		return nil, err
	}
	return &serverpb.AllocateNodeIDResponse{NodeID: int32(nodeID)}, nil
}
