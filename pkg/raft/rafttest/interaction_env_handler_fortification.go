// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rafttest

import (
	"testing"

	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/datadriven"
)

func (env *InteractionEnv) handleSendDeFortify(t *testing.T, d datadriven.TestData) error {
	lead := firstAsNodeIdx(t, d)
	follower := nthAsInt(t, d, 1)

	return env.Nodes[lead].TestingSendDeFortify(pb.PeerID(follower))
}
