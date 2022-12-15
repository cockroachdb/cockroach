// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package raftfbs

import (
	"testing"

	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	flatbuffers "github.com/google/flatbuffers/go"
	"github.com/stretchr/testify/require"
)

func TestCommand(t *testing.T) {
	defer leaktest.AfterTest(t)()

	id := []byte("deadbeef")
	protoBytes := []byte("some proto bytes")

	b := flatbuffers.NewBuilder(0)
	b.Finish(BuildCommand(b, id, protoBytes))

	cmd := GetRootAsCommand(b.FinishedBytes(), 0)
	require.Equal(t, id, cmd.IdBytes())
	require.Equal(t, protoBytes, cmd.RaftCommandPbBytes())
}
