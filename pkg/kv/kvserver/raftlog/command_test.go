// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package raftlog

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestReplicatedCmdString checks that ReplicatedCmd does not implement
// fmt.Stringer. It is fine to implement this in the future, however previously
// at one point it "accidentally" implemented fmt.Stringer via embedding, which
// could hide information.
func TestReplicatedCmdString(t *testing.T) {
	var cmd interface{} = (*ReplicatedCmd)(nil)
	_, ok := cmd.(fmt.Stringer)
	require.False(t, ok)
}
