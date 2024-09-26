// This code has been modified from its original form by The Cockroach Authors.
// All modifications are Copyright 2024 The Cockroach Authors.
//
// Copyright 2023 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package rafttest

import (
	"testing"

	"github.com/cockroachdb/datadriven"
)

func (env *InteractionEnv) handleForgetLeader(t *testing.T, d datadriven.TestData) error {
	idx := firstAsNodeIdx(t, d)
	env.ForgetLeader(idx)
	return nil
}

// ForgetLeader makes the follower at the given index forget its leader.
func (env *InteractionEnv) ForgetLeader(idx int) {
	env.Nodes[idx].ForgetLeader()
}

func (env *InteractionEnv) handleStepDown(t *testing.T, d datadriven.TestData) error {
	idx := firstAsNodeIdx(t, d)
	return env.Nodes[idx].TestingStepDown()
}
