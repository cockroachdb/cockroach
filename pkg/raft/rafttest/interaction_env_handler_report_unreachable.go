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
	"github.com/cockroachdb/errors"
)

func (env *InteractionEnv) handleReportUnreachable(t *testing.T, d datadriven.TestData) error {
	sl := nodeIdxs(t, d)
	if len(sl) != 2 {
		return errors.New("must specify exactly two node indexes: node on which to report, and reported node")
	}
	env.Nodes[sl[0]].ReportUnreachable(env.Nodes[sl[1]].Config.ID)
	return nil
}
