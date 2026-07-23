// This code has been modified from its original form by The Cockroach Authors.
// All modifications are Copyright 2024 The Cockroach Authors.
//
// Copyright 2019 The etcd Authors
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

package confchange

import (
	"fmt"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft/quorum"
	pb "github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/raft/tracker"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
)

func TestConfChangeDataDriven(t *testing.T) {
	datadriven.Walk(t, "testdata", func(t *testing.T, path string) {
		c := Changer{
			Config:           quorum.MakeEmptyConfig(),
			ProgressMap:      tracker.MakeEmptyProgressMap(),
			MaxInflight:      10,
			MaxInflightBytes: 0,
			LastIndex:        0, // incremented in this test with each cmd
		}

		// The test files use the commands
		// - simple: run a simple conf change (i.e. no joint consensus),
		// - enter-joint: enter a joint config, and
		// - leave-joint: leave a joint config.
		// The first two take a list of config changes, which have the following
		// syntax:
		// - vn: make n a voter,
		// - ln: make n a learner,
		// - rn: remove n, and
		// - un: update n.
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			defer func() {
				c.LastIndex++
			}()
			var ccs []pb.ConfChangeSingle
			toks := strings.Split(strings.TrimSpace(d.Input), " ")
			if toks[0] == "" {
				toks = nil
			}
			for _, tok := range toks {
				if len(tok) < 2 {
					return fmt.Sprintf("unknown token %s", tok)
				}
				var cc pb.ConfChangeSingle
				switch tok[0] {
				case 'v':
					cc.Type = pb.ConfChangeAddNode
				case 'l':
					cc.Type = pb.ConfChangeAddLearnerNode
				case 'r':
					cc.Type = pb.ConfChangeRemoveNode
				case 'u':
					cc.Type = pb.ConfChangeUpdateNode
				default:
					return fmt.Sprintf("unknown input: %s", tok)
				}
				id, err := strconv.ParseUint(tok[1:], 10, 64)
				if err != nil {
					return err.Error()
				}
				cc.NodeID = pb.PeerID(id)
				ccs = append(ccs, cc)
			}

			var cfg quorum.Config
			var progressMap tracker.ProgressMap
			var err error
			switch d.Cmd {
			case "simple":
				cfg, progressMap, err = c.Simple(ccs...)
			case "enter-joint":
				var autoLeave bool
				if len(d.CmdArgs) > 0 {
					d.ScanArgs(t, "autoleave", &autoLeave)
				}
				cfg, progressMap, err = c.EnterJoint(autoLeave, ccs...)
			case "leave-joint":
				if len(ccs) > 0 {
					err = errors.New("this command takes no input")
				} else {
					cfg, progressMap, err = c.LeaveJoint()
				}
			default:
				return "unknown command"
			}
			if err != nil {
				return err.Error() + "\n"
			}
			c.Config, c.ProgressMap = cfg, progressMap
			return fmt.Sprintf("%s\n%s", c.Config, c.ProgressMap)
		})
	})
}
