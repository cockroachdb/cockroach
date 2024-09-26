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

package rafttest

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/raft"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/datadriven"
)

func (env *InteractionEnv) handleDeliverMsgs(t *testing.T, d datadriven.TestData) error {
	var typ raftpb.MessageType = -1
	var rs []Recipient
	for _, arg := range d.CmdArgs {
		if len(arg.Vals) == 0 {
			id, err := strconv.ParseUint(arg.Key, 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			rs = append(rs, Recipient{ID: raftpb.PeerID(id)})
		}
		for i := range arg.Vals {
			switch arg.Key {
			case "drop":
				var rawID uint64
				arg.Scan(t, i, &rawID)
				id := raftpb.PeerID(rawID)
				var found bool
				for _, r := range rs {
					if r.ID == id {
						found = true
					}
				}
				if found {
					t.Fatalf("can't both deliver and drop msgs to %d", id)
				}
				rs = append(rs, Recipient{ID: id, Drop: true})
			case "type":
				var s string
				arg.Scan(t, i, &s)
				v, ok := raftpb.MessageType_value[s]
				if !ok {
					t.Fatalf("unknown message type %s", s)
				}
				typ = raftpb.MessageType(v)
			}
		}
	}

	if n := env.DeliverMsgs(typ, rs...); n == 0 {
		env.Output.WriteString("no messages\n")
	}
	return nil
}

type Recipient struct {
	ID   raftpb.PeerID
	Drop bool
}

// DeliverMsgs goes through env.Messages and, depending on the Drop flag,
// delivers or drops messages to the specified Recipients. Only messages of type
// typ are delivered (-1 for all types). Returns the number of messages handled
// (i.e. delivered or dropped). A handled message is removed from env.Messages.
func (env *InteractionEnv) DeliverMsgs(typ raftpb.MessageType, rs ...Recipient) int {
	var n int
	for _, r := range rs {
		var msgs []raftpb.Message
		msgs, env.Messages = splitMsgs(env.Messages, r.ID, typ, r.Drop)
		n += len(msgs)
		for _, msg := range msgs {
			if r.Drop {
				fmt.Fprint(env.Output, "dropped: ")
			}
			fmt.Fprintln(env.Output, raft.DescribeMessage(msg, defaultEntryFormatter))
			if r.Drop {
				// NB: it's allowed to drop messages to nodes that haven't been instantiated yet,
				// we haven't used msg.To yet.
				continue
			}
			toIdx := int(msg.To - 1)
			if err := env.Nodes[toIdx].Step(msg); err != nil {
				fmt.Fprintln(env.Output, err)
			}
		}
	}
	return n
}
