// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Ben Darnell

package storage

import (
	"log"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/multiraft"
	"github.com/cockroachdb/cockroach/proto"
	gogoproto "github.com/gogo/protobuf/proto"
)

type committedCommand struct {
	cmdIDKey cmdIDKey
	cmd      proto.InternalRaftCommand
}

// raft is the interface exposed by a raft implementation.
type raft interface {
	// propose a command to raft. If accepted by the consensus protocol it will
	// eventually appear in the committed channel, but this is not guaranteed
	// so callers may need to retry.
	propose(cmdIDKey, proto.InternalRaftCommand)

	// committed returns a channel that yields commands as they are
	// committed. Note that this includes commands proposed by this node
	// and others.
	committed() <-chan committedCommand

	stop()
}

type singleNodeRaft struct {
	mr       *multiraft.MultiRaft
	mu       sync.Mutex
	groups   map[int64]struct{}
	commitCh chan committedCommand
	stopper  chan struct{}
}

func newSingleNodeRaft() *singleNodeRaft {
	mr, err := multiraft.NewMultiRaft(1, &multiraft.Config{
		Transport:              multiraft.NewLocalRPCTransport(),
		Storage:                multiraft.NewMemoryStorage(),
		TickInterval:           time.Millisecond,
		ElectionTimeoutTicks:   5,
		HeartbeatIntervalTicks: 1,
	})
	if err != nil {
		log.Fatal(err)
	}
	snr := &singleNodeRaft{
		mr:       mr,
		groups:   map[int64]struct{}{},
		commitCh: make(chan committedCommand, 10),
		stopper:  make(chan struct{}),
	}
	mr.Start()
	go snr.run()
	return snr
}

var _ raft = (*singleNodeRaft)(nil)

func (snr *singleNodeRaft) propose(cmdIDKey cmdIDKey, cmd proto.InternalRaftCommand) {
	snr.mu.Lock()
	defer snr.mu.Unlock()
	if _, ok := snr.groups[cmd.RaftID]; !ok {
		snr.groups[cmd.RaftID] = struct{}{}
		// Lazily create group. TODO(bdarnell): make this non-lazy
		err := snr.mr.CreateGroup(uint64(cmd.RaftID), []uint64{1})
		if err != nil {
			log.Fatal(err)
		}
	}
	data, err := gogoproto.Marshal(&cmd)
	if err != nil {
		log.Fatal(err)
	}
	snr.mr.SubmitCommand(uint64(cmd.RaftID), string(cmdIDKey), data)
}

func (snr *singleNodeRaft) committed() <-chan committedCommand {
	return snr.commitCh
}

func (snr *singleNodeRaft) stop() {
	close(snr.stopper)
}

func (snr *singleNodeRaft) run() {
	for {
		select {
		case e := <-snr.mr.Events:
			switch e := e.(type) {
			case *multiraft.EventCommandCommitted:
				var cmd proto.InternalRaftCommand
				err := gogoproto.Unmarshal(e.Command, &cmd)
				if err != nil {
					log.Fatal(err)
				}
				snr.commitCh <- committedCommand{cmdIDKey(e.CommandID), cmd}
			}
		case <-snr.stopper:
			snr.mr.Stop()
			return
		}
	}
}
