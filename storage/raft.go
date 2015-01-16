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
	"github.com/cockroachdb/cockroach/util"
	gogoproto "github.com/gogo/protobuf/proto"
)

type committedCommand struct {
	cmdIDKey cmdIDKey
	cmd      proto.InternalRaftCommand
}

// raftInterface is the interface exposed by a raft implementation.
type raftInterface interface {
	// createGroup initializes a raft group with the given id.
	createGroup(int64) error

	// removeGroup removes the raft group with the given id.
	// Note that committed commands for the removed group may still be
	// present in the channel buffer.
	removeGroup(int64) error

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
	stopper  *util.Stopper
}

func newSingleNodeRaft(storage multiraft.Storage) *singleNodeRaft {
	mr, err := multiraft.NewMultiRaft(1, &multiraft.Config{
		Transport:              multiraft.NewLocalRPCTransport(),
		Storage:                storage,
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
		stopper:  util.NewStopper(1),
	}
	mr.Start()
	go snr.run()
	return snr
}

var _ raftInterface = (*singleNodeRaft)(nil)

func (snr *singleNodeRaft) createGroup(id int64) error {
	snr.mu.Lock()
	if _, ok := snr.groups[id]; !ok {
		snr.groups[id] = struct{}{}
		snr.mu.Unlock()
		return snr.mr.CreateGroup(uint64(id))
	}
	snr.mu.Unlock()
	return nil
}

func (snr *singleNodeRaft) removeGroup(id int64) error {
	snr.mu.Lock()
	if _, ok := snr.groups[id]; ok {
		delete(snr.groups, id)
		snr.mu.Unlock()
		return snr.mr.RemoveGroup(uint64(id))
	}
	snr.mu.Unlock()
	return nil
}

func (snr *singleNodeRaft) propose(cmdIDKey cmdIDKey, cmd proto.InternalRaftCommand) {
	if cmd.Cmd.GetValue() == nil {
		panic("proposed a nil command")
	}
	// Lazily create group. TODO(bdarnell): make this non-lazy
	err := snr.createGroup(cmd.RaftID)
	if err != nil {
		log.Fatal(err)
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
	snr.stopper.Stop()
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
		case <-snr.stopper.ShouldStop():
			snr.mr.Stop()
			snr.stopper.SetStopped()
			return
		}
	}
}
