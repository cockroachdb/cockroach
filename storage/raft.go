// Copyright 2015 The Cockroach Authors.
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
	"fmt"
	"sync"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/security"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/coreos/etcd/raft"
)

// init installs an adapter to use clog for log messages from raft which
// don't belong to any raft group.
func init() {
	raft.SetLogger(&raftLogger{})
}

// *clogLogger implements the raft.Logger interface. Note that all methods
// must be defined on the pointer type rather than the value type because
// (at least in the go 1.4 compiler), methods on a value type called through
// an interface pointer go through an additional layer of indirection that
// appears on the stack, and would make all our stack frame offsets incorrect.
//
// Raft is fairly verbose at the "info" level, so we map "info" messages to
// clog.V(1) and "debug" messages to clog.V(2).
//
// This file is named raft.go instead of something like logger.go because this
// file's name is used to determine the vmodule parameter: --vmodule=raft=1
type raftLogger struct {
	group uint64
}

func (r *raftLogger) prependContext(format string, v []interface{}) string {
	var s string
	if r.group != 0 {
		v2 := append([]interface{}{r.group}, v...)
		s = fmt.Sprintf("[group %d] "+format, v2...)
	} else {
		s = fmt.Sprintf(format, v...)
	}
	return s
}

func (*raftLogger) Debug(v ...interface{}) {
	if log.V(2) {
		log.InfoDepth(1, v...)
	}
}

func (r *raftLogger) Debugf(format string, v ...interface{}) {
	if log.V(2) {
		s := r.prependContext(format, v)
		log.InfoDepth(1, s)
	}
}

func (*raftLogger) Info(v ...interface{}) {
	if log.V(1) {
		log.InfoDepth(1, v...)
	}
}

func (r *raftLogger) Infof(format string, v ...interface{}) {
	if log.V(1) {
		s := r.prependContext(format, v)
		log.InfoDepth(1, s)
	}
}

func (*raftLogger) Warning(v ...interface{}) {
	log.WarningDepth(1, v...)
}

func (r *raftLogger) Warningf(format string, v ...interface{}) {
	s := r.prependContext(format, v)
	log.WarningDepth(1, s)
}

func (*raftLogger) Error(v ...interface{}) {
	log.ErrorDepth(1, v...)
}

func (r *raftLogger) Errorf(format string, v ...interface{}) {
	s := r.prependContext(format, v)
	log.ErrorDepth(1, s)
}

func (*raftLogger) Fatal(v ...interface{}) {
	log.FatalDepth(1, v...)
}

func (r *raftLogger) Fatalf(format string, v ...interface{}) {
	s := r.prependContext(format, v)
	log.FatalDepth(1, s)
}

func (*raftLogger) Panic(v ...interface{}) {
	s := fmt.Sprint(v...)
	log.ErrorDepth(1, s)
	panic(s)
}

func (r *raftLogger) Panicf(format string, v ...interface{}) {
	s := r.prependContext(format, v)
	log.ErrorDepth(1, s)
	panic(s)
}

var logRaftReadyMu sync.Mutex

func logRaftReady(storeID roachpb.StoreID, groupID roachpb.RangeID, ready raft.Ready) {
	if log.V(5) {
		// Globally synchronize to avoid interleaving different sets of logs in tests.
		logRaftReadyMu.Lock()
		defer logRaftReadyMu.Unlock()
		log.Infof("store %s: group %s raft ready", storeID, groupID)
		if ready.SoftState != nil {
			log.Infof("SoftState updated: %+v", *ready.SoftState)
		}
		if !raft.IsEmptyHardState(ready.HardState) {
			log.Infof("HardState updated: %+v", ready.HardState)
		}
		for i, e := range ready.Entries {
			log.Infof("New Entry[%d]: %.200s", i, raft.DescribeEntry(e, raftEntryFormatter))
		}
		for i, e := range ready.CommittedEntries {
			log.Infof("Committed Entry[%d]: %.200s", i, raft.DescribeEntry(e, raftEntryFormatter))
		}
		if !raft.IsEmptySnap(ready.Snapshot) {
			log.Infof("Snapshot updated: %.200s", ready.Snapshot.String())
		}
		for i, m := range ready.Messages {
			log.Infof("Outgoing Message[%d]: %.200s", i, raft.DescribeMessage(m, raftEntryFormatter))
		}
	}
}

var _ security.RequestWithUser = &RaftMessageRequest{}

// GetUser implements security.RequestWithUser.
// Raft messages are always sent by the node user.
func (*RaftMessageRequest) GetUser() string {
	return security.NodeUser
}
