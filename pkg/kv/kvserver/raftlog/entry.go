// Copyright 2022 The Cockroach Authors.
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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Entry contains data related to the current raft log entry.
type Entry struct {
	Meta enginepb.MVCCMetadata // optional
	Ent  raftpb.Entry
	ID   kvserverbase.CmdIDKey
	CC1  *raftpb.ConfChange
	CC2  *raftpb.ConfChangeV2
	CCC  *kvserverpb.ConfChangeContext
	Cmd  kvserverpb.RaftCommand
}

func (e *Entry) CC() raftpb.ConfChangeI {
	if e.CC1 != nil {
		return e.CC1
	}
	if e.CC2 != nil {
		return e.CC2
	}
	// NB: nil != interface{}(nil).
	return nil
}

func (e *Entry) LoadFromRawValue(b []byte) error {
	if err := protoutil.Unmarshal(b, &e.Meta); err != nil {
		return errors.Wrap(err, "decoding raft log MVCCMetadata")
	}

	if err := storage.MakeValue(e.Meta).GetProto(&e.Ent); err != nil {
		return errors.Wrap(err, "unmarshalling raft Entry")
	}

	return e.load()
}

func (e *Entry) Load() error {
	e.Meta = enginepb.MVCCMetadata{}
	return e.load()
}

func (e *Entry) load() error {
	if len(e.Ent.Data) == 0 {
		// Raft-proposed empty entry.
		return nil
	}

	var payload []byte
	switch e.Ent.Type {
	case raftpb.EntryNormal:
		e.ID, payload = kvserverbase.DecodeRaftCommand(e.Ent.Data)
	case raftpb.EntryConfChange:
		e.CC1 = &raftpb.ConfChange{}
		if err := protoutil.Unmarshal(e.Ent.Data, e.CC1); err != nil {
			return errors.Wrap(err, "unmarshalling ConfChange")
		}
		e.CCC = &kvserverpb.ConfChangeContext{}
		if err := protoutil.Unmarshal(e.CC1.Context, e.CCC); err != nil {
			return errors.Wrap(err, "unmarshalling ConfChangeContext")
		}
		payload = e.CCC.Payload
		e.ID = kvserverbase.CmdIDKey(e.CCC.CommandID)
	case raftpb.EntryConfChangeV2:
		e.CC2 = &raftpb.ConfChangeV2{}
		if err := protoutil.Unmarshal(e.Ent.Data, e.CC2); err != nil {
			return errors.Wrap(err, "unmarshalling ConfChangeV2")
		}
		e.CCC = &kvserverpb.ConfChangeContext{}
		if err := protoutil.Unmarshal(e.CC2.Context, e.CCC); err != nil {
			return errors.Wrap(err, "unmarshalling ConfChangeContext")
		}
		payload = e.CCC.Payload
		e.ID = kvserverbase.CmdIDKey(e.CCC.CommandID)
	default:
		return errors.AssertionFailedf("unknown entry type %d", e.Ent.Type)
	}

	// TODO(tbg): can len(payload)==0 if we propose an empty command to wake up leader?
	// If so, is that a problem here?
	return errors.Wrap(protoutil.Unmarshal(payload, &e.Cmd), "unmarshalling RaftCommand")
}
