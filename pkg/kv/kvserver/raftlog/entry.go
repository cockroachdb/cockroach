// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

// TODO(sep-raft-log): there's also a `raftentry` package (it has the raft entry
// cache), that package might want to move closer to this code. Revisit when
// we've made progress factoring out the raft state and apply logic out of
// `kvserver`.

package raftlog

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// Entry contains data related to a raft log entry. This is the raftpb.Entry
// itself but also all encapsulated data relevant for command application.
type Entry struct {
	raftpb.Entry
	ID                kvserverbase.CmdIDKey // may be empty for zero Entry
	Cmd               kvserverpb.RaftCommand
	ConfChangeV1      *raftpb.ConfChange            // only set for config change
	ConfChangeV2      *raftpb.ConfChangeV2          // only set for config change
	ConfChangeContext *kvserverpb.ConfChangeContext // only set for config change
}

var entryPool = sync.Pool{
	New: func() interface{} {
		return &Entry{}
	},
}

// NewEntry populates an Entry from the provided raftpb.Entry.
func NewEntry(ent raftpb.Entry) (*Entry, error) {
	e := entryPool.Get().(*Entry)
	*e = Entry{Entry: ent}
	if err := e.load(); err != nil {
		return nil, err
	}
	return e, nil
}

// NewEntryFromRawValue populates an Entry from a raw value, i.e. from bytes as
// stored on a storage.Engine.
func NewEntryFromRawValue(b []byte) (*Entry, error) {
	var meta enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(b, &meta); err != nil {
		return nil, errors.Wrap(err, "decoding raft log MVCCMetadata")
	}

	e := entryPool.Get().(*Entry)

	if err := storage.MakeValue(meta).GetProto(&e.Entry); err != nil {
		return nil, errors.Wrap(err, "unmarshalling raft Entry")
	}

	err := e.load()
	if err != nil {
		return nil, err
	}
	return e, nil
}

// raftEntryFromRawValue decodes a raft.Entry from a raw MVCC value.
//
// Same as NewEntryFromRawValue, but doesn't decode the command and doesn't use
// the pool of entries.
func raftEntryFromRawValue(b []byte) (raftpb.Entry, error) {
	var meta enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(b, &meta); err != nil {
		return raftpb.Entry{}, errors.Wrap(err, "decoding raft log MVCCMetadata")
	}
	var entry raftpb.Entry
	if err := storage.MakeValue(meta).GetProto(&entry); err != nil {
		return raftpb.Entry{}, errors.Wrap(err, "unmarshalling raft Entry")
	}
	return entry, nil
}

func (e *Entry) load() error {
	if len(e.Data) == 0 {
		// Raft-proposed empty entry.
		return nil
	}

	var payload []byte
	switch e.Type {
	case raftpb.EntryNormal:
		e.ID, payload = kvserverbase.DecodeRaftCommand(e.Data)
	case raftpb.EntryConfChange:
		e.ConfChangeV1 = &raftpb.ConfChange{}
		if err := protoutil.Unmarshal(e.Data, e.ConfChangeV1); err != nil {
			return errors.Wrap(err, "unmarshalling ConfChange")
		}
		e.ConfChangeContext = &kvserverpb.ConfChangeContext{}
		if err := protoutil.Unmarshal(e.ConfChangeV1.Context, e.ConfChangeContext); err != nil {
			return errors.Wrap(err, "unmarshalling ConfChangeContext")
		}
		payload = e.ConfChangeContext.Payload
		e.ID = kvserverbase.CmdIDKey(e.ConfChangeContext.CommandID)
	case raftpb.EntryConfChangeV2:
		e.ConfChangeV2 = &raftpb.ConfChangeV2{}
		if err := protoutil.Unmarshal(e.Data, e.ConfChangeV2); err != nil {
			return errors.Wrap(err, "unmarshalling ConfChangeV2")
		}
		e.ConfChangeContext = &kvserverpb.ConfChangeContext{}
		if err := protoutil.Unmarshal(e.ConfChangeV2.Context, e.ConfChangeContext); err != nil {
			return errors.Wrap(err, "unmarshalling ConfChangeContext")
		}
		payload = e.ConfChangeContext.Payload
		e.ID = kvserverbase.CmdIDKey(e.ConfChangeContext.CommandID)
	default:
		return errors.AssertionFailedf("unknown entry type %d", e.Type)
	}

	// TODO(tbg): can len(payload)==0 if we propose an empty command to wake up leader?
	// If so, is that a problem here?
	return errors.Wrap(protoutil.Unmarshal(payload, &e.Cmd), "unmarshalling RaftCommand")
}

// ConfChange returns ConfChangeV1 or ConfChangeV2 as an interface, if set.
// Otherwise, returns nil.
func (e *Entry) ConfChange() raftpb.ConfChangeI {
	if e.ConfChangeV1 != nil {
		return e.ConfChangeV1
	}
	if e.ConfChangeV2 != nil {
		return e.ConfChangeV2
	}
	// NB: nil != interface{}(nil).
	return nil
}

// ToRawBytes produces the raw bytes which would store the entry in the raft
// log, i.e. it is the inverse to NewEntryFromRawBytes.
func (e *Entry) ToRawBytes() ([]byte, error) {
	var value roachpb.Value
	if err := value.SetProto(&e.Entry); err != nil {
		return nil, err
	}

	metaB, err := protoutil.Marshal(&enginepb.MVCCMetadata{RawBytes: value.RawBytes})
	if err != nil {
		return nil, err
	}

	return metaB, nil
}

// Release can be called when no more access to the Entry object will take
// place, allowing it to be re-used for future operations.
func (e *Entry) Release() {
	*e = Entry{}
	entryPool.Put(e)
}
