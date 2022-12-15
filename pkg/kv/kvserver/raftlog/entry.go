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
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/raftfbs"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

// EntryData reflects the information returned by DecomposeEntryData.
type EntryData struct {
	Type raftpb.EntryType

	// ConfChangeBytes is a marshaled raftpb.ConfChange{,V2}.
	// Non-nil if and only if Type is EntryConfChange{,V2}
	ConfChangeBytes []byte // raftpb.ConfChange{,V2}

	// The fields below are set if and only if Type is EntryNormal.
	// For EntryConfChange{,V2} these have to be pulled out of the unmarshaled
	// ConfChangeBytes.

	RaftCommandBytes []byte // kvserverpb.RaftCommand
	Version          kvserverbase.RaftCommandEncodingVersion
	CmdID            kvserverbase.CmdIDKey
}

// DecomposeEntryData takes a raftpb.Entry's Type and Data fields and extracts
// EntryData from it. This method does not perform any unmarshaling nor does it
// allocate, so it should be considered relatively cheap.
//
// For regular entries (raftpb.EntryNormal), reads out the
// RaftCommandEncodingVersion and CmdID. For configuration changes, these are
// wrapped in two layers of protobuf marshaling, so if the caller needs to know
// them they need to fully load the entry (using raftlog.NewEntry).
func DecomposeEntryData(typ raftpb.EntryType, data []byte) (EntryData, error) {
	if len(data) == 0 {
		// An empty command.
		return EntryData{}, nil
	}

	switch typ {
	case raftpb.EntryNormal:
	case raftpb.EntryConfChange:
		return EntryData{
			Type:            typ,
			ConfChangeBytes: data,
		}, nil
	case raftpb.EntryConfChangeV2:
		return EntryData{
			Type:            typ,
			ConfChangeBytes: data,
		}, nil
	default:
		return EntryData{}, errors.AssertionFailedf("unknown EntryType %d", typ)
	}

	v := kvserverbase.RaftCommandEncodingVersion(data[0])
	switch v {
	case kvserverbase.RaftVersionStandard:
	case kvserverbase.RaftVersionSideloaded:
	case kvserverbase.RaftVersionFlatBuffer:
	default:
		return EntryData{}, errors.AssertionFailedf("unknown command encoding version %d", v)
	}

	return EntryData{
		Type:             typ,
		RaftCommandBytes: data[1+kvserverbase.RaftCommandIDLen:],
		Version:          v,
		CmdID:            kvserverbase.CmdIDKey(data[1 : 1+kvserverbase.RaftCommandIDLen]),
	}, nil
}

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

	d, err := DecomposeEntryData(e.Type, e.Data)
	if err != nil {
		return err
	}

	var ccTarget interface {
		protoutil.Message
		AsV2() raftpb.ConfChangeV2
	}
	switch d.Type {
	case raftpb.EntryNormal:
		if d.Version == kvserverbase.RaftVersionFlatBuffer {
			// NB: this allocs, can inline to do better.
			fbCmd := raftfbs.GetRootAsCommand(e.Data[1:], 0)
			e.Data = fbCmd.RaftCommandPbBytes()
			e.ID = kvserverbase.CmdIDKey(fbCmd.IdBytes())
		}
	case raftpb.EntryConfChange:
		e.ConfChangeV1 = &raftpb.ConfChange{}
		ccTarget = e.ConfChangeV1
	case raftpb.EntryConfChangeV2:
		e.ConfChangeV2 = &raftpb.ConfChangeV2{}
		ccTarget = e.ConfChangeV2
	default:
		return errors.AssertionFailedf("unknown entry type %d", e.Type)
	}

	var payload []byte
	if ccTarget == nil {
		// Regular entry - can just read off the data we need from `d`.
		e.ID = d.CmdID
		payload = d.RaftCommandBytes
	} else {
		// Conf change - need to get payload and command ID from ConfChangeContext.
		if err := protoutil.Unmarshal(d.ConfChangeBytes, ccTarget); err != nil {
			return errors.Wrap(err, "unmarshalling ConfChange")
		}
		e.ConfChangeContext = &kvserverpb.ConfChangeContext{}
		if err := protoutil.Unmarshal(ccTarget.AsV2().Context, e.ConfChangeContext); err != nil {
			return errors.Wrap(err, "unmarshalling ConfChangeContext")
		}
		e.ID = kvserverbase.CmdIDKey(e.ConfChangeContext.CommandID)
		payload = e.ConfChangeContext.Payload
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
