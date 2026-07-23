// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

// TODO(sep-raft-log): there's also a `raftentry` package (it has the raft entry
// cache), that package might want to move closer to this code. Revisit when
// we've made progress factoring out the raft state and apply logic out of
// `kvserver`.

package raftlog

import (
	"sync"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// EncodingOf determines the EntryEncoding for a given Entry. When
// EntryEncoding is one of the WithACAndPriority encodings, the
// raftpb.Priority is populated.
func EncodingOf(ent raftpb.Entry) (EntryEncoding, raftpb.Priority, error) {
	if len(ent.Data) == 0 {
		// An empty command.
		return EntryEncodingEmpty, 0, nil
	}

	switch ent.Type {
	case raftpb.EntryConfChange:
		return EntryEncodingRaftConfChange, 0, nil
	case raftpb.EntryConfChangeV2:
		return EntryEncodingRaftConfChangeV2, 0, nil
	case raftpb.EntryNormal:
	default:
		return 0, 0, errors.AssertionFailedf("unknown EntryType %d", ent.Type)
	}

	encoding := ent.Data[0] & encodingMask
	switch encoding {
	case entryEncodingStandardWithACPrefixByte:
		return EntryEncodingStandardWithAC, 0, nil
	case entryEncodingSideloadedWithACPrefixByte:
		return EntryEncodingSideloadedWithAC, 0, nil
	case entryEncodingStandardWithoutACPrefixByte:
		return EntryEncodingStandardWithoutAC, 0, nil
	case entryEncodingSideloadedWithoutACPrefixByte:
		return EntryEncodingSideloadedWithoutAC, 0, nil
	case entryEncodingStandardWithACAndPriorityPrefixByte:
		return EntryEncodingStandardWithACAndPriority, getPriority(ent.Data[0]), nil
	case entryEncodingSideloadedWithACAndPriorityPrefixByte:
		return EntryEncodingSideloadedWithACAndPriority, getPriority(ent.Data[0]), nil
	default:
		return 0, 0, errors.AssertionFailedf("unknown command encoding version %d", ent.Data[0])
	}
}

// DecomposeRaftEncodingStandardOrSideloaded extracts the CmdIDKey and the
// marshaled kvserverpb.RaftCommand from a raftpb.Entry slice known to have
// Entry with type EntryEncoding{Standard,Sideloaded}With{,out}AC.
// All these variants, mod the prefix byte, share an encoding.
func DecomposeRaftEncodingStandardOrSideloaded(data []byte) (kvserverbase.CmdIDKey, []byte) {
	return kvserverbase.CmdIDKey(data[1 : 1+RaftCommandIDLen]), data[1+RaftCommandIDLen:]
}

// Entry contains data related to a raft log entry. This is the raftpb.Entry
// itself but also all encapsulated data relevant for command application and
// admission control.
type Entry struct {
	raftpb.Entry
	ID                kvserverbase.CmdIDKey // may be empty for zero Entry
	Cmd               kvserverpb.RaftCommand
	ConfChangeV1      *raftpb.ConfChange            // only set for config change
	ConfChangeV2      *raftpb.ConfChangeV2          // only set for config change
	ConfChangeContext *kvserverpb.ConfChangeContext // only set for config change
	// ApplyAdmissionControl determines whether this entry is subject to
	// replication admission control. Only applies for entries with encoding
	// EntryEncoding{Standard,Sideloaded}WithAC.
	ApplyAdmissionControl bool
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

// RaftEntryFromRawValue decodes a raft.Entry from a raw MVCC value.
//
// Same as NewEntryFromRawValue, but doesn't decode the command and doesn't use
// the pool of entries.
func RaftEntryFromRawValue(b []byte) (raftpb.Entry, error) {
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
	typ, _, err := EncodingOf(e.Entry)
	if err != nil {
		return err
	}

	// We're trying to arrive at the marshaled representation of
	// kvserverpb.RaftCommand.
	var raftCmdBytes []byte
	// Set if this entry represents a raft configuration change, in which case
	// ccTarget will be set to either a raftpb.ConfChange or raftpb.ConfChangeV2
	// and unmarshaled into, to further unwrap towards the kvserverpb.RaftCommand.
	var ccTarget interface {
		protoutil.Message
		AsV2() raftpb.ConfChangeV2
	}
	switch typ {
	case EntryEncodingStandardWithAC, EntryEncodingSideloadedWithAC,
		EntryEncodingStandardWithACAndPriority, EntryEncodingSideloadedWithACAndPriority:
		e.ID, raftCmdBytes = DecomposeRaftEncodingStandardOrSideloaded(e.Entry.Data)
		e.ApplyAdmissionControl = true
	case EntryEncodingStandardWithoutAC, EntryEncodingSideloadedWithoutAC:
		e.ID, raftCmdBytes = DecomposeRaftEncodingStandardOrSideloaded(e.Entry.Data)
	case EntryEncodingEmpty:
		// Nothing to load, the empty raftpb.Entry is represented by a trivial
		// Entry.
		return nil
	case EntryEncodingRaftConfChange:
		e.ConfChangeV1 = &raftpb.ConfChange{}
		ccTarget = e.ConfChangeV1
	case EntryEncodingRaftConfChangeV2:
		e.ConfChangeV2 = &raftpb.ConfChangeV2{}
		ccTarget = e.ConfChangeV2
	default:
		return errors.AssertionFailedf("unknown entry type %d", e.Type)
	}

	if ccTarget != nil {
		// Conf change - more unmarshaling to do.
		if err := protoutil.Unmarshal(e.Entry.Data, ccTarget); err != nil {
			return errors.Wrap(err, "unmarshalling ConfChange")
		}
		e.ConfChangeContext = &kvserverpb.ConfChangeContext{}
		if err := protoutil.Unmarshal(ccTarget.AsV2().Context, e.ConfChangeContext); err != nil {
			return errors.Wrap(err, "unmarshalling ConfChangeContext")
		}
		e.ID = kvserverbase.CmdIDKey(e.ConfChangeContext.CommandID)
		raftCmdBytes = e.ConfChangeContext.Payload
	} else if len(raftCmdBytes) == 0 {
		// Empty commands may be proposed to wake the leader, e.g. during
		// unquiescence. Ignore them during application by clearing the command ID
		// and other fields (see CheckForcedErr), similarly to those submitted by
		// Raft on leader changes (see EntryEncodingEmpty).
		*e = Entry{Entry: e.Entry}
		return nil
	}

	return errors.Wrap(protoutil.Unmarshal(raftCmdBytes, &e.Cmd), "unmarshalling RaftCommand")
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
