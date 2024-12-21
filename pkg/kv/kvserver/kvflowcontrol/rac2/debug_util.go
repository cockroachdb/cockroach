// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package rac2

import (
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/redact"
)

// NOTE: These functions are currently only used for debugging and testing
// RaftEvent. They are verbose and not optimized for performance, so should not
// be used unless behind a vmodule filter, or in testing, or on a terminating
// condition.
//
// An example output from `HandleRaftEventRaftMuLocked` of the RaftEvent:
//
// TODO(kvoli): Consider moving relevant functions closer to raft, for shared
// use.

func debugFmtRaftEvent(re RaftEvent) redact.StringBuilder {
	var buf redact.StringBuilder
	buf.Printf("mode=%v term=%d snap=%v", re.MsgAppMode, re.Term, re.Snap)
	buf.Printf("\n[")
	buf.Printf("\n  replicas_state_info(%d)=", len(re.ReplicasStateInfo))
	debugFmtReplicaStateInfos(&buf, re.ReplicasStateInfo)
	buf.Printf("\n  entries(%d)=", len(re.Entries))
	debugFmtEntries(&buf, re.Entries)
	buf.Printf("\n  msg_apps=")
	debugFmtMsgApps(&buf, re.MsgApps)
	buf.Printf("\n]")
	return buf
}

func debugFmtEntry(buf *redact.StringBuilder, entry raftpb.Entry) {
	buf.Printf("(%v/%v %v)", entry.Term, entry.Index, entry.Type)
}

func debugFmtEntries(buf *redact.StringBuilder, entries []raftpb.Entry) {
	for _, entry := range entries {
		debugFmtEntry(buf, entry)
	}
}

func debugFmtMsg(buf *redact.StringBuilder, msg raftpb.Message) {
	buf.Printf(
		"%v->%v %v/%v log_term=%v match=%v commit=%v lead=%v vote=%v "+
			"reject=%v reject_hint=%v",
		msg.From, msg.To, msg.Term, msg.Index, msg.LogTerm,
		msg.Match, msg.Commit, msg.Lead, msg.Vote,
		msg.Reject, msg.RejectHint)
	buf.Printf("\n        entries=")
	debugFmtEntries(buf, msg.Entries)
	if len(msg.Responses) > 0 {
		buf.Printf("\n        responses=")
		debugFmtMsgs(buf, msg.Responses)
	}
}

func debugFmtMsgs(buf *redact.StringBuilder, msgs []raftpb.Message) {
	for _, msg := range msgs {
		buf.Printf("\n      ")
		debugFmtMsg(buf, msg)
	}
}

func debugFmtMsgApps(buf *redact.StringBuilder, msgApps map[roachpb.ReplicaID][]raftpb.Message) {
	for replicaID, msgs := range msgApps {
		if len(msgs) == 0 {
			continue
		}
		buf.Printf("\n    /%v:", replicaID)
		debugFmtMsgs(buf, msgs)
	}
}

func debugFmtReplicaStateInfos(
	buf *redact.StringBuilder, replicaStateInfos map[roachpb.ReplicaID]ReplicaStateInfo,
) {
	buf.Printf("[")
	i := 0
	for replicaID, rsi := range replicaStateInfos {
		if i > 0 {
			buf.Printf(" ")
		}
		buf.Printf("%v=%v", replicaID, rsi)
		i++
	}
	buf.Printf("]")
}
