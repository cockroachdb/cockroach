// Copyright 2023 The Cockroach Authors.
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
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// encodingMask is used to encode the encoding type in the first 6 bits of the
// first byte in the entry encoding.
const encodingMask byte = 0x3F

// priMask is used to encode the raft admission control priority of the command
// in the last 2 bits of the first byte in the entry encoding.
const priMask byte = 0xC0

// EncodeCommand encodes the provided command into a slice.
//
// useRACv2 is relevant iff raftAdmissionMeta != nil.
func EncodeCommand(
	ctx context.Context,
	command *kvserverpb.RaftCommand,
	idKey kvserverbase.CmdIDKey,
	raftAdmissionMeta *kvflowcontrolpb.RaftAdmissionMeta,
	useRACv2 bool,
) ([]byte, error) {
	// Determine the encoding style for the Raft command.
	prefix := true
	entryEncoding := EntryEncodingStandardWithoutAC
	var rpri kvflowcontrolpb.RaftPriority
	if raftAdmissionMeta != nil {
		if useRACv2 {
			entryEncoding = EntryEncodingStandardWithRaftPriority
			rpri = kvflowcontrolpb.RaftPriority(raftAdmissionMeta.AdmissionPriority)
		} else {
			entryEncoding = EntryEncodingStandardWithAC
		}
	}
	if crt := command.ReplicatedEvalResult.ChangeReplicas; crt != nil {
		// EndTxnRequest with a ChangeReplicasTrigger is special because Raft
		// needs to understand it; it cannot simply be an opaque command. To
		// permit this, the command is proposed by the proposal buffer using
		// ProposeConfChange. For that reason, we also don't need a Raft command
		// prefix because the command ID is stored in a field in
		// raft.ConfChange.
		prefix = false
		if raftAdmissionMeta != nil {
			return nil, errors.AssertionFailedf("expected to encode prefix for raft commands using replication admission control")
		}
	} else if command.ReplicatedEvalResult.AddSSTable != nil {
		entryEncoding = EntryEncodingSideloadedWithoutAC
		if raftAdmissionMeta != nil {
			if useRACv2 {
				entryEncoding = EntryEncodingSideloadedWithRaftPriority
				rpri = kvflowcontrolpb.RaftPriority(raftAdmissionMeta.AdmissionPriority)
			} else {
				entryEncoding = EntryEncodingSideloadedWithAC
			}
		}

		if command.ReplicatedEvalResult.AddSSTable.Data == nil {
			return nil, errors.Errorf("cannot sideload empty SSTable")
		}
	}

	// NB: If (significantly) re-working how raft commands are encoded, make the
	// equivalent change in BenchmarkRaftAdmissionMetaOverhead.

	// Create encoding buffer.
	// INVARIANT: raftAdmissionMeta != nil => prefix
	preLen := 0
	cmdLen := command.Size()

	var admissionMetaLen int
	if prefix {
		preLen = RaftCommandPrefixLen
		if raftAdmissionMeta != nil {
			// Encode admission metadata data at the start, right after the command
			// prefix.

			admissionMetaLen = raftAdmissionMeta.Size()
			cmdLen += admissionMetaLen
		}
	}

	// Allocate the data slice with enough capacity to eventually hold the two
	// "footers" that are filled later.
	needed := preLen + cmdLen + kvserverpb.MaxRaftCommandFooterSize()
	data := make([]byte, preLen, needed)
	// Encode prefix with command ID, if necessary.
	if prefix {
		EncodeRaftCommandPrefix(data, entryEncoding, idKey, rpri)
	}

	// Encode the body of the command.
	data = data[:preLen+cmdLen]
	// Encode below-raft admission data, if any.
	if raftAdmissionMeta != nil {
		if buildutil.CrdbTestBuild {
			if raftAdmissionMeta.AdmissionOriginNode == roachpb.NodeID(0) {
				return nil, errors.AssertionFailedf("missing origin node for flow token returns")
			}
		}
		if _, err := protoutil.MarshalToSizedBuffer(
			raftAdmissionMeta,
			data[preLen:preLen+admissionMetaLen],
		); err != nil {
			return nil, err
		}
		log.VInfof(ctx, 1, "encoded raft admission meta: pri=%s create-time=%d proposer=n%s",
			admissionpb.WorkPriority(raftAdmissionMeta.AdmissionPriority),
			raftAdmissionMeta.AdmissionCreateTime,
			raftAdmissionMeta.AdmissionOriginNode,
		)
		// We could zero out what we've already encoded and marshaled to avoid
		// re-marshaling again, however that may change the size of the encoded
		// command, for which we already presized the buffer.
	}

	// Encode the rest of the command.
	if _, err := protoutil.MarshalToSizedBuffer(command, data[preLen+admissionMetaLen:]); err != nil {
		return nil, err
	}
	return data, nil
}
