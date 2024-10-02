// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftlog

import (
	"context"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/errors"
)

// EncodeOptions provides additional information that may be need to be
// encoded.
type EncodeOptions struct {
	// RaftAdmissionMeta is non-nil iff this entry should be encoded using an AC
	// encoding.
	RaftAdmissionMeta *kvflowcontrolpb.RaftAdmissionMeta
	// When this entry should be encoded using an AC encoding, this specifies
	// whether a WithACAndPriority encoding should be used.
	EncodePriority bool
}

// EncodeCommand encodes the provided command into a slice.
func EncodeCommand(
	ctx context.Context,
	command *kvserverpb.RaftCommand,
	idKey kvserverbase.CmdIDKey,
	opts EncodeOptions,
) ([]byte, error) {
	// Defensive: zero out fields that are specified via opts.RaftAdmissionMeta,
	// in case the caller happens to have accidentally set them. We will
	// serialize both opts.RaftAdmissionMeta and the command, expecting a merge,
	// since the tag numbers in RaftAdmissionMeta are a subset of those in the
	// command, and we want the RaftAdmissionMeta values to be the ones that are
	// decoded. This merge behavior relies on zero valued fields not being
	// encoded.
	command.AdmissionPriority = 0
	command.AdmissionCreateTime = 0
	command.AdmissionOriginNode = 0

	// Determine whether the command has a prefix, and if yes, the encoding
	// style for the Raft command.
	var prefix bool
	var entryEncoding EntryEncoding
	if crt := command.ReplicatedEvalResult.ChangeReplicas; crt != nil {
		// EndTxnRequest with a ChangeReplicasTrigger is special because Raft
		// needs to understand it; it cannot simply be an opaque command. To
		// permit this, the command is proposed by the proposal buffer using
		// ProposeConfChange. For that reason, we also don't need a Raft command
		// prefix because the command ID is stored in a field in
		// raft.ConfChange.
		prefix = false
	} else {
		prefix = true
		isAddSStable := command.ReplicatedEvalResult.AddSSTable != nil
		if isAddSStable {
			if command.ReplicatedEvalResult.AddSSTable.Data == nil {
				return nil, errors.Errorf("cannot sideload empty SSTable")
			}
			entryEncoding = EntryEncodingSideloadedWithoutAC
			if opts.RaftAdmissionMeta != nil {
				if opts.EncodePriority {
					entryEncoding = EntryEncodingSideloadedWithACAndPriority
				} else {
					entryEncoding = EntryEncodingSideloadedWithAC
				}
			}
		} else {
			entryEncoding = EntryEncodingStandardWithoutAC
			if opts.RaftAdmissionMeta != nil {
				if opts.EncodePriority {
					entryEncoding = EntryEncodingStandardWithACAndPriority
				} else {
					entryEncoding = EntryEncodingStandardWithAC
				}
			}
		}
	}
	// pri is only used for the WithACAndPriority encodings.
	var pri raftpb.Priority
	if entryEncoding == EntryEncodingStandardWithACAndPriority ||
		entryEncoding == EntryEncodingSideloadedWithACAndPriority {
		pri = raftpb.Priority(opts.RaftAdmissionMeta.AdmissionPriority)
		if buildutil.CrdbTestBuild && (opts.RaftAdmissionMeta.AdmissionPriority > int32(raftpb.HighPri) ||
			opts.RaftAdmissionMeta.AdmissionPriority < int32(raftpb.LowPri)) {
			panic(errors.AssertionFailedf("priority %d is not a valid raftpb.Priority",
				opts.RaftAdmissionMeta.AdmissionPriority))
		}
	}
	// NB: If (significantly) re-working how raft commands are encoded, make the
	// equivalent change in BenchmarkRaftAdmissionMetaOverhead.

	// Create encoding buffer.
	preLen := 0
	if prefix {
		preLen = RaftCommandPrefixLen
	}
	var admissionMetaLen int
	if opts.RaftAdmissionMeta != nil {
		// Encode admission metadata data at the start, right after the command
		// prefix.
		admissionMetaLen = opts.RaftAdmissionMeta.Size()
	}
	cmdLen := command.Size() + admissionMetaLen
	// Allocate the data slice with enough capacity to eventually hold the two
	// "footers" that are filled later.
	needed := preLen + cmdLen + kvserverpb.MaxRaftCommandFooterSize()
	data := make([]byte, preLen, needed)

	// Encode prefix with command ID, if necessary.
	if prefix {
		EncodeRaftCommandPrefix(data, entryEncoding, idKey, pri)
	}

	// Encode the body of the command.
	data = data[:preLen+cmdLen]
	// Encode below-raft admission data, if any.
	if opts.RaftAdmissionMeta != nil {
		if buildutil.CrdbTestBuild && !opts.EncodePriority {
			if opts.RaftAdmissionMeta.AdmissionOriginNode == roachpb.NodeID(0) {
				return nil, errors.AssertionFailedf("missing origin node for flow token returns")
			}
		}
		if _, err := protoutil.MarshalToSizedBuffer(
			opts.RaftAdmissionMeta,
			data[preLen:preLen+admissionMetaLen],
		); err != nil {
			return nil, err
		}
	}
	// Encode the rest of the command.
	if _, err := protoutil.MarshalToSizedBuffer(command, data[preLen+admissionMetaLen:]); err != nil {
		return nil, err
	}
	return data, nil
}
