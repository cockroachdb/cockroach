// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftlog

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverpb"
	"github.com/cockroachdb/cockroach/pkg/raft/raftpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
)

// BenchmarkRaftAdmissionMetaOverhead measures the overhead of encoding/decoding
// raft metadata, compared to not doing it. It's structured similar to how raft
// command data is encoded + decoded end-to-end, including the optional
// below-raft admission control data, where steps (2) and (4) below are only
// done for proposals subject to below-raft admission.
//
//	name                                                old time/op  new time/op  delta
//	RaftAdmissionMetaOverhead/bytes=1.0_KiB,raft-ac-10  1.30µs ± 1%  1.70µs ± 1%  +30.43%  (p=0.008 n=5+5)
//	RaftAdmissionMetaOverhead/bytes=256_KiB,raft-ac-10  51.6µs ± 4%  50.6µs ± 5%     ~     (p=0.421 n=5+5)
//	RaftAdmissionMetaOverhead/bytes=512_KiB,raft-ac-10  91.9µs ± 4%  91.2µs ± 5%     ~     (p=1.000 n=5+5)
//	RaftAdmissionMetaOverhead/bytes=1.0_MiB,raft-ac-10   148µs ± 4%   151µs ± 5%     ~     (p=0.095 n=5+5)
//	RaftAdmissionMetaOverhead/bytes=2.0_MiB,raft-ac-10   290µs ± 3%   292µs ± 1%     ~     (p=0.151 n=5+5)
func BenchmarkRaftAdmissionMetaOverhead(b *testing.B) {
	defer log.Scope(b).Close(b)

	const KiB = 1 << 10
	const MiB = 1 << 20

	for _, withRaftAdmissionMeta := range []bool{false, true} {
		for _, bytes := range []int64{1 * KiB, 256 * KiB, 512 * KiB, 1 * MiB, 2 * MiB} {
			var raftAdmissionMetaLen int
			var raftAdmissionMeta *kvflowcontrolpb.RaftAdmissionMeta
			entryEnc := EntryEncodingStandardWithoutAC

			raftCmd := mkRaftCommand(100, int(bytes), int(bytes+200))
			marshaledRaftCmd, err := protoutil.Marshal(raftCmd)
			require.NoError(b, err)

			if withRaftAdmissionMeta {
				raftAdmissionMeta = &kvflowcontrolpb.RaftAdmissionMeta{
					AdmissionPriority:   int32(admissionpb.BulkNormalPri),
					AdmissionCreateTime: 18581258253,
				}
				raftAdmissionMetaLen = raftAdmissionMeta.Size()
				entryEnc = EntryEncodingStandardWithAC
			}

			encodingBuf := make([]byte, RaftCommandPrefixLen+raftAdmissionMeta.Size()+len(marshaledRaftCmd))
			raftEnt := Entry{
				Entry: raftpb.Entry{
					Term:  1,
					Index: 1,
					Type:  raftpb.EntryNormal,
					Data:  encodingBuf,
				},
			}

			b.Run(fmt.Sprintf("bytes=%s,raft-ac=%t", humanizeutil.IBytes(bytes), withRaftAdmissionMeta),
				func(b *testing.B) {
					for i := 0; i < b.N; i++ {
						// 1. Encode the raft command prefix.
						EncodeRaftCommandPrefix(encodingBuf[:RaftCommandPrefixLen], entryEnc, "deadbeef", 0)

						// 2. If using below-raft admission, encode the raft
						// metadata right after the command prefix.
						if withRaftAdmissionMeta {
							_, err = protoutil.MarshalTo(
								raftAdmissionMeta,
								encodingBuf[RaftCommandPrefixLen:RaftCommandPrefixLen+raftAdmissionMetaLen],
							)
							require.NoError(b, err)
						}

						// 3. Marshal the rest of the command.
						_, err = protoutil.MarshalTo(raftCmd, encodingBuf[RaftCommandPrefixLen+raftAdmissionMetaLen:])
						require.NoError(b, err)

						// 4. If using below-raft admission, decode the raft
						// metadata.
						if withRaftAdmissionMeta {
							_, err := DecodeRaftAdmissionMeta(encodingBuf)
							require.NoError(b, err)
						}

						// 5. Decode the entire raft command.
						require.NoError(b, raftEnt.load())
					}
				},
			)
		}
	}
}

// TestRaftAdmissionEncodingDecoding tests encoding/decoding for
// EntryEncoding{Standard,Sideloaded}With{,out}AC{AndPriority}.
func TestRaftAdmissionEncodingDecoding(t *testing.T) {
	defer log.Scope(t).Close(t)
	ctx := context.Background()

	const bytes = 1000
	ramV1 := kvflowcontrolpb.RaftAdmissionMeta{
		AdmissionPriority:   int32(admissionpb.HighPri),
		AdmissionCreateTime: 18581258253,
		AdmissionOriginNode: 1,
	}
	ramV2 := kvflowcontrolpb.RaftAdmissionMeta{
		AdmissionPriority:   int32(raftpb.HighPri),
		AdmissionCreateTime: 18581258253,
	}
	raftCmd := mkRaftCommand(100, int(bytes), int(bytes+200))
	// These values should be ignored.
	raftCmd.AdmissionPriority = 5
	raftCmd.AdmissionCreateTime = 5
	raftCmd.AdmissionOriginNode = 5

	addSST := &kvserverpb.ReplicatedEvalResult_AddSSTable{
		Data: []byte("foo"), CRC32: 0, // not checked
	}
	cmdCopy := *raftCmd
	raftCmdWithAddSST := &cmdCopy
	raftCmdWithAddSST.WriteBatch = nil
	raftCmdWithAddSST.ReplicatedEvalResult.AddSSTable = addSST

	cmdIDKey := MakeCmdIDKey()
	for _, tc := range []struct {
		name string
		cmd  *kvserverpb.RaftCommand
		// Options for EncodeCommand.
		opts EncodeOptions
		// Encoding for EncodeCommandBytes.
		encoding     EntryEncoding
		isSideloaded bool
	}{
		{
			name: "standard-without-ac",
			cmd:  raftCmd,
			opts: EncodeOptions{
				RaftAdmissionMeta: nil,
				EncodePriority:    false,
			},
			encoding: EntryEncodingStandardWithoutAC,
		},
		{
			name: "standard-with-ac",
			cmd:  raftCmd,
			opts: EncodeOptions{
				RaftAdmissionMeta: &ramV1,
				EncodePriority:    false,
			},
			encoding: EntryEncodingStandardWithAC,
		},
		{
			name: "standard-with-ac-and-priority",
			cmd:  raftCmd,
			opts: EncodeOptions{
				RaftAdmissionMeta: &ramV2,
				EncodePriority:    true,
			},
			encoding: EntryEncodingStandardWithACAndPriority,
		},
		{
			name: "sideloaded-without-ac",
			cmd:  raftCmdWithAddSST,
			opts: EncodeOptions{
				RaftAdmissionMeta: nil,
				EncodePriority:    false,
			},
			encoding:     EntryEncodingSideloadedWithoutAC,
			isSideloaded: true,
		},
		{
			name: "sideloaded-with-ac",
			cmd:  raftCmdWithAddSST,
			opts: EncodeOptions{
				RaftAdmissionMeta: &ramV1,
				EncodePriority:    false,
			},
			encoding:     EntryEncodingSideloadedWithAC,
			isSideloaded: true,
		},
		{
			name: "sideloaded-with-ac-and-priority",
			cmd:  raftCmdWithAddSST,
			opts: EncodeOptions{
				RaftAdmissionMeta: &ramV2,
				EncodePriority:    true,
			},
			encoding:     EntryEncodingSideloadedWithACAndPriority,
			isSideloaded: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			buf1, err := EncodeCommand(ctx, tc.cmd, cmdIDKey, tc.opts)
			require.NoError(t, err)

			// Setup cmd for manual encoding.
			raftCmdMaybeWithRaftAdmissionMeta := tc.cmd
			if tc.opts.RaftAdmissionMeta != nil {
				raftCmdMaybeWithRaftAdmissionMeta.AdmissionPriority =
					tc.opts.RaftAdmissionMeta.AdmissionPriority
				raftCmdMaybeWithRaftAdmissionMeta.AdmissionCreateTime =
					tc.opts.RaftAdmissionMeta.AdmissionCreateTime
				raftCmdMaybeWithRaftAdmissionMeta.AdmissionOriginNode =
					tc.opts.RaftAdmissionMeta.AdmissionOriginNode
			} else {
				raftCmdMaybeWithRaftAdmissionMeta.AdmissionPriority = 0
				raftCmdMaybeWithRaftAdmissionMeta.AdmissionCreateTime = 0
				raftCmdMaybeWithRaftAdmissionMeta.AdmissionOriginNode = 0
			}
			cmdBytes, err := protoutil.Marshal(raftCmdMaybeWithRaftAdmissionMeta)
			require.NoError(t, err)

			var pri raftpb.Priority
			if tc.opts.RaftAdmissionMeta != nil && tc.opts.EncodePriority {
				pri = raftpb.Priority(tc.opts.RaftAdmissionMeta.AdmissionPriority)
			}
			buf2 := EncodeCommandBytes(tc.encoding, cmdIDKey, cmdBytes, pri)

			// buf1 and buf2 are not identical in terms of bytes, but should be logically
			// equivalent.

			if tc.opts.RaftAdmissionMeta != nil {
				// Decode RaftAdmissionMeta.
				meta1, err := DecodeRaftAdmissionMeta(buf1)
				require.NoError(t, err)
				meta2, err := DecodeRaftAdmissionMeta(buf2)
				require.NoError(t, err)
				require.Equal(t, *tc.opts.RaftAdmissionMeta, meta1)
				require.Equal(t, *tc.opts.RaftAdmissionMeta, meta2)
			}
			for _, buf := range [][]byte{buf1, buf2} {
				ent := raftpb.Entry{Term: 1, Index: 1, Data: buf}
				// Decode via NewEntry.
				entry, err := NewEntry(ent)
				require.NoError(t, err)
				require.Equal(t, cmdIDKey, entry.ID)
				require.Equal(t, tc.opts.RaftAdmissionMeta != nil, entry.ApplyAdmissionControl)
				ee, pri, err := EncodingOf(ent)
				require.NoError(t, err)
				require.Equal(t, tc.encoding, ee)
				if tc.opts.RaftAdmissionMeta != nil {
					require.True(t, ee.UsesAdmissionControl())
					if tc.opts.EncodePriority {
						require.Equal(t, raftpb.Priority(tc.opts.RaftAdmissionMeta.AdmissionPriority), pri)
					} else {
						require.Zero(t, pri)
					}
				}
				require.Equal(t, tc.isSideloaded, ee.IsSideloaded())
			}
		})
	}
}
