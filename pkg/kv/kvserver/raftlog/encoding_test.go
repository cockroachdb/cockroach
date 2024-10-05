// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package raftlog

import (
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvflowcontrol/kvflowcontrolpb"
	"github.com/cockroachdb/cockroach/pkg/util/admission/admissionpb"
	"github.com/cockroachdb/cockroach/pkg/util/humanizeutil"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
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
						EncodeRaftCommandPrefix(encodingBuf[:RaftCommandPrefixLen], entryEnc, "deadbeef")

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
