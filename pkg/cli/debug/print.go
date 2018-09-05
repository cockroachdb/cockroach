// Copyright 2018 The Cockroach Authors.
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

package debug

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagebase"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"go.etcd.io/etcd/raft/raftpb"
)

// PrintKeyValue attempts to pretty-print the specified MVCCKeyValue to
// os.Stdout, falling back to '%q' formatting.
func PrintKeyValue(kv engine.MVCCKeyValue, sizes bool) {
	fmt.Println(SprintKeyValue(kv, sizes))
}

// SprintKeyValue is like PrintKeyValue, but returns a string.
func SprintKeyValue(kv engine.MVCCKeyValue, sizes bool) string {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "%s %s: ", kv.Key.Timestamp, kv.Key.Key)
	decoders := []func(kv engine.MVCCKeyValue) (string, error){
		tryRaftLogEntry,
		tryRangeDescriptor,
		tryMeta,
		tryTxn,
		tryRangeIDKey,
		tryIntent,
		func(kv engine.MVCCKeyValue) (string, error) {
			// No better idea, just print raw bytes and hope that folks use `less -S`.
			return fmt.Sprintf("%q", kv.Value), nil
		},
	}
	for _, decoder := range decoders {
		out, err := decoder(kv)
		if err != nil {
			continue
		}
		fmt.Fprintln(&buf, out)
		return buf.String()
	}
	panic("unreachable")
}

func tryRangeDescriptor(kv engine.MVCCKeyValue) (string, error) {
	if err := IsRangeDescriptorKey(kv.Key); err != nil {
		return "", err
	}
	var desc roachpb.RangeDescriptor
	if err := getProtoValue(kv.Value, &desc); err != nil {
		return "", err
	}
	return descStr(desc), nil
}

func tryIntent(kv engine.MVCCKeyValue) (string, error) {
	if len(kv.Value) == 0 {
		return "", errors.New("empty")
	}
	var meta enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(kv.Value, &meta); err != nil {
		return "", err
	}
	s := fmt.Sprintf("%+v", meta)
	if meta.Txn != nil {
		s = meta.Txn.Timestamp.String() + " " + s
	}
	return s, nil
}

func tryRaftLogEntry(kv engine.MVCCKeyValue) (string, error) {
	var ent raftpb.Entry
	if err := maybeUnmarshalInline(kv.Value, &ent); err != nil {
		return "", err
	}
	if ent.Type == raftpb.EntryNormal {
		if len(ent.Data) > 0 {
			_, cmdData := storage.DecodeRaftCommand(ent.Data)
			var cmd storagebase.RaftCommand
			if err := protoutil.Unmarshal(cmdData, &cmd); err != nil {
				return "", err
			}
			ent.Data = nil
			var leaseStr string
			if l := cmd.DeprecatedProposerLease; l != nil {
				// Use the full lease, if available.
				leaseStr = l.String()
			} else {
				leaseStr = fmt.Sprintf("lease #%d", cmd.ProposerLeaseSequence)
			}
			return fmt.Sprintf("%s by %s\n%s\n", &ent, leaseStr, &cmd), nil
		}
		return fmt.Sprintf("%s: EMPTY\n", &ent), nil
	} else if ent.Type == raftpb.EntryConfChange {
		var cc raftpb.ConfChange
		if err := protoutil.Unmarshal(ent.Data, &cc); err != nil {
			return "", err
		}
		var ctx storage.ConfChangeContext
		if err := protoutil.Unmarshal(cc.Context, &ctx); err != nil {
			return "", err
		}
		var cmd storagebase.ReplicatedEvalResult
		if err := protoutil.Unmarshal(ctx.Payload, &cmd); err != nil {
			return "", err
		}
		ent.Data = nil
		return fmt.Sprintf("%s\n%s\n", &ent, &cmd), nil
	}
	return "", fmt.Errorf("unknown log entry type: %s", &ent)
}

func tryTxn(kv engine.MVCCKeyValue) (string, error) {
	var txn roachpb.Transaction
	if err := maybeUnmarshalInline(kv.Value, &txn); err != nil {
		return "", err
	}
	return txn.String() + "\n", nil
}

func tryRangeIDKey(kv engine.MVCCKeyValue) (string, error) {
	if kv.Key.Timestamp != (hlc.Timestamp{}) {
		return "", fmt.Errorf("range ID keys shouldn't have timestamps: %s", kv.Key)
	}
	_, _, suffix, _, err := keys.DecodeRangeIDKey(kv.Key.Key)
	if err != nil {
		return "", err
	}

	// All range ID keys are stored inline on the metadata.
	var meta enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(kv.Value, &meta); err != nil {
		return "", err
	}
	value := roachpb.Value{RawBytes: meta.RawBytes}

	// Values encoded as protobufs set msg and continue outside the
	// switch. Other types are handled inside the switch and return.
	var msg protoutil.Message
	switch {
	case bytes.Equal(suffix, keys.LocalLeaseAppliedIndexLegacySuffix):
		fallthrough
	case bytes.Equal(suffix, keys.LocalRaftAppliedIndexLegacySuffix):
		i, err := value.GetInt()
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(i, 10), nil

	case bytes.Equal(suffix, keys.LocalRangeFrozenStatusSuffix):
		b, err := value.GetBool()
		if err != nil {
			return "", err
		}
		return strconv.FormatBool(b), nil

	case bytes.Equal(suffix, keys.LocalAbortSpanSuffix):
		msg = &roachpb.AbortSpanEntry{}

	case bytes.Equal(suffix, keys.LocalRangeLastGCSuffix):
		msg = &hlc.Timestamp{}

	case bytes.Equal(suffix, keys.LocalRaftTombstoneSuffix):
		msg = &roachpb.RaftTombstone{}

	case bytes.Equal(suffix, keys.LocalRaftTruncatedStateSuffix):
		msg = &roachpb.RaftTruncatedState{}

	case bytes.Equal(suffix, keys.LocalRangeLeaseSuffix):
		msg = &roachpb.Lease{}

	case bytes.Equal(suffix, keys.LocalRangeAppliedStateSuffix):
		msg = &enginepb.RangeAppliedState{}

	case bytes.Equal(suffix, keys.LocalRangeStatsLegacySuffix):
		msg = &enginepb.MVCCStats{}

	case bytes.Equal(suffix, keys.LocalRaftHardStateSuffix):
		msg = &raftpb.HardState{}

	case bytes.Equal(suffix, keys.LocalRaftLastIndexSuffix):
		i, err := value.GetInt()
		if err != nil {
			return "", err
		}
		return strconv.FormatInt(i, 10), nil

	case bytes.Equal(suffix, keys.LocalRangeLastVerificationTimestampSuffixDeprecated):
		msg = &hlc.Timestamp{}

	case bytes.Equal(suffix, keys.LocalRangeLastReplicaGCTimestampSuffix):
		msg = &hlc.Timestamp{}

	default:
		return "", fmt.Errorf("unknown raft id key %s", suffix)
	}

	if err := value.GetProto(msg); err != nil {
		return "", err
	}
	return msg.String(), nil
}

func tryMeta(kv engine.MVCCKeyValue) (string, error) {
	if !bytes.HasPrefix(kv.Key.Key, keys.Meta1Prefix) && !bytes.HasPrefix(kv.Key.Key, keys.Meta2Prefix) {
		return "", errors.New("not a meta key")
	}
	value := roachpb.Value{
		Timestamp: kv.Key.Timestamp,
		RawBytes:  kv.Value,
	}
	var desc roachpb.RangeDescriptor
	if err := value.GetProto(&desc); err != nil {
		return "", err
	}
	return descStr(desc), nil
}

// IsRangeDescriptorKey returns nil if the key decodes as a RangeDescriptor.
func IsRangeDescriptorKey(key engine.MVCCKey) error {
	_, suffix, _, err := keys.DecodeRangeKey(key.Key)
	if err != nil {
		return err
	}
	if !bytes.Equal(suffix, keys.LocalRangeDescriptorSuffix) {
		return fmt.Errorf("wrong suffix: %s", suffix)
	}
	return nil
}

func getProtoValue(data []byte, msg protoutil.Message) error {
	value := roachpb.Value{
		RawBytes: data,
	}
	return value.GetProto(msg)
}

func descStr(desc roachpb.RangeDescriptor) string {
	return fmt.Sprintf("[%s, %s)\n\tRaw:%s\n",
		desc.StartKey, desc.EndKey, &desc)
}

func maybeUnmarshalInline(v []byte, dest protoutil.Message) error {
	var meta enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(v, &meta); err != nil {
		return err
	}
	value := roachpb.Value{
		RawBytes: meta.RawBytes,
	}
	return value.GetProto(dest)
}
