// Copyright 2018 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"bytes"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"go.etcd.io/etcd/raft/raftpb"
)

// PrintKeyValue attempts to pretty-print the specified MVCCKeyValue to
// os.Stdout, falling back to '%q' formatting.
func PrintKeyValue(kv engine.MVCCKeyValue) {
	fmt.Println(SprintKeyValue(kv, true /* printKey */))
}

// SprintKey pretty-prings the specified MVCCKey.
func SprintKey(key engine.MVCCKey) string {
	return fmt.Sprintf("%s %s (%#x): ", key.Timestamp, key.Key, engine.EncodeKey(key))
}

// SprintKeyValue is like PrintKeyValue, but returns a string. If
// printKey is true, prints the key and the value together; otherwise,
// prints just the value.
func SprintKeyValue(kv engine.MVCCKeyValue, printKey bool) string {
	var sb strings.Builder
	if printKey {
		sb.WriteString(SprintKey(kv.Key))
	}
	decoders := []func(kv engine.MVCCKeyValue) (string, error){
		tryRaftLogEntry,
		tryRangeDescriptor,
		tryMeta,
		tryTxn,
		tryRangeIDKey,
		tryTimeSeries,
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
		sb.WriteString(out)
		return sb.String()
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

func decodeWriteBatch(writeBatch *storagepb.WriteBatch) (string, error) {
	if writeBatch == nil {
		return "<nil>\n", nil
	}

	r, err := engine.NewRocksDBBatchReader(writeBatch.Data)
	if err != nil {
		return "", err
	}

	// NB: always return sb.String() as the first arg, even on error, to give
	// the caller all the info we have (in case the writebatch is corrupted).
	var sb strings.Builder
	for r.Next() {
		switch r.BatchType() {
		case engine.BatchTypeDeletion:
			mvccKey, err := r.MVCCKey()
			if err != nil {
				return sb.String(), err
			}
			sb.WriteString(fmt.Sprintf("Delete: %s\n", SprintKey(mvccKey)))
		case engine.BatchTypeValue:
			mvccKey, err := r.MVCCKey()
			if err != nil {
				return sb.String(), err
			}
			sb.WriteString(fmt.Sprintf("Put: %s\n", SprintKeyValue(engine.MVCCKeyValue{
				Key:   mvccKey,
				Value: r.Value(),
			}, true /* printKey */)))
		case engine.BatchTypeMerge:
			mvccKey, err := r.MVCCKey()
			if err != nil {
				return sb.String(), err
			}
			sb.WriteString(fmt.Sprintf("Merge: %s\n", SprintKeyValue(engine.MVCCKeyValue{
				Key:   mvccKey,
				Value: r.Value(),
			}, true /* printKey */)))
		case engine.BatchTypeSingleDeletion:
			mvccKey, err := r.MVCCKey()
			if err != nil {
				return sb.String(), err
			}
			sb.WriteString(fmt.Sprintf("Single Delete: %s\n", SprintKey(mvccKey)))
		case engine.BatchTypeRangeDeletion:
			mvccStartKey, err := r.MVCCKey()
			if err != nil {
				return sb.String(), err
			}
			mvccEndKey, err := r.MVCCEndKey()
			if err != nil {
				return sb.String(), err
			}
			sb.WriteString(fmt.Sprintf(
				"Delete Range: [%s, %s)\n", SprintKey(mvccStartKey), SprintKey(mvccEndKey),
			))
		default:
			sb.WriteString(fmt.Sprintf("unsupported batch type: %d\n", r.BatchType()))
		}
	}
	return sb.String(), r.Error()
}

func tryRaftLogEntry(kv engine.MVCCKeyValue) (string, error) {
	var ent raftpb.Entry
	if err := maybeUnmarshalInline(kv.Value, &ent); err != nil {
		return "", err
	}

	var cmd storagepb.RaftCommand
	switch ent.Type {
	case raftpb.EntryNormal:
		if len(ent.Data) == 0 {
			return fmt.Sprintf("%s: EMPTY\n", &ent), nil
		}
		_, cmdData := DecodeRaftCommand(ent.Data)
		if err := protoutil.Unmarshal(cmdData, &cmd); err != nil {
			return "", err
		}
	case raftpb.EntryConfChange, raftpb.EntryConfChangeV2:
		var c raftpb.ConfChangeI
		if ent.Type == raftpb.EntryConfChange {
			var cc raftpb.ConfChange
			if err := protoutil.Unmarshal(ent.Data, &cc); err != nil {
				return "", err
			}
			c = cc
		} else {
			var cc raftpb.ConfChangeV2
			if err := protoutil.Unmarshal(ent.Data, &cc); err != nil {
				return "", err
			}
			c = cc
		}

		var ctx ConfChangeContext
		if err := protoutil.Unmarshal(c.AsV2().Context, &ctx); err != nil {
			return "", err
		}
		if err := protoutil.Unmarshal(ctx.Payload, &cmd); err != nil {
			return "", err
		}
	default:
		return "", fmt.Errorf("unknown log entry type: %s", &ent)
	}
	ent.Data = nil

	var leaseStr string
	if l := cmd.DeprecatedProposerLease; l != nil {
		leaseStr = l.String() // use full lease, if available
	} else {
		leaseStr = fmt.Sprintf("lease #%d", cmd.ProposerLeaseSequence)
	}

	wbStr, err := decodeWriteBatch(cmd.WriteBatch)
	if err != nil {
		wbStr = "failed to decode: " + err.Error() + "\nafter:\n" + wbStr
	}
	cmd.WriteBatch = nil

	return fmt.Sprintf("%s by %s\n%s\nwrite batch:\n%s", &ent, leaseStr, &cmd, wbStr), nil
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

	case bytes.Equal(suffix, keys.LocalRaftTruncatedStateLegacySuffix):
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

func tryTimeSeries(kv engine.MVCCKeyValue) (string, error) {
	if len(kv.Value) == 0 || !bytes.HasPrefix(kv.Key.Key, keys.TimeseriesPrefix) {
		return "", errors.New("empty or not TS")
	}
	var meta enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(kv.Value, &meta); err != nil {
		return "", err
	}
	v := roachpb.Value{RawBytes: meta.RawBytes}
	var ts roachpb.InternalTimeSeriesData
	if err := v.GetProto(&ts); err != nil {
		return "", err
	}
	return fmt.Sprintf("%+v [mergeTS=%s]", &ts, meta.MergeTimestamp), nil
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

type stringifyWriteBatch storagepb.WriteBatch

func (s *stringifyWriteBatch) String() string {
	wbStr, err := decodeWriteBatch((*storagepb.WriteBatch)(s))
	if err == nil {
		return wbStr
	}
	return fmt.Sprintf("failed to stringify write batch (%x): %s", s.Data, err)
}
