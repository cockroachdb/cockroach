// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver

import (
	"bytes"
	fmt "fmt"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/redact"
)

// ReplicaSnapshotDiff is a part of a []ReplicaSnapshotDiff which represents a diff between
// two replica snapshots. For now it's only a diff between their KV pairs.
type ReplicaSnapshotDiff struct {
	// LeaseHolder is set to true of this kv pair is only present on the lease
	// holder.
	LeaseHolder bool
	Key         roachpb.Key
	EndKey      roachpb.Key // only set for range keys (MVCCRangeKey)
	Timestamp   hlc.Timestamp
	Value       []byte
}

// ReplicaSnapshotDiffSlice groups multiple ReplicaSnapshotDiff records and
// exposes a formatting helper.
type ReplicaSnapshotDiffSlice []ReplicaSnapshotDiff

// SafeFormat implements redact.SafeFormatter.
func (rsds ReplicaSnapshotDiffSlice) SafeFormat(buf redact.SafePrinter, _ rune) {
	buf.Printf("--- leaseholder\n+++ follower\n")
	for _, d := range rsds {
		prefix := redact.SafeString("+")
		if d.LeaseHolder {
			// Lease holder (RHS) has something follower (LHS) does not have.
			prefix = redact.SafeString("-")
		}
		if len(d.EndKey) > 0 {
			const rangeKVFormat = `%s%s %s
%s    ts:%s
%s    value:%s
%s    raw from/to/ts/value: %x %x %x %x
`
			buf.Printf(rangeKVFormat,
				prefix, d.Timestamp, roachpb.Span{Key: d.Key, EndKey: d.EndKey},
				prefix, d.Timestamp.GoTime(),
				prefix, fmt.Sprintf("%q", d.Value), // just print the raw value for now
				prefix, storage.EncodeMVCCKeyPrefix(d.Key), storage.EncodeMVCCKeyPrefix(d.EndKey),
				storage.EncodeMVCCTimestampSuffix(d.Timestamp), d.Value)

		} else {
			const kvFormat = `%s%s %s
%s    ts:%s
%s    value:%s
%s    raw mvcc_key/value: %x %x
`
			mvccKey := storage.MVCCKey{Key: d.Key, Timestamp: d.Timestamp}
			buf.Printf(kvFormat,
				prefix, d.Timestamp, d.Key,
				prefix, d.Timestamp.GoTime(),
				prefix, SprintMVCCKeyValue(
					storage.MVCCKeyValue{Key: mvccKey, Value: d.Value}, false /* printKey */),
				prefix, storage.EncodeMVCCKey(mvccKey), d.Value)
		}
	}
}

func (rsds ReplicaSnapshotDiffSlice) String() string {
	return redact.StringWithoutMarkers(rsds)
}

// diffs the two kv dumps between the lease holder and the replica.
func diffRange(l, r *roachpb.RaftSnapshotData) ReplicaSnapshotDiffSlice {
	if l == nil || r == nil {
		return nil
	}
	var diff ReplicaSnapshotDiffSlice
	diff = append(diff, diffKVs(l.KV, r.KV)...)
	diff = append(diff, diffRangeKeys(l.RangeKV, r.RangeKV)...)
	return diff
}

func diffKVs(l, r []roachpb.RaftSnapshotData_KeyValue) ReplicaSnapshotDiffSlice {
	var diff []ReplicaSnapshotDiff
	i, j := 0, 0
	for {
		var e, v roachpb.RaftSnapshotData_KeyValue
		if i < len(l) {
			e = l[i]
		}
		if j < len(r) {
			v = r[j]
		}

		addLeaseHolder := func() {
			diff = append(diff, ReplicaSnapshotDiff{LeaseHolder: true, Key: e.Key, Timestamp: e.Timestamp, Value: e.Value})
			i++
		}
		addReplica := func() {
			diff = append(diff, ReplicaSnapshotDiff{LeaseHolder: false, Key: v.Key, Timestamp: v.Timestamp, Value: v.Value})
			j++
		}

		// Compare keys.
		var comp int
		if e.Key == nil && v.Key == nil {
			// Done!
			break
		} else if e.Key == nil {
			// Finished traversing over all the lease holder keys.
			comp = 1
		} else if v.Key == nil {
			// Finished traversing over all the replica keys.
			comp = -1
		} else {
			// Both lease holder and replica keys exist. Compare them.
			comp = storage.MVCCKey{Key: e.Key, Timestamp: e.Timestamp}.Compare(
				storage.MVCCKey{Key: v.Key, Timestamp: v.Timestamp})
		}

		switch comp {
		case -1:
			addLeaseHolder()
		case 1:
			addReplica()
		case 0:
			if !bytes.Equal(e.Value, v.Value) {
				addLeaseHolder()
				addReplica()
			} else {
				// No diff; skip.
				i++
				j++
			}
		}
	}
	return diff
}

func diffRangeKeys(l, r []roachpb.RaftSnapshotData_RangeKeyValue) ReplicaSnapshotDiffSlice {
	var diff []ReplicaSnapshotDiff
	i, j := 0, 0
	for {
		var e, v roachpb.RaftSnapshotData_RangeKeyValue
		if i < len(l) {
			e = l[i]
		}
		if j < len(r) {
			v = r[j]
		}

		addLeaseHolder := func() {
			diff = append(diff, ReplicaSnapshotDiff{LeaseHolder: true, Key: e.StartKey, EndKey: e.EndKey,
				Timestamp: e.Timestamp, Value: e.Value})
			i++
		}
		addReplica := func() {
			diff = append(diff, ReplicaSnapshotDiff{LeaseHolder: false, Key: v.StartKey, EndKey: v.EndKey,
				Timestamp: v.Timestamp, Value: v.Value})
			j++
		}

		// Compare keys.
		var comp int
		if e.StartKey == nil && e.EndKey == nil && v.StartKey == nil && v.EndKey == nil {
			// Done!
			break
		} else if e.StartKey == nil && e.EndKey == nil {
			// Finished traversing over all the lease holder keys.
			comp = 1
		} else if v.StartKey == nil && v.EndKey == nil {
			// Finished traversing over all the replica keys.
			comp = -1
		} else {
			// Both lease holder and replica keys exist. Compare them.
			eMVCC := storage.MVCCRangeKey{StartKey: e.StartKey, EndKey: e.EndKey, Timestamp: e.Timestamp}
			vMVCC := storage.MVCCRangeKey{StartKey: v.StartKey, EndKey: v.EndKey, Timestamp: v.Timestamp}
			comp = eMVCC.Compare(vMVCC)
		}

		switch comp {
		case -1:
			addLeaseHolder()
		case 1:
			addReplica()
		case 0:
			if !bytes.Equal(e.Value, v.Value) {
				addLeaseHolder()
				addReplica()
			} else {
				// No diff; skip.
				i++
				j++
			}
		}
	}
	return diff
}
