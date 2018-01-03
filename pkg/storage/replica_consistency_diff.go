// Copyright 2014 The Cockroach Authors.
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
// permissions and limitations under the License.

package storage

import (
	"bytes"
	"fmt"
	"io"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
)

// ReplicaSnapshotDiff is a part of a []ReplicaSnapshotDiff which represents a diff between
// two replica snapshots. For now it's only a diff between their KV pairs.
type ReplicaSnapshotDiff struct {
	// LeaseHolder is set to true of this kv pair is only present on the lease
	// holder.
	LeaseHolder bool
	Key         roachpb.Key
	Timestamp   hlc.Timestamp
	Value       []byte
}

// ReplicaSnapshotDiffSlice groups multiple ReplicaSnapshotDiff records and
// exposes a formatting helper.
type ReplicaSnapshotDiffSlice []ReplicaSnapshotDiff

// WriteTo writes a string representation of itself to the given writer.
func (rsds ReplicaSnapshotDiffSlice) WriteTo(w io.Writer) (int64, error) {
	n, err := w.Write([]byte("--- leaseholder\n+++ follower\n"))
	if err != nil {
		return 0, err
	}
	for _, d := range rsds {
		prefix := "+"
		if d.LeaseHolder {
			// follower (RHS) has something proposer (LHS) does not have
			prefix = "-"
		}
		ts := d.Timestamp
		const format = `%s%d.%09d,%d %s
%s  ts:%s
%s  value:%s
%s  raw_key:%x raw_value:%x
`
		// TODO(tschottdorf): add pretty-printed value. We have the code in
		// cli/debug.go (printKeyValue).
		var prettyTime string
		if d.Timestamp == (hlc.Timestamp{}) {
			prettyTime = "<zero>"
		} else {
			prettyTime = d.Timestamp.GoTime().UTC().String()
		}
		num, err := fmt.Fprintf(w, format,
			prefix, ts.WallTime/1E9, ts.WallTime%1E9, ts.Logical, d.Key,
			prefix, prettyTime,
			prefix, d.Value,
			prefix, d.Key, d.Value)
		if err != nil {
			return 0, err
		}
		n += num
	}
	return int64(n), nil
}

func (rsds ReplicaSnapshotDiffSlice) String() string {
	var buf bytes.Buffer
	_, _ = rsds.WriteTo(&buf)
	return buf.String()
}

// diffs the two kv dumps between the lease holder and the replica.
func diffRange(l, r *roachpb.RaftSnapshotData) ReplicaSnapshotDiffSlice {
	if l == nil || r == nil {
		return nil
	}
	var diff []ReplicaSnapshotDiff
	i, j := 0, 0
	for {
		var e, v roachpb.RaftSnapshotData_KeyValue
		if i < len(l.KV) {
			e = l.KV[i]
		}
		if j < len(r.KV) {
			v = r.KV[j]
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
		// Check if it has finished traversing over all the lease holder keys.
		if e.Key == nil {
			if v.Key == nil {
				// Done traversing over all the replica keys. Done!
				break
			} else {
				comp = 1
			}
		} else {
			// Check if it has finished traversing over all the replica keys.
			if v.Key == nil {
				comp = -1
			} else {
				// Both lease holder and replica keys exist. Compare them.
				comp = bytes.Compare(e.Key, v.Key)
			}
		}
		switch comp {
		case -1:
			addLeaseHolder()

		case 0:
			// Timestamp sorting is weird. Timestamp{} sorts first, the
			// remainder sort in descending order. See storage/engine/doc.go.
			if e.Timestamp != v.Timestamp {
				if e.Timestamp == (hlc.Timestamp{}) {
					addLeaseHolder()
				} else if v.Timestamp == (hlc.Timestamp{}) {
					addReplica()
				} else if v.Timestamp.Less(e.Timestamp) {
					addLeaseHolder()
				} else {
					addReplica()
				}
			} else if !bytes.Equal(e.Value, v.Value) {
				addLeaseHolder()
				addReplica()
			} else {
				// No diff; skip.
				i++
				j++
			}

		case 1:
			addReplica()

		}
	}
	return diff
}
