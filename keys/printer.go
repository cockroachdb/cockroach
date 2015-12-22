// Copyright 2015 The Cockroach Authors.
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
//
// Author: Veteran Lu (23907238@qq.com)

package keys

import (
	"bytes"
	"fmt"
	"time"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/encoding"
)

type dictEntry struct {
	name   string
	prefix roachpb.Key
	// print the key's pretty value, key has been removed prefix data
	ppFunc func(r encoding.Reader) string
}

var (
	keyDict = []struct {
		name    string
		start   roachpb.Key
		end     roachpb.Key
		entries []dictEntry
	}{
		{name: "/Local", start: localPrefix, end: LocalMax, entries: []dictEntry{
			{name: "/Store", prefix: roachpb.Key(localStorePrefix), ppFunc: localStoreKeyPrint},
			{name: "/RangeID", prefix: roachpb.Key(LocalRangeIDPrefix), ppFunc: localRangeIDKeyPrint},
			{name: "/Range", prefix: LocalRangePrefix, ppFunc: localRangeKeyPrint},
		}},
		{name: "/Meta1", start: Meta1Prefix, end: Meta1KeyMax, entries: []dictEntry{
			{name: "", prefix: Meta1Prefix, ppFunc: print},
		}},
		{name: "/Meta2", start: Meta2Prefix, end: Meta2KeyMax, entries: []dictEntry{
			{name: "", prefix: Meta2Prefix, ppFunc: print},
		}},
		{name: "/System", start: SystemPrefix, end: SystemMax, entries: []dictEntry{
			{name: "/StatusStore", prefix: StatusStorePrefix, ppFunc: decodeKeyPrint},
			{name: "/StatusNode", prefix: StatusNodePrefix, ppFunc: decodeKeyPrint},
		}},
		{name: "/Table", start: TableDataMin, end: TableDataMax, entries: []dictEntry{
			{name: "", prefix: nil, ppFunc: decodeKeyPrint},
		}},
	}

	rangeIDSuffixDict = []struct {
		name   string
		suffix []byte
		ppFunc func(r encoding.Reader) string
	}{
		{name: "SequenceCache", suffix: LocalSequenceCacheSuffix, ppFunc: sequenceCacheKeyPrint},
		{name: "RaftLeaderLease", suffix: localRaftLeaderLeaseSuffix},
		{name: "RaftTombstone", suffix: localRaftTombstoneSuffix},
		{name: "RaftHardState", suffix: localRaftHardStateSuffix},
		{name: "RaftAppliedIndex", suffix: localRaftAppliedIndexSuffix},
		{name: "RaftLog", suffix: localRaftLogSuffix, ppFunc: raftLogKeyPrint},
		{name: "RaftTruncatedState", suffix: localRaftTruncatedStateSuffix},
		{name: "RaftLastIndex", suffix: localRaftLastIndexSuffix},
		{name: "RangeGCMetadata", suffix: localRangeGCMetadataSuffix},
		{name: "RangeLastVerificationTimestamp", suffix: localRangeLastVerificationTimestampSuffix},
		{name: "RangeStats", suffix: localRangeStatsSuffix},
	}

	rangeSuffixDict = []struct {
		name   string
		suffix []byte
		atEnd  bool
	}{
		{name: "RangeDescriptor", suffix: LocalRangeDescriptorSuffix, atEnd: true},
		{name: "RangeTreeNode", suffix: localRangeTreeNodeSuffix, atEnd: true},
		{name: "Transaction", suffix: localTransactionSuffix, atEnd: false},
	}
)

func localStoreKeyPrint(key encoding.Reader) string {
	if bytes.HasPrefix(key.RawBytesRemaining(), localStoreIdentSuffix) {
		return "/storeIdent"
	}

	return fmt.Sprintf("%q", key.RawBytesRemaining())
}

func raftLogKeyPrint(key encoding.Reader) string {
	var logIndex uint64
	var err error
	logIndex, err = encoding.DecodeUint64(key)
	if err != nil {
		return fmt.Sprintf("/err<%v:%q>", err, key.RawBytesRemaining())
	}

	return fmt.Sprintf("/logIndex:%d", logIndex)
}

func localRangeIDKeyPrint(key encoding.Reader) string {
	var buf bytes.Buffer
	if encoding.PeekType(key) != encoding.Int {
		return fmt.Sprintf("/err<%q>", key.RawBytesRemaining())
	}

	// get range id
	i, err := encoding.DecodeVarint(key)
	if err != nil {
		return fmt.Sprintf("/err<%v:%q>", err, key.RawBytesRemaining())
	}

	fmt.Fprintf(&buf, "/%d", i)

	// get suffix

	hasSuffix := false
	for _, s := range rangeIDSuffixDict {
		if bytes.HasPrefix(key.RawBytesRemaining(), s.suffix) {
			fmt.Fprintf(&buf, "/%s", s.name)
			if _, err = key.Read(len(s.suffix)); err != nil {
				panic(err)
			}
			if s.ppFunc != nil && len(key.RawBytesRemaining()) != 0 {
				fmt.Fprintf(&buf, "%s", s.ppFunc(key))
				return buf.String()
			}
			hasSuffix = true
			break
		}
	}

	// get encode values
	if hasSuffix {
		fmt.Fprintf(&buf, "%s", decodeKeyPrint(key))
	} else {
		fmt.Fprintf(&buf, "%q", key.RawBytesRemaining())
	}

	return buf.String()
}

func localRangeKeyPrint(key encoding.Reader) string {
	var buf bytes.Buffer

	for _, s := range rangeSuffixDict {
		if s.atEnd {
			rawBytes := key.RawBytesRemaining()
			if bytes.HasSuffix(rawBytes, s.suffix) {
				key = encoding.NewBufferReader(rawBytes[:len(rawBytes)-len(s.suffix)])
				fmt.Fprintf(&buf, "/%s%s", s.name, decodeKeyPrint(key))
				return buf.String()
			}
		} else {
			begin := bytes.Index(key.RawBytesRemaining(), s.suffix)
			if begin > 0 {
				if addrKey, err := key.Read(begin); err != nil {
					panic(err)
				}
				if _, err = key.Read(len(s.suffix)); err != nil {
					panic(err)
				}
				id := key.RawBytesRemaining()
				fmt.Fprintf(&buf, "/%s/addrKey:%s/id:%q", s.name,
					decodeKeyPrint(encoding.NewBufferReader(addrKey)), []byte(id))
				return buf.String()
			}
		}
	}
	fmt.Fprintf(&buf, "%s", decodeKeyPrint(key))

	return buf.String()
}

func sequenceCacheKeyPrint(key encoding.Reader) string {
	id, err := encoding.DecodeBytes(key, nil)
	if err != nil {
		return fmt.Sprintf("/%q/err:%v", key.RawBytesRemaining(), err)
	}

	if key.EOF == 0 {
		return fmt.Sprintf("/%q", id)
	}

	epoch, err := encoding.DecodeUint32Decreasing(key)
	if err != nil {
		return fmt.Sprintf("/%q/err:%v", id, err)
	}

	seq, err := encoding.DecodeUint32Decreasing(key)
	if err != nil {
		return fmt.Sprintf("/%q/epoch:%d/err:%v", id, epoch, err)
	}

	return fmt.Sprintf("/%q/epoch:%d/seq:%d", id, epoch, seq)
}

func print(key encoding.Reader) string {
	return fmt.Sprintf("/%q", key.RawBytesRemaining())
}

func decodeKeyPrint(key encoding.Reader) string {
	var buf bytes.Buffer
	for k := 0; !key.EOF(); k++ {
		var err error
		switch encoding.PeekType(key) {
		case encoding.Null:
			if _, err = encoding.DecodeIfNull(key); err != nil {
				panic(err)
			}
			fmt.Fprintf(&buf, "/NULL")
		case encoding.NotNull:
			if _, err = encoding.DecodeIfNotNull(key); err != nil {
				panic(err)
			}
			fmt.Fprintf(&buf, "/#")
		case encoding.Int:
			var i int64
			key, i, err = encoding.DecodeVarint(key)
			if err == nil {
				fmt.Fprintf(&buf, "/%d", i)
			}
		case encoding.Float:
			var f float64
			key, f, err = encoding.DecodeFloat(key, nil)
			if err == nil {
				fmt.Fprintf(&buf, "/%f", f)
			}
		case encoding.Bytes:
			var s string
			key, s, err = encoding.DecodeString(key, nil)
			if err == nil {
				fmt.Fprintf(&buf, "/%q", s)
			}
		case encoding.Time:
			var t time.Time
			key, t, err = encoding.DecodeTime(key)
			if err == nil {
				fmt.Fprintf(&buf, "/%s", t.UTC().Format(time.UnixDate))
			}
		default:
			// This shouldn't ever happen, but if it does let the loop exit.
			fmt.Fprintf(&buf, "/%q", []byte(key))
			key = nil
		}

		if err != nil {
			fmt.Fprintf(&buf, "/<%v>", err)
			continue
		}
	}
	return buf.String()
}

// PrettyPrint print the key in a human readable format, which organized as below.
//
// Note: all components of `key` are assumed to be encoded ascendingly.
//
// Key's Format												Key's Value
// /Local/...												"\x01"+...
// 		/Store/...											"\x01s"+...
//		/RangeID/...										"\x01s"+[rangeid]
//			/[rangeid]/SequenceCache/[id]/seq:[seq]			"\x01s"+[rangeid]+"res-"+[id]+[seq]
//			/[rangeid]/RaftLeaderLease						"\x01s"+[rangeid]+"rfll"
//			/[rangeid]/RaftTombstone						"\x01s"+[rangeid]+"rftb"
//			/[rangeid]/RaftHardState						"\x01s"+[rangeid]+"rfth"
//			/[rangeid]/RaftAppliedIndex						"\x01s"+[rangeid]+"rfta"
//			/[rangeid]/RaftLog/logIndex:[logIndex]			"\x01s"+[rangeid]+"rftl"+[logIndex]
//			/[rangeid]/RaftTruncatedState					"\x01s"+[rangeid]+"rftt"
//			/[rangeid]/RaftLastIndex						"\x01s"+[rangeid]+"rfti"
//			/[rangeid]/RangeGCMetadata						"\x01s"+[rangeid]+"rgcm"
//			/[rangeid]/RangeLastVerificationTimestamp		"\x01s"+[rangeid]+"rlvt"
//			/[rangeid]/RangeStats							"\x01s"+[rangeid]+"stat"
//		/Range/...											"\x01k"+...
//			/RangeDescriptor/[key]							"\x01k"+[key]+"rdsc"
//			/RangeTreeNode/[key]							"\x01k"+[key]+"rtn-"
//			/Transaction/addrKey:[key]/id:[id]				"\x01k"+[key]+"txn-"+[id]
// /Local/Max 												"\x02"
//
// /Meta1/[key]										    "\x02"+[key]
// /Meta2/[key]										    "\x03"+[key]
// /System/...												"\x04"
//		/StatusStore/[key]									"\x04status-store-"+[key]
//		/StatusNode/[key]									"\x04status-node-"+[key]
// /System/Max												"\x05"
//
// /Table/[key]												"\xff"+[key]
//
// /Min														""
// /Max														"\xff\xff"
func PrettyPrint(key roachpb.Key) string {
	if bytes.Equal(key.RawBytesRemaining(), MaxKey) {
		return "/Max"
	} else if bytes.Equal(key, MinKey) {
		return "/Min"
	}

	var buf bytes.Buffer
	for _, k := range keyDict {
		if key.Compare(k.start) >= 0 && (k.end == nil || key.Compare(k.end) <= 0) {
			fmt.Fprintf(&buf, "%s", k.name)
			if k.end != nil && k.end.Compare(key) == 0 {
				fmt.Fprintf(&buf, "/Max")
				return buf.String()
			}

			hasPrefix := false
			for _, e := range k.entries {
				if bytes.HasPrefix(key, e.prefix) {
					hasPrefix = true
					key = key[len(e.prefix):]

					fmt.Fprintf(&buf, "%s%s", e.name, e.ppFunc(key))
					break
				}
			}
			if !hasPrefix {
				key = key[len(k.start):]
				fmt.Fprintf(&buf, "/%q", []byte(key))
			}

			return buf.String()
		}
	}

	return fmt.Sprintf("%q", []byte(key))
}

func init() {
	roachpb.PrettyPrintKey = PrettyPrint
}
