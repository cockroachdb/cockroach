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
	ppFunc func(key roachpb.Key) string
}

var (
	constKeyDict = []struct {
		name  string
		value roachpb.Key
	}{
		{"/Max", MaxKey},
		{"/Min", MinKey},
		{"/System/Meta1/Max", Meta1KeyMax},
		{"/System/Meta2/Max", Meta2KeyMax},
	}

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
		{name: "/System", start: SystemPrefix, end: SystemMax, entries: []dictEntry{

			{name: "/StatusStore", prefix: StatusStorePrefix, ppFunc: decodeKeyPrint},
			{name: "/StatusNode", prefix: StatusNodePrefix, ppFunc: decodeKeyPrint},
		}},
		{name: "/Table", start: TableDataPrefix, end: nil, entries: []dictEntry{
			{name: "", prefix: TableDataPrefix, ppFunc: decodeKeyPrint},
		}},
	}

	// keyofKeyDict means the key of suffix which is itself a key,
	// should recursively pretty print it, see issue #3228
	keyOfKeyDict = []struct {
		name   string
		prefix []byte
	}{
		{name: "/System/Meta2", prefix: Meta2Prefix},
		{name: "/System/Meta1", prefix: Meta1Prefix},
	}

	rangeIDSuffixDict = []struct {
		name   string
		suffix []byte
		ppFunc func(key roachpb.Key) string
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

func localStoreKeyPrint(key roachpb.Key) string {
	if bytes.HasPrefix(key, localStoreIdentSuffix) {
		return "/storeIdent"
	}

	return fmt.Sprintf("%q", []byte(key))
}

func raftLogKeyPrint(key roachpb.Key) string {
	var logIndex uint64
	var err error
	key, logIndex, err = encoding.DecodeUint64(key)
	if err != nil {
		return fmt.Sprintf("/err<%v:%q>", err, []byte(key))
	}

	return fmt.Sprintf("/logIndex:%d", logIndex)
}

func localRangeIDKeyPrint(key roachpb.Key) string {
	var buf bytes.Buffer
	if encoding.PeekType(key) != encoding.Int {
		return fmt.Sprintf("/err<%q>", []byte(key))
	}

	// get range id
	key, i, err := encoding.DecodeVarint(key)
	if err != nil {
		return fmt.Sprintf("/err<%v:%q>", err, []byte(key))
	}

	fmt.Fprintf(&buf, "/%d", i)

	// get suffix

	hasSuffix := false
	for _, s := range rangeIDSuffixDict {
		if bytes.HasPrefix(key, s.suffix) {
			fmt.Fprintf(&buf, "/%s", s.name)
			key = key[len(s.suffix):]
			if s.ppFunc != nil && len(key) != 0 {
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
		fmt.Fprintf(&buf, "%q", []byte(key))
	}

	return buf.String()
}

func localRangeKeyPrint(key roachpb.Key) string {
	var buf bytes.Buffer

	for _, s := range rangeSuffixDict {
		if s.atEnd {
			if bytes.HasSuffix(key, s.suffix) {
				key = key[:len(key)-len(s.suffix)]
				fmt.Fprintf(&buf, "/%s%s", s.name, decodeKeyPrint(key))
				return buf.String()
			}
		} else {
			begin := bytes.Index(key, s.suffix)
			if begin > 0 {
				addrKey := key[:begin]
				id := key[(begin + len(s.suffix)):]
				fmt.Fprintf(&buf, "/%s/addrKey:%s/id:%q", s.name, decodeKeyPrint(addrKey), []byte(id))
				return buf.String()
			}
		}
	}
	fmt.Fprintf(&buf, "%s", decodeKeyPrint(key))

	return buf.String()
}

func sequenceCacheKeyPrint(key roachpb.Key) string {
	b, id, err := encoding.DecodeBytes([]byte(key), nil)
	if err != nil {
		return fmt.Sprintf("/%q/err:%v", key, err)
	}

	if len(b) == 0 {
		return fmt.Sprintf("/%q", id)
	}

	b, epoch, err := encoding.DecodeUint32Decreasing(b)
	if err != nil {
		return fmt.Sprintf("/%q/err:%v", id, err)
	}

	_, seq, err := encoding.DecodeUint32Decreasing(b)
	if err != nil {
		return fmt.Sprintf("/%q/epoch:%d/err:%v", id, epoch, err)
	}

	return fmt.Sprintf("/%q/epoch:%d/seq:%d", id, epoch, seq)
}

func print(key roachpb.Key) string {
	return fmt.Sprintf("/%q", []byte(key))
}

// TableDataPrefix
func decodeKeyPrint(key roachpb.Key) string {
	var buf bytes.Buffer
	for k := 0; len(key) > 0; k++ {
		var err error
		switch encoding.PeekType(key) {
		case encoding.Null:
			key, _ = encoding.DecodeIfNull(key)
			fmt.Fprintf(&buf, "/NULL")
		case encoding.NotNull:
			key, _ = encoding.DecodeIfNotNull(key)
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

// prettyPrintInternal parse key with prefix in keyDict,
// if the key don't march any prefix in keyDict, return its byte value with quotation and false,
// or else return its human readable value and true.
func prettyPrintInternal(key roachpb.Key) (string, bool) {
	var buf bytes.Buffer
	for _, k := range keyDict {
		if key.Compare(k.start) >= 0 && (k.end == nil || key.Compare(k.end) <= 0) {
			fmt.Fprintf(&buf, "%s", k.name)
			if k.end != nil && k.end.Compare(key) == 0 {
				fmt.Fprintf(&buf, "/Max")
				return buf.String(), true
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

			return buf.String(), true
		}
	}

	return fmt.Sprintf("%q", []byte(key)), false
}

// PrettyPrint print the key in a human readable format, which's organized as:
// Key's Format										Key's Value
// /Local/...										"\x00\x00\x00"+...
// 		/Store/...									"\x00\x00\x00s"+...
//		/RangeID/...								"\x00\x00\x00s"+[rangeid]
//			/[rangeid]/SequenceCache/[id]/seq:[seq]				"\x00\x00\x00s"+[rangeid]+"res-"+[id]+[seq]
//			/[rangeid]/RaftLeaderLease					"\x00\x00\x00s"+[rangeid]+"rfll"
//			/[rangeid]/RaftTombstone						"\x00\x00\x00s"+[rangeid]+"rftb"
//			/[rangeid]/RaftHardState						"\x00\x00\x00s"+[rangeid]+"rfth"
//			/[rangeid]/RaftAppliedIndex					"\x00\x00\x00s"+[rangeid]+"rfta"
//			/[rangeid]/RaftLog/logIndex:[logIndex]				"\x00\x00\x00s"+[rangeid]+"rftl"+[logIndex]
//			/[rangeid]/RaftTruncatedState					"\x00\x00\x00s"+[rangeid]+"rftt"
//			/[rangeid]/RaftLastIndex						"\x00\x00\x00s"+[rangeid]+"rfti"
//			/[rangeid]/RangeGCMetadata					"\x00\x00\x00s"+[rangeid]+"rgcm"
//			/[rangeid]/RangeLastVerificationTimestamp				"\x00\x00\x00s"+[rangeid]+"rlvt"
//			/[rangeid]/RangeStats						"\x00\x00\x00s"+[rangeid]+"stat"
//		/Range/...								"\x00\x00\x00k"+...
//			/RangeDescriptor/[key]						"\x00\x00\x00k"+[key]+"rdsc"
//			/RangeTreeNode/[key]						"\x00\x00\x00k"+[key]+"rtn-"
//			/Transaction/addrKey:[key]/id:[id]					"\x00\x00\x00k"+[key]+"txn-"+[id]
// /Local/Max 										"\x00\x00\x01"
//
// /System/...										"\x00"
//		/StatusStore/[key]							"\x00status-store-"+[key]
//		/StatusNode/[key]							"\x00status-node-"+[key]
// /System/Max										"\x01"
//
// /Table/[key]										"\xff"+[key]
//
// "%q"											others
//
// /Min											""
// /Max											"\xff\xff"
// /System/Meta1/Max									"\x00\x00meta1\xff"
// /System/Meta2/Max									"\x00\x00meta2\xff"
//
// /System/Meta1/[recursively key]								"\x00\x00meta1"+[recursively key]
// /System/Meta2/[recursively key]								"\x00\x00meta2"+[recursively key]
// the recursively key referece issue #3228
func PrettyPrint(key roachpb.Key) string {
	for _, k := range constKeyDict {
		if bytes.Equal(key, k.value) {
			return k.name
		}
	}

	for _, k := range keyOfKeyDict {
		if bytes.HasPrefix(key, k.prefix) {
			key = key[len(k.prefix):]
			str, formatted := prettyPrintInternal(key)
			if formatted {
				return fmt.Sprintf("%s%s", k.name, str)
			}

			return fmt.Sprintf("%s/%s", k.name, str)
		}
	}
	str, _ := prettyPrintInternal(key)
	return str
}

func init() {
	roachpb.PrettyPrintKey = PrettyPrint
}
