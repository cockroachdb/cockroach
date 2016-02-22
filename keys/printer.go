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
// permissions and limitations under the License.
//
// Author: Veteran Lu (23907238@qq.com)

package keys

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/uuid"
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
		{"/Meta1/Max", Meta1KeyMax},
		{"/Meta2/Max", Meta2KeyMax},
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

	// keyofKeyDict means the key of suffix which is itself a key,
	// should recursively pretty print it, see issue #3228
	keyOfKeyDict = []struct {
		name   string
		prefix []byte
	}{
		{name: "/Meta2", prefix: Meta2Prefix},
		{name: "/Meta1", prefix: Meta1Prefix},
	}

	rangeIDSuffixDict = []struct {
		name   string
		suffix []byte
		ppFunc func(key roachpb.Key) string
	}{
		{name: "SequenceCache", suffix: LocalSequenceCacheSuffix, ppFunc: sequenceCacheKeyPrint},
		{name: "RaftTombstone", suffix: localRaftTombstoneSuffix},
		{name: "RaftHardState", suffix: localRaftHardStateSuffix},
		{name: "RaftAppliedIndex", suffix: localRaftAppliedIndexSuffix},
		{name: "RaftLog", suffix: localRaftLogSuffix, ppFunc: raftLogKeyPrint},
		{name: "RaftTruncatedState", suffix: localRaftTruncatedStateSuffix},
		{name: "RaftLastIndex", suffix: localRaftLastIndexSuffix},
		{name: "RangeLastVerificationTimestamp", suffix: localRangeLastVerificationTimestampSuffix},
		{name: "RangeLeaderLease", suffix: localRangeLeaderLeaseSuffix},
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
	} else if bytes.HasPrefix(key, localStoreGossipSuffix) {
		return "/gossipBootstrap"
	}

	return fmt.Sprintf("%q", []byte(key))
}

func raftLogKeyPrint(key roachpb.Key) string {
	var logIndex uint64
	var err error
	key, logIndex, err = encoding.DecodeUint64Ascending(key)
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
	key, i, err := encoding.DecodeVarintAscending(key)
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
				fmt.Fprintf(&buf, "%s/%s", decodeKeyPrint(key), s.name)
				return buf.String()
			}
		} else {
			begin := bytes.Index(key, s.suffix)
			if begin > 0 {
				addrKey := key[:begin]
				txnID, err := uuid.FromBytes(key[(begin + len(s.suffix)):])
				if err != nil {
					return fmt.Sprintf("/%q/err:%v", key, err)
				}
				fmt.Fprintf(&buf, "%s/%s/addrKey:/id:%q", decodeKeyPrint(addrKey), s.name, txnID)
				return buf.String()
			}
		}
	}
	fmt.Fprintf(&buf, "%s", decodeKeyPrint(key))

	return buf.String()
}

func sequenceCacheKeyPrint(key roachpb.Key) string {
	b, id, err := encoding.DecodeBytesAscending([]byte(key), nil)
	if err != nil {
		return fmt.Sprintf("/%q/err:%v", key, err)
	}

	txnID, err := uuid.FromBytes(id)
	if err != nil {
		return fmt.Sprintf("/%q/err:%v", key, err)
	}

	if len(b) == 0 {
		return fmt.Sprintf("/%q", txnID)
	}

	b, epoch, err := encoding.DecodeUint32Descending(b)
	if err != nil {
		return fmt.Sprintf("/%q/err:%v", txnID, err)
	}

	_, seq, err := encoding.DecodeUint32Descending(b)
	if err != nil {
		return fmt.Sprintf("/%q/epoch:%d/err:%v", txnID, epoch, err)
	}

	return fmt.Sprintf("/%q/epoch:%d/seq:%d", txnID, epoch, seq)
}

func print(key roachpb.Key) string {
	return fmt.Sprintf("/%q", []byte(key))
}

func decodeKeyPrint(key roachpb.Key) string {
	return encoding.PrettyPrintValue(key, "/")
}

// prettyPrintInternal parse key with prefix in keyDict,
// if the key don't march any prefix in keyDict, return its byte value with quotation and false,
// or else return its human readable value and true.
func prettyPrintInternal(key roachpb.Key) (string, bool) {
	var buf bytes.Buffer
	for _, k := range keyDict {
		if key.Compare(k.start) >= 0 && (k.end == nil || key.Compare(k.end) <= 0) {
			buf.WriteString(k.name)
			if k.end != nil && k.end.Compare(key) == 0 {
				buf.WriteString("/Max")
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

// PrettyPrint prints the key in a human readable format:
//
// Key's Format                                   Key's Value
// /Local/...                                     "\x01"+...
// 		/Store/...                                  "\x01s"+...
//		/RangeID/...                                "\x01s"+[rangeid]
//			/[rangeid]/SequenceCache/[id]/seq:[seq]   "\x01s"+[rangeid]+"res-"+[id]+[seq]
//			/[rangeid]/RaftLeaderLease                "\x01s"+[rangeid]+"rfll"
//			/[rangeid]/RaftTombstone                  "\x01s"+[rangeid]+"rftb"
//			/[rangeid]/RaftHardState						      "\x01s"+[rangeid]+"rfth"
//			/[rangeid]/RaftAppliedIndex						    "\x01s"+[rangeid]+"rfta"
//			/[rangeid]/RaftLog/logIndex:[logIndex]    "\x01s"+[rangeid]+"rftl"+[logIndex]
//			/[rangeid]/RaftTruncatedState             "\x01s"+[rangeid]+"rftt"
//			/[rangeid]/RaftLastIndex                  "\x01s"+[rangeid]+"rfti"
//			/[rangeid]/RangeLastVerificationTimestamp "\x01s"+[rangeid]+"rlvt"
//			/[rangeid]/RangeStats                     "\x01s"+[rangeid]+"stat"
//		/Range/...                                  "\x01k"+...
//			/RangeDescriptor/[key]                    "\x01k"+[key]+"rdsc"
//			/RangeTreeNode/[key]                      "\x01k"+[key]+"rtn-"
//			/Transaction/addrKey:[key]/id:[id]				"\x01k"+[key]+"txn-"+[id]
// /Local/Max                                     "\x02"
//
// /Meta1/[key]                                   "\x02"+[key]
// /Meta2/[key]                                   "\x03"+[key]
// /System/...                                    "\x04"
//		/StatusStore/[key]                          "\x04status-store-"+[key]
//		/StatusNode/[key]                           "\x04status-node-"+[key]
// /System/Max                                    "\x05"
//
// /Table/[key]                                   [key]
//
// /Min                                           ""
// /Max                                           "\xff\xff"
func PrettyPrint(key roachpb.Key) string {
	for _, k := range constKeyDict {
		if key.Equal(k.value) {
			return k.name
		}
	}

	for _, k := range keyOfKeyDict {
		if bytes.HasPrefix(key, k.prefix) {
			key = key[len(k.prefix):]
			str, formatted := prettyPrintInternal(key)
			if formatted {
				return k.name + str
			}
			return k.name + "/" + str
		}
	}
	str, _ := prettyPrintInternal(key)
	return str
}

func init() {
	roachpb.PrettyPrintKey = PrettyPrint
}

// MassagePrettyPrintedSpanForTest does some transformations on pretty-printed spans and keys:
// - if dirs is not nil, replace all ints with their ones' complement for
// descendingly-encoded columns.
// - strips line numbers from error messages.
func MassagePrettyPrintedSpanForTest(span string, dirs []encoding.Direction) string {
	var r string
	colIdx := -1
	for i := 0; i < len(span); i++ {
		d := -789
		fmt.Sscanf(span[i:], "%d", &d)
		if (dirs != nil) && (d != -789) {
			// We've managed to consume an int.
			dir := dirs[colIdx]
			i += len(strconv.Itoa(d)) - 1
			x := d
			if dir == encoding.Descending {
				x = ^x
			}
			r += strconv.Itoa(x)
		} else {
			r += string(span[i])
			switch span[i] {
			case '/':
				colIdx++
			case '-', ' ':
				// We're switching from the start constraints to the end constraints,
				// or starting another span.
				colIdx = -1
			case '<':
				// This is an error message, like <util/encoding/encoding.go:256: ....>.
				end := strings.Index(span[i:], ">")
				if end == -1 {
					panic("parse error")
				}
				errMsg := span[i+1 : i+end+1]
				lineIdx := strings.Index(errMsg, ":")
				if lineIdx != -1 {
					var lineEnd int
					for lineEnd = lineIdx + 1; errMsg[lineEnd] >= '0' && errMsg[lineEnd] <= '9'; lineEnd++ {
					}
					errMsg = errMsg[:lineIdx] + errMsg[lineIdx+(lineEnd-lineIdx):]
				}
				r += errMsg
				i += end
			}
		}
	}
	return r
}
