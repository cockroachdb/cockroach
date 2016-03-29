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
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/encoding"
	"github.com/cockroachdb/cockroach/util/uuid"
)

type dictEntry struct {
	name   string
	prefix roachpb.Key
	// print the key's pretty value, key has been removed prefix data
	ppFunc func(key roachpb.Key) string
	// Parses the relevant prefix of the input into a roachpb.Key, returning
	// the remainder and the key corresponding to the consumed prefix of
	// 'input'. Allowed to panic on errors.
	psFunc func(input string) (string, roachpb.Key)
}

func parseUnsupported(_ string) (string, roachpb.Key) {
	panic(&errUglifyUnsupported{})
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
			{name: "/Store", prefix: roachpb.Key(localStorePrefix),
				ppFunc: localStoreKeyPrint, psFunc: localStoreKeyParse},
			{name: "/RangeID", prefix: roachpb.Key(LocalRangeIDPrefix),
				ppFunc: localRangeIDKeyPrint, psFunc: localRangeIDKeyParse},
			{name: "/Range", prefix: LocalRangePrefix, ppFunc: localRangeKeyPrint,
				psFunc: parseUnsupported},
		}},
		{name: "/Meta1", start: Meta1Prefix, end: Meta1KeyMax, entries: []dictEntry{
			{name: "", prefix: Meta1Prefix, ppFunc: print,
				psFunc: func(input string) (string, roachpb.Key) {
					input = mustShiftSlash(input)
					unq, err := strconv.Unquote(input)
					if err != nil {
						panic(err)
					}
					if len(unq) == 0 {
						return "", Meta1Prefix
					}
					return "", RangeMetaKey(mustAddr(RangeMetaKey(mustAddr(
						roachpb.Key(unq)))))
				},
			}},
		},
		{name: "/Meta2", start: Meta2Prefix, end: Meta2KeyMax, entries: []dictEntry{
			{name: "", prefix: Meta2Prefix, ppFunc: print,
				psFunc: func(input string) (string, roachpb.Key) {
					input = mustShiftSlash(input)
					unq, err := strconv.Unquote(input)
					if err != nil {
						panic(&errUglifyUnsupported{err})
					}
					if len(unq) == 0 {
						return "", Meta2Prefix
					}
					return "", RangeMetaKey(mustAddr(roachpb.Key(unq)))
				},
			}},
		},
		{name: "/System", start: SystemPrefix, end: SystemMax, entries: []dictEntry{
			{name: "/StatusNode", prefix: StatusNodePrefix,
				ppFunc: decodeKeyPrint,
				psFunc: parseUnsupported,
			},
		}},
		{name: "/Table", start: TableDataMin, end: TableDataMax, entries: []dictEntry{
			{name: "", prefix: nil, ppFunc: decodeKeyPrint,
				psFunc: parseUnsupported},
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
		psFunc func(rangeID roachpb.RangeID, input string) (string, roachpb.Key)
	}{
		{name: "AbortCache", suffix: LocalAbortCacheSuffix, ppFunc: abortCacheKeyPrint, psFunc: abortCacheKeyParse},
		{name: "RaftTombstone", suffix: localRaftTombstoneSuffix},
		{name: "RaftHardState", suffix: localRaftHardStateSuffix},
		{name: "RaftAppliedIndex", suffix: localRaftAppliedIndexSuffix},
		{name: "RaftLog", suffix: localRaftLogSuffix,
			ppFunc: raftLogKeyPrint,
			psFunc: raftLogKeyParse,
		},
		{name: "RaftTruncatedState", suffix: localRaftTruncatedStateSuffix},
		{name: "RaftLastIndex", suffix: localRaftLastIndexSuffix},
		{name: "RangeLastReplicaGCTimestamp", suffix: localRangeLastReplicaGCTimestampSuffix},
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

var constSubKeyDict = []struct {
	name string
	key  roachpb.RKey
}{
	{"/storeIdent", localStoreIdentSuffix},
	{"/gossipBootstrap", localStoreGossipSuffix},
}

func localStoreKeyPrint(key roachpb.Key) string {
	for _, v := range constSubKeyDict {
		if bytes.HasPrefix(key, v.key) {
			return v.name
		}
	}

	return fmt.Sprintf("%q", []byte(key))
}

func localStoreKeyParse(input string) (remainder string, output roachpb.Key) {
	for _, s := range constSubKeyDict {
		if strings.HasPrefix(input, s.name) {
			remainder = input[len(s.name):]
			output = MakeStoreKey(s.key, nil)
			return
		}
	}
	slashPos := strings.Index(input[1:], "/")
	if slashPos < 0 {
		slashPos = len(input)
	}
	remainder = input[:slashPos] // `/something/else` -> `/else`
	output = roachpb.Key(input[1:slashPos])
	return
}

const strLogIndex = "/logIndex:"

func raftLogKeyParse(rangeID roachpb.RangeID, input string) (string, roachpb.Key) {
	if !strings.HasPrefix(input, strLogIndex) {
		panic("expected log index")
	}
	input = input[len(strLogIndex):]
	index, err := strconv.ParseUint(input, 10, 64)
	if err != nil {
		panic(err)
	}
	return "", RaftLogKey(rangeID, index)
}

func raftLogKeyPrint(key roachpb.Key) string {
	var logIndex uint64
	var err error
	key, logIndex, err = encoding.DecodeUint64Ascending(key)
	if err != nil {
		return fmt.Sprintf("/err<%v:%q>", err, []byte(key))
	}

	return fmt.Sprintf("%s%d", strLogIndex, logIndex)
}

func mustShiftSlash(in string) string {
	slash, out := mustShift(in)
	if slash != "/" {
		panic("expected /: " + in)
	}
	return out
}

func mustShift(in string) (first, remainder string) {
	if len(in) == 0 {
		panic("premature end of string")
	}
	return in[:1], in[1:]
}

func localRangeIDKeyParse(input string) (remainder string, key roachpb.Key) {
	var rangeID int64
	var err error
	input = mustShiftSlash(input)
	if endPos := strings.Index(input, "/"); endPos > 0 {
		rangeID, err = strconv.ParseInt(input[:endPos], 10, 64)
		if err != nil {
			panic(err)
		}
		input = input[endPos:]
	} else {
		panic(errors.New("illegal RangeID"))
	}
	input = mustShiftSlash(input)
	var infix string
	infix, input = mustShift(input)
	var replicated bool
	switch {
	case bytes.Equal(localRangeIDUnreplicatedInfix, []byte(infix)):
	case bytes.Equal(localRangeIDReplicatedInfix, []byte(infix)):
		replicated = true
	default:
		panic(fmt.Errorf("invalid infix"))
	}

	input = mustShiftSlash(input)
	// Get the suffix.
	var suffix roachpb.RKey
	for _, s := range rangeIDSuffixDict {
		if strings.HasPrefix(input, s.name) {
			input = input[len(s.name):]
			if s.psFunc != nil {
				remainder, key = s.psFunc(roachpb.RangeID(rangeID), input)
				return
			}
			suffix = roachpb.RKey(s.suffix)
			break
		}
	}
	maker := MakeRangeIDUnreplicatedKey
	if replicated {
		maker = MakeRangeIDReplicatedKey
	}
	if suffix != nil {
		if input != "" {
			panic(&errUglifyUnsupported{errors.New("nontrivial detail")})
		}
		var detail roachpb.RKey
		// TODO(tschottdorf): can't do this, init cycle:
		// detail, err := UglyPrint(input)
		// if err != nil {
		// 	return "", nil, err
		// }
		remainder = ""
		key = maker(roachpb.RangeID(rangeID), suffix, roachpb.RKey(detail))
		return
	}
	panic(&errUglifyUnsupported{errors.New("unhandled general range key")})
}

func localRangeIDKeyPrint(key roachpb.Key) string {
	var buf bytes.Buffer
	if encoding.PeekType(key) != encoding.Int {
		return fmt.Sprintf("/err<%q>", []byte(key))
	}

	// Get the rangeID.
	key, i, err := encoding.DecodeVarintAscending(key)
	if err != nil {
		return fmt.Sprintf("/err<%v:%q>", err, []byte(key))
	}

	fmt.Fprintf(&buf, "/%d", i)

	// Print and remove the rangeID infix specifier.
	if len(key) != 0 {
		fmt.Fprintf(&buf, "/%s", string(key[0]))
		key = key[1:]
	}

	// Get the suffix.
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

	// Get the encode values.
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

type errUglifyUnsupported struct {
	wrapped error
}

func (euu *errUglifyUnsupported) Error() string {
	return fmt.Sprintf("unsupported pretty key: %s", euu.wrapped)
}

func abortCacheKeyParse(rangeID roachpb.RangeID, input string) (string, roachpb.Key) {
	var err error
	input = mustShiftSlash(input)
	_, input = mustShift(input[:len(input)-1])
	if len(input) != len(uuid.EmptyUUID.String()) {
		panic(&errUglifyUnsupported{errors.New("txn id not available")})
	}
	id, err := uuid.FromString(input)
	if err != nil {
		panic(&errUglifyUnsupported{err})
	}
	return "", AbortCacheKey(rangeID, id)
}

func abortCacheKeyPrint(key roachpb.Key) string {
	_, id, err := encoding.DecodeBytesAscending([]byte(key), nil)
	if err != nil {
		return fmt.Sprintf("/%q/err:%v", key, err)
	}

	txnID, err := uuid.FromBytes(id)
	if err != nil {
		return fmt.Sprintf("/%q/err:%v", key, err)
	}

	return fmt.Sprintf("/%q", txnID)
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
//			/[rangeid]/AbortCache/[id]                "\x01s"+[rangeid]+"abc-"+[id]
//			/[rangeid]/RaftLeaderLease                "\x01s"+[rangeid]+"rfll"
//			/[rangeid]/RaftTombstone                  "\x01s"+[rangeid]+"rftb"
//			/[rangeid]/RaftHardState						      "\x01s"+[rangeid]+"rfth"
//			/[rangeid]/RaftAppliedIndex						    "\x01s"+[rangeid]+"rfta"
//			/[rangeid]/RaftLog/logIndex:[logIndex]    "\x01s"+[rangeid]+"rftl"+[logIndex]
//			/[rangeid]/RaftTruncatedState             "\x01s"+[rangeid]+"rftt"
//			/[rangeid]/RaftLastIndex                  "\x01s"+[rangeid]+"rfti"
//			/[rangeid]/RangeLastReplicaGCTimestamp    "\x01s"+[rangeid]+"rlrt"
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

var errIllegalInput = errors.New("illegal input")

// UglyPrint is a partial right inverse to PrettyPrint: it takes a key
// formatted for human consumption and attempts to translate it into a
// roachpb.Key. Not all key types are supported and no optimization has been
// performed. This is intended for use in debugging only.
func UglyPrint(input string) (_ roachpb.Key, rErr error) {
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				rErr = err
				return
			}
			rErr = fmt.Errorf("%v", r)
		}
	}()

	origInput := input
	var output roachpb.Key

	mkErr := func(err error) (roachpb.Key, error) {
		if err == nil {
			err = errIllegalInput
		}
		return nil, util.ErrorfSkipFrames(1, `can't parse "%s" after reading %s: %s`, input, origInput[:len(origInput)-len(input)], err)
	}

	var entries []dictEntry // nil if not pinned to a subrange
outer:
	for len(input) > 0 {
		if entries != nil {
			for _, v := range entries {
				if strings.HasPrefix(input, v.name) {
					input = input[len(v.name):]
					if v.psFunc == nil {
						return mkErr(nil)
					}
					remainder, key := v.psFunc(input)
					input = remainder
					output = append(output, key...)
					entries = nil
					continue outer
				}
			}
			return nil, &errUglifyUnsupported{errors.New("known key, but unsupported subtype")}
		}
		for _, v := range constKeyDict {
			if strings.HasPrefix(input, v.name) {
				output = append(output, v.value...)
				input = input[len(v.name):]
				continue outer
			}
		}
		for _, v := range keyDict {
			if strings.HasPrefix(input, v.name) {
				// No appending to output yet, the dictionary will take care of
				// it.
				input = input[len(v.name):]
				entries = v.entries
				continue outer
			}
		}
		return mkErr(errors.New("can't handle key"))
	}
	if out := PrettyPrint(output); out != origInput {
		return nil, fmt.Errorf("constructed key deviates from original: %s vs %s", out, origInput)
	}
	return output, nil
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
