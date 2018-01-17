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

package keys

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/pkg/errors"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
)

// PrettyPrintTimeseriesKey is a hook for pretty printing a timeseries key. The
// timeseries key prefix will already have been stripped off.
var PrettyPrintTimeseriesKey func(key roachpb.Key) string

type dictEntry struct {
	name   string
	prefix roachpb.Key
	// print the key's pretty value, key has been removed prefix data
	ppFunc func(valDirs []encoding.Direction, key roachpb.Key) string
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
					return "", RangeMetaKey(RangeMetaKey(MustAddr(
						roachpb.Key(unq)))).AsRawKey()
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
					return "", RangeMetaKey(MustAddr(roachpb.Key(unq))).AsRawKey()
				},
			}},
		},
		{name: "/System", start: SystemPrefix, end: SystemMax, entries: []dictEntry{
			{name: "/NodeLiveness", prefix: NodeLivenessPrefix,
				ppFunc: decodeKeyPrint,
				psFunc: parseUnsupported,
			},
			{name: "/NodeLivenessMax", prefix: NodeLivenessKeyMax,
				ppFunc: decodeKeyPrint,
				psFunc: parseUnsupported,
			},
			{name: "/StatusNode", prefix: StatusNodePrefix,
				ppFunc: decodeKeyPrint,
				psFunc: parseUnsupported,
			},
			{name: "/tsd", prefix: TimeseriesPrefix,
				ppFunc: decodeTimeseriesKey,
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
		{name: "AbortSpan", suffix: LocalAbortSpanSuffix, ppFunc: abortSpanKeyPrint, psFunc: abortSpanKeyParse},
		{name: "RaftTombstone", suffix: LocalRaftTombstoneSuffix},
		{name: "RaftHardState", suffix: LocalRaftHardStateSuffix},
		{name: "RaftAppliedIndex", suffix: LocalRaftAppliedIndexSuffix},
		{name: "LeaseAppliedIndex", suffix: LocalLeaseAppliedIndexSuffix},
		{name: "RaftLog", suffix: LocalRaftLogSuffix,
			ppFunc: raftLogKeyPrint,
			psFunc: raftLogKeyParse,
		},
		{name: "RaftTruncatedState", suffix: LocalRaftTruncatedStateSuffix},
		{name: "RaftLastIndex", suffix: LocalRaftLastIndexSuffix},
		{name: "RangeLastReplicaGCTimestamp", suffix: LocalRangeLastReplicaGCTimestampSuffix},
		{name: "RangeLastVerificationTimestamp", suffix: LocalRangeLastVerificationTimestampSuffixDeprecated},
		{name: "RangeLease", suffix: LocalRangeLeaseSuffix},
		{name: "RangeStats", suffix: LocalRangeStatsSuffix},
		{name: "RangeTxnSpanGCThreshold", suffix: LocalTxnSpanGCThresholdSuffix},
		{name: "RangeFrozenStatus", suffix: LocalRangeFrozenStatusSuffix},
		{name: "RangeLastGC", suffix: LocalRangeLastGCSuffix},
	}

	rangeSuffixDict = []struct {
		name   string
		suffix []byte
		atEnd  bool
	}{
		{name: "RangeDescriptor", suffix: LocalRangeDescriptorSuffix, atEnd: true},
		{name: "Transaction", suffix: LocalTransactionSuffix, atEnd: false},
		{name: "QueueLastProcessed", suffix: LocalQueueLastProcessedSuffix, atEnd: false},
	}
)

var constSubKeyDict = []struct {
	name string
	key  roachpb.RKey
}{
	{"/storeIdent", localStoreIdentSuffix},
	{"/gossipBootstrap", localStoreGossipSuffix},
	{"/clusterVersion", localStoreClusterVersionSuffix},
	{"/suggestedCompaction", localStoreSuggestedCompactionSuffix},
}

func suggestedCompactionKeyPrint(key roachpb.Key) string {
	start, end, err := DecodeStoreSuggestedCompactionKey(key)
	if err != nil {
		return fmt.Sprintf("<invalid: %s>", err)
	}
	return fmt.Sprintf("{%s-%s}", start, end)
}

func localStoreKeyPrint(_ []encoding.Direction, key roachpb.Key) string {
	for _, v := range constSubKeyDict {
		if bytes.HasPrefix(key, v.key) {
			if v.key.Equal(localStoreSuggestedCompactionSuffix) {
				return v.name + "/" + suggestedCompactionKeyPrint(
					append(roachpb.Key(nil), append(localStorePrefix, key...)...),
				)
			}
			return v.name
		}
	}

	return fmt.Sprintf("%q", []byte(key))
}

func localStoreKeyParse(input string) (remainder string, output roachpb.Key) {
	for _, s := range constSubKeyDict {
		if strings.HasPrefix(input, s.name) {
			if s.key.Equal(localStoreSuggestedCompactionSuffix) {
				panic(&errUglifyUnsupported{errors.New("cannot parse suggested compaction key")})
			}
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
		panic(errors.Errorf("illegal RangeID: %q", input))
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
		panic(errors.Errorf("invalid infix: %q", infix))
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
	maker := makeRangeIDUnreplicatedKey
	if replicated {
		maker = makeRangeIDReplicatedKey
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
		key = maker(roachpb.RangeID(rangeID), suffix, detail)
		return
	}
	panic(&errUglifyUnsupported{errors.New("unhandled general range key")})
}

func localRangeIDKeyPrint(valDirs []encoding.Direction, key roachpb.Key) string {
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
		fmt.Fprintf(&buf, "%s", decodeKeyPrint(valDirs, key))
	} else {
		fmt.Fprintf(&buf, "%q", []byte(key))
	}

	return buf.String()
}

func localRangeKeyPrint(valDirs []encoding.Direction, key roachpb.Key) string {
	var buf bytes.Buffer

	for _, s := range rangeSuffixDict {
		if s.atEnd {
			if bytes.HasSuffix(key, s.suffix) {
				key = key[:len(key)-len(s.suffix)]
				_, decodedKey, err := encoding.DecodeBytesAscending([]byte(key), nil)
				if err != nil {
					fmt.Fprintf(&buf, "%s/%s", decodeKeyPrint(valDirs, key), s.name)
				} else {
					fmt.Fprintf(&buf, "%s/%s", roachpb.Key(decodedKey), s.name)
				}
				return buf.String()
			}
		} else {
			begin := bytes.Index(key, s.suffix)
			if begin > 0 {
				addrKey := key[:begin]
				_, decodedAddrKey, err := encoding.DecodeBytesAscending([]byte(addrKey), nil)
				if err != nil {
					fmt.Fprintf(&buf, "%s/%s", decodeKeyPrint(valDirs, addrKey), s.name)
				} else {
					fmt.Fprintf(&buf, "%s/%s", roachpb.Key(decodedAddrKey), s.name)
				}
				if bytes.Equal(s.suffix, LocalTransactionSuffix) {
					txnID, err := uuid.FromBytes(key[(begin + len(s.suffix)):])
					if err != nil {
						return fmt.Sprintf("/%q/err:%v", key, err)
					}
					fmt.Fprintf(&buf, "/%q", txnID)
				} else {
					id := key[(begin + len(s.suffix)):]
					fmt.Fprintf(&buf, "/%q", []byte(id))
				}
				return buf.String()
			}
		}
	}

	_, decodedKey, err := encoding.DecodeBytesAscending([]byte(key), nil)
	if err != nil {
		fmt.Fprintf(&buf, "%s", decodeKeyPrint(valDirs, key))
	} else {
		fmt.Fprintf(&buf, "%s", roachpb.Key(decodedKey))
	}

	return buf.String()
}

type errUglifyUnsupported struct {
	wrapped error
}

func (euu *errUglifyUnsupported) Error() string {
	return fmt.Sprintf("unsupported pretty key: %v", euu.wrapped)
}

func abortSpanKeyParse(rangeID roachpb.RangeID, input string) (string, roachpb.Key) {
	var err error
	input = mustShiftSlash(input)
	_, input = mustShift(input[:len(input)-1])
	if len(input) != len(uuid.UUID{}.String()) {
		panic(&errUglifyUnsupported{errors.New("txn id not available")})
	}
	id, err := uuid.FromString(input)
	if err != nil {
		panic(&errUglifyUnsupported{err})
	}
	return "", AbortSpanKey(rangeID, id)
}

func abortSpanKeyPrint(key roachpb.Key) string {
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

func print(_ []encoding.Direction, key roachpb.Key) string {
	return fmt.Sprintf("/%q", []byte(key))
}

func decodeKeyPrint(valDirs []encoding.Direction, key roachpb.Key) string {
	if key.Equal(SystemConfigSpan.Key) {
		return "/SystemConfigSpan/Start"
	}
	return encoding.PrettyPrintValue(valDirs, key, "/")
}

func decodeTimeseriesKey(_ []encoding.Direction, key roachpb.Key) string {
	return PrettyPrintTimeseriesKey(key)
}

// prettyPrintInternal parse key with prefix in keyDict.
// For table keys, valDirs correspond to the encoding direction of each encoded
// value in key.
// If valDirs is unspecified, the default encoding direction for each value
// type is used (see encoding.go:prettyPrintFirstValue).
// If the key doesn't match any prefix in keyDict, return its byte value with
// quotation and false, or else return its human readable value and true.
func prettyPrintInternal(valDirs []encoding.Direction, key roachpb.Key, quoteRawKeys bool) string {
	for _, k := range constKeyDict {
		if key.Equal(k.value) {
			return k.name
		}
	}

	helper := func(key roachpb.Key) (string, bool) {
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
						fmt.Fprintf(&buf, "%s%s", e.name, e.ppFunc(valDirs, key))
						break
					}
				}
				if !hasPrefix {
					key = key[len(k.start):]
					if quoteRawKeys {
						fmt.Fprintf(&buf, "/%q", []byte(key))
					} else {
						fmt.Fprintf(&buf, "/%s", []byte(key))
					}
				}

				return buf.String(), true
			}
		}

		if quoteRawKeys {
			return fmt.Sprintf("%q", []byte(key)), false
		}
		return fmt.Sprintf("%s", []byte(key)), false
	}

	for _, k := range keyOfKeyDict {
		if bytes.HasPrefix(key, k.prefix) {
			key = key[len(k.prefix):]
			str, formatted := helper(key)
			if formatted {
				return k.name + str
			}
			return k.name + "/" + str
		}
	}
	str, _ := helper(key)
	return str
}

// PrettyPrint prints the key in a human readable format:
//
// Key's Format                                   Key's Value
// /Local/...                                        "\x01"+...
// 		/Store/...                                     "\x01s"+...
//		/RangeID/...                                   "\x01s"+[rangeid]
//			/[rangeid]/AbortSpan/[id]                   "\x01s"+[rangeid]+"abc-"+[id]
//			/[rangeid]/Lease						                 "\x01s"+[rangeid]+"rfll"
//			/[rangeid]/RaftTombstone                     "\x01s"+[rangeid]+"rftb"
//			/[rangeid]/RaftHardState						         "\x01s"+[rangeid]+"rfth"
//			/[rangeid]/RaftAppliedIndex						       "\x01s"+[rangeid]+"rfta"
//			/[rangeid]/RaftLog/logIndex:[logIndex]       "\x01s"+[rangeid]+"rftl"+[logIndex]
//			/[rangeid]/RaftTruncatedState                "\x01s"+[rangeid]+"rftt"
//			/[rangeid]/RaftLastIndex                     "\x01s"+[rangeid]+"rfti"
//			/[rangeid]/RangeLastReplicaGCTimestamp       "\x01s"+[rangeid]+"rlrt"
//			/[rangeid]/RangeLastVerificationTimestamp    "\x01s"+[rangeid]+"rlvt"
//			/[rangeid]/RangeStats                        "\x01s"+[rangeid]+"stat"
//		/Range/...                                     "\x01k"+...
//			[key]/RangeDescriptor                        "\x01k"+[key]+"rdsc"
//			[key]/Transaction/[id]	                     "\x01k"+[key]+"txn-"+[txn-id]
//			[key]/QueueLastProcessed/[queue]             "\x01k"+[key]+"qlpt"+[queue]
// /Local/Max                                        "\x02"
//
// /Meta1/[key]                                      "\x02"+[key]
// /Meta2/[key]                                      "\x03"+[key]
// /System/...                                       "\x04"
//		/NodeLiveness/[key]                            "\x04\0x00liveness-"+[key]
//		/StatusNode/[key]                              "\x04status-node-"+[key]
// /System/Max                                       "\x05"
//
// /Table/[key]                                      [key]
//
// /Min                                              ""
// /Max                                              "\xff\xff"
//
// valDirs correspond to the encoding direction of each encoded value in key.
// For example, table keys could have column values encoded in ascending or
// descending directions.
// If valDirs is unspecified, the default encoding direction for each value
// type is used (see encoding.go:prettyPrintFirstValue).
func PrettyPrint(valDirs []encoding.Direction, key roachpb.Key) string {
	return prettyPrintInternal(valDirs, key, true /* quoteRawKeys */)
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
			rErr = errors.Errorf("%v", r)
		}
	}()

	origInput := input
	var output roachpb.Key

	mkErr := func(err error) (roachpb.Key, error) {
		if err == nil {
			err = errIllegalInput
		}
		return nil, errors.Errorf(`can't parse "%s" after reading %s: %s`, input, origInput[:len(origInput)-len(input)], err)
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
	if out := PrettyPrint(nil /* valDirs */, output); out != origInput {
		return nil, errors.Errorf("constructed key deviates from original: %s vs %s", out, origInput)
	}
	return output, nil
}

func init() {
	roachpb.PrettyPrintKey = PrettyPrint
	roachpb.PrettyPrintRange = PrettyPrintRange
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
			}
		}
	}
	return r
}

// PrettyPrintRange pretty prints a compact representation of a key range. The
// output is of the form:
//    commonPrefix{remainingStart-remainingEnd}
// If the end key is empty, the outut is of the form:
//    start
// It prints at most maxChars, truncating components as needed. See
// TestPrettyPrintRange for some examples.
func PrettyPrintRange(start, end roachpb.Key, maxChars int) string {
	var b bytes.Buffer
	if maxChars < 8 {
		maxChars = 8
	}
	prettyStart := prettyPrintInternal(nil /* valDirs */, start, false /* quoteRawKeys */)
	if len(end) == 0 {
		if len(prettyStart) <= maxChars {
			return prettyStart
		}
		b.WriteString(prettyStart[:maxChars-1])
		b.WriteRune('…')
		return b.String()
	}
	prettyEnd := prettyPrintInternal(nil /* valDirs */, end, false /* quoteRawKeys */)
	i := 0
	// Find the common prefix.
	for ; i < len(prettyStart) && i < len(prettyEnd) && prettyStart[i] == prettyEnd[i]; i++ {
	}
	// If we don't have space for at least '{a…-b…}' after the prefix, only print
	// the prefix (or part of it).
	if i > maxChars-7 {
		if i > maxChars-1 {
			i = maxChars - 1
		}
		b.WriteString(prettyStart[:i])
		b.WriteRune('…')
		return b.String()
	}
	b.WriteString(prettyStart[:i])
	remaining := (maxChars - i - 3) / 2

	printTrunc := func(b *bytes.Buffer, what string, maxChars int) {
		if len(what) <= maxChars {
			b.WriteString(what)
		} else {
			b.WriteString(what[:maxChars-1])
			b.WriteRune('…')
		}
	}

	b.WriteByte('{')
	printTrunc(&b, prettyStart[i:], remaining)
	b.WriteByte('-')
	printTrunc(&b, prettyEnd[i:], remaining)
	b.WriteByte('}')

	return b.String()
}
