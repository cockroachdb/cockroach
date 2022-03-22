// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package keys

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
)

// PrettyPrintTimeseriesKey is a hook for pretty printing a timeseries key. The
// timeseries key prefix will already have been stripped off.
var PrettyPrintTimeseriesKey func(key roachpb.Key) string

// DictEntry contains info on pretty-printing and pretty-scanning keys in a
// region of the key space.
type DictEntry struct {
	Name   string
	prefix roachpb.Key
	// print the key's pretty value, key has been removed prefix data
	ppFunc func(valDirs []encoding.Direction, key roachpb.Key) string
	// PSFunc parses the relevant prefix of the input into a roachpb.Key,
	// returning the remainder and the key corresponding to the consumed prefix of
	// 'input'. Allowed to panic on errors.
	PSFunc KeyParserFunc
}

// KeyParserFunc is a function able to reverse pretty-printed keys.
type KeyParserFunc func(input string) (string, roachpb.Key)

func parseUnsupported(_ string) (string, roachpb.Key) {
	panic(&ErrUglifyUnsupported{})
}

// KeyComprehensionTable contains information about how to decode pretty-printed
// keys, split by key spans.
type KeyComprehensionTable []struct {
	Name    string
	start   roachpb.Key
	end     roachpb.Key
	Entries []DictEntry
}

var (
	// ConstKeyDict translates some pretty-printed keys.
	ConstKeyDict = []struct {
		Name  string
		Value roachpb.Key
	}{
		{"/Max", MaxKey},
		{"/Min", MinKey},
		{"/Meta1/Max", Meta1KeyMax},
		{"/Meta2/Max", Meta2KeyMax},
	}

	// KeyDict drives the pretty-printing and pretty-scanning of the key space.
	KeyDict = KeyComprehensionTable{
		{Name: "/Local", start: LocalPrefix, end: LocalMax, Entries: []DictEntry{
			{Name: "/Store", prefix: roachpb.Key(LocalStorePrefix),
				ppFunc: localStoreKeyPrint, PSFunc: localStoreKeyParse},
			{Name: "/RangeID", prefix: roachpb.Key(LocalRangeIDPrefix),
				ppFunc: localRangeIDKeyPrint, PSFunc: localRangeIDKeyParse},
			{Name: "/Range", prefix: LocalRangePrefix, ppFunc: localRangeKeyPrint,
				PSFunc: parseUnsupported},
			{Name: "/Lock", prefix: LocalRangeLockTablePrefix, ppFunc: localRangeLockTablePrint,
				PSFunc: parseUnsupported},
		}},
		{Name: "/Meta1", start: Meta1Prefix, end: Meta1KeyMax, Entries: []DictEntry{
			{Name: "", prefix: Meta1Prefix, ppFunc: print,
				PSFunc: func(input string) (string, roachpb.Key) {
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
		{Name: "/Meta2", start: Meta2Prefix, end: Meta2KeyMax, Entries: []DictEntry{
			{Name: "", prefix: Meta2Prefix, ppFunc: print,
				PSFunc: func(input string) (string, roachpb.Key) {
					input = mustShiftSlash(input)
					unq, err := strconv.Unquote(input)
					if err != nil {
						panic(&ErrUglifyUnsupported{err})
					}
					if len(unq) == 0 {
						return "", Meta2Prefix
					}
					return "", RangeMetaKey(MustAddr(roachpb.Key(unq))).AsRawKey()
				},
			}},
		},
		{Name: "/System", start: SystemPrefix, end: SystemMax, Entries: []DictEntry{
			{Name: "/NodeLiveness", prefix: NodeLivenessPrefix,
				ppFunc: decodeKeyPrint,
				PSFunc: parseUnsupported,
			},
			{Name: "/NodeLivenessMax", prefix: NodeLivenessKeyMax,
				ppFunc: decodeKeyPrint,
				PSFunc: parseUnsupported,
			},
			{Name: "/StatusNode", prefix: StatusNodePrefix,
				ppFunc: decodeKeyPrint,
				PSFunc: parseUnsupported,
			},
			{Name: "/tsd", prefix: TimeseriesPrefix,
				ppFunc: timeseriesKeyPrint,
				PSFunc: parseUnsupported,
			},
			{Name: "/SystemSpanConfigKeys", prefix: SystemSpanConfigPrefix,
				ppFunc: decodeKeyPrint,
				PSFunc: parseUnsupported,
			},
		}},
		{Name: "/NamespaceTable", start: NamespaceTableMin, end: NamespaceTableMax, Entries: []DictEntry{
			{Name: "", prefix: nil, ppFunc: decodeKeyPrint, PSFunc: parseUnsupported},
		}},
		{Name: "/Table", start: TableDataMin, end: TableDataMax, Entries: []DictEntry{
			{Name: "", prefix: nil, ppFunc: decodeKeyPrint, PSFunc: tableKeyParse},
		}},
		{Name: "/Tenant", start: TenantTableDataMin, end: TenantTableDataMax, Entries: []DictEntry{
			{Name: "", prefix: nil, ppFunc: tenantKeyPrint, PSFunc: tenantKeyParse},
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
		{name: "RangeTombstone", suffix: LocalRangeTombstoneSuffix},
		{name: "RaftHardState", suffix: LocalRaftHardStateSuffix},
		{name: "RangeAppliedState", suffix: LocalRangeAppliedStateSuffix},
		{name: "RaftLog", suffix: LocalRaftLogSuffix,
			ppFunc: raftLogKeyPrint,
			psFunc: raftLogKeyParse,
		},
		{name: "RaftTruncatedState", suffix: LocalRaftTruncatedStateSuffix},
		{name: "RangeLastReplicaGCTimestamp", suffix: LocalRangeLastReplicaGCTimestampSuffix},
		{name: "RangeLease", suffix: LocalRangeLeaseSuffix},
		{name: "RangePriorReadSummary", suffix: LocalRangePriorReadSummarySuffix},
		{name: "RangeStats", suffix: LocalRangeStatsLegacySuffix},
		{name: "RangeGCThreshold", suffix: LocalRangeGCThresholdSuffix},
		{name: "RangeVersion", suffix: LocalRangeVersionSuffix},
	}

	rangeSuffixDict = []struct {
		name   string
		suffix []byte
		atEnd  bool
	}{
		{name: "RangeDescriptor", suffix: LocalRangeDescriptorSuffix, atEnd: true},
		{name: "Transaction", suffix: LocalTransactionSuffix, atEnd: false},
		{name: "QueueLastProcessed", suffix: LocalQueueLastProcessedSuffix, atEnd: false},
		{name: "RangeProbe", suffix: LocalRangeProbeSuffix, atEnd: true},
	}
)

var constSubKeyDict = []struct {
	name string
	key  roachpb.RKey
}{
	{"/storeIdent", localStoreIdentSuffix},
	{"/gossipBootstrap", localStoreGossipSuffix},
	{"/clusterVersion", localStoreClusterVersionSuffix},
	{"/nodeTombstone", localStoreNodeTombstoneSuffix},
	{"/cachedSettings", localStoreCachedSettingsSuffix},
	{"/lossOfQuorumRecovery/applied", localStoreUnsafeReplicaRecoverySuffix},
}

func nodeTombstoneKeyPrint(key roachpb.Key) string {
	nodeID, err := DecodeNodeTombstoneKey(key)
	if err != nil {
		return fmt.Sprintf("<invalid: %s>", err)
	}
	return fmt.Sprint("n", nodeID)
}

func cachedSettingsKeyPrint(key roachpb.Key) string {
	settingKey, err := DecodeStoreCachedSettingsKey(key)
	if err != nil {
		return fmt.Sprintf("<invalid: %s>", err)
	}
	return settingKey.String()
}

func localStoreKeyPrint(_ []encoding.Direction, key roachpb.Key) string {
	for _, v := range constSubKeyDict {
		if bytes.HasPrefix(key, v.key) {
			if v.key.Equal(localStoreNodeTombstoneSuffix) {
				return v.name + "/" + nodeTombstoneKeyPrint(
					append(roachpb.Key(nil), append(LocalStorePrefix, key...)...),
				)
			} else if v.key.Equal(localStoreCachedSettingsSuffix) {
				return v.name + "/" + cachedSettingsKeyPrint(
					append(roachpb.Key(nil), append(LocalStorePrefix, key...)...),
				)
			} else if v.key.Equal(localStoreUnsafeReplicaRecoverySuffix) {
				return v.name + "/" + lossOfQuorumRecoveryEntryKeyPrint(
					append(roachpb.Key(nil), append(LocalStorePrefix, key...)...),
				)
			}
			return v.name
		}
	}

	return fmt.Sprintf("%q", []byte(key))
}

func lossOfQuorumRecoveryEntryKeyPrint(key roachpb.Key) string {
	entryID, err := DecodeStoreUnsafeReplicaRecoveryKey(key)
	if err != nil {
		return fmt.Sprintf("<invalid: %s>", err)
	}
	return entryID.String()
}

func localStoreKeyParse(input string) (remainder string, output roachpb.Key) {
	for _, s := range constSubKeyDict {
		if strings.HasPrefix(input, s.name) {
			switch {
			case
				s.key.Equal(localStoreNodeTombstoneSuffix),
				s.key.Equal(localStoreCachedSettingsSuffix):
				panic(&ErrUglifyUnsupported{errors.Errorf("cannot parse local store key with suffix %s", s.key)})
			case s.key.Equal(localStoreUnsafeReplicaRecoverySuffix):
				recordIDString := input[len(localStoreUnsafeReplicaRecoverySuffix):]
				recordUUID, err := uuid.FromString(recordIDString)
				if err != nil {
					panic(&ErrUglifyUnsupported{errors.Errorf("cannot parse local store key with suffix %s", s.key)})
				}
				output = StoreUnsafeReplicaRecoveryKey(recordUUID)
			default:
				output = MakeStoreKey(s.key, nil)
			}
			return
		}
	}
	input = mustShiftSlash(input)
	slashPos := strings.IndexByte(input, '/')
	if slashPos < 0 {
		slashPos = len(input)
	}
	remainder = input[slashPos:] // `/something/else` -> `/else`
	output = roachpb.Key(input[:slashPos])
	return
}

const strTable = "/Table/"
const strSystemConfigSpan = "SystemConfigSpan"
const strSystemConfigSpanStart = "Start"

func tenantKeyParse(input string) (remainder string, output roachpb.Key) {
	input = mustShiftSlash(input)
	slashPos := strings.Index(input, "/")
	if slashPos < 0 {
		slashPos = len(input)
	}
	remainder = input[slashPos:] // `/something/else` -> `/else`
	tenantIDStr := input[:slashPos]
	tenantID, err := strconv.ParseUint(tenantIDStr, 10, 64)
	if err != nil {
		panic(&ErrUglifyUnsupported{err})
	}
	output = MakeTenantPrefix(roachpb.MakeTenantID(tenantID))
	if strings.HasPrefix(remainder, strTable) {
		var indexKey roachpb.Key
		remainder = remainder[len(strTable)-1:]
		remainder, indexKey = tableKeyParse(remainder)
		output = append(output, indexKey...)
	}
	return remainder, output
}

func tableKeyParse(input string) (remainder string, output roachpb.Key) {
	input = mustShiftSlash(input)
	slashPos := strings.Index(input, "/")
	if slashPos < 0 {
		slashPos = len(input)
	}
	remainder = input[slashPos:] // `/something/else` -> `/else`
	tableIDStr := input[:slashPos]
	if tableIDStr == strSystemConfigSpan {
		if remainder[1:] == strSystemConfigSpanStart {
			remainder = ""
		}
		output = SystemConfigSpan.Key
		return
	}
	tableID, err := strconv.ParseUint(tableIDStr, 10, 32)
	if err != nil {
		panic(&ErrUglifyUnsupported{err})
	}
	output = encoding.EncodeUvarintAscending(nil /* key */, tableID)
	if remainder != "" {
		var indexKey roachpb.Key
		remainder, indexKey = tableIndexParse(remainder)
		output = append(output, indexKey...)
	}
	return remainder, output
}

// tableIndexParse parses an index id out of the input and returns the remainder.
// The input is expected to be of the form "/<index id>[/...]".
func tableIndexParse(input string) (string, roachpb.Key) {
	input = mustShiftSlash(input)
	slashPos := strings.Index(input, "/")
	if slashPos < 0 {
		// We accept simply "/<id>"; if there's no further slashes, the whole string
		// has to be the index id.
		slashPos = len(input)
	}
	remainder := input[slashPos:] // `/something/else` -> `/else`
	indexIDStr := input[:slashPos]
	indexID, err := strconv.ParseUint(indexIDStr, 10, 32)
	if err != nil {
		panic(&ErrUglifyUnsupported{err})
	}
	output := encoding.EncodeUvarintAscending(nil /* key */, indexID)
	return remainder, output
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
	if endPos := strings.IndexByte(input, '/'); endPos > 0 {
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
	case bytes.Equal(LocalRangeIDReplicatedInfix, []byte(infix)):
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
			panic(&ErrUglifyUnsupported{errors.New("nontrivial detail")})
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
	panic(&ErrUglifyUnsupported{errors.New("unhandled general range key")})
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

// lockTablePrintLockedKey is initialized to prettyPrintInternal in init() to break an
// initialization loop.
var lockTablePrintLockedKey func(valDirs []encoding.Direction, key roachpb.Key, quoteRawKeys bool) string

func localRangeLockTablePrint(valDirs []encoding.Direction, key roachpb.Key) string {
	var buf bytes.Buffer
	if !bytes.HasPrefix(key, LockTableSingleKeyInfix) {
		fmt.Fprintf(&buf, "/\"%x\"", key)
		return buf.String()
	}
	buf.WriteString("/Intent")
	key = key[len(LockTableSingleKeyInfix):]
	b, lockedKey, err := encoding.DecodeBytesAscending(key, nil)
	if err != nil || len(b) != 0 {
		fmt.Fprintf(&buf, "/\"%x\"", key)
		return buf.String()
	}
	buf.WriteString(lockTablePrintLockedKey(valDirs, lockedKey, true))
	return buf.String()
}

// ErrUglifyUnsupported is returned when UglyPrint doesn't know how to process a
// key.
type ErrUglifyUnsupported struct {
	Wrapped error
}

func (euu *ErrUglifyUnsupported) Error() string {
	return fmt.Sprintf("unsupported pretty key: %v", euu.Wrapped)
}

func abortSpanKeyParse(rangeID roachpb.RangeID, input string) (string, roachpb.Key) {
	var err error
	input = mustShiftSlash(input)
	_, input = mustShift(input[:len(input)-1])
	if len(input) != len(uuid.UUID{}.String()) {
		panic(&ErrUglifyUnsupported{errors.New("txn id not available")})
	}
	id, err := uuid.FromString(input)
	if err != nil {
		panic(&ErrUglifyUnsupported{err})
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

func timeseriesKeyPrint(_ []encoding.Direction, key roachpb.Key) string {
	return PrettyPrintTimeseriesKey(key)
}

func tenantKeyPrint(valDirs []encoding.Direction, key roachpb.Key) string {
	key, tID, err := DecodeTenantPrefix(key)
	if err != nil {
		return fmt.Sprintf("/err:%v", err)
	}
	if len(key) == 0 {
		return fmt.Sprintf("/%s", tID)
	}
	return fmt.Sprintf("/%s%s", tID, key.StringWithDirs(valDirs, 0))
}

// prettyPrintInternal parse key with prefix in KeyDict.
// For table keys, valDirs correspond to the encoding direction of each encoded
// value in key.
// If valDirs is unspecified, the default encoding direction for each value
// type is used (see encoding.go:prettyPrintFirstValue).
// If the key doesn't match any prefix in KeyDict, return its byte value with
// quotation and false, or else return its human readable value and true.
func prettyPrintInternal(valDirs []encoding.Direction, key roachpb.Key, quoteRawKeys bool) string {
	for _, k := range ConstKeyDict {
		if key.Equal(k.Value) {
			return k.Name
		}
	}

	helper := func(key roachpb.Key) (string, bool) {
		var b strings.Builder
		for _, k := range KeyDict {
			if key.Compare(k.start) >= 0 && (k.end == nil || key.Compare(k.end) <= 0) {
				b.WriteString(k.Name)
				if k.end != nil && k.end.Compare(key) == 0 {
					b.WriteString("/Max")
					return b.String(), true
				}

				hasPrefix := false
				for _, e := range k.Entries {
					if bytes.HasPrefix(key, e.prefix) {
						hasPrefix = true
						key = key[len(e.prefix):]
						b.WriteString(e.Name)
						b.WriteString(e.ppFunc(valDirs, key))
						break
					}
				}
				if !hasPrefix {
					key = key[len(k.start):]
					if quoteRawKeys {
						b.WriteByte('/')
						b.WriteByte('"')
					}
					b.Write([]byte(key))
					if quoteRawKeys {
						b.WriteByte('"')
					}
				}

				return b.String(), true
			}
		}

		if quoteRawKeys {
			return fmt.Sprintf("%q", []byte(key)), false
		}
		return string(key), false
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

// PrettyPrint prints the key in a human readable format, see TestPrettyPrint.
// The output does not indicate whether a key is part of the replicated or un-
// replicated keyspace.
//
// valDirs correspond to the encoding direction of each encoded value in key.
// For example, table keys could have column values encoded in ascending or
// descending directions.
// If valDirs is unspecified, the default encoding direction for each value
// type is used (see encoding.go:prettyPrintFirstValue).
//
// See keysutil.UglyPrint() for an inverse.
func PrettyPrint(valDirs []encoding.Direction, key roachpb.Key) string {
	return prettyPrintInternal(valDirs, key, true /* quoteRawKeys */)
}

func init() {
	roachpb.PrettyPrintKey = PrettyPrint
	roachpb.PrettyPrintRange = PrettyPrintRange
	lockTablePrintLockedKey = prettyPrintInternal
}

// PrettyPrintRange pretty prints a compact representation of a key range. The
// output is of the form:
//    commonPrefix{remainingStart-remainingEnd}
// If the end key is empty, the output is of the form:
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
		copyEscape(&b, prettyStart[:maxChars-1])
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
		copyEscape(&b, prettyStart[:i])
		b.WriteRune('…')
		return b.String()
	}
	b.WriteString(prettyStart[:i])
	remaining := (maxChars - i - 3) / 2

	printTrunc := func(b *bytes.Buffer, what string, maxChars int) {
		if len(what) <= maxChars {
			copyEscape(b, what)
		} else {
			copyEscape(b, what[:maxChars-1])
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

// copyEscape copies the string to the buffer, and avoids writing
// invalid UTF-8 sequences and control characters.
func copyEscape(buf *bytes.Buffer, s string) {
	buf.Grow(len(s))
	// k is the index in s before which characters have already
	// been copied into buf.
	k := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c < utf8.RuneSelf && strconv.IsPrint(rune(c)) {
			continue
		}
		buf.WriteString(s[k:i])
		l, width := utf8.DecodeRuneInString(s[i:])
		if l == utf8.RuneError || l < 0x20 {
			const hex = "0123456789abcdef"
			buf.WriteByte('\\')
			buf.WriteByte('x')
			buf.WriteByte(hex[c>>4])
			buf.WriteByte(hex[c&0xf])
		} else {
			buf.WriteRune(l)
		}
		k = i + width
		i += width - 1
	}
	buf.WriteString(s[k:])
}
