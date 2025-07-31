// Copyright 2015 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package keys

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// PrettyPrintTimeseriesKey is a hook for pretty printing a timeseries key. The
// timeseries key prefix will already have been stripped off.
var PrettyPrintTimeseriesKey func(buf *redact.StringBuilder, key roachpb.Key)

// QuoteOpt is a flag option used when pretty-printing keys to indicate whether
// to quote raw key values.
type QuoteOpt bool

const (
	// QuoteRaw is the QuoteOpt used to indicate that we should use quotes when
	// printing raw keys.
	QuoteRaw QuoteOpt = true
	// DontQuoteRaw is the QuoteOpt used to indicate that we shouldn't use quotes
	// when printing raw keys.
	DontQuoteRaw QuoteOpt = false
)

// DictEntry contains info on pretty-printing and pretty-scanning keys in a
// region of the key space.
type DictEntry struct {
	Name   redact.SafeString
	prefix roachpb.Key
	// print the key's pretty value, key has been removed prefix data
	ppFunc func(buf *redact.StringBuilder, valDirs []encoding.Direction, key roachpb.Key)
	// safe format the key's pretty value into a RedactableString
	sfFunc func(buf *redact.StringBuilder, valDirs []encoding.Direction, key roachpb.Key, quoteRawKeys QuoteOpt)
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
	Name    redact.SafeString
	start   roachpb.Key
	end     roachpb.Key
	Entries []DictEntry
}

// KeyDict drives the pretty-printing and pretty-scanning of the key space.
// This is initialized in init().
var KeyDict KeyComprehensionTable

var (
	// ConstKeyOverrides provides overrides that define how to translate specific
	// pretty-printed keys.
	ConstKeyOverrides = []struct {
		Name  redact.SafeString
		Value roachpb.Key
	}{
		{"/Max", MaxKey},
		{"/Min", MinKey},
		{"/Meta1/Max", Meta1KeyMax},
		{"/Meta2/Max", Meta2KeyMax},
	}

	// keyofKeyDict means the key of suffix which is itself a key,
	// should recursively pretty print it, see issue #3228
	keyOfKeyDict = []struct {
		name   redact.SafeString
		prefix []byte
	}{
		{name: "/Meta2", prefix: Meta2Prefix},
		{name: "/Meta1", prefix: Meta1Prefix},
	}

	rangeIDSuffixDict = []struct {
		name   string
		suffix []byte
		ppFunc func(buf *redact.StringBuilder, key roachpb.Key)
		psFunc func(rangeID roachpb.RangeID, input string) (string, roachpb.Key)
	}{
		{name: "AbortSpan", suffix: LocalAbortSpanSuffix, ppFunc: abortSpanKeyPrint, psFunc: abortSpanKeyParse},
		{name: "ReplicatedSharedLocksTransactionLatch",
			suffix: LocalReplicatedSharedLocksTransactionLatchingKeySuffix,
			ppFunc: replicatedSharedLocksTransactionLatchingKeyPrint,
		},
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
		{name: "RangeGCHint", suffix: LocalRangeGCHintSuffix},
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
	{"/lossOfQuorumRecovery/status", localStoreLossOfQuorumRecoveryStatusSuffix},
	{"/lossOfQuorumRecovery/cleanup", localStoreLossOfQuorumRecoveryCleanupActionsSuffix},
}

func nodeTombstoneKeyPrint(buf *redact.StringBuilder, key roachpb.Key) {
	nodeID, err := DecodeNodeTombstoneKey(key)
	if err != nil {
		buf.Printf("<invalid: %s>", err)
	}
	buf.Printf("n%s", nodeID)
}

func cachedSettingsKeyPrint(buf *redact.StringBuilder, key roachpb.Key) {
	settingKey, err := DecodeStoreCachedSettingsKey(key)
	if err != nil {
		buf.Printf("<invalid: %s>", err)
	}
	buf.Print(settingKey.String())
}

func localStoreKeyPrint(buf *redact.StringBuilder, _ []encoding.Direction, key roachpb.Key) {
	for _, v := range constSubKeyDict {
		if bytes.HasPrefix(key, v.key) {
			buf.Print(v.name)
			if v.key.Equal(localStoreNodeTombstoneSuffix) {
				buf.SafeRune('/')
				nodeTombstoneKeyPrint(
					buf, append(roachpb.Key(nil), append(LocalStorePrefix, key...)...),
				)
			} else if v.key.Equal(localStoreCachedSettingsSuffix) {
				buf.SafeRune('/')
				cachedSettingsKeyPrint(
					buf, append(roachpb.Key(nil), append(LocalStorePrefix, key...)...),
				)
			} else if v.key.Equal(localStoreUnsafeReplicaRecoverySuffix) {
				buf.SafeRune('/')
				lossOfQuorumRecoveryEntryKeyPrint(
					buf, append(roachpb.Key(nil), append(LocalStorePrefix, key...)...),
				)
			}
			return
		}
	}
	buf.Printf("%q", []byte(key))
}

func lossOfQuorumRecoveryEntryKeyPrint(buf *redact.StringBuilder, key roachpb.Key) {
	entryID, err := DecodeStoreUnsafeReplicaRecoveryKey(key)
	if err != nil {
		buf.Printf("<invalid: %s>", err)
	}
	buf.Print(entryID.String())
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

// GetTenantKeyParseFn returns a function that parses the relevant prefix of the
// tenant data into a roachpb.Key, returning the remainder and the key
// corresponding to the consumed prefix of 'input'. It is expected that the
// '/Tenant' prefix has already been removed (i.e. the input is assumed to be of
// the form '/<tenantID>/...'). If the input is of the form
// '/<tenantID>/Table/<tableID>/...', then passed-in tableKeyParseFn function is
// invoked on the '/<tableID>/...' part.
func GetTenantKeyParseFn(
	tableKeyParseFn func(string) (string, roachpb.Key),
) func(input string) (remainder string, output roachpb.Key) {
	return func(input string) (remainder string, output roachpb.Key) {
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
		output = MakeTenantPrefix(roachpb.MustMakeTenantID(tenantID))
		if strings.HasPrefix(remainder, strTable) {
			var indexKey roachpb.Key
			remainder = remainder[len(strTable)-1:]
			remainder, indexKey = tableKeyParseFn(remainder)
			output = append(output, indexKey...)
		}
		return remainder, output
	}
}

func tableKeyParse(input string) (remainder string, output roachpb.Key) {
	input = mustShiftSlash(input)
	slashPos := strings.Index(input, "/")
	if slashPos < 0 {
		slashPos = len(input)
	}
	remainder = input[slashPos:] // `/something/else` -> `/else`
	tableIDStr := input[:slashPos]
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
	return "", RaftLogKey(rangeID, kvpb.RaftIndex(index))
}

func raftLogKeyPrint(buf *redact.StringBuilder, key roachpb.Key) {
	var logIndex uint64
	var err error
	key, logIndex, err = encoding.DecodeUint64Ascending(key)
	if err != nil {
		buf.Printf("/err<%v:%q>", err, []byte(key))
		return
	}

	buf.Printf("%s%d", strLogIndex, logIndex)
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

func localRangeIDKeyPrint(
	buf *redact.StringBuilder, valDirs []encoding.Direction, key roachpb.Key,
) {
	if encoding.PeekType(key) != encoding.Int {
		buf.Printf("/err<%q>", []byte(key))
		return
	}

	// Get the rangeID.
	key, i, err := encoding.DecodeVarintAscending(key)
	if err != nil {
		buf.Printf("/err<%v:%q>", err, []byte(key))
		return
	}

	fmt.Fprintf(buf, "/%d", i)

	// Print and remove the rangeID infix specifier.
	if len(key) != 0 {
		buf.Printf("/%s", string(key[0]))
		key = key[1:]
	}

	// Get the suffix.
	hasSuffix := false
	for _, s := range rangeIDSuffixDict {
		if bytes.HasPrefix(key, s.suffix) {
			buf.Printf("/%s", s.name)
			key = key[len(s.suffix):]
			if s.ppFunc != nil && len(key) != 0 {
				s.ppFunc(buf, key)
				return
			}
			hasSuffix = true
			break
		}
	}

	// Get the encode values.
	if hasSuffix {
		decodeKeyPrint(buf, valDirs, key)
	} else {
		buf.Printf("%q", []byte(key))
	}
}

func localRangeKeyPrint(buf *redact.StringBuilder, valDirs []encoding.Direction, key roachpb.Key) {
	for _, s := range rangeSuffixDict {
		if s.atEnd {
			if bytes.HasSuffix(key, s.suffix) {
				key = key[:len(key)-len(s.suffix)]
				_, decodedKey, err := encoding.DecodeBytesAscending([]byte(key), nil)
				if err != nil {
					decodeKeyPrint(buf, valDirs, key)
					buf.SafeRune('/')
					buf.Print(s.name)
				} else {
					buf.Printf("%s/%s", roachpb.Key(decodedKey), s.name)
				}
				return
			}
		} else {
			begin := bytes.Index(key, s.suffix)
			if begin > 0 {
				addrKey := key[:begin]
				_, decodedAddrKey, err := encoding.DecodeBytesAscending([]byte(addrKey), nil)
				if err != nil {
					decodeKeyPrint(buf, valDirs, addrKey)
					buf.SafeRune('/')
					buf.Print(s.name)
				} else {
					buf.Printf("%s/%s", roachpb.Key(decodedAddrKey), s.name)
				}
				if bytes.Equal(s.suffix, LocalTransactionSuffix) {
					txnID, err := uuid.FromBytes(key[(begin + len(s.suffix)):])
					if err != nil {
						buf.Printf("/%q/err:%v", key, err)
						return
					}
					buf.Printf("/%q", txnID)
				} else {
					id := key[(begin + len(s.suffix)):]
					buf.Printf("/%q", []byte(id))
				}
				return
			}
		}
	}

	_, decodedKey, err := encoding.DecodeBytesAscending([]byte(key), nil)
	if err != nil {
		decodeKeyPrint(buf, valDirs, key)
	} else {
		buf.Printf("%s", roachpb.Key(decodedKey))
	}
}

// lockTablePrintLockedKey is initialized to prettyPrintInternal in init() to break an
// initialization loop.
var lockTablePrintLockedKey func(
	valDirs []encoding.Direction, key roachpb.Key, quoteRawKeys QuoteOpt,
) redact.RedactableString

func localRangeLockTablePrint(
	buf *redact.StringBuilder, valDirs []encoding.Direction, key roachpb.Key,
) {
	if !bytes.HasPrefix(key, LockTableSingleKeyInfix) {
		buf.Printf("/\"%x\"", key)
		return
	}
	key = key[len(LockTableSingleKeyInfix):]
	b, lockedKey, err := encoding.DecodeBytesAscending(key, nil)
	if err != nil || len(b) != 0 {
		buf.Printf("/\"%x\"", key)
		return
	}
	buf.Print(lockTablePrintLockedKey(valDirs, lockedKey, QuoteRaw))
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

func abortSpanKeyPrint(buf *redact.StringBuilder, key roachpb.Key) {
	_, id, err := encoding.DecodeBytesAscending([]byte(key), nil)
	if err != nil {
		buf.Printf("/%q/err:%v", key, err)
		return
	}

	txnID, err := uuid.FromBytes(id)
	if err != nil {
		buf.Printf("/%q/err:%v", key, err)
		return
	}

	buf.Printf("/%q", txnID)
}

func replicatedSharedLocksTransactionLatchingKeyPrint(buf *redact.StringBuilder, key roachpb.Key) {
	_, id, err := encoding.DecodeBytesAscending([]byte(key), nil)
	if err != nil {
		buf.Printf("/%q/err:%v", key, err)
		return
	}

	txnID, err := uuid.FromBytes(id)
	if err != nil {
		buf.Printf("/%q/err:%v", key, err)
		return
	}

	buf.Printf("/%q", txnID)
}

func print(buf *redact.StringBuilder, _ []encoding.Direction, key roachpb.Key) {
	buf.Printf("/%q", []byte(key))
}

func decodeKeyPrint(buf *redact.StringBuilder, valDirs []encoding.Direction, key roachpb.Key) {
	encoding.PrettyPrintValue(buf, valDirs, key, "/")
}

func timeseriesKeyPrint(buf *redact.StringBuilder, _ []encoding.Direction, key roachpb.Key) {
	PrettyPrintTimeseriesKey(buf, key)
}

func tenantKeyPrint(buf *redact.StringBuilder, valDirs []encoding.Direction, key roachpb.Key) {
	key, tID, err := DecodeTenantPrefix(key)
	if err != nil {
		buf.Printf("/err:%v", err)
		return
	}
	if len(key) == 0 {
		buf.Printf("/%s", tID)
		return
	}
	buf.SafeRune('/')
	buf.Print(tID)
	key.StringWithDirs(buf, valDirs)
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
//
// See SafeFormat for a redaction-safe implementation.
func PrettyPrint(valDirs []encoding.Direction, key roachpb.Key) string {
	return safeFormatInternal(valDirs, key, QuoteRaw).StripMarkers()
}

// formatTableKey formats the given key in the system tenant table keyspace & redacts any
// sensitive information from the result. Sensitive information is considered any value other
// than the table ID or index ID (e.g. any index-key/value-literal).
//
// NB: It's the responsibility of the caller to prefix the printed key values with the relevant
// keyspace identifier (e.g. `/Table`).
//
// For example:
//   - `/42/‹"index key"›`
//   - `/42/122/‹"index key"›`
//   - `/42/122/‹"index key"›/‹"some value"›`
//   - `/42/122/‹"index key"›/‹"some value"›/‹"some other value"›`
func formatTableKey(
	buf *redact.StringBuilder, valDirs []encoding.Direction, key roachpb.Key, _ QuoteOpt,
) {
	vals, types := encoding.PrettyPrintValuesWithTypes(valDirs, key)
	prefixLength := 1

	if len(vals) > 0 && types[0] != encoding.Int {
		buf.Printf("/err:ExpectedTableID-FoundType%v", redact.Safe(types[0]))
		return
	}

	// Accommodate cases where the table key contains a primary index field in
	// the prefix. ex: `/<table id>/<index id>`
	if len(vals) > 1 && types[1] == encoding.Int {
		prefixLength++
	}

	for i := 0; i < prefixLength; i++ {
		buf.Printf("/%v", redact.Safe(vals[i]))
	}
	for _, val := range vals[prefixLength:] {
		buf.Printf("/%s", val)
	}
}

// formatTenantKey formats the given key for a tenant table & redacts any sensitive information
// from the result. Sensitive information is considered any value other than the TenantID,
// table ID, or index ID (e.g. any index-key/value-literal).
//
// NB: It's the responsibility of the caller to prefix the printed key values with the relevant
// keyspace identifier (e.g. `/Tenant`).
//
// For example:
//   - `/5/Table/42/‹"index key"›`
//   - `/5/Table/42/122/‹"index key"›`
func formatTenantKey(
	buf *redact.StringBuilder, valDirs []encoding.Direction, key roachpb.Key, quoteRawKeys QuoteOpt,
) {
	key, tID, err := DecodeTenantPrefix(key)
	if err != nil {
		buf.Printf("/err:%v", err)
		return
	}

	buf.Printf("/%s", tID)
	if len(key) != 0 {
		buf.Print(safeFormatInternal(valDirs, key, quoteRawKeys))
	}
}

// SafeFormat is the generalized redaction function used to redact pretty-printed keys.
func SafeFormat(w redact.SafeWriter, valDirs []encoding.Direction, key roachpb.Key) {
	w.Print(safeFormatInternal(valDirs, key, QuoteRaw))
}

func safeFormatInternal(
	valDirs []encoding.Direction, key roachpb.Key, quoteRawKeys QuoteOpt,
) redact.RedactableString {
	for _, k := range ConstKeyOverrides {
		if key.Equal(k.Value) {
			return redact.Sprint(k.Name)
		}
	}

	helper := func(b *redact.StringBuilder, key roachpb.Key) {
		for _, k := range KeyDict {
			if key.Compare(k.start) >= 0 && (k.end == nil || key.Compare(k.end) <= 0) {
				b.Print(k.Name)
				if k.end != nil && k.end.Compare(key) == 0 {
					b.Print(redact.Safe("/Max"))
					return
				}

				safeFormatted := false
				var buf redact.StringBuilder
				for _, e := range k.Entries {
					if bytes.HasPrefix(key, e.prefix) && e.sfFunc != nil {
						key = key[len(e.prefix):]
						b.Print(e.Name)
						e.sfFunc(&buf, valDirs, key, quoteRawKeys)
						b.Print(buf.RedactableString())
						safeFormatted = true
					}
				}

				if !safeFormatted {
					hasPrefix := false
					for _, e := range k.Entries {
						if bytes.HasPrefix(key, e.prefix) {
							hasPrefix = true
							key = key[len(e.prefix):]
							b.Print(redact.Safe(e.Name))
							e.ppFunc(&buf, valDirs, key)
							b.Print(buf.RedactableString())
							break
						}
					}
					if !hasPrefix {
						key = key[len(k.start):]
						if quoteRawKeys {
							b.Print("/")
							b.Print(`"`)
						}
						if _, err := b.Write([]byte(key)); err != nil {
							b.Printf("<invalid: %s>", err)
						}
						if quoteRawKeys {
							b.Print(`"`)
						}
					}
				}
				return
			}
		}
		// If we reach this point, the key is not recognized based on KeyDict.
		// Print the raw bytes instead.
		if quoteRawKeys {
			b.Printf("%q", []byte(key))
			return
		}
		b.Print(string(key))
	}

	var b redact.StringBuilder
	for _, k := range keyOfKeyDict {
		if bytes.HasPrefix(key, k.prefix) {
			key = key[len(k.prefix):]
			helper(&b, key)
			str := b.RedactableString()
			if len(str) > 0 && strings.Index(str.StripMarkers(), "/") != 0 {
				return redact.Sprintf("%v/%v", k.name, str)
			}
			return redact.Sprintf("%v%v", k.name, str)
		}
	}
	helper(&b, key)
	return b.RedactableString()
}

func init() {
	roachpb.PrettyPrintKey = PrettyPrint
	roachpb.SafeFormatKey = SafeFormat
	roachpb.PrettyPrintRange = PrettyPrintRange
	lockTablePrintLockedKey = safeFormatInternal

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
			{Name: "", prefix: nil, ppFunc: decodeKeyPrint, PSFunc: tableKeyParse, sfFunc: formatTableKey},
		}},
		{Name: "/Tenant", start: TenantTableDataMin, end: TenantTableDataMax, Entries: []DictEntry{
			{Name: "", prefix: nil, ppFunc: tenantKeyPrint, PSFunc: GetTenantKeyParseFn(tableKeyParse), sfFunc: formatTenantKey},
		}},
	}
}

// PrettyPrintRange pretty prints a compact representation of a key range. The
// output is of the form:
//
//	commonPrefix{remainingStart-remainingEnd}
//
// If the end key is empty, the output is of the form:
//
//	start
//
// It prints at most maxChars, truncating components as needed. See
// TestPrettyPrintRange for some examples.
func PrettyPrintRange(start, end roachpb.Key, maxChars int) string {
	var b bytes.Buffer
	if maxChars < 8 {
		maxChars = 8
	}
	prettyStart := safeFormatInternal(nil /* valDirs */, start, DontQuoteRaw).StripMarkers()
	if len(end) == 0 {
		if len(prettyStart) <= maxChars {
			return prettyStart
		}
		copyEscape(&b, prettyStart[:maxChars-1])
		b.WriteRune('…')
		return b.String()
	}
	prettyEnd := safeFormatInternal(nil /* valDirs */, end, DontQuoteRaw).StripMarkers()
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
