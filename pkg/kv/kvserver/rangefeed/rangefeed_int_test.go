// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package rangefeed_test

import (
	"bytes"
	"container/list"
	"context"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	crangefeed "github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	securityassets.SetLoader(securitytest.EmbeddedAssets)
	serverutils.InitTestServerFactory(server.TestServerFactory)
	serverutils.InitTestClusterFactory(testcluster.TestClusterFactory)
	os.Exit(m.Run())
}

// TODO(oleg): this test is a placeholder for randomized test for rangefeed.
// There are extra history validators that are not used by datadriven yet
// to validate history, but used here to stop linter complaining and as a usage
// example.
func TestRangeFeed(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	_, seed := randutil.NewTestRand()
	t.Logf("using test rand seed %d", seed)

	tc := testcluster.NewTestCluster(t, 1, base.TestClusterArgs{
		// ServerArgs: base.TestServerArgs{
		// 	DefaultTestTenant: base.TestControlsTenantsExplicitly,
		// },
	})
	tc.Start(t)
	defer tc.Stopper().Stop(ctx)

	sr := tc.ScratchRange(t)
	k := func(s int) roachpb.Key {
		return append(sr[:len(sr):len(sr)], fmt.Sprintf("%010d", s)...)
	}

	srv := tc.Server(0)

	db0 := srv.DB()
	i := 0
	for ; i < 10; i++ {
		err := db0.Put(ctx, k( /*rng.Intn(20)*/ 10), fmt.Sprintf("%05d", i))
		require.NoError(t, err, "failed to put value")
	}

	// Rangefeed using factory.
	rf, ff := createTestFeed(t, ctx, srv, roachpb.Span{Key: sr, EndKey: sr.PrefixEnd()},
		feedOpts{withDiff: true})
	defer func() {
		rf.Close()
	}()

	// Write more data.
	for ; i < 20; i++ {
		err := db0.Put(ctx, k( /*rng.Intn(20)*/ 10), fmt.Sprintf("%05d", i))
		require.NoError(t, err, "failed to put value")
	}
	t.Log("done writing")
	<-time.After(5 * time.Second)

	rf.Close()
	values := ff.values()
	values.requireAllEvents(t, k(10), 20)
	values.requirePreviousKeyValue(t)
	values.requireCpBelowFuture(t)
}

// Note: when using transactions to create intents, don't create conflicts as
// if could deadlock the test. Maybe add a detection and fail tests in those
// cases?
type txnInfo struct {
	txn *kv.Txn
}

type env struct {
	// test cluster
	tc *testcluster.TestCluster

	// range feeds
	feeds    []*crangefeed.RangeFeed
	captures []*feedData

	// named timestamps
	tts  timestamps
	txns map[string]txnInfo

	// work keys
	startKey testKey
}

func (e *env) close() {
	for _, f := range e.feeds {
		f.Close()
	}
	e.tc.Stopper().Stop(context.Background())
}

func newEnv(t *testing.T, servers int) *env {
	tc := testcluster.NewTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			// DefaultTestTenant: base.TestControlsTenantsExplicitly,
			Knobs: base.TestingKnobs{
				Store: &kvserver.StoreTestingKnobs{
					// Disable rangefeed intent pushing as it may interfere with tests
					// asserting intents.
					RangeFeedPushTxnsAge:      60 * time.Hour,
					RangeFeedPushTxnsInterval: 60 * time.Hour,
				},
			},
		},
	})
	tc.Start(t)

	// Lower the closed timestamp target duration to speed up the test.
	_, err := tc.ServerConn(0).Exec("SET CLUSTER SETTING kv.closed_timestamp.target_duration = '100ms'")
	require.NoError(t, err)

	sr := tc.ScratchRange(t)
	return &env{
		tc: tc,
		tts: timestamps{
			nameToTs: make(map[string]hlc.Timestamp),
			tsToName: make(map[hlc.Timestamp]string),
		},
		txns:     make(map[string]txnInfo),
		startKey: testKey(sr),
	}
}

func TestRangeFeeds(t *testing.T) {
	skip.UnderStress(t, "test uses test cluster and flakes under stress because of timeout checks")
	defer log.Scope(t).Close(t)

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		e := newEnv(t, 1)
		defer e.close()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) (output string) {
			var out []string
			switch d.Cmd {
			case "add-sst":
				out = handleAddSST(t, e, d)
			case "put-keys":
				out = handlePutKeys(t, e, d)
			case "create-feed":
				out = handleCreateFeed(t, e, d)
			case "wait-feed":
				out = handleWaitFeed(t, e, d)
			case "dump-feed":
				out = handleDumpFeed(t, e, d)
			case "check-error":
				out = handleCheckError(t, e, d)
			case "split-range":
				out = handleSplitRange(t, e, d)
			case "clear-range":
				out = handleClearRange(t, e, d)
			case "wait-initial-scan":
				out = handleWaitInitialScan(t, e, d)
			// case "move-replica":
			// case "move-leaseholder":
			default:
				t.Fatalf("unknown command on %s: %s", d.Pos, d.Cmd)
			}
			var builder strings.Builder
			for _, s := range out {
				builder.WriteString(s)
				builder.WriteRune('\n')
			}
			return builder.String()
		})
	})
}

func handleCheckError(t *testing.T, e *env, d *datadriven.TestData) []string {
	var feedIndex int
	var timeoutStr string
	d.MaybeScanArgs(t, "id", &feedIndex)
	d.MaybeScanArgs(t, "timeout", &timeoutStr)
	timeout := 30 * time.Second
	if timeoutStr != "" {
		var err error
		timeout, err = time.ParseDuration(timeoutStr)
		require.NoError(t, err, "invalid duration value '%s'", timeoutStr)
	}
	var res string
	select {
	case <-e.captures[feedIndex].waitError():
		res = fmt.Sprintf("%q", e.captures[feedIndex].err())
	case <-time.After(timeout):
		res = "timeout"
	}
	return []string{res}
}

func handleWaitFeed(t *testing.T, e *env, d *datadriven.TestData) []string {
	var (
		feedIndex                    int
		startKey, endKey, timeoutStr string
	)
	d.MaybeScanArgs(t, "id", &feedIndex)
	d.MaybeScanArgs(t, "startKey", &startKey)
	d.MaybeScanArgs(t, "endKey", &endKey)
	d.MaybeScanArgs(t, "timeout", &timeoutStr)
	timeout := 30 * time.Second
	if timeoutStr != "" {
		var err error
		timeout, err = time.ParseDuration(timeoutStr)
		require.NoError(t, err, "invalid duration value '%s'", timeoutStr)
	}
	now := e.tc.Server(0).Clock().Now()
	var startSpan, endSpan roachpb.Key
	if len(startKey) > 0 {
		startSpan = e.startKey.key(startKey)
	}
	if len(endKey) > 0 {
		endSpan = e.startKey.key(endKey)
	}
	feed := e.captures[feedIndex]
	c := feed.waitCheckpoint(now, roachpb.Span{Key: startSpan, EndKey: endSpan})
	select {
	case <-c:
	case <-time.After(timeout):
		return []string{"timeout"}
	case <-feed.waitError():
		return []string{fmt.Sprintf("%q", e.captures[feedIndex].err())}
	}
	return []string{"ok"}
}

func handleWaitInitialScan(t *testing.T, e *env, d *datadriven.TestData) []string {
	var (
		feedIndex  int
		timeoutStr string
	)
	d.MaybeScanArgs(t, "id", &feedIndex)
	d.MaybeScanArgs(t, "timeout", &timeoutStr)
	timeout := 30 * time.Second
	if timeoutStr != "" {
		var err error
		timeout, err = time.ParseDuration(timeoutStr)
		require.NoError(t, err, "invalid duration value '%s'", timeoutStr)
	}
	c := e.captures[feedIndex].waitForInitialScan()
	select {
	case <-c:
	case <-time.After(timeout):
		return []string{"timeout"}
	}
	return []string{"ok"}
}

func handleDumpFeed(t *testing.T, e *env, d *datadriven.TestData) []string {
	var feedIndex int
	var byTimestamp bool
	d.MaybeScanArgs(t, "id", &feedIndex)
	d.MaybeScanArgs(t, "byTs", &byTimestamp)
	e.feeds[feedIndex].Close()
	stream := e.captures[feedIndex].values().asSortedData(byTimestamp)
	return dumpKVS(t, stream, "", e.tts, e.startKey)
}

func handleCreateFeed(t *testing.T, e *env, d *datadriven.TestData) []string {
	var (
		server               int
		key, endKey, startTs string
		// If we don't set initial ts, then feed will start after request completes
		// and we will skip first events. We can always explicitly set time to 0
		// from the test itself if we need to test that.
		fo = feedOpts{initialTs: hlc.Timestamp{WallTime: 1}}
	)
	d.MaybeScanArgs(t, "server", &server)
	d.MaybeScanArgs(t, "startKey", &key)
	d.MaybeScanArgs(t, "endKey", &endKey)
	d.MaybeScanArgs(t, "startTs", &startTs)
	d.MaybeScanArgs(t, "readSSTs", &fo.consumeSST)
	d.MaybeScanArgs(t, "withDiff", &fo.withDiff)
	d.MaybeScanArgs(t, "withInitialScan", &fo.withInitialScan)
	k := e.startKey.key(key)
	ek := roachpb.Key(e.startKey).PrefixEnd()
	if len(endKey) > 0 {
		ek = e.startKey.key(endKey)
	}
	if len(startTs) > 0 {
		var ok bool
		fo.initialTs, ok = e.tts.getTimestamp(startTs)
		if !ok {
			if wall, err := strconv.Atoi(startTs); err == nil {
				fo.initialTs = hlc.Timestamp{WallTime: int64(wall)}
			} else {
				t.Fatalf("not found named timestamp and can't parse as wall time '%s'", startTs)
			}
		}
	}
	rf, cd := createTestFeed(t, context.Background(), e.tc.Server(server),
		roachpb.Span{Key: k, EndKey: ek}, fo)
	e.feeds = append(e.feeds, rf)
	e.captures = append(e.captures, cd)
	return []string{"ok"}
}

func handlePutKeys(t *testing.T, e *env, d *datadriven.TestData) []string {
	db := e.tc.Server(0).DB()
	readKvs(e, d.Input, func(key roachpb.Key, val string, tsName, txnName string) {
		put := db.Put
		if txnName != "" {
			require.Empty(t, tsName, "can't use transactional put and capture timestamp")
			txn, ok := e.txns[txnName]
			if !ok {
				txn.txn = db.NewTxn(context.Background(), tsName)
				e.txns[txnName] = txn
			}
			put = txn.txn.Put
		}
		require.NoError(t, put(context.Background(), key, val), "failed to put value")
		// Might not work with txn
		if tsName != "" {
			kv, err := db.Get(context.Background(), key)
			require.NoError(t, err, "failed to read written value")
			e.tts.addTs(tsName, kv.Value.Timestamp)
		}
	}, func(key roachpb.Key, endKey roachpb.Key, tsName string) {
		require.Empty(t, tsName, "can't set/save timestamp on put")
		require.NoError(t, db.DelRangeUsingTombstone(context.Background(), key, endKey))
	}, func(txnName, tsName string) {
		txn, ok := e.txns[txnName]
		require.True(t, ok, "failed to commit non existing transaction %s", txnName)
		require.NoError(t, txn.txn.Commit(context.Background()), "failed to commit txn")
		if tsName != "" {
			ts, err := txn.txn.CommitTimestamp()
			require.NoError(t, err, "failed to read transaction timestamp")
			e.tts.addTs(tsName, ts)
		}
		delete(e.txns, txnName)
	})
	return []string{"ok"}
}

var keyRe = regexp.MustCompile(`^(\s*)key=(\w+?),\s*val=(\w+)(,\s*ts=(\w+?))?(,\s*txn=(\w+?))?$`)
var rangeRe = regexp.MustCompile(`^(\s*)key=(\w+?),\s*endKey=(\w+?)(,\s*ts=(\w+))?$`)
var commitRe = regexp.MustCompile(`^(\s*)commit\s+txn=(\w+?)(,\s*ts=(\w+?))?$`)

func readKvs(
	e *env,
	input string,
	kv func(key roachpb.Key, val string, tsName, txnName string),
	dr func(key roachpb.Key, endKey roachpb.Key, tsName string),
	commit func(txnName, tsName string),
) {
	ls := strings.Split(input, "\n")
	var submatch []string
	match := func(l string, r *regexp.Regexp) bool {
		submatch = r.FindStringSubmatch(l)
		return submatch != nil
	}
	for _, l := range ls {
		switch {
		case match(l, keyRe):
			kk := e.startKey.key(submatch[2])
			val := submatch[3]
			tsName := submatch[5]
			txn := submatch[7]
			kv(kk, val, tsName, txn)
		case match(l, rangeRe):
			kk := e.startKey.key(submatch[2])
			ekk := e.startKey.key(submatch[3])
			tsName := submatch[5]
			dr(kk, ekk, tsName)
		case match(l, commitRe):
			txn := submatch[2]
			tsName := submatch[4]
			commit(txn, tsName)
		default:
			panic(fmt.Sprintf("bad line: %s", l))
		}
	}
}

func handleAddSST(t *testing.T, e *env, d *datadriven.TestData) []string {
	var tsName string
	srv := e.tc.Server(0)
	d.MaybeScanArgs(t, "ts", &tsName)
	ts := srv.Clock().Now()
	if tsName != "" {
		e.tts.addTs(tsName, ts)
	}
	sstKVs := kvs{}
	readKvs(e, d.Input, func(key roachpb.Key, val string, tsName, txnName string) {
		require.Empty(t, txnName, "transactions can't be put into SST")
		var mvccValue storage.MVCCValue
		if val != "" {
			mvccValue = storage.MVCCValue{Value: roachpb.MakeValueFromString(val)}
		}
		v, err := storage.EncodeMVCCValue(mvccValue)
		require.NoError(t, err, "failed to serialize value")
		sstKVs = append(sstKVs, storage.MVCCKeyValue{
			Key:   storage.MVCCKey{Key: key, Timestamp: ts},
			Value: v,
		})
	}, func(key roachpb.Key, endKey roachpb.Key, tsName string) {
		v, err := storage.EncodeMVCCValue(storage.MVCCValue{})
		require.NoError(t, err, "failed to serialize value")
		sstKVs = append(sstKVs, storage.MVCCRangeKeyValue{
			RangeKey: storage.MVCCRangeKey{
				StartKey:  key,
				EndKey:    endKey,
				Timestamp: ts,
			},
			Value: v,
		})
	}, func(_, _ string) {
		t.Fatalf("transactions can't be put into SST")
	})
	db := srv.DB()
	sst, sstStart, sstEnd := storageutils.MakeSST(t, srv.ClusterSettings(), sstKVs)
	_, _, _, pErr := db.AddSSTableAtBatchTimestamp(context.Background(), sstStart, sstEnd, sst,
		false /* disallowConflicts */, false /* disallowShadowing */, hlc.Timestamp{}, nil, /* stats */
		false /* ingestAsWrites */, ts)
	require.NoError(t, pErr)
	return []string{"ok"}
}

func handleSplitRange(t *testing.T, e *env, d *datadriven.TestData) []string {
	var splitKey string
	d.ScanArgs(t, "key", &splitKey)
	k := e.startKey.key(splitKey)
	d1, d2 := e.tc.SplitRangeOrFatal(t, k)
	return []string{
		fmt.Sprintf("[%s, %s), [%s, %s)",
			e.startKey.print(d1.StartKey.AsRawKey()), e.startKey.print(d1.EndKey.AsRawKey()),
			e.startKey.print(d2.StartKey.AsRawKey()), e.startKey.print(d2.EndKey.AsRawKey())),
	}
}

func handleClearRange(t *testing.T, e *env, d *datadriven.TestData) []string {
	var key, endKey string
	d.ScanArgs(t, "startKey", &key)
	d.ScanArgs(t, "endKey", &endKey)
	var b kv.Batch
	b.AddRawRequest(&kvpb.ClearRangeRequest{
		RequestHeader: kvpb.RequestHeader{
			Key:    e.startKey.key(key),
			EndKey: e.startKey.key(endKey),
		},
	})
	if err := e.tc.Server(0).DB().Run(context.Background(), &b); err != nil {
		return []string{fmt.Sprintf("error: %s", err)}
	}
	return []string{"ok"}
}

type testFeedEvent struct {
	v        *kvpb.RangeFeedValue
	cp       *kvpb.RangeFeedCheckpoint
	frontier *hlc.Timestamp
	sst      *kvpb.RangeFeedSSTable
	sstSpan  *roachpb.Span
	delRange *kvpb.RangeFeedDeleteRange
}

type eventStream []testFeedEvent

// requirePreviousKeyValue asserts that for each key prev value is equal to
// the value recorded for the preceding version of the same key.
func (s eventStream) requirePreviousKeyValue(t *testing.T) {
	// Group keys using their pretty print. Should be okay for our case on scratch
	// ranges.
	kvs := make(map[string]*kvpb.RangeFeedValue)
	for _, e := range s {
		if e.v != nil {
			keyStr := e.v.Key.String()
			pv, hasPrev := kvs[keyStr]
			if hasPrev {
				if pv.Value.EqualTagAndData(e.v.Value) {
					require.True(t, e.v.PrevValue.EqualTagAndData(pv.PrevValue),
						"prev value is not equal on duplicate event. Expected %s, found %s", pv.PrevValue,
						e.v.PrevValue)
					continue
				}
				require.True(t, e.v.PrevValue.EqualTagAndData(pv.Value),
					"prev value is not equal on value of previous event. Expected %s, found %s", pv.Value,
					e.v.PrevValue)
			} else {
				require.False(t, e.v.PrevValue.IsPresent(), "prev value on the first key value")
			}
			kvs[keyStr] = e.v
		}
	}
}

// requireCpBelowFuture asserts that for any checkpoint in the stream, all data
// following it will have higher timestamps if their keyspans overlap with one
// of checkpoint.
func (s eventStream) requireCpBelowFuture(t *testing.T) {
	for i, e := range s {
		if e.cp != nil {
			for j := i + 1; j < len(s); j++ {
				ne := s[j]
				switch {
				case ne.v != nil:
					if e.cp.Span.ContainsKey(ne.v.Key) {
						require.True(t, e.cp.ResolvedTS.Less(ne.v.Value.Timestamp),
							"change after checkpoint at or below checkpoint timestamp")
					}
					continue
				case ne.cp != nil:
					if e.cp.Span.Overlaps(ne.cp.Span) {
						require.True(t, e.cp.ResolvedTS.LessEq(ne.cp.ResolvedTS),
							"subsequent checkpoint is below checkpoint timestamp")
					}
				}
			}
		}
	}
}

// requireAllEvents assert that stream contains full history of events for the
// key from version zero (inclusive) up to certain sequence value (excluding)
// Duplicates are allowed.
func (s eventStream) requireAllEvents(t *testing.T, key roachpb.Key, total int) {
	prev := -1
	for _, e := range s {
		if e.v != nil {
			if e.v.Key.Equal(key) {
				b, err := e.v.Value.GetBytes()
				require.NoError(t, err, "failed to get string value")
				strVal := string(b)
				if strVal == fmt.Sprintf("%05d", prev) {
					continue
				}
				require.Equal(t, fmt.Sprintf("%05d", prev+1), strVal, "next value for key %s", key)
				prev++
			}
		}
	}
	require.Equal(t, total, prev+1, "number of sequential keys")
}

type sorted struct {
	storage.MVCCRangeKey
	val interface{}
}

// Compare sorts timestamps in ascending order (first events come first),
// point keys prior to range keys.
// When used with sort, this will create a history per key which should be
// easier to comprehend.
func (s sorted) compare(o sorted) int {
	if c := s.StartKey.Compare(o.StartKey); c != 0 {
		return c
	}
	if s.Timestamp.IsEmpty() && !o.Timestamp.IsEmpty() {
		return -1
	} else if !s.Timestamp.IsEmpty() && o.Timestamp.IsEmpty() {
		return 1
	} else if c := s.Timestamp.Compare(o.Timestamp); c != 0 {
		return c
	}
	return s.EndKey.Compare(o.EndKey)
}

func (s sorted) compareByTime(o sorted) int {
	if s.Timestamp.IsEmpty() && !o.Timestamp.IsEmpty() {
		return -1
	} else if !s.Timestamp.IsEmpty() && o.Timestamp.IsEmpty() {
		return 1
	} else if c := s.Timestamp.Compare(o.Timestamp); c != 0 {
		return c
	}
	if c := s.StartKey.Compare(o.StartKey); c != 0 {
		return c
	}
	return s.EndKey.Compare(o.EndKey)
}

func (s sorted) equals(o sorted) bool {
	switch v := s.val.(type) {
	case storage.MVCCKeyValue:
		if ov, ok := o.val.(storage.MVCCKeyValue); ok {
			return v.Key.Equal(ov.Key) && bytes.Equal(v.Value, ov.Value)
		}
		return false
	case storage.MVCCRangeKeyValue:
		if ov, ok := o.val.(storage.MVCCRangeKeyValue); ok {
			return v.RangeKey.Compare(ov.RangeKey) == 0 && bytes.Equal(v.Value, ov.Value)
		}
		return false
	case sstInfo:
		if ov, ok := o.val.(sstInfo); ok {
			return v.span.Equal(ov.span) && v.writeTs.Equal(ov.writeTs) && bytes.Equal(v.data, ov.data)
		}
		return false
	case kvpb.RangeFeedValue:
		if ov, ok := o.val.(kvpb.RangeFeedEvent); ok {
			return v.Key.Equal(ov.Val.Key) && v.Value.EqualTagAndData(ov.Val.Value) && v.PrevValue.EqualTagAndData(ov.Val.PrevValue)
		}
		return false
	default:
		panic(fmt.Sprintf("unknown data event type %s", s))
	}
}

// asSortedData produces sequence of values ordered by key and timestamp.
// Consecutive equal entries are removed. For entries to be equal they must have
// the same type and both mvcc keys and values equal. For sst's metadata and
// byte contents must be equal.
// Produced slice could be used by dumpKVS function to create a human-readable
// representation.
func (s eventStream) asSortedData(byTimestamp bool) kvs {
	var data []sorted
	for _, e := range s {
		switch {
		case e.v != nil && !e.v.PrevValue.IsPresent():
			data = append(data, sorted{
				MVCCRangeKey: storage.MVCCRangeKey{
					StartKey:  e.v.Key,
					Timestamp: e.v.Timestamp(),
				},
				val: storage.MVCCKeyValue{
					Key: storage.MVCCKey{
						Key:       e.v.Key,
						Timestamp: e.v.Timestamp(),
					},
					Value: e.v.Value.RawBytes,
				},
			})
		case e.v != nil && e.v.PrevValue.IsPresent():
			data = append(data, sorted{
				MVCCRangeKey: storage.MVCCRangeKey{
					StartKey:  e.v.Key,
					Timestamp: e.v.Timestamp(),
				},
				val: *e.v,
			})
		case e.sst != nil:
			dataCopy := make([]byte, len(e.sst.Data))
			copy(dataCopy, e.sst.Data)
			data = append(data, sorted{
				MVCCRangeKey: storage.MVCCRangeKey{
					StartKey:  e.sst.Span.Key,
					Timestamp: e.sst.WriteTS,
					EndKey:    e.sst.Span.EndKey,
				},
				val: sstInfo{
					span:    e.sst.Span,
					writeTs: e.sst.WriteTS,
					data:    dataCopy,
				},
			})
		case e.delRange != nil:
			data = append(data, sorted{
				MVCCRangeKey: storage.MVCCRangeKey{
					StartKey:  e.delRange.Span.Key,
					EndKey:    e.delRange.Span.EndKey,
					Timestamp: e.delRange.Timestamp,
				},
				val: storage.MVCCRangeKeyValue{
					RangeKey: storage.MVCCRangeKey{
						StartKey:  e.delRange.Span.Key,
						EndKey:    e.delRange.Span.EndKey,
						Timestamp: e.delRange.Timestamp,
					},
				},
			})
		}
	}
	sort.Slice(data, func(i, j int) bool {
		if byTimestamp {
			return data[i].compareByTime(data[j]) < 0
		}
		return data[i].compare(data[j]) < 0
	})
	result := make(kvs, 0, len(data))
	var prev sorted
	for _, s := range data {
		if s.equals(prev) {
			continue
		}
		result = append(result, s.val)
		prev = s
	}
	return result
}

type timestamps struct {
	nameToTs map[string]hlc.Timestamp
	tsToName map[hlc.Timestamp]string
}

func (t timestamps) addTs(name string, ts hlc.Timestamp) (hlc.Timestamp, bool) {
	// If name is used, return previous ts value to use instead.
	if cts, ok := t.nameToTs[name]; ok {
		return cts, cts.Equal(ts)
	}
	// If same timestamp is registered with different name, increment logical.
	unchanged := true
	if _, ok := t.tsToName[ts]; ok {
		ts.Logical += 1
		unchanged = false
	}

	t.nameToTs[name] = ts
	t.tsToName[ts] = name
	return ts, unchanged
}

func (t timestamps) getTsName(ts hlc.Timestamp) (string, bool) {
	if n, ok := t.tsToName[ts]; ok {
		return n, true
	}
	return "?", false
}

func (t timestamps) getTimestamp(name string) (hlc.Timestamp, bool) {
	ts, ok := t.nameToTs[name]
	return ts, ok
}

type testKey roachpb.Key

func (p testKey) print(key roachpb.Key) string {
	if len(key) > len(p) && key[:len(p)].Equal(roachpb.Key(p)) {
		if key[len(key)-1] == 0 {
			// This is generated end interval from some key, turn it into key+ to
			// hint that this is past real key.
			return string(key[len(p):len(key)-1]) + "+"
		}
		return string(key[len(p):])
	}
	return key.String()
}

func (p testKey) key(k string) roachpb.Key {
	return roachpb.Key(append(p[:len(k):len(k)], k...))
}

type sstInfo struct {
	span    roachpb.Span
	writeTs hlc.Timestamp
	data    []byte
	// Needs matching range span as well
}

// Any dumpable values.
type kvs = []interface{}

// dumpKVS produced human-readable dump of provided slice of data items.
func dumpKVS(t *testing.T, data kvs, indent string, tts timestamps, kk testKey) []string {
	var ss []string
	for _, v := range data {
		switch kv := v.(type) {
		case storage.MVCCKeyValue:
			if tsn, exists := tts.getTsName(kv.Key.Timestamp); exists {
				ss = append(ss, fmt.Sprintf("%skey=%s, val=%s, ts=%s", indent, kk.print(kv.Key.Key),
					stringValue(kv.Value), tsn))
			} else {
				ss = append(ss,
					fmt.Sprintf("%skey=%s, val=%s", indent, kk.print(kv.Key.Key), stringValue(kv.Value)))
			}
		case kvpb.RangeFeedValue:
			if tsn, exists := tts.getTsName(kv.Value.Timestamp); exists {
				ss = append(ss, fmt.Sprintf("%skey=%s, val=%s, ts=%s, prev=%s", indent, kk.print(kv.Key),
					stringValue(kv.Value.RawBytes), tsn, stringValue(kv.PrevValue.RawBytes)))
			} else {
				ss = append(ss, fmt.Sprintf("%skey=%s, val=%s, prev=%s", indent, kk.print(kv.Key),
					stringValue(kv.Value.RawBytes), stringValue(kv.PrevValue.RawBytes)))
			}
		case storage.MVCCRangeKeyValue:
			ss = append(ss, fmt.Sprintf("%skey=%s, endKey=%s", indent, kk.print(kv.RangeKey.StartKey),
				kk.print(kv.RangeKey.EndKey)))
		case sstInfo:
			tsn, _ := tts.getTsName(kv.writeTs)
			ss = append(ss,
				fmt.Sprintf("%ssst span=[%s, %s), ts=%s", indent, kk.print(kv.span.Key),
					kk.print(kv.span.EndKey),
					tsn))
			ss = append(ss, dumpKVS(t, storageutils.ScanSST(t, kv.data), indent+" ", tts, kk)...)
		default:
			panic(fmt.Sprintf("unknown data element in dump: %+q", v))
		}
	}
	return ss
}

func stringValue(data []byte) string {
	val, err := storage.DecodeMVCCValue(data)
	if err != nil {
		return fmt.Sprintf("%q", err)
	}
	b, err := val.Value.GetBytes()
	if err != nil {
		return fmt.Sprintf("%q", err)
	}
	return string(b)
}

type waitCP struct {
	timeReached chan interface{}
	targetTS    hlc.Timestamp
	span        roachpb.Span
	received    *list.List
}

// add span to cp. if all desired span is covered close channel and return true
func (w *waitCP) addCP(ts hlc.Timestamp, newSpan roachpb.Span) bool {
	// Disregard timestamps that are too early.
	if ts.Less(w.targetTS) {
		return false
	}
	// Trim span to interesting part only.
	newSpan = newSpan.Intersect(w.span)
	if !newSpan.Valid() {
		return false
	}

	e := w.received.Front()
	for e != nil {
		span := e.Value.(roachpb.Span)
		if span.EqualValue(newSpan) {
			// We already saw this span after desired timestamp.
			return false
		}
		// Overlapping or adjacent spans merge into larger span superseding new one and removing
		// previous.
		if span.Overlaps(newSpan) || span.Key.Equal(newSpan.EndKey) || span.EndKey.Equal(newSpan.Key) {
			// Merge spans
			newSpan = span.Combine(newSpan)
			pe := e
			e = e.Next()
			w.received.Remove(pe)
			continue
		}
		// If new span is before existing, insert it and finish.
		if newSpan.Key.Compare(span.EndKey) < 0 {
			w.received.InsertBefore(newSpan, e)
			break
		}
		e = e.Next()
	}
	// If no more elements after, then push back to the list after all other
	// ranges. This includes the empty list case.
	if e == nil {
		w.received.PushBack(newSpan)
	}

	// Simple check that keys are chained and that end key is equal to span end.
	prev := w.span.Key
	for e := w.received.Front(); e != nil; e = e.Next() {
		span := e.Value.(roachpb.Span)
		if !prev.Equal(span.Key) {
			return false
		}
		if span.EndKey.Equal(w.span.EndKey) {
			return true
		}
	}
	return false
}

type feedData struct {
	t      *testing.T
	span   roachpb.Span
	dataMu struct {
		syncutil.Mutex
		values     eventStream
		failure    error
		failureSet bool
	}
	failedC chan interface{}

	signalCP sync.Once
	firstCP  chan interface{}

	initialScan chan interface{}

	cpMu struct {
		syncutil.Mutex
		waiters []waitCP
	}
}

func (d *feedData) onValue(_ context.Context, v *kvpb.RangeFeedValue) {
	d.dataMu.Lock()
	defer d.dataMu.Unlock()
	d.dataMu.values = append(d.dataMu.values, testFeedEvent{v: v})
	d.t.Logf("on Value: %s/%s", v.Key.String(), v.Value.String())
}

func (d *feedData) onCheckpoint(_ context.Context, cp *kvpb.RangeFeedCheckpoint) {
	d.dataMu.Lock()
	d.dataMu.values = append(d.dataMu.values, testFeedEvent{cp: cp})
	defer d.dataMu.Unlock()
	d.signalCP.Do(func() {
		close(d.firstCP)
	})
	d.t.Logf("on Checkpoint: [%s, %s) %s", cp.Span.Key.String(), cp.Span.EndKey.String(),
		cp.ResolvedTS.String())
	d.cpMu.Lock()
	defer d.cpMu.Unlock()
	for i, w := range d.cpMu.waiters {
		if !w.targetTS.IsEmpty() {
			if w.addCP(cp.ResolvedTS, cp.Span) {
				close(w.timeReached)
				d.cpMu.waiters[i] = waitCP{}
			}
		}
	}
}

func (d *feedData) onFrontierAdvance(_ context.Context, f hlc.Timestamp) {
	d.dataMu.Lock()
	defer d.dataMu.Unlock()
	d.dataMu.values = append(d.dataMu.values, testFeedEvent{frontier: &f})
	d.t.Logf("on Frontier: %s", f.String())
}

func (d *feedData) onSST(_ context.Context, sst *kvpb.RangeFeedSSTable, span roachpb.Span) {
	d.dataMu.Lock()
	defer d.dataMu.Unlock()
	d.dataMu.values = append(d.dataMu.values, testFeedEvent{sst: sst, sstSpan: &span})
	d.t.Logf("on SST in: %s", span.String())
}

func (d *feedData) onDeleteRange(_ context.Context, dr *kvpb.RangeFeedDeleteRange) {
	d.dataMu.Lock()
	defer d.dataMu.Unlock()
	d.dataMu.values = append(d.dataMu.values, testFeedEvent{delRange: dr})
	d.t.Logf("on delete range: %s", dr.Span.String())
}

func (d *feedData) onInternalError(_ context.Context, err error) {
	d.dataMu.Lock()
	defer d.dataMu.Unlock()
	d.dataMu.failure = err
	close(d.failedC)
	d.t.Logf("on internal error: %s", err)
}

func (d *feedData) onInitialScan(_ context.Context) {
	close(d.initialScan)
}

// NB: frontier update happens after checkpoint update so there's no guarantee
// that frontier is up to date at the time returned channel is closed.
func (d *feedData) waitCheckpoint(ts hlc.Timestamp, span roachpb.Span) <-chan interface{} {
	d.cpMu.Lock()
	defer d.cpMu.Unlock()
	if span.Key == nil {
		span.Key = d.span.Key
	}
	if span.EndKey == nil {
		span.EndKey = d.span.EndKey
	}
	w := waitCP{
		timeReached: make(chan interface{}),
		targetTS:    ts,
		span:        span,
		received:    list.New(),
	}
	for i, oldW := range d.cpMu.waiters {
		if oldW.targetTS.IsEmpty() {
			d.cpMu.waiters[i] = w
			return w.timeReached
		}
	}
	d.cpMu.waiters = append(d.cpMu.waiters, w)
	return w.timeReached
}

func (d *feedData) waitError() <-chan interface{} {
	return d.failedC
}

func (d *feedData) err() error {
	d.dataMu.Lock()
	defer d.dataMu.Unlock()
	return d.dataMu.failure
}

func (d *feedData) waitForInitialScan() <-chan interface{} {
	return d.initialScan
}

func (d *feedData) values() eventStream {
	d.dataMu.Lock()
	defer d.dataMu.Unlock()
	return d.dataMu.values
}

type feedOpts struct {
	consumeSST      bool
	withDiff        bool
	withInitialScan bool
	initialTs       hlc.Timestamp
}

func createTestFeed(
	t *testing.T,
	ctx context.Context,
	s serverutils.TestServerInterface,
	span roachpb.Span,
	o feedOpts,
) (*crangefeed.RangeFeed, *feedData) {
	db0 := s.DB()

	fd := &feedData{
		t:           t,
		span:        span,
		firstCP:     make(chan interface{}),
		failedC:     make(chan interface{}),
		initialScan: make(chan interface{}),
	}

	rff, err := crangefeed.NewFactory(s.Stopper(), db0, s.ClusterSettings(), nil)
	require.NoError(t, err, "failed to create client rangefeed factory")

	opts := []crangefeed.Option{
		crangefeed.WithOnCheckpoint(fd.onCheckpoint),
		crangefeed.WithOnFrontierAdvance(fd.onFrontierAdvance),
		crangefeed.WithDiff(o.withDiff),
		crangefeed.WithOnDeleteRange(fd.onDeleteRange),
		crangefeed.WithOnInternalError(fd.onInternalError),
	}
	if o.consumeSST {
		opts = append(opts, crangefeed.WithOnSSTable(fd.onSST))
	}
	if o.withInitialScan {
		opts = append(opts, crangefeed.WithInitialScan(fd.onInitialScan))
	}
	rf, err := rff.RangeFeed(ctx, "nice", []roachpb.Span{span}, o.initialTs, fd.onValue, opts...)
	require.NoError(t, err, "failed to start rangefeed")
	return rf, fd
}

func TestWaitCP(t *testing.T) {

	sp := func(s, e string) roachpb.Span {
		return roachpb.Span{Key: roachpb.Key(s), EndKey: roachpb.Key(e)}
	}
	ts := func(wall int64) hlc.Timestamp {
		return hlc.Timestamp{WallTime: wall}
	}

	for i, d := range []struct {
		spans   []roachpb.Span
		tss     []hlc.Timestamp
		outcome bool
	}{
		{
			spans:   []roachpb.Span{sp("a", "z")},
			tss:     []hlc.Timestamp{ts(1)},
			outcome: false,
		},
		{
			spans:   []roachpb.Span{sp("a", "z")},
			tss:     []hlc.Timestamp{ts(11)},
			outcome: true,
		},
		{
			spans:   []roachpb.Span{sp("a", "k"), sp("k", "z")},
			tss:     []hlc.Timestamp{ts(11), ts(11)},
			outcome: true,
		},
		{
			spans:   []roachpb.Span{sp("k", "z"), sp("a", "k")},
			tss:     []hlc.Timestamp{ts(11), ts(11)},
			outcome: true,
		},
		{
			spans:   []roachpb.Span{sp("a", "h"), sp("o", "z"), sp("g", "p")},
			tss:     []hlc.Timestamp{ts(11), ts(11), ts(11)},
			outcome: true,
		},
		{
			spans:   []roachpb.Span{sp("a", "h"), sp("j", "z")},
			tss:     []hlc.Timestamp{ts(11), ts(11)},
			outcome: false,
		},
	} {
		t.Run(fmt.Sprintf("%d", i), func(t *testing.T) {
			w := waitCP{
				span:     roachpb.Span{Key: roachpb.Key("f"), EndKey: roachpb.Key("r")},
				received: list.New(),
				targetTS: hlc.Timestamp{WallTime: 10},
			}
			for i := range d.spans {
				r := w.addCP(d.tss[i], d.spans[i])
				if i < len(d.tss)-1 {
					require.False(t, r, "intermediate result on step %d", i)
				} else {
					require.Equal(t, d.outcome, r, "final result")
				}
			}
		})
	}
}
