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
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv"
	crangefeed "github.com/cockroachdb/cockroach/pkg/kv/kvclient/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/rangefeed"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security/securityassets"
	"github.com/cockroachdb/cockroach/pkg/security/securitytest"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/storageutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/container/list"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
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

func TestRangeFeeds(t *testing.T) {
	defer leaktest.AfterTest(t)()
	skip.UnderStress(t, "test uses test cluster and flakes under stress because of timeout checks")
	defer log.Scope(t).Close(t)

	datadriven.Walk(t, datapathutils.TestDataPath(t), func(t *testing.T, path string) {
		e := newEnv(t)
		defer e.close()
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) (output string) {
			e.setDefaultStepOptions()
			return handleCommandBatch(t, e, d)
		})
	})
}

type command func(t *testing.T, e *env, d *datadriven.TestData)

var commands = map[string]command{
	"options":     handleOptions,
	"put":         handlePutKey,
	"commit":      handleCommit,
	"add-sst":     handleAddSST,
	"del-range":   handleDelRange,
	"clear-range": handleClearRange,
	"split-range": handleSplitRange,
	"create-feed": handleCreateFeed,
}

var linePosRe = regexp.MustCompile(`^(.*:)(\d+)$`)

func handleCommandBatch(t *testing.T, e *env, d *datadriven.TestData) string {
	// Preserve current command state so not to surprise datadriven lib.
	defer func(pos, cmd string, cmdArgs []datadriven.CmdArg, input string) {
		d.Pos = pos
		d.Cmd = cmd
		d.CmdArgs = cmdArgs
		d.Input = input
	}(d.Pos, d.Cmd, d.CmdArgs, d.Input)

	remaining := strings.Split(d.Input, "\n")
	lastInputLine := len(remaining)
	prefix := d.Pos + "+"
	if m := linePosRe.FindStringSubmatch(d.Pos); m != nil {
		prefix = m[1]
		if p, err := strconv.Atoi(m[2]); err == nil {
			lastInputLine += p
		}
	}
	nextCmd := func() (string, string) {
		for ; len(remaining) > 0; remaining = remaining[1:] {
			if trimmed := strings.TrimSpace(remaining[0]); len(trimmed) > 0 && trimmed[0] != '#' {
				res := remaining[0]
				remaining = remaining[1:]
				return res, fmt.Sprintf("%s%d", prefix, lastInputLine-len(remaining))
			}
		}
		return "", ""
	}
	nextInput := func() string {
		for i := range remaining {
			if len(remaining[i]) > 0 && remaining[i][0] != ' ' {
				res := strings.Join(remaining[0:i], "\n")
				remaining = remaining[i:]
				return res
			}
		}
		res := strings.Join(remaining, "\n")
		remaining = nil
		return res
	}

	for d.Cmd != "" {
		d.Input = nextInput()
		f := commands[d.Cmd]
		if f == nil {
			t.Fatalf("unknown command %s at %s", d.Cmd, d.Pos)
		}
		f(t, e, d)
		line, cmdPos := nextCmd()
		cmd, args, err := datadriven.ParseLine(line)
		if err != nil {
			t.Fatalf("%s: %v", cmdPos, err)
		}
		d.Cmd = cmd
		d.CmdArgs = args
		d.Pos = cmdPos
	}

	output := waitAllFeeds(t, e)

	return strings.Join(output, "\n")
}

// waitAllFeeds checks that rangefeeds get data at least up to now and then
// dumps captured content. If timeout happens, no dump is produced and events
// are kept for the next wait attempt.
func waitAllFeeds(t *testing.T, e *env) []string {
	now := e.tc.SystemLayer(0).Clock().Now()
	var rs []string
	ids := make([]string, 0, len(e.feeds))
	for i := range e.feeds {
		ids = append(ids, i)
	}
	sort.Strings(ids)
	for _, id := range ids {
		data := e.feeds[id].capture
		prefix := ""
		if len(e.feeds) > 1 {
			prefix = fmt.Sprintf("feed %s: ", id)
		}
		select {
		case <-data.waitCheckpoint(now, data.span):
			stream := data.takeValues().asSortedData(e.sortByTime)
			rs = append(rs, dumpKVS(t, stream, prefix, e.tts, e.startKey)...)
		case <-time.After(e.checkpointTimeout):
			rs = append(rs, prefix+"timeout")
		case <-data.waitError():
			stream := data.takeValues().asSortedData(e.sortByTime)
			rs = append(rs, dumpKVS(t, stream, prefix, e.tts, e.startKey)...)
			rs = append(rs, fmt.Sprintf("%s%q", prefix, data.err()))
		}
	}
	return rs
}

func handleOptions(t *testing.T, e *env, d *datadriven.TestData) {
	e.sortByTime = d.HasArg("sortByTime")
	var timeoutStr string
	d.MaybeScanArgs(t, "timeout", &timeoutStr)
	if timeoutStr != "" {
		var err error
		e.checkpointTimeout, err = time.ParseDuration(timeoutStr)
		require.NoError(t, err, "invalid duration value '%s'", timeoutStr)
	}
}

func handlePutKey(t *testing.T, e *env, d *datadriven.TestData) {
	db := e.tc.SystemLayer(0).DB()
	var key, val, tsName, txnName string
	d.ScanArgs(t, "k", &key)
	d.ScanArgs(t, "v", &val)
	d.MaybeScanArgs(t, "ts", &tsName)
	d.MaybeScanArgs(t, "txn", &txnName)
	kk := e.startKey.key(key)
	put := db.Put
	if txnName != "" {
		require.Empty(t, tsName, "can't use transactional put and capture timestamp")
		txn, ok := e.txns[txnName]
		if !ok {
			txn = db.NewTxn(context.Background(), tsName)
			e.txns[txnName] = txn
		}
		put = txn.Put
	}
	require.NoError(t, put(context.Background(), kk, val), "failed to put value")
	if txnName == "" {
		kv, err := db.Get(context.Background(), kk)
		require.NoError(t, err, "failed to read written value")
		if tsName != "" {
			e.tts.addTs(tsName, kv.Value.Timestamp)
		} else {
			e.tts.addNextTs(kv.Value.Timestamp)
		}
	}
}

func handleCommit(t *testing.T, e *env, d *datadriven.TestData) {
	var txnName, tsName string
	d.MaybeScanArgs(t, "txn", &txnName)
	d.MaybeScanArgs(t, "ts", &tsName)
	txn, ok := e.txns[txnName]
	require.True(t, ok, "failed to commit non existing transaction %s", txnName)
	require.NoError(t, txn.Commit(context.Background()), "failed to commit txn")
	ts, err := txn.CommitTimestamp()
	require.NoError(t, err, "failed to read transaction timestamp")
	if tsName != "" {
		e.tts.addTs(tsName, ts)
	} else {
		e.tts.addNextTs(ts)
	}
	delete(e.txns, txnName)
}

func handleDelRange(t *testing.T, e *env, d *datadriven.TestData) {
	db := e.tc.SystemLayer(0).DB()
	var key, endKey string
	d.ScanArgs(t, "start", &key)
	d.ScanArgs(t, "end", &endKey)
	kk := e.startKey.key(key)
	ekk := e.startKey.key(endKey)
	require.NoError(t, db.DelRangeUsingTombstone(context.Background(), kk, ekk),
		"failed to del range")
}

func handleAddSST(t *testing.T, e *env, d *datadriven.TestData) {
	var tsName string
	srv := e.tc.Server(0)
	d.MaybeScanArgs(t, "ts", &tsName)
	ts := srv.Clock().Now()
	if tsName != "" {
		e.tts.addTs(tsName, ts)
	}
	sstKVs := kvs{}
	readKvs(t, e, d.Input, func(key roachpb.Key, val string, tsName string) {
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
	})
	db := srv.DB()
	sst, sstStart, sstEnd := storageutils.MakeSST(t, srv.ClusterSettings(), sstKVs)
	_, _, _, pErr := db.AddSSTableAtBatchTimestamp(context.Background(), sstStart, sstEnd, sst,
		false /* disallowConflicts */, false /* disallowShadowing */, hlc.Timestamp{}, nil, /* stats */
		false /* ingestAsWrites */, ts)
	require.NoError(t, pErr)
}

var keyRe = regexp.MustCompile(`^(\s*)k=(\w+?)\s+v=(\w+?)(\s+ts=(\w+?))?$`)
var rangeRe = regexp.MustCompile(`^(\s*)start=(\w+?)\s+end=(\w+?)(\s+ts=(\w+))?$`)

func readKvs(
	t *testing.T,
	e *env,
	input string,
	kv func(key roachpb.Key, val string, tsName string),
	dr func(key roachpb.Key, endKey roachpb.Key, tsName string),
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
			kv(kk, val, tsName)
		case match(l, rangeRe):
			kk := e.startKey.key(submatch[2])
			ekk := e.startKey.key(submatch[3])
			tsName := submatch[5]
			dr(kk, ekk, tsName)
		default:
			t.Fatalf("failed to parse line: %s", l)
		}
	}
}

func handleClearRange(t *testing.T, e *env, d *datadriven.TestData) {
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
	err := e.tc.Server(0).DB().Run(context.Background(), &b)
	require.NoError(t, err, "failed to clear range")
}

func handleSplitRange(t *testing.T, e *env, d *datadriven.TestData) {
	var splitKey string
	d.ScanArgs(t, "key", &splitKey)
	k := e.startKey.key(splitKey)
	e.tc.SplitRangeOrFatal(t, k)
}

func handleCreateFeed(t *testing.T, e *env, d *datadriven.TestData) {
	var (
		server               int
		key, endKey, startTs string
		// If we don't set initial ts, then feed will start after request completes
		// and we will skip first events. We can always explicitly set time to 0
		// from the test itself if we need to test that.
		fo = feedOpts{initialTs: hlc.Timestamp{WallTime: 1}}
	)
	// Always allocate feed ids to avoid any ambiguity.
	feedID := string(e.nextFeedId)
	e.nextFeedId++
	d.MaybeScanArgs(t, "id", &feedID)
	d.MaybeScanArgs(t, "server", &server)
	d.MaybeScanArgs(t, "startKey", &key)
	d.MaybeScanArgs(t, "endKey", &endKey)
	d.MaybeScanArgs(t, "startTs", &startTs)
	d.MaybeScanArgs(t, "withDiff", &fo.withDiff)
	d.MaybeScanArgs(t, "readSSTs", &fo.consumeSST)
	_, ok := e.feeds[feedID]
	require.False(t, ok, "feed with id '%s' already registered", feedID)
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
	rf, cd := createTestFeed(t, context.Background(), e.tc.SystemLayer(server),
		e.tc.Server(0).Stopper(), roachpb.Span{Key: k, EndKey: ek}, fo)
	e.feeds[feedID] = feedAndData{feed: rf, capture: cd}
}

func createTestFeed(
	t *testing.T,
	ctx context.Context,
	s serverutils.ApplicationLayerInterface,
	stopper *stop.Stopper,
	span roachpb.Span,
	o feedOpts,
) (*crangefeed.RangeFeed, *feedData) {

	fd := &feedData{
		t:       t,
		span:    span,
		firstCP: make(chan interface{}),
		failedC: make(chan interface{}),
	}

	rff, err := crangefeed.NewFactory(stopper, s.DB(), s.ClusterSettings(), nil)
	require.NoError(t, err, "failed to create client rangefeed factory")

	opts := []crangefeed.Option{
		crangefeed.WithOnCheckpoint(fd.onCheckpoint),
		crangefeed.WithDiff(o.withDiff),
		crangefeed.WithOnDeleteRange(fd.onDeleteRange),
		crangefeed.WithOnInternalError(fd.onInternalError),
	}
	if o.consumeSST {
		opts = append(opts, crangefeed.WithOnSSTable(fd.onSST))
	}
	rf, err := rff.RangeFeed(ctx, "nice", []roachpb.Span{span}, o.initialTs, fd.onValue, opts...)
	require.NoError(t, err, "failed to start rangefeed")
	return rf, fd
}

type feedOpts struct {
	withDiff   bool
	initialTs  hlc.Timestamp
	consumeSST bool
}

type feedAndData struct {
	feed    *crangefeed.RangeFeed
	capture *feedData
}

type env struct {
	// test cluster
	tc *testcluster.TestCluster

	// range feeds
	nextFeedId byte
	feeds      map[string]feedAndData

	// named timestamps
	tts  timestamps
	txns map[string]*kv.Txn

	// work keys
	startKey testKey

	// runtime configurable output options
	sortByTime        bool
	checkpointTimeout time.Duration
}

func newEnv(t *testing.T) *env {
	ctx := context.Background()
	s := cluster.MakeClusterSettings()
	closedts.TargetDuration.Override(ctx, &s.SV, 100*time.Millisecond)
	kvserver.RangefeedEnabled.Override(ctx, &s.SV, true)
	kvserver.RangeFeedUseScheduler.Override(ctx, &s.SV, true)
	kvserver.RangeFeedRefreshInterval.Override(ctx, &s.SV, 20*time.Millisecond)
	rangefeed.DefaultPushTxnsInterval = 60 * time.Hour
	tc := testcluster.NewTestCluster(t, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{
			Settings:          s,
			DefaultTestTenant: base.TestIsSpecificToStorageLayerAndNeedsASystemTenant,
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

	sr := append(tc.SystemLayer(0).Codec().TenantPrefix(), keys.ScratchRangeMin...)
	tc.ScratchRange(t)
	return &env{
		tc:         tc,
		nextFeedId: 'A',
		feeds:      make(map[string]feedAndData),
		tts: timestamps{
			nameToTs: make(map[string]hlc.Timestamp),
			tsToName: make(map[hlc.Timestamp]string),
		},
		txns:     make(map[string]*kv.Txn),
		startKey: testKey(sr),
	}
}

func (e *env) close() {
	for _, f := range e.feeds {
		f.feed.Close()
	}
	e.tc.Stopper().Stop(context.Background())
}

func (e *env) setDefaultStepOptions() {
	e.sortByTime = false
	e.checkpointTimeout = 30 * time.Second
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

func (t timestamps) addNextTs(ts hlc.Timestamp) {
	name := fmt.Sprintf("ts%d", len(t.tsToName)+1)
	t.addTs(name, ts)
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

	cpMu struct {
		syncutil.Mutex
		waiters []waitCP
	}
}

func (d *feedData) onValue(_ context.Context, v *kvpb.RangeFeedValue) {
	d.dataMu.Lock()
	defer d.dataMu.Unlock()
	d.dataMu.values = append(d.dataMu.values, testFeedEvent{v: v})
	d.t.Logf("on Value: %s/'%s'", v.Key.String(), stringValue(v.Value.RawBytes))
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
				close(w.timeReachedC)
				d.cpMu.waiters[i] = waitCP{}
			}
		}
	}
}

func (d *feedData) onInternalError(_ context.Context, err error) {
	d.dataMu.Lock()
	defer d.dataMu.Unlock()
	d.dataMu.failure = err
	close(d.failedC)
	d.t.Logf("on internal error: %s", err)
}

func (d *feedData) onDeleteRange(_ context.Context, dr *kvpb.RangeFeedDeleteRange) {
	d.dataMu.Lock()
	defer d.dataMu.Unlock()
	d.dataMu.values = append(d.dataMu.values, testFeedEvent{delRange: dr})
	d.t.Logf("on delete range: %s", dr.Span.String())
}

func (d *feedData) onSST(_ context.Context, sst *kvpb.RangeFeedSSTable, span roachpb.Span) {
	d.dataMu.Lock()
	defer d.dataMu.Unlock()
	d.dataMu.values = append(d.dataMu.values, testFeedEvent{sst: sst, sstSpan: &span})
	d.t.Logf("on SST in: %s", span.String())
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
		timeReachedC: make(chan interface{}),
		targetTS:     ts,
		span:         span,
		received:     list.New[roachpb.Span](),
	}
	for i, oldW := range d.cpMu.waiters {
		if oldW.targetTS.IsEmpty() {
			d.cpMu.waiters[i] = w
			return w.timeReachedC
		}
	}
	d.cpMu.waiters = append(d.cpMu.waiters, w)
	return w.timeReachedC
}

func (d *feedData) waitError() <-chan interface{} {
	return d.failedC
}

func (d *feedData) err() error {
	d.dataMu.Lock()
	defer d.dataMu.Unlock()
	return d.dataMu.failure
}

func (d *feedData) takeValues() (s eventStream) {
	d.dataMu.Lock()
	defer d.dataMu.Unlock()
	s, d.dataMu.values = d.dataMu.values, nil
	return s
}

type testFeedEvent struct {
	v        *kvpb.RangeFeedValue
	cp       *kvpb.RangeFeedCheckpoint
	delRange *kvpb.RangeFeedDeleteRange
	sst      *kvpb.RangeFeedSSTable
	sstSpan  *roachpb.Span
}

type eventStream []testFeedEvent

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
		case e.v != nil:
			data = append(data, sorted{
				MVCCRangeKey: storage.MVCCRangeKey{
					StartKey:  e.v.Key,
					Timestamp: e.v.Timestamp(),
				},
				val: *e.v,
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
		}
	}
	if byTimestamp {
		sort.Sort(sortedByTime(data))
	} else {
		sort.Sort(sortedByKey(data))
	}
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

type sstInfo struct {
	span    roachpb.Span
	writeTs hlc.Timestamp
	data    []byte
}

// sorted is a helper type that allows test to present and transform data in
// different order.
type sorted struct {
	storage.MVCCRangeKey
	val interface{}
}

func (s sorted) equals(o sorted) bool {
	switch v := s.val.(type) {
	case kvpb.RangeFeedValue:
		if ov, ok := o.val.(kvpb.RangeFeedValue); ok {
			if !v.Key.Equal(ov.Key) || !v.Value.EqualTagAndData(ov.Value) {
				return false
			}
			if v.PrevValue.IsPresent() != ov.PrevValue.IsPresent() {
				return false
			}
			return !v.PrevValue.IsPresent() || v.PrevValue.EqualTagAndData(ov.PrevValue)
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
	default:
		panic(fmt.Sprintf("unknown data event type %T, %+q", s, s))
	}
}

type sortedByKey []sorted

var _ sort.Interface = (sortedByKey)(nil)

func (s sortedByKey) Len() int      { return len(s) }
func (s sortedByKey) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s sortedByKey) Less(i, j int) bool {
	o1, o2 := s[i], s[j]
	if c := o1.StartKey.Compare(o2.StartKey); c != 0 {
		return c < 0
	}
	if o1.Timestamp.IsEmpty() && !o2.Timestamp.IsEmpty() {
		return true
	} else if !o1.Timestamp.IsEmpty() && o2.Timestamp.IsEmpty() {
		return false
	} else if c := o1.Timestamp.Compare(o2.Timestamp); c != 0 {
		return c < 0
	}
	return o1.EndKey.Compare(o2.EndKey) < 0
}

type sortedByTime []sorted

var _ sort.Interface = (sortedByTime)(nil)

func (s sortedByTime) Len() int      { return len(s) }
func (s sortedByTime) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s sortedByTime) Less(i, j int) bool {
	o1, o2 := s[i], s[j]
	if o1.Timestamp.IsEmpty() && !o2.Timestamp.IsEmpty() {
		return true
	} else if !o1.Timestamp.IsEmpty() && o2.Timestamp.IsEmpty() {
		return false
	} else if c := o1.Timestamp.Compare(o2.Timestamp); c != 0 {
		return c < 0
	}
	if c := o1.StartKey.Compare(o2.StartKey); c != 0 {
		return c < 0
	}
	return o1.EndKey.Compare(o2.EndKey) < 0
}

// kvs is any data that could be written into test output for diffing.
type kvs = []interface{}

// dumpKVS produced human-readable dump of provided slice of data items.
func dumpKVS(t *testing.T, data kvs, indent string, tts timestamps, kk testKey) []string {
	var ss []string
	for _, v := range data {
		switch kv := v.(type) {
		case storage.MVCCKeyValue:
			val := fmt.Sprintf("%skey=%s, val=%s", indent, kk.print(kv.Key.Key),
				stringValue(kv.Value))
			if tsn, exists := tts.getTsName(kv.Key.Timestamp); exists {
				val += fmt.Sprintf(", ts=%s", tsn)
			}
			ss = append(ss, val)
		case kvpb.RangeFeedValue:
			val := fmt.Sprintf("%skey=%s, val=%s", indent, kk.print(kv.Key),
				stringValue(kv.Value.RawBytes))
			if kv.PrevValue.IsPresent() {
				val += fmt.Sprintf(", prev=%s", stringValue(kv.PrevValue.RawBytes))
			}
			if tsn, exists := tts.getTsName(kv.Value.Timestamp); exists {
				val += fmt.Sprintf(", ts=%s", tsn)
			}
			ss = append(ss, val)
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
			panic(fmt.Sprintf("unknown data element in dump: %T, %+q", v, v))
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
	timeReachedC chan interface{}
	targetTS     hlc.Timestamp
	span         roachpb.Span
	received     *list.List[roachpb.Span]
}

// add span to wait frontier. returns true if span is fully covered by
// checkpoints at or higher than targetTS.
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
		span := e.Value
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
		span := e.Value
		if !prev.Equal(span.Key) {
			return false
		}
		if span.EndKey.Equal(w.span.EndKey) {
			return true
		}
	}
	return false
}

func TestWaitCP(t *testing.T) {
	defer leaktest.AfterTest(t)()

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
				targetTS: hlc.Timestamp{WallTime: 10},
				received: list.New[roachpb.Span](),
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
