// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/storage/meta"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

func TestCheckSSTConflictsMaxIntents(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	keys := []string{"aa", "bb", "cc", "dd"}
	intents := []string{"a", "b", "c"}
	start, end := "a", "z"

	testcases := []struct {
		maxIntents    int64
		expectIntents []string
	}{
		{maxIntents: -1, expectIntents: []string{"a"}},
		{maxIntents: 0, expectIntents: []string{"a"}},
		{maxIntents: 1, expectIntents: []string{"a"}},
		{maxIntents: 2, expectIntents: []string{"a", "b"}},
		{maxIntents: 3, expectIntents: []string{"a", "b", "c"}},
		{maxIntents: 4, expectIntents: []string{"a", "b", "c"}},
	}

	// Create SST with keys equal to intents at txn2TS.
	cs := cluster.MakeTestingClusterSettings()
	var sstFile bytes.Buffer
	sstWriter := MakeBackupSSTWriter(context.Background(), cs, &sstFile)
	defer sstWriter.Close()
	for _, k := range intents {
		key := MVCCKey{Key: roachpb.Key(k), Timestamp: txn2TS}
		value := roachpb.Value{}
		value.SetString("sst")
		value.InitChecksum(key.Key)
		require.NoError(t, sstWriter.Put(key, value.RawBytes))
	}
	require.NoError(t, sstWriter.Finish())
	sstWriter.Close()

	ctx := context.Background()
	engine, err := Open(context.Background(), InMemory(), cs, MaxSize(1<<20))
	if err != nil {
		t.Fatal(err)
	}
	defer engine.Close()

	// Write some committed keys and intents at txn1TS.
	batch := engine.NewBatch()
	for _, key := range keys {
		mvccKey := MVCCKey{Key: roachpb.Key(key), Timestamp: txn1TS}
		mvccValue := MVCCValue{Value: roachpb.MakeValueFromString("value")}
		require.NoError(t, batch.PutMVCC(mvccKey, mvccValue))
	}
	for _, key := range intents {
		require.NoError(t, MVCCPut(ctx, batch, nil, roachpb.Key(key), txn1TS, hlc.ClockTimestamp{}, roachpb.MakeValueFromString("intent"), txn1))
	}
	require.NoError(t, batch.Commit(true))
	batch.Close()
	require.NoError(t, engine.Flush())

	for _, tc := range testcases {
		t.Run(fmt.Sprintf("maxIntents=%d", tc.maxIntents), func(t *testing.T) {
			for _, usePrefixSeek := range []bool{false, true} {
				t.Run(fmt.Sprintf("usePrefixSeek=%v", usePrefixSeek), func(t *testing.T) {
					// Provoke and check WriteIntentErrors.
					startKey, endKey := MVCCKey{Key: roachpb.Key(start)}, MVCCKey{Key: roachpb.Key(end)}
					_, err := CheckSSTConflicts(ctx, sstFile.Bytes(), engine, startKey, endKey, startKey.Key, endKey.Key.Next(),
						false /*disallowShadowing*/, hlc.Timestamp{} /*disallowShadowingBelow*/, hlc.Timestamp{} /* sstReqTS */, tc.maxIntents, usePrefixSeek)
					require.Error(t, err)
					writeIntentErr := &kvpb.WriteIntentError{}
					require.ErrorAs(t, err, &writeIntentErr)

					actual := []string{}
					for _, i := range writeIntentErr.Intents {
						actual = append(actual, string(i.Key))
					}
					require.Equal(t, tc.expectIntents, actual)
				})
			}
		})
	}
}

func BenchmarkUpdateSSTTimestamps(b *testing.B) {
	defer log.Scope(b).Close(b)
	skip.UnderShort(b)

	ctx := context.Background()

	for _, numKeys := range []int{1, 10, 100, 1000, 10000, 100000} {
		b.Run(fmt.Sprintf("numKeys=%d", numKeys), func(b *testing.B) {
			for _, concurrency := range []int{0, 1, 2, 4, 8} { // 0 uses naïve read/write loop
				b.Run(fmt.Sprintf("concurrency=%d", concurrency), func(b *testing.B) {
					runUpdateSSTTimestamps(ctx, b, numKeys, concurrency)
				})
			}
		})
	}
}

func runUpdateSSTTimestamps(ctx context.Context, b *testing.B, numKeys int, concurrency int) {
	const valueSize = 8

	r := rand.New(rand.NewSource(7))
	st := cluster.MakeTestingClusterSettings()
	sstFile := &MemObject{}
	writer := MakeIngestionSSTWriter(ctx, st, sstFile)
	defer writer.Close()

	sstTimestamp := hlc.MinTimestamp
	reqTimestamp := hlc.Timestamp{WallTime: 1634899098417970999, Logical: 9}

	key := make([]byte, 8)
	value := make([]byte, valueSize)
	for i := 0; i < numKeys; i++ {
		binary.BigEndian.PutUint64(key, uint64(i))
		r.Read(value)

		var mvccValue MVCCValue
		mvccValue.Value.SetBytes(value)
		mvccValue.Value.InitChecksum(key)

		if err := writer.PutMVCC(MVCCKey{Key: key, Timestamp: sstTimestamp}, mvccValue); err != nil {
			require.NoError(b, err) // for performance
		}
	}
	require.NoError(b, writer.Finish())

	b.SetBytes(int64(numKeys * (len(key) + len(value))))
	b.ResetTimer()

	var res []byte
	for i := 0; i < b.N; i++ {
		var ms enginepb.MVCCStats
		var err error
		res, _, err = UpdateSSTTimestamps(
			ctx, st, sstFile.Bytes(), sstTimestamp, reqTimestamp, concurrency, &ms)
		if err != nil {
			require.NoError(b, err) // for performance
		}
	}
	_ = res
}

func TestCheckSSTConflictsRandomized(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	const numOperations = 10_000
	ctx := context.Background()
	prng, seed := randutil.NewPseudoRand()
	t.Logf("Using seed %d; to re-run set COCKROACH_RANDOM_SEED=%d", seed, seed)

	// Generate a random set of operations.
	g := &sstOpGenerator{
		maxKeyLen:        6,
		maxValLen:        10,
		tsLow:            randutil.RandInt63InRange(prng, 0, 100),
		tsHigh:           randutil.RandInt63InRange(prng, 100, 200),
		pendingPointKeys: map[string][]int64{},
	}
	ops := meta.Generate(prng, numOperations,
		meta.Weighted([]meta.ItemWeight[func(*rand.Rand) meta.Op[*sstTestState]]{
			{Weight: 100, Item: g.generateMVCCPut},
			{Weight: 30, Item: g.generateMVCCDel},
			{Weight: 5, Item: g.generateMVCCDelRangeTombstone},
			{Weight: 10, Item: g.generateCheckSSTConflicts},
			{Weight: 10, Item: g.generateAddSSTable},
			{Weight: 10, Item: g.generateComputeStats},
		}))

	eng, err := Open(ctx, InMemory(), cluster.MakeTestingClusterSettings())
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()
	s := &sstTestState{engine: eng}
	meta.RunOne[*sstTestState](t, s, ops)
}

type sstTestState struct {
	engine      Engine
	bufferedOps sstWriterOps
}

// sstWriterOp is an operation that writes to a sstable writer. The
// CheckSSTConflicts randomized test buffers these operations and sorts them by
// key before applying them to a sstable writer.
type sstWriterOp interface {
	fmt.Stringer
	sortKey() MVCCKey
	apply(*SSTWriter) error
}

// sstWriterOps implements sort.Interface, sorting ops by their sort key. It's
// used to order ops before applying them to sstable writer which requires
// writes to be ordered.
type sstWriterOps []sstWriterOp

func (s sstWriterOps) Len() int           { return len(s) }
func (s sstWriterOps) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s sstWriterOps) Less(i, j int) bool { return s[i].sortKey().Compare(s[j].sortKey()) < 0 }

type sstOpGenerator struct {
	maxKeyLen int
	maxValLen int
	// rangeStart is the start key of the current KV range that the built
	// sstable is targeting. It's a byte in a-z, except if unset in which case
	// it's the zero byte.
	rangeStart byte
	// ts{Low,High} hold the inclusive (low) and exclusive (high) timestamp
	// bounds for timestamps that should be generated.
	tsLow, tsHigh int64
	// pendingPointKeys contains a map from MVCC user key to all the timestamp
	// WallTimes at which the key has been written since the last AddSSTable.
	// It's used to prevent writing an exactly duplicative key (same user at
	// same timestamp), which is porohibited.
	pendingPointKeys map[string][]int64
	pendingRangeKeys []MVCCRangeKey
}

func (g *sstOpGenerator) generateTS(rng *rand.Rand) hlc.Timestamp {
	g.tsLow += rng.Int63n(3)
	g.tsHigh += rng.Int63n(3)
	if g.tsHigh <= g.tsLow {
		g.tsHigh = g.tsLow + 1
	}
	return hlc.Timestamp{WallTime: randutil.RandInt63InRange(rng, g.tsLow, g.tsHigh)}
}

// generateKey generates a new user key within the current range
// (`g.rangeStart`). If `g.rangeStart` is unset, it'll set a random range.
func (g *sstOpGenerator) generateKey(rng *rand.Rand) roachpb.Key {
	if g.rangeStart == 0 {
		g.rangeStart = randutil.LowercaseAlphabet[rng.Intn(len(randutil.LowercaseAlphabet))]
	}
	return append(roachpb.Key{g.rangeStart},
		randutil.RandBytesInRange(rng, randutil.LowercaseAlphabet, 1, g.maxKeyLen-1)...)
}

func (g *sstOpGenerator) pointKeyConflicts(k MVCCKey) bool {
	timestamps, ok := g.pendingPointKeys[string(k.Key)]
	if ok {
		for _, ts := range timestamps {
			if ts == k.Timestamp.WallTime {
				return true
			}
		}
	}
	for _, p := range g.pendingRangeKeys {
		if p.Includes(k.Key) && p.Timestamp == k.Timestamp {
			return true
		}
	}
	return false
}

func (g *sstOpGenerator) rangeKeyConflicts(rk MVCCRangeKey) bool {
	for _, p := range g.pendingRangeKeys {
		if rk.Overlaps(p) && rk.Timestamp == p.Timestamp {
			return true
		}
	}
	for k, timestamps := range g.pendingPointKeys {
		if !rk.Includes(roachpb.Key(k)) {
			continue
		}
		for _, ts := range timestamps {
			if rk.Timestamp.WallTime == ts {
				return true
			}
		}
	}
	return false
}

// generateMVCCKey generates a new random MVCC key within the current KV range.
// It avoids producing a MVCC key with the same user key and timestamp as
// another key added since the last AddSSTable operation.
func (g *sstOpGenerator) generateMVCCKey(rng *rand.Rand) MVCCKey {
	k := MVCCKey{Key: g.generateKey(rng), Timestamp: g.generateTS(rng)}
	// Avoid duplicates.
	for g.pointKeyConflicts(k) {
		k = MVCCKey{Key: g.generateKey(rng), Timestamp: g.generateTS(rng)}
	}
	g.pendingPointKeys[string(k.Key)] = append(g.pendingPointKeys[string(k.Key)], k.Timestamp.WallTime)
	return k
}

func (g *sstOpGenerator) generateVal(rng *rand.Rand) []byte {
	return randutil.RandBytesInRange(rng, randutil.LowercaseAlphabet, 0, g.maxValLen)
}

func (g *sstOpGenerator) rangeBounds() (start, end roachpb.Key) {
	return roachpb.Key{g.rangeStart}, roachpb.Key{g.rangeStart + 1}
}

type putOp struct {
	key       MVCCKey
	userValue []byte
}

func (o *putOp) String() string {
	return fmt.Sprintf("MVCCPut(%q@%d, %q)", o.key.Key, o.key.Timestamp.WallTime, o.userValue)
}
func (o *putOp) sortKey() MVCCKey { return o.key }
func (o *putOp) apply(w *SSTWriter) error {
	var v MVCCValue
	v.Value.SetBytes(o.userValue)
	v.Value.InitChecksum(o.key.Key)
	return w.PutMVCC(o.key, v)
}
func (o *putOp) Run(l *meta.Logger, s *sstTestState) {
	s.bufferedOps = append(s.bufferedOps, o)
	l.Log("ok")
}

func (g *sstOpGenerator) generateMVCCPut(rng *rand.Rand) meta.Op[*sstTestState] {
	return &putOp{
		key:       g.generateMVCCKey(rng),
		userValue: g.generateVal(rng),
	}
}

type delOp struct {
	key MVCCKey
}

func (o *delOp) String() string {
	return fmt.Sprintf("MVCCDelete(%q@%d)", o.key.Key, o.key.Timestamp.WallTime)
}
func (o *delOp) sortKey() MVCCKey         { return o.key }
func (o *delOp) apply(w *SSTWriter) error { return w.PutMVCC(o.key, MVCCValue{}) }
func (o *delOp) Run(l *meta.Logger, s *sstTestState) {
	s.bufferedOps = append(s.bufferedOps, o)
	l.Log("ok")
}

func (g *sstOpGenerator) generateMVCCDel(rng *rand.Rand) meta.Op[*sstTestState] {
	return &delOp{key: g.generateMVCCKey(rng)}
}

// Del range

type sstDelRangeTombstoneOp struct {
	key       MVCCRangeKey
	peekLeft  roachpb.Key
	peekRight roachpb.Key
}

func (o *sstDelRangeTombstoneOp) String() string {
	return fmt.Sprintf("MVCCDeleteRangeUsingTombstone(%s, %s, %s)",
		o.key.StartKey, o.key.EndKey, o.key.Timestamp)
}
func (o *sstDelRangeTombstoneOp) sortKey() MVCCKey {
	return MVCCKey{Key: o.key.StartKey}
}
func (o *sstDelRangeTombstoneOp) apply(w *SSTWriter) error {
	return w.PutMVCCRangeKey(o.key, MVCCValue{})
}
func (o *sstDelRangeTombstoneOp) Run(l *meta.Logger, s *sstTestState) {
	s.bufferedOps = append(s.bufferedOps, o)
	l.Log("ok")
}

func (g *sstOpGenerator) generateMVCCDelRangeTombstone(rng *rand.Rand) meta.Op[*sstTestState] {
	generateRangeKey := func() (rk MVCCRangeKey) {
		rk.Timestamp = g.generateTS(rng)
		if rng.Intn(3) == 0 {
			rk.StartKey = roachpb.Key{g.rangeStart}
		} else {
			rk.StartKey = g.generateKey(rng)
		}
		if rng.Intn(3) == 0 {
			rk.EndKey = roachpb.Key{rk.StartKey[0] + 1}
		} else {
			rk.EndKey = g.generateKey(rng)
		}
		// Ensure the bounds are properly ordered and nonequal.
		if v := bytes.Compare(rk.StartKey, rk.EndKey); v > 0 {
			rk.EndKey, rk.StartKey = rk.StartKey, rk.EndKey
		} else if v == 0 {
			rk.EndKey = append(append(rk.EndKey[:0], rk.StartKey...), 0x00)
		}
		return rk
	}
	rk := generateRangeKey()
	for g.rangeKeyConflicts(rk) {
		rk = generateRangeKey()
	}

	// TODO(jackson): Increase the probability of generating abutting
	// range tombstones in neighboring ranges.
	rangeStart, rangeEnd := g.rangeBounds()
	return &sstDelRangeTombstoneOp{
		key:       rk,
		peekLeft:  rangeStart,
		peekRight: rangeEnd,
	}
}

type checkSSTConflictsOp struct {
	// start and end hold the start and end keys of the sstable.
	start, end roachpb.Key
	// usePrefix determines whether or not the CheckSSTConflicts call should use
	// prefix iteration if possible.
	//
	// TODO(jackson): This can be varied metamorphically at /execution/ time,
	// but currently we don't have support for execution time metamorphism in
	// the `meta` package.
	usePrefix         bool
	disallowShadowing *hlc.Timestamp
}

func (o *checkSSTConflictsOp) String() string {
	return fmt.Sprintf("CheckSSTConflicts(disallowShadowing=%s, usePrefix=%t)", o.disallowShadowing, o.usePrefix)
}
func (o *checkSSTConflictsOp) Run(l *meta.Logger, s *sstTestState) {
	if len(s.bufferedOps) == 0 {
		l.Log("noop")
		return
	}

	ctx := context.Background()
	sort.Stable(s.bufferedOps)
	f := new(MemObject)
	w := MakeIngestionSSTWriter(ctx, cluster.MakeTestingClusterSettings(), f)
	for i := 0; i < len(s.bufferedOps); i++ {
		if err := s.bufferedOps[i].apply(&w); err != nil {
			require.NoError(l, errors.Wrapf(err, "during op %d: %s", i, s.bufferedOps[i].String()))
		}
	}
	require.NoError(l, w.Finish())

	var disallowShadowingTS hlc.Timestamp
	if o.disallowShadowing != nil {
		disallowShadowingTS = *o.disallowShadowing
	}

	stats, err := CheckSSTConflicts(
		ctx,
		f.Bytes(),
		s.engine,
		s.bufferedOps[0].sortKey(),
		s.bufferedOps[len(s.bufferedOps)-1].sortKey(),
		o.start,
		o.end,
		o.disallowShadowing != nil,
		disallowShadowingTS,
		hlc.Timestamp{},
		1000,
		o.usePrefix)
	l.Logf("\nStats: %s\nError: %v", stats.Formatted(false /* delta */), err)
}

func (g *sstOpGenerator) generateCheckSSTConflicts(rng *rand.Rand) meta.Op[*sstTestState] {
	start, end := g.rangeBounds()
	o := &checkSSTConflictsOp{
		start:     start,
		end:       end,
		usePrefix: rng.Intn(2) == 1,
	}
	if rng.Intn(2) == 1 {
		// Disallow shadowing.
		o.disallowShadowing = new(hlc.Timestamp)
		*o.disallowShadowing = g.generateTS(rng)
	}
	return o
}

type addSSTableOp struct {
	// start and end hold the start and end keys of the sstable.
	start, end roachpb.Key
	// usePrefix determines whether or not the CheckSSTConflicts call should use
	// prefix iteration if possible.
	//
	// TODO(jackson): This can be varied metamorphically at /execution/ time,
	// but currently we don't have support for execution time metamorphism in
	// the `meta` package.
	usePrefix         bool
	disallowShadowing *hlc.Timestamp
	readAt            hlc.Timestamp
}

func (o *addSSTableOp) String() string {
	return fmt.Sprintf("AddSSTable(disallowShadowing=%s, usePrefix=%t, readAt=%d)",
		o.disallowShadowing, o.usePrefix, o.readAt.WallTime)
}
func (o *addSSTableOp) Run(l *meta.Logger, s *sstTestState) {
	if len(s.bufferedOps) == 0 {
		l.Log("noop")
		return
	}

	ctx := context.Background()
	sort.Sort(s.bufferedOps)
	f := new(MemObject)
	w := MakeIngestionSSTWriter(ctx, cluster.MakeTestingClusterSettings(), f)
	for i := 0; i < len(s.bufferedOps); i++ {
		if err := s.bufferedOps[i].apply(&w); err != nil {
			require.NoError(l, errors.Wrapf(err, "apply op to writer %d: %s", i, s.bufferedOps[i].String()))
		}
	}
	require.NoError(l, w.Finish())

	var disallowShadowingTS hlc.Timestamp
	if o.disallowShadowing != nil {
		disallowShadowingTS = *o.disallowShadowing
	}

	sstIter, err := NewMemSSTIterator(f.Data(), true /* verify */, IterOptions{
		KeyTypes:   IterKeyTypePointsAndRanges,
		LowerBound: keys.MinKey,
		UpperBound: keys.MaxKey,
	})
	require.NoError(l, err)
	defer sstIter.Close()

	sstIter.SeekGE(MVCCKey{Key: keys.MinKey})
	_, err = sstIter.Valid()
	require.NoError(l, err)

	logKey := func(k MVCCKey, v []byte) error {
		mv, err := DecodeMVCCValue(v)
		if err != nil {
			return err
		}
		l.Logf("  %s→%s\n", k, mv)
		return nil
	}
	logRangeKey := func(rkv MVCCRangeKeyValue) error {
		mv, err := DecodeMVCCValue(rkv.Value)
		if err != nil {
			return err
		}
		l.Logf("  %s→%s\n", rkv.RangeKey, mv)
		return nil
	}

	l.Log("\nSST:\n")
	sstStats, err := computeStatsForIterWithVisitors(
		sstIter, o.readAt.WallTime, logKey, logRangeKey)
	require.NoError(l, err)
	l.Log("Before:\n")
	beforeStats, err := ComputeStatsWithVisitors(
		s.engine, o.start, o.end, o.readAt.WallTime, logKey, logRangeKey)
	require.NoError(l, err)

	conflictStats, err := CheckSSTConflicts(
		ctx,
		f.Bytes(),
		s.engine,
		s.bufferedOps[0].sortKey(),
		MVCCKey{Key: o.end},
		o.start,
		o.end,
		o.disallowShadowing != nil,
		disallowShadowingTS,
		hlc.Timestamp{},
		1000,
		o.usePrefix)
	if err != nil {
		l.Log("error ", err)
		s.bufferedOps = nil
		return
	}

	{
		f2, err := s.engine.Create("op.sst")
		require.NoError(l, err)
		_, err = f2.Write(f.Data())
		require.NoError(l, err)
		require.NoError(l, f2.Sync())
		require.NoError(l, f2.Close())
		require.NoError(l, s.engine.IngestExternalFiles(ctx, []string{"op.sst"}))
	}

	ms := beforeStats
	ms.Add(sstStats)
	ms.Add(conflictStats)
	l.Log("After:\n")
	afterStats, err := ComputeStatsWithVisitors(
		s.engine, o.start, o.end, o.readAt.WallTime, logKey, logRangeKey)
	require.NoError(l, err)

	if ms.ContainsEstimates == 0 && !ms.Equal(&afterStats) {
		delta := afterStats
		delta.Subtract(ms)
		l.Fatal(errors.AssertionFailedf("range %s: stats calculated from CheckSSTConflicts differ:\n"+
			"SST:\n  %s\nConflict:\n  %s\nBefore:\n  %s\nBefore+SST+Conflict:\n  %s\n"+
			"ComputeStats:\n  %s\nDiff:\n  %s",
			o.start,
			sstStats.Formatted(true /* delta */),
			conflictStats.Formatted(true /* delta */),
			beforeStats.Formatted(false /* delta */),
			ms.Formatted(false /* delta */),
			afterStats.Formatted(false /* delta */),
			delta.Formatted(true /* delta */)))
	}
	l.Commentf("%s: %s", o.start, afterStats.Formatted(false /* delta */))

	// Next sstable starts anew.
	s.bufferedOps = nil
	l.Log("ok")
}

func (g *sstOpGenerator) generateAddSSTable(rng *rand.Rand) meta.Op[*sstTestState] {
	start, end := g.rangeBounds()
	o := &addSSTableOp{
		start:     start,
		end:       end,
		usePrefix: rng.Intn(2) == 1,
	}
	if rng.Intn(2) == 1 {
		// Disallow shadowing.
		o.disallowShadowing = new(hlc.Timestamp)
		*o.disallowShadowing = g.generateTS(rng)
	}
	o.readAt = hlc.Timestamp{WallTime: g.tsHigh}
	// Next table starts anew.
	g.pendingPointKeys = map[string][]int64{}
	g.pendingRangeKeys = nil
	g.rangeStart = randutil.LowercaseAlphabet[rng.Intn(len(randutil.LowercaseAlphabet))]
	return o
}

type sstComputeStatsOp struct {
	timestamp int64
}

func (o *sstComputeStatsOp) String() string {
	return fmt.Sprintf("ComputeStats(timestamp=%d)", o.timestamp)
}
func (o *sstComputeStatsOp) Run(l *meta.Logger, s *sstTestState) {
	// Compute stats for every range, where ranges are [a,b),[b,c), ..., [z,A)
	for start, end := byte('a'), byte('b'); start <= 'z'; start, end = start+1, end+1 {
		stats, err := ComputeStats(s.engine, roachpb.Key{start}, roachpb.Key{end}, o.timestamp)
		require.NoError(l, err)
		l.Logf("\n%s: %s", string(start), stats.Formatted(false /* delta */))
	}
}

func (g *sstOpGenerator) generateComputeStats(rng *rand.Rand) meta.Op[*sstTestState] {
	return &sstComputeStatsOp{timestamp: g.generateTS(rng).WallTime}
}
