// Copyright 2020 The Cockroach Authors.
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
	"flag"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/stretchr/testify/require"
)

func scanRoachKey(t *testing.T, td *datadriven.TestData, field string) roachpb.Key {
	var k string
	td.ScanArgs(t, field, &k)
	rk := roachpb.Key(k)
	if strings.HasPrefix(k, "L") {
		rk = append(keys.LocalRangePrefix, rk[1:]...)
	} else if strings.HasPrefix(k, "S") {
		rk = append(keys.LocalStorePrefix, rk[1:]...)
	} else if strings.HasPrefix(k, "Y") {
		rk = append(keys.LocalRangeLockTablePrefix.PrefixEnd(), rk[1:]...)
	} else if strings.HasPrefix(k, "Z") {
		if len(rk) != 1 {
			panic("Z represents LocalMax and should not have more than one character")
		}
		rk = keys.LocalMax
	}
	return bytes.ReplaceAll(rk, []byte("\\0"), []byte{0})
}

func makePrintableKey(k MVCCKey) MVCCKey {
	k.Key = makePrintableRoachpbKey(k.Key)
	return k
}

func makePrintableRoachpbKey(k roachpb.Key) roachpb.Key {
	if bytes.HasPrefix(k, keys.LocalRangePrefix) {
		k = append([]byte("L"), k[len(keys.LocalRangePrefix):]...)
	} else if bytes.HasPrefix(k, keys.LocalStorePrefix) {
		k = append([]byte("S"), k[len(keys.LocalStorePrefix):]...)
	} else if bytes.HasPrefix(k, keys.LocalRangeLockTablePrefix.PrefixEnd()) {
		k = append([]byte("Y"), k[len(keys.LocalRangeLockTablePrefix):]...)
	} else if bytes.Equal(k, keys.LocalMax) {
		k = []byte("Z")
	}
	k = bytes.ReplaceAll(k, []byte{0}, []byte("\\0"))
	return k
}

func scanSeekKey(t *testing.T, td *datadriven.TestData) MVCCKey {
	key := MVCCKey{Key: scanRoachKey(t, td, "k")}
	if td.HasArg("ts") {
		var tsS string
		td.ScanArgs(t, "ts", &tsS)
		ts, err := hlc.ParseTimestamp(tsS)
		if err != nil {
			t.Fatalf("%v", err)
		}
		key.Timestamp = ts
	}
	return key
}

func checkAndOutputIter(iter MVCCIterator, b *strings.Builder) {
	valid, err := iter.Valid()
	if err != nil {
		fmt.Fprintf(b, "output: err: %s\n", err)
		return
	}
	if !valid {
		fmt.Fprintf(b, "output: .\n")
		return
	}
	k1 := makePrintableKey(iter.UnsafeKey())
	k2 := makePrintableKey(iter.Key())
	if !k1.Equal(k2) {
		fmt.Fprintf(b, "output: key: %s != %s\n", k1, k2)
		return
	}
	engineKey, ok := DecodeEngineKey(iter.UnsafeRawKey())
	if !ok {
		fmt.Fprintf(b, "output: could not DecodeEngineKey: %x\n", iter.UnsafeRawKey())
		return
	}
	rawMVCCKey := iter.UnsafeRawMVCCKey()
	if iter.IsCurIntentSeparated() {
		if !engineKey.IsLockTableKey() {
			fmt.Fprintf(b, "output: engineKey should be a lock table key: %s\n", engineKey)
			return
		}
		ltKey, err := engineKey.ToLockTableKey()
		if err != nil {
			fmt.Fprintf(b, "output: engineKey should be a lock table key: %s\n", err.Error())
			return
		}
		// Strip off the sentinel byte.
		rawMVCCKey = rawMVCCKey[:len(rawMVCCKey)-1]
		if !bytes.Equal(ltKey.Key, rawMVCCKey) {
			fmt.Fprintf(b, "output: rawMVCCKey %x != ltKey.Key %x\n", rawMVCCKey, ltKey.Key)
			return
		}
	} else {
		if !engineKey.IsMVCCKey() {
			fmt.Fprintf(b, "output: engineKey should be a MVCC key: %s\n", engineKey)
			return
		}
		mvccKey, err := engineKey.ToMVCCKey()
		if err != nil {
			fmt.Fprintf(b, "output: engineKey should be a MVCC key: %s\n", err.Error())
			return
		}
		if !bytes.Equal(iter.UnsafeRawKey(), iter.UnsafeRawMVCCKey()) {
			fmt.Fprintf(b, "output: UnsafeRawKey %x != UnsafeRawMVCCKey %x\n",
				iter.UnsafeRawKey(), iter.UnsafeRawMVCCKey())
			return
		}
		if !mvccKey.Equal(iter.UnsafeKey()) {
			fmt.Fprintf(b, "output: mvccKey %s != UnsafeKey %s\n", mvccKey, iter.UnsafeKey())
			return
		}
	}
	v1 := iter.UnsafeValue()
	v2 := iter.Value()
	if !bytes.Equal(v1, v2) {
		fmt.Fprintf(b, "output: value: %x != %x\n", v1, v2)
		return
	}
	if k1.Timestamp.IsEmpty() {
		var meta enginepb.MVCCMetadata
		if err := protoutil.Unmarshal(v1, &meta); err != nil {
			fmt.Fprintf(b, "output: meta parsing: %s\n", err)
			return
		}
		if meta.Timestamp.ToTimestamp().IsEmpty() {
			fmt.Fprintf(b, "output: meta k=%s\n", string(k1.Key))
		} else {
			uuid := meta.Txn.ID.ToUint128()
			var hiStr string
			if uuid.Hi != 0 {
				hiStr = fmt.Sprintf("%d,", uuid.Hi)
			}
			fmt.Fprintf(b, "output: meta k=%s ts=%s txn=%s%d\n",
				string(k1.Key), meta.Timestamp, hiStr, uuid.Lo)
		}
		return
	}
	fmt.Fprintf(b, "output: value k=%s ts=%s v=%s\n",
		string(k1.Key), k1.Timestamp, string(v1))
}

// TestIntentInterleavingIter is a datadriven test consisting of two commands:
// - define: defines key-value pairs in the lock table and MVCC key spaces.
//   Intents can be in both key spaces, and inline meta and MVCC values in
//   the latter.
//   meta k=<key> ts=<ts> txn=<txn>  defines an intent
//   meta k=<key>                    defines an inline meta
//   value k=<key> ts=<ts> v=<value> defines an MVCC value
//   It is acceptable to define intents without provisional values to test
//   out error checking code paths.
// - iter: for iterating, is defined as
//   iter [lower=<lower>] [upper=<upper>] [prefix=<true|false>]
//   followed by newline separated sequence of operations:
//     next, prev, seek-lt, seek-ge, set-upper, next-key, stats
//
// Keys are interpreted as:
// - starting with L is interpreted as a local-range key.
// - starting with S is interpreted as a store local key.
// - starting with Y is interpreted as a local key starting immediately after
//   the lock table key space. This is for testing edge cases wrt bounds.
// - a single Z is interpreted as LocalMax
func TestIntentInterleavingIter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	var eng Engine
	defer func() {
		if eng != nil {
			eng.Close()
		}
	}()

	datadriven.Walk(t, "testdata/intent_interleaving_iter", func(t *testing.T, path string) {
		if (util.RaceEnabled && strings.HasSuffix(path, "race_off")) ||
			(!util.RaceEnabled && strings.HasSuffix(path, "race")) {
			return
		}
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "define":
				if eng != nil {
					eng.Close()
				}
				eng = createTestPebbleEngine()
				batch := eng.NewBatch()
				var locksSection bool
				var mvccSection bool
				// pos is the original <file>:<lineno> prefix computed by
				// datadriven. It points to the top "define" command itself.
				// We editing d.Pos in-place below by extending `pos` upon
				// each new line.
				pos := d.Pos
				for i, line := range strings.Split(d.Input, "\n") {
					switch line {
					case "locks":
						locksSection = true
						mvccSection = false
						continue
					case "mvcc":
						locksSection = false
						mvccSection = true
						continue
					}
					// Compute a line prefix, to clarify error message. We
					// prefix a newline character because some text editor do
					// not know how to jump to the location of an error if
					// there are multiple file:line prefixes on the same line.
					d.Pos = fmt.Sprintf("\n%s: (+%d)", pos, i+1)
					if !locksSection && !mvccSection {
						t.Fatalf("%s: not a locks or mvcc section", d.Pos)
					}
					var err error
					if d.Cmd, d.CmdArgs, err = datadriven.ParseLine(line); err != nil {
						t.Fatalf("%s: %s", d.Pos, err)
					}
					switch d.Cmd {
					case "meta":
						key := scanRoachKey(t, d, "k")
						// We don't bother populating most fields in the proto.
						var meta enginepb.MVCCMetadata
						var txnUUID uuid.UUID
						if locksSection || d.HasArg("ts") {
							var tsS string
							d.ScanArgs(t, "ts", &tsS)
							ts, err := hlc.ParseTimestamp(tsS)
							if err != nil {
								t.Fatalf("%v", err)
							}
							meta.Timestamp = ts.ToLegacyTimestamp()
							var txn int
							d.ScanArgs(t, "txn", &txn)
							txnUUID = uuid.FromUint128(uint128.FromInts(0, uint64(txn)))
							meta.Txn = &enginepb.TxnMeta{ID: txnUUID}
						}
						val, err := protoutil.Marshal(&meta)
						if err != nil {
							return err.Error()
						}
						if mvccSection {
							// This is an abuse of PutUnversioned, but we know the
							// implementation and can be sure that it will not change the key.
							if err := batch.PutUnversioned(key, val); err != nil {
								return err.Error()
							}
						} else {
							ltKey := LockTableKey{Key: key, Strength: lock.Exclusive, TxnUUID: txnUUID[:]}
							eKey, _ := ltKey.ToEngineKey(nil)
							if err := batch.PutEngineKey(eKey, val); err != nil {
								return err.Error()
							}
						}
					case "value":
						if locksSection {
							t.Fatalf("%s: value in locks section", d.Pos)
						}
						key := scanRoachKey(t, d, "k")
						var tsS string
						d.ScanArgs(t, "ts", &tsS)
						ts, err := hlc.ParseTimestamp(tsS)
						if err != nil {
							t.Fatalf("%v", err)
						}
						var value string
						d.ScanArgs(t, "v", &value)
						mvccKey := MVCCKey{Key: key, Timestamp: ts}
						if err := batch.PutMVCC(mvccKey, []byte(value)); err != nil {
							return err.Error()
						}
					}
				}
				d.Pos = pos
				if err := batch.Commit(true); err != nil {
					return err.Error()
				}
				return ""

			case "iter":
				var opts IterOptions
				if d.HasArg("lower") {
					opts.LowerBound = scanRoachKey(t, d, "lower")
				}
				if d.HasArg("upper") {
					opts.UpperBound = scanRoachKey(t, d, "upper")
				}
				if d.HasArg("prefix") {
					d.ScanArgs(t, "prefix", &opts.Prefix)
				}
				iter := wrapInUnsafeIter(newIntentInterleavingIterator(eng, opts))
				var b strings.Builder
				defer iter.Close()
				// pos is the original <file>:<lineno> prefix computed by
				// datadriven. It points to the top "define" command itself.
				// We editing d.Pos in-place below by extending `pos` upon
				// each new line.
				pos := d.Pos
				for i, line := range strings.Split(d.Input, "\n") {
					// Compute a line prefix, to clarify error message. We
					// prefix a newline character because some text editor do
					// not know how to jump to the location of an error if
					// there are multiple file:line prefixes on the same line.
					d.Pos = fmt.Sprintf("\n%s: (+%d)", pos, i+1)
					var err error
					if d.Cmd, d.CmdArgs, err = datadriven.ParseLine(line); err != nil {
						t.Fatalf("%s: %s", d.Pos, err)
					}
					switch d.Cmd {
					case "seek-ge":
						key := scanSeekKey(t, d)
						iter.SeekGE(key)
						fmt.Fprintf(&b, "seek-ge %s: ", makePrintableKey(key))
						checkAndOutputIter(iter, &b)
					case "next":
						iter.Next()
						fmt.Fprintf(&b, "next: ")
						checkAndOutputIter(iter, &b)
					case "seek-lt":
						key := scanSeekKey(t, d)
						iter.SeekLT(key)
						fmt.Fprintf(&b, "seek-lt %s: ", makePrintableKey(key))
						checkAndOutputIter(iter, &b)
					case "prev":
						iter.Prev()
						fmt.Fprintf(&b, "prev: ")
						checkAndOutputIter(iter, &b)
					case "next-key":
						iter.NextKey()
						fmt.Fprintf(&b, "next-key: ")
						checkAndOutputIter(iter, &b)
					case "set-upper":
						k := scanRoachKey(t, d, "k")
						iter.SetUpperBound(k)
						fmt.Fprintf(&b, "set-upper %s\n", string(makePrintableKey(MVCCKey{Key: k}).Key))
					case "stats":
						stats := iter.Stats()
						fmt.Fprintf(&b, "stats: %s\n", stats.Stats.String())
					default:
						fmt.Fprintf(&b, "unknown command: %s\n", d.Cmd)
					}
				}
				d.Pos = pos
				return b.String()
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
	})
}

func TestIntentInterleavingIterBoundaries(t *testing.T) {
	defer leaktest.AfterTest(t)()

	eng := createTestPebbleEngine()
	defer eng.Close()
	// Boundary cases for constrainedToLocal.
	func() {
		opts := IterOptions{LowerBound: keys.MinKey}
		iter := newIntentInterleavingIterator(eng, opts).(*intentInterleavingIter)
		require.Equal(t, constrainedToLocal, iter.constraint)
		iter.SetUpperBound(keys.LocalMax)
		require.Equal(t, constrainedToLocal, iter.constraint)
		iter.SeekLT(MVCCKey{Key: keys.LocalMax})
		iter.Close()
	}()
	func() {
		opts := IterOptions{UpperBound: keys.LocalMax}
		iter := newIntentInterleavingIterator(eng, opts).(*intentInterleavingIter)
		require.Equal(t, constrainedToLocal, iter.constraint)
		iter.SetUpperBound(keys.LocalMax)
		require.Equal(t, constrainedToLocal, iter.constraint)
		iter.Close()
	}()
	require.Panics(t, func() {
		opts := IterOptions{UpperBound: keys.LocalMax}
		iter := newIntentInterleavingIterator(eng, opts).(*intentInterleavingIter)
		iter.SeekLT(MVCCKey{Key: keys.MaxKey})
	})
	// Boundary cases for constrainedToGlobal
	func() {
		opts := IterOptions{LowerBound: keys.LocalMax}
		iter := newIntentInterleavingIterator(eng, opts).(*intentInterleavingIter)
		require.Equal(t, constrainedToGlobal, iter.constraint)
		iter.Close()
	}()
	require.Panics(t, func() {
		opts := IterOptions{LowerBound: keys.LocalMax}
		iter := newIntentInterleavingIterator(eng, opts).(*intentInterleavingIter)
		require.Equal(t, constrainedToGlobal, iter.constraint)
		iter.SetUpperBound(keys.LocalMax)
		iter.Close()
	})
	require.Panics(t, func() {
		opts := IterOptions{LowerBound: keys.LocalMax}
		iter := newIntentInterleavingIterator(eng, opts).(*intentInterleavingIter)
		require.Equal(t, constrainedToGlobal, iter.constraint)
		iter.SeekLT(MVCCKey{Key: keys.LocalMax})
		iter.Close()
	})
	// Panics for using a local key that is above the lock table.
	require.Panics(t, func() {
		opts := IterOptions{UpperBound: keys.LocalMax}
		iter := newIntentInterleavingIterator(eng, opts).(*intentInterleavingIter)
		require.Equal(t, constrainedToLocal, iter.constraint)
		iter.SeekLT(MVCCKey{Key: keys.LocalRangeLockTablePrefix.PrefixEnd()})
		iter.Close()
	})
	require.Panics(t, func() {
		opts := IterOptions{UpperBound: keys.LocalMax}
		iter := newIntentInterleavingIterator(eng, opts).(*intentInterleavingIter)
		require.Equal(t, constrainedToLocal, iter.constraint)
		iter.SeekGE(MVCCKey{Key: keys.LocalRangeLockTablePrefix.PrefixEnd()})
		iter.Close()
	})
	// Prefix iteration does not affect the constraint if bounds are
	// specified.
	func() {
		opts := IterOptions{Prefix: true, LowerBound: keys.LocalMax}
		iter := newIntentInterleavingIterator(eng, opts).(*intentInterleavingIter)
		require.Equal(t, constrainedToGlobal, iter.constraint)
		iter.Close()
	}()
	// Prefix iteration with no bounds.
	func() {
		iter := newIntentInterleavingIterator(eng, IterOptions{Prefix: true}).(*intentInterleavingIter)
		require.Equal(t, notConstrained, iter.constraint)
		iter.Close()
	}()
}

type lockKeyValue struct {
	key LockTableKey
	val []byte
	// An intent that is not live is written along with a Delete or SingleDelete
	// to delete it. Deleted intents don't exercise any code in
	// intentInterleavingIter, but test the underlying Pebble code, just in case
	// there are any undiscovered bugs.
	liveIntent bool
	// When using intentInterleavingIter, there is a mix of interleaved and
	// separated intents. This bool determines which kind of intent is written.
	separated bool
}

func generateRandomData(
	t *testing.T, rng *rand.Rand, isLocal bool,
) (lkv []lockKeyValue, mvcckv []MVCCKeyValue) {
	numKeys := 10000
	txnIDMap := make(map[int32]struct{})
	for i := 0; i < numKeys; i++ {
		var key roachpb.Key
		if isLocal {
			key = append(key, keys.LocalRangePrefix...)
		}
		key = append(key, roachpb.Key(fmt.Sprintf("key%08d", i))...)
		hasIntent := rng.Int31n(2) == 0
		numVersions := int(rng.Int31n(4)) + 1
		var timestamps []int
		for j := 0; j < numVersions; j++ {
			timestamps = append(timestamps, rng.Intn(1<<20)+1)
		}
		// Sort in descending order and make unique.
		sort.Sort(sort.Reverse(sort.IntSlice(timestamps)))
		last := 0
		for j := 1; j < len(timestamps); j++ {
			if timestamps[j] != timestamps[last] {
				last++
				timestamps[last] = timestamps[j]
			}
		}
		timestamps = timestamps[:last+1]
		for i, ts := range timestamps {
			var txnID int32
			for {
				// Find unique txn ID.
				txnID = rng.Int31()
				if _, found := txnIDMap[txnID]; found {
					continue
				}
				txnIDMap[txnID] = struct{}{}
				break
			}
			txnUUID := uuid.FromUint128(uint128.FromInts(0, uint64(txnID)))
			meta := enginepb.MVCCMetadata{
				Timestamp: hlc.LegacyTimestamp{WallTime: int64(ts)},
				Txn:       &enginepb.TxnMeta{ID: txnUUID},
			}
			val, err := protoutil.Marshal(&meta)
			require.NoError(t, err)
			isSeparated := rng.Int31n(2) == 0
			ltKey := LockTableKey{Key: key, Strength: lock.Exclusive, TxnUUID: txnUUID[:]}
			lkv = append(lkv, lockKeyValue{
				key: ltKey, val: val, liveIntent: hasIntent && i == 0, separated: isSeparated})
			mvcckv = append(mvcckv, MVCCKeyValue{
				Key:   MVCCKey{Key: key, Timestamp: hlc.Timestamp{WallTime: int64(ts)}},
				Value: []byte("value"),
			})
		}
	}
	return lkv, mvcckv
}

func writeRandomData(
	t *testing.T, eng Engine, lkv []lockKeyValue, mvcckv []MVCCKeyValue, interleave bool,
) {
	batch := eng.NewBatch()
	// Iterate in reverse order, so that older locks for a key are encountered
	// before newer ones. This is because we use ClearUnversioned below to
	// delete the non-live locks, for interleaved locks, and don't want that to
	// delete the live ones (since interleaved locks reuse the same key).
	//
	// Even though we are writing and clearing in the same batch, the total data
	// volume is small enough that these will be placed in the memtable and not
	// flushed, so both will be in the engine during iteration.
	for i := len(lkv) - 1; i >= 0; i-- {
		kv := lkv[i]
		if interleave || !kv.separated {
			require.NoError(t, batch.PutUnversioned(kv.key.Key, kv.val))
			if !kv.liveIntent {
				require.NoError(t, batch.ClearUnversioned(kv.key.Key))
			}
		} else {
			eKey, _ := kv.key.ToEngineKey(nil)
			require.NoError(t, batch.PutEngineKey(eKey, kv.val))
			if !kv.liveIntent {
				require.NoError(t, batch.SingleClearEngineKey(eKey))
			}
		}
	}
	for _, kv := range mvcckv {
		if kv.Key.Timestamp.IsEmpty() {
			panic("timestamp should not be empty")
		} else {
			require.NoError(t, batch.PutMVCC(kv.Key, kv.Value))
		}
	}
	require.NoError(t, batch.Commit(true))
}

func generateIterOps(rng *rand.Rand, mvcckv []MVCCKeyValue, isLocal bool) []string {
	var ops []string
	lowerIndex := rng.Intn(len(mvcckv) / 2)
	upperIndex := lowerIndex + rng.Intn(len(mvcckv)/2)
	if upperIndex == len(mvcckv) {
		upperIndex = len(mvcckv) - 1
	}
	lower := mvcckv[lowerIndex].Key.Key
	upper := mvcckv[upperIndex].Key.Key
	var iterStr string
	if bytes.Equal(lower, upper) || rng.Intn(2) == 0 {
		// Generate only one of lower or upper bound. Since we will be comparing
		// with a non interleaving iterator, which is not restricted to either
		// local or global keys, make sure to set a lower bound for global key and
		// an upper bound for local keys, to avoid spurious test failures.
		if isLocal {
			lowerIndex = 0
			lower = nil
			iterStr = fmt.Sprintf("iter upper=%s", string(makePrintableRoachpbKey(upper)))
		} else {
			upperIndex = len(mvcckv) - 1
			upper = nil
			iterStr = fmt.Sprintf("iter lower=%s", string(makePrintableRoachpbKey(lower)))
		}
	} else {
		iterStr = fmt.Sprintf("iter lower=%s upper=%s",
			string(makePrintableRoachpbKey(lower)), string(makePrintableRoachpbKey(upper)))
	}
	ops = append(ops, iterStr)
	for i := 0; i < 100; i++ {
		// Seek key
		seekIndex := rng.Intn(upperIndex-lowerIndex) + lowerIndex
		useTimestamp := rng.Intn(2) == 0
		seekKey := mvcckv[seekIndex].Key
		if !useTimestamp {
			seekKey.Timestamp = hlc.Timestamp{}
		}
		op := "seek-ge"
		fwdDirection := true
		if rng.Intn(2) == 0 {
			op = "seek-lt"
			fwdDirection = false
		}
		if useTimestamp {
			op = fmt.Sprintf("%s k=%s ts=%s", op, string(makePrintableRoachpbKey(seekKey.Key)),
				seekKey.Timestamp)
		} else {
			op = fmt.Sprintf("%s k=%s", op, string(makePrintableRoachpbKey(seekKey.Key)))
		}
		ops = append(ops, op)
		iterCount := rng.Intn(8)
		for j := 0; j < iterCount; j++ {
			// 40% prev, 40% next, 20% next-key
			p := rng.Intn(10)
			if p < 4 {
				op = "prev"
				fwdDirection = false
			} else if p < 8 {
				op = "next"
				fwdDirection = true
			} else {
				op = "next-key"
				// NextKey cannot be used to switch direction
				if !fwdDirection {
					ops = append(ops, "next")
					fwdDirection = true
				}
			}
			ops = append(ops, op)
		}
	}
	return ops
}

func doOps(t *testing.T, ops []string, eng Engine, interleave bool, out *strings.Builder) {
	var iter MVCCIterator
	var d datadriven.TestData
	var err error
	for _, op := range ops {
		d.Cmd, d.CmdArgs, err = datadriven.ParseLine(op)
		require.NoError(t, err)
		switch d.Cmd {
		case "iter":
			var opts IterOptions
			if d.HasArg("lower") {
				opts.LowerBound = scanRoachKey(t, &d, "lower")
			}
			if d.HasArg("upper") {
				opts.UpperBound = scanRoachKey(t, &d, "upper")
			}
			if interleave {
				iter = newIntentInterleavingIterator(eng, opts)
			} else {
				iter = eng.NewMVCCIterator(MVCCKeyIterKind, opts)
			}
			lowerStr := "nil"
			if opts.LowerBound != nil {
				lowerStr = string(makePrintableRoachpbKey(opts.LowerBound))
			}
			upperStr := "nil"
			if opts.UpperBound != nil {
				upperStr = string(makePrintableRoachpbKey(opts.UpperBound))
			}
			fmt.Fprintf(out, "iter lower=%s upper=%s\n", lowerStr, upperStr)
		case "seek-ge":
			key := scanSeekKey(t, &d)
			iter.SeekGE(key)
			fmt.Fprintf(out, "seek-ge %s: ", makePrintableKey(key))
			checkAndOutputIter(iter, out)
		case "seek-lt":
			key := scanSeekKey(t, &d)
			iter.SeekLT(key)
			fmt.Fprintf(out, "seek-lt %s: ", makePrintableKey(key))
			checkAndOutputIter(iter, out)
		case "next":
			iter.Next()
			fmt.Fprintf(out, "next: ")
			checkAndOutputIter(iter, out)
		case "next-key":
			iter.NextKey()
			fmt.Fprintf(out, "next-key: ")
			checkAndOutputIter(iter, out)
		case "prev":
			iter.Prev()
			fmt.Fprintf(out, "prev: ")
			checkAndOutputIter(iter, out)
		default:
			fmt.Fprintf(out, "unknown command: %s\n", d.Cmd)
		}
	}
}

var seedFlag = flag.Int64("seed", -1, "specify seed to use for random number generator")

func TestRandomizedIntentInterleavingIter(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	seed := *seedFlag
	if seed < 0 {
		seed = rand.Int63()
	}
	rng := rand.New(rand.NewSource(seed))
	lockKV, mvccKV := generateRandomData(t, rng, false /* isLocal */)
	localLockKV, localMvccKV := generateRandomData(t, rng, true /* isLocal */)
	eng1 := createTestPebbleEngine()
	eng2 := createTestPebbleEngine()
	defer eng1.Close()
	defer eng2.Close()
	writeRandomData(t, eng1, lockKV, mvccKV, false /* interleave */)
	writeRandomData(t, eng1, localLockKV, localMvccKV, false /* interleave */)
	writeRandomData(t, eng2, lockKV, mvccKV, true /* interleave */)
	writeRandomData(t, eng2, localLockKV, localMvccKV, true /* interleave */)
	var ops []string
	for _, isLocal := range []bool{false, true} {
		for i := 0; i < 10; i++ {
			kv := mvccKV
			if isLocal {
				kv = localMvccKV
			}
			ops = append(ops, generateIterOps(rng, kv, isLocal)...)
		}
	}
	var out1, out2 strings.Builder
	doOps(t, ops, eng1, true /* interleave */, &out1)
	doOps(t, ops, eng2, false /* interleave */, &out2)
	require.Equal(t, out1.String(), out2.String(),
		fmt.Sprintf("seed=%d\n=== separated ===\n%s\n=== interleaved ===\n%s\n",
			seed, out1.String(), out2.String()))
}

// TODO(sumeer): configure engine such that benchmark has data in multiple levels.

func writeBenchData(
	b *testing.B,
	eng Engine,
	numKeys int,
	versionsPerKey int,
	intentKeyStride int,
	prefix []byte,
	separated bool,
) {
	batch := eng.NewBatch()
	txnUUID := uuid.FromUint128(uint128.FromInts(0, uint64(1000)))
	for i := 0; i < numKeys; i++ {
		key := makeKey(prefix, i)
		if i%intentKeyStride == 0 {
			// Write intent.
			meta := enginepb.MVCCMetadata{
				Timestamp: hlc.LegacyTimestamp{WallTime: int64(versionsPerKey)},
				Txn:       &enginepb.TxnMeta{ID: txnUUID},
			}
			val, err := protoutil.Marshal(&meta)
			require.NoError(b, err)
			if separated {
				eKey, _ :=
					LockTableKey{Key: key, Strength: lock.Exclusive, TxnUUID: txnUUID[:]}.ToEngineKey(nil)
				require.NoError(b, batch.PutEngineKey(eKey, val))
			} else {
				require.NoError(b, batch.PutUnversioned(key, val))
			}
		}
		for j := versionsPerKey; j >= 1; j-- {
			require.NoError(b, batch.PutMVCC(
				MVCCKey{Key: key, Timestamp: hlc.Timestamp{WallTime: int64(j)}}, []byte("value")))
		}
	}
	require.NoError(b, batch.Commit(true))
}

func makeKey(prefix []byte, num int) roachpb.Key {
	return append(prefix, []byte(fmt.Sprintf("%08d", num))...)
}

type benchState struct {
	benchPrefix string
	keyPrefix   roachpb.Key
	eng         Engine
	separated   bool
}

var numBenchKeys = 10000

func intentInterleavingIterBench(b *testing.B, runFunc func(b *testing.B, state benchState)) {
	for _, separated := range []bool{false, true} {
		for _, versionsPerKey := range []int{1, 5} {
			for _, intentKeyStride := range []int{1, 100, 1000000} {
				for _, keyLength := range []int{10, 100} {
					func() {
						state := benchState{
							benchPrefix: fmt.Sprintf(
								"separated=%t/version=%d/intentStride=%d/keyLen=%d",
								separated, versionsPerKey, intentKeyStride, keyLength),
							keyPrefix: bytes.Repeat([]byte("k"), keyLength),
							eng:       createTestPebbleEngine(),
							separated: separated,
						}
						defer state.eng.Close()
						writeBenchData(b, state.eng, numBenchKeys, versionsPerKey, intentKeyStride,
							state.keyPrefix, separated)
						runFunc(b, state)
					}()
				}
			}
		}
	}
}

func BenchmarkIntentInterleavingIterNext(b *testing.B) {
	defer log.Scope(b).Close(b)

	intentInterleavingIterBench(b, func(b *testing.B, state benchState) {
		b.Run(state.benchPrefix,
			func(b *testing.B) {
				var iter MVCCIterator
				opts := IterOptions{LowerBound: state.keyPrefix, UpperBound: state.keyPrefix.PrefixEnd()}
				if state.separated {
					iter = newIntentInterleavingIterator(state.eng, opts)
				} else {
					iter = state.eng.NewMVCCIterator(MVCCKeyIterKind, opts)
				}
				startKey := MVCCKey{Key: state.keyPrefix}
				iter.SeekGE(startKey)
				b.ResetTimer()
				var unsafeKey MVCCKey
				// Each iteration does a Next(). It may additionally also do a SeekGE
				// if the iterator is exhausted, but we stop the timer for that.
				for i := 0; i < b.N; i++ {
					valid, err := iter.Valid()
					if err != nil {
						b.Fatal(err)
					}
					if !valid {
						b.StopTimer()
						iter.SeekGE(startKey)
						b.StartTimer()
					}
					unsafeKey = iter.UnsafeKey()
					iter.Next()
				}
				_ = unsafeKey
			})
	})
}

func BenchmarkIntentInterleavingIterPrev(b *testing.B) {
	defer log.Scope(b).Close(b)

	intentInterleavingIterBench(b, func(b *testing.B, state benchState) {
		b.Run(state.benchPrefix,
			func(b *testing.B) {
				var iter MVCCIterator
				endKey := MVCCKey{Key: state.keyPrefix.PrefixEnd()}
				opts := IterOptions{LowerBound: state.keyPrefix, UpperBound: endKey.Key}
				if state.separated {
					iter = newIntentInterleavingIterator(state.eng, opts)
				} else {
					iter = state.eng.NewMVCCIterator(MVCCKeyIterKind, opts)
				}
				iter.SeekLT(endKey)
				b.ResetTimer()
				var unsafeKey MVCCKey
				// Each iteration does a Prev(). It may additionally also do a SeekLT
				// if the iterator is exhausted, but we stop the timer for that.
				for i := 0; i < b.N; i++ {
					valid, err := iter.Valid()
					if err != nil {
						b.Fatal(err)
					}
					if !valid {
						b.StopTimer()
						iter.SeekLT(endKey)
						b.StartTimer()
					}
					unsafeKey = iter.UnsafeKey()
					iter.Prev()
				}
				_ = unsafeKey
			})
	})
}

func BenchmarkIntentInterleavingSeekGEAndIter(b *testing.B) {
	defer log.Scope(b).Close(b)

	intentInterleavingIterBench(b, func(b *testing.B, state benchState) {
		for _, seekStride := range []int{1, 10} {
			b.Run(fmt.Sprintf("%s/seekStride=%d", state.benchPrefix, seekStride),
				func(b *testing.B) {
					var seekKeys []roachpb.Key
					for i := 0; i < numBenchKeys; i += seekStride {
						seekKeys = append(seekKeys, makeKey(state.keyPrefix, i))
					}
					var iter MVCCIterator
					endKey := state.keyPrefix.PrefixEnd()
					opts := IterOptions{LowerBound: state.keyPrefix, UpperBound: endKey}
					if state.separated {
						iter = newIntentInterleavingIterator(state.eng, opts)
					} else {
						iter = state.eng.NewMVCCIterator(MVCCKeyIterKind, opts)
					}
					b.ResetTimer()
					var unsafeKey MVCCKey
					for i := 0; i < b.N; i++ {
						j := i % len(seekKeys)
						upperIndex := j + 1
						if upperIndex < len(seekKeys) {
							iter.SetUpperBound(seekKeys[upperIndex])
						} else {
							iter.SetUpperBound(endKey)
						}
						iter.SeekGE(MVCCKey{Key: seekKeys[j]})
						for {
							valid, err := iter.Valid()
							if err != nil {
								b.Fatal(err)
							}
							if !valid {
								break
							}
							unsafeKey = iter.UnsafeKey()
							iter.Next()
						}
					}
					_ = unsafeKey
				})
		}
	})
}

// TODO(sumeer): add SeekLTAndIter benchmark -- needs the ability to do
// SetLowerBound.
