// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"strings"
	"testing"
	"testing/quick"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/randutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
)

func scanLockTableKey(t *testing.T, d *datadriven.TestData, field string) roachpb.Key {
	var k string
	d.ScanArgs(t, field, &k)
	var rk roachpb.Key
	if k == "nil" {
		rk = nil
	} else if strings.HasPrefix(k, "Global/") {
		rk = roachpb.Key(k[7:])
	} else {
		rk, _ = keys.LockTableSingleKey(roachpb.Key(k), nil)
	}
	return rk
}

func printLockTableKey(k roachpb.Key) string {
	if len(k) == 0 {
		return "nil"
	}
	lk, err := keys.DecodeLockTableSingleKey(k)
	if err != nil {
		return fmt.Sprintf("Global/%s", string(k))
	}
	return string(lk)
}

func scanLockStrength(t *testing.T, td *datadriven.TestData, field string) lock.Strength {
	var strS string
	td.ScanArgs(t, field, &strS)
	switch strS {
	case "shared":
		return lock.Shared
	case "exclusive":
		return lock.Exclusive
	case "intent":
		return lock.Intent
	default:
		t.Fatalf("unknown lock strength: %s", strS)
		return 0
	}
}

func scanTxnID(t *testing.T, td *datadriven.TestData, field string) uuid.UUID {
	var txn int
	td.ScanArgs(t, field, &txn)
	return uuid.FromUint128(uint128.FromInts(0, uint64(txn)))
}

func checkAndOutputLockTableIterator(
	iter *LockTableIterator, b *strings.Builder, valid bool, err error,
) {
	state := pebble.IterExhausted
	if valid {
		state = pebble.IterValid
	}
	checkAndOutputLockTableIteratorWithState(iter, b, state, err)
}

func checkAndOutputLockTableIteratorWithState(
	iter *LockTableIterator, b *strings.Builder, state pebble.IterValidityState, err error,
) {
	if err != nil {
		fmt.Fprintf(b, "output: err: %s\n", err)
		return
	}
	if state != pebble.IterValid {
		switch state {
		case pebble.IterExhausted:
			fmt.Fprintf(b, "output: . (exhausted)\n")
		case pebble.IterAtLimit:
			fmt.Fprintf(b, "output: . (at limit)\n")
		default:
			fmt.Fprintf(b, "output: err: unexpected state %d\n", state)
		}
		return
	}
	key, err := iter.UnsafeEngineKey()
	if err != nil {
		fmt.Fprintf(b, "output: could not fetch key: %s\n", err)
		return
	}
	ltKey, err := key.ToLockTableKey()
	if err != nil {
		fmt.Fprintf(b, "output: could not decode lock table key: %s\n", err)
		return
	}

	v1, err := iter.UnsafeValue()
	if err != nil {
		fmt.Fprintf(b, "output: unable to fetch value: %s\n", err)
		return
	}
	v2, err := iter.Value()
	if err != nil {
		fmt.Fprintf(b, "output: unable to fetch value: %s\n", err)
		return
	}
	if !bytes.Equal(v1, v2) {
		fmt.Fprintf(b, "output: value: %x != %x\n", v1, v2)
		return
	}
	if len(v1) != iter.ValueLen() {
		fmt.Fprintf(b, "output: value len: %d != %d\n", len(v1), iter.ValueLen())
		return
	}

	var meta enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(v1, &meta); err != nil {
		fmt.Fprintf(b, "output: meta parsing: %s\n", err)
		return
	}
	if meta.Txn == nil {
		fmt.Fprintf(b, "output: value txn empty\n")
		return
	}
	if ltKey.TxnUUID != meta.Txn.ID {
		fmt.Fprintf(b, "output: key txn id %v != value txn id %v\n", ltKey.TxnUUID, meta.Txn.ID)
		return
	}
	fmt.Fprintf(b, "output: k=%s str=%s txn=%d\n",
		string(ltKey.Key), strings.ToLower(ltKey.Strength.String()), ltKey.TxnUUID.ToUint128().Lo)
}

// TestLockTableIterator is a datadriven test consisting of two commands:
//
//   - define: defines key-value pairs in the lock table through newline
//     separated lock declarations:
//
//     lock k=<key> str=<strength> txn=<txn>
//
//   - iter: for iterating, is defined as:
//
//     iter [lower=<lower>] [upper=<upper>] [prefix=<true|false>] [match-txn-id=<txn>] [match-min-str=<str>]
//
//     followed by newline separated sequence of operations:
//
//     seek-ge k=<key>
//     seek-lt k=<key>
//     seek-ge-with-limit k=<key> limit=<key>
//     seek-lt-with-limit k=<key> limit=<key>
//     next
//     prev
//     next-with-limit limit=<key>
//     prev-with-limit limit=<key>
//     stats
//
// Keys starting with "Gloabl/" are interpreted as global (not lock table) keys.
// Keys without this prefix are interpreted as lock table keys.
func TestLockTableIterator(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Disable the metamorphic value for deterministic iteration stats.
	DisableMetamorphicLockTableItersBeforeSeek(t)

	var eng Engine
	defer func() {
		if eng != nil {
			eng.Close()
		}
	}()

	datadriven.Walk(t, datapathutils.TestDataPath(t, "lock_table_iterator"), func(t *testing.T, path string) {
		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "define":
				if eng != nil {
					eng.Close()
				}
				eng = createTestPebbleEngine()
				batch := eng.NewBatch()
				// pos is the original <file>:<lineno> prefix computed by
				// datadriven. It points to the top "define" command itself.
				// We are editing d.Pos in-place below by extending `pos` upon
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
					case "lock":
						key := scanRoachKey(t, d, "k")
						str := scanLockStrength(t, d, "str")
						txnID := scanTxnID(t, d, "txn")

						ltKey := LockTableKey{Key: key, Strength: str, TxnUUID: txnID}
						eKey, _ := ltKey.ToEngineKey(nil)

						var ltVal enginepb.MVCCMetadata
						ltVal.Txn = &enginepb.TxnMeta{ID: txnID}
						val, err := protoutil.Marshal(&ltVal)
						if err != nil {
							t.Fatal(err)
						}

						if err := batch.PutEngineKey(eKey, val); err != nil {
							t.Fatal(err)
						}
					default:
						t.Fatalf("%s: unknown command %q", d.Pos, d.Cmd)
					}
				}
				d.Pos = pos
				if err := batch.Commit(true); err != nil {
					t.Fatal(err)
				}
				return ""

			case "iter":
				var opts LockTableIteratorOptions
				if d.HasArg("lower") {
					opts.LowerBound = scanLockTableKey(t, d, "lower")
				}
				if d.HasArg("upper") {
					opts.UpperBound = scanLockTableKey(t, d, "upper")
				}
				if d.HasArg("prefix") {
					d.ScanArgs(t, "prefix", &opts.Prefix)
				}
				if d.HasArg("match-txn-id") {
					opts.MatchTxnID = scanTxnID(t, d, "match-txn-id")
				}
				if d.HasArg("match-min-str") {
					opts.MatchMinStr = scanLockStrength(t, d, "match-min-str")
				}
				iter, err := NewLockTableIterator(context.Background(), eng, opts)
				if err != nil {
					return fmt.Sprintf("error constructing new iter: %s", err)
				}
				defer iter.Close()

				var b strings.Builder
				// pos is the original <file>:<lineno> prefix computed by
				// datadriven. It points to the top "define" command itself.
				// We are editing d.Pos in-place below by extending `pos` upon
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
						key := scanLockTableKey(t, d, "k")
						valid, err := iter.SeekEngineKeyGE(EngineKey{Key: key})
						fmt.Fprintf(&b, "seek-ge k=%s: ", printLockTableKey(key))
						checkAndOutputLockTableIterator(iter, &b, valid, err)
					case "seek-lt":
						key := scanLockTableKey(t, d, "k")
						valid, err := iter.SeekEngineKeyLT(EngineKey{Key: key})
						fmt.Fprintf(&b, "seek-lt k=%s: ", printLockTableKey(key))
						checkAndOutputLockTableIterator(iter, &b, valid, err)
					case "seek-ge-with-limit":
						key := scanLockTableKey(t, d, "k")
						limit := scanLockTableKey(t, d, "limit")
						state, err := iter.SeekEngineKeyGEWithLimit(EngineKey{Key: key}, limit)
						fmt.Fprintf(&b, "seek-ge-with-limit k=%s limit=%s: ",
							printLockTableKey(key), printLockTableKey(limit))
						checkAndOutputLockTableIteratorWithState(iter, &b, state, err)
					case "seek-lt-with-limit":
						key := scanLockTableKey(t, d, "k")
						limit := scanLockTableKey(t, d, "limit")
						state, err := iter.SeekEngineKeyLTWithLimit(EngineKey{Key: key}, limit)
						fmt.Fprintf(&b, "seek-lt-with-limit k=%s limit=%s: ",
							printLockTableKey(key), printLockTableKey(limit))
						checkAndOutputLockTableIteratorWithState(iter, &b, state, err)
					case "next":
						valid, err := iter.NextEngineKey()
						fmt.Fprintf(&b, "next: ")
						checkAndOutputLockTableIterator(iter, &b, valid, err)
					case "prev":
						valid, err := iter.PrevEngineKey()
						fmt.Fprintf(&b, "prev: ")
						checkAndOutputLockTableIterator(iter, &b, valid, err)
					case "next-with-limit":
						limit := scanLockTableKey(t, d, "limit")
						state, err := iter.NextEngineKeyWithLimit(limit)
						fmt.Fprintf(&b, "next-with-limit limit=%s: ", printLockTableKey(limit))
						checkAndOutputLockTableIteratorWithState(iter, &b, state, err)
					case "prev-with-limit":
						limit := scanLockTableKey(t, d, "limit")
						state, err := iter.PrevEngineKeyWithLimit(limit)
						fmt.Fprintf(&b, "prev-with-limit limit=%s: ", printLockTableKey(limit))
						checkAndOutputLockTableIteratorWithState(iter, &b, state, err)
					case "stats":
						stats := iter.Stats()
						// Setting non-deterministic InternalStats to empty.
						stats.Stats.InternalStats = pebble.InternalIteratorStats{}
						fmt.Fprintf(&b, "stats: %s\n", stats.Stats.String())
					default:
						t.Fatalf("%s: unknown command %q", d.Pos, d.Cmd)
					}
				}
				d.Pos = pos
				return b.String()
			default:
				t.Fatalf("%s: unknown command %q", d.Pos, d.Cmd)
				return ""
			}
		})
	})
}

// randKey is a random key.
type randKey []byte

func (randKey) generate(r *rand.Rand) randKey {
	k := roachpb.Key(randutil.RandBytes(r, 1))
	if r.Intn(2) != 0 {
		// Randomly generate keys that are the direct successor to other keys.
		k = k.Next()
	}
	return randKey(k)
}

func (k randKey) toRoachKey() roachpb.Key {
	rk, _ := keys.LockTableSingleKey(roachpb.Key(k), nil)
	return rk
}

func (k randKey) toEngineKey() EngineKey {
	return EngineKey{Key: k.toRoachKey()}
}

// randLockStrength is a random lock strength.
type randLockStrength lock.Strength

func (randLockStrength) generate(r *rand.Rand) randLockStrength {
	strs := []lock.Strength{lock.Shared, lock.Exclusive, lock.Intent}
	return randLockStrength(strs[r.Intn(len(strs))])
}

func (s randLockStrength) toStr() lock.Strength {
	return lock.Strength(s)
}

// randTxnID is a random transaction ID.
type randTxnID int

func (randTxnID) generate(r *rand.Rand) randTxnID {
	return randTxnID(r.Intn(10) + 1)
}

func (id randTxnID) toUUID() uuid.UUID {
	return uuid.FromUint128(uint128.FromInts(0, uint64(id)))
}

// randLockTableKey is a quick.Generator for lock table keys.
type randLockTableKey struct {
	k randKey
	s randLockStrength
	t randTxnID
}

func (randLockTableKey) Generate(r *rand.Rand, size int) reflect.Value {
	return reflect.ValueOf(randLockTableKey{
		k: randKey{}.generate(r),
		s: randLockStrength(0).generate(r),
		t: randTxnID(0).generate(r),
	})
}

func (k randLockTableKey) write(t *testing.T, w Writer) {
	ltKey := LockTableKey{
		Key:      roachpb.Key(k.k),
		Strength: k.s.toStr(),
		TxnUUID:  k.t.toUUID(),
	}
	eKey, _ := ltKey.ToEngineKey(nil)

	var ltVal enginepb.MVCCMetadata
	ltVal.Txn = &enginepb.TxnMeta{ID: ltKey.TxnUUID}
	val, err := protoutil.Marshal(&ltVal)
	require.NoError(t, err)

	err = w.PutEngineKey(eKey, val)
	require.NoError(t, err)
}

// randLockTableIterOpKind is a random LockTableIterator operation.
type randLockTableIterOpKind int

const (
	lockTableIterOpKindSeekGE randLockTableIterOpKind = iota
	lockTableIterOpKindSeekLT
	lockTableIterOpKindSeekGEWithLimit
	lockTableIterOpKindSeekLTWithLimit
	lockTableIterOpKindNext
	lockTableIterOpKindPrev
	lockTableIterOpKindNextWithLimit
	lockTableIterOpKindPrevWithLimit
	numLockTableIterOpKinds
)

func (randLockTableIterOpKind) generate(r *rand.Rand) randLockTableIterOpKind {
	return randLockTableIterOpKind(r.Intn(int(numLockTableIterOpKinds)))
}

// lockTableIterOpKindPrefixIterPermitted is a map from randLockTableIterOpKind
// to a boolean indicating whether the operation is compatible with iterators
// configured for prefix iteration.
var lockTableIterOpKindPrefixIterPermitted = [...]bool{
	lockTableIterOpKindSeekGE:          true,
	lockTableIterOpKindSeekLT:          false,
	lockTableIterOpKindSeekGEWithLimit: false,
	lockTableIterOpKindSeekLTWithLimit: false,
	lockTableIterOpKindNext:            true,
	lockTableIterOpKindPrev:            false,
	lockTableIterOpKindNextWithLimit:   false,
	lockTableIterOpKindPrevWithLimit:   false,
}

func (op randLockTableIterOpKind) prefixIterPermitted() bool {
	return lockTableIterOpKindPrefixIterPermitted[op]
}

// randLockTableIterOp is a quick.Generator for LockTableIterator operations.
type randLockTableIterOp struct {
	op    randLockTableIterOpKind
	k     randKey
	limit randKey
}

func (randLockTableIterOp) Generate(r *rand.Rand, size int) reflect.Value {
	return reflect.ValueOf(randLockTableIterOp{
		op:    randLockTableIterOpKind(0).generate(r),
		k:     randKey{}.generate(r),
		limit: randKey{}.generate(r),
	})
}

func (op randLockTableIterOp) prefixIterPermitted() bool {
	return op.op.prefixIterPermitted()
}

func (op randLockTableIterOp) apply(t *testing.T, iter EngineIterator, b *strings.Builder) {
	printKeyValue := func() {
		key, err := iter.UnsafeEngineKey()
		require.NoError(t, err)
		ltKey, err := key.ToLockTableKey()
		require.NoError(t, err)
		fmt.Fprintf(b, "k=%s str=%s txn=%s", string(ltKey.Key), ltKey.Strength, ltKey.TxnUUID)
	}
	printInvalid := func(err error) {
		fmt.Fprintf(b, "valid=false err=%v", err)
	}
	switch op.op {
	case lockTableIterOpKindSeekGE:
		fmt.Fprintf(b, "seek-ge k=%s: ", op.k)
		valid, err := iter.SeekEngineKeyGE(op.k.toEngineKey())
		if !valid {
			printInvalid(err)
		} else {
			printKeyValue()
		}
	case lockTableIterOpKindSeekLT:
		fmt.Fprintf(b, "seek-lt k=%s: ", op.k)
		valid, err := iter.SeekEngineKeyLT(op.k.toEngineKey())
		if !valid {
			printInvalid(err)
		} else {
			printKeyValue()
		}
	case lockTableIterOpKindSeekGEWithLimit:
		fmt.Fprintf(b, "seek-ge-with-limit k=%s limit=%s: ", op.k, op.limit)
		state, err := iter.SeekEngineKeyGEWithLimit(op.k.toEngineKey(), op.limit.toRoachKey())
		if state != pebble.IterValid {
			// NOTE: we don't distinguish between pebble.IterExhausted and
			// pebble.IterAtLimit in this test, as the two are not always
			// equivalent when filtering above an engine iterator vs. filtering
			// before writing to the engine.
			printInvalid(err)
		} else {
			printKeyValue()
		}
	case lockTableIterOpKindSeekLTWithLimit:
		fmt.Fprintf(b, "seek-lt-with-limit k=%s limit=%s: ", op.k, op.limit)
		state, err := iter.SeekEngineKeyLTWithLimit(op.k.toEngineKey(), op.limit.toRoachKey())
		if state != pebble.IterValid {
			printInvalid(err)
		} else {
			printKeyValue()
		}
	case lockTableIterOpKindNext:
		fmt.Fprintf(b, "next: ")
		valid, err := iter.NextEngineKey()
		if !valid {
			printInvalid(err)
		} else {
			printKeyValue()
		}
	case lockTableIterOpKindPrev:
		fmt.Fprintf(b, "prev: ")
		valid, err := iter.PrevEngineKey()
		if !valid {
			printInvalid(err)
		} else {
			printKeyValue()
		}
	case lockTableIterOpKindNextWithLimit:
		fmt.Fprintf(b, "next-with-limit limit=%s ", op.limit)
		state, err := iter.NextEngineKeyWithLimit(op.limit.toRoachKey())
		if state != pebble.IterValid {
			printInvalid(err)
		} else {
			printKeyValue()
		}
	case lockTableIterOpKindPrevWithLimit:
		fmt.Fprintf(b, "prev-with-limit limit=%s ", op.limit)
		state, err := iter.PrevEngineKeyWithLimit(op.limit.toRoachKey())
		if state != pebble.IterValid {
			printInvalid(err)
		} else {
			printKeyValue()
		}
	default:
		panic("unreachable")
	}
	fmt.Fprint(b, "\n")
}

// randLockTableIterFilter is a quick.Generator for LockTableIterator filters.
type randLockTableIterFilter struct {
	matchTxnID  randTxnID
	matchMinStr randLockStrength
}

func (randLockTableIterFilter) Generate(r *rand.Rand, size int) reflect.Value {
	var filter randLockTableIterFilter
	switch r.Intn(3) {
	case 0:
		// Match transaction ID.
		filter.matchTxnID = randTxnID(0).generate(r)
	case 1:
		// Match minimum lock strength.
		filter.matchMinStr = randLockStrength(0).generate(r)
	case 2:
		// Match both transaction ID and minimum lock strength.
		filter.matchTxnID = randTxnID(0).generate(r)
		filter.matchMinStr = randLockStrength(0).generate(r)
	default:
		panic("unreachable")
	}
	return reflect.ValueOf(filter)
}

func (f randLockTableIterFilter) filterKeys(keys []randLockTableKey) []randLockTableKey {
	var res []randLockTableKey
	for _, key := range keys {
		matchTxnID := f.matchTxnID != 0 && f.matchTxnID == key.t
		matchMinStr := f.matchMinStr != 0 && f.matchMinStr <= key.s
		if matchTxnID || matchMinStr {
			res = append(res, key)
		}
	}
	return res
}

// TestLockTableIteratorEquivalence is a quickcheck test that verifies that a
// LockTableIterator is equivalent to a raw engine iterator with non-matching
// lock keys filtered before writing to the engine.
//
// The test does not avoid generating lock combinations that are not possible in
// the lock table in practice, like two exclusive locks held on the same key by
// different transactions. The LockTableIterator does not sufficiently benefit
// for making assumptions about lock compatibility, so we want to verify that it
// does not make any such assumptions.
func TestLockTableIteratorEquivalence(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	lockTableIter := func(
		ks []randLockTableKey, ops []randLockTableIterOp, f randLockTableIterFilter, prefix bool,
	) string {
		eng := createTestPebbleEngine()
		defer eng.Close()

		// Write all keys to the engine, without filtering.
		for _, key := range ks {
			key.write(t, eng)
		}

		// Then use a LockTableIterator with an appropriate filter config.
		iter, err := NewLockTableIterator(context.Background(), eng, LockTableIteratorOptions{
			Prefix:      prefix,
			UpperBound:  keys.LockTableSingleKeyEnd,
			MatchTxnID:  f.matchTxnID.toUUID(),
			MatchMinStr: f.matchMinStr.toStr(),
		})
		require.NoError(t, err)
		defer iter.Close()

		var b strings.Builder
		for _, op := range ops {
			if prefix && !op.prefixIterPermitted() {
				continue
			}
			op.apply(t, iter, &b)
		}
		return b.String()
	}

	preFilterIter := func(
		ks []randLockTableKey, ops []randLockTableIterOp, f randLockTableIterFilter, prefix bool,
	) string {
		eng := createTestPebbleEngine()
		defer eng.Close()

		// Filter the keys before writing them to the engine.
		ks = f.filterKeys(ks)
		for _, key := range ks {
			key.write(t, eng)
		}

		// Then use a raw engine iterator.
		iter, err := eng.NewEngineIterator(context.Background(), IterOptions{
			Prefix:     prefix,
			UpperBound: keys.LockTableSingleKeyEnd,
		})
		require.NoError(t, err)
		defer iter.Close()

		var b strings.Builder
		for _, op := range ops {
			if prefix && !op.prefixIterPermitted() {
				continue
			}
			op.apply(t, iter, &b)
		}
		return b.String()
	}

	require.NoError(t, quick.CheckEqual(lockTableIter, preFilterIter, nil))
}

func TestLockTableItersBeforeSeekHelper(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// Disable the metamorphic value.
	DisableMetamorphicLockTableItersBeforeSeek(t)

	// Check that the value is 5.
	require.Equal(t, 5, lockTableItersBeforeSeek)

	var h lockTableItersBeforeSeekHelper
	keyA, keyB := roachpb.Key("a"), roachpb.Key("b")

	// Seek to keyA. Should start stepping.
	require.False(t, h.shouldSeek(keyA))
	// Step. Same key. Should step again.
	require.False(t, h.shouldSeek(keyA))
	// Step. Same key. Should step again.
	require.False(t, h.shouldSeek(keyA))
	// Step. Same key. Should step again.
	require.False(t, h.shouldSeek(keyA))
	// Step. Same key. Should step again.
	require.False(t, h.shouldSeek(keyA))
	// Step. Same key. Should start seeking.
	require.True(t, h.shouldSeek(keyA))
	// Seek. Same key. Should keep seeking if not new key prefix.
	require.True(t, h.shouldSeek(keyA))
	// Seek. New key. Should start stepping again.
	require.False(t, h.shouldSeek(keyB))

	// Test that the key is copied and not referenced.
	for i := 0; i < lockTableItersBeforeSeek; i++ {
		keyUnstable := roachpb.Key("unstable")
		require.False(t, h.shouldSeek(keyUnstable))
		keyUnstable[0] = 'a'
	}
	require.True(t, h.shouldSeek(roachpb.Key("unstable")))
}
