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
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/lock"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
)

// TODO(sumeer):
// - randomized test with random intents that are interleaved and
//   compare with no interleaved intents without using intentInterleavingIter.
// - microbenchmarks to compare with non-interleaved intents.

func scanRoachKey(t *testing.T, td *datadriven.TestData, field string) roachpb.Key {
	var k string
	td.ScanArgs(t, field, &k)
	rk := roachpb.Key(k)
	if strings.HasPrefix(k, "L") {
		return append(keys.LocalRangePrefix, rk[1:]...)
	}
	return rk
}

func makePrintableKey(k MVCCKey) MVCCKey {
	if bytes.HasPrefix(k.Key, keys.LocalRangePrefix) {
		k.Key = append([]byte("L"), k.Key[len(keys.LocalRangePrefix):]...)
	}
	return k
}

func scanSeekKey(t *testing.T, td *datadriven.TestData) MVCCKey {
	key := MVCCKey{Key: scanRoachKey(t, td, "k")}
	if td.HasArg("ts") {
		var ts int
		td.ScanArgs(t, "ts", &ts)
		key.Timestamp.WallTime = int64(ts)
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
			fmt.Fprintf(b, "output: meta k=%s ts=%d txn=%s%d\n",
				string(k1.Key), meta.Timestamp.WallTime, hiStr, uuid.Lo)
		}
		return
	}
	fmt.Fprintf(b, "output: value k=%s ts=%d v=%s\n",
		string(k1.Key), k1.Timestamp.WallTime, string(v1))
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
//     next, prev, seek-lt, seek-ge, set-upper, next-key
//
// A key starting with L is interpreted as a local-range key.
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
							var ts, txn int
							d.ScanArgs(t, "ts", &ts)
							meta.Timestamp.WallTime = int64(ts)
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
							if err := batch.PutEngineKey(ltKey.ToEngineKey(), val); err != nil {
								return err.Error()
							}
						}
					case "value":
						if locksSection {
							t.Fatalf("%s: value in locks section", d.Pos)
						}
						key := scanRoachKey(t, d, "k")
						var ts int
						d.ScanArgs(t, "ts", &ts)
						var value string
						d.ScanArgs(t, "v", &value)
						mvccKey := MVCCKey{Key: key, Timestamp: hlc.Timestamp{WallTime: int64(ts)}}
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
				iter := newIntentInterleavingIterator(eng, opts)
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
