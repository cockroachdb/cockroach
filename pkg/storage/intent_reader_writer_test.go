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
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
)

func readPrecedingIntentState(t *testing.T, d *datadriven.TestData) PrecedingIntentState {
	var str string
	d.ScanArgs(t, "preceding", &str)
	switch str {
	case "interleaved":
		return ExistingIntentInterleaved
	case "separated":
		return ExistingIntentSeparated
	case "none":
		return NoExistingIntent
	}
	panic("unknown state")
}

func readTxnDidNotUpdateMeta(t *testing.T, d *datadriven.TestData) bool {
	var txnDidNotUpdateMeta bool
	d.ScanArgs(t, "txn-did-not-update-meta", &txnDidNotUpdateMeta)
	return txnDidNotUpdateMeta
}

func printMeta(meta *enginepb.MVCCMetadata) string {
	uuid := meta.Txn.ID.ToUint128()
	var hiStr string
	if uuid.Hi != 0 {
		hiStr = fmt.Sprintf("%d,", uuid.Hi)
	}
	return fmt.Sprintf("meta{ts: %s, txn: %s%d}", meta.Timestamp.String(), hiStr, uuid.Lo)
}

func printLTKey(k LockTableKey) string {
	var id uuid.UUID
	copy(id[:], k.TxnUUID[0:uuid.Size])
	idInt := id.ToUint128()
	var hiStr string
	if idInt.Hi != 0 {
		hiStr = fmt.Sprintf("%d,", idInt.Hi)
	}
	return fmt.Sprintf("LT{k: %s, strength: %s, uuid:%s%d}",
		string(k.Key), k.Strength, hiStr, idInt.Lo)
}

func printEngContents(b *strings.Builder, eng Engine) {
	iter := eng.NewEngineIterator(IterOptions{UpperBound: roachpb.KeyMax})
	defer iter.Close()
	fmt.Fprintf(b, "=== Storage contents ===\n")
	valid, err := iter.SeekEngineKeyGE(EngineKey{Key: []byte("")})
	for valid || err != nil {
		if err != nil {
			fmt.Fprintf(b, "error: %s\n", err.Error())
			break
		}
		var key EngineKey
		if key, err = iter.UnsafeEngineKey(); err != nil {
			fmt.Fprintf(b, "error: %s\n", err.Error())
			break
		}
		var meta enginepb.MVCCMetadata
		if err := protoutil.Unmarshal(iter.UnsafeValue(), &meta); err != nil {
			fmt.Fprintf(b, "error: %s\n", err.Error())
			break
		}
		if key.IsMVCCKey() {
			var k MVCCKey
			if k, err = key.ToMVCCKey(); err != nil {
				fmt.Fprintf(b, "error: %s\n", err.Error())
				break
			}
			fmt.Fprintf(b, "k: %s, v: %s\n", k, printMeta(&meta))
		} else {
			var k LockTableKey
			if k, err = key.ToLockTableKey(); err != nil {
				fmt.Fprintf(b, "error: %s\n", err.Error())
				break
			}
			fmt.Fprintf(b, "k: %s, v: %s\n", printLTKey(k), printMeta(&meta))
		}
		valid, err = iter.NextEngineKey()
	}
}

// printWriter wraps Writer and writes the method call to the strings.Builder.
type printWriter struct {
	Writer
	b strings.Builder
}

var _ Writer = &printWriter{}

func (p *printWriter) reset() {
	p.b.Reset()
	fmt.Fprintf(&p.b, "=== Calls ===\n")
}

func (p *printWriter) ClearUnversioned(key roachpb.Key) error {
	fmt.Fprintf(&p.b, "ClearUnversioned(%s)\n", string(key))
	return p.Writer.ClearUnversioned(key)
}

func (p *printWriter) ClearEngineKey(key EngineKey) error {
	ltKey, err := key.ToLockTableKey()
	var str string
	if err != nil {
		fmt.Fprintf(&p.b, "ClearEngineKey param is not a lock table key: %s\n", key)
		str = fmt.Sprintf("%s", key)
	} else {
		str = printLTKey(ltKey)
	}
	fmt.Fprintf(&p.b, "ClearEngineKey(%s)\n", str)
	return p.Writer.ClearEngineKey(key)
}

func (p *printWriter) ClearRawRange(start, end roachpb.Key) error {
	if bytes.HasPrefix(start, keys.LocalRangeLockTablePrefix) {
		ltStart, err := keys.DecodeLockTableSingleKey(start)
		if err != nil {
			fmt.Fprintf(&p.b, "ClearRawRange start is not a lock table key: %s\n", start)
		}
		ltEnd, err := keys.DecodeLockTableSingleKey(end)
		if err != nil {
			fmt.Fprintf(&p.b, "ClearRawRange end is not a lock table key: %s\n", end)
		}
		fmt.Fprintf(&p.b, "ClearRawRange(LT{%s}, LT{%s})\n", string(ltStart), string(ltEnd))
	} else {
		fmt.Fprintf(&p.b, "ClearRawRange(%s, %s)\n", string(start), string(end))
	}
	return p.Writer.ClearRawRange(start, end)
}

func (p *printWriter) PutUnversioned(key roachpb.Key, value []byte) error {
	var meta enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(value, &meta); err != nil {
		fmt.Fprintf(&p.b, "PutUnversioned value param is not MVCCMetadata: %s\n", err)
	} else {
		fmt.Fprintf(&p.b, "PutUnversioned(%s, %s)\n", string(key), printMeta(&meta))
	}
	return p.Writer.PutUnversioned(key, value)
}

func (p *printWriter) PutEngineKey(key EngineKey, value []byte) error {
	var meta enginepb.MVCCMetadata
	if err := protoutil.Unmarshal(value, &meta); err != nil {
		fmt.Fprintf(&p.b, "PutEngineKey value param is not MVCCMetadata: %s\n", err)
		return p.Writer.PutEngineKey(key, value)
	}
	ltKey, err := key.ToLockTableKey()
	var keyStr string
	if err != nil {
		fmt.Fprintf(&p.b, "ClearEngineKey param is not a lock table key: %s\n", key)
		keyStr = fmt.Sprintf("%s", key)
	} else {
		keyStr = printLTKey(ltKey)
	}
	fmt.Fprintf(&p.b, "PutEngineKey(%s, %s)\n", keyStr, printMeta(&meta))
	return p.Writer.PutEngineKey(key, value)
}

func (p *printWriter) SingleClearEngineKey(key EngineKey) error {
	ltKey, err := key.ToLockTableKey()
	var str string
	if err != nil {
		fmt.Fprintf(&p.b, "SingleClearEngineKey param is not a lock table key: %s\n", key)
		str = fmt.Sprintf("%s", key)
	} else {
		str = printLTKey(ltKey)
	}
	fmt.Fprintf(&p.b, "SingleClearEngineKey(%s)\n", str)
	return p.Writer.SingleClearEngineKey(key)
}

func TestIntentDemuxWriter(t *testing.T) {
	defer leaktest.AfterTest(t)()

	eng := createTestPebbleEngine()
	defer eng.Close()
	pw := printWriter{Writer: eng}
	var w intentDemuxWriter
	var scratch []byte
	var err error
	datadriven.RunTest(t, "testdata/intent_demux_writer",
		func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "new-writer":
				var separated bool
				d.ScanArgs(t, "enable-separated", &separated)
				// This is a low-level test that explicitly wraps the writer, so it
				// doesn't matter how the original call to createTestPebbleEngine
				// behaved in terms of separated intents config.
				w = wrapIntentWriter(context.Background(), &pw,
					makeSettingsForSeparatedIntents(false /* oldClusterVersion */, separated),
					false /* isLongLived */)
				return ""
			case "put-intent":
				pw.reset()
				key := scanRoachKey(t, d, "k")
				// We don't bother populating most fields in the proto.
				var meta enginepb.MVCCMetadata
				var tsS string
				d.ScanArgs(t, "ts", &tsS)
				ts, err := hlc.ParseTimestamp(tsS)
				if err != nil {
					t.Fatalf("%v", err)
				}
				meta.Timestamp = ts.ToLegacyTimestamp()
				var txn int
				d.ScanArgs(t, "txn", &txn)
				txnUUID := uuid.FromUint128(uint128.FromInts(0, uint64(txn)))
				meta.Txn = &enginepb.TxnMeta{ID: txnUUID}
				val, err := protoutil.Marshal(&meta)
				if err != nil {
					return err.Error()
				}
				state := readPrecedingIntentState(t, d)
				txnDidNotUpdateMeta := readTxnDidNotUpdateMeta(t, d)
				var delta int
				scratch, delta, err = w.PutIntent(
					context.Background(), key, val, state, txnDidNotUpdateMeta, txnUUID, scratch)
				if err != nil {
					return err.Error()
				}
				fmt.Fprintf(&pw.b, "Return Value: separated-delta=%d\n", delta)
				printEngContents(&pw.b, eng)
				return pw.b.String()
			case "clear-intent":
				pw.reset()
				key := scanRoachKey(t, d, "k")
				var txn int
				d.ScanArgs(t, "txn", &txn)
				txnUUID := uuid.FromUint128(uint128.FromInts(0, uint64(txn)))
				state := readPrecedingIntentState(t, d)
				txnDidNotUpdateMeta := readTxnDidNotUpdateMeta(t, d)
				var delta int
				scratch, delta, err = w.ClearIntent(key, state, txnDidNotUpdateMeta, txnUUID, scratch)
				if err != nil {
					return err.Error()
				}
				fmt.Fprintf(&pw.b, "Return Value: separated-delta=%d\n", delta)
				printEngContents(&pw.b, eng)
				return pw.b.String()
			case "clear-range":
				pw.reset()
				start := scanRoachKey(t, d, "start")
				end := scanRoachKey(t, d, "end")
				if scratch, err = w.ClearMVCCRangeAndIntents(start, end, scratch); err != nil {
					return err.Error()
				}
				printEngContents(&pw.b, eng)
				return pw.b.String()
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
}
