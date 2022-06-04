// Copyright 2019 The Cockroach Authors.
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
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/uncertainty"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/descpb"
	"github.com/cockroachdb/cockroach/pkg/sql/rowenc"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/cockroachdb/redact"
)

// TestMVCCHistories verifies that sequences of MVCC reads and writes
// perform properly.
//
// The input files use the following DSL:
//
// run            [ok|trace|stats|error]
//
// txn_begin      t=<name> [ts=<int>[,<int>]] [globalUncertaintyLimit=<int>[,<int>]]
// txn_remove     t=<name>
// txn_restart    t=<name>
// txn_update     t=<name> t2=<name>
// txn_step       t=<name> [n=<int>]
// txn_advance    t=<name> ts=<int>[,<int>]
// txn_status     t=<name> status=<txnstatus>
//
// resolve_intent t=<name> k=<key> [status=<txnstatus>] [clockWhilePending=<int>[,<int>]]
// check_intent   k=<key> [none]
//
// cput           [t=<name>] [ts=<int>[,<int>]] [localTs=<int>[,<int>]] [resolve [status=<txnstatus>]] k=<key> v=<string> [raw] [cond=<string>]
// del            [t=<name>] [ts=<int>[,<int>]] [localTs=<int>[,<int>]] [resolve [status=<txnstatus>]] k=<key>
// del_range      [t=<name>] [ts=<int>[,<int>]] [localTs=<int>[,<int>]] [resolve [status=<txnstatus>]] k=<key> [end=<key>] [max=<max>] [returnKeys]
// increment      [t=<name>] [ts=<int>[,<int>]] [localTs=<int>[,<int>]] [resolve [status=<txnstatus>]] k=<key> [inc=<val>]
// put            [t=<name>] [ts=<int>[,<int>]] [localTs=<int>[,<int>]] [resolve [status=<txnstatus>]] k=<key> v=<string> [raw]
// put_rangekey   ts=<int>[,<int>] [localTS=<int>[,<int>]] k=<key> end=<key>
// get            [t=<name>] [ts=<int>[,<int>]]                         [resolve [status=<txnstatus>]] k=<key> [inconsistent] [tombstones] [failOnMoreRecent] [localUncertaintyLimit=<int>[,<int>]] [globalUncertaintyLimit=<int>[,<int>]]
// scan           [t=<name>] [ts=<int>[,<int>]]                         [resolve [status=<txnstatus>]] k=<key> [end=<key>] [inconsistent] [tombstones] [reverse] [failOnMoreRecent] [localUncertaintyLimit=<int>[,<int>]] [globalUncertaintyLimit=<int>[,<int>]] [max=<max>] [targetbytes=<target>] [avoidExcess] [allowEmpty]
//
// iter_new       [k=<key>] [end=<key>] [prefix] [kind=key|keyAndIntents] [types=pointsOnly|pointsWithRanges|pointsAndRanges|rangesOnly]
// iter_seek_ge   k=<key> [ts=<int>[,<int>]]
// iter_seek_lt   k=<key> [ts=<int>[,<int>]]
// iter_seek_intent_ge k=<key> txn=<name>
// iter_next
// iter_next_key
// iter_prev
// iter_scan      [reverse]
//
// merge     [ts=<int>[,<int>]] k=<key> v=<string> [raw]
//
// clear				  k=<key> [ts=<int>[,<int>]]
// clear_range    k=<key> end=<key>
// clear_rangekey k=<key> end=<key> ts=<int>[,<int>]
//
// Where `<key>` can be a simple string, or a string
// prefixed by the following characters:
//
// - `=foo` means exactly key `foo`
// - `+foo` means `Key(foo).Next()`
// - `-foo` means `Key(foo).PrefixEnd()`
// - `%foo` means `append(LocalRangePrefix, "foo")`
// - `/foo/7` means SQL row with key foo, optional column family 7 (system tenant, table/index 1).
//
// Additionally, the pseudo-command `with` enables sharing
// a group of arguments between multiple commands, for example:
//   with t=A
//     txn_begin
//     with k=a
//       put v=b
//       resolve_intent
// Really means:
//   txn_begin          t=A
//   put v=b        k=a t=A
//   resolve_intent k=a t=A
//
func TestMVCCHistories(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	SkipIfSimpleValueEncodingDisabled(t)

	ctx := context.Background()

	// Everything reads/writes under the same prefix.
	span := roachpb.Span{Key: keys.LocalMax, EndKey: roachpb.KeyMax}

	// Timestamp for MVCC stats calculations, in nanoseconds.
	const statsTS = 100e9

	datadriven.Walk(t, testutils.TestDataPath(t, "mvcc_histories"), func(t *testing.T, path string) {
		// We start from a clean slate in every test file.
		engine, err := Open(ctx, InMemory(), CacheSize(1<<20 /* 1 MiB */), ForTesting)
		if err != nil {
			t.Fatal(err)
		}
		defer engine.Close()

		if strings.HasSuffix(path, "_norace") {
			skip.UnderRace(t)
		}

		if strings.Contains(path, "_disable_local_timestamps") {
			localTimestampsEnabled.Override(ctx, &engine.settings.SV, false)
		}

		reportDataEntries := func(buf *redact.StringBuilder) error {
			var hasData bool

			iter := engine.NewMVCCIterator(MVCCKeyIterKind, IterOptions{
				KeyTypes:   IterKeyTypeRangesOnly,
				LowerBound: span.Key,
				UpperBound: span.EndKey,
			})
			defer iter.Close()
			iter.SeekGE(MVCCKey{Key: span.Key})
			for {
				if ok, err := iter.Valid(); err != nil {
					return err
				} else if !ok {
					break
				}
				hasData = true
				buf.Printf("rangekey: %s/[", iter.RangeBounds())
				for i, rangeKV := range iter.RangeKeys() {
					val, err := DecodeMVCCValue(rangeKV.Value)
					if err != nil {
						t.Fatal(err)
					}
					if i > 0 {
						buf.Printf(" ")
					}
					buf.Printf("%s=%s", rangeKV.RangeKey.Timestamp, val)
				}
				buf.Printf("]\n")
				iter.Next()
			}

			err = engine.MVCCIterate(span.Key, span.EndKey, MVCCKeyAndIntentsIterKind, func(r MVCCKeyValue) error {
				hasData = true
				if r.Key.Timestamp.IsEmpty() {
					// Meta is at timestamp zero.
					meta := enginepb.MVCCMetadata{}
					if err := protoutil.Unmarshal(r.Value, &meta); err != nil {
						buf.Printf("meta: %v -> error decoding proto from %v: %v\n", r.Key, r.Value, err)
					} else {
						buf.Printf("meta: %v -> %+v\n", r.Key, &meta)
					}
				} else {
					val, err := DecodeMVCCValue(r.Value)
					if err != nil {
						buf.Printf("data: %v -> error decoding value %v: %v\n", r.Key, r.Value, err)
					} else {
						buf.Printf("data: %v -> %s\n", r.Key, val)
					}
				}
				return nil
			})
			if !hasData {
				buf.SafeString("<no data>\n")
			}
			return err
		}

		e := newEvalCtx(ctx, engine)
		defer e.close()

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			// We'll be overriding cmd/cmdargs below, because the
			// datadriven reader does not know about sub-commands.
			defer func(pos, cmd string, cmdArgs []datadriven.CmdArg) {
				d.Pos = pos
				d.Cmd = cmd
				d.CmdArgs = cmdArgs
			}(d.Pos, d.Cmd, d.CmdArgs)
			// The various evalCtx helpers want access to the current test
			// and testdata structs.
			e.t = t
			e.td = d

			switch d.Cmd {
			case "skip":
				if len(d.CmdArgs) == 0 {
					skip.IgnoreLint(e.t, "skipped")
				}
				return d.Expected
			case "run":
				// Syntax: run [trace] [error]
				// (other words - in particular "ok" - are accepted but ignored)
				//
				// "run" executes a script of zero or more operations from
				// the commands library defined below.
				// It stops upon the first error encountered, if any.
				//
				// Options:
				// - trace: emit intermediate results after each operation.
				// - stats: emit MVCC statistics for each operation and at the end.
				// - error: expect an error to occur. The specific error type/ message
				//   to expect is spelled out in the expected output.
				//
				trace := e.hasArg("trace")
				stats := e.hasArg("stats")
				expectError := e.hasArg("error")

				// buf will accumulate the actual output, which the
				// datadriven driver will use to compare to the expected
				// output.
				var buf redact.StringBuilder
				e.results.buf = &buf
				e.results.traceIntentWrites = trace

				// foundErr remembers which error was last encountered while
				// executing the script under "run".
				var foundErr error

				// pos is the original <file>:<lineno> prefix computed by
				// datadriven. It points to the top "run" command itself.
				// We editing d.Pos in-place below by extending `pos` upon
				// each new line of the script.
				pos := d.Pos

				// dataChange indicates whether some command in the script
				// has modified the stored data. When this becomes true, the
				// current content of storage is printed in the results
				// buffer at the end.
				dataChange := false
				// txnChange indicates whether some command has modified
				// a transaction object. When set, the last modified txn
				// object is reported in the result buffer at the end.
				txnChange := false

				reportResults := func(printTxn, printData bool) {
					if printTxn && e.results.txn != nil {
						buf.Printf("txn: %v\n", e.results.txn)
					}
					if printData {
						err := reportDataEntries(&buf)
						if err != nil {
							if foundErr == nil {
								// Handle the error below.
								foundErr = err
							} else {
								buf.Printf("error reading data: (%T:) %v\n", err, err)
							}
						}
					}
				}

				// sharedCmdArgs is updated by "with" pseudo-commands,
				// to pre-populate common arguments for the following
				// indented commands.
				var sharedCmdArgs []datadriven.CmdArg

				// The lines of the script under "run".
				lines := strings.Split(d.Input, "\n")
				for i, line := range lines {
					if short := strings.TrimSpace(line); short == "" || strings.HasPrefix(short, "#") {
						// Comment or empty line. Do nothing.
						continue
					}

					// Compute a line prefix, to clarify error message. We
					// prefix a newline character because some text editor do
					// not know how to jump to the location of an error if
					// there are multiple file:line prefixes on the same line.
					d.Pos = fmt.Sprintf("\n%s: (+%d)", pos, i+1)

					// Trace the execution in testing.T, to clarify where we
					// are in case an error occurs.
					log.Infof(context.Background(), "TestMVCCHistories:\n\t%s: %s", d.Pos, line)

					// Decompose the current script line.
					var err error
					d.Cmd, d.CmdArgs, err = datadriven.ParseLine(line)
					if err != nil {
						e.t.Fatalf("%s: %v", d.Pos, err)
					}

					// Expand "with" commands:
					//   with t=A
					//       txn_begin
					//       resolve_intent k=a
					// is equivalent to:
					//   txn_begin      t=A
					//   resolve_intent k=a t=A
					isIndented := strings.TrimLeft(line, " \t") != line
					if d.Cmd == "with" {
						if !isIndented {
							// Reset shared args.
							sharedCmdArgs = d.CmdArgs
						} else {
							// Prefix shared args. We use prefix so that the
							// innermost "with" can override/shadow the outermost
							// "with".
							sharedCmdArgs = append(d.CmdArgs, sharedCmdArgs...)
						}
						continue
					} else if isIndented {
						// line is indented. Inherit arguments.
						if len(sharedCmdArgs) == 0 {
							// sanity check.
							e.Fatalf("indented command without prior 'with': %s", line)
						}
						// We prepend the args that are provided on the command
						// itself so it's possible to override those provided
						// via "with".
						d.CmdArgs = append(d.CmdArgs, sharedCmdArgs...)
					} else {
						// line is not indented. Clear shared arguments.
						sharedCmdArgs = nil
					}

					cmd := e.getCmd()
					txnChange = txnChange || cmd.typ == typTxnUpdate
					dataChange = dataChange || cmd.typ == typDataUpdate

					if trace || (stats && cmd.typ == typDataUpdate) {
						// If tracing is also requested by the datadriven input,
						// we'll trace the statement in the actual results too.
						buf.Printf(">> %s", d.Cmd)
						for i := range d.CmdArgs {
							buf.Printf(" %s", &d.CmdArgs[i])
						}
						_ = buf.WriteByte('\n')
					}

					var msInitial enginepb.MVCCStats
					if stats {
						msInitial = computeStats(e.t, e.engine, span.Key, span.EndKey, statsTS)
					}

					// Run the command.
					e.ms = &enginepb.MVCCStats{}
					foundErr = cmd.fn(e)

					if trace {
						// If tracing is enabled, we report the intermediate results
						// after each individual step in the script.
						// This may modify foundErr too.
						reportResults(cmd.typ == typTxnUpdate, cmd.typ == typDataUpdate)
					}

					if stats && cmd.typ == typDataUpdate {
						// If stats are enabled, emit evaluated stats returned by the
						// command, and compare them with the real computed stats diff.
						msEval := *e.ms
						msEval.AgeTo(statsTS)
						buf.Printf("stats: %s\n", formatStats(msEval, true))

						msDiff := computeStats(e.t, e.engine, span.Key, span.EndKey, statsTS)
						msDiff.Subtract(msInitial)
						if msEval != msDiff {
							e.t.Errorf("MVCC stats mismatch for %q at %s\nReturned: %s\nExpected: %s",
								d.Cmd, d.Pos, formatStats(msEval, true), formatStats(msDiff, true))
						}
					}

					if foundErr != nil {
						// An error occurred. Stop the script prematurely.
						break
					}
				}
				// End of script.

				if !trace {
					// If we were not tracing, no results were printed yet. Do it now.
					if txnChange || dataChange {
						buf.SafeString(">> at end:\n")
					}
					reportResults(txnChange, dataChange)
				}

				// Calculate and output final stats if requested and the data changed.
				if stats && dataChange {
					ms := computeStats(t, e.engine, span.Key, span.EndKey, statsTS)
					buf.Printf("stats: %s\n", formatStats(ms, false))
				}

				signalError := e.t.Errorf
				if txnChange || dataChange {
					// We can't recover from an error and continue
					// to proceed further tests, because the state
					// may have changed from what the test may be expecting.
					signalError = e.t.Fatalf
				}

				// Check for errors.
				if foundErr == nil && expectError {
					signalError("%s: expected error, got success", d.Pos)
					return d.Expected
				} else if foundErr != nil {
					if expectError {
						buf.Printf("error: (%T:) %v\n", foundErr, foundErr)
					} else /* !expectError */ {
						signalError("%s: expected success, found: (%T:) %v", d.Pos, foundErr, foundErr)
						return d.Expected
					}
				}

				// We're done. Report the actual results and errors to the
				// datadriven executor.
				return buf.String()

			default:
				e.t.Errorf("%s: unknown command: %s", d.Pos, d.Cmd)
				return d.Expected
			}
		})
	})
}

// getCmd retrieves the cmd entry for the current script line.
func (e *evalCtx) getCmd() cmd {
	e.t.Helper()
	c, ok := commands[e.td.Cmd]
	if !ok {
		e.Fatalf("unknown command: %s", e.td.Cmd)
	}
	return c
}

// cmd represents one supported script command.
type cmd struct {
	typ cmdType
	fn  func(e *evalCtx) error
}

type cmdType int

const (
	typReadOnly cmdType = iota
	typTxnUpdate
	typDataUpdate
)

// commands is the list of all supported script commands.
var commands = map[string]cmd{
	"txn_advance":     {typTxnUpdate, cmdTxnAdvance},
	"txn_begin":       {typTxnUpdate, cmdTxnBegin},
	"txn_ignore_seqs": {typTxnUpdate, cmdTxnIgnoreSeqs},
	"txn_remove":      {typTxnUpdate, cmdTxnRemove},
	"txn_restart":     {typTxnUpdate, cmdTxnRestart},
	"txn_status":      {typTxnUpdate, cmdTxnSetStatus},
	"txn_step":        {typTxnUpdate, cmdTxnStep},
	"txn_update":      {typTxnUpdate, cmdTxnUpdate},

	"resolve_intent": {typDataUpdate, cmdResolveIntent},
	// TODO(nvanbenschoten): test "resolve_intent_range".
	"check_intent": {typReadOnly, cmdCheckIntent},

	"clear":          {typDataUpdate, cmdClear},
	"clear_range":    {typDataUpdate, cmdClearRange},
	"clear_rangekey": {typDataUpdate, cmdClearRangeKey},
	"cput":           {typDataUpdate, cmdCPut},
	"del":            {typDataUpdate, cmdDelete},
	"del_range":      {typDataUpdate, cmdDeleteRange},
	"get":            {typReadOnly, cmdGet},
	"increment":      {typDataUpdate, cmdIncrement},
	"merge":          {typDataUpdate, cmdMerge},
	"put":            {typDataUpdate, cmdPut},
	"put_rangekey":   {typDataUpdate, cmdPutRangeKey},
	"scan":           {typReadOnly, cmdScan},

	"iter_new":            {typReadOnly, cmdIterNew},
	"iter_seek_ge":        {typReadOnly, cmdIterSeekGE},
	"iter_seek_lt":        {typReadOnly, cmdIterSeekLT},
	"iter_seek_intent_ge": {typReadOnly, cmdIterSeekIntentGE},
	"iter_next":           {typReadOnly, cmdIterNext},
	"iter_next_key":       {typReadOnly, cmdIterNextKey},
	"iter_prev":           {typReadOnly, cmdIterPrev},
	"iter_scan":           {typReadOnly, cmdIterScan},
}

func cmdTxnAdvance(e *evalCtx) error {
	txn := e.getTxn(mandatory)
	ts := e.getTs(txn)
	if ts.Less(txn.ReadTimestamp) {
		e.Fatalf("cannot advance txn to earlier (%s) than its ReadTimestamp (%s)",
			ts, txn.ReadTimestamp)
	}
	txn.WriteTimestamp = ts
	e.results.txn = txn
	return nil
}

func cmdTxnBegin(e *evalCtx) error {
	var txnName string
	e.scanArg("t", &txnName)
	ts := e.getTs(nil)
	globalUncertaintyLimit := e.getTsWithName("globalUncertaintyLimit")
	key := roachpb.KeyMin
	if e.hasArg("k") {
		key = e.getKey()
	}
	txn, err := e.newTxn(txnName, ts, globalUncertaintyLimit, key)
	e.results.txn = txn
	return err
}

func cmdTxnIgnoreSeqs(e *evalCtx) error {
	txn := e.getTxn(mandatory)
	seql := e.getList("seqs")
	is := []enginepb.IgnoredSeqNumRange{}
	for _, s := range seql {
		parts := strings.Split(s, "-")
		if len(parts) != 2 {
			e.Fatalf("syntax error: expected 'a-b', got: '%s'", s)
		}
		a, err := strconv.ParseInt(parts[0], 10, 32)
		if err != nil {
			e.Fatalf("%v", err)
		}
		b, err := strconv.ParseInt(parts[1], 10, 32)
		if err != nil {
			e.Fatalf("%v", err)
		}
		is = append(is, enginepb.IgnoredSeqNumRange{Start: enginepb.TxnSeq(a), End: enginepb.TxnSeq(b)})
	}
	txn.IgnoredSeqNums = is
	e.results.txn = txn
	return nil
}

func cmdTxnRemove(e *evalCtx) error {
	txn := e.getTxn(mandatory)
	delete(e.txns, txn.Name)
	e.results.txn = nil
	return nil
}

func cmdTxnRestart(e *evalCtx) error {
	txn := e.getTxn(mandatory)
	ts := e.getTs(txn)
	up := roachpb.NormalUserPriority
	tp := enginepb.MinTxnPriority
	txn.Restart(up, tp, ts)
	e.results.txn = txn
	return nil
}

func cmdTxnSetStatus(e *evalCtx) error {
	txn := e.getTxn(mandatory)
	status := e.getTxnStatus()
	txn.Status = status
	e.results.txn = txn
	return nil
}

func cmdTxnStep(e *evalCtx) error {
	txn := e.getTxn(mandatory)
	n := 1
	if e.hasArg("seq") {
		e.scanArg("seq", &n)
		txn.Sequence = enginepb.TxnSeq(n)
	} else {
		if e.hasArg("n") {
			e.scanArg("n", &n)
		}
		txn.Sequence += enginepb.TxnSeq(n)
	}
	e.results.txn = txn
	return nil
}

func cmdTxnUpdate(e *evalCtx) error {
	txn := e.getTxn(mandatory)
	var txnName2 string
	e.scanArg("t2", &txnName2)
	txn2, err := e.lookupTxn(txnName2)
	if err != nil {
		e.Fatalf("%v", err)
	}
	txn.Update(txn2)
	e.results.txn = txn
	return nil
}

type intentPrintingReadWriter struct {
	ReadWriter
	buf *redact.StringBuilder
}

func (rw intentPrintingReadWriter) PutIntent(
	ctx context.Context, key roachpb.Key, value []byte, txnUUID uuid.UUID,
) error {
	rw.buf.Printf("called PutIntent(%v, _, %v)\n",
		key, txnUUID)
	return rw.ReadWriter.PutIntent(ctx, key, value, txnUUID)
}

func (rw intentPrintingReadWriter) ClearIntent(
	key roachpb.Key, txnDidNotUpdateMeta bool, txnUUID uuid.UUID,
) error {
	rw.buf.Printf("called ClearIntent(%v, TDNUM(%t), %v)\n",
		key, txnDidNotUpdateMeta, txnUUID)
	return rw.ReadWriter.ClearIntent(key, txnDidNotUpdateMeta, txnUUID)
}

func (e *evalCtx) tryWrapForIntentPrinting(rw ReadWriter) ReadWriter {
	if e.results.traceIntentWrites {
		return intentPrintingReadWriter{ReadWriter: rw, buf: e.results.buf}
	}
	return rw
}

func cmdResolveIntent(e *evalCtx) error {
	txn := e.getTxn(mandatory)
	key := e.getKey()
	status := e.getTxnStatus()
	clockWhilePending := hlc.ClockTimestamp(e.getTsWithName("clockWhilePending"))
	return e.resolveIntent(e.tryWrapForIntentPrinting(e.engine), key, txn, status, clockWhilePending)
}

func (e *evalCtx) resolveIntent(
	rw ReadWriter,
	key roachpb.Key,
	txn *roachpb.Transaction,
	resolveStatus roachpb.TransactionStatus,
	clockWhilePending hlc.ClockTimestamp,
) error {
	intent := roachpb.MakeLockUpdate(txn, roachpb.Span{Key: key})
	intent.Status = resolveStatus
	intent.ClockWhilePending = roachpb.ObservedTimestamp{Timestamp: clockWhilePending}
	_, err := MVCCResolveWriteIntent(e.ctx, rw, e.ms, intent)
	return err
}

func cmdCheckIntent(e *evalCtx) error {
	key := e.getKey()
	wantIntent := true
	if e.hasArg("none") {
		wantIntent = false
	}
	metaKey := mvccKey(key)
	var meta enginepb.MVCCMetadata
	ok, _, _, err := e.engine.MVCCGetProto(metaKey, &meta)
	if err != nil {
		return err
	}
	if !ok && wantIntent {
		return errors.Newf("meta: %v -> expected intent, found none", key)
	}
	if ok {
		e.results.buf.Printf("meta: %v -> %+v\n", key, &meta)
		if !wantIntent {
			return errors.Newf("meta: %v -> expected no intent, found one", key)
		}
	}
	return nil
}

func cmdClear(e *evalCtx) error {
	key := e.getKey()
	ts := e.getTs(nil)
	return e.engine.ClearMVCC(MVCCKey{Key: key, Timestamp: ts})
}

func cmdClearRange(e *evalCtx) error {
	key, endKey := e.getKeyRange()
	// NB: We can't test ClearRawRange or ClearRangeUsingHeuristic here, because
	// it does not handle separated intents.
	if util.ConstantWithMetamorphicTestBool("clear-range-using-iterator", false) {
		return e.engine.ClearMVCCIteratorRange(key, endKey)
	}
	return e.engine.ClearMVCCRange(key, endKey)
}

func cmdClearRangeKey(e *evalCtx) error {
	key, endKey := e.getKeyRange()
	ts := e.getTs(nil)
	return e.engine.ExperimentalClearMVCCRangeKey(
		MVCCRangeKey{StartKey: key, EndKey: endKey, Timestamp: ts})
}

func cmdCPut(e *evalCtx) error {
	txn := e.getTxn(optional)
	ts := e.getTs(txn)
	localTs := hlc.ClockTimestamp(e.getTsWithName("localTs"))

	key := e.getKey()
	val := e.getVal()
	// Condition val is optional.
	var expVal []byte
	if e.hasArg("cond") {
		rexpVal := e.getValInternal("cond")
		expVal = rexpVal.TagAndDataBytes()
	}
	behavior := CPutFailIfMissing
	if e.hasArg("allow_missing") {
		behavior = CPutAllowIfMissing
	}
	resolve, resolveStatus := e.getResolve()

	return e.withWriter("cput", func(rw ReadWriter) error {
		if err := MVCCConditionalPut(e.ctx, rw, e.ms, key, ts, localTs, val, expVal, behavior, txn); err != nil {
			return err
		}
		if resolve {
			return e.resolveIntent(rw, key, txn, resolveStatus, hlc.ClockTimestamp{})
		}
		return nil
	})
}

func cmdDelete(e *evalCtx) error {
	txn := e.getTxn(optional)
	key := e.getKey()
	ts := e.getTs(txn)
	localTs := hlc.ClockTimestamp(e.getTsWithName("localTs"))
	resolve, resolveStatus := e.getResolve()
	return e.withWriter("del", func(rw ReadWriter) error {
		if err := MVCCDelete(e.ctx, rw, e.ms, key, ts, localTs, txn); err != nil {
			return err
		}
		if resolve {
			return e.resolveIntent(rw, key, txn, resolveStatus, hlc.ClockTimestamp{})
		}
		return nil
	})
}

func cmdDeleteRange(e *evalCtx) error {
	txn := e.getTxn(optional)
	key, endKey := e.getKeyRange()
	ts := e.getTs(txn)
	localTs := hlc.ClockTimestamp(e.getTsWithName("localTs"))
	returnKeys := e.hasArg("returnKeys")
	max := 0
	if e.hasArg("max") {
		e.scanArg("max", &max)
	}

	resolve, resolveStatus := e.getResolve()
	return e.withWriter("del_range", func(rw ReadWriter) error {
		deleted, resumeSpan, num, err := MVCCDeleteRange(
			e.ctx, rw, e.ms, key, endKey, int64(max), ts, localTs, txn, returnKeys)
		if err != nil {
			return err
		}
		e.results.buf.Printf("del_range: %v-%v -> deleted %d key(s)\n", key, endKey, num)
		for _, key := range deleted {
			e.results.buf.Printf("del_range: returned %v\n", key)
		}
		if resumeSpan != nil {
			e.results.buf.Printf("del_range: resume span [%s,%s)\n", resumeSpan.Key, resumeSpan.EndKey)
		}

		if resolve {
			return e.resolveIntent(rw, key, txn, resolveStatus, hlc.ClockTimestamp{})
		}
		return nil
	})
}

func cmdGet(e *evalCtx) error {
	txn := e.getTxn(optional)
	key := e.getKey()
	ts := e.getTs(txn)
	opts := MVCCGetOptions{Txn: txn}
	if e.hasArg("inconsistent") {
		opts.Inconsistent = true
		opts.Txn = nil
	}
	if e.hasArg("tombstones") {
		opts.Tombstones = true
	}
	if e.hasArg("failOnMoreRecent") {
		opts.FailOnMoreRecent = true
	}
	opts.Uncertainty = uncertainty.Interval{
		GlobalLimit: e.getTsWithName("globalUncertaintyLimit"),
		LocalLimit:  hlc.ClockTimestamp(e.getTsWithName("localUncertaintyLimit")),
	}
	if opts.Txn != nil {
		if !opts.Uncertainty.GlobalLimit.IsEmpty() {
			e.Fatalf("globalUncertaintyLimit arg incompatible with txn")
		}
		opts.Uncertainty.GlobalLimit = txn.GlobalUncertaintyLimit
	}
	val, intent, err := MVCCGet(e.ctx, e.engine, key, ts, opts)
	// NB: the error is returned below. This ensures the test can
	// ascertain no result is populated in the intent when an error
	// occurs.
	if intent != nil {
		e.results.buf.Printf("get: %v -> intent {%s}\n", key, intent.Txn)
	}
	if val != nil {
		e.results.buf.Printf("get: %v -> %v @%v\n", key, val.PrettyPrint(), val.Timestamp)
	} else {
		e.results.buf.Printf("get: %v -> <no data>\n", key)
	}
	return err
}

func cmdIncrement(e *evalCtx) error {
	txn := e.getTxn(optional)
	ts := e.getTs(txn)
	localTs := hlc.ClockTimestamp(e.getTsWithName("localTs"))

	key := e.getKey()
	inc := int64(1)
	if e.hasArg("inc") {
		var incI int
		e.scanArg("inc", &incI)
		inc = int64(incI)
	}

	resolve, resolveStatus := e.getResolve()

	return e.withWriter("increment", func(rw ReadWriter) error {
		curVal, err := MVCCIncrement(e.ctx, rw, e.ms, key, ts, localTs, txn, inc)
		if err != nil {
			return err
		}
		e.results.buf.Printf("inc: current value = %d\n", curVal)
		if resolve {
			return e.resolveIntent(rw, key, txn, resolveStatus, hlc.ClockTimestamp{})
		}
		return nil
	})
}

func cmdMerge(e *evalCtx) error {
	key := e.getKey()
	val := e.getVal()
	ts := e.getTs(nil)
	return e.withWriter("merge", func(rw ReadWriter) error {
		return MVCCMerge(e.ctx, rw, e.ms, key, ts, val)
	})
}

func cmdPut(e *evalCtx) error {
	txn := e.getTxn(optional)
	ts := e.getTs(txn)
	localTs := hlc.ClockTimestamp(e.getTsWithName("localTs"))

	key := e.getKey()
	val := e.getVal()

	resolve, resolveStatus := e.getResolve()

	return e.withWriter("put", func(rw ReadWriter) error {
		if err := MVCCPut(e.ctx, rw, e.ms, key, ts, localTs, val, txn); err != nil {
			return err
		}
		if resolve {
			return e.resolveIntent(rw, key, txn, resolveStatus, hlc.ClockTimestamp{})
		}
		return nil
	})
}

func cmdScan(e *evalCtx) error {
	txn := e.getTxn(optional)
	key, endKey := e.getKeyRange()
	ts := e.getTs(txn)
	opts := MVCCScanOptions{Txn: txn}
	if e.hasArg("inconsistent") {
		opts.Inconsistent = true
		opts.Txn = nil
	}
	if e.hasArg("tombstones") {
		opts.Tombstones = true
	}
	if e.hasArg("reverse") {
		opts.Reverse = true
	}
	if e.hasArg("failOnMoreRecent") {
		opts.FailOnMoreRecent = true
	}
	opts.Uncertainty = uncertainty.Interval{
		GlobalLimit: e.getTsWithName("globalUncertaintyLimit"),
		LocalLimit:  hlc.ClockTimestamp(e.getTsWithName("localUncertaintyLimit")),
	}
	if opts.Txn != nil {
		if !opts.Uncertainty.GlobalLimit.IsEmpty() {
			e.Fatalf("globalUncertaintyLimit arg incompatible with txn")
		}
		opts.Uncertainty.GlobalLimit = txn.GlobalUncertaintyLimit
	}
	if e.hasArg("max") {
		var n int
		e.scanArg("max", &n)
		opts.MaxKeys = int64(n)
	}
	if key := "targetbytes"; e.hasArg(key) {
		var tb int
		e.scanArg(key, &tb)
		opts.TargetBytes = int64(tb)
	}
	if e.hasArg("avoidExcess") {
		opts.TargetBytesAvoidExcess = true
	}
	if e.hasArg("allowEmpty") {
		opts.AllowEmpty = true
	}
	if e.hasArg("wholeRows") {
		opts.WholeRowsOfSize = 10 // arbitrary, must be greater than largest column family in tests
	}
	res, err := MVCCScan(e.ctx, e.engine, key, endKey, ts, opts)
	// NB: the error is returned below. This ensures the test can
	// ascertain no result is populated in the intents when an error
	// occurs.
	for _, intent := range res.Intents {
		e.results.buf.Printf("scan: %v -> intent {%s}\n", key, intent.Txn)
	}
	for _, val := range res.KVs {
		e.results.buf.Printf("scan: %v -> %v @%v\n", val.Key, val.Value.PrettyPrint(), val.Value.Timestamp)
	}
	if res.ResumeSpan != nil {
		e.results.buf.Printf("scan: resume span [%s,%s) %s nextBytes=%d\n", res.ResumeSpan.Key, res.ResumeSpan.EndKey, res.ResumeReason, res.ResumeNextBytes)
	}
	if opts.TargetBytes > 0 {
		e.results.buf.Printf("scan: %d bytes (target %d)\n", res.NumBytes, opts.TargetBytes)
	}
	if len(res.KVs) == 0 {
		e.results.buf.Printf("scan: %v-%v -> <no data>\n", key, endKey)
	}
	return err
}

func cmdPutRangeKey(e *evalCtx) error {
	var rangeKey MVCCRangeKey
	rangeKey.StartKey, rangeKey.EndKey = e.getKeyRange()
	rangeKey.Timestamp = e.getTs(nil)
	var value MVCCValue
	value.MVCCValueHeader.LocalTimestamp = hlc.ClockTimestamp(e.getTsWithName("localTs"))

	return e.withWriter("put_rangekey", func(rw ReadWriter) error {
		return rw.ExperimentalPutMVCCRangeKey(rangeKey, value)
	})
}

func cmdIterNew(e *evalCtx) error {
	var opts IterOptions
	opts.Prefix = e.hasArg("prefix")
	if e.hasArg("k") {
		opts.LowerBound, opts.UpperBound = e.getKeyRange()
	}
	if len(opts.UpperBound) == 0 {
		opts.UpperBound = keys.MaxKey
	}
	kind := MVCCKeyAndIntentsIterKind
	if e.hasArg("kind") {
		var arg string
		e.scanArg("kind", &arg)
		switch arg {
		case "keys":
			kind = MVCCKeyIterKind
		case "keysAndIntents":
			kind = MVCCKeyAndIntentsIterKind
		default:
			return errors.Errorf("unknown iterator kind %s", arg)
		}
	}
	if e.hasArg("types") {
		var arg string
		e.scanArg("types", &arg)
		switch arg {
		case "pointsOnly":
			opts.KeyTypes = IterKeyTypePointsOnly
		case "pointsAndRanges":
			opts.KeyTypes = IterKeyTypePointsAndRanges
		case "rangesOnly":
			opts.KeyTypes = IterKeyTypeRangesOnly
		default:
			return errors.Errorf("unknown key type %s", arg)
		}
	}

	var r, closeReader Reader
	rType := util.ConstantWithMetamorphicTestChoice(
		fmt.Sprintf("iter-reader@%s", filepath.Base(e.td.Pos)),
		"engine", "readonly", "batch", "snapshot").(string)
	switch rType {
	case "engine":
		r = e.engine
	case "readonly":
		r = e.engine.NewReadOnly(StandardDurability)
	case "batch":
		r = e.engine.NewBatch()
		closeReader = r
	case "snapshot":
		r = e.engine.NewSnapshot()
		closeReader = r
	default:
		return errors.Errorf("unknown reader type %s", rType)
	}

	if e.iter != nil {
		e.iter.Close()
	}
	e.iter = &iterWithCloseReader{
		MVCCIterator: r.NewMVCCIterator(kind, opts),
		closeReader:  closeReader,
	}
	return nil
}

func cmdIterSeekGE(e *evalCtx) error {
	key := e.getKey()
	ts := e.getTs(nil)
	e.iter.SeekGE(MVCCKey{Key: key, Timestamp: ts})
	printIter(e)
	return nil
}

func cmdIterSeekIntentGE(e *evalCtx) error {
	key := e.getKey()
	var txnName string
	e.scanArg("txn", &txnName)
	txn := e.txns[txnName]
	e.iter.SeekIntentGE(key, txn.ID)
	printIter(e)
	return nil
}

func cmdIterSeekLT(e *evalCtx) error {
	key := e.getKey()
	ts := e.getTs(nil)
	e.iter.SeekLT(MVCCKey{Key: key, Timestamp: ts})
	printIter(e)
	return nil
}

func cmdIterNext(e *evalCtx) error {
	e.iter.Next()
	printIter(e)
	return nil
}

func cmdIterNextKey(e *evalCtx) error {
	e.iter.NextKey()
	printIter(e)
	return nil
}

func cmdIterPrev(e *evalCtx) error {
	e.iter.Prev()
	printIter(e)
	return nil
}

func cmdIterScan(e *evalCtx) error {
	reverse := e.hasArg("reverse")
	for {
		printIter(e)
		if ok, err := e.iter.Valid(); err != nil {
			return err
		} else if !ok {
			return nil
		}
		if reverse {
			e.iter.Prev()
		} else {
			e.iter.Next()
		}
	}
}

func printIter(e *evalCtx) {
	e.results.buf.Printf("%s:", e.td.Cmd)
	defer e.results.buf.Printf("\n")

	hasPoint, hasRange := e.iter.HasPointAndRange()
	ok, err := e.iter.Valid()
	if err != nil {
		e.results.buf.Printf(" err=%v", err)
		return
	}
	if !ok {
		if hasPoint || hasRange {
			e.t.Fatalf("invalid iterator gave hasPoint=%t hasRange=%t", hasPoint, hasRange)
		}
		e.results.buf.Print(" .")
		return
	}
	if !hasPoint && !hasRange {
		e.t.Fatalf("valid iterator at %s without point nor range keys", e.iter.UnsafeKey())
	}

	if hasPoint {
		if !e.iter.UnsafeKey().IsValue() {
			meta := enginepb.MVCCMetadata{}
			if err := protoutil.Unmarshal(e.iter.UnsafeValue(), &meta); err != nil {
				e.Fatalf("%v", err)
			}
			e.results.buf.Printf(" %s=%+v", e.iter.UnsafeKey(), &meta)
		} else {
			value, err := DecodeMVCCValue(e.iter.UnsafeValue())
			if err != nil {
				e.Fatalf("%v", err)
			}
			e.results.buf.Printf(" %s=%s", e.iter.UnsafeKey(), value)
		}
	}
	if hasRange {
		e.results.buf.Printf(" %s/[", e.iter.RangeBounds())
		for i, rangeKV := range e.iter.RangeKeys() {
			value, err := DecodeMVCCValue(rangeKV.Value)
			if err != nil {
				e.Fatalf("%v", err)
			}
			if i > 0 {
				e.results.buf.Printf(" ")
			}
			e.results.buf.Printf("%s=%s", rangeKV.RangeKey.Timestamp, value)
		}
		e.results.buf.Printf("]")
	}
}

// formatStats formats MVCC stats.
func formatStats(ms enginepb.MVCCStats, delta bool) string {
	// Split stats into field pairs. Subindex 1 is key, 2 is value.
	fields := regexp.MustCompile(`(\w+):(-?\d+)`).FindAllStringSubmatch(ms.String(), -1)

	// Sort some fields in preferred order, keeping the rest as-is at the end.
	//
	// TODO(erikgrinaker): Consider just reordering the MVCCStats struct fields
	// instead, which determines the order of MVCCStats.String().
	order := []string{"key_count", "key_bytes", "val_count", "val_bytes",
		"live_count", "live_bytes", "gc_bytes_age",
		"intent_count", "intent_bytes", "separated_intent_count", "intent_age"}
	sort.SliceStable(fields, func(i, j int) bool {
		for _, name := range order {
			if fields[i][1] == name {
				return true
			} else if fields[j][1] == name {
				return false
			}
		}
		return false
	})

	// Format and output fields.
	var s string
	for _, field := range fields {
		key, value := field[1], field[2]

		// Always skip zero-valued fields and LastUpdateNanos.
		if value == "0" || key == "last_update_nanos" {
			continue
		}

		if len(s) > 0 {
			s += " "
		}
		s += key + "="
		if delta && value[0] != '-' {
			s += "+" // prefix unsigned deltas with +
		}
		s += value
	}
	if len(s) == 0 && delta {
		return "no change"
	}
	return s
}

// evalCtx stored the current state of the environment of a running
// script.
type evalCtx struct {
	results struct {
		buf               *redact.StringBuilder
		txn               *roachpb.Transaction
		traceIntentWrites bool
	}
	ctx        context.Context
	engine     Engine
	iter       MVCCIterator
	t          *testing.T
	td         *datadriven.TestData
	txns       map[string]*roachpb.Transaction
	txnCounter uint128.Uint128
	ms         *enginepb.MVCCStats
}

func newEvalCtx(ctx context.Context, engine Engine) *evalCtx {
	return &evalCtx{
		ctx:        ctx,
		engine:     engine,
		txns:       make(map[string]*roachpb.Transaction),
		txnCounter: uint128.FromInts(0, 1),
	}
}

func (e *evalCtx) close() {
	if e.iter != nil {
		e.iter.Close()
	}
	// engine is passed in, so it's the caller's responsibility to close it.
}

func (e *evalCtx) getTxnStatus() roachpb.TransactionStatus {
	status := roachpb.COMMITTED
	if e.hasArg("status") {
		var sn string
		e.scanArg("status", &sn)
		s, ok := roachpb.TransactionStatus_value[sn]
		if !ok {
			e.Fatalf("invalid status: %s", sn)
		}
		status = roachpb.TransactionStatus(s)
	}
	return status
}

func (e *evalCtx) scanArg(key string, dests ...interface{}) {
	e.t.Helper()
	e.td.ScanArgs(e.t, key, dests...)
}

func (e *evalCtx) hasArg(key string) bool {
	for _, c := range e.td.CmdArgs {
		if c.Key == key {
			return true
		}
	}
	return false
}

func (e *evalCtx) Fatalf(format string, args ...interface{}) {
	e.t.Helper()
	e.td.Fatalf(e.t, format, args...)
}

func (e *evalCtx) getResolve() (bool, roachpb.TransactionStatus) {
	e.t.Helper()
	if !e.hasArg("resolve") {
		return false, roachpb.PENDING
	}
	return true, e.getTxnStatus()
}

func (e *evalCtx) getTs(txn *roachpb.Transaction) hlc.Timestamp {
	return e.getTsWithTxnAndName(txn, "ts")
}

func (e *evalCtx) getTsWithName(name string) hlc.Timestamp {
	return e.getTsWithTxnAndName(nil, name)
}

func (e *evalCtx) getTsWithTxnAndName(txn *roachpb.Transaction, name string) hlc.Timestamp {
	var ts hlc.Timestamp
	if txn != nil {
		ts = txn.ReadTimestamp
	}
	if !e.hasArg(name) {
		return ts
	}
	var tsS string
	e.scanArg(name, &tsS)
	ts, err := hlc.ParseTimestamp(tsS)
	if err != nil {
		e.Fatalf("%v", err)
	}
	return ts
}

type optArg int

const (
	optional optArg = iota
	mandatory
)

func (e *evalCtx) getList(argName string) []string {
	for _, c := range e.td.CmdArgs {
		if c.Key == argName {
			return c.Vals
		}
	}
	e.Fatalf("missing argument: %s", argName)
	return nil
}

func (e *evalCtx) getTxn(opt optArg) *roachpb.Transaction {
	e.t.Helper()
	if opt == optional && (e.hasArg("notxn") || !e.hasArg("t")) {
		return nil
	}
	var txnName string
	e.scanArg("t", &txnName)
	txn, err := e.lookupTxn(txnName)
	if err != nil {
		e.Fatalf("%v", err)
	}
	return txn
}

func (e *evalCtx) withWriter(cmd string, fn func(_ ReadWriter) error) error {
	var rw ReadWriter
	rw = e.engine
	var batch Batch
	if e.hasArg("batched") {
		batch = e.engine.NewBatch()
		defer batch.Close()
		rw = batch
	}
	rw = e.tryWrapForIntentPrinting(rw)
	origErr := fn(rw)
	if batch != nil {
		batchStatus := "non-empty"
		if batch.Empty() {
			batchStatus = "empty"
		}
		e.results.buf.Printf("%s: batch after write is %s\n", cmd, batchStatus)
	}
	if origErr != nil {
		return origErr
	}
	if batch != nil {
		return batch.Commit(true)
	}
	return nil
}

func (e *evalCtx) getVal() roachpb.Value { return e.getValInternal("v") }
func (e *evalCtx) getValInternal(argName string) roachpb.Value {
	var value string
	e.scanArg(argName, &value)
	var val roachpb.Value
	if e.hasArg("raw") {
		val.RawBytes = []byte(value)
	} else {
		val.SetString(value)
	}
	return val
}

func (e *evalCtx) getKey() roachpb.Key {
	e.t.Helper()
	var keyS string
	e.scanArg("k", &keyS)
	return toKey(keyS)
}

func (e *evalCtx) getKeyRange() (sk, ek roachpb.Key) {
	e.t.Helper()
	var keyS string
	e.scanArg("k", &keyS)
	sk = toKey(keyS)
	ek = sk.Next()
	if e.hasArg("end") {
		var endKeyS string
		e.scanArg("end", &endKeyS)
		ek = toKey(endKeyS)
	}
	return sk, ek
}

func (e *evalCtx) newTxn(
	txnName string, ts, globalUncertaintyLimit hlc.Timestamp, key roachpb.Key,
) (*roachpb.Transaction, error) {
	if _, ok := e.txns[txnName]; ok {
		e.Fatalf("txn %s already open", txnName)
	}
	txn := &roachpb.Transaction{
		TxnMeta: enginepb.TxnMeta{
			ID:             uuid.FromUint128(e.txnCounter),
			Key:            []byte(key),
			WriteTimestamp: ts,
			Sequence:       0,
		},
		Name:                   txnName,
		ReadTimestamp:          ts,
		GlobalUncertaintyLimit: globalUncertaintyLimit,
		Status:                 roachpb.PENDING,
	}
	e.txnCounter = e.txnCounter.Add(1)
	e.txns[txnName] = txn
	return txn, nil
}

func (e *evalCtx) lookupTxn(txnName string) (*roachpb.Transaction, error) {
	txn, ok := e.txns[txnName]
	if !ok {
		e.Fatalf("txn %s not open", txnName)
	}
	return txn, nil
}

func toKey(s string) roachpb.Key {
	if len(s) == 0 {
		return roachpb.Key(s)
	}
	switch s[0] {
	case '+':
		return roachpb.Key(s[1:]).Next()
	case '=':
		return roachpb.Key(s[1:])
	case '-':
		return roachpb.Key(s[1:]).PrefixEnd()
	case '%':
		return append(keys.LocalRangePrefix, s[1:]...)
	case '/':
		var pk string
		var columnFamilyID uint64
		var err error
		parts := strings.Split(s[1:], "/")
		switch len(parts) {
		case 2:
			if columnFamilyID, err = strconv.ParseUint(parts[1], 10, 32); err != nil {
				panic(fmt.Sprintf("invalid column family ID %s in row key %s: %s", parts[1], s, err))
			}
			fallthrough
		case 1:
			pk = parts[0]
		default:
			panic(fmt.Sprintf("expected at most one / separator in row key %s", s))
		}

		var colMap catalog.TableColMap
		colMap.Set(0, 0)
		key := keys.SystemSQLCodec.IndexPrefix(1, 1)
		key, _, err = rowenc.EncodeColumns([]descpb.ColumnID{0}, nil /* directions */, colMap, []tree.Datum{tree.NewDString(pk)}, key)
		if err != nil {
			panic(err)
		}
		key = keys.MakeFamilyKey(key, uint32(columnFamilyID))
		return key
	default:
		return roachpb.Key(s)
	}
}

// iterWithCloseReader will close the underlying reader when the
// iterator is closed.
type iterWithCloseReader struct {
	MVCCIterator
	closeReader Reader
}

func (i *iterWithCloseReader) Close() {
	i.MVCCIterator.Close()
	if i.closeReader != nil {
		i.closeReader.Close()
	}
}
