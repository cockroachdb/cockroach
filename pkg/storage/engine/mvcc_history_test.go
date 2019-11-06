// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package engine

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"regexp"
	"strings"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/storage/engine/enginepb"
	"github.com/cockroachdb/cockroach/pkg/util/hlc"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/protoutil"
	"github.com/cockroachdb/cockroach/pkg/util/uint128"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
)

// TestMVCCHistories verifies that sequences of MVCC reads and writes
// perform properly.
func TestMVCCHistories(t *testing.T) {
	defer leaktest.AfterTest(t)()

	ctx := context.Background()
	for _, engineImpl := range mvccEngineImpls {
		t.Run(engineImpl.name, func(t *testing.T) {

			// Everything reads/writes under the same prefix.
			key := roachpb.Key("k/")
			span := roachpb.Span{Key: key, EndKey: key.PrefixEnd()}

			datadriven.Walk(t, "testdata/mvcc_histories", func(t *testing.T, path string) {
				// Skip over temp files.
				if strings.HasSuffix(path, "~") || strings.HasPrefix(path, "#") {
					return
				}

				// We start from a clean slate in every test file.
				engine := engineImpl.create()
				defer engine.Close()

				reportDataEntries := func(buf *bytes.Buffer) error {
					hasData := false
					err := engine.Iterate(
						span.Key,
						span.EndKey,
						func(r MVCCKeyValue) (bool, error) {
							hasData = true
							if r.Key.Timestamp.IsEmpty() {
								// Meta is at timestamp zero.
								meta := enginepb.MVCCMetadata{}
								if err := protoutil.Unmarshal(r.Value, &meta); err != nil {
									fmt.Fprintf(buf, "meta: %v -> error decoding proto from %v: %v\n", r.Key, r.Value, err)
								} else {
									fmt.Fprintf(buf, "meta: %v -> %+v\n", r.Key, &meta)
								}
							} else {
								fmt.Fprintf(buf, "data: %v -> %s\n", r.Key, roachpb.Value{RawBytes: r.Value}.PrettyPrint())
							}
							return false, nil
						})
					if !hasData {
						buf.WriteString("<no data>\n")
					}
					return err
				}

				e := newEvalCtx(ctx, engine)

				datadriven.RunTest(t, path, func(d *datadriven.TestData) string {
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
					case "run":
						// Syntax: run [trace] [error]
						// (other words - in particular "ok" - are accepted but ignored)
						//
						// "run" executes a script of zero or more operations from
						// the commands library defined below.
						// It stops upon the first error encountered, if any.
						//
						// Options:
						// "trace" means detail each operation in the output.
						// "error" means expect an error to occur. The specific error type/
						// message to expect is spelled out in the expected output.
						//
						trace := false
						if e.hasArg("trace") {
							trace = true
						}
						expectError := false
						if e.hasArg("error") {
							expectError = true
						}

						// buf will accumulate the actual output, which the
						// datadriven driver will use to compare to the expected
						// output.
						var buf bytes.Buffer
						e.results.buf = &buf

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
								fmt.Fprintf(&buf, "txn: %v\n", e.results.txn)
							}
							if printData {
								err := reportDataEntries(&buf)
								if err != nil {
									if foundErr == nil {
										// Handle the error below.
										foundErr = err
									} else {
										fmt.Fprintf(&buf, "error reading data: (%T:) %v\n", err, err)
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
							// Compute a line prefix, to clarify error message. We
							// prefix a newline character because some text editor do
							// not know how to jump to the location of an error if
							// there are multiple file:line prefixes on the same line.
							d.Pos = fmt.Sprintf("\n%s: (+%d)", pos, i+1)

							// Trace the execution in testing.T, to clarify where we
							// are in case an error occurs.
							t.Logf("%s: %s", d.Pos, line)

							// Decompose the current script line.
							d.Cmd, d.CmdArgs = parseLine(t, d.Pos, line)

							// Expand "with" commands:
							//   with t=A
							//       begin_txn
							//       commit_txn
							// is equivalent to:
							//   begin_txn  t=A
							//   commit_txn t=A
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
							txnChange = txnChange || cmd.txn
							dataChange = dataChange || cmd.data

							if trace {
								// If tracing is also requested by the datadriven input,
								// we'll trace the statement in the actual results too.
								fmt.Fprintf(&buf, ">> %s", d.Cmd)
								for i := range d.CmdArgs {
									fmt.Fprintf(&buf, " %s", &d.CmdArgs[i])
								}
								buf.WriteByte('\n')
							}

							// Run the command.
							foundErr = cmd.fn(e)

							if trace {
								// If tracing is enabled, we report the intermediate results
								// after each individual step in the script.
								// This may modify foundErr too.
								reportResults(cmd.txn, cmd.data)
							}

							if foundErr != nil {
								// An error occured. Stop the script prematurely.
								break
							}
						}
						// End of script.

						if !trace {
							// If we were not tracing, no results were printed yet. Do it now.
							reportResults(txnChange, dataChange)
						}

						// Check for errors.
						if foundErr == nil && expectError {
							t.Errorf("%s: expected error, got success", d.Pos)
							return d.Expected
						} else if foundErr != nil {
							if expectError {
								fmt.Fprintf(&buf, "error: (%T:) %v\n", foundErr, foundErr)
							} else /* !expectError */ {
								t.Errorf("%s: expected success, found: (%T:) %v", d.Pos, foundErr, foundErr)
								return d.Expected
							}
						}

						// We're done. Report the actual results and errors to the
						// datadriven executor.
						return buf.String()

					default:
						t.Errorf("%s: unknown command: %s", d.Pos, d.Cmd)
						return d.Expected
					}
				})
			})
		})
	}
}

// getCmd retrieves the cmd entry for the current script line.
func (e *evalCtx) getCmd() cmd {
	e.t.Helper()
	cmd, ok := commands[e.td.Cmd]
	if !ok {
		e.Fatalf("unknown command: %s", e.td.Cmd)
	}
	return cmd
}

// cmd represents one supported script command.
type cmd struct {
	txn, data bool
	fn        func(e *evalCtx) error
}

// commands is the list of all supported script commands.
var commands = map[string]cmd{
	"begin_txn":      cmd{true, false, cmdBegin},
	"commit_txn":     cmd{true, true, cmdCommit},
	"abort_txn":      cmd{true, true, cmdAbort},
	"resolve_intent": cmd{false, true, cmdResolve},
	"clear_range":    cmd{false, true, cmdClearRange},
	"update_txn":     cmd{true, false, cmdUpdateTxn},
	"restart_txn":    cmd{true, false, cmdRestartTxn},
	"step_txn":       cmd{true, false, cmdStepTxn},
	"mvcc_put":       cmd{false, true, cmdPut},
	"mvcc_merge":     cmd{false, true, cmdMerge},
	"mvcc_del":       cmd{false, true, cmdDelete},
	"mvcc_get":       cmd{false, false, cmdGet},
	"mvcc_scan":      cmd{false, false, cmdScan},
}

func cmdClearRange(e *evalCtx) error {
	key, endKey := e.getKeyRange()
	return e.engine.ClearRange(
		MVCCKey{Key: key},
		MVCCKey{Key: endKey},
	)
}

func cmdBegin(e *evalCtx) error {
	var txnName string
	e.scanArg("t", &txnName)
	var tsW uint64
	e.scanArg("ts", &tsW)
	key := roachpb.KeyMin
	if e.hasArg("k") {
		var keyS string
		e.scanArg("k", &keyS)
		key = toKey(keyS)
	}
	txn, err := e.newTxn(txnName, hlc.Timestamp{WallTime: int64(tsW)}, key)
	e.results.txn = txn
	return err
}

func cmdResolve(e *evalCtx) error {
	txn := e.getTxn()
	key := e.getKey()
	status := e.getTxnStatus()
	return MVCCResolveWriteIntent(e.ctx, e.engine, nil, roachpb.Intent{
		Span:   roachpb.Span{Key: key},
		Status: status,
		Txn:    txn.TxnMeta,
	})
}

func cmdSetTxnStatus(e *evalCtx) error {
	txn := e.getTxn()
	status := e.getTxnStatus()
	txn.Status = status
	e.results.txn = txn
	return nil
}

func cmdCommit(e *evalCtx) error {
	return cmdFinishTxn(e, roachpb.COMMITTED)
}
func cmdAbort(e *evalCtx) error {
	return cmdFinishTxn(e, roachpb.ABORTED)
}

func cmdFinishTxn(e *evalCtx, desiredStatus roachpb.TransactionStatus) error {
	txn := e.getTxn()
	txn.Status = desiredStatus
	resolve := true
	if e.hasArg("noresolve") {
		resolve = false
	}
	if resolve {
		for _, is := range txn.IntentSpans {
			if err := MVCCResolveWriteIntent(e.ctx, e.engine, nil, roachpb.Intent{
				Span:   is,
				Status: txn.Status,
				Txn:    txn.TxnMeta,
			}); err != nil {
				return errors.Wrapf(err, "resolving %v", is.Key)
			}
		}
	}
	e.results.txn = txn
	delete(e.txns, txn.Name)
	return nil
}

func cmdUpdateTxn(e *evalCtx) error {
	txn := e.getTxn()
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

func cmdGet(e *evalCtx) error {
	var txn *roachpb.Transaction
	if e.hasArg("t") {
		txn = e.getTxn()
	}
	key := e.getKey()
	ts := txn.Timestamp
	if e.hasArg("ts") {
		var tsW uint64
		e.scanArg("ts", &tsW)
		ts.WallTime = int64(tsW)
	}
	opts := MVCCGetOptions{Txn: txn}
	if e.hasArg("inconsistent") {
		opts.Inconsistent = true
		opts.Txn = nil
	} else {
		if opts.Txn == nil {
			e.Fatalf("cannot issue consistent reads without txn")
		}
	}
	if e.hasArg("tombstones") {
		opts.Tombstones = true
	}
	val, intent, err := MVCCGet(e.ctx, e.engine, key, ts, opts)
	if err != nil {
		return err
	}
	if intent != nil {
		fmt.Fprintf(e.results.buf, "get: %v -> intent {%s} %s\n", key, intent.Txn, intent.Status)
	}
	if val != nil {
		fmt.Fprintf(e.results.buf, "get: %v -> %v\n", key, val.PrettyPrint())
	} else {
		fmt.Fprintf(e.results.buf, "get: %v -> <no data>\n", key)
	}
	return nil
}

func cmdScan(e *evalCtx) error {
	var txn *roachpb.Transaction
	if e.hasArg("t") {
		txn = e.getTxn()
	}
	key, endKey := e.getKeyRange()
	ts := txn.Timestamp
	if e.hasArg("ts") {
		var tsW uint64
		e.scanArg("ts", &tsW)
		ts.WallTime = int64(tsW)
	}
	opts := MVCCScanOptions{Txn: txn}
	if e.hasArg("inconsistent") {
		opts.Inconsistent = true
		opts.Txn = nil
	} else {
		if opts.Txn == nil {
			e.Fatalf("cannot issue consistent reads without txn")
		}
	}
	if e.hasArg("tombstones") {
		opts.Tombstones = true
	}
	if e.hasArg("reverse") {
		opts.Reverse = true
	}
	max := int64(-1)
	if e.hasArg("max") {
		var imax int
		e.scanArg("max", &imax)
		max = int64(imax)
	}
	vals, _, intents, err := MVCCScan(e.ctx, e.engine, key, endKey, max, ts, opts)
	if err != nil {
		return err
	}
	for _, intent := range intents {
		fmt.Fprintf(e.results.buf, "scan: %v -> intent {%s} %s\n", key, intent.Txn, intent.Status)
	}
	for _, val := range vals {
		fmt.Fprintf(e.results.buf, "scan: %v -> %v\n", val.Key, val.Value.PrettyPrint())
	}
	if len(vals) == 0 {
		fmt.Fprintf(e.results.buf, "scan: %v-%v -> <no data>\n", key, endKey)
	}
	return nil
}

func cmdPut(e *evalCtx) error {
	txn := e.getTxn()
	key := e.getKey()
	var value string
	e.scanArg("v", &value)
	var val roachpb.Value
	if e.hasArg("raw") {
		val.RawBytes = []byte(value)
	} else {
		val.SetString(value)
	}
	ts := txn.Timestamp
	if e.hasArg("ts") {
		var tsW uint64
		e.scanArg("ts", &tsW)
		ts.WallTime = int64(tsW)
	}
	err := MVCCPut(e.ctx, e.engine, nil, key, ts, val, txn)
	if err != nil {
		return err
	}
	// Store the intent for later resolution.
	// We want to guarantee the intents are unique, so do a little work.
	found := false
	for _, i := range txn.IntentSpans {
		if i.Key.Equal(key) {
			found = true
			break
		}
	}
	if !found {
		txn.IntentSpans = append(txn.IntentSpans, roachpb.Span{Key: key})
	}
	return nil
}

func cmdMerge(e *evalCtx) error {
	key := e.getKey()
	var value string
	e.scanArg("v", &value)
	var val roachpb.Value
	if e.hasArg("raw") {
		val.RawBytes = []byte(value)
	} else {
		val.SetString(value)
	}
	var tsW uint64
	e.scanArg("ts", &tsW)
	ts := hlc.Timestamp{WallTime: int64(tsW)}
	return MVCCMerge(e.ctx, e.engine, nil, key, ts, val)
}

func cmdDelete(e *evalCtx) error {
	txn := e.getTxn()
	key := e.getKey()
	ts := txn.Timestamp
	if e.hasArg("ts") {
		var tsW uint64
		e.scanArg("ts", &tsW)
		ts.WallTime = int64(tsW)
	}
	err := MVCCDelete(e.ctx, e.engine, nil, key, ts, txn)
	if err != nil {
		return err
	}
	// Store the intent for later resolution.
	txn.IntentSpans = append(txn.IntentSpans, roachpb.Span{Key: key})
	return nil
}

func cmdRestartTxn(e *evalCtx) error {
	txn := e.getTxn()
	ts := txn.Timestamp
	if e.hasArg("ts") {
		var tsW uint64
		e.scanArg("ts", &tsW)
		ts.WallTime = int64(tsW)
	}
	up := roachpb.NormalUserPriority
	if e.hasArg("userprio") {
		var upF float64
		e.scanArg("userprio", &upF)
		up = roachpb.UserPriority(upF)
	}
	tp := enginepb.MinTxnPriority
	if e.hasArg("txnprio") {
		var tpI int
		e.scanArg("txnprio", &tpI)
		tp = enginepb.TxnPriority(tpI)
	}
	txn.Restart(up, tp, ts)
	e.results.txn = txn
	return nil
}

func cmdStepTxn(e *evalCtx) error {
	n := 1
	if e.hasArg("n") {
		e.scanArg("n", &n)
	}
	txn := e.getTxn()
	txn.Sequence += enginepb.TxnSeq(n)
	e.results.txn = txn
	return nil
}

func toKey(s string) roachpb.Key {
	switch {
	case len(s) > 0 && s[0] == '+':
		return roachpb.Key("k/" + s[1:]).Next()
	case len(s) > 0 && s[0] == '=':
		return roachpb.Key("k/" + s[1:])
	case len(s) > 0 && s[0] == '-':
		return roachpb.Key("k/" + s[1:]).PrefixEnd()
	default:
		return roachpb.Key("k/" + s)
	}
}

// evalCtx stored the current state of the environment of a running
// script.
type evalCtx struct {
	results struct {
		buf io.Writer
		txn *roachpb.Transaction
	}
	ctx        context.Context
	engine     Engine
	t          *testing.T
	td         *datadriven.TestData
	txns       map[string]*roachpb.Transaction
	txnCounter uint128.Uint128
}

func newEvalCtx(ctx context.Context, engine Engine) *evalCtx {
	return &evalCtx{
		ctx:        ctx,
		engine:     engine,
		txns:       make(map[string]*roachpb.Transaction),
		txnCounter: uint128.FromInts(0, 1),
	}
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

func (e *evalCtx) getTxn() *roachpb.Transaction {
	e.t.Helper()
	var txnName string
	e.scanArg("t", &txnName)
	txn, err := e.lookupTxn(txnName)
	if err != nil {
		e.Fatalf("%v", err)
	}
	return txn
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
	txnName string, ts hlc.Timestamp, key roachpb.Key,
) (*roachpb.Transaction, error) {
	if _, ok := e.txns[txnName]; ok {
		e.Fatalf("txn %s already open", txnName)
	}
	txn := &roachpb.Transaction{
		TxnMeta: enginepb.TxnMeta{
			ID:        uuid.FromUint128(e.txnCounter),
			Key:       []byte(key),
			Timestamp: ts,
			Sequence:  1,
		},
		Name:          txnName,
		OrigTimestamp: ts,
		Status:        roachpb.PENDING,
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

// The following is re-implemented from datadriven, as the facilities
// there are not exported. It also extends the set of allowable values
// in a k=v pair with more characters (includes + = /).

func parseLine(t *testing.T, pos, line string) (cmd string, cmdArgs []datadriven.CmdArg) {
	fields := splitDirectives(t, pos, line)
	if len(fields) == 0 {
		return "", nil
	}
	cmd = fields[0]

	for _, arg := range fields[1:] {
		key := arg
		var vals []string
		if pos := strings.IndexByte(key, '='); pos >= 0 {
			key = arg[:pos]
			val := arg[pos+1:]

			if len(val) > 2 && val[0] == '(' && val[len(val)-1] == ')' {
				vals = strings.Split(val[1:len(val)-1], ",")
				for i := range vals {
					vals[i] = strings.TrimSpace(vals[i])
				}
			} else {
				vals = []string{val}
			}
		}
		cmdArgs = append(cmdArgs, datadriven.CmdArg{Key: key, Vals: vals})
	}
	return cmd, cmdArgs
}

var splitDirectivesRE = regexp.MustCompile(`^ *[a-zA-Z0-9_,-\.]+(|=[-a-zA-Z0-9_@=+/]+|=\([^)]*\))( |$)`)

// splits a directive line into tokens, where each token is
// either:
//  - a,list,of,things
//  - argument
//  - argument=value
//  - argument=(values, ...)
func splitDirectives(t *testing.T, pos, line string) []string {
	var res []string

	for line != "" {
		str := splitDirectivesRE.FindString(line)
		if len(str) == 0 {
			t.Fatalf("%s: cannot parse directive: %s\n", pos, line)
		}
		res = append(res, strings.TrimSpace(line[0:len(str)]))
		line = line[len(str):]
	}
	return res
}
