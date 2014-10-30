// Copyright 2014 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	gogoproto "code.google.com/p/gogoprotobuf/proto"
	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/proto"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/log"
)

// setCorrectnessRetryOptions sets client for aggressive retries with a
// limit on number of attempts so we don't get stuck behind indefinite
// backoff/retry loops. If MaxAttempts is reached, transaction will
// return retry error.
func setCorrectnessRetryOptions() {
	client.TxnRetryOptions = util.RetryOptions{
		Backoff:     1 * time.Millisecond,
		MaxBackoff:  10 * time.Millisecond,
		Constant:    2,
		MaxAttempts: 3,
		UseV1Info:   true,
	}
}

// The following structs and methods provide a mechanism for verifying
// the correctness of Cockroach's transaction model. They do this by
// allowing transaction histories to be specified for concurrent txns
// and then expanding those histories to enumerate all possible
// priorities, isolation levels and interleavings of commands in the
// histories.

// cmd is a command to run within a transaction. Commands keep a
// reference to the previous command's wait channel, in order to
// enforce an ordering. If a previous wait channel is set, the
// command waits on it before execution.
type cmd struct {
	name        string // name of the cmd for debug output
	key, endKey string // key and optional endKey
	debug       string // optional debug string
	txnIdx      int    // transaction index in the history
	historyIdx  int    // this suffixes key so tests get unique keys
	fn          func(
		c *cmd, kv *client.KV, t *testing.T) error // execution function
	ch   chan struct{}    // channel for other commands to wait
	prev <-chan struct{}  // channel this command must wait on before executing
	env  map[string]int64 // contains all previously read values
}

func (c *cmd) init(prevCmd *cmd) {
	if prevCmd != nil {
		c.prev = prevCmd.ch
	} else {
		c.prev = nil
	}
	c.ch = make(chan struct{}, 1)
	c.debug = ""
}

func (c *cmd) execute(db *client.KV, t *testing.T) (string, error) {
	if c.prev != nil {
		<-c.prev
	}
	log.V(1).Infof("executing %s", c)
	err := c.fn(c, db, t)
	if c.ch != nil {
		c.ch <- struct{}{}
	}
	if len(c.key) > 0 && len(c.endKey) > 0 {
		return fmt.Sprintf("%s%%d.%%d(%s-%s)%s", c.name, c.key, c.endKey, c.debug), err
	}
	if len(c.key) > 0 {
		return fmt.Sprintf("%s%%d.%%d(%s)%s", c.name, c.key, c.debug), err
	}
	return fmt.Sprintf("%s%%d.%%d%s", c.name, c.debug), err
}

func (c *cmd) done() {
	close(c.ch)
	c.ch = nil
	c.prev = nil
	c.debug = ""
}

func (c *cmd) getKey() []byte {
	return []byte(fmt.Sprintf("%d.%s", c.historyIdx, c.key))
}

func (c *cmd) getEndKey() []byte {
	if len(c.endKey) == 0 {
		return nil
	}
	return []byte(fmt.Sprintf("%d.%s", c.historyIdx, c.endKey))
}

func (c *cmd) String() string {
	if len(c.key) > 0 && len(c.endKey) > 0 {
		return fmt.Sprintf("%s%d(%s-%s)", c.name, c.txnIdx, c.key, c.endKey)
	}
	if len(c.key) > 0 {
		return fmt.Sprintf("%s%d(%s)", c.name, c.txnIdx, c.key)
	}
	return fmt.Sprintf("%s%d", c.name, c.txnIdx)
}

// readCmd reads a value from the db and stores it in the env.
func readCmd(c *cmd, db *client.KV, t *testing.T) error {
	r := &proto.GetResponse{}
	if err := db.Call(proto.Get, &proto.GetRequest{
		RequestHeader: proto.RequestHeader{Key: c.getKey()},
	}, r); err != nil {
		return err
	}
	if r.Value != nil {
		c.env[c.key] = r.Value.GetInteger()
		c.debug = fmt.Sprintf("[%d ts=%d]", r.Value.GetInteger(), r.Timestamp.Logical)
	}
	return nil
}

// deleteRngCmd deletes the range of values from the db from [key, endKey).
func deleteRngCmd(c *cmd, db *client.KV, t *testing.T) error {
	return db.Call(proto.DeleteRange, &proto.DeleteRangeRequest{
		RequestHeader: proto.RequestHeader{Key: c.getKey(), EndKey: c.getEndKey()},
	}, &proto.DeleteRangeResponse{})
}

// scanCmd reads the values from the db from [key, endKey).
func scanCmd(c *cmd, db *client.KV, t *testing.T) error {
	r := &proto.ScanResponse{}
	if err := db.Call(proto.Scan, &proto.ScanRequest{
		RequestHeader: proto.RequestHeader{Key: c.getKey(), EndKey: c.getEndKey()},
	}, r); err != nil {
		return err
	}
	var vals []string
	keyPrefix := []byte(fmt.Sprintf("%d.", c.historyIdx))
	for _, kv := range r.Rows {
		key := bytes.TrimPrefix(kv.Key, keyPrefix)
		c.env[string(key)] = kv.Value.GetInteger()
		vals = append(vals, fmt.Sprintf("%d", kv.Value.GetInteger()))
	}
	c.debug = fmt.Sprintf("[%s ts=%d]", strings.Join(vals, " "), r.Timestamp.Logical)
	return nil
}

// incCmd adds one to the value of c.key in the env and writes
// it to the db. If c.key isn't in the db, writes 1.
func incCmd(c *cmd, db *client.KV, t *testing.T) error {
	r := &proto.IncrementResponse{}
	if err := db.Call(proto.Increment, &proto.IncrementRequest{
		RequestHeader: proto.RequestHeader{Key: c.getKey()},
		Increment:     int64(1),
	}, r); err != nil {
		return err
	}
	c.env[c.key] = r.NewValue
	c.debug = fmt.Sprintf("[%d ts=%d]", r.NewValue, r.Timestamp.Logical)
	return nil
}

// sumCmd sums the values of all keys read during the transaction
// and writes the result to the db.
func sumCmd(c *cmd, db *client.KV, t *testing.T) error {
	sum := int64(0)
	for _, v := range c.env {
		sum += v
	}
	r := &proto.PutResponse{}
	err := db.Call(proto.Put, &proto.PutRequest{
		RequestHeader: proto.RequestHeader{Key: c.getKey()},
		Value:         proto.Value{Integer: gogoproto.Int64(sum)},
	}, r)
	c.debug = fmt.Sprintf("[%d ts=%d]", sum, r.Timestamp.Logical)
	return err
}

// commitCmd commits the transaction.
func commitCmd(c *cmd, db *client.KV, t *testing.T) error {
	r := &proto.EndTransactionResponse{}
	err := db.Call(proto.EndTransaction, &proto.EndTransactionRequest{Commit: true}, r)
	c.debug = fmt.Sprintf("[ts=%d]", r.Timestamp.Logical)
	return err
}

// cmdDict maps from command name to function implementing the command.
// Use only upper case letters for commands. More than one letter is OK.
var cmdDict = map[string]func(c *cmd, db *client.KV, t *testing.T) error{
	"R":   readCmd,
	"I":   incCmd,
	"DR":  deleteRngCmd,
	"SC":  scanCmd,
	"SUM": sumCmd,
	"C":   commitCmd,
}

var cmdRE = regexp.MustCompile(`([A-Z]+)(?:\(([A-Z]+)(?:-([A-Z]+))?\))?`)

func historyString(cmds []*cmd) string {
	var cmdStrs []string
	for _, c := range cmds {
		cmdStrs = append(cmdStrs, c.String())
	}
	return strings.Join(cmdStrs, " ")
}

// parseHistory parses the history string into individual commands
// and returns a slice.
func parseHistory(txnIdx int, history string, t *testing.T) []*cmd {
	// Parse commands.
	var cmds []*cmd
	elems := strings.Split(history, " ")
	for _, elem := range elems {
		match := cmdRE.FindStringSubmatch(elem)
		if match == nil {
			t.Fatalf("failed to parse command %q", elem)
		}
		fn, ok := cmdDict[match[1]]
		if !ok {
			t.Fatalf("cmd %s not defined", match[1])
		}
		var key, endKey string
		if len(match) > 2 {
			key = match[2]
		}
		if len(match) > 3 {
			endKey = match[3]
		}
		c := &cmd{name: match[1], key: key, endKey: endKey, txnIdx: txnIdx, fn: fn}
		cmds = append(cmds, c)
	}
	return cmds
}

// parseHistories parses a slice of history strings and returns
// a slice of command slices, one for each history.
func parseHistories(histories []string, t *testing.T) [][]*cmd {
	var results [][]*cmd
	for i, history := range histories {
		results = append(results, parseHistory(i+1, history, t))
	}
	return results
}

// Easily accessible slices of transaction isolation variations.
var (
	bothIsolations   = []proto.IsolationType{proto.SERIALIZABLE, proto.SNAPSHOT}
	onlySerializable = []proto.IsolationType{proto.SERIALIZABLE}
	onlySnapshot     = []proto.IsolationType{proto.SNAPSHOT}
)

// enumerateIsolations returns a slice enumerating all combinations of
// isolation types across the transactions. The inner slice describes
// the isolation type for each transaction. The outer slice contains
// each possible combination of such transaction isolations.
func enumerateIsolations(numTxns int, isolations []proto.IsolationType) [][]proto.IsolationType {
	// Use a count from 0 to pow(# isolations, numTxns) and examine
	// n-ary digits to get all possible combinations of txn isolations.
	n := len(isolations)
	result := [][]proto.IsolationType{}
	for i := 0; i < int(math.Pow(float64(n), float64(numTxns))); i++ {
		desc := make([]proto.IsolationType, numTxns)
		val := i
		for j := 0; j < numTxns; j++ {
			desc[j] = isolations[val%n]
			val /= n
		}
		result = append(result, desc)
	}
	return result
}

func TestEnumerateIsolations(t *testing.T) {
	SSI := proto.SERIALIZABLE
	SI := proto.SNAPSHOT
	expIsolations := [][]proto.IsolationType{
		{SSI, SSI, SSI},
		{SI, SSI, SSI},
		{SSI, SI, SSI},
		{SI, SI, SSI},
		{SSI, SSI, SI},
		{SI, SSI, SI},
		{SSI, SI, SI},
		{SI, SI, SI},
	}
	if !reflect.DeepEqual(enumerateIsolations(3, bothIsolations), expIsolations) {
		t.Errorf("expected enumeration to match %s; got %s", expIsolations, enumerateIsolations(3, bothIsolations))
	}

	expDegenerate := [][]proto.IsolationType{
		{SSI, SSI, SSI},
	}
	if !reflect.DeepEqual(enumerateIsolations(3, onlySerializable), expDegenerate) {
		t.Errorf("expected enumeration to match %s; got %s", expDegenerate, enumerateIsolations(3, onlySerializable))
	}
}

// enumeratePriorities returns a slice enumerating all combinations of the
// specified slice of priorities.
func enumeratePriorities(priorities []int32) [][]int32 {
	var results [][]int32
	for i := 0; i < len(priorities); i++ {
		leftover := enumeratePriorities(append(append([]int32(nil), priorities[:i]...), priorities[i+1:]...))
		if len(leftover) == 0 {
			results = [][]int32{[]int32{priorities[i]}}
		}
		for j := 0; j < len(leftover); j++ {
			results = append(results, append([]int32{priorities[i]}, leftover[j]...))
		}
	}
	return results
}

func TestEnumeratePriorities(t *testing.T) {
	p1 := int32(1)
	p2 := int32(2)
	p3 := int32(3)
	expPriorities := [][]int32{
		{p1, p2, p3},
		{p1, p3, p2},
		{p2, p1, p3},
		{p2, p3, p1},
		{p3, p1, p2},
		{p3, p2, p1},
	}
	enum := enumeratePriorities([]int32{p1, p2, p3})
	if !reflect.DeepEqual(enum, expPriorities) {
		t.Errorf("expected enumeration to match %v; got %v", expPriorities, enum)
	}
}

// enumerateHistories returns a slice enumerating all combinations of
// collated histories possible given the specified transactions. Each
// input transaction is a slice of commands. The order of commands for
// each transaction is stable, but the enumeration provides all
// possible interleavings between transactions. If symmetric is true,
// skips exactly N-1/N of the enumeration (where N=len(txns)).
func enumerateHistories(txns [][]*cmd, symmetric bool) [][]*cmd {
	var results [][]*cmd
	numTxns := len(txns)
	if symmetric {
		numTxns = 1
	}
	for i := 0; i < numTxns; i++ {
		if len(txns[i]) == 0 {
			continue
		}
		cp := append([][]*cmd(nil), txns...)
		cp[i] = append([]*cmd(nil), cp[i][1:]...)
		leftover := enumerateHistories(cp, false)
		if len(leftover) == 0 {
			results = [][]*cmd{[]*cmd{txns[i][0]}}
		}
		for j := 0; j < len(leftover); j++ {
			results = append(results, append([]*cmd{txns[i][0]}, leftover[j]...))
		}
	}
	return results
}

func TestEnumerateHistories(t *testing.T) {
	txns := parseHistories([]string{"I(A) C", "I(A) C"}, t)
	enum := enumerateHistories(txns, false)
	enumStrs := make([]string, len(enum))
	for i, history := range enum {
		enumStrs[i] = historyString(history)
	}
	enumSymmetric := enumerateHistories(txns, true)
	enumSymmetricStrs := make([]string, len(enumSymmetric))
	for i, history := range enumSymmetric {
		enumSymmetricStrs[i] = historyString(history)
	}
	expEnumStrs := []string{
		"I1(A) C1 I2(A) C2",
		"I1(A) I2(A) C1 C2",
		"I1(A) I2(A) C2 C1",
		"I2(A) I1(A) C1 C2",
		"I2(A) I1(A) C2 C1",
		"I2(A) C2 I1(A) C1",
	}
	expEnumSymmetricStrs := []string{
		"I1(A) C1 I2(A) C2",
		"I1(A) I2(A) C1 C2",
		"I1(A) I2(A) C2 C1",
	}
	if !reflect.DeepEqual(enumStrs, expEnumStrs) {
		t.Errorf("expected enumeration to match %s; got %s", expEnumStrs, enumStrs)
	}
	if !reflect.DeepEqual(enumSymmetricStrs, expEnumSymmetricStrs) {
		t.Errorf("expected symmetric enumeration to match %s; got %s", expEnumSymmetricStrs, enumSymmetricStrs)
	}
}

// verifier executes the history and then invokes checkFn to verify
// the environment (map from key to value) left from executing the
// history.
type verifier struct {
	history string
	checkFn func(env map[string]int64) error
}

// historyVerifier parses a planned transaction execution history into
// commands per transaction and each command's previous dependency.
// When run, each transaction's commands are executed via a goroutine
// in a separate txn. The results of the execution are added to the
// actual commands slice. When all txns have completed the actual history
// is compared to the expected history.
type historyVerifier struct {
	name       string
	txns       [][]*cmd
	verify     *verifier
	verifyCmds []*cmd
	expSuccess bool
	symmetric  bool

	sync.Mutex // protects actual slice of command outcomes.
	actual     []string
	wg         sync.WaitGroup
}

func newHistoryVerifier(name string, txns []string, verify *verifier, expSuccess bool, t *testing.T) *historyVerifier {
	return &historyVerifier{
		name:       name,
		txns:       parseHistories(txns, t),
		verify:     verify,
		verifyCmds: parseHistory(0, verify.history, t),
		expSuccess: expSuccess,
		symmetric:  areHistoriesSymmetric(txns),
	}
}

// areHistoriesSymmetric returns whether all txn histories are the same.
func areHistoriesSymmetric(txns []string) bool {
	for i := 1; i < len(txns); i++ {
		if txns[i] != txns[0] {
			return false
		}
	}
	return true
}

func (hv *historyVerifier) run(isolations []proto.IsolationType, db *client.KV, t *testing.T) {
	log.Infof("verifying all possible histories for the %q anomaly", hv.name)
	priorities := make([]int32, len(hv.txns))
	for i := 0; i < len(hv.txns); i++ {
		priorities[i] = int32(i + 1)
	}
	enumPri := enumeratePriorities(priorities)
	enumIso := enumerateIsolations(len(hv.txns), isolations)
	enumHis := enumerateHistories(hv.txns, hv.symmetric)

	historyIdx := 1
	var failures []error
	for _, p := range enumPri {
		for _, i := range enumIso {
			for _, h := range enumHis {
				if err := hv.runHistory(historyIdx, p, i, h, db, t); err != nil {
					failures = append(failures, err)
				}
				historyIdx++
			}
		}
	}

	if hv.expSuccess == true && len(failures) > 0 {
		t.Errorf("expected success, experienced %d errors", len(failures))
	} else if !hv.expSuccess && len(failures) == 0 {
		t.Errorf("expected failures for the %q anomaly, but experienced none", hv.name)
	}
}

func (hv *historyVerifier) runHistory(historyIdx int, priorities []int32,
	isolations []proto.IsolationType, cmds []*cmd, db *client.KV, t *testing.T) error {
	plannedStr := historyString(cmds)
	log.V(1).Infof("attempting iso=%v pri=%v history=%s", isolations, priorities, plannedStr)

	hv.actual = []string{}
	hv.wg.Add(len(priorities))
	txnMap := map[int][]*cmd{}
	var prev *cmd
	for _, c := range cmds {
		c.historyIdx = historyIdx
		txnMap[c.txnIdx] = append(txnMap[c.txnIdx], c)
		c.init(prev)
		prev = c
	}
	for i, txnCmds := range txnMap {
		go func(i int, txnCmds []*cmd) {
			if err := hv.runTxn(i, priorities[i-1], isolations[i-1], txnCmds, db, t); err != nil {
				t.Errorf("unexpected failure running transaction %d (%s): %v", i, cmds, err)
			}
		}(i, txnCmds)
	}
	hv.wg.Wait()

	// Construct string for actual history.
	actualStr := strings.Join(hv.actual, " ")

	// Verify history.
	var verifyStrs []string
	verifyEnv := map[string]int64{}
	for _, c := range hv.verifyCmds {
		c.historyIdx = historyIdx
		c.env = verifyEnv
		c.init(nil)
		fmtStr, err := c.execute(db, t)
		if err != nil {
			t.Errorf("failed on execution of verification cmd %s: %s", c, err)
			return err
		}
		cmdStr := fmt.Sprintf(fmtStr, 0, 0)
		verifyStrs = append(verifyStrs, cmdStr)
	}

	err := hv.verify.checkFn(verifyEnv)
	if err == nil {
		log.V(1).Infof("PASSED: iso=%v, pri=%v, history=%q", isolations, priorities, actualStr)
	}
	if hv.expSuccess && err != nil {
		verifyStr := strings.Join(verifyStrs, " ")
		t.Errorf("%d: iso=%v, pri=%v, history=%q: actual=%q, verify=%q: %s",
			historyIdx, isolations, priorities, plannedStr, actualStr, verifyStr, err)
	}
	return err
}

func (hv *historyVerifier) runTxn(txnIdx int, priority int32,
	isolation proto.IsolationType, cmds []*cmd, db *client.KV, t *testing.T) error {
	var retry int
	txnName := fmt.Sprintf("txn%d", txnIdx)
	txnOpts := &client.TransactionOptions{
		Name:      txnName,
		Isolation: isolation,
	}
	err := db.RunTransaction(txnOpts, func(txn *client.KV) error {
		txn.UserPriority = -priority

		env := map[string]int64{}
		// TODO(spencer): restarts must create additional histories. They
		// look like: given the current partial history and a restart on
		// txn txnIdx, re-enumerate a set of all histories containing the
		// remaining commands from extant txns and all commands from this
		// restarted txn.

		// If this is attempt > 1, reset cmds so no waits.
		if retry++; retry == 2 {
			for _, c := range cmds {
				c.done()
			}
		}
		log.V(1).Infof("%s, retry=%d", txnName, retry)
		for i := range cmds {
			cmds[i].env = env
			if err := hv.runCmd(txn, txnIdx, retry, i, cmds, t); err != nil {
				return err
			}
		}
		return nil
	})
	hv.wg.Done()
	return err
}

func (hv *historyVerifier) runCmd(db *client.KV, txnIdx, retry, cmdIdx int, cmds []*cmd, t *testing.T) error {
	fmtStr, err := cmds[cmdIdx].execute(db, t)
	if err != nil {
		return err
	}
	hv.Lock()
	cmdStr := fmt.Sprintf(fmtStr, txnIdx, retry)
	hv.actual = append(hv.actual, cmdStr)
	hv.Unlock()
	return nil
}

// checkConcurrency creates a history verifier, starts a new database
// and runs the verifier.
func checkConcurrency(name string, isolations []proto.IsolationType, txns []string,
	verify *verifier, expSuccess bool, t *testing.T) {
	setCorrectnessRetryOptions()
	verifier := newHistoryVerifier(name, txns, verify, expSuccess, t)
	db, _, _, _ := createTestDB(t)
	verifier.run(isolations, db, t)
}

// The following tests for concurrency anomalies include documentation
// taken from the "Concurrency Control Chapter" from the Handbook of
// Database Technology, written by Patrick O'Neil <poneil@cs.umb.edu>:
// http://www.cs.umb.edu/~poneil/CCChapter.PDF.
//
// Notation for planned histories:
//   R(x) - read from key "x"
//   I(x) - increment key "x" by 1
//   SC(x-y) - scan values from keys "x"-"y"
//   SUM(x) - sums all values read during txn and writes sum to "x"
//   C - commit
//
// Notation for actual histories:
//   Rn.m(x) - read from txn "n" ("m"th retry) of key "x"
//   In.m(x) - increment from txn "n" ("m"th retry) of key "x"
//   SCn.m(x-y) - scan from txn "n" ("m"th retry) of keys "x"-"y"
//   SUMn.m(x) - sums all values read from txn "n" ("m"th retry)
//   Cn.m - commit of txn "n" ("m"th retry)

// TestTxnDBInconsistentAnalysisAnomaly verifies that neither SI nor
// SSI isolation are subject to the inconsistent analysis anomaly.
// This anomaly is also known as dirty reads and is prevented by the
// READ_COMMITTED ANSI isolation level.
//
// With inconsistent analysis, there are two concurrent txns. One
// reads keys A & B, the other reads and then writes keys A & B. The
// reader must not see intermediate results from the reader/writer.
//
// Lost update would typically fail with a history such as:
//    R1(A) R2(B) W2(B) R2(A) W2(A) R1(B) C1 C2
func TestTxnDBInconsistentAnalysisAnomaly(t *testing.T) {
	txn1 := "R(A) R(B) SUM(C) C"
	txn2 := "I(A) I(B) C"
	verify := &verifier{
		history: "R(C)",
		checkFn: func(env map[string]int64) error {
			if env["C"] != 2 && env["C"] != 0 {
				return util.Errorf("expected C to be either 0 or 2, got %d", env["C"])
			}
			return nil
		},
	}
	checkConcurrency("inconsistent analysis", bothIsolations, []string{txn1, txn2}, verify, true, t)
}

// TestTxnDBLostUpdateAnomaly verifies that neither SI nor SSI isolation
// are subject to the lost update anomaly. This anomaly is prevented
// in most cases by using the the READ_COMMITTED ANSI isolation level.
// However, only REPEATABLE_READ fully protects against it.
//
// With lost update, the write from txn1 is overwritten by the write
// from txn2, and thus txn1's update is lost. Both SI and SSI notice
// this write/write conflict and either txn1 or txn2 is aborted,
// depending on priority.
//
// Lost update would typically fail with a history such as:
//   R1(A) R2(A) I1(A) I2(A) C1 C2
//
// However, the following variant will cause a lost update in
// READ_COMMITTED and in practice requires REPEATABLE_READ to avoid.
//   R1(A) R2(A) I1(A) C1 I2(A) C2
func TestTxnDBLostUpdateAnomaly(t *testing.T) {
	txn := "R(A) I(A) C"
	verify := &verifier{
		history: "R(A)",
		checkFn: func(env map[string]int64) error {
			if env["A"] != 2 {
				return util.Errorf("expected A=2, got %d", env["A"])
			}
			return nil
		},
	}
	checkConcurrency("lost update", bothIsolations, []string{txn, txn}, verify, true, t)
}

// TestTxnDBPhantomReadAnomaly verifies that neither SI nor SSI isolation
// are subject to the phantom reads anomaly. This anomaly is prevented by
// the SQL ANSI SERIALIZABLE isolation level, though it's also prevented
// by snapshot isolation (i.e. Oracle's traditional "serializable").
//
// Phantom reads occur when a single txn does two identical queries but
// ends up reading different results. This is a variant of non-repeatable
// reads, but is special because it requires the database to be aware of
// ranges when settling concurrency issues.
//
// Phantom reads would typically fail with a history such as:
//   SC1(A-C) I2(B) C2 SC1(A-C) C1
func TestTxnDBPhantomReadAnomaly(t *testing.T) {
	txn1 := "SC(A-C) SUM(D) SC(A-C) SUM(E) C"
	txn2 := "I(B) C"
	verify := &verifier{
		history: "R(D) R(E)",
		checkFn: func(env map[string]int64) error {
			if env["D"] != env["E"] {
				return util.Errorf("expected first SUM == second SUM (%d != %d)", env["D"], env["E"])
			}
			return nil
		},
	}
	checkConcurrency("phantom read", bothIsolations, []string{txn1, txn2}, verify, true, t)
}

// TestTxnDBPhantomDeleteAnomaly verifies that neither SI nor SSI
// isolation are subject to the phantom deletion anomaly; this is
// similar to phantom reads, but verifies the delete range
// functionality causes read/write conflicts.
//
// Phantom deletes would typically fail with a history such as:
//   DR1(A-C) I2(B) C2 SC1(A-C) C1
func TestTxnDBPhantomDeleteAnomaly(t *testing.T) {
	txn1 := "DR(A-C) SC(A-C) SUM(D) C"
	txn2 := "I(B) C"
	verify := &verifier{
		history: "R(D)",
		checkFn: func(env map[string]int64) error {
			if env["D"] != 0 {
				return util.Errorf("expected delete range to yield an empty scan of same range, sum=%d", env["D"])
			}
			return nil
		},
	}
	checkConcurrency("phantom delete", bothIsolations, []string{txn1, txn2}, verify, true, t)
}

// TestTxnDBWriteSkewAnomaly verifies that SI suffers from the write
// skew anomaly but not SSI. The write skew anamoly is a condition which
// illustrates that snapshot isolation is not serializable in practice.
//
// With write skew, two transactions both read values from A and B
// respectively, but each writes to either A or B only. Thus there are
// no write/write conflicts but a cycle of dependencies which result in
// "skew". Only serializable isolation prevents this anomaly.
//
// Write skew would typically fail with a history such as:
//   SC1(A-C) SC2(A-C) I1(A) SUM1(A) I2(B) SUM2(B)
//
// In the test below, each txn reads A and B and increments one by 1.
// The read values and increment are then summed and written either to
// A or B. If we have serializable isolation, then the final value of
// A + B must be equal to 3 (the first txn sets A or B to 1, the
// second sets the other value to 2, so the total should be
// 3). Snapshot isolation, however, may not notice any conflict (see
// history above) and may set A=1, B=1.
func TestTxnDBWriteSkewAnomaly(t *testing.T) {
	txn1 := "SC(A-C) I(A) SUM(A) C"
	txn2 := "SC(A-C) I(B) SUM(B) C"
	verify := &verifier{
		history: "R(A) R(B)",
		checkFn: func(env map[string]int64) error {
			if !((env["A"] == 1 && env["B"] == 2) || (env["A"] == 2 && env["B"] == 1)) {
				return util.Errorf("expected either A=1, B=2 -or- A=2, B=1, but have A=%d, B=%d", env["A"], env["B"])
			}
			return nil
		},
	}
	checkConcurrency("write skew", onlySerializable, []string{txn1, txn2}, verify, true, t)
	checkConcurrency("write skew", onlySnapshot, []string{txn1, txn2}, verify, false, t)
}
