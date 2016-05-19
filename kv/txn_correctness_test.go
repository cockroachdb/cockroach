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
// permissions and limitations under the License.
//
// Author: Spencer Kimball (spencer.kimball@gmail.com)

package kv

import (
	"bytes"
	"fmt"
	"math"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/client"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/storage"
	"github.com/cockroachdb/cockroach/util"
	"github.com/cockroachdb/cockroach/util/leaktest"
	"github.com/cockroachdb/cockroach/util/log"
	"github.com/cockroachdb/cockroach/util/retry"
)

// correctnessTestRetryOptions uses aggressive retries with a limit on
// number of attempts so we don't get stuck behind indefinite
// backoff/retry loops. If MaxAttempts is reached, transaction will
// return retry error.
//
// Note that these options are used twice, at the stores via
// setCorrectnessRetryOptions, and when constructing the client.
var correctnessTestRetryOptions = retry.Options{
	InitialBackoff: 1 * time.Millisecond,
	MaxBackoff:     50 * time.Millisecond,
	Multiplier:     10,
	MaxRetries:     2,
}

func setCorrectnessRetryOptions(stores *storage.Stores) {
	if err := stores.VisitStores(func(s *storage.Store) error {
		s.SetRangeRetryOptions(correctnessTestRetryOptions)
		return nil
	}); err != nil {
		panic(err)
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
		c *cmd, txn *client.Txn, t *testing.T) error // execution function
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

func (c *cmd) execute(txn *client.Txn, t *testing.T) (string, error) {
	if c.prev != nil {
		<-c.prev
	}
	if log.V(2) {
		log.Infof("executing %s", c)
	}
	err := c.fn(c, txn, t)
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

func (c *cmd) makeKey(key string) []byte {
	return []byte(fmt.Sprintf("%d.%s", c.historyIdx, key))
}

func (c *cmd) getKey() []byte {
	return c.makeKey(c.key)
}

func (c *cmd) getEndKey() []byte {
	if len(c.endKey) == 0 {
		return nil
	}
	return c.makeKey(c.endKey)
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
func readCmd(c *cmd, txn *client.Txn, t *testing.T) error {
	r, err := txn.Get(c.getKey())
	if err != nil {
		return err
	}
	var value int64
	if r.Value != nil {
		value = r.ValueInt()
	}
	c.env[c.key] = value
	c.debug = fmt.Sprintf("[%d]", value)
	return nil
}

// deleteCmd deletes the value at the given key from the db.
func deleteCmd(c *cmd, txn *client.Txn, t *testing.T) error {
	return txn.Del(c.getKey())
}

// deleteRngCmd deletes the range of values from the db from [key, endKey).
func deleteRngCmd(c *cmd, txn *client.Txn, t *testing.T) error {
	return txn.DelRange(c.getKey(), c.getEndKey())
}

// scanCmd reads the values from the db from [key, endKey).
func scanCmd(c *cmd, txn *client.Txn, t *testing.T) error {
	rows, err := txn.Scan(c.getKey(), c.getEndKey(), 0)
	if err != nil {
		return err
	}
	var vals []string
	keyPrefix := []byte(fmt.Sprintf("%d.", c.historyIdx))
	for _, kv := range rows {
		key := bytes.TrimPrefix(kv.Key, keyPrefix)
		c.env[string(key)] = kv.ValueInt()
		vals = append(vals, fmt.Sprintf("%d", kv.ValueInt()))
	}
	c.debug = fmt.Sprintf("[%s]", strings.Join(vals, " "))
	return nil
}

// incCmd adds one to the value of c.key in the env (as determined by
// a previous read or write, or else assumed to be zero) and writes it
// to the db.
func incCmd(c *cmd, txn *client.Txn, t *testing.T) error {
	val, ok := c.env[c.key]
	if !ok {
		panic(fmt.Sprintf("can't increment key %q; not yet read", c.key))
	}
	r := val + 1
	if err := txn.Put(c.getKey(), r); err != nil {
		return err
	}
	c.env[c.key] = r
	c.debug = fmt.Sprintf("[%d]", r)
	return nil
}

// writeCmd sums values from the env (and possibly numeric constants)
// and writes the value to the db. "c.endKey" here needs to be parsed
// in the context of this command, which is a "+"-separated list of
// keys from the env or numeric constants to sum.
func writeCmd(c *cmd, txn *client.Txn, t *testing.T) error {
	sum := int64(0)
	for _, sp := range strings.Split(c.endKey, "+") {
		if constant, err := strconv.Atoi(sp); err != nil {
			sum += c.env[sp]
		} else {
			sum += int64(constant)
		}
	}
	err := txn.Put(c.getKey(), sum)
	c.debug = fmt.Sprintf("[%d]", sum)
	return err
}

// commitCmd commits the transaction.
func commitCmd(c *cmd, txn *client.Txn, t *testing.T) error {
	return txn.Commit()
}

type cmdSpec struct {
	fn func(c *cmd, txn *client.Txn, t *testing.T) error
	re *regexp.Regexp
}

var cmdSpecs = []*cmdSpec{
	{
		readCmd,
		regexp.MustCompile(`(R)\(([A-Z]+)\)`),
	},
	{
		incCmd,
		regexp.MustCompile(`(I)\(([A-Z]+)\)`),
	},
	{
		deleteCmd,
		regexp.MustCompile(`(D)\(([A-Z]+)\)`),
	},

	{
		deleteRngCmd,
		regexp.MustCompile(`(DR)\(([A-Z]+)-([A-Z]+)\)`),
	},
	{
		scanCmd,
		regexp.MustCompile(`(SC)\(([A-Z]+)-([A-Z]+)\)`),
	},
	{
		writeCmd,
		regexp.MustCompile(`(W)\(([A-Z]+),([A-Z0-9+]+)\)`),
	},
	{
		commitCmd,
		regexp.MustCompile(`(C)`),
	},
}

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
	if len(history) == 0 {
		return cmds
	}
	elems := strings.Split(history, " ")
	for _, elem := range elems {
		var c *cmd
		for _, spec := range cmdSpecs {
			match := spec.re.FindStringSubmatch(elem)
			if len(match) < 2 {
				continue
			}
			var key, endKey string
			if len(match) > 2 {
				key = match[2]
			}
			if len(match) > 3 {
				endKey = match[3]
			}
			c = &cmd{name: match[1], key: key, endKey: endKey, txnIdx: txnIdx, fn: spec.fn}
			break
		}
		if c == nil {
			t.Fatalf("failed to parse command %q", elem)
		}
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
	bothIsolations   = []roachpb.IsolationType{roachpb.SERIALIZABLE, roachpb.SNAPSHOT}
	onlySerializable = []roachpb.IsolationType{roachpb.SERIALIZABLE}
	onlySnapshot     = []roachpb.IsolationType{roachpb.SNAPSHOT}
)

// enumerateIsolations returns a slice enumerating all combinations of
// isolation types across the transactions. The inner slice describes
// the isolation type for each transaction. The outer slice contains
// each possible combination of such transaction isolations.
func enumerateIsolations(numTxns int, isolations []roachpb.IsolationType) [][]roachpb.IsolationType {
	// Use a count from 0 to pow(# isolations, numTxns)-1 and examine
	// n-ary digits to get all possible combinations of txn isolations.
	n := len(isolations)
	result := [][]roachpb.IsolationType{}
	for i := 0; i < int(math.Pow(float64(n), float64(numTxns))); i++ {
		desc := make([]roachpb.IsolationType, numTxns)
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
	defer leaktest.AfterTest(t)()
	SSI := roachpb.SERIALIZABLE
	SI := roachpb.SNAPSHOT
	expIsolations := [][]roachpb.IsolationType{
		{SSI, SSI, SSI},
		{SI, SSI, SSI},
		{SSI, SI, SSI},
		{SI, SI, SSI},
		{SSI, SSI, SI},
		{SI, SSI, SI},
		{SSI, SI, SI},
		{SI, SI, SI},
	}
	if enum := enumerateIsolations(3, bothIsolations); !reflect.DeepEqual(enum, expIsolations) {
		t.Errorf("expected enumeration to match %s; got %s", expIsolations, enum)
	}

	expDegenerate := [][]roachpb.IsolationType{
		{SSI, SSI, SSI},
	}
	if enum := enumerateIsolations(3, onlySerializable); !reflect.DeepEqual(enum, expDegenerate) {
		t.Errorf("expected enumeration to match %s; got %s", expDegenerate, enum)
	}
}

// enumeratePriorities returns a slice enumerating all combinations of the
// specified slice of priorities.
func enumeratePriorities(priorities []int32) [][]int32 {
	var results [][]int32
	for i := 0; i < len(priorities); i++ {
		leftover := enumeratePriorities(append(append([]int32(nil), priorities[:i]...), priorities[i+1:]...))
		if len(leftover) == 0 {
			results = [][]int32{{priorities[i]}}
		}
		for j := 0; j < len(leftover); j++ {
			results = append(results, append([]int32{priorities[i]}, leftover[j]...))
		}
	}
	return results
}

func TestEnumeratePriorities(t *testing.T) {
	defer leaktest.AfterTest(t)()
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
			results = [][]*cmd{{txns[i][0]}}
		}
		for j := 0; j < len(leftover); j++ {
			results = append(results, append([]*cmd{txns[i][0]}, leftover[j]...))
		}
	}
	return results
}

func TestEnumerateHistories(t *testing.T) {
	defer leaktest.AfterTest(t)()
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

// verifier first executes the pre-history, which sets existing values
// as necessary, then executes the history and then invokes checkFn to
// verify the environment (map from key to value) left from executing
// the history.
type verifier struct {
	preHistory string
	history    string
	checkFn    func(env map[string]int64) error
}

// historyVerifier parses a planned transaction execution history into
// commands per transaction and each command's previous dependency.
// When run, each transaction's commands are executed via a goroutine
// in a separate txn. The results of the execution are added to the
// actual commands slice. When all txns have completed the actual history
// is compared to the expected history.
type historyVerifier struct {
	name           string
	txns           [][]*cmd
	verify         *verifier
	preHistoryCmds []*cmd
	verifyCmds     []*cmd
	expSuccess     bool
	symmetric      bool

	sync.Mutex // protects actual slice of command outcomes.
	actual     []string
	wg         sync.WaitGroup
}

func newHistoryVerifier(name string, txns []string, verify *verifier, expSuccess bool, t *testing.T) *historyVerifier {
	return &historyVerifier{
		name:           name,
		txns:           parseHistories(txns, t),
		verify:         verify,
		preHistoryCmds: parseHistory(0, verify.preHistory, t),
		verifyCmds:     parseHistory(0, verify.history, t),
		expSuccess:     expSuccess,
		symmetric:      areHistoriesSymmetric(txns),
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

func (hv *historyVerifier) run(isolations []roachpb.IsolationType, db *client.DB, t *testing.T) {
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
	isolations []roachpb.IsolationType, cmds []*cmd, db *client.DB, t *testing.T) error {
	// Execute pre-history if applicable.
	if hv.preHistoryCmds != nil {
		if str, _, err := hv.runCmds(hv.preHistoryCmds, historyIdx, db, t); err != nil {
			t.Errorf("failed on execution of pre history %s: %s", str, err)
			return err
		}
	}
	plannedStr := historyString(cmds)
	if log.V(1) {
		log.Infof("attempting iso=%v pri=%v history=%s", isolations, priorities, plannedStr)
	}

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
				t.Errorf("(%s): unexpected failure running %s: %v", cmds, cmds[i], err)
			}
		}(i, txnCmds)
	}
	hv.wg.Wait()

	// Construct string for actual history.
	actualStr := strings.Join(hv.actual, " ")

	// Verify history.
	verifyStr, verifyEnv, err := hv.runCmds(hv.verifyCmds, historyIdx, db, t)
	if err != nil {
		t.Errorf("failed on execution of verification history %s: %s", verifyStr, err)
		return err
	}
	err = hv.verify.checkFn(verifyEnv)
	if err == nil {
		if log.V(1) {
			log.Infof("PASSED: iso=%v, pri=%v, history=%q", isolations, priorities, actualStr)
		}
	}
	if hv.expSuccess && err != nil {
		t.Errorf("%d: iso=%v, pri=%v, history=%q: actual=%q, verify=%q: %s",
			historyIdx, isolations, priorities, plannedStr, actualStr, verifyStr, err)
	}
	return err
}

func (hv *historyVerifier) runCmds(cmds []*cmd, historyIdx int, db *client.DB, t *testing.T) (string, map[string]int64, error) {
	var strs []string
	env := map[string]int64{}
	err := db.Txn(func(txn *client.Txn) error {
		for _, c := range cmds {
			c.historyIdx = historyIdx
			c.env = env
			c.init(nil)
			fmtStr, err := c.execute(txn, t)
			if err != nil {
				return err
			}
			strs = append(strs, fmt.Sprintf(fmtStr, 0, 0))
		}
		return nil
	})
	return strings.Join(strs, " "), env, err
}

func (hv *historyVerifier) runTxn(txnIdx int, priority int32,
	isolation roachpb.IsolationType, cmds []*cmd, db *client.DB, t *testing.T) error {
	var retry int
	txnName := fmt.Sprintf("txn%d", txnIdx)
	err := db.Txn(func(txn *client.Txn) error {
		txn.SetDebugName(txnName, 0)
		if isolation == roachpb.SNAPSHOT {
			if err := txn.SetIsolation(roachpb.SNAPSHOT); err != nil {
				return err
			}
		}
		txn.InternalSetPriority(priority)

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
		if log.V(2) {
			log.Infof("%s, retry=%d", txnName, retry)
		}
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

func (hv *historyVerifier) runCmd(txn *client.Txn, txnIdx, retry, cmdIdx int, cmds []*cmd, t *testing.T) error {
	fmtStr, err := cmds[cmdIdx].execute(txn, t)
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
func checkConcurrency(name string, isolations []roachpb.IsolationType, txns []string,
	verify *verifier, expSuccess bool, t *testing.T) {
	verifier := newHistoryVerifier(name, txns, verify, expSuccess, t)
	dbCtx := client.DefaultDBContext()
	dbCtx.TxnRetryOptions = correctnessTestRetryOptions
	s, _ := createTestDBWithContext(t, dbCtx)
	defer s.Stop()
	setCorrectnessRetryOptions(s.Stores)
	verifier.run(isolations, s.DB, t)
}

// The following tests for concurrency anomalies include documentation
// taken from the "Concurrency Control Chapter" from the Handbook of
// Database Technology, written by Patrick O'Neil <poneil@cs.umb.edu>:
// http://www.cs.umb.edu/~poneil/CCChapter.PDF.
//
// Notation for planned histories:
//   R(x) - read from key "x"
//   I(x) - increment key "x" by 1 (shorthand for W(x,x+1)
//   SC(x-y) - scan values from keys "x"-"y"
//   D(x) - delete key "x"
//   DR(x-y) - delete range of keys "x"-"y"
//   W(x,y[+z+...]) - writes sum of values y+z+... to x
//   C - commit
//
// Notation for actual histories:
//   Rn.m(x) - read from txn "n" ("m"th retry) of key "x"
//   In.m(x) - increment from txn "n" ("m"th retry) of key "x"
//   Dn.m(x) - delete key from txn ("m"th retry) of key "x"
//   DRn.m(x-y) - delete range from txn "n" ("m"th retry) of keys "x"-"y"
//   SCn.m(x-y) - scan from txn "n" ("m"th retry) of keys "x"-"y"
//   Wn.m(x,y[+z+...]) - write sum of values y+z+... to x from txn "n" ("m"th retry)
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
// Inconsistent analysis would typically fail with a history such as:
//    R1(A) R2(B) I2(B) R2(A) I2(A) R1(B) C1 C2
func TestTxnDBInconsistentAnalysisAnomaly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	txn1 := "R(A) R(B) W(C,A+B) C"
	txn2 := "R(A) R(B) I(A) I(B) C"
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
// in most cases by using the READ_COMMITTED ANSI isolation level.
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
	defer leaktest.AfterTest(t)()
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

// TestTxnDBLostDeleteAnomaly verifies that neither SI nor SSI
// isolation are subject to the lost delete anomaly. See #6240.
//
// With lost delete, the two deletions from txn2 are interleaved
// with a read and write from txn1, allowing txn1 to read a pre-
// existing value for A and then write to B, rewriting history
// underneath txn2's deletion of B.
//
// This anomaly is prevented by the use of deletion tombstones,
// even on keys which have no values written.
//
// Lost delete would typically fail with a history such as:
//   D2(A) R1(A) D2(B) C2 W1(B,A) C1
func TestTxnDBLostDeleteAnomaly(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// B must not exceed A.
	txn1 := "R(A) W(B,A) C"
	txn2 := "D(A) D(B) C"
	verify := &verifier{
		preHistory: "W(A,1)",
		history:    "R(A) R(B)",
		checkFn: func(env map[string]int64) error {
			if env["B"] != 0 && env["A"] == 0 {
				return util.Errorf("expected B = %d <= %d = A", env["B"], env["A"])
			}
			return nil
		},
	}
	checkConcurrency("lost update (delete)", onlySnapshot, []string{txn1, txn2}, verify, true, t)
}

// TestTxnDBLostDeleteRangeAnomaly verifies that neither SI nor SSI
// isolation are subject to the lost delete range anomaly. See #6240.
//
// With lost delete range, the delete range for keys B-C leave no
// deletion tombstones (as there are an infinite number of keys in the
// range [B,C)). Without deletion tombstones, the anomaly manifests in
// snapshot mode when txn1 pushes txn2 to commit at a higher timestamp
// and then txn1 writes B and commits an an earlier timestamp. The
// delete range request therefore committed but failed to delete the
// value written to key B.
//
// This anomaly is prevented by making snapshot transactions which
// involve a range deletion restart when they are pushed.
//
// Lost delete range would typically fail with a history such as:
//   D2(A) DR2(B-C) R1(A) C2 W1(B,A) C1
func TestTxnDBLostDeleteRangeAnomaly(t *testing.T) {
	defer leaktest.AfterTest(t)()

	// B must not exceed A.
	txn1 := "R(A) W(B,A) C"
	txn2 := "D(A) DR(B-C) C"
	verify := &verifier{
		preHistory: "W(A,1)",
		history:    "R(A) R(B)",
		checkFn: func(env map[string]int64) error {
			if env["B"] != 0 && env["A"] == 0 {
				return util.Errorf("expected B = %d <= %d = A", env["B"], env["A"])
			}
			return nil
		},
	}
	checkConcurrency("lost update (range delete)", onlySnapshot, []string{txn1, txn2}, verify, true, t)
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
//   R2(B) SC1(A-C) I2(B) C2 SC1(A-C) C1
func TestTxnDBPhantomReadAnomaly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	txn1 := "SC(A-C) W(D,A+B) SC(A-C) W(E,A+B) C"
	txn2 := "R(B) I(B) C"
	verify := &verifier{
		history: "R(D) R(E)",
		checkFn: func(env map[string]int64) error {
			if env["D"] != env["E"] {
				return util.Errorf("expected D == E (%d != %d)", env["D"], env["E"])
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
//   R2(B) DR1(A-C) I2(B) C2 SC1(A-C) W1(D,A+B) C1
func TestTxnDBPhantomDeleteAnomaly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	txn1 := "DR(A-C) SC(A-C) W(D,A+B) C"
	txn2 := "R(B) I(B) C"
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
//   SC1(A-C) SC2(A-C) W1(A,A+B+1) C1 W2(B,A+B+1) C2
//
// In the test below, each txn reads A and B and increments one by 1.
// The read values and increment are then summed and written either to
// A or B. If we have serializable isolation, then the final value of
// A + B must be equal to 3 (the first txn sets A or B to 1, the
// second sets the other value to 2, so the total should be
// 3). Snapshot isolation, however, may not notice any conflict (see
// history above) and may set A=1, B=1.
func TestTxnDBWriteSkewAnomaly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	txn1 := "SC(A-C) W(A,A+B+1) C"
	txn2 := "SC(A-C) W(B,A+B+1) C"
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
