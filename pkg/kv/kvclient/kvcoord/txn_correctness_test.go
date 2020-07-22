// Copyright 2014 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvcoord

import (
	"bytes"
	"context"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/localtestcluster"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
)

type retryError struct {
	txnIdx, cmdIdx int
}

func (re *retryError) Error() string {
	return fmt.Sprintf("retry error at txn %d, cmd %d", re.txnIdx+1, re.cmdIdx)
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
	name        string                                               // name of the cmd for debug output
	key, endKey string                                               // key and optional endKey
	debug       string                                               // optional debug string
	txnIdx      int                                                  // transaction index in the history
	historyIdx  int                                                  // this suffixes key so tests get unique keys
	expRetry    bool                                                 // true if we expect a retry
	fn          func(ctx context.Context, c *cmd, txn *kv.Txn) error // execution function
	ch          chan error                                           // channel for other commands to wait
	prev        *cmd                                                 // this command must wait on previous command before executing
	env         map[string]int64                                     // contains all previously read values
}

func (c *cmd) init(prev *cmd) {
	c.prev = prev
	c.ch = make(chan error, 1)
	c.debug = ""
}

func (c *cmd) done(err error) {
	c.ch <- err
}

func (c *cmd) clone() *cmd {
	clone := *c
	clone.ch = nil
	clone.prev = nil
	return &clone
}

func (c *cmd) execute(txn *kv.Txn, t *testing.T) (string, error) {
	if c.prev != nil {
		if log.V(2) {
			log.Infof(context.Background(), "%s waiting on %s", c, c.prev)
		}
		if err := <-c.prev.ch; err != nil {
			return "", err
		}
	}
	if log.V(2) {
		log.Infof(context.Background(), "executing %s", c)
	}
	err := c.fn(context.Background(), c, txn)
	if err == nil {
		c.ch <- nil
	}
	if len(c.key) > 0 && len(c.endKey) > 0 {
		return fmt.Sprintf("%s%%d.%%d(%s-%s)%s", c.name, c.key, c.endKey, c.debug), err
	}
	if len(c.key) > 0 {
		return fmt.Sprintf("%s%%d.%%d(%s)%s", c.name, c.key, c.debug), err
	}
	return fmt.Sprintf("%s%%d.%%d%s", c.name, c.debug), err
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
	var retryStr string
	if c.expRetry {
		retryStr = "[exp retry]"
	}
	if len(c.key) > 0 && len(c.endKey) > 0 {
		if c.name == "W" {
			return fmt.Sprintf("%s%d(%s,%s)%s", c.name, c.txnIdx+1, c.key, c.endKey, retryStr)
		}
		// c.name == "SC".
		return fmt.Sprintf("%s%d(%s-%s)%s", c.name, c.txnIdx+1, c.key, c.endKey, retryStr)
	}
	if len(c.key) > 0 {
		return fmt.Sprintf("%s%d(%s)%s", c.name, c.txnIdx+1, c.key, retryStr)
	}
	return fmt.Sprintf("%s%d%s", c.name, c.txnIdx+1, retryStr)
}

// readCmd reads a value from the db and stores it in the env.
func readCmd(ctx context.Context, c *cmd, txn *kv.Txn) error {
	r, err := txn.Get(ctx, c.getKey())
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
func deleteCmd(ctx context.Context, c *cmd, txn *kv.Txn) error {
	return txn.Del(ctx, c.getKey())
}

// deleteRngCmd deletes the range of values from the db from [key, endKey).
func deleteRngCmd(ctx context.Context, c *cmd, txn *kv.Txn) error {
	return txn.DelRange(ctx, c.getKey(), c.getEndKey())
}

// scanCmd reads the values from the db from [key, endKey).
func scanCmd(ctx context.Context, c *cmd, txn *kv.Txn) error {
	rows, err := txn.Scan(ctx, c.getKey(), c.getEndKey(), 0)
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
func incCmd(ctx context.Context, c *cmd, txn *kv.Txn) error {
	val, ok := c.env[c.key]
	if !ok {
		panic(fmt.Sprintf("can't increment key %q; not yet read", c.key))
	}
	r := val + 1
	if err := txn.Put(ctx, c.getKey(), r); err != nil {
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
func writeCmd(ctx context.Context, c *cmd, txn *kv.Txn) error {
	sum := int64(0)
	for _, sp := range strings.Split(c.endKey, "+") {
		if constant, err := strconv.Atoi(sp); err != nil {
			sum += c.env[sp]
		} else {
			sum += int64(constant)
		}
	}
	err := txn.Put(ctx, c.getKey(), sum)
	c.debug = fmt.Sprintf("[%d]", sum)
	return err
}

// commitCmd commits the transaction.
func commitCmd(ctx context.Context, c *cmd, txn *kv.Txn) error {
	return txn.Commit(ctx)
}

type cmdSpec struct {
	fn func(ctx context.Context, c *cmd, txn *kv.Txn) error
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
		results = append(results, parseHistory(i, history, t))
	}
	return results
}

// enumeratePriorities returns a slice enumerating all combinations of
// priorities across the transactions. The inner slice describes the
// priority for each transaction. The outer slice contains each possible
// combination of such transaction priorities.
func enumeratePriorities(numTxns int, priorities []enginepb.TxnPriority) [][]enginepb.TxnPriority {
	n := len(priorities)
	result := [][]enginepb.TxnPriority{}
	for i := 0; i < int(math.Pow(float64(n), float64(numTxns))); i++ {
		desc := make([]enginepb.TxnPriority, numTxns)
		val := i
		for j := 0; j < numTxns; j++ {
			desc[j] = priorities[val%n]
			val /= n
		}
		result = append(result, desc)
	}
	return result
}

func TestEnumeratePriorities(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	p1 := enginepb.TxnPriority(1)
	p2 := enginepb.TxnPriority(2)
	expPriorities := [][]enginepb.TxnPriority{
		{p1, p1, p1},
		{p2, p1, p1},
		{p1, p2, p1},
		{p2, p2, p1},
		{p1, p1, p2},
		{p2, p1, p2},
		{p1, p2, p2},
		{p2, p2, p2},
	}
	enum := enumeratePriorities(3, []enginepb.TxnPriority{p1, p2})
	if !reflect.DeepEqual(enum, expPriorities) {
		t.Errorf("expected enumeration to match %v; got %v", expPriorities, enum)
	}
}

// sampleHistories returns a random sub sample of histories up
// to and including the specified sample percentage.
func sampleHistories(enumHis [][]*cmd, samplePct float64) [][]*cmd {
	if skip := int(1.0 / samplePct); skip > 1 {
		// Randomize and sample.
		perm := rand.Perm(len(enumHis))
		newHis := [][]*cmd{}
		for i := 0; i < len(enumHis); i += skip {
			newHis = append(newHis, enumHis[perm[i]])
		}
		enumHis = newHis
	}
	return enumHis
}

// enumerateHistories returns a slice enumerating all combinations of
// collated histories possible given the specified transactions. Each
// input transaction is a slice of commands. The order of commands for
// each transaction is stable, but the enumeration provides all
// possible interleavings between transactions. If equal is true,
// skips exactly N-1/N of the enumeration (where N=len(txns)).
func enumerateHistories(txns [][]*cmd, equal bool) [][]*cmd {
	var results [][]*cmd
	numTxns := len(txns)
	if equal {
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
	defer log.Scope(t).Close(t)
	txns := parseHistories([]string{"I(A) C", "I(A) C"}, t)
	enum := enumerateHistories(txns, false)
	enumStrs := make([]string, len(enum))
	for i, history := range enum {
		enumStrs[i] = historyString(history)
	}
	enumEqual := enumerateHistories(txns, true)
	enumEqualStrs := make([]string, len(enumEqual))
	for i, history := range enumEqual {
		enumEqualStrs[i] = historyString(history)
	}
	expEnumStrs := []string{
		"I1(A) C1 I2(A) C2",
		"I1(A) I2(A) C1 C2",
		"I1(A) I2(A) C2 C1",
		"I2(A) I1(A) C1 C2",
		"I2(A) I1(A) C2 C1",
		"I2(A) C2 I1(A) C1",
	}
	expEnumEqualStrs := []string{
		"I1(A) C1 I2(A) C2",
		"I1(A) I2(A) C1 C2",
		"I1(A) I2(A) C2 C1",
	}
	if !reflect.DeepEqual(enumStrs, expEnumStrs) {
		t.Errorf("expected enumeration to match %s; got %s", expEnumStrs, enumStrs)
	}
	if !reflect.DeepEqual(enumEqualStrs, expEnumEqualStrs) {
		t.Errorf("expected equal enumeration to match %s; got %s", expEnumEqualStrs, enumEqualStrs)
	}
}

// enumerateHistoriesAfterRetry returns a slice enumerating all
// combinations of alternate histories starting after the command
// indicated by the supplied retry error.
func enumerateHistoriesAfterRetry(err *retryError, h []*cmd) [][]*cmd {
	// First, capture the history up to and including the
	// command which caused the retry error.
	var retryH []*cmd
	var cmds [][]*cmd
	var foundRetry bool
	for _, c := range h {
		// Once we've recaptured the entire history up to the retry error,
		// we add all commands to the txnMap. We also always add all
		// commands which belong to the transaction which encountered the
		// retry error, as those will need to be retried in full.
		if foundRetry || err.txnIdx == c.txnIdx {
			if c.txnIdx >= len(cmds) {
				for i := len(cmds); i <= c.txnIdx; i++ {
					cmds = append(cmds, []*cmd{})
				}
			}
			cmds[c.txnIdx] = append(cmds[c.txnIdx], c)
		}
		if !foundRetry {
			cloned := c.clone() // clone the command and set the expect retry flag
			if err.txnIdx == c.txnIdx && err.cmdIdx+1 == len(cmds[c.txnIdx]) {
				foundRetry = true
				cloned.expRetry = true
			}
			retryH = append(retryH, cloned)
		}
	}

	// Now, enumerate histories containing all commands remaining from non-
	// retrying txns as well as the complete history of the retrying txn.
	results := enumerateHistories(cmds, false)

	// Prefix all histories with the retry history.
	for i, h := range results {
		results[i] = append(append([]*cmd(nil), retryH...), h...)
	}

	return results
}

func TestEnumerateHistoriesAfterRetry(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	txns := parseHistories([]string{"R(A) W(B,A) C", "D(A) D(B) C"}, t)
	enum := enumerateHistories(txns, false)
	for i, e := range enum {
		if log.V(1) {
			log.Infof(context.Background(), "enum(%d): %s", i, historyString(e))
		}
	}
	testCases := []struct {
		enumIdx     int
		txnIdx      int
		cmdIdx      int
		expEnumStrs []string
	}{
		{16, 0, 1, []string{
			"D2(A) D2(B) R1(A) W1(B,A)[exp retry] R1(A) W1(B,A) C1 C2",
			"D2(A) D2(B) R1(A) W1(B,A)[exp retry] R1(A) W1(B,A) C2 C1",
			"D2(A) D2(B) R1(A) W1(B,A)[exp retry] R1(A) C2 W1(B,A) C1",
			"D2(A) D2(B) R1(A) W1(B,A)[exp retry] C2 R1(A) W1(B,A) C1",
		}},
		{4, 1, 0, []string{
			"R1(A) D2(A)[exp retry] W1(B,A) C1 D2(A) D2(B) C2",
			"R1(A) D2(A)[exp retry] W1(B,A) D2(A) C1 D2(B) C2",
			"R1(A) D2(A)[exp retry] W1(B,A) D2(A) D2(B) C1 C2",
			"R1(A) D2(A)[exp retry] W1(B,A) D2(A) D2(B) C2 C1",
			"R1(A) D2(A)[exp retry] D2(A) W1(B,A) C1 D2(B) C2",
			"R1(A) D2(A)[exp retry] D2(A) W1(B,A) D2(B) C1 C2",
			"R1(A) D2(A)[exp retry] D2(A) W1(B,A) D2(B) C2 C1",
			"R1(A) D2(A)[exp retry] D2(A) D2(B) W1(B,A) C1 C2",
			"R1(A) D2(A)[exp retry] D2(A) D2(B) W1(B,A) C2 C1",
			"R1(A) D2(A)[exp retry] D2(A) D2(B) C2 W1(B,A) C1",
		}},
	}

	for i, c := range testCases {
		retryErr := &retryError{txnIdx: c.txnIdx, cmdIdx: c.cmdIdx}
		retryEnum := enumerateHistoriesAfterRetry(retryErr, enum[c.enumIdx])
		enumStrs := make([]string, len(retryEnum))
		for j, history := range retryEnum {
			enumStrs[j] = historyString(history)
		}
		if !reflect.DeepEqual(enumStrs, c.expEnumStrs) {
			t.Errorf("%d: expected enumeration to match %s; got %s", i, c.expEnumStrs, enumStrs)
		}
	}
}

// areHistoriesEqual returns whether all txn histories are the same.
func areHistoriesEqual(txns []string) bool {
	for i := 1; i < len(txns); i++ {
		if txns[i] != txns[0] {
			return false
		}
	}
	return true
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
	idx            int
	txns           [][]*cmd
	verify         *verifier
	preHistoryCmds []*cmd
	verifyCmds     []*cmd
	equal          bool

	// retriedTxns keeps track of which transaction histories have retried
	// so that we can avoid histories where endless retries occur.
	retriedTxns map[int]struct{}

	mu struct {
		syncutil.Mutex
		actual []string
	}
}

func newHistoryVerifier(
	name string, txns []string, verify *verifier, t *testing.T,
) *historyVerifier {
	return &historyVerifier{
		name:           name,
		txns:           parseHistories(txns, t),
		verify:         verify,
		preHistoryCmds: parseHistory(0, verify.preHistory, t),
		verifyCmds:     parseHistory(0, verify.history, t),
		equal:          areHistoriesEqual(txns),
	}
}

func (hv *historyVerifier) run(db *kv.DB, t *testing.T) {
	log.Infof(context.Background(), "verifying all possible histories for the %q anomaly", hv.name)
	enumPri := enumeratePriorities(len(hv.txns), []enginepb.TxnPriority{1, enginepb.MaxTxnPriority})
	enumHis := enumerateHistories(hv.txns, hv.equal)

	for _, p := range enumPri {
		for _, h := range enumHis {
			hv.retriedTxns = map[int]struct{}{} // always reset the retried txns set
			if err := hv.runHistoryWithRetry(p, h, db, t); err != nil {
				t.Errorf("expected success, experienced %s", err)
				return
			}
		}
	}
}

// runHistoryWithRetry intercepts retry errors. If one is encountered,
// alternate histories are generated which all contain the exact
// history prefix which encountered the error, but which recombine the
// remaining commands with all of the commands from the retrying
// history.
//
// This process continues recursively if there are further retries.
func (hv *historyVerifier) runHistoryWithRetry(
	priorities []enginepb.TxnPriority, cmds []*cmd, db *kv.DB, t *testing.T,
) error {
	if err := hv.runHistory(priorities, cmds, db, t); err != nil {
		if log.V(1) {
			log.Infof(context.Background(), "got an error running history %s: %s", historyString(cmds), err)
		}
		var retry *retryError
		if !errors.As(err, &retry) {
			return err
		}

		if _, hasRetried := hv.retriedTxns[retry.txnIdx]; hasRetried {
			if log.V(1) {
				log.Infof(context.Background(), "retried txn %d twice; skipping history", retry.txnIdx+1)
			}
			return nil
		}
		hv.retriedTxns[retry.txnIdx] = struct{}{}

		// Randomly subsample 5% of histories for reduced execution time.
		enumHis := sampleHistories(enumerateHistoriesAfterRetry(retry, cmds), 0.05)
		for i, h := range enumHis {
			if log.V(1) {
				log.Infof(context.Background(), "after retry, running alternate history %d of %d", i, len(enumHis))
			}
			if err := hv.runHistoryWithRetry(priorities, h, db, t); err != nil {
				return err
			}
		}
	}
	return nil
}

func (hv *historyVerifier) runHistory(
	priorities []enginepb.TxnPriority, cmds []*cmd, db *kv.DB, t *testing.T,
) error {
	hv.idx++
	if t.Failed() {
		return errors.New("already failed")
	}
	// Execute pre-history if applicable.
	if hv.preHistoryCmds != nil {
		if str, _, err := hv.runCmds("pre-history", hv.preHistoryCmds, db, t); err != nil {
			t.Errorf("failed on execution of pre history %s: %s", str, err)
			return err
		}
	}
	plannedStr := historyString(cmds)

	if log.V(1) {
		log.Infof(context.Background(), "pri=%d history=%s", priorities, plannedStr)
	}

	hv.mu.actual = []string{}
	txnMap := map[int][]*cmd{}
	var prev *cmd
	for _, c := range cmds {
		c.historyIdx = hv.idx
		txnMap[c.txnIdx] = append(txnMap[c.txnIdx], c)
		c.init(prev)
		prev = c
	}

	var wg sync.WaitGroup
	wg.Add(len(txnMap))
	retryErrs := make(chan *retryError, len(txnMap))
	errs := make(chan error, 1) // only populated while buffer available

	for i, txnCmds := range txnMap {
		go func(i int, txnCmds []*cmd) {
			if err := hv.runTxn(i, priorities[i], txnCmds, db, t); err != nil {
				if re := (*retryError)(nil); !errors.As(err, &re) {
					reportErr := errors.Wrapf(err, "(%s): unexpected failure", cmds)
					select {
					case errs <- reportErr:
					default:
						t.Error(reportErr)
					}
				} else {
					retryErrs <- re
				}
			}
			wg.Done()
		}(i, txnCmds)
	}
	wg.Wait()

	// For serious errors, report the first one.
	select {
	case err := <-errs:
		return err
	default:
	}
	// In the absence of serious errors, report the first retry error, if any.
	select {
	case re := <-retryErrs:
		return re
	default:
	}

	// Construct string for actual history.
	actualStr := strings.Join(hv.mu.actual, " ")

	// Verify history.
	verifyStr, verifyEnv, err := hv.runCmds("verify", hv.verifyCmds, db, t)
	if err != nil {
		t.Errorf("failed on execution of verification history %s: %s", verifyStr, err)
		return err
	}
	err = hv.verify.checkFn(verifyEnv)
	if err == nil {
		if log.V(1) {
			log.Infof(context.Background(), "PASSED: pri=%d, history=%q", priorities, actualStr)
		}
	}
	if err != nil {
		t.Errorf("%d: pri=%d, history=%q: actual=%q, verify=%q: %s",
			hv.idx, priorities, plannedStr, actualStr, verifyStr, err)
	}
	return err
}

func (hv *historyVerifier) runCmds(
	txnName string, cmds []*cmd, db *kv.DB, t *testing.T,
) (string, map[string]int64, error) {
	var strs []string
	env := map[string]int64{}
	err := db.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		txn.SetDebugName(txnName)
		for _, c := range cmds {
			c.historyIdx = hv.idx
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

func (hv *historyVerifier) runTxn(
	txnIdx int, priority enginepb.TxnPriority, cmds []*cmd, db *kv.DB, t *testing.T,
) error {
	var retry int
	txnName := fmt.Sprintf("txn %d", txnIdx+1)
	cmdIdx := -1

	// db.Txn will set the transaction's original timestamp, so if this txn's
	// first command has a prev command, wait for it before calling db.Txn so
	// that we're guaranteed to have a later timestamp.
	if prev := cmds[0].prev; prev != nil {
		err := <-prev.ch
		prev.ch <- err
	}

	err := db.Txn(context.Background(), func(ctx context.Context, txn *kv.Txn) error {
		// If this is 2nd attempt, and a retry wasn't expected, return a
		// retry error which results in further histories being enumerated.
		if retry++; retry > 1 {
			if !cmds[cmdIdx].expRetry {
				// Propagate retry error to history execution to enumerate all
				// histories where this txn retries at this command.
				return &retryError{txnIdx: txnIdx, cmdIdx: cmdIdx}
			}
			// We're expecting a retry, so just send nil down the done channel.
			cmds[cmdIdx].done(nil)
		}

		txn.SetDebugName(txnName)
		txn.TestingSetPriority(priority)

		env := map[string]int64{}
		for cmdIdx+1 < len(cmds) {
			cmdIdx++
			cmds[cmdIdx].env = env
			_, err := hv.runCmd(txn, txnIdx, retry, cmds[cmdIdx], t)
			if err != nil {
				if log.V(1) {
					log.Infof(context.Background(), "%s: failed running %s: %s", txnName, cmds[cmdIdx], err)
				}
				return err
			}
		}
		return nil
	})
	if err != nil {
		for _, c := range cmds[cmdIdx:] {
			c.done(err)
		}
	}
	return err
}

func (hv *historyVerifier) runCmd(
	txn *kv.Txn, txnIdx, retry int, c *cmd, t *testing.T,
) (string, error) {
	fmtStr, err := c.execute(txn, t)
	cmdStr := fmt.Sprintf(fmtStr, txnIdx+1, retry)
	hv.mu.Lock()
	hv.mu.actual = append(hv.mu.actual, cmdStr)
	hv.mu.Unlock()
	return cmdStr, err
}

// checkConcurrency creates a history verifier, starts a new database
// and runs the verifier.
func checkConcurrency(name string, txns []string, verify *verifier, t *testing.T) {
	verifier := newHistoryVerifier(name, txns, verify, t)
	s := &localtestcluster.LocalTestCluster{
		StoreTestingKnobs: &kvserver.StoreTestingKnobs{
			DontRetryPushTxnFailures: true,
			// Immediately attempt to recover pushed transactions with STAGING
			// statuses, even if the push would otherwise fail because the
			// pushee has not yet expired. This prevents low-priority pushes from
			// occasionally throwing retry errors due to DontRetryPushTxnFailures
			// after the pushee's commit has already returned successfully. This
			// is a result of the asynchronous nature of making transaction commits
			// explicit after a parallel commit.
			EvalKnobs: kvserverbase.BatchEvalTestingKnobs{
				RecoverIndeterminateCommitsOnFailedPushes: true,
			},
		},
	}
	s.Start(t, testutils.NewNodeTestBaseContext(), InitFactoryForLocalTestCluster)
	defer s.Stop()
	verifier.run(s.DB, t)
}

// The following tests for concurrency anomalies include documentation
// taken from the "Concurrency Control Chapter" from the Handbook of
// Database Technology, written by Patrick O'Neil <poneil@cs.umb.edu>:
// http://www.cs.umb.edu/~poneil/CCChapter.PDF.
//
// Notation for planned histories:
//   R(x) - read from key "x"
//   SC(x-y) - scan values from keys "x"-"y"
//   D(x) - delete key "x"
//   DR(x-y) - delete range of keys "x"-"y"
//   W(x,y+z+...) - writes sum of values y+z+... to x
//   I(x) - increment key "x" by 1 (shorthand for W(x,x+1)
//   C - commit
//
// Notation for actual histories:
//   Rn.m(x) - read from txn "n" ("m"th retry) of key "x"
//   SCn.m(x-y) - scan from txn "n" ("m"th retry) of keys "x"-"y"
//   Dn.m(x) - delete key from txn ("m"th retry) of key "x"
//   DRn.m(x-y) - delete range from txn "n" ("m"th retry) of keys "x"-"y"
//   Wn.m(x,y+z+...) - write sum of values y+z+... to x from txn "n" ("m"th retry)
//   In.m(x) - increment from txn "n" ("m"th retry) of key "x"
//   Cn.m - commit of txn "n" ("m"th retry)

// TestTxnDBReadSkewAnomaly verifies that transactions are not
// subject to the read skew anomaly, an example of a database
// constraint violation known as inconsistent analysis (see
// http://research.microsoft.com/pubs/69541/tr-95-51.pdf). This anomaly
// is prevented by REPEATABLE_READ.
//
// With read skew, there are two concurrent txns. One
// reads keys A & B, the other reads and then writes keys A & B. The
// reader must not see intermediate results from the reader/writer.
//
// Read skew would typically fail with a history such as:
//    R1(A) R2(B) I2(B) R2(A) I2(A) R1(B) C1 C2
func TestTxnDBReadSkewAnomaly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderShort(t)

	txn1 := "R(A) R(B) W(C,A+B) C"
	txn2 := "R(A) R(B) I(A) I(B) C"
	verify := &verifier{
		history: "R(C)",
		checkFn: func(env map[string]int64) error {
			if env["C"] != 2 && env["C"] != 0 {
				return errors.Errorf("expected C to be either 0 or 2, got %d", env["C"])
			}
			return nil
		},
	}
	checkConcurrency("read skew", []string{txn1, txn2}, verify, t)
}

// TestTxnDBLostUpdateAnomaly verifies that transactions are not
// subject to the lost update anomaly. This anomaly is prevented
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
	defer log.Scope(t).Close(t)
	txn := "R(A) I(A) C"
	verify := &verifier{
		history: "R(A)",
		checkFn: func(env map[string]int64) error {
			if env["A"] != 2 {
				return errors.Errorf("expected A=2, got %d", env["A"])
			}
			return nil
		},
	}
	checkConcurrency("lost update", []string{txn, txn}, verify, t)
}

// TestTxnDBLostDeleteAnomaly verifies that transactions are not
// subject to the lost delete anomaly. See #6240.
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
	defer log.Scope(t).Close(t)

	// B must not exceed A.
	txn1 := "R(A) W(B,A) C"
	txn2 := "D(A) D(B) C"
	verify := &verifier{
		preHistory: "W(A,1)",
		history:    "R(A) R(B)",
		checkFn: func(env map[string]int64) error {
			if env["B"] != 0 && env["A"] == 0 {
				return errors.Errorf("expected B = %d <= %d = A", env["B"], env["A"])
			}
			return nil
		},
	}
	checkConcurrency("lost update (delete)", []string{txn1, txn2}, verify, t)
}

// TestTxnDBLostDeleteRangeAnomaly verifies that transactions are not
// subject to the lost delete range anomaly. See #6240.
//
// With lost delete range, the delete range for keys B-C leave no
// deletion tombstones (as there are an infinite number of keys in the
// range [B,C)). Without deletion tombstones, the anomaly manifests in
// snapshot mode when txn1 pushes txn2 to commit at a higher timestamp
// and then txn1 writes B and commits an an earlier timestamp. The
// delete range request therefore committed but failed to delete the
// value written to key B.
//
// Note that the snapshot isolation level is no longer supported. This
// test is retained for good measure.
//
// Lost delete range would typically fail with a history such as:
//   D2(A) DR2(B-C) R1(A) C2 W1(B,A) C1
func TestTxnDBLostDeleteRangeAnomaly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	// B must not exceed A.
	txn1 := "R(A) W(B,A) C"
	txn2 := "D(A) DR(B-C) C"
	verify := &verifier{
		preHistory: "W(A,1)",
		history:    "R(A) R(B)",
		checkFn: func(env map[string]int64) error {
			if env["B"] != 0 && env["A"] == 0 {
				return errors.Errorf("expected B = %d <= %d = A", env["B"], env["A"])
			}
			return nil
		},
	}
	checkConcurrency("lost update (range delete)", []string{txn1, txn2}, verify, t)
}

// TestTxnDBPhantomReadAnomaly verifies that transactions are not subject
// to the phantom reads anomaly. This anomaly is prevented by
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
	defer log.Scope(t).Close(t)
	txn1 := "SC(A-C) W(D,A+B) SC(A-C) W(E,A+B) C"
	txn2 := "R(B) I(B) C"
	verify := &verifier{
		history: "R(D) R(E)",
		checkFn: func(env map[string]int64) error {
			if env["D"] != env["E"] {
				return errors.Errorf("expected D == E (%d != %d)", env["D"], env["E"])
			}
			return nil
		},
	}
	checkConcurrency("phantom read", []string{txn1, txn2}, verify, t)
}

// TestTxnDBPhantomDeleteAnomaly verifies that transactions are not
// subject to the phantom deletion anomaly; this is
// similar to phantom reads, but verifies the delete range
// functionality causes read/write conflicts.
//
// Phantom deletes would typically fail with a history such as:
//   R2(B) DR1(A-C) I2(B) C2 SC1(A-C) W1(D,A+B) C1
func TestTxnDBPhantomDeleteAnomaly(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)
	txn1 := "DR(A-C) SC(A-C) W(D,A+B) C"
	txn2 := "R(B) I(B) C"
	verify := &verifier{
		history: "R(D)",
		checkFn: func(env map[string]int64) error {
			if env["D"] != 0 {
				return errors.Errorf("expected delete range to yield an empty scan of same range, sum=%d", env["D"])
			}
			return nil
		},
	}
	checkConcurrency("phantom delete", []string{txn1, txn2}, verify, t)
}

// TestTxnDBWriteSkewAnomaly verifies that transactions are not
// subject to the write skew anomaly. Write skew is only possible
// at the weaker, snapshot isolation level, which is no longer
// supported.
//
// With write skew, two transactions both read values from A and B
// respectively, but each writes to either A or B only. Thus there are
// no write/write conflicts but a cycle of dependencies which result in
// "skew".
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
	defer log.Scope(t).Close(t)
	txn1 := "SC(A-C) W(A,A+B+1) C"
	txn2 := "SC(A-C) W(B,A+B+1) C"
	verify := &verifier{
		history: "R(A) R(B)",
		checkFn: func(env map[string]int64) error {
			if !((env["A"] == 1 && env["B"] == 2) || (env["A"] == 2 && env["B"] == 1)) {
				return errors.Errorf("expected either A=1, B=2 -or- A=2, B=1, but have A=%d, B=%d", env["A"], env["B"])
			}
			return nil
		},
	}
	checkConcurrency("write skew", []string{txn1, txn2}, verify, t)
}
