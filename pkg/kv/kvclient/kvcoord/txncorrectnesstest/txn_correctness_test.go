// Copyright 2025 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package txncorrectnesstest

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
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/concurrency/isolation"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/kvserverbase"
	"github.com/cockroachdb/cockroach/pkg/storage/enginepb"
	"github.com/cockroachdb/cockroach/pkg/testutils/localtestcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/syncutil"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
)

// The following structs and methods provide a mechanism for verifying
// the correctness of Cockroach's transaction model. They do this by
// allowing transaction histories to be specified for concurrent txns
// and then expanding those histories to enumerate all possible
// priorities, isolation levels and interleavings of commands in the
// histories.

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
//   A - abort ("rollback")
//
// Notation for actual histories:
//   Rn.m(x) - read from txn "n" ("m"th retry) of key "x"
//   SCn.m(x-y) - scan from txn "n" ("m"th retry) of keys "x"-"y"
//   Dn.m(x) - delete key from txn ("m"th retry) of key "x"
//   DRn.m(x-y) - delete range from txn "n" ("m"th retry) of keys "x"-"y"
//   Wn.m(x,y+z+...) - write sum of values y+z+... to x from txn "n" ("m"th retry)
//   In.m(x) - increment from txn "n" ("m"th retry) of key "x"
//   Cn.m - commit of txn "n" ("m"th retry)
//   An.m - abort of txn "n" ("m"th retry)

type retryError struct {
	txnIdx, cmdIdx int
}

func (re *retryError) Error() string {
	return fmt.Sprintf("retry error at txn %d, cmd %d", re.txnIdx+1, re.cmdIdx)
}

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
	_, err := txn.Del(ctx, c.getKey())
	return err
}

// deleteRngCmd deletes the range of values from the db from [key, endKey).
func deleteRngCmd(ctx context.Context, c *cmd, txn *kv.Txn) error {
	_, err := txn.DelRange(ctx, c.getKey(), c.getEndKey(), false /* returnKeys */)
	return err
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

// abortCmd aborts the transaction.
func abortCmd(ctx context.Context, c *cmd, txn *kv.Txn) error {
	return txn.Rollback(ctx)
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
	{
		abortCmd,
		regexp.MustCompile(`(A)`),
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

// Easily accessible slices of transaction isolation variations.
var (
	allLevels               = []isolation.Level{isolation.Serializable, isolation.Snapshot, isolation.ReadCommitted}
	serializableAndSnapshot = []isolation.Level{isolation.Serializable, isolation.Snapshot}
	onlySerializable        = []isolation.Level{isolation.Serializable}
)

// enumerateIsolations returns a slice enumerating all combinations of
// isolation types across the transactions. The inner slice describes
// the isolation level for each transaction. The outer slice contains
// each possible combination of such transaction isolations.
func enumerateIsolations(numTxns int, isoLevels []isolation.Level) [][]isolation.Level {
	// Use a count from 0 to pow(# isolations, numTxns)-1 and examine
	// n-ary digits to get all possible combinations of txn isolations.
	n := len(isoLevels)
	var result [][]isolation.Level
	for i := 0; i < int(math.Pow(float64(n), float64(numTxns))); i++ {
		desc := make([]isolation.Level, numTxns)
		val := i
		for j := 0; j < numTxns; j++ {
			desc[j] = isoLevels[val%n]
			val /= n
		}
		result = append(result, desc)
	}
	return result
}

func TestEnumerateIsolations(t *testing.T) {
	defer leaktest.AfterTest(t)()
	SSI := isolation.Serializable
	SI := isolation.Snapshot
	RC := isolation.ReadCommitted

	expAll := [][]isolation.Level{
		{SSI, SSI, SSI},
		{SI, SSI, SSI},
		{RC, SSI, SSI},
		{SSI, SI, SSI},
		{SI, SI, SSI},
		{RC, SI, SSI},
		{SSI, RC, SSI},
		{SI, RC, SSI},
		{RC, RC, SSI},
		{SSI, SSI, SI},
		{SI, SSI, SI},
		{RC, SSI, SI},
		{SSI, SI, SI},
		{SI, SI, SI},
		{RC, SI, SI},
		{SSI, RC, SI},
		{SI, RC, SI},
		{RC, RC, SI},
		{SSI, SSI, RC},
		{SI, SSI, RC},
		{RC, SSI, RC},
		{SSI, SI, RC},
		{SI, SI, RC},
		{RC, SI, RC},
		{SSI, RC, RC},
		{SI, RC, RC},
		{RC, RC, RC},
	}
	require.Equal(t, expAll, enumerateIsolations(3, allLevels))

	expSSIAndSI := [][]isolation.Level{
		{SSI, SSI, SSI},
		{SI, SSI, SSI},
		{SSI, SI, SSI},
		{SI, SI, SSI},
		{SSI, SSI, SI},
		{SI, SSI, SI},
		{SSI, SI, SI},
		{SI, SI, SI},
	}
	require.Equal(t, expSSIAndSI, enumerateIsolations(3, serializableAndSnapshot))

	expSSI := [][]isolation.Level{
		{SSI, SSI, SSI},
	}
	require.Equal(t, expSSI, enumerateIsolations(3, onlySerializable))
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

func (hv *historyVerifier) run(isoLevels []isolation.Level, db *kv.DB, t *testing.T) {
	log.Infof(context.Background(), "verifying all possible histories for the %q anomaly", hv.name)
	enumIso := enumerateIsolations(len(hv.txns), isoLevels)
	enumPri := enumeratePriorities(len(hv.txns), []enginepb.TxnPriority{1, enginepb.MaxTxnPriority})
	enumHis := enumerateHistories(hv.txns, hv.equal)

	for _, i := range enumIso {
		for _, p := range enumPri {
			for _, h := range enumHis {
				hv.retriedTxns = map[int]struct{}{} // always reset the retried txns set
				if err := hv.runHistoryWithRetry(i, p, h, db, t); err != nil {
					t.Errorf("expected success, experienced %s", err)
					return
				}
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
	isoLevels []isolation.Level,
	priorities []enginepb.TxnPriority,
	cmds []*cmd,
	db *kv.DB,
	t *testing.T,
) error {
	if err := hv.runHistory(isoLevels, priorities, cmds, db, t); err != nil {
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
			if err := hv.runHistoryWithRetry(isoLevels, priorities, h, db, t); err != nil {
				return err
			}
		}
	}
	return nil
}

func (hv *historyVerifier) runHistory(
	isoLevels []isolation.Level,
	priorities []enginepb.TxnPriority,
	cmds []*cmd,
	db *kv.DB,
	t *testing.T,
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
		log.Infof(context.Background(), "iso=%s pri=%d history=%s", isoLevels, priorities, plannedStr)
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
			if err := hv.runTxn(i, isoLevels[i], priorities[i], txnCmds, db, t); err != nil {
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
			log.Infof(context.Background(), "PASSED: iso=%s pri=%d, history=%q", isoLevels, priorities, actualStr)
		}
	}
	if err != nil {
		t.Errorf("%d: iso=%s, pri=%d, history=%q: actual=%q, verify=%q: %s",
			hv.idx, isoLevels, priorities, plannedStr, actualStr, verifyStr, err)
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
	txnIdx int,
	isoLevel isolation.Level,
	priority enginepb.TxnPriority,
	cmds []*cmd,
	db *kv.DB,
	t *testing.T,
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
		if err := txn.SetIsoLevel(isoLevel); err != nil {
			return err
		}
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
func checkConcurrency(
	name string, isoLevels []isolation.Level, txns []string, verify *verifier, t *testing.T,
) {
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
	s.Start(t, kvcoord.InitFactoryForLocalTestCluster)
	defer s.Stop()
	// Reduce the deadlock detection push delay so that txns that encounter locks
	// begin deadlock detection immediately. This speeds up tests significantly.
	concurrency.LockTableDeadlockOrLivenessDetectionPushDelay.Override(context.Background(), &s.Cfg.Settings.SV, 0)
	verifier.run(isoLevels, s.DB, t)
}

func runWriteSkewTest(t *testing.T, iso isolation.Level) {
	checks := make(map[isolation.Level]func(map[string]int64) error)
	checks[isolation.Serializable] = func(env map[string]int64) error {
		if !((env["A"] == 1 && env["B"] == 2) || (env["A"] == 2 && env["B"] == 1)) {
			return errors.Errorf("expected either A=1, B=2 -or- A=2, B=1, but have A=%d, B=%d", env["A"], env["B"])
		}
		return nil
	}
	checks[isolation.Snapshot] = func(env map[string]int64) error {
		if env["A"] == 1 && env["B"] == 1 {
			return nil
		}
		return checks[isolation.Serializable](env)
	}

	txn1 := "SC(A-C) W(A,A+B+1) C"
	txn2 := "SC(A-C) W(B,A+B+1) C"
	verify := &verifier{
		history: "R(A) R(B)",
		checkFn: checks[iso],
	}
	checkConcurrency("write skew", []isolation.Level{iso}, []string{txn1, txn2}, verify, t)
}
