// Copyright 2023 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package kvserver_test

// TODO(sarkesian): Separate this test into its own package.

import (
	"context"
	"flag"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/kv"
	"github.com/cockroachdb/cockroach/pkg/kv/kvclient/kvcoord"
	"github.com/cockroachdb/cockroach/pkg/kv/kvpb"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/closedts"
	"github.com/cockroachdb/cockroach/pkg/kv/kvserver/txnwait"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/rpc/nodedialer"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/bootstrap"
	"github.com/cockroachdb/cockroach/pkg/storage"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/skip"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
)

const NilNode = roachpb.NodeID(0)

var (
	interceptTestVerbosityFlag = flag.Int(
		"intercept-test-verbosity", 2,
		"set intercept transaction test verbosity (0-3)",
	)
	interceptTestUseDiskFlag = flag.Bool(
		"intercept-test-use-disk", false,
		"when set to true, uses on-disk pebble storage. "+
			"Note that this slows the test considerably.",
	)
)

type InterceptingTransport struct {
	kvcoord.Transport
	intercept func(context.Context, *kvpb.BatchRequest) (*kvpb.BatchResponse, error)
}

func (t *InterceptingTransport) SendNext(
	ctx context.Context, ba *kvpb.BatchRequest,
) (*kvpb.BatchResponse, error) {
	return t.intercept(ctx, ba)
}

// TestInterceptedTransactions tests sequences of concurrent transactions
// interspersed with various operations being blocked, paused, or jammed on
// send or response. Inspired by autonomous simulation testing of the "bank"
// workload, these transactions work on kv-pairs of ints, similar to a SQL
// table with the following schema:
//
//	CREATE TABLE bank (id INT PRIMARY KEY, balance INT)
//
// By using integer kv-pairs, this allows us to circumvent key and value encodings
// in the data-driven test and simply use "account IDs" as keys, as well as simply
// print the output of gets and scans.
//
// Since these tests produce useful debugging output when verbosity is enabled,
// it is recommended to run the tests with the `-datadriven-quiet` flag.
//
// The input files use the following DSL:
// (Note that <duration> is a string of the form 1h45m30s)
//
// Running/Configuration commands:
//
//   - init num-nodes=<int> [intercept-on-nodes=<int>...]
//     Creates the test cluster with the given settings. By default enables
//     transport request intercepting on n1, but can be overridden.
//
//   - set-txn-expiration <duration>
//     Override the transaction expiration limit, by default 5s.
//
//   - set-closed-ts-override lag=<duration>
//     Set the closed timestamp lag duration, so as to control when transactions
//     are pushed, by default 3s.
//
// Transaction commands:
//
//   - new-txn name=<txn-name>
//     Creates a transaction with the given name.
//
//   - commit-txn name=<txn-name>
//     Synchronously attempts to commit the txn.
//     On success, prints "committed".
//
//   - abort-txn name=<txn-name>
//     Synchronously attempts to abort the txn.
//     On success, prints "aborted".
//
//   - exec-batch txn=<txn-name> [commit=<bool>]
//     load-balances accounts=<int>... [for-update=<bool>]
//     modify-balances accounts=<int>... delta=<int>
//     store-balances accounts=<int>...
//
// TODO(sarkesian): Get rid of modify-balances and switch to simple Gets/Puts
// rather than maintaining in-memory state, especially as the returned values
// are validated in the data-driven test anyway.
//
// TODO(sarkesian): Consider adding support for arbitrary request types.
//
//   - await op=<string> [timeout=<duration>]
//     Waits for results from the named operation and returns.
//     If a timeout of "min" is passed, waits 1ns before timing out.
//
// TODO(sarkesian): Have await (and synchronous commands) print a request log (using
// the context) so operations can be validated, possibly optionally.
//
//   - async op=<string> commit-txn name=<txn-name>
//     Asynchronously attempts to commit the txn, with results to be returned
//     later via `await op`.
//     Tests should not execute multiple async operations on the same txn.
//
//   - async op=<string> exec-batch txn=<txn-name>
//     Asynchronously attempts to execute a batch (see `exec-batch`), with
//     results to be returned later via `await op`.
//     Tests should not execute multiple async operations on the same txn.
//
// DB commands:
//
//   - dump-accounts
//     Scan and print all the "accounts" KVs.
//
//   - put account=<int> balance=<int>
//     Execute a Put(k=account, v=balance).
//
//   - get account=<int>
//     Execute a Get(k=account).
//
//   - delete account=<int>
//     Execute a Delete(k=account).
//
//   - scan start=<int> end=<int>
//     Scan all "accounts" KVs from start to end.
//
//   - range-desc account=<int>
//     Print RangeDescriptor of the range containing the account key
//     and output the span bounds and voter node IDs for validation.
//
//   - split [account=<int>]
//     Split off a range starting with the key for the account.
//     If no account is given, splits off at the start of the account
//     keyspace (testing table prefix).
//
//   - transfer-lease account=<int> node=<int>
//     Move the lease for the range containing the account key.
//
//   - check-lease account=<int>
//     Output the leaseholder nodeID for the range containing the account key.
//
// TODO(sarkesian): Consider adding support for "[add|remove]-voters" commands.
//
// Interception commands:
// (Note that the "method" filter only applies to single-request batches.)
//
//   - pause-req token=<string> [txn=<txn-name>] [src-node=<int>] [dst-node=<int>] [range=<int>] [method=<string>]
//     Pause all requests matching the provided filter criteria at the
//     transport layer. Paused requests are held before sending until the
//     named token is released, followed by the request and response being
//     sent and returned as normal.
//
//   - pause-resp token=<string> [txn=<txn-name>] [src-node=<int>] [dst-node=<int>] [range=<int>] [method=<string>]
//     Pause the responses to all requests matching the provided filter
//     criteria at the transport layer. Paused responses are held after receipt
//     until the named token is released, followed by the response being
//     returned as normal.
//
//   - jam-req token=<string> [txn=<txn-name>] [src-node=<int>] [dst-node=<int>] [range=<int>] [method=<string>] [batch-err=<bool>]
//     Jam all requests matching the provided filter criteria at the
//     transport layer. Jammed requests are held before sending until the
//     named token is released, followed by an error being returned.
//
//   - jam-resp token=<string> [txn=<txn-name>] [src-node=<int>] [dst-node=<int>] [range=<int>] [method=<string>] [batch-err=<bool>]
//     Jam the responses to all requests matching the provided filter
//     criteria at the transport layer. Jammed responses are held after receipt
//     until the named token is released, followed by an error being returned.
//
//   - block-req token=<string> [txn=<txn-name>] [src-node=<int>] [dst-node=<int>] [range=<int>] [method=<string>] [batch-err=<bool>]
//     Block all requests matching the provided filter criteria at the
//     transport layer. Blocked requests are not sent, and cause the transport
//     to immediately return an error.
//
//   - block-resp token=<string> [txn=<txn-name>] [src-node=<int>] [dst-node=<int>] [range=<int>] [method=<string>] [batch-err=<bool>]
//     Block the responses to all requests matching the provided filter
//     criteria at the transport layer. Blocked responses cause the transport to
//     return an error instead of the real received response.
//
//   - release token=<string>
//     Release the operations waiting on the interceptor using the named token,
//     and clean up the interceptor.
//
//   - release-all
//     Release all operations waiting on interceptors, and clean up all interceptors.
//
//   - chain release=<token> on=<token>
//     Release all operations waiting on a particular interceptor ("release")
//     only once another interceptor ("on") has received a matching request that
//     it has begun pausing/jamming/blocking.
//
// TODO(sarkesian): Consider removing chain if possible (and request validation is supported).
//
// Debug commands:
//
//   - debug [<string>...]
//     Print the debug message to the test log.
//
// TODO(sarkesian): Replace data-driven test "debug" commands with comments.
//
//   - echo [<string>...]
//     Simply echo the args.
//
//   - sleep <duration>
//     Sleep for some duration, e.g. 20ms.
func TestInterceptedTransactions(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	skip.UnderShort(t)
	skip.UnderRace(t)

	datadriven.Walk(t, datapathutils.TestDataPath(t, "intercepted_transactions"), func(t *testing.T, path string) {
		ctx, cancelContext := context.WithCancel(context.Background())
		defer cancelContext()
		tt := newInterceptedTransactionsTest(t)
		defer func() {
			if tt.tc != nil {
				tt.tc.Stopper().Stop(ctx)
			}
		}()

		datadriven.RunTest(t, path, func(t *testing.T, d *datadriven.TestData) string {
			switch d.Cmd {
			case "init":
				if tt.tc != nil {
					d.Fatalf(t, "it is invalid to re-init the cluster")
				}
				var nodeCount int
				var nodeIDsToIntercept []int
				d.ScanArgs(t, "num-nodes", &nodeCount)
				if ok := d.MaybeScanArgs(t, "intercept-on-nodes", &nodeIDsToIntercept); !ok {
					nodeIDsToIntercept = []int{1}
				}
				for _, nodeID := range nodeIDsToIntercept {
					tt.interceptedNodeIDs[roachpb.NodeID(nodeID)] = struct{}{}
				}
				tt.trace("initializing test cluster")
				tt.initCluster(nodeCount)
				return fmt.Sprintf("%s", nodeIDList(tt.tc.NodeIDs()))
			case "set-txn-expiration":
				if len(d.CmdArgs) == 0 {
					d.Fatalf(t, "missing argument")
				}
				txnExpiration, err := time.ParseDuration(d.CmdArgs[0].Key)
				if err != nil {
					d.Fatalf(t, "could not parse duration: %s", err)
				}
				tt.trace("setting txn liveness expiration to %s", time.Duration(txnwait.TxnLivenessThreshold.Load()))
				txnwait.TestingOverrideTxnLivenessThreshold(txnExpiration)
				return ""
			case "set-closed-ts-override":
				var lag string
				d.ScanArgs(t, "lag", &lag)
				lagDuration, err := time.ParseDuration(lag)
				if err != nil {
					d.Fatalf(t, "could not parse duration: %s", err)
				}
				tt.trace("setting closed timestamp lag to %s", closedts.TargetDuration.Get(&tt.st.SV))
				closedts.TargetDuration.Override(ctx, &tt.st.SV, lagDuration)
				return ""
			case "new-txn":
				var name string
				d.ScanArgs(t, "name", &name)
				tt.trace("creating txn \"%s\"", name)
				if ok := tt.newTxn(name); !ok {
					d.Fatalf(t, "txn named \"%s\" already existed", name)
				}
				return ""
			case "commit-txn":
				var name string
				d.ScanArgs(t, "name", &name)
				tt.trace("synchronously attempting to commit txn \"%s\"", name)
				return tt.commitTxn(name)
			case "abort-txn":
				var name string
				d.ScanArgs(t, "name", &name)
				tt.trace("synchronously attempting to abort txn \"%s\"", name)
				return tt.abortTxn(name)
			case "exec-batch":
				reqBatch := tt.parseBatch(d)
				tt.trace("synchronously attempting %s", reqBatch)
				return tt.execBatch(reqBatch)
			case "async":
				var opName string
				var ok bool
				d.ScanArgs(t, "op", &opName)
				if d.HasArg("commit-txn") {
					var name string
					d.ScanArgs(t, "name", &name)
					tt.trace("asynchronously attempting to commit txn \"%s\"", name)
					ok = tt.async(opName, func() string {
						return tt.commitTxn(name)
					})
				} else if d.HasArg("exec-batch") {
					reqBatch := tt.parseBatch(d)
					tt.trace("asynchronously attempting %s", reqBatch)
					ok = tt.async(opName, func() string {
						return tt.execBatch(reqBatch)
					})
				} else {
					d.Fatalf(t, "Unsupported async operation.")
				}
				if !ok {
					d.Fatalf(t, "duplicate async op named %s", opName)
				}
				return ""
			case "await":
				var opName string
				d.ScanArgs(t, "op", &opName)

				aCtx := ctx
				if d.HasArg("timeout") {
					var timeout time.Duration
					var cancel context.CancelFunc
					var timeStr string
					d.ScanArgs(t, "timeout", &timeStr)
					if timeStr == "min" {
						timeout = time.Nanosecond
					} else {
						var err error
						timeout, err = time.ParseDuration(timeStr)
						if err != nil {
							d.Fatalf(t, "could not parse duration: %s", err)
						}
					}

					aCtx, cancel = context.WithTimeout(ctx, timeout)
					defer cancel()
					tt.trace("awaiting op %s for %s", opName, timeout)
				} else {
					tt.trace("awaiting op %s", opName)
				}
				retVal, ok := tt.await(aCtx, opName)
				if !ok {
					d.Fatalf(t, "unknown async op %s", opName)
				}
				return retVal
			case "dump-accounts":
				accounts := accountSpan()
				tt.trace("scanning from %s to %s", accounts.Key, accounts.EndKey)
				scannedKVs, err := tt.DB().Scan(ctx, accounts.Key, accounts.EndKey, 0)
				require.NoError(t, err)
				return tt.kvsToString(scannedKVs, true)
			case "put":
				var accountID uint64
				var balance int64
				d.ScanArgs(t, "account", &accountID)
				d.ScanArgs(t, "balance", &balance)
				tt.trace("executing a Put(acct=%d, balance=%d)", accountID, balance)
				require.NoError(t, tt.DB().Put(ctx, accountKey(accountID), balance))
				return ""
			case "get":
				var accountID uint64
				d.ScanArgs(t, "account", &accountID)
				tt.trace("executing a Get(acct=%d)", accountID)
				keyVal, err := tt.DB().Get(ctx, accountKey(accountID))
				require.NoError(t, err)
				if keyVal.Value == nil {
					return ""
				}
				mvccValue, err := storage.DecodeMVCCValue(keyVal.Value.RawBytes)
				require.NoError(t, err)
				return fmt.Sprintf("key: %s, value: %s", keyVal.Key, mvccValue)
			case "delete":
				var accountID uint64
				d.ScanArgs(t, "account", &accountID)
				tt.trace("executing a Delete(acct=%d)", accountID)
				_, err := tt.DB().Del(ctx, accountKey(accountID))
				require.NoError(t, err)
				return ""
			case "scan":
				var accountIDStart, accountIDEnd uint64
				d.ScanArgs(t, "start", &accountIDStart)
				d.ScanArgs(t, "end", &accountIDEnd)
				tt.trace("executing a Scan(start=%d, end=%d)", accountIDStart, accountIDEnd)
				scannedKVs, err := tt.DB().Scan(ctx, accountKey(accountIDStart), accountKey(accountIDEnd), 0)
				require.NoError(t, err)
				return tt.kvsToString(scannedKVs, false)
			case "range-desc":
				var accountID uint64
				d.ScanArgs(t, "account", &accountID)
				tt.trace("looking up RangeDescriptor(acct=%d)", accountID)
				rangeDesc := tt.tc.LookupRangeOrFatal(t, accountKey(accountID))
				tt.log("range %s", rangeDesc)
				return validatableRangeDesc(&rangeDesc)
			case "split":
				var accountID uint64
				ok := d.MaybeScanArgs(t, "account", &accountID)
				splitKey := accountSpan().Key
				if ok {
					splitKey = accountKey(accountID)
				}
				tt.trace("splitting at %s", splitKey)
				tt.tc.SplitRangeOrFatal(t, splitKey)
				return ""
			case "transfer-lease":
				var accountID uint64
				var targetNodeID int
				d.ScanArgs(t, "account", &accountID)
				d.ScanArgs(t, "node-id", &targetNodeID)
				tt.trace("moving lease for range with acct=%d to n%d", accountID, targetNodeID)
				rangeDesc := tt.tc.LookupRangeOrFatal(t, accountKey(accountID))
				tt.tc.TransferRangeLeaseOrFatal(t, rangeDesc, tt.tc.Target(targetNodeID-1))
				tt.waitForLeaseholder(ctx, rangeDesc, roachpb.NodeID(targetNodeID))
				tt.log("r%d lease -> n%d", rangeDesc.RangeID, targetNodeID)
				return ""
			case "check-lease":
				var accountID uint64
				d.ScanArgs(t, "account", &accountID)
				tt.trace("checking lease for range with acct=%d", accountID)
				rangeDesc := tt.tc.LookupRangeOrFatal(t, accountKey(accountID))
				nodeID, err := tt.leaseholder(ctx, rangeDesc)
				require.NoError(t, err)
				return fmt.Sprintf("n%d", nodeID)
			case "pause-req":
				tok, filter := tt.parseInterceptArgs(d)
				tt.trace("adding interceptor \"%s-%s\" where %s", d.Cmd, tok, filter)
				err := tt.addInterceptor(d.Cmd, tok, filter, onRequest, returnResponse, false)
				if err != nil {
					d.Fatalf(t, "error intercepting: %v", err)
				}
				return ""
			case "pause-resp":
				tok, filter := tt.parseInterceptArgs(d)
				tt.trace("adding interceptor \"%s-%s\" where %s", d.Cmd, tok, filter)
				err := tt.addInterceptor(d.Cmd, tok, filter, onResponse, returnResponse, false)
				if err != nil {
					d.Fatalf(t, "error intercepting: %v", err)
				}
				return ""
			case "jam-req":
				tok, filter := tt.parseInterceptArgs(d)
				errAction := tt.parseInterceptErrType(d)
				tt.trace("adding interceptor \"%s-%s\" where %s", d.Cmd, tok, filter)
				err := tt.addInterceptor(d.Cmd, tok, filter, onRequest, errAction, false)
				if err != nil {
					d.Fatalf(t, "error intercepting: %v", err)
				}
				return ""
			case "jam-resp":
				tok, filter := tt.parseInterceptArgs(d)
				errAction := tt.parseInterceptErrType(d)
				tt.trace("adding interceptor \"%s-%s\" where %s", d.Cmd, tok, filter)
				err := tt.addInterceptor(d.Cmd, tok, filter, onResponse, errAction, false)
				if err != nil {
					d.Fatalf(t, "error intercepting: %v", err)
				}
				return ""
			case "block-req":
				tok, filter := tt.parseInterceptArgs(d)
				errAction := tt.parseInterceptErrType(d)
				tt.trace("adding interceptor \"%s-%s\" where %s", d.Cmd, tok, filter)
				err := tt.addInterceptor(d.Cmd, tok, filter, onRequest, errAction, true)
				if err != nil {
					d.Fatalf(t, "error intercepting: %v", err)
				}
				return ""
			case "block-resp":
				tok, filter := tt.parseInterceptArgs(d)
				errAction := tt.parseInterceptErrType(d)
				tt.trace("adding interceptor \"%s-%s\" where %s", d.Cmd, tok, filter)
				err := tt.addInterceptor(d.Cmd, tok, filter, onResponse, errAction, true)
				if err != nil {
					d.Fatalf(t, "error intercepting: %v", err)
				}
				return ""
			case "release":
				var tok InterceptToken
				d.ScanArgs(t, "token", (*string)(&tok))
				tt.trace("releasing interceptor %s", tok)
				if !tt.release(tok) {
					d.Fatalf(t, "could not find token %s to release", tok)
				}
				return ""
			case "release-all":
				tt.trace("releasing all interceptors")
				tt.releaseAll()
				return ""
			case "chain":
				var toRelease, toAwait InterceptToken
				d.ScanArgs(t, "release", (*string)(&toRelease))
				d.ScanArgs(t, "on", (*string)(&toAwait))
				tt.trace("chaining interceptor %s to be released once requests are waiting on interceptor %s",
					toRelease, toAwait)
				err := tt.chainRelease(ctx, toRelease, toAwait)
				if err != nil {
					d.Fatalf(t, "error chaining release: %v", err)
				}
				return ""
			case "debug":
				msg := unquote(argsAsString(d))
				tt.dbg(msg)
				return ""
			case "echo":
				msg := argsAsString(d)
				tt.trace("echoing \"%s\"", msg)
				return msg
			case "sleep":
				if len(d.CmdArgs) == 0 {
					d.Fatalf(t, "missing argument")
				}
				sleepDuration, err := time.ParseDuration(d.CmdArgs[0].Key)
				if err != nil {
					d.Fatalf(t, "could not parse duration: %s", err)
				}
				tt.trace("sleeping for %s", sleepDuration)
				time.Sleep(sleepDuration)
				tt.dbg("Slept for %s", sleepDuration)
				return ""
			default:
				return fmt.Sprintf("unknown command: %s", d.Cmd)
			}
		})
		tt.dbg("Completed all test commands.")
	})
}

type accountBalanceReqType int

const (
	loadBalances accountBalanceReqType = iota
	modifyBalances
	storeBalances
)

// String implements the Stringer interface.
func (rt accountBalanceReqType) String() string {
	switch rt {
	case loadBalances:
		return "load"
	case modifyBalances:
		return "modify"
	case storeBalances:
		return "store"
	default:
		panic("unknown type")
	}
}

// Intermediate representation translating to a single KV request per account.
type accountBalanceReq struct {
	reqType   accountBalanceReqType
	accounts  []uint64
	forUpdate bool
	delta     int64
}

// String implements the Stringer interface.
func (abr accountBalanceReq) String() string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s(accts=[", abr.reqType)
	for i, account := range abr.accounts {
		if i > 0 {
			fmt.Fprintf(&sb, ",")
		}
		fmt.Fprintf(&sb, "%d", account)
	}
	fmt.Fprintf(&sb, "]")
	if abr.forUpdate {
		fmt.Fprintf(&sb, ", ForUpdate")
	}
	if abr.delta > 0 {
		fmt.Fprintf(&sb, ", +%d", abr.delta)
	} else if abr.delta < 0 {
		fmt.Fprintf(&sb, ", %d", abr.delta)
	}

	fmt.Fprintf(&sb, ")")
	return sb.String()
}

// Intermediate representation of batch operations.
type accountBalanceBatch struct {
	txnName       string
	reqs          []accountBalanceReq
	commitInBatch bool
}

// String implements the Stringer interface.
func (abb accountBalanceBatch) String() string {
	// TODO(sarkesian): Consider improving this logging to be more standard.
	var sb strings.Builder
	fmt.Fprintf(&sb, "%s.batch{", abb.txnName)
	for i, req := range abb.reqs {
		if i > 0 {
			fmt.Fprintf(&sb, ",")
		}
		fmt.Fprintf(&sb, "%s", req)
	}
	fmt.Fprintf(&sb, "}")
	if abb.commitInBatch {
		fmt.Fprintf(&sb, ".CommitInBatch()")
	}

	return sb.String()
}

type nodeIDList []roachpb.NodeID
type nodeIDSet map[roachpb.NodeID]struct{}

// String implements the Stringer interface.
func (n nodeIDList) String() string {
	var sb strings.Builder
	for i, nodeID := range n {
		if i > 0 {
			fmt.Fprintf(&sb, ", ")
		}
		fmt.Fprintf(&sb, "n%d", nodeID)
	}
	return sb.String()
}

// String implements the Stringer interface.
func (n nodeIDSet) String() string {
	var sb strings.Builder
	for nodeID := range n {
		if sb.Len() > 0 {
			fmt.Fprintf(&sb, ", ")
		}
		fmt.Fprintf(&sb, "n%d", nodeID)
	}
	return sb.String()
}

// -- Data-driven test helper functions --

func unquote(s string) string {
	return strings.TrimFunc(s, func(r rune) bool {
		return r == '"'
	})
}

func argsAsString(d *datadriven.TestData) string {
	var sb strings.Builder
	for i, arg := range d.CmdArgs {
		if i > 0 {
			fmt.Fprintf(&sb, " ")
		}
		fmt.Fprintf(&sb, "%s", arg)
	}

	return sb.String()
}

var txnRE = regexp.MustCompile(`\[txn: [0-9a-f]+\]`)
var metaRE = regexp.MustCompile(`meta=\{.*\}`)
var tsRE = regexp.MustCompile(`(rts|gul)=[0-9]+\.[0-9]+,[0-9]+`)

// validatableCommitErr converts a txn commit error to a format that can be
// validated in the data-driven test using redaction markers.
func validatableCommitErr(err error) string {
	var errStr string
	errStr = err.Error()
	errStr = txnRE.ReplaceAllString(errStr, "[txn: ‹×›]")
	errStr = metaRE.ReplaceAllString(errStr, "meta=‹×›")
	errStr = tsRE.ReplaceAllString(errStr, "$1=‹×›")
	return errStr
}

// validatableRangeDesc converts a RangeDescriptor to a format that can be
// validated in the data-driven test (as rangeIDs and replicaIDs may not be static).
func validatableRangeDesc(desc *roachpb.RangeDescriptor) string {
	var sb strings.Builder
	fmt.Fprintf(&sb, "span: %s\n", desc.KeySpan())
	var voters, nonVoters roachpb.NodeIDSlice
	for _, replicaDesc := range desc.Replicas().VoterDescriptors() {
		voters = append(voters, replicaDesc.NodeID)
	}
	for _, replicaDesc := range desc.Replicas().NonVoterDescriptors() {
		nonVoters = append(nonVoters, replicaDesc.NodeID)
	}
	sort.Sort(voters)
	sort.Sort(nonVoters)
	fmt.Fprintf(&sb, "voters: %s\n", nodeIDList(voters))
	if len(nonVoters) > 0 {
		fmt.Fprintf(&sb, "non-voters: %s\n", nodeIDList(nonVoters))
	}

	return sb.String()
}

// -- Database helper functions --

func svrToNodeID(svrIdx int) roachpb.NodeID {
	return roachpb.NodeID(svrIdx + 1)
}

// accountSpan returns the span of the keys written and read by the test.
func accountSpan() roachpb.Span {
	tablePrefix := bootstrap.TestingUserTableDataMin()
	return roachpb.Span{
		Key:    tablePrefix,
		EndKey: tablePrefix.PrefixEnd(),
	}
}

// accountKey returns the encoded key for an "account" with a given ID.
func accountKey(accountID uint64) roachpb.Key {
	tablePrefix := bootstrap.TestingUserTableDataMin()
	return encoding.EncodeUvarintAscending(tablePrefix, accountID)
}

type InterceptToken string

// Representation of an in-progress transaction used by the test.
type monitoredTxn struct {
	ctx context.Context
	txn *kv.Txn

	// Finalized is used for test management, not for actual transaction state.
	// While a txn that errors on an attempt commit is not technically finalized,
	// for the purposes of the test this txn should accept no more requests.
	finalized bool

	// Any loaded "account-balance" KV pairs.
	inMemState map[uint64]int64
}

// Encapsulates the test state and management, including any configuration,
// interceptors, and the running cluster.
type interceptedTransactionsTest struct {
	*testing.T

	st *cluster.Settings
	tc *testcluster.TestCluster

	verbosity          int
	interceptedNodeIDs nodeIDSet

	// The set of in-flight asynchronous commands in the data-driven test.
	// Stores the mapping of a named async op to the channel on which the result
	// can be received.
	inFlightOps map[string]chan string

	// The set of in-progress transactions that the test cares about.
	// Used by the test manager for running the transactions, but also by the
	// interceptors to check what txns to log operations for.
	observedTxns sync.Map // map[string]monitoredTxn

	// The set of currently-in-use interceptors.
	// TODO(sarkesian): Consider using a map with a syncutil.Mutex instead of a sync.Map.
	transportReqInterceptors  sync.Map // map[InterceptToken]requestIntercept
	transportRespInterceptors sync.Map // map[InterceptToken]requestIntercept
}

func newInterceptedTransactionsTest(t *testing.T) *interceptedTransactionsTest {
	v := 0
	if testing.Verbose() {
		v = *interceptTestVerbosityFlag
	}
	return &interceptedTransactionsTest{
		T:                  t,
		st:                 cluster.MakeTestingClusterSettings(),
		interceptedNodeIDs: make(map[roachpb.NodeID]struct{}),
		verbosity:          v,
		inFlightOps:        make(map[string]chan string),
	}
}

func (tt *interceptedTransactionsTest) trace(format string, args ...any) {
	tt.Helper()
	if tt.verbosity >= 3 {
		tt.Logf("[dbg] "+format, args...)
	}
}

func (tt *interceptedTransactionsTest) dbg(format string, args ...any) {
	tt.Helper()
	if tt.verbosity >= 2 {
		tt.Logf("[dbg] "+format, args...)
	}
}

func (tt *interceptedTransactionsTest) log(format string, args ...any) {
	tt.Helper()
	if tt.verbosity >= 1 {
		tt.Logf(format, args...)
	}
}

// storeSpec defines a single store, by default in memory though can use disk
// if the test is run with the "-intercept-test-use-disk" flag.
func (tt *interceptedTransactionsTest) storeSpec(serverIdx int) base.StoreSpec {
	storeSpec := base.DefaultTestStoreSpec
	if *interceptTestUseDiskFlag {
		storeSpec.InMemory = false
		storeSpec.Path = tt.TempDir()
		tt.log("n%d using store: %s", svrToNodeID(serverIdx), storeSpec.Path)
	}
	return storeSpec
}

// initCluster creates a new TestCluster for use in the test.
func (tt *interceptedTransactionsTest) initCluster(numNodes int) {
	serverArgs := make(map[int]base.TestServerArgs)
	for idx := 0; idx < numNodes; idx++ {
		args := base.TestServerArgs{
			Settings:   tt.st,
			Insecure:   true,
			StoreSpecs: []base.StoreSpec{tt.storeSpec(idx)},
		}
		if _, ok := tt.interceptedNodeIDs[svrToNodeID(idx)]; ok {
			args.Knobs.KVClient = &kvcoord.ClientTestingKnobs{
				DisableCommitSanityCheck: true,
				TransportFactory:         tt.createInterceptingTransportFactory(svrToNodeID(idx)),
			}
		}

		serverArgs[idx] = args
	}
	startTime := time.Now()
	tc := testcluster.StartTestCluster(tt, numNodes, base.TestClusterArgs{
		ServerArgsPerNode: serverArgs,
	})
	tt.dbg("Cluster startup took %s", time.Since(startTime))

	// Validate that nodeID==svrIdx+1
	for idx, svr := range tc.Servers {
		require.Equalf(tt, svrToNodeID(idx), svr.NodeID(), "programming error: nodeID mismatch")
	}

	require.NoError(tt, tc.WaitForFullReplication())

	tt.tc = tc
}

func (tt *interceptedTransactionsTest) DB() *kv.DB {
	return tt.tc.Server(0).DB()
}

// kvsToString turns the scan results into a string, and also logs them.
func (tt *interceptedTransactionsTest) kvsToString(scanResults []kv.KeyValue, log bool) string {
	tt.Helper()
	var sb strings.Builder
	for _, kv := range scanResults {
		mvccValue, err := storage.DecodeMVCCValue(kv.Value.RawBytes)
		require.NoError(tt, err)
		line := fmt.Sprintf("key: %s, value: %s", kv.Key, mvccValue)
		if log {
			tt.log(line)
		}

		fmt.Fprintf(&sb, "%s\n", line)
	}
	return sb.String()
}

// parseInterceptArgs is used to parse the arguments for all the pause, jam,
// and block data-driven test commands.
func (tt *interceptedTransactionsTest) parseInterceptArgs(d *datadriven.TestData) (InterceptToken, requestInterceptFilter) {
	var tok string
	var filter requestInterceptFilter
	var srcNodeID, dstNodeID, rangeID int

	d.ScanArgs(tt, "token", &tok)
	d.MaybeScanArgs(tt, "txn", &filter.txnName)
	if d.MaybeScanArgs(tt, "src-node", &srcNodeID) {
		filter.src = roachpb.NodeID(srcNodeID)
	}
	if d.MaybeScanArgs(tt, "dst-node", &dstNodeID) {
		filter.dst = roachpb.NodeID(dstNodeID)
	}
	if d.MaybeScanArgs(tt, "range", &rangeID) {
		filter.rangeID = roachpb.RangeID(rangeID)
	}
	d.MaybeScanArgs(tt, "method", &filter.method)

	for _, interceptorMap := range tt.interceptorMaps() {
		if _, ok := interceptorMap.Load(tok); ok {
			d.Fatalf(tt, "duplicate intercept with token %s", tok)
		}
	}

	return InterceptToken(tok), filter
}

// parseInterceptErrType returns the type of error for an interceptor to return;
// by default, returns a gRPC error, but if "batch-err=true" is passed, returns
// a valid batch response with the error field set.
func (tt *interceptedTransactionsTest) parseInterceptErrType(d *datadriven.TestData) postInterceptAction {
	action := returnError
	var batchErr bool
	if ok := d.MaybeScanArgs(tt, "batch-err", &batchErr); ok && batchErr {
		action = returnBatchWithError
	}

	return action
}

// parseBatch is a helper for parsing the command syntax into an intermediate
// representation that can be executed synchronously or asynchronously.
//   - exec-batch txn=<txn-name> [commit=<bool>]
//     load-balances accounts=<int>... [for-update=<bool>]
//     modify-balances accounts=<int>... delta=<int>
//     store-balances accounts=<int>...
func (tt *interceptedTransactionsTest) parseBatch(d *datadriven.TestData) accountBalanceBatch {
	var txnName string
	d.ScanArgs(tt, "txn", &txnName)
	commitInBatch := d.HasArg("commit")

	val, ok := tt.observedTxns.Load(txnName)
	if !ok {
		d.Fatalf(tt, "unknown txn \"%s\"", txnName)
	}

	txnState := val.(monitoredTxn)
	if txnState.finalized {
		d.Fatalf(tt, "cannot execute a batch on a finalized txn")
	}

	var reqs []accountBalanceReq
	reqLines := strings.Split(d.Input, "\n")
	for _, line := range reqLines {
		var err error
		d.Cmd, d.CmdArgs, err = datadriven.ParseLine(line)
		if err != nil {
			d.Fatalf(tt, "error parsing single request: %v", err)
		}

		req := accountBalanceReq{}
		d.ScanArgs(tt, "accounts", &req.accounts)
		switch d.Cmd {
		case "load-balances":
			req.reqType = loadBalances
			d.MaybeScanArgs(tt, "for-update", &req.forUpdate)
		case "modify-balances":
			req.reqType = modifyBalances
			d.ScanArgs(tt, "delta", &req.delta)
		case "store-balances":
			req.reqType = storeBalances
		default:
			d.Fatalf(tt, "unknown batch command %s", d.Cmd)
		}

		reqs = append(reqs, req)
	}

	return accountBalanceBatch{
		txnName:       txnName,
		reqs:          reqs,
		commitInBatch: commitInBatch,
	}
}

// newTxn initialized a new txn wrapped by a monitor for use in the test,
// returning false if this cannot be done because the name is already in use.
func (tt *interceptedTransactionsTest) newTxn(name string) (ok bool) {
	tCtx := context.Background()
	txn := tt.DB().NewTxn(tCtx, name)
	txnState := monitoredTxn{
		ctx:        tCtx,
		txn:        txn,
		inMemState: make(map[uint64]int64),
	}
	_, loaded := tt.observedTxns.LoadOrStore(name, txnState)
	return !loaded
}

// commitTxn synchronously attempts to commits a transaction and returns the
// error as a string for data-driven test output.
func (tt *interceptedTransactionsTest) commitTxn(name string) string {
	val, ok := tt.observedTxns.Load(name)
	if !ok {
		return fmt.Sprintf("txn \"%s\" not found", name)
	}
	txnState := val.(monitoredTxn)
	defer func() {
		txnState.finalized = true
		tt.observedTxns.Store(name, txnState)
	}()
	err := txnState.txn.Commit(txnState.ctx)
	if err != nil {
		return err.Error()
	}
	return "committed"
}

// abortTxn synchronously attempts to abort a transaction and returns the
// error as a string for data-driven test output.
func (tt *interceptedTransactionsTest) abortTxn(name string) string {
	val, ok := tt.observedTxns.Load(name)
	if !ok {
		return fmt.Sprintf("txn \"%s\" not found", name)
	}
	txnState := val.(monitoredTxn)
	defer func() {
		txnState.finalized = true
		tt.observedTxns.Store(name, txnState)
	}()
	err := txnState.txn.Rollback(txnState.ctx)
	if err != nil {
		return err.Error()
	}
	return "aborted"
}

// execBatch synchronously attempts to execute a KV batch on a transaction
// based on the intermediate representation parsed from the data-driven test,
// returning any error message or the batch results as a string as data-driven
// test output.
func (tt *interceptedTransactionsTest) execBatch(accountBatch accountBalanceBatch) string {
	val, ok := tt.observedTxns.Load(accountBatch.txnName)
	if !ok {
		// This should not occur as we checked the txn name when parsing.
		return fmt.Sprintf("unknown txn %s in batch", accountBatch.txnName)
	}

	txnState := val.(monitoredTxn)
	defer func() {
		if accountBatch.commitInBatch {
			txnState.finalized = true
		}
		tt.observedTxns.Store(accountBatch.txnName, txnState)
	}()

	// Keep track of the reverse mapping of pretty-printed
	// keys to accounts for ease of key->account conversion.
	accountsByUsedKeys := make(map[string]uint64)

	batch := txnState.txn.NewBatch()
	for _, req := range accountBatch.reqs {
		switch req.reqType {
		case loadBalances:
			for _, account := range req.accounts {
				key := accountKey(account)
				accountsByUsedKeys[key.String()] = account
				if req.forUpdate {
					batch.GetForUpdate(key)
				} else {
					batch.Get(key)
				}
			}
		case modifyBalances:
			for _, account := range req.accounts {
				txnState.inMemState[account] += req.delta
			}
		case storeBalances:
			for _, account := range req.accounts {
				key := accountKey(account)
				accountsByUsedKeys[key.String()] = account
				batch.Put(key, txnState.inMemState[account])
			}
		}
	}

	if accountBatch.commitInBatch {
		err := txnState.txn.CommitInBatch(txnState.ctx, batch)
		if err != nil {
			return validatableCommitErr(err)
		}
		return "committed"
	}

	err := txnState.txn.Run(txnState.ctx, batch)
	if err != nil {
		return err.Error()
	}

	var sb strings.Builder
	printed := false
	for _, result := range batch.Results {
		for _, row := range result.Rows {
			if row.Value == nil {
				continue
			}
			if printed {
				fmt.Fprintf(&sb, "\n")
			}
			account, ok := accountsByUsedKeys[row.Key.String()]
			if !ok {
				return fmt.Sprintf("unknown key %s returned in batch results", row.Key)
			}
			val := row.ValueInt()
			txnState.inMemState[account] = val

			mvccValue, err := storage.DecodeMVCCValue(row.Value.RawBytes)
			if err != nil {
				return err.Error()
			}
			fmt.Fprintf(&sb, "key: %s, value: %s", row.Key, mvccValue)
			printed = true
		}
	}

	return sb.String()
}

// async initiates an operation on a Goroutine and stores the channel on which
// the result will be received in the manager's list of in-flight operations.
// Returns false only if the op name duplicates another in-flight operation.
func (tt *interceptedTransactionsTest) async(opName string, op func() string) (ok bool) {
	_, found := tt.inFlightOps[opName]
	if found {
		return false
	}

	startedChan := make(chan struct{})
	doneChan := make(chan string, 1)
	tt.inFlightOps[opName] = doneChan
	go func() {
		close(startedChan)
		retVal := op()
		tt.trace("completed async op %s", opName)
		doneChan <- retVal
		close(doneChan)
	}()

	<-startedChan
	return true
}

// await waits on the result of an op, ending early and logging/returning an
// error message if stopped early. If successfully gets a value, the in-flight
// op will be cleaned up. Ok == true indicates the op was found.
func (tt *interceptedTransactionsTest) await(ctx context.Context, opName string) (result string, ok bool) {
	doneChan, found := tt.inFlightOps[opName]
	if !found {
		return "not found", false
	}

	startTime := time.Now()
	select {
	case <-ctx.Done():
		msg := fmt.Sprintf("%s: context canceled while awaiting", opName)
		tt.log(msg)
		return msg, true
	case <-tt.tc.Stopper().ShouldQuiesce():
		msg := fmt.Sprintf("%s: quiescing while awaiting", opName)
		tt.log(msg)
		return msg, true
	case retVal := <-doneChan:
		duration := time.Since(startTime)
		tt.trace("%s: waited %s on op", opName, duration)
		return retVal, true
	}
}

// leaseholder looks up the current lease for a given range descriptor.
func (tt *interceptedTransactionsTest) leaseholder(ctx context.Context, desc roachpb.RangeDescriptor) (roachpb.NodeID, error) {
	li, _, err := tt.tc.FindRangeLeaseEx(ctx, desc, nil)
	if err != nil {
		return NilNode, errors.Wrapf(err, "could not find lease for %s", desc)
	}
	curLease := li.Current()
	if curLease.Empty() {
		return NilNode, errors.Errorf("could not find lease for %s", desc)
	}
	return curLease.Replica.NodeID, nil
}

// waitForLeaseholder waits until the current leaseholder for a range
// descriptor matches the expected nodeID.
func (tt *interceptedTransactionsTest) waitForLeaseholder(ctx context.Context, desc roachpb.RangeDescriptor, expectedLeaseholder roachpb.NodeID) {
	testutils.SucceedsSoon(tt, func() error {
		lh, err := tt.leaseholder(ctx, desc)
		if err != nil {
			return err
		}

		if lh != expectedLeaseholder {
			return errors.Errorf("expected n%d to own the lease for %s\n"+
				"actual leaseholder: n%d",
				expectedLeaseholder, desc, lh)
		}
		return nil
	})
}

func (tt *interceptedTransactionsTest) interceptorMaps() []*sync.Map {
	return []*sync.Map{
		&tt.transportReqInterceptors,
		&tt.transportRespInterceptors,
	}
}

// release signals to all waiting requests/responses caught by the interceptor
// specified by the token to proceed unblocked, also cleaning up the interceptor.
func (tt *interceptedTransactionsTest) release(token InterceptToken) bool {
	released := false
	for _, interceptorMap := range tt.interceptorMaps() {
		if val, ok := interceptorMap.LoadAndDelete(token); ok {
			intercept := val.(requestIntercept)
			intercept.signalRelease()
			released = true
		}
	}

	return released
}

// release signals all waiting requests/responses caught by interceptors to
// proceed unblocked, also cleaning up all interceptors.
func (tt *interceptedTransactionsTest) releaseAll() {
	existingInterceptors := tt.interceptorMaps()
	for _, interceptorMap := range existingInterceptors {
		interceptorMap.Range(func(key, val any) bool {
			tok := key.(InterceptToken)
			intercept := val.(requestIntercept)
			interceptorMap.Delete(tok)
			intercept.signalRelease()
			return true
		})
	}
}

func (tt *interceptedTransactionsTest) findInterceptorByToken(tok InterceptToken) (interceptor *requestIntercept, srcMap *sync.Map) {
	for _, interceptorMap := range tt.interceptorMaps() {
		if val, ok := interceptorMap.Load(tok); ok {
			typedVal := val.(requestIntercept)
			interceptor = &typedVal
			srcMap = interceptorMap
			break
		}
	}
	return interceptor, srcMap
}

// chainRelease configures the release of an interceptor (referred to by
// releaseTok) to occur only once requests have been intercepted by another
// interceptor (referred to by awaitTok). These two interceptors must be unique,
// and while they can be of any type (transport request/response, store request),
// only the first interceptor found for each is used. As interceptor tokens
// should be unique, there should only ever be one of each.
func (tt *interceptedTransactionsTest) chainRelease(ctx context.Context, releaseTok, awaitTok InterceptToken) error {
	if releaseTok == awaitTok {
		return errors.Errorf("cannot chain interceptor to itself")
	}

	interceptorToRelease, toReleaseSrcMap := tt.findInterceptorByToken(releaseTok)
	if interceptorToRelease == nil {
		return errors.Errorf("could not find interceptor %s", releaseTok)
	}

	interceptorToAwait, _ := tt.findInterceptorByToken(awaitTok)
	if interceptorToAwait == nil {
		return errors.Errorf("could not find interceptor %s", awaitTok)
	}

	startedChan := make(chan struct{})
	go func() {
		close(startedChan)
		select {
		case <-interceptorToAwait.interceptedChan:
			tt.trace("releasing interceptor %s upon catching requests via interceptor %s",
				releaseTok, awaitTok)
			interceptorToRelease.signalRelease()
			toReleaseSrcMap.Delete(releaseTok)
		case <-ctx.Done():
			tt.log("chain %s <- %s: context canceled while awaiting", releaseTok, awaitTok)
		case <-tt.tc.Stopper().ShouldQuiesce():
			tt.log("chain %s <- %s: quiescing while awaiting", releaseTok, awaitTok)
		}
	}()

	<-startedChan
	return nil
}

// addInterceptor creates an interceptor, referred to by the specified token, and
// applies it in the manner specified. This provides support for all pause, jam,
// and block commands for both the transport and store interceptors.
func (tt *interceptedTransactionsTest) addInterceptor(
	logTag string,
	token InterceptToken,
	filter requestInterceptFilter,
	reqOrResp interceptPoint,
	respOrErr postInterceptAction,
	closeImmediately bool,
) error {
	jam := make(chan struct{})
	var jamDoneOnce sync.Once
	intercepted := make(chan struct{})
	var interceptedDoneOnce sync.Once
	if closeImmediately {
		jamDoneOnce.Do(func() {
			close(jam)
		})
	}

	var targetCollection *sync.Map
	if reqOrResp == onRequest {
		targetCollection = &tt.transportReqInterceptors
	} else if reqOrResp == onResponse {
		targetCollection = &tt.transportRespInterceptors
	} else {
		return errors.Errorf("unknown interceptor target")
	}

	intercept := requestIntercept{
		logTag:              logTag,
		filter:              filter,
		jamChan:             jam,
		jamDoneOnce:         &jamDoneOnce,
		interceptedChan:     intercepted,
		interceptedDoneOnce: &interceptedDoneOnce,
		onRelease:           respOrErr,
	}

	targetCollection.Store(token, intercept)
	return nil
}

// signalRelease is a helper function for releasing the intercept channel.
func (ri *requestIntercept) signalRelease() {
	ri.jamDoneOnce.Do(func() {
		close(ri.jamChan)
	})
}

// signalIntercepted is a helper function for releasing the "intercepted"
// channel, indicating that the interceptor has intercepted a request.
func (ri *requestIntercept) signalIntercepted() {
	ri.interceptedDoneOnce.Do(func() {
		close(ri.interceptedChan)
	})
}

func (tt *interceptedTransactionsTest) isObservedTxn(ba *kvpb.BatchRequest) bool {
	if ba.Txn == nil || ba.Txn.Name == "" {
		return false
	}

	_, found := tt.observedTxns.Load(ba.Txn.Name)
	return found
}

// Utility for logging observed and intercepted operations.
type interceptTagBuilder struct {
	strings.Builder
}

func (tb *interceptTagBuilder) addTxnTag(ba *kvpb.BatchRequest) {
	if ba.Txn != nil && ba.Txn.Name != "" {
		fmt.Fprintf(tb, "(%s) ", ba.Txn.Name)
	}
}

func (tb *interceptTagBuilder) addInterceptTag(tok InterceptToken, interceptor *requestIntercept) {
	if interceptor != nil {
		fmt.Fprintf(tb, "[%s-%s] ", interceptor.logTag, tok)
	}
}

func (tb *interceptTagBuilder) Build(
	reqTok InterceptToken, reqI *requestIntercept,
	respTok InterceptToken, respI *requestIntercept,
	ba *kvpb.BatchRequest, srcNodeID roachpb.NodeID, target roachpb.ReplicaDescriptor) {
	tb.addTxnTag(ba)
	tb.addInterceptTag(reqTok, reqI)
	tb.addInterceptTag(respTok, respI)
	fmt.Fprintf(tb, "n%d->n%d:r%d/%d", srcNodeID, target.NodeID, ba.RangeID, target.ReplicaID)
}

// createInterceptingTransportFactory creates a transport factory that returns
// the custom intercepting transport controlled by the data-driven test.
func (tt *interceptedTransactionsTest) createInterceptingTransportFactory(srcNodeID roachpb.NodeID) kvcoord.TransportFactory {
	tt.trace("enabled interception on transport on n%d", srcNodeID)
	return func(options kvcoord.SendOptions, dialer *nodedialer.Dialer, slice kvcoord.ReplicaSlice) (kvcoord.Transport, error) {
		transport, err := kvcoord.GRPCTransportFactory(options, dialer, slice)
		interceptor := &InterceptingTransport{
			Transport: transport,
			intercept: func(ctx context.Context, ba *kvpb.BatchRequest) (*kvpb.BatchResponse, error) {
				reqITok, reqI := getFirstMatchingInterceptor(&tt.transportReqInterceptors, ba, srcNodeID, transport.NextReplica())
				respITok, respI := getFirstMatchingInterceptor(&tt.transportRespInterceptors, ba, srcNodeID, transport.NextReplica())

				isObserved := tt.isObservedTxn(ba) || reqI != nil || respI != nil

				var tb interceptTagBuilder
				tb.Build(reqITok, reqI, respITok, respI, ba, srcNodeID, transport.NextReplica())

				if isObserved {
					var txnSuffix string
					if ba.Txn != nil {
						txnSuffix = fmt.Sprintf("meta={key=%s ts=%s seq=%d, epoch=%d}",
							roachpb.Key(ba.Txn.TxnMeta.Key), ba.Txn.TxnMeta.WriteTimestamp, ba.Txn.TxnMeta.Sequence,
							ba.Txn.TxnMeta.Epoch)
					}

					tt.log("%s batchReq={%s} %s", tb.String(), ba, txnSuffix)
				}

				if reqI != nil {
					reqI.signalIntercepted()
					select {
					case <-tt.tc.Stopper().ShouldQuiesce():
						tt.log("quiescing while held on request intercept %s", reqITok)
					case <-reqI.jamChan:
						tt.log("%s request released", tb.String())
					}
					if reqI.onRelease == returnBatchWithError {
						br := &kvpb.BatchResponse{}
						br.Error = kvpb.NewErrorf("request intercepted on n%d->n%d", srcNodeID, transport.NextReplica().NodeID)
						return br, nil
					}
					if reqI.onRelease == returnError {
						return nil, grpcstatus.Errorf(codes.Unavailable, "request intercepted on n%d->n%d", srcNodeID, transport.NextReplica().NodeID)
					}
				}

				// Perform actual request
				br, rpcErr := transport.SendNext(ctx, ba)

				if respI != nil {
					respI.signalIntercepted()
					select {
					case <-tt.tc.Stopper().ShouldQuiesce():
						tt.log("quiescing while held on response intercept %s", respITok)
					case <-respI.jamChan:
						tt.log("%s response released", tb.String())
					}
					if respI.onRelease == returnBatchWithError {
						br := &kvpb.BatchResponse{}
						br.Error = kvpb.NewErrorf("response intercepted on n%d->n%d", srcNodeID, transport.NextReplica().NodeID)
						return br, nil
					}
					if respI.onRelease == returnError {
						return nil, grpcstatus.Errorf(codes.Unavailable, "response intercepted on n%d<-n%d", srcNodeID, transport.NextReplica().NodeID)
					}
				}

				return br, rpcErr
			},
		}
		return interceptor, err
	}
}

// -- Request Intercepting --

// getFirstMatchingInterceptor gets the first interceptor with filters that
// match the given batch request. Criteria is not prioritized, the result
// is simply the first one found in the map, or nil if none found.
func getFirstMatchingInterceptor(
	interceptors *sync.Map,
	ba *kvpb.BatchRequest,
	src roachpb.NodeID,
	target roachpb.ReplicaDescriptor,
) (tok InterceptToken, interceptor *requestIntercept) {
	interceptors.Range(func(key, value any) bool {
		interceptIter := value.(requestIntercept)
		if interceptIter.filter.matchesReq(ba, src, target) {
			tok = key.(InterceptToken)
			interceptor = &interceptIter
			return false
		}
		return true
	})

	return tok, interceptor
}

// requestIntercept represents an intercept that, if having a filter that
// matches a particular batch request, will cause that batch request to
// stall until the channel is closed.
type requestIntercept struct {
	logTag string
	filter requestInterceptFilter

	// The channel the interceptor waits on.
	jamChan     chan struct{}
	jamDoneOnce *sync.Once

	// The channel that, when closed, signifies that a request was intercepted.
	interceptedChan     chan struct{}
	interceptedDoneOnce *sync.Once

	// The action to take after the waiting is completed.
	onRelease postInterceptAction
}

// requestInterceptFilter represents a specific blocking filter to match against a
// batch request.
// By using the associated helper functions, a test can create an intercept
// filter that catches all requests on some criteria (e.g. all txns no txnName
// filter set), while only some requests on other criteria
// (e.g. src == n1 && dst == n2). The criteria are always AND'ed together.
type requestInterceptFilter struct {
	txnName string
	src     roachpb.NodeID
	dst     roachpb.NodeID
	rangeID roachpb.RangeID
	method  string
}

// String implements the Stringer interface.
func (rif requestInterceptFilter) String() string {
	var sb strings.Builder
	maybeAnd := func() {
		if sb.Len() > 0 {
			fmt.Fprintf(&sb, " AND ")
		}
	}
	if rif.txnName != "" {
		fmt.Fprintf(&sb, "txn = \"%s\"", rif.txnName)
	}
	if rif.src != NilNode {
		maybeAnd()
		fmt.Fprintf(&sb, "src-node = n%d", rif.src)
	}
	if rif.dst != NilNode {
		maybeAnd()
		fmt.Fprintf(&sb, "dst-node = n%d", rif.dst)
	}
	if rif.rangeID != roachpb.RangeID(0) {
		maybeAnd()
		fmt.Fprintf(&sb, "range = r%d", rif.rangeID)
	}
	if rif.method != "" {
		maybeAnd()
		fmt.Fprintf(&sb, "method = %s", rif.method)
	}

	if sb.Len() == 0 {
		fmt.Fprintf(&sb, "*")
	}
	return sb.String()
}

// matchesReq returns true if the filter should catch the given batch request.
// This requires all of the set criteria to match.
func (rif *requestInterceptFilter) matchesReq(ba *kvpb.BatchRequest, src roachpb.NodeID, target roachpb.ReplicaDescriptor) bool {
	if rif.txnName != "" && (ba.Txn == nil || ba.Txn.Name != rif.txnName) {
		return false
	}

	if rif.src != NilNode && src != rif.src {
		return false
	}

	if rif.dst != NilNode && target.NodeID != rif.dst {
		return false
	}

	if rif.rangeID != roachpb.RangeID(0) && ba.RangeID != rif.rangeID {
		return false
	}

	if rif.method != "" {
		if !ba.IsSingleRequest() {
			return false
		}

		expectedMethod := kvpb.StringToMethodMap[rif.method]
		if expectedMethod != ba.Requests[0].GetInner().Method() {
			return false
		}
	}

	return true
}

type postInterceptAction int

const (
	returnResponse postInterceptAction = iota
	returnError
	returnBatchWithError
)

type interceptPoint int

const (
	onRequest interceptPoint = iota
	onResponse
)
