// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var debugResolveTxnIDCmd = &cobra.Command{
	Use:   "resolve-txn-id <txn_id UUID> [<txn coordinator NodeID>] --url=<cluster connection string>",
	Short: "resolve txn ID to txn fingerprint ID (NodeID is optional)",
	Long: `
Resolves the txn ID (KV-level concept) to the corresponding txn fingerprint ID
(SQL-level concept) which can later be used to connect the txn to SQL
observability tooling.

It takes the txn ID as well as an optional txn coordinator node ID as arguments.
If the txn coordinator node is not specified, every node in the cluster will be
consulted in parallel. It will produce output like

  n1: txn ID c5893b24-a780-4b52-8d5c-27e0b94c75fc -> txn fingerprint ID 1125673994937403159 -> transaction_fingerprint_id '\x0f9f3288c9238717'

where the last encoded integer can be used to query SQL observability utilities.

Note that since we keep a limited number of resolved txn IDs in the cache on
each node, it's possible for the target txn to be removed from the cache, so
this command might not actually resolve anything.
`,
	Args: cobra.RangeArgs(1, 2),
	RunE: clierrorplus.MaybeDecorateError(runDebugResolveTxnID),
}

func runDebugResolveTxnID(_ *cobra.Command, args []string) error {
	txnID, err := uuid.FromString(args[0])
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second) // nolint:context
	defer cancel()

	// Connect to the node pointed to in the command line.
	conn, finish, err := newClientConn(ctx, serverCfg)
	if err != nil {
		return err
	}
	defer finish()

	status := conn.NewStatusClient()

	var nodesToTry []string
	if len(args) > 1 {
		nodeID, err := strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return err
		}
		fmt.Printf("will contact n%d\n", nodeID)
		nodesToTry = []string{args[1]}
	} else {
		// Retrieve the list of all nodes.
		nodes, err := status.Nodes(ctx, &serverpb.NodesRequest{})
		if err != nil {
			return err
		}
		for _, node := range nodes.Nodes {
			nodesToTry = append(nodesToTry, strconv.Itoa(int(node.Desc.NodeID)))
		}
		fmt.Printf("will contact nodes %s\n", nodesToTry)
	}

	type res struct {
		resp   *serverpb.TxnIDResolutionResponse
		nodeID string
	}
	// Each goroutine will communicate on exactly one of the channels.
	resCh := make(chan res, len(nodesToTry))
	errCh := make(chan error, len(nodesToTry))
	var wg sync.WaitGroup
	defer wg.Wait()
	for i := range nodesToTry {
		coordinatorNodeID := nodesToTry[i]
		// Each goroutine either completes the gRPC call or is canceled once
		// another goroutine successfully resolved the txn.
		wg.Go(func() {
			resp, err := status.TxnIDResolution(ctx, &serverpb.TxnIDResolutionRequest{
				CoordinatorID: coordinatorNodeID,
				TxnIDs:        []uuid.UUID{txnID},
			})
			if err != nil {
				errCh <- err
			} else {
				resCh <- res{
					resp:   resp,
					nodeID: coordinatorNodeID,
				}
			}
		})
	}

	var resolved res
	var firstErr error
	for i := 0; i < len(nodesToTry); i++ {
		select {
		case res := <-resCh:
			if len(res.resp.ResolvedTxnIDs) > 0 {
				resolved = res
				fmt.Print("txn resolution succeeded, canceling outstanding gRPC requests\n")
				cancel()
			}
		case err := <-errCh:
			if firstErr == nil {
				firstErr = err
			}
		}
	}

	// Ignore all errors if we resolved the txn ID.
	if resolved.resp != nil {
		txn := resolved.resp.ResolvedTxnIDs[0]
		encoded := encoding.EncodeUint64Ascending(nil /* b */, uint64(txn.TxnFingerprintID))
		fmt.Printf(
			"n%s: txn ID %v -> txn fingerprint ID %v -> transaction_fingerprint_id '\\x%x'\n",
			resolved.nodeID, txn.TxnID, txn.TxnFingerprintID, encoded,
		)
		return nil
	}
	// If we didn't succeed, then return the first error if present.
	if firstErr != nil {
		return firstErr
	}
	// We didn't resolve the txn ID nor did we get an error - the target
	// transaction must have been purged from the cache.
	return errors.Newf("didn't resolve txn ID %v", txnID)
}
