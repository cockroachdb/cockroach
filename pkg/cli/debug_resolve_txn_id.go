// Copyright 2026 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cli

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/cli/clierrorplus"
	"github.com/cockroachdb/cockroach/pkg/server/serverpb"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/uuid"
	"github.com/spf13/cobra"
)

var debugResolveTxnIDCmd = &cobra.Command{
	Use:   "resolve-txn-id <txn_id UUID> <txn coordinator NodeID> --url=<cluster connection string>",
	Short: "resolve txn ID to txn fingerprint ID (NodeID is optional)",
	Long: `
Resolves the txn ID (KV-level concept) to the corresponding txn fingerprint ID
(SQL-level concept) which can later be used to connect the txn to SQL
observability tooling.

It takes the txn ID as well as an optional txn coordinator node ID as arguments.
If the txn coordinator node is not specified, every node in the cluster will be
consulted sequentially. It will produce output like

  n1: txn ID c5893b24-a780-4b52-8d5c-27e0b94c75fc -> txn fingerprint ID 1125673994937403159 -> transaction_fingerprint_id '\x0f9f3288c9238717'

where the last encoded integer can be used to query SQL observability utilities.

Note that since we keep a limited number of resolved txn IDs in the cache on
each node, it's possible for the target txn to be removed from the cache, so
this command might not actually resolve anything.
`,
	Args: cobra.MinimumNArgs(1),
	RunE: clierrorplus.MaybeDecorateError(runDebugTxnID),
}

func runDebugTxnID(_ *cobra.Command, args []string) (resErr error) {
	txnID, err := uuid.FromString(args[0])
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
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
		_, err = strconv.ParseInt(args[1], 10, 64)
		if err != nil {
			return err
		}
		nodesToTry = []string{args[1]}
	} else {
		// Retrieve the list of all nodes.
		nodes, err := status.Nodes(ctx, &serverpb.NodesRequest{})
		if err != nil {
			fmt.Printf("cannot retrieve node list (will try node IDs 1-100): %v\n", err)
			for i := 1; i <= 100; i++ {
				nodesToTry = append(nodesToTry, strconv.Itoa(i))
			}
		} else {
			for _, node := range nodes.Nodes {
				nodesToTry = append(nodesToTry, strconv.Itoa(int(node.Desc.NodeID)))
			}
		}
	}

	var resolved bool
	for _, coordinatorID := range nodesToTry {
		resp, err := status.TxnIDResolution(ctx, &serverpb.TxnIDResolutionRequest{
			CoordinatorID: coordinatorID,
			TxnIDs:        []uuid.UUID{txnID},
		})
		if err != nil {
			fmt.Printf("got an error when contacting n%s: %v\n", coordinatorID, err)
		} else {
			for _, txn := range resp.ResolvedTxnIDs {
				resolved = true
				encoded := encoding.EncodeUint64Ascending(nil /* b */, uint64(txn.TxnFingerprintID))
				fmt.Printf(
					"n%s: txn ID %v -> txn fingerprint ID %v -> transaction_fingerprint_id '\\x%x'\n",
					coordinatorID, txn.TxnID, txn.TxnFingerprintID, encoded,
				)
			}
		}
	}
	if !resolved {
		fmt.Printf("couldn't resolve txn ID\n", txnID)
	}
	return nil
}
