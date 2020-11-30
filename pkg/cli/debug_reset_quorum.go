// Copyright 2020 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.
package cli

import (
	"context"
	"fmt"
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/spf13/cobra"
)

var debugResetQuorumCmd = &cobra.Command{
	Use:   "reset-quorum",
	Short: "Reset quorum on the given range by designating a store on the target node as the sole voter.",
	Long: `
Reset quorum on the given range by designating the current node as the sole voter. 
Any existing data for the range is discarded. 

This command is UNSAFE and should only be used with the supervision of
a Cockroach Labs engineer. It is a last-resort option to recover a specified 
range after multiple node failures and loss of quorum.
Rest of description TBD.
`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runDebugResetQuorumRequest),
}

func runDebugResetQuorumRequest(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rangeID, err := strconv.ParseInt(args[0], 10, 32)
	if err != nil {
		return err
	}

	// Set up GRPC Connection for running ResetQuorum.
	cc, _, finish, err := getClientGRPCConn(ctx, serverCfg)
	_ = cc
	if err != nil {
		return err
	}
	defer finish()
	// Call ResetQuorum to reset quorum for given range on current node.
	_, err = roachpb.NewInternalClient(cc).ResetQuorum(ctx, &roachpb.ResetQuorumRequest{
		RangeID: int32(rangeID),
	})
	if err != nil {
		return err
	}
	fmt.Printf("ok; please verify https://<ui>/#/reports/range/%d", rangeID)

	return nil
}
