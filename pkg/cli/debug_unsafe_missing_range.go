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
	"strconv"

	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var debugUnsafeHealMissingRangeCmd = &cobra.Command{
	Use:   "unsafe-heal-missing-range r<rangeID> n<nodeID> s<storeID>",
	Short: "Unsafely heal a range that has lost all replicas",
	Long: `

This command is UNSAFE and should only be used with the supervision of
a Cockroach Labs engineer. It is a last-resort option to recover data
after multiple node failures. The recovered data is not guaranteed to
be consistent.

TODO
`,
	Args: cobra.ExactArgs(3),
	RunE: MaybeDecorateGRPCError(runDebugUnsafeHealMissingRange),
}

func runDebugUnsafeHealMissingRange(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sRangeID := args[0]
	if len(sRangeID) == 0 || sRangeID[0] != 'r' {
		return errors.New("first argument must be of form r<rangeID>")
	}
	rangeID, err := strconv.ParseInt(sRangeID[1:], 10, 64)
	if err != nil {
		return err
	}

	sNodeID := args[1]
	if len(sNodeID) == 0 || sNodeID[0] != 'n' {
		return errors.New("second argument must be of form n<nodeID>")
	}
	nodeID, err := strconv.ParseInt(sNodeID[1:], 10, 32)
	if err != nil {
		return err
	}

	sStoreID := args[2]
	if len(sStoreID) == 0 || sStoreID[0] != 's' {
		return errors.New("third argument must be of form s<storeID>")
	}
	storeID, err := strconv.ParseInt(sStoreID[1:], 10, 32)
	if err != nil {
		return err
	}

	req := roachpb.UnsafeHealRangeRequest{
		RangeID: roachpb.RangeID(rangeID),
		NodeID:  roachpb.NodeID(nodeID),
		StoreID: roachpb.StoreID(storeID),
	}

	cc, _, finish, err := getClientGRPCConn(ctx, serverCfg)
	if err != nil {
		return err
	}
	defer finish()

	_, err = roachpb.NewInternalClient(cc).UnsafeHealRange(ctx, &req)
	return err
}
