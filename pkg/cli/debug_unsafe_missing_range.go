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

var debugUnsafeHealMissingRangeCmd = &cobra.Command{
	Use:   "unsafe-heal-missing-range r<rangeID>",
	Short: "Unsafely heal a range that has lost all replicas",
	Long: `

This command is UNSAFE and should only be used with the supervision of
a Cockroach Labs engineer. It is a last-resort option to reset a range
after multiple node failures, without recovering any of its data.

TODO
`,
	Args: cobra.ExactArgs(1),
	RunE: MaybeDecorateGRPCError(runDebugUnsafeHealMissingRange),
}

func runDebugUnsafeHealMissingRange(cmd *cobra.Command, args []string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rangeID, err := strconv.ParseInt(args[0], 10, 64)
	if err != nil {
		return err
	}

	cc, _, finish, err := getClientGRPCConn(ctx)
	_ = cc
	if err != nil {
		return err
	}
	defer finish()
	_, err = roachpb.NewInternalClient(cc).UnsafeHealRange(ctx, &roachpb.UnsafeHealRangeRequest{
		RangeID: roachpb.RangeID(rangeID),
	})
	if err != nil {
		return err
	}
	fmt.Printf("ok; please verify https://<ui>/#/reports/range/%d", rangeID)
	return nil
}
