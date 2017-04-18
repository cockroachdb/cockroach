// Copyright 2015 The Cockroach Authors.
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
// Author: Peter Mattis (peter@cockroachlabs.com)

package cli

import (
	"fmt"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/internal/client"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/security"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// MakeDBClient creates a kv client for use in cli tools.
func MakeDBClient() (*client.DB, *stop.Stopper, error) {
	// The KV endpoints require the node user.
	baseCfg.User = security.NodeUser
	conn, clock, stopper, err := getClientGRPCConn()
	if err != nil {
		return nil, nil, err
	}
	return client.NewDB(client.NewSender(conn), clock), stopper, nil
}

// A lsRangesCmd command lists the ranges in a cluster.
var lsRangesCmd = &cobra.Command{
	Use:   "ls [options] [<start-key>]",
	Short: "lists the ranges",
	Long: `
Lists the ranges in a cluster.
`,
	RunE: MaybeDecorateGRPCError(runLsRanges),
}

func runLsRanges(cmd *cobra.Command, args []string) error {
	if len(args) > 1 {
		return usageAndError(cmd)
	}

	var startKey roachpb.Key
	{
		k := roachpb.KeyMin.Next()
		if len(args) > 0 {
			k = roachpb.Key(args[0])
		}
		rk, err := keys.Addr(k)
		if err != nil {
			panic(err)
		}
		startKey = keys.RangeMetaKey(rk)
	}
	endKey := keys.Meta2Prefix.PrefixEnd()

	kvDB, stopper, err := MakeDBClient()
	if err != nil {
		return err
	}
	defer stopper.Stop(stopperContext(stopper))

	rows, err := kvDB.Scan(context.Background(), startKey, endKey, maxResults)
	if err != nil {
		return err
	}

	for _, row := range rows {
		desc := &roachpb.RangeDescriptor{}
		if err := row.ValueProto(desc); err != nil {
			return errors.Wrapf(err, "unable to unmarshal range descriptor at %s", row.Key)
		}
		fmt.Printf("%s-%s [%d]\n", desc.StartKey, desc.EndKey, desc.RangeID)
		for i, replica := range desc.Replicas {
			fmt.Printf("\t%d: node-id=%d store-id=%d\n",
				i, replica.NodeID, replica.StoreID)
		}
	}
	fmt.Printf("%d result(s)\n", len(rows))
	return nil
}

// A splitRangeCmd command splits a range.
var splitRangeCmd = &cobra.Command{
	Use:   "split [options] <key>",
	Short: "splits a range",
	Long: `
Splits the range containing <key> at <key>.
`,
	RunE: MaybeDecorateGRPCError(runSplitRange),
}

func runSplitRange(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return usageAndError(cmd)
	}

	key := roachpb.Key(args[0])

	kvDB, stopper, err := MakeDBClient()
	if err != nil {
		return err
	}
	defer stopper.Stop(stopperContext(stopper))
	return errors.Wrap(kvDB.AdminSplit(context.Background(), key), "split failed")
}

var rangeCmds = []*cobra.Command{
	lsRangesCmd,
	splitRangeCmd,
}

var rangeCmd = &cobra.Command{
	Use:   "range",
	Short: "list and split ranges",
	RunE: func(cmd *cobra.Command, args []string) error {
		return cmd.Usage()
	},
}

func init() {
	rangeCmd.AddCommand(rangeCmds...)
}
