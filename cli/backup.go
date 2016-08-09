// Copyright 2016 The Cockroach Authors.
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
// permissions and limitations under the License. See the AUTHORS file
// for names of contributors.
//
// Author: Daniel Harrison (daniel.harrison@gmail.com)

// +build experimental

package cli

import (
	"fmt"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"

	netcontext "golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/internal/client"
	"github.com/cockroachdb/cockroach/keys"
	"github.com/cockroachdb/cockroach/roachpb"
	"github.com/cockroachdb/cockroach/sql"
	"github.com/cockroachdb/cockroach/util/timeutil"
)

type backupContext struct {
	table     string
	overwrite bool
}

var backupCtx = backupContext{}

func init() {
	f := restoreCmd.Flags()
	f.StringVar(&backupCtx.table, "table", "", "table or restore (or empty for all user tables)")
	f.BoolVar(&backupCtx.overwrite, "overwrite", false, "true to overwrite existing tables")
}

func allRangeDescriptors(txn *client.Txn) ([]roachpb.RangeDescriptor, error) {
	var startKey roachpb.Key
	{
		k := roachpb.KeyMin.Next()
		rk, err := keys.Addr(k)
		if err != nil {
			return nil, err
		}
		startKey = keys.RangeMetaKey(rk)
	}
	endKey := keys.Meta2Prefix.PrefixEnd()

	rows, err := txn.Scan(startKey, endKey, 0)
	if err != nil {
		return nil, errors.Wrap(err, "scan failed")
	}

	rangeDescs := make([]roachpb.RangeDescriptor, len(rows))
	for i, row := range rows {
		if err := row.ValueProto(&rangeDescs[i]); err != nil {
			return nil, errors.Wrapf(err, "%s: unable to unmarshal range descriptor", row.Key)
		}
	}
	return rangeDescs, nil
}

func runBackup(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return errors.New("output basepath argument is required")
	}
	base = filepath.Join(args[0], timeutil.Now().Format(time.RFC3339))

	ctx := netcontext.Background()
	kvDB, stopper := makeDBClient()
	defer stopper.Stop()

	if err := sql.Backup(ctx, *kvDB, base); err != nil {
		return err
	}

	fmt.Printf("Backed up to %s\n", base)
	return nil
}

var backupCmd = &cobra.Command{
	Use:   "backup [options] <basepath>",
	Short: "backup all SQL tables",
	Long:  "Exports a consistent snapshot of all SQL tables to storage.",
	RunE:  runBackup,
}

func runRestore(cmd *cobra.Command, args []string) error {
	if len(args) != 1 {
		return errors.New("input basepath argument is required")
	}
	base := args[0]

	ctx := netcontext.Background()
	kvDB, stopper := makeDBClient()
	defer stopper.Stop()

	if err := sql.Restore(ctx, *kvDB, base, backupCtx.table, backupCtx.overwrite); err != nil {
		return err
	}

	fmt.Printf("Restored from %s\n", base)
	return nil
}

var restoreCmd = &cobra.Command{
	Use:   "restore [options] <basepath>",
	Short: "restore SQL tables from a backup",
	Long:  "Imports one or all SQL tables, restoring them to a previously snapshotted state.",
	RunE:  runRestore,
}

func backupCmds() []*cobra.Command {
	return []*cobra.Command{backupCmd, restoreCmd}
}
