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
	"os"
	"os/signal"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
	"github.com/cockroachdb/errors"
	"github.com/spf13/cobra"
)

var mtStartSQLCmd = &cobra.Command{
	Use:   "start-sql",
	Short: "start a standalone SQL server",
	Long: `
Start a standalone SQL server.

This functionality is **experimental** and for internal use only.
`,
	Args: cobra.NoArgs,
	RunE: MaybeDecorateGRPCError(runStartSQL),
}

func runStartSQL(cmd *cobra.Command, args []string) error {
	ctx := context.Background()
	const clusterName = ""
	stopper := stop.NewStopper()
	defer stopper.Stop(ctx)

	st := serverCfg.BaseConfig.Settings

	// TODO(tbg): this has to be passed in. See the upgrade strategy in:
	// https://github.com/cockroachdb/cockroach/issues/47919
	if err := clusterversion.Initialize(ctx, st.Version.BinaryVersion(), &st.SV); err != nil {
		return err
	}

	tempStorageMaxSizeBytes := int64(base.DefaultInMemTempStorageMaxSizeBytes)
	if err := diskTempStorageSizeValue.Resolve(
		&tempStorageMaxSizeBytes, memoryPercentResolver,
	); err != nil {
		return err
	}

	serverCfg.SQLConfig.TempStorageConfig = base.TempStorageConfigFromEnv(
		ctx,
		st,
		base.StoreSpec{InMemory: true},
		"", // parentDir
		tempStorageMaxSizeBytes,
	)

	addr, err := server.StartTenant(
		ctx,
		stopper,
		clusterName,
		serverCfg.BaseConfig,
		serverCfg.SQLConfig,
	)
	if err != nil {
		return err
	}
	log.Infof(ctx, "SQL server for tenant %s listening at %s", serverCfg.SQLConfig.TenantID, addr)

	// TODO(tbg): make the other goodies in `./cockroach start` reusable, such as
	// logging to files, periodic memory output, heap and goroutine dumps, debug
	// server, graceful drain. Then use them here.

	ch := make(chan os.Signal, 1)
	signal.Notify(ch, drainSignals...)
	sig := <-ch
	return errors.Newf("received signal %v", sig)
}
