// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package storage

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/testutils/datapathutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/datadriven"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestWALFailover(t *testing.T) {
	defer leaktest.AfterTest(t)()
	defer log.Scope(t).Close(t)

	var allEnvs fs.Envs
	defer func() { allEnvs.CloseAll() }()

	// Create an in-memory FS to serve as the default FS (used
	// when explicit paths are provided).
	defaultFS := vfs.NewMem()

	getEnv := func(name string) *fs.Env {
		for _, e := range allEnvs {
			if e.Dir == name {
				return e
			}
		}
		return nil
	}

	datadriven.RunTest(t, datapathutils.TestDataPath(t, "wal_failover_config"),
		func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "mkenv":
				dir := td.CmdArgs[0].String()
				if e := getEnv(dir); e != nil {
					return fmt.Sprintf("env %s already exists", e.Dir)
				}
				memfs := vfs.NewMem()
				require.NoError(t, memfs.MkdirAll(dir, os.ModePerm))
				e, err := fs.InitEnv(context.Background(), memfs, dir, fs.EnvConfig{})
				if err != nil {
					return fmt.Sprintf("err = %q", err)
				}
				allEnvs = append(allEnvs, e)
				return ""
			case "open":
				var flagStr, openDir string
				var envDirs []string
				td.ScanArgs(t, "flag", &flagStr)
				td.ScanArgs(t, "open", &openDir)
				td.ScanArgs(t, "envs", &envDirs)

				var cfg base.WALFailoverConfig
				if flagStr != "" {
					if err := cfg.Set(flagStr); err != nil {
						return fmt.Sprintf("error parsing flag: %q", err)
					}
				}
				openEnv := getEnv(openDir)
				if openEnv == nil {
					return fmt.Sprintf("unknown env %q", openDir)
				}
				var envs fs.Envs
				for _, dir := range envDirs {
					e := getEnv(dir)
					if e == nil {
						return fmt.Sprintf("unknown env %q", dir)
					}
					envs = append(envs, e)
				}
				openEnv.Ref()

				// Mock a cluster version, defaulting to 24.1.
				minVersionMajor, minVersionMinor := 24, 1
				minVersion := clusterversion.V24_1.Version()
				if td.MaybeScanArgs(t, "min-version", &minVersionMajor, &minVersionMinor) {
					// TODO(jackson): Add datadriven support for scanning into int32s so we can
					// scan directly into minVersion.
					minVersion.Major, minVersion.Minor = int32(minVersionMajor), int32(minVersionMinor)
				}
				settings := cluster.MakeTestingClusterSettingsWithVersions(minVersion, minVersion, true /* initializeVersion */)

				// Mock an enterpise license, or not if disable-enterprise is specified.
				enterpriseEnabledFunc := base.CCLDistributionAndEnterpriseEnabled
				base.CCLDistributionAndEnterpriseEnabled = func(st *cluster.Settings) bool { return !td.HasArg("disable-enterprise") }
				defer func() { base.CCLDistributionAndEnterpriseEnabled = enterpriseEnabledFunc }()

				engine, err := Open(context.Background(), openEnv, settings, WALFailover(cfg, envs, defaultFS))
				if err != nil {
					openEnv.Close()
					return fmt.Sprintf("open error: %s", err)
				}
				var buf bytes.Buffer
				fmt.Fprintln(&buf, "OK")
				if failoverCfg := engine.cfg.opts.WALFailover; failoverCfg != nil {
					fmt.Fprintf(&buf, "secondary = %s\n", failoverCfg.Secondary.Dirname)
					dur, ok := failoverCfg.UnhealthyOperationLatencyThreshold()
					fmt.Fprintf(&buf, "UnhealthyOperationLatencyThreshold() = (%s,%t)\n", dur, ok)
				}
				for _, recoveryDir := range engine.cfg.opts.WALRecoveryDirs {
					fmt.Fprintf(&buf, "WALRecoveryDir: %s\n", recoveryDir.Dirname)
				}
				engine.Close()
				return buf.String()
			default:
				return fmt.Sprintf("unrecognized command %q", td.Cmd)
			}
		})
}
