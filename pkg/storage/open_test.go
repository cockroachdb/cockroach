// Copyright 2024 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package storage

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/clusterversion"
	"github.com/cockroachdb/cockroach/pkg/roachpb"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/storage/fs"
	"github.com/cockroachdb/cockroach/pkg/storage/storagepb"
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

	// Mock the encryption-at-rest constructor.
	oldNewEncryptedEnvFunc := fs.NewEncryptedEnvFunc
	defer func() { fs.NewEncryptedEnvFunc = oldNewEncryptedEnvFunc }()
	fs.NewEncryptedEnvFunc = fauxNewEncryptedEnvFunc

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

				var envConfig fs.EnvConfig
				if td.HasArg("encrypted-at-rest") {
					envConfig.EncryptionOptions = &storagepb.EncryptionOptions{}
				}
				if td.HasArg("read-only") {
					envConfig.RW = fs.ReadOnly
				}
				e, err := fs.InitEnv(context.Background(), memfs, dir, envConfig, nil)
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

				var cfg storagepb.WALFailover
				if flagStr != "" {
					if err := cfg.Set(flagStr); err != nil {
						return fmt.Sprintf("error parsing flag: %q", err)
					}
				}
				if td.HasArg("path-encrypted") {
					cfg.Path.Encryption = &storagepb.EncryptionOptions{}
				}
				if td.HasArg("prev-path-encrypted") {
					cfg.PrevPath.Encryption = &storagepb.EncryptionOptions{}
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

				// Mock a cluster version, defaulting to latest.
				version := clusterversion.Latest.Version()
				if td.HasArg("min-version") {
					var major, minor int
					td.ScanArgs(t, "min-version", &major, &minor)
					version = roachpb.Version{
						Major: int32(major),
						Minor: int32(minor),
					}
				}
				// Match the current offsetting policy.
				if clusterversion.Latest.Version().Major > clusterversion.DevOffset {
					version.Major += clusterversion.DevOffset
				}
				settings := cluster.MakeTestingClusterSettingsWithVersions(version, version, true /* initializeVersion */)

				engine, err := Open(context.Background(), openEnv, settings, WALFailover(cfg, envs, defaultFS, nil))
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
