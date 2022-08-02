// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cliccl

import (
	"context"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/cli"
	"github.com/cockroachdb/cockroach/pkg/testutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/datadriven"
)

// This test doctoring backup images.
func TestDoctorCluster(t *testing.T) {
	defer leaktest.AfterTest(t)()
	c := cli.NewCLITest(cli.TestCLIParams{T: t, NoServer: true})
	defer c.Cleanup()

	backupDir := testutils.TestDataPath(t, "doctor")
	ctx := context.Background()
	srv, _, _ := serverutils.StartServer(t, base.TestServerArgs{ExternalIODir: backupDir, Insecure: true})
	defer srv.Stopper().Stop(ctx)

	t.Run("examine", func(t *testing.T) {

		// Using datadriven allows TESTFLAGS=-rewrite.
		datadriven.RunTest(t, testutils.TestDataPath(t, "doctor", "test_examine_cluster"), func(t *testing.T, td *datadriven.TestData) string {
			switch td.Cmd {
			case "examine-backup":
				version := ""
				if len(td.CmdArgs) >= 2 {
					version = td.CmdArgs[1].Key
				}
				out, err := c.RunWithCapture(
					fmt.Sprintf("debug doctor examine backup %s %s %s",
						td.CmdArgs[0].Key,
						backupDir,
						version))
				if err != nil {
					t.Fatal(err)

				}
				return out
			default:
				t.Fatalf("unknown command %s", td.Cmd)
			}
			return ""
		})
	})
}
