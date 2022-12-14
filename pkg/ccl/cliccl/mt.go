// Copyright 2022 The Cockroach Authors.
//
// Licensed as a CockroachDB Enterprise file under the Cockroach Community
// License (the "License"); you may not use this file except in compliance with
// the License. You may obtain a copy of the License at
//
//     https://github.com/cockroachdb/cockroach/blob/master/licenses/CCL.txt

package cliccl

import "github.com/cockroachdb/cockroach/pkg/cli"

func init() {
	cli.MTCmd.AddCommand(mtStartSQLProxyCmd)
	cli.RegisterCommandWithCustomLogging(mtStartSQLProxyCmd)
	cli.MTCmd.AddCommand(mtTestDirectorySvr)
	cli.RegisterCommandWithCustomLogging(mtTestDirectorySvr)
}
