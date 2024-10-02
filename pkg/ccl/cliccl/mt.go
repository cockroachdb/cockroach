// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package cliccl

import "github.com/cockroachdb/cockroach/pkg/cli"

func init() {
	cli.MTCmd.AddCommand(mtStartSQLProxyCmd)
	cli.RegisterCommandWithCustomLogging(mtStartSQLProxyCmd)
	cli.MTCmd.AddCommand(mtTestDirectorySvr)
	cli.RegisterCommandWithCustomLogging(mtTestDirectorySvr)
}
