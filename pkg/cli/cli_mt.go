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

import "github.com/spf13/cobra"

func init() {
	cockroachCmd.AddCommand(mtCmd)
	mtCmd.AddCommand(mtStartSQLCmd)
}

// mtCmd is the base command for functionality related to multi-tenancy.
var mtCmd = &cobra.Command{
	Use:    "mt [command]",
	Short:  "commands related to multi-tenancy",
	RunE:   usageAndErr,
	Hidden: true,
}
