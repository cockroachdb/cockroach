// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

const (
	versionFlag = "version"
)

var orchestrationFlags = struct {
	version      string
	templatesDir string
	outputDir    string
}{}

var setOrchestrationVersionCmd = &cobra.Command{
	Use:   "set-orchestration-version",
	Short: "Set orchestration version",
	Long:  "Updates orchestration version under the ./cloud/kubernetes directory",
	RunE:  setOrchestrationVersion,
}

func init() {
	setOrchestrationVersionCmd.Flags().StringVar(&orchestrationFlags.version, versionFlag, "", "cockroachdb version")
	_ = setOrchestrationVersionCmd.MarkFlagRequired(versionFlag)
}

func setOrchestrationVersion(_ *cobra.Command, _ []string) error {
	pwd, err := os.Getwd()
	if err != nil {
		return fmt.Errorf("getting working directory: %w", err)
	}
	return updateOrchestration(pwd, orchestrationFlags.version)
}
