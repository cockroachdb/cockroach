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

import "github.com/spf13/cobra"

var rootCmd = &cobra.Command{Use: "release"}

const (
	envSMTPUser     = "SMTP_USER"
	envSMTPPassword = "SMTP_PASSWORD"
	envGithubToken  = "GITHUB_TOKEN"
	releaseSeries   = "release-series"
	templatesDir    = "template-dir"
	smtpUser        = "smtp-user"
	smtpHost        = "smtp-host"
	smtpPort        = "smtp-port"
	emailAddresses  = "to"
	dryRun          = "dry-run"

	fromEmailFormat = "Justin Beaver <%s>"
)

func main() {
	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}

func init() {
	rootCmd.AddCommand(pickSHACmd)
	rootCmd.AddCommand(postReleaseSeriesBlockersCmd)
	rootCmd.AddCommand(cancelReleaseSeriesDateCmd)
	rootCmd.AddCommand(setOrchestrationVersionCmd)
	rootCmd.AddCommand(updateReleasesTestFileCmd)
	rootCmd.AddCommand(setCockroachVersionCmd)
	rootCmd.AddCommand(updateVersionsCmd)
}
