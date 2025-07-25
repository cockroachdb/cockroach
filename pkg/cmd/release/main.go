// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import "github.com/spf13/cobra"

var rootCmd = &cobra.Command{Use: "release"}
var artifactsDir string

func init() {
	rootCmd.PersistentFlags().StringVar(&artifactsDir, "artifacts-dir", "", "artifacts directory")
}

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
	rootCmd.AddCommand(updateReleasesTestFilesCmd)
	rootCmd.AddCommand(updateVersionsCmd)
}
