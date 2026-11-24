// Copyright 2019 The Cockroach Authors.
//
// Use of this software is governed by the CockroachDB Software License
// included in the /LICENSE file.

package main

import (
	"os"

	"github.com/spf13/cobra"
)

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

	// envIsProductionRepo is the env var the GHA workflow forwards from a
	// repository variable that's set only on the production release repo.
	// It's the single signal this binary uses to choose between prod and
	// non-prod side-effect targets (e.g. Slack channels). When unset
	// (TeamCity, local invocations, forks) we treat the run as non-prod.
	envIsProductionRepo = "IS_PRODUCTION_REPO"
)

// isProductionRepo reports whether the binary is running under the
// production release repo, as signalled by the IS_PRODUCTION_REPO env var
// (forwarded from a GHA repository variable that's only set on the
// canonical repo). When unset (TeamCity, local invocations, forks) the
// default is false — non-prod paths are safer.
func isProductionRepo() bool {
	return os.Getenv(envIsProductionRepo) == "true"
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		panic(err)
	}
}

func init() {
	rootCmd.AddCommand(cutStagingBranchesCmd)
	rootCmd.AddCommand(pickSHACmd)
	rootCmd.AddCommand(updateReleasesTestFilesCmd)
	rootCmd.AddCommand(updateVersionsCmd)
	rootCmd.AddCommand(updateWorkflowBranchesCmd)
}
