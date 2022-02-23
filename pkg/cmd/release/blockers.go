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

var blockersFlags = struct {
	releaseSeries  string
	templatesDir   string
	smtpUser       string
	smtpHost       string
	smtpPort       int
	emailAddresses []string
}{}

var postReleaseSeriesBlockersCmd = &cobra.Command{
	Use:   "post-blockers",
	Short: "Post blockers against release series",
	Long:  "Fetch post blockers against release series",
	RunE:  fetchReleaseSeriesBlockers,
}

func init() {
	// TODO: improve flag usage comments
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.releaseSeries, releaseSeries, "", "major release series")
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.templatesDir, templatesDir, "", "templates directory")
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.smtpUser, smtpUser, os.Getenv(envSMTPUser), "SMTP user name")
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.smtpHost, smtpHost, "", "SMTP host")
	postReleaseSeriesBlockersCmd.Flags().IntVar(&blockersFlags.smtpPort, smtpPort, 0, "SMTP port")
	postReleaseSeriesBlockersCmd.Flags().StringArrayVar(&blockersFlags.emailAddresses, emailAddresses, []string{}, "email addresses")

	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(releaseSeries)
	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(templatesDir)
	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(smtpHost)
	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(smtpPort)
	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(emailAddresses)
}

func fetchReleaseSeriesBlockers(_ *cobra.Command, _ []string) error {
	smtpPassword := os.Getenv(envSMTPPassword)
	if smtpPassword == "" {
		return fmt.Errorf("%s environment variable should be set", envSMTPPassword)
	}
	githubToken := os.Getenv(envGithubToken)
	if githubToken == "" {
		return fmt.Errorf("%s environment variable should be set", envGithubToken)
	}
	if blockersFlags.smtpUser == "" {
		return fmt.Errorf("either %s environment variable or %s flag should be set", envSMTPUser, smtpUser)
	}

	// TODO(celia): fetch blockers
	fmt.Println("TODO(celia): fetch blockers")

	blockersURL := "go.crdb.dev/blockers/" + blockersFlags.releaseSeries
	releaseBranch := "release-" + blockersFlags.releaseSeries

	// TODO(celia): dynamically set branchExists, based on whether `releaseBranch` branch exists in crdb repo
	branchExists := true
	if !branchExists {
		blockersURL = "go.crdb.dev/blockers"
		releaseBranch = "master"
	}
	args := messageDataPostBlockers{
		Version:       "v99.9.999 - alpha - 23",
		PrepDate:      "Saturday, April 1",
		ReleaseDate:   "Saturday, April 11",
		TotalBlockers: 23,
		BlockersURL:   blockersURL,
		ReleaseBranch: releaseBranch,
		BlockerList: []ProjectBlocker{
			{
				ProjectName: "Project ABC",
				NumBlockers: 11,
			},
			{
				ProjectName: "Project XYZ",
				NumBlockers: 12,
			},
		},
	}
	opts := sendOpts{
		templatesDir: blockersFlags.templatesDir,
		from:         fmt.Sprintf("Justin Beaver <%s>", blockersFlags.smtpUser),
		host:         blockersFlags.smtpHost,
		port:         blockersFlags.smtpPort,
		user:         blockersFlags.smtpUser,
		password:     smtpPassword,
		to:           blockersFlags.emailAddresses,
	}

	fmt.Println("Sending email")
	if err := sendMailPostBlockers(args, opts); err != nil {
		return fmt.Errorf("cannot send email: %w", err)
	}
	return nil
}
