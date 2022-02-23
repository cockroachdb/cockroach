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
	"time"

	"github.com/spf13/cobra"
)

const (
	// dateLayout is the expected date format for prepDate and publishDate input values.
	dateLayout = "2006-01-02"

	// prepDate is the date when we expect to select the release candidate.
	prepDate = "prep-date"
	// publishDate is the date when we expect to publish the release candidate.
	publishDate = "release-date"

	// nextVersion can be left out for stable/patch releases, but needs to be passed in for pre-releases (alpha/beta/rc).
	nextVersion = "next-version"
)

var blockersFlags = struct {
	releaseSeries  string
	templatesDir   string
	prepDate       string
	publishDate    string
	nextVersion    string
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
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.releaseSeries, releaseSeries, "", "major release series")
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.templatesDir, templatesDir, "", "template directory")
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.prepDate, prepDate, "", "date to select candidate")
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.publishDate, publishDate, "", "date to publish candidate")
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.nextVersion, nextVersion, "", "next release version")

	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.smtpUser, smtpUser, os.Getenv(envSMTPUser), "SMTP user name")
	postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.smtpHost, smtpHost, "", "SMTP host")
	postReleaseSeriesBlockersCmd.Flags().IntVar(&blockersFlags.smtpPort, smtpPort, 0, "SMTP port")
	postReleaseSeriesBlockersCmd.Flags().StringArrayVar(&blockersFlags.emailAddresses, emailAddresses, []string{}, "email addresses")

	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(releaseSeries)
	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(templatesDir)
	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(prepDate)
	_ = postReleaseSeriesBlockersCmd.MarkFlagRequired(publishDate)

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
	releasePrepDate, err := time.Parse(dateLayout, blockersFlags.prepDate)
	if err != nil {
		return fmt.Errorf("%s is not parseable into %s date layout", blockersFlags.prepDate, dateLayout)
	}
	releasePublishDate, err := time.Parse(dateLayout, blockersFlags.prepDate)
	if err != nil {
		return fmt.Errorf("%s is not parseable into %s date layout", blockersFlags.prepDate, dateLayout)
	}
	if blockersFlags.nextVersion == "" {
		var err error
		blockersFlags.nextVersion, err = findNextVersion(blockersFlags.releaseSeries)
		if err != nil {
			return fmt.Errorf("cannot find next release version: %w", err)
		}
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
		Version:       blockersFlags.nextVersion,
		PrepDate:      releasePrepDate.Format("Monday, January 1"),
		ReleaseDate:   releasePublishDate.Format("Monday, January 1"),
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
