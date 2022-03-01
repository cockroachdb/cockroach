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
  "context"
  "fmt"
  "os"
  "sort"

  "github.com/spf13/cobra"
)

const NoProjectName = "No Project"

var blockersFlags = struct {
  releaseSeries  string
  templatesDir   string
  smtpUser       string
  smtpHost       string
  smtpPort       int
  emailAddresses []string
  dryRun         bool
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
  postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.templatesDir, templatesDir, "", "major release series")
  postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.smtpUser, smtpUser, os.Getenv(envSMTPUser), "SMTP user name")
  postReleaseSeriesBlockersCmd.Flags().StringVar(&blockersFlags.smtpHost, smtpHost, "", "SMTP host")
  postReleaseSeriesBlockersCmd.Flags().IntVar(&blockersFlags.smtpPort, smtpPort, 0, "SMTP port")
  postReleaseSeriesBlockersCmd.Flags().StringArrayVar(&blockersFlags.emailAddresses, emailAddresses, []string{}, "email addresses")
  postReleaseSeriesBlockersCmd.Flags().BoolVar(&blockersFlags.dryRun, dryRun, false, "use dry run Jira project for issues")

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
    return fmt.Errorf("either %s environment variable or %s flag should be set", envGithubToken, smtpUser)
  }

  client := newGithubClient(context.Background(), githubToken)
  blockers, err := fetchOpenBlockers(client, blockersFlags.releaseSeries)
  if err != nil {
    return fmt.Errorf("cannot fetch blockers: %w", err)
  }

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
    BlockerList:   blockers,
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

func fetchOpenBlockers(
  client githubClient,
  releaseSeries string,
) ([]ProjectBlocker, error) {
  // TODO(celia) account for master branch, i.e. what should `releaseSeries` value be here?
  releaseBranch := fmt.Sprintf("branch-release-%s", releaseSeries)
  issues, err := client.openIssues([]string{
    "release-blocker",
    releaseBranch,
  })
  if err != nil {
    return nil, err
  }
  if len(issues) == 0 {
    return []ProjectBlocker{}, nil
  }
  numBlockersByProject := make(map[string]int)
  for _, issue := range issues {
    projectName := NoProjectName
    if len(issue.ProjectName) > 0 {
      projectName = issue.ProjectName
    }
    total, _ := numBlockersByProject[projectName]
    numBlockersByProject[projectName] = total + 1
  }
  var blockers projectBlockers
  for projectName, numBlockers := range numBlockersByProject {
    blockers = append(blockers, ProjectBlocker{
      ProjectName: projectName,
      NumBlockers: numBlockers,
    })
  }
  // sorting blockers Projects alphabetically, except for "No Project", which always goes last.
  sort.Sort(blockers)
  return blockers, nil
}

// projectBlockers implements the sort.Sort interface, so that we can
// sort blockers alphabetically by Project Name,
// except for "No Project" (blockers without a project), which should always
// be listed last.
type projectBlockers []ProjectBlocker

func (p projectBlockers) Len() int {
  return len(p)
}

func (p projectBlockers) Less(i, j int) bool {
  if p[i].ProjectName == NoProjectName {
    return false
  }
  if p[j].ProjectName == NoProjectName {
    return true
  }
  return p[i].ProjectName < p[j].ProjectName
}

func (p projectBlockers) Swap(i, j int) {
  p[i], p[j] = p[j], p[i]
}
